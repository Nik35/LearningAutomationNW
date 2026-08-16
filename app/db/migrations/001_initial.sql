-- =============================================================================
-- Migration 001 — Initial schema for F5 GTM automation service
-- Target: Microsoft SQL Server (Azure SQL / SQL Server 2019+)
-- §6 of the implementation plan: five tables + concurrency guard index
-- =============================================================================

-- ---------------------------------------------------------------------------
-- requests
-- Central record for every provisioning workflow.
-- status is constrained to the 15 values defined in §3.6.
-- ---------------------------------------------------------------------------
CREATE TABLE requests (
    request_id              UNIQUEIDENTIFIER    NOT NULL DEFAULT NEWID(),
    idempotency_key         NVARCHAR(64)        NOT NULL,   -- sha256 hex digest
    action                  NVARCHAR(50)        NOT NULL,   -- create | update | delete
    wip_fqdn                NVARCHAR(255)       NOT NULL,
    target_device           NVARCHAR(255)       NOT NULL,
    payload_hash            NVARCHAR(64)        NOT NULL,   -- sha256 hex digest of raw payload
    payload_json            NVARCHAR(MAX)       NOT NULL,
    status                  NVARCHAR(50)        NOT NULL
                            CONSTRAINT CK_requests_status CHECK (
                                status IN (
                                    'RECEIVED', 'VALIDATING', 'QUEUED', 'RUNNING',
                                    'VERIFYING', 'COMPLETED', 'VERIFY_FAILED',
                                    'REMEDIATING', 'FAILED', 'ROLLING_BACK',
                                    'ROLLED_BACK', 'ROLLBACK_FAILED',
                                    'CANCELLED', 'REJECTED', 'NEEDS_ATTENTION'
                                )
                            ),
    created_at              DATETIME2           NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at              DATETIME2           NOT NULL DEFAULT SYSUTCDATETIME(),
    started_at              DATETIME2           NULL,
    completed_at            DATETIME2           NULL,
    worker_id               NVARCHAR(255)       NULL,
    pod_id                  NVARCHAR(255)       NULL,
    last_heartbeat_at       DATETIME2           NULL,
    attempt_count           INT                 NOT NULL DEFAULT 0,
    last_error              NVARCHAR(MAX)       NULL,
    needs_attention_reason  NVARCHAR(MAX)       NULL,

    CONSTRAINT PK_requests PRIMARY KEY (request_id)
);

-- Idempotency lookup: retrieve a previous request by its computed key.
CREATE UNIQUE INDEX UX_requests_idempotency_key
    ON requests (idempotency_key);

-- ---------------------------------------------------------------------------
-- Concurrency guard (T-1.2)
-- Prevents two active workflows on the same WideIP FQDN.
-- The WHERE clause must match ACTIVE_STATES in app/domain/states.py exactly.
-- ---------------------------------------------------------------------------
CREATE UNIQUE INDEX UX_requests_active_wip
    ON requests (wip_fqdn)
    WHERE status IN ('RECEIVED', 'VALIDATING', 'QUEUED', 'RUNNING', 'VERIFYING');

-- Supporting indexes for common queries.
CREATE INDEX IX_requests_status         ON requests (status);
CREATE INDEX IX_requests_wip_fqdn       ON requests (wip_fqdn);
CREATE INDEX IX_requests_target_device  ON requests (target_device);
CREATE INDEX IX_requests_heartbeat      ON requests (last_heartbeat_at) WHERE status = 'RUNNING';

-- ---------------------------------------------------------------------------
-- request_steps
-- One row per atomic step within a workflow.
-- ---------------------------------------------------------------------------
CREATE TABLE request_steps (
    step_id             UNIQUEIDENTIFIER    NOT NULL DEFAULT NEWID(),
    request_id          UNIQUEIDENTIFIER    NOT NULL,
    step_name           NVARCHAR(100)       NOT NULL,
    step_order          INT                 NOT NULL,
    target_system       NVARCHAR(50)        NOT NULL,   -- f5 | infoblox
    object_type         NVARCHAR(50)        NOT NULL,   -- monitor | pool | pool_member | wideip | cname
    object_key          NVARCHAR(512)       NOT NULL,   -- natural key on target system
    intent_json         NVARCHAR(MAX)       NOT NULL,
    pre_state_json      NVARCHAR(MAX)       NULL,
    result_json         NVARCHAR(MAX)       NULL,
    status              NVARCHAR(50)        NOT NULL DEFAULT 'PENDING'
                        CONSTRAINT CK_request_steps_status CHECK (
                            status IN ('PENDING', 'RUNNING', 'SUCCEEDED', 'FAILED', 'SKIPPED')
                        ),
    attempts            INT                 NOT NULL DEFAULT 0,
    error               NVARCHAR(MAX)       NULL,
    started_at          DATETIME2           NULL,
    completed_at        DATETIME2           NULL,
    compensation_status NVARCHAR(50)        NULL
                        CONSTRAINT CK_request_steps_compensation_status CHECK (
                            compensation_status IS NULL OR
                            compensation_status IN ('PENDING', 'SUCCEEDED', 'FAILED')
                        ),

    CONSTRAINT PK_request_steps PRIMARY KEY (step_id),
    CONSTRAINT FK_request_steps_request
        FOREIGN KEY (request_id) REFERENCES requests (request_id)
);

CREATE INDEX IX_request_steps_request_id
    ON request_steps (request_id, step_order);

-- ---------------------------------------------------------------------------
-- managed_objects
-- Long-lived record of every GSLB object under management.
-- Used by the reconciler to detect drift.
-- ---------------------------------------------------------------------------
CREATE TABLE managed_objects (
    object_id           UNIQUEIDENTIFIER    NOT NULL DEFAULT NEWID(),
    wip_fqdn            NVARCHAR(255)       NOT NULL,
    object_type         NVARCHAR(50)        NOT NULL,
    object_key          NVARCHAR(512)       NOT NULL,
    target_device       NVARCHAR(255)       NOT NULL,
    desired_state_json  NVARCHAR(MAX)       NOT NULL,
    last_verified_at    DATETIME2           NULL,
    drift_detected_at   DATETIME2           NULL,
    drift_details_json  NVARCHAR(MAX)       NULL,
    owning_request_id   UNIQUEIDENTIFIER    NULL,
    status              NVARCHAR(50)        NOT NULL DEFAULT 'ACTIVE'
                        CONSTRAINT CK_managed_objects_status CHECK (
                            status IN ('ACTIVE', 'PENDING_DELETE', 'DELETED')
                        ),

    CONSTRAINT PK_managed_objects PRIMARY KEY (object_id),
    CONSTRAINT FK_managed_objects_request
        FOREIGN KEY (owning_request_id) REFERENCES requests (request_id)
);

-- Natural-key uniqueness per target device (a given object exists once per device).
CREATE UNIQUE INDEX UX_managed_objects_key
    ON managed_objects (object_type, object_key, target_device);

CREATE INDEX IX_managed_objects_wip_fqdn
    ON managed_objects (wip_fqdn);

CREATE INDEX IX_managed_objects_last_verified
    ON managed_objects (last_verified_at);

-- ---------------------------------------------------------------------------
-- state_transitions  (append-only audit log)
-- Every status change is recorded here.  No updates, no deletes.
-- ---------------------------------------------------------------------------
CREATE TABLE state_transitions (
    -- Surrogate PK for pagination; not exposed externally.
    transition_id   BIGINT              NOT NULL IDENTITY(1, 1),
    request_id      UNIQUEIDENTIFIER    NOT NULL,
    from_status     NVARCHAR(50)        NOT NULL,
    to_status       NVARCHAR(50)        NOT NULL,
    reason          NVARCHAR(MAX)       NOT NULL DEFAULT '',
    actor           NVARCHAR(255)       NOT NULL DEFAULT '',
    timestamp       DATETIME2           NOT NULL DEFAULT SYSUTCDATETIME(),

    CONSTRAINT PK_state_transitions PRIMARY KEY (transition_id),
    CONSTRAINT FK_state_transitions_request
        FOREIGN KEY (request_id) REFERENCES requests (request_id)
);

CREATE INDEX IX_state_transitions_request_id
    ON state_transitions (request_id, timestamp);

-- ---------------------------------------------------------------------------
-- remediation_queue
-- Failed steps awaiting automated retry with exponential backoff.
-- ---------------------------------------------------------------------------
CREATE TABLE remediation_queue (
    remediation_id      UNIQUEIDENTIFIER    NOT NULL DEFAULT NEWID(),
    request_id          UNIQUEIDENTIFIER    NOT NULL,
    step_id             UNIQUEIDENTIFIER    NOT NULL,
    failure_category    NVARCHAR(100)       NOT NULL,
    retry_count         INT                 NOT NULL DEFAULT 0,
    next_retry_at       DATETIME2           NULL,
    escalated_at        DATETIME2           NULL,
    resolution          NVARCHAR(MAX)       NULL,

    CONSTRAINT PK_remediation_queue PRIMARY KEY (remediation_id),
    CONSTRAINT FK_remediation_queue_request
        FOREIGN KEY (request_id) REFERENCES requests (request_id),
    CONSTRAINT FK_remediation_queue_step
        FOREIGN KEY (step_id) REFERENCES request_steps (step_id)
);

CREATE INDEX IX_remediation_queue_next_retry
    ON remediation_queue (next_retry_at)
    WHERE escalated_at IS NULL AND resolution IS NULL;

CREATE INDEX IX_remediation_queue_request_id
    ON remediation_queue (request_id);


-- =============================================================================
-- ROLLBACK SCRIPT
-- Run in reverse to tear down this migration cleanly.
-- =============================================================================
/*

-- Drop indexes before tables to avoid implicit drops confusing review tooling.
DROP INDEX IF EXISTS IX_remediation_queue_request_id    ON remediation_queue;
DROP INDEX IF EXISTS IX_remediation_queue_next_retry    ON remediation_queue;

DROP INDEX IF EXISTS IX_state_transitions_request_id    ON state_transitions;

DROP INDEX IF EXISTS IX_managed_objects_last_verified   ON managed_objects;
DROP INDEX IF EXISTS IX_managed_objects_wip_fqdn        ON managed_objects;
DROP INDEX IF EXISTS UX_managed_objects_key             ON managed_objects;

DROP INDEX IF EXISTS IX_request_steps_request_id        ON request_steps;

DROP INDEX IF EXISTS IX_requests_heartbeat              ON requests;
DROP INDEX IF EXISTS IX_requests_target_device          ON requests;
DROP INDEX IF EXISTS IX_requests_wip_fqdn               ON requests;
DROP INDEX IF EXISTS IX_requests_status                 ON requests;
DROP INDEX IF EXISTS UX_requests_active_wip             ON requests;
DROP INDEX IF EXISTS UX_requests_idempotency_key        ON requests;

DROP TABLE IF EXISTS remediation_queue;
DROP TABLE IF EXISTS state_transitions;
DROP TABLE IF EXISTS managed_objects;
DROP TABLE IF EXISTS request_steps;
DROP TABLE IF EXISTS requests;

*/
