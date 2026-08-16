"""
Application configuration — all runtime-tunable parameters.

P-n parameters
--------------
Every load-governing number (concurrency limits, token bucket sizes,
circuit-breaker thresholds, timeouts, queue depth limits) is listed here
with a clearly wrong placeholder value and a ``# TODO: awaiting T-0.x``
comment.

**Do not replace placeholders until WP-0 measurements are available.**
A plausible-looking but wrong value silently overloads production F5
devices.  See CLAUDE.md § "Never invent a P-n parameter value".

Non-P-n settings
----------------
Connection strings, environment names, and log level are also here.
They have safe defaults that work in a local dev environment but must
be overridden for production via environment variables (or a .env file).
"""

from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    All application configuration, loaded from environment variables.

    Fields are grouped:
      1. Infrastructure connection strings
      2. Application behaviour
      3. P-n parameters (all placeholders — see WP-0)
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        # Allow extra env vars so container deployments with extra vars don't error.
        extra="ignore",
    )

    # -------------------------------------------------------------------------
    # Infrastructure
    # -------------------------------------------------------------------------

    DB_CONNECTION_STRING: str = (
        "Driver={ODBC Driver 18 for SQL Server};"
        "Server=localhost;Database=gtm_automation;Trusted_Connection=yes;"
    )

    REDIS_URL: str = "redis://localhost:6379/0"

    # -------------------------------------------------------------------------
    # Application behaviour
    # -------------------------------------------------------------------------

    APP_ENV: str = "development"  # development | staging | production
    LOG_LEVEL: str = "INFO"       # DEBUG | INFO | WARNING | ERROR | CRITICAL

    # -------------------------------------------------------------------------
    # P-1 — Per-device concurrency limit
    # Source: T-0.6, T-0.7  (measured throughput/latency curve + device saturation)
    # Must satisfy D-7: high enough that the queue can drain at the arrival rate.
    # TODO: awaiting T-0.6, T-0.7
    # -------------------------------------------------------------------------
    P1_PER_DEVICE_CONCURRENCY: int = -1

    # -------------------------------------------------------------------------
    # P-2 — Token bucket size (per device)
    # Source: T-0.7  (which metric saturates first and at what rate)
    # TODO: awaiting T-0.7
    # -------------------------------------------------------------------------
    P2_TOKEN_BUCKET_SIZE: int = -1

    # -------------------------------------------------------------------------
    # P-3 — Token refill rate (per device), tokens per second
    # Source: T-0.7
    # TODO: awaiting T-0.7
    # -------------------------------------------------------------------------
    P3_TOKEN_REFILL_RATE: float = 0.0

    # -------------------------------------------------------------------------
    # P-4 — Semaphore acquire timeout, seconds
    # Source: derived from P-1 and measured service time (T-0.1)
    # TODO: awaiting T-0.1, T-0.6, T-0.7
    # -------------------------------------------------------------------------
    P4_SEMAPHORE_ACQUIRE_TIMEOUT_SECONDS: int = -1

    # -------------------------------------------------------------------------
    # P-5 — Heartbeat interval, seconds
    # Suggested range: 15–30 s.  Choose after measuring step durations (T-0.1).
    # TODO: awaiting T-0.1
    # -------------------------------------------------------------------------
    P5_HEARTBEAT_INTERVAL_SECONDS: int = -1

    # -------------------------------------------------------------------------
    # P-6 — Stale heartbeat threshold, seconds
    # Definition: 3 × P-5.  A RUNNING row whose last_heartbeat_at is older
    # than this is eligible for reclaim.
    # TODO: awaiting T-0.1 (set P-5 first)
    # -------------------------------------------------------------------------
    P6_STALE_HEARTBEAT_THRESHOLD_SECONDS: int = -1

    # -------------------------------------------------------------------------
    # P-7 — Global queue depth limit (across all devices)
    # Source: derived from measured throughput and acceptable queuing time.
    # TODO: awaiting T-0.6
    # -------------------------------------------------------------------------
    P7_GLOBAL_QUEUE_DEPTH_LIMIT: int = -1

    # -------------------------------------------------------------------------
    # P-8 — Per-device queue depth limit
    # Source: derived from P-1 and measured service time.
    # TODO: awaiting T-0.1, T-0.6, T-0.7
    # -------------------------------------------------------------------------
    P8_PER_DEVICE_QUEUE_DEPTH_LIMIT: int = -1

    # -------------------------------------------------------------------------
    # P-9 — Max delete operations per rolling window (blast-radius cap)
    # Source: business decision (WP-7 / T-7.3)
    # TODO: awaiting owner decision
    # -------------------------------------------------------------------------
    P9_MAX_DELETES_PER_WINDOW: int = -1

    # -------------------------------------------------------------------------
    # P-10 — Circuit breaker thresholds (per device)
    # Three sub-parameters; all three must be tuned together.
    # Source: T-0.6, T-0.7

    # Error rate (0.0–1.0) above which the breaker opens.
    # TODO: awaiting T-0.6, T-0.7
    P10_BREAKER_ERROR_RATE: float = 0.0

    # p95 latency in milliseconds above which the breaker opens.
    # TODO: awaiting T-0.6, T-0.7
    P10_BREAKER_P95_LATENCY_MS: float = 0.0

    # Consecutive timeout count above which the breaker opens.
    # TODO: awaiting T-0.6, T-0.7
    P10_BREAKER_CONSECUTIVE_TIMEOUTS: int = -1
    # -------------------------------------------------------------------------

    # Aliases for backwards-compat with older field names used during scaffolding
    @property
    def P4_SEMAPHORE_ACQUIRE_TIMEOUT(self) -> int:  # noqa: N802
        return self.P4_SEMAPHORE_ACQUIRE_TIMEOUT_SECONDS

    @property
    def P6_STALE_HEARTBEAT_THRESHOLD(self) -> int:  # noqa: N802
        return self.P6_STALE_HEARTBEAT_THRESHOLD_SECONDS

    # -------------------------------------------------------------------------
    # Infrastructure — F5 devices
    # -------------------------------------------------------------------------

    # Comma-separated list of known device IDs, e.g. "dc-a-grid1,dc-a-grid2,dc-b-grid1,dc-b-grid2"
    KNOWN_DEVICE_IDS_CSV: str = ""

    # Per-device config JSON, e.g.:
    # '{"dc-a-grid1": {"host": "10.0.0.1", "username": "admin", "password": "..."}}'
    F5_DEVICE_CONFIG_JSON: str = "{}"

    F5_REQUEST_TIMEOUT_SECONDS: float = 30.0
    F5_VERIFY_SSL: bool = True

    # Auth provider name passed in the iControl REST login payload.
    # "tmos" = local auth. For TACACS+: set to the auth source name configured
    # on the BIG-IP (run: tmsh list auth tacacs — use the object name shown).
    # Confirmed: this deployment uses TACACS+. Set F5_LOGIN_PROVIDER_NAME to
    # the TACACS+ source name from the BIG-IP configuration.
    F5_LOGIN_PROVIDER_NAME: str = "tmos"   # REPLACE with TACACS+ source name

    # BIG-IP rSeries — confirmed platform. Latest tested version: 17.1.x
    # GTM endpoints: /mgmt/tm/gtm/* (unchanged from 15.x/16.x)
    F5_BIGIP_VERSION: str = "17.1"

    # -------------------------------------------------------------------------
    # Infrastructure — Infoblox
    # -------------------------------------------------------------------------

    INFOBLOX_HOST: str = ""
    INFOBLOX_USERNAME: str = ""
    INFOBLOX_PASSWORD: str = ""
    INFOBLOX_WAPI_VERSION: str = "2.12"
    INFOBLOX_VERIFY_SSL: bool = True
    INFOBLOX_REQUEST_TIMEOUT_SECONDS: float = 30.0

    # -------------------------------------------------------------------------
    # Runtime identity
    # -------------------------------------------------------------------------

    # Injected by OpenShift via downward API (metadata.name)
    POD_ID: str = "local-dev"

    # -------------------------------------------------------------------------
    # Helpers (not fields — not loaded from env)
    # -------------------------------------------------------------------------

    @property
    def KNOWN_DEVICE_IDS(self) -> list[str]:  # noqa: N802
        if not self.KNOWN_DEVICE_IDS_CSV:
            return []
        return [d.strip() for d in self.KNOWN_DEVICE_IDS_CSV.split(",") if d.strip()]

    def is_known_device(self, device_id: str) -> bool:
        return device_id in self.KNOWN_DEVICE_IDS

    def get_device_config(self, device_id: str) -> dict:
        import json
        cfg = json.loads(self.F5_DEVICE_CONFIG_JSON)
        if device_id not in cfg:
            raise ValueError(f"No config for device '{device_id}'")
        return cfg[device_id]


# Module-level singleton — import this in application code.
settings = Settings()
