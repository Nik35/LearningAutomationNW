# app/core — configuration, logging, and metrics.
#
# Importing this package does NOT configure anything automatically.
# Callers should import the specific sub-modules they need:
#
#   from app.core.logging import get_logger, bind_request_id
#   from app.core.metrics import request_total, workflow_duration_seconds
#
# app/core/config.py is built by the db/domain agent and is intentionally
# not imported here to avoid circular-import issues during startup.
