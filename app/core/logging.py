"""
Structured logging for the GTM automation service.

Uses structlog with:
- JSON output in production (LOG_FORMAT=json or LOG_ENV=production)
- Pretty-printing in development (default)
- request_id bound to every log line via structlog contextvars
- Log level driven by LOG_LEVEL env var (default INFO)

Usage
-----
    from app.core.logging import get_logger, bind_request_id

    bind_request_id("abc-123")          # call once per request context
    log = get_logger(__name__)
    log.info("doing_work", key="value")  # request_id appears automatically
"""

from __future__ import annotations

import logging
import os
import sys

import structlog
from structlog.contextvars import (
    bind_contextvars,
    clear_contextvars,
    merge_contextvars,
)

# ---------------------------------------------------------------------------
# Read configuration from environment
# ---------------------------------------------------------------------------

_LOG_LEVEL_NAME: str = os.environ.get("LOG_LEVEL", "INFO").upper()
_LOG_FORMAT: str = os.environ.get("LOG_FORMAT", "pretty").lower()
# Convenience alias: LOG_ENV=production implies JSON output
if os.environ.get("LOG_ENV", "").lower() == "production":
    _LOG_FORMAT = "json"

_LOG_LEVEL: int = getattr(logging, _LOG_LEVEL_NAME, logging.INFO)


# ---------------------------------------------------------------------------
# Configure structlog — called once at import time
# ---------------------------------------------------------------------------

def _configure_structlog() -> None:
    """
    Set up structlog processors and renderer.

    Call order matters: processors run left-to-right, renderer is last.
    contextvars merge must come before any timestamp/level processors so
    that bound context variables (including request_id) appear in output.
    """
    shared_processors: list[structlog.types.Processor] = [
        # Merge bound contextvars (e.g. request_id) into the event dict first.
        merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
    ]

    if _LOG_FORMAT == "json":
        renderer: structlog.types.Processor = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer(colors=sys.stderr.isatty())

    structlog.configure(
        processors=shared_processors
        + [
            structlog.processors.format_exc_info,
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(_LOG_LEVEL),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
        cache_logger_on_first_use=True,
    )

    # Also configure stdlib logging so that any library using logging.*
    # is captured and surfaced at the same level.
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stderr,
        level=_LOG_LEVEL,
    )


_configure_structlog()


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------


def bind_request_id(request_id: str) -> None:
    """
    Bind *request_id* to the current async context variable.

    Must be called once per request (e.g. from a FastAPI middleware or at
    the top of each Celery task).  All log calls in the same context will
    automatically include the bound value.

    Parameters
    ----------
    request_id:
        The UUID string for the current provisioning request.
    """
    bind_contextvars(request_id=request_id)


def clear_request_id() -> None:
    """
    Clear all context variables bound to the current async context.

    Call this in a ``finally`` block after a request completes to prevent
    context leakage between tasks sharing the same thread/coroutine pool.
    """
    clear_contextvars()


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """
    Return a structlog bound logger for the given module *name*.

    Parameters
    ----------
    name:
        Typically ``__name__`` of the calling module.

    Returns
    -------
    structlog.stdlib.BoundLogger
        A logger whose every call includes the module name and any
        context variables (e.g. ``request_id``) bound to the current
        async context.
    """
    return structlog.get_logger(name)
