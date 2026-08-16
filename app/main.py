"""
FastAPI application entry point.

Wires together all components at startup. Each dependency is configured
from settings — no hardcoded values.
"""
from __future__ import annotations

import structlog
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import make_asgi_app

from app.api.routes import router as wideip_router
from app.api.notifications import router as notifications_router
from app.core.config import settings
from app.core.logging import get_logger

log = get_logger(__name__)


def create_app() -> FastAPI:
    app = FastAPI(
        title="F5 GTM Automation API",
        version="0.1.0",
        docs_url="/docs" if settings.APP_ENV != "production" else None,
        redoc_url=None,
    )

    # ── Startup / shutdown ────────────────────────────────────────────────
    @app.on_event("startup")
    async def startup() -> None:
        import redis.asyncio as aioredis
        import pyodbc
        from app.ops.controls import OperationalControls

        # Redis client — shared across all requests
        redis_client = aioredis.from_url(
            settings.REDIS_URL,
            encoding="utf-8",
            decode_responses=True,
        )
        # Verify Redis reachable at startup; if not, the app still starts
        # (fail-closed is per-request, not startup-time)
        try:
            await redis_client.ping()
            log.info("startup.redis_connected")
        except Exception as exc:
            log.warning("startup.redis_unavailable", error=str(exc))

        app.state.redis = redis_client
        app.state.controls = OperationalControls(
            redis_client=redis_client,
            delete_cap=settings.P9_MAX_DELETES_PER_WINDOW,  # TODO: awaiting business decision
        )

        # DB connection — pyodbc, one connection per request via dependency
        # (connection pooling managed by the ODBC driver / SQL Server)
        app.state.db_connection_string = settings.DB_CONNECTION_STRING
        log.info("startup.complete", env=settings.APP_ENV)

    @app.on_event("shutdown")
    async def shutdown() -> None:
        await app.state.redis.aclose()
        log.info("shutdown.complete")

    # ── Routes ────────────────────────────────────────────────────────────
    app.include_router(wideip_router, prefix="/api/v1")
    app.include_router(notifications_router, prefix="/api/v1")

    # Prometheus metrics endpoint
    metrics_app = make_asgi_app()
    app.mount("/metrics", metrics_app)

    # ── Ops status endpoint ───────────────────────────────────────────────
    from app.ops.status import build_status_snapshot

    @app.get("/ops/status")
    async def ops_status() -> dict:
        return await build_status_snapshot(
            redis_client=app.state.redis,
            controls=app.state.controls,
            device_ids=settings.KNOWN_DEVICE_IDS,
        )

    @app.get("/healthz")
    async def healthz() -> dict:
        return {"status": "ok"}

    return app


app = create_app()
