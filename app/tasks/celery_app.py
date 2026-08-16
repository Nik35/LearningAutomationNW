"""
Celery application factory.

Key configuration decisions (from implementation plan):
- task_ignore_result = True  → status lives in MSSQL, not Redis result backend
- task_acks_late = True      → task is only ACKed after the worker finishes
                               (so it re-queues on a hard worker crash before ACK)
- worker_prefetch_multiplier = 1 → each worker fetches one task at a time;
                               prefetch > 1 would let a worker hold tasks it can't
                               start due to semaphore pressure, starving other workers
"""
from celery import Celery

from app.core.config import settings


def make_celery() -> Celery:
    app = Celery("f5_gtm_automation")

    app.conf.update(
        broker_url=settings.REDIS_URL,
        # No result backend — status is in MSSQL (D-2)
        task_ignore_result=True,
        # Late ACK: if the worker dies before finishing, the task is requeued
        task_acks_late=True,
        # One task at a time per worker — important for semaphore semantics
        worker_prefetch_multiplier=1,
        # Serialisation
        task_serializer="json",
        accept_content=["json"],
        result_serializer="json",
        # Timezone
        timezone="UTC",
        enable_utc=True,
        # Beat schedule is defined in beat.py
        beat_schedule_filename="/tmp/celerybeat-schedule",
        # Routes — all workflow tasks go to the gtm queue
        task_routes={
            "app.tasks.workflows.*": {"queue": "gtm"},
            "app.tasks.beat.*": {"queue": "beat"},
        },
    )

    return app


celery_app = make_celery()
