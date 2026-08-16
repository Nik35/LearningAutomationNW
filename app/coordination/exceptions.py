"""
Custom exception hierarchy for Redis coordination failures.

These are the two error conditions that every coordination primitive must
propagate cleanly so the admission layer can return the correct HTTP response.

    RedisUnavailableError  → 503 + Retry-After  (fail-closed per D-4)
    RedisOOMError          → 503 + Retry-After  (noeviction OOM per §2.1)

Neither should ever be swallowed silently in a caller.
"""

from __future__ import annotations


class RedisUnavailableError(Exception):
    """
    Raised when Redis cannot be reached (ConnectionError, TimeoutError, etc.).

    Triggers the fail-closed path: reject new work with 503 + Retry-After.
    In-flight work that already holds a semaphore slot is allowed to finish.

    Architecture note (D-4): never proceed without limits when Redis is down.
    """


class RedisOOMError(Exception):
    """
    Raised when Redis returns an OOM error on a write operation.

    This happens when ``maxmemory-policy`` is ``noeviction`` (mandatory per D-3)
    and Redis memory is exhausted.  The correct response is 503 + Retry-After,
    NOT a 500.  See §2.1 of the implementation plan.
    """
