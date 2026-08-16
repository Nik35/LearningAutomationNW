"""
coordination — all Redis-backed concurrency primitives for GTM automation.

Every Redis operation in this package is atomic via Lua script.
No Python-level read-modify-write is permitted.

Public surface
--------------
DeviceSemaphore   per-device concurrency slots (semaphore.py)
DeviceTokenBucket per-device rate limiting (ratelimit.py)
DeviceCircuitBreaker per-device circuit breaker (breaker.py)
BreakerState      enum for breaker states (breaker.py)

Exception hierarchy
-------------------
RedisUnavailableError  raised when Redis is unreachable → caller returns 503
RedisOOMError          raised when Redis is out of memory → caller returns 503
"""

from app.coordination.breaker import BreakerState, DeviceCircuitBreaker
from app.coordination.ratelimit import DeviceTokenBucket
from app.coordination.semaphore import DeviceSemaphore
from app.coordination.exceptions import RedisOOMError, RedisUnavailableError

__all__ = [
    "DeviceSemaphore",
    "DeviceTokenBucket",
    "DeviceCircuitBreaker",
    "BreakerState",
    "RedisUnavailableError",
    "RedisOOMError",
]
