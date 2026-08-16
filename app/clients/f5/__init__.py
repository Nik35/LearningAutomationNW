"""
app/clients/f5/__init__.py
==========================
Public surface of the F5 iControl REST client package.

Import from here in all other application code so that internal module
organisation can change without touching callers.
"""

from app.clients.f5.auth import F5TokenManager
from app.clients.f5.gtm import (
    ActionTaken,
    F5GTMClient,
    OperationResult,
)
from app.clients.f5.session import F5Session
from app.clients.f5.gtm import (
    F5ConflictError,
    F5Error,
    F5NotFoundError,
    F5ServerError,
    F5TimeoutError,
)

__all__ = [
    # Session
    "F5Session",
    # Auth
    "F5TokenManager",
    # GTM client
    "F5GTMClient",
    # Result types
    "ActionTaken",
    "OperationResult",
    # Exceptions
    "F5Error",
    "F5NotFoundError",
    "F5ConflictError",
    "F5TimeoutError",
    "F5ServerError",
]
