"""
app/clients/infoblox/__init__.py
================================
Public surface of the Infoblox WAPI client package.

Import from here in all other application code so that internal module
organisation can change without touching callers.
"""

from app.clients.infoblox.records import (
    ActionTaken,
    InfobloxClient,
    OperationResult,
)
from app.clients.infoblox.session import InfobloxSession
from app.clients.infoblox.records import (
    InfobloxConflictError,
    InfobloxError,
    InfobloxNotFoundError,
    InfobloxServerError,
    InfobloxTimeoutError,
)

__all__ = [
    # Session
    "InfobloxSession",
    # Client
    "InfobloxClient",
    # Result types
    "ActionTaken",
    "OperationResult",
    # Exceptions
    "InfobloxError",
    "InfobloxNotFoundError",
    "InfobloxConflictError",
    "InfobloxTimeoutError",
    "InfobloxServerError",
]
