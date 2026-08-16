"""
Idempotency key computation — §3.1 step 4 of the implementation plan.

``compute_idempotency_key`` produces a deterministic sha256 hex digest
from the (action, wip_fqdn, payload) triple such that two requests that
are semantically identical but differ only in:

  - dict key ordering
  - FQDN / string casing
  - leading/trailing whitespace in string values

produce the **identical** key.  Structurally different payloads always
produce different keys.

The normalisation step is intentionally simple and explicit; no
third-party library is used.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any


# ---------------------------------------------------------------------------
# Normalisation
# ---------------------------------------------------------------------------


def _normalise_value(value: Any) -> Any:
    """
    Recursively normalise a Python value for deterministic hashing.

    Rules:
    - ``str``  → strip whitespace, lowercase
    - ``dict`` → recurse on values, then sort keys
    - ``list`` → recurse on each element (order preserved — list ordering
                 is semantically meaningful in payloads)
    - Everything else → unchanged (int, float, bool, None)
    """
    if isinstance(value, str):
        return value.strip().lower()
    if isinstance(value, dict):
        return {k: _normalise_value(v) for k, v in sorted(value.items())}
    if isinstance(value, list):
        return [_normalise_value(item) for item in value]
    return value


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def compute_idempotency_key(action: str, wip_fqdn: str, payload: dict[str, Any]) -> str:
    """
    Return a deterministic sha256 hex digest for the given request triple.

    Parameters
    ----------
    action:
        The operation type — ``"create"``, ``"update"``, or ``"delete"``.
        Normalised (stripped, lowercased) before hashing.
    wip_fqdn:
        The WideIP FQDN being acted on.  Normalised before hashing.
    payload:
        The full request payload dict.  Keys are sorted recursively and
        string values are stripped + lowercased before hashing.

    Returns
    -------
    str
        64-character lowercase hexadecimal sha256 digest.

    Notes
    -----
    The canonical form is:
        sha256(json.dumps({"action": <normalised>, "wip_fqdn": <normalised>,
                           "payload": <normalised>},
                          separators=(',', ':')))
    Using compact separators (no spaces) eliminates any whitespace ambiguity
    in the serialised form.
    """
    canonical: dict[str, Any] = {
        "action": _normalise_value(action),
        "wip_fqdn": _normalise_value(wip_fqdn),
        "payload": _normalise_value(payload),
    }
    # The outer dict keys ("action", "wip_fqdn", "payload") are already
    # in sorted alphabetical order.  json.dumps with sort_keys=True makes
    # this explicit in case a future refactor adds keys.
    serialised = json.dumps(canonical, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(serialised.encode("utf-8")).hexdigest()
