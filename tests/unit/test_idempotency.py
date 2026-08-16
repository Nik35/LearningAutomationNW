"""
Unit tests for app.api.idempotency.

Coverage:
  - Key ordering: payloads with same keys in different order → same key
  - Casing: FQDNs and string values in different cases → same key
  - Whitespace: leading/trailing spaces in values → same key
  - Action normalisation: action casing / whitespace → same key
  - Different payloads → different keys
  - Recursive normalisation (nested dicts, lists)
  - Return type and length
"""

from __future__ import annotations

import pytest

from app.api.idempotency import compute_idempotency_key, _normalise_value


# ---------------------------------------------------------------------------
# _normalise_value (internal, but worth testing directly)
# ---------------------------------------------------------------------------


class TestNormaliseValue:
    def test_string_lowercased(self) -> None:
        assert _normalise_value("HELLO") == "hello"

    def test_string_stripped(self) -> None:
        assert _normalise_value("  hello  ") == "hello"

    def test_string_stripped_and_lowercased(self) -> None:
        assert _normalise_value("  HELLO  ") == "hello"

    def test_int_unchanged(self) -> None:
        assert _normalise_value(42) == 42

    def test_float_unchanged(self) -> None:
        assert _normalise_value(3.14) == 3.14

    def test_bool_unchanged(self) -> None:
        assert _normalise_value(True) is True
        assert _normalise_value(False) is False

    def test_none_unchanged(self) -> None:
        assert _normalise_value(None) is None

    def test_dict_keys_sorted(self) -> None:
        result = _normalise_value({"b": "B", "a": "A"})
        keys = list(result.keys())
        assert keys == ["a", "b"]

    def test_dict_values_normalised(self) -> None:
        result = _normalise_value({"key": "  VALUE  "})
        assert result == {"key": "value"}

    def test_nested_dict(self) -> None:
        result = _normalise_value({"outer": {"inner": "  HELLO  "}})
        assert result == {"outer": {"inner": "hello"}}

    def test_list_elements_normalised(self) -> None:
        result = _normalise_value(["  A  ", "  B  "])
        assert result == ["a", "b"]

    def test_list_order_preserved(self) -> None:
        """List element order is semantically meaningful; must not sort."""
        result = _normalise_value(["z", "a", "m"])
        assert result == ["z", "a", "m"]

    def test_dict_in_list(self) -> None:
        result = _normalise_value([{"B": "VAL", "A": "VAL2"}])
        assert result == [{"a": "val2", "b": "val"}]


# ---------------------------------------------------------------------------
# compute_idempotency_key — return type
# ---------------------------------------------------------------------------


class TestReturnType:
    def test_returns_str(self) -> None:
        key = compute_idempotency_key("create", "www.example.com", {})
        assert isinstance(key, str)

    def test_returns_64_hex_chars(self) -> None:
        key = compute_idempotency_key("create", "www.example.com", {})
        assert len(key) == 64
        assert all(c in "0123456789abcdef" for c in key)


# ---------------------------------------------------------------------------
# Key ordering
# ---------------------------------------------------------------------------


class TestKeyOrdering:
    def test_dict_key_order_independent(self) -> None:
        payload_a = {"pool": "pool1", "monitor": "mon1", "members": ["10.0.0.1"]}
        payload_b = {"members": ["10.0.0.1"], "pool": "pool1", "monitor": "mon1"}
        key_a = compute_idempotency_key("create", "wip.example.com", payload_a)
        key_b = compute_idempotency_key("create", "wip.example.com", payload_b)
        assert key_a == key_b

    def test_nested_dict_key_order_independent(self) -> None:
        payload_a = {"config": {"b": 2, "a": 1}}
        payload_b = {"config": {"a": 1, "b": 2}}
        assert (
            compute_idempotency_key("create", "wip.example.com", payload_a)
            == compute_idempotency_key("create", "wip.example.com", payload_b)
        )

    def test_three_orderings_produce_same_key(self) -> None:
        base = {"x": "1", "y": "2", "z": "3"}
        orderings = [
            {"x": "1", "y": "2", "z": "3"},
            {"z": "3", "x": "1", "y": "2"},
            {"y": "2", "z": "3", "x": "1"},
        ]
        keys = [compute_idempotency_key("create", "wip.example.com", o) for o in orderings]
        assert len(set(keys)) == 1


# ---------------------------------------------------------------------------
# Casing
# ---------------------------------------------------------------------------


class TestCasing:
    def test_fqdn_case_independent(self) -> None:
        key_lower = compute_idempotency_key("create", "wip.example.com", {"k": "v"})
        key_upper = compute_idempotency_key("create", "WIP.EXAMPLE.COM", {"k": "v"})
        key_mixed = compute_idempotency_key("create", "Wip.Example.Com", {"k": "v"})
        assert key_lower == key_upper == key_mixed

    def test_action_case_independent(self) -> None:
        key_lower = compute_idempotency_key("create", "wip.example.com", {"k": "v"})
        key_upper = compute_idempotency_key("CREATE", "wip.example.com", {"k": "v"})
        assert key_lower == key_upper

    def test_string_value_case_independent(self) -> None:
        key_a = compute_idempotency_key("create", "wip.example.com", {"pool": "MyPool"})
        key_b = compute_idempotency_key("create", "wip.example.com", {"pool": "mypool"})
        key_c = compute_idempotency_key("create", "wip.example.com", {"pool": "MYPOOL"})
        assert key_a == key_b == key_c

    def test_nested_value_case_independent(self) -> None:
        key_a = compute_idempotency_key(
            "create", "wip.example.com", {"config": {"name": "MONITOR"}}
        )
        key_b = compute_idempotency_key(
            "create", "wip.example.com", {"config": {"name": "monitor"}}
        )
        assert key_a == key_b


# ---------------------------------------------------------------------------
# Whitespace
# ---------------------------------------------------------------------------


class TestWhitespace:
    def test_leading_trailing_whitespace_in_value(self) -> None:
        key_a = compute_idempotency_key("create", "wip.example.com", {"name": "pool1"})
        key_b = compute_idempotency_key("create", "wip.example.com", {"name": "  pool1  "})
        assert key_a == key_b

    def test_leading_trailing_whitespace_in_fqdn(self) -> None:
        key_a = compute_idempotency_key("create", "wip.example.com", {})
        key_b = compute_idempotency_key("create", "  wip.example.com  ", {})
        assert key_a == key_b

    def test_leading_trailing_whitespace_in_action(self) -> None:
        key_a = compute_idempotency_key("create", "wip.example.com", {})
        key_b = compute_idempotency_key("  create  ", "wip.example.com", {})
        assert key_a == key_b

    def test_combined_whitespace_and_casing(self) -> None:
        key_a = compute_idempotency_key("create", "wip.example.com", {"pool": "MyPool"})
        key_b = compute_idempotency_key(
            "  CREATE  ", "  WIP.EXAMPLE.COM  ", {"pool": "  MYPOOL  "}
        )
        assert key_a == key_b


# ---------------------------------------------------------------------------
# Different payloads produce different keys
# ---------------------------------------------------------------------------


class TestDifferentPayloads:
    def test_different_action_different_key(self) -> None:
        key_create = compute_idempotency_key("create", "wip.example.com", {})
        key_delete = compute_idempotency_key("delete", "wip.example.com", {})
        assert key_create != key_delete

    def test_different_fqdn_different_key(self) -> None:
        key_a = compute_idempotency_key("create", "wip-a.example.com", {})
        key_b = compute_idempotency_key("create", "wip-b.example.com", {})
        assert key_a != key_b

    def test_different_payload_values_different_key(self) -> None:
        key_a = compute_idempotency_key("create", "wip.example.com", {"pool": "pool1"})
        key_b = compute_idempotency_key("create", "wip.example.com", {"pool": "pool2"})
        assert key_a != key_b

    def test_extra_payload_key_different_key(self) -> None:
        key_a = compute_idempotency_key("create", "wip.example.com", {"pool": "pool1"})
        key_b = compute_idempotency_key(
            "create", "wip.example.com", {"pool": "pool1", "monitor": "mon1"}
        )
        assert key_a != key_b

    def test_empty_payload_vs_nonempty(self) -> None:
        key_a = compute_idempotency_key("create", "wip.example.com", {})
        key_b = compute_idempotency_key("create", "wip.example.com", {"k": "v"})
        assert key_a != key_b

    def test_list_order_matters(self) -> None:
        """List ordering is semantically meaningful — different order → different key."""
        key_a = compute_idempotency_key(
            "create", "wip.example.com", {"members": ["10.0.0.1", "10.0.0.2"]}
        )
        key_b = compute_idempotency_key(
            "create", "wip.example.com", {"members": ["10.0.0.2", "10.0.0.1"]}
        )
        assert key_a != key_b


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------


class TestDeterminism:
    def test_same_input_same_key_repeated_calls(self) -> None:
        payload = {"pool": "pool1", "monitor": "mon1"}
        keys = [compute_idempotency_key("create", "wip.example.com", payload) for _ in range(10)]
        assert len(set(keys)) == 1

    def test_known_hash_value(self) -> None:
        """
        Pin one known hash so a future refactor of the normalisation logic
        cannot silently break existing keys stored in the database.

        If this test fails after an intentional algorithm change, update
        all stored idempotency_key values in MSSQL before deploying.
        """
        import hashlib
        import json

        # Manually compute what the function should produce.
        canonical = {
            "action": "create",
            "payload": {"monitor": "mon1", "pool": "pool1"},
            "wip_fqdn": "wip.example.com",
        }
        expected = hashlib.sha256(
            json.dumps(canonical, separators=(",", ":"), sort_keys=True).encode("utf-8")
        ).hexdigest()

        actual = compute_idempotency_key(
            "create", "wip.example.com", {"pool": "pool1", "monitor": "mon1"}
        )
        assert actual == expected
