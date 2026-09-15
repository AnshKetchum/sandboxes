"""Pure container-naming logic — no docker daemon needed, unlike
test_docker_provider.py's real-daemon integration tests."""

from __future__ import annotations

from sandboxes.docker_provider import _sanitize_name_prefix


def test_sanitize_name_prefix_passes_through_a_safe_prefix():
    assert _sanitize_name_prefix("r-20260914-231318-t-0004") == "r-20260914-231318-t-0004"


def test_sanitize_name_prefix_collapses_unsafe_characters():
    assert _sanitize_name_prefix("weird prefix/with:bad*chars") == "weird-prefix-with-bad-chars"


def test_sanitize_name_prefix_trims_leading_and_trailing_dashes():
    assert _sanitize_name_prefix("--already--dashed--") == "already--dashed"


def test_sanitize_name_prefix_falls_back_to_sbx_when_nothing_survives():
    assert _sanitize_name_prefix("///") == "sbx"


def test_sanitize_name_prefix_truncates_to_max_len():
    long = "a" * 500
    out = _sanitize_name_prefix(long, max_len=20)
    assert out == "a" * 20
