"""The Prime backend's offline parts: labels as strings, the build context as Prime's builder reads it, and which refusals
the router treats as "Prime is full"."""
import tarfile

import pytest

from sandboxes.backends import prime_raw
from sandboxes.errors import CapacityError
from sandboxes.pricing import cost_model_for


def test_labels_travel_as_key_value_strings_and_come_back():
    labels = {"sandboxes": "1", "sandboxes.managed": "true", "sandboxes.run": "r-1=x"}
    encoded = prime_raw.encode_labels(labels)
    assert "sandboxes.managed=true" in encoded and prime_raw.MANAGED_LABEL in encoded
    assert prime_raw.decode_labels(encoded) == labels          # a "=" inside a value survives
    assert prime_raw.decode_labels(["bare"]) == {"bare": ""}


def test_build_context_carries_the_dockerfile_where_the_builder_reads_it(tmp_path):
    (tmp_path / "ctx").mkdir()
    (tmp_path / "ctx" / "app.py").write_text("print(1)\n")
    dockerfile = tmp_path / "Dockerfile.alt"
    dockerfile.write_text("FROM python:3.11-slim\nCOPY app.py /app.py\n")
    path = prime_raw.pack_context(str(tmp_path / "ctx"), dockerfile)
    with tarfile.open(path) as tar:
        names = tar.getnames()
        assert tar.extractfile(prime_raw.PACKAGED_DOCKERFILE).read() == dockerfile.read_bytes()
    assert "./app.py" in names


def test_funds_and_rate_limits_are_capacity_errors_other_failures_are_not(monkeypatch):
    sdk = pytest.importorskip("prime_sandboxes")
    monkeypatch.delenv("PRIME_TEAM_ID", raising=False)
    funds = prime_raw.to_capacity_error(sdk.PaymentRequiredError("Payment required. Check billing status."))
    assert isinstance(funds, CapacityError) and "personal wallet" in str(funds) and "PRIME_TEAM_ID" in str(funds)
    rate = prime_raw.to_capacity_error(sdk.APIError("HTTP 429: Too Many Requests"))
    assert isinstance(rate, CapacityError) and "rate limit" in str(rate)        # raw.start() backs off and retries these
    assert prime_raw.to_capacity_error(sdk.APIError("HTTP 500: internal error")) is None


def test_cost_model_matches_the_published_example():
    # the overview's example: 1 vCPU, 2 GiB RAM, 5 GiB disk = $0.046/hour
    assert abs(cost_model_for("prime").hourly_usd(1, 2, 5) - 0.046) < 0.0005
