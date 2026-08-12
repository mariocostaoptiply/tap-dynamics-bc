"""Tests for Dynamics BC response handling."""

import json

import pytest
import requests
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError

from tap_dynamics_bc.client import dynamicsBcStream


def _response(status_code, payload):
    response = requests.Response()
    response.status_code = status_code
    response.reason = "Conflict"
    response.encoding = "utf-8"
    response._content = json.dumps(payload).encode("utf-8")
    return response


def _stream():
    stream = object.__new__(dynamicsBcStream)
    stream.path = "/companies({company_id})/salesOrders"
    return stream


def _deadlock_response():
    return _response(
        409,
        {
            "error": {
                "code": "Internal_ServerError",
                "message": (
                    "The activity was deadlocked with another user. "
                    "Please retry the activity."
                ),
            }
        },
    )


def test_deadlock_conflict_is_retriable():
    """Retry Business Central conflicts that explicitly report a deadlock."""
    with pytest.raises(RetriableAPIError):
        _stream().validate_response(_deadlock_response())


def test_generic_conflict_is_fatal():
    """Keep unrelated conflict responses fatal."""
    response = _response(
        409,
        {"error": {"code": "Conflict", "message": "The record has changed."}},
    )

    with pytest.raises(FatalAPIError):
        _stream().validate_response(response)


def test_deadlock_conflict_retries_then_succeeds():
    """Exercise the SDK retry decorator with a transient deadlock response."""
    stream = _stream()
    stream.backoff_wait_generator = lambda: iter([0])
    attempts = 0

    def request():
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            stream.validate_response(_deadlock_response())
        return "ok"

    assert stream.request_decorator(request)() == "ok"
    assert attempts == 2
