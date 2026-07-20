import pytest
import requests
from singer_sdk.exceptions import RetriableAPIError

from tap_dynamics_bc import client
from tap_dynamics_bc.client import dynamicsBcStream


def response(status_code):
    result = requests.Response()
    result.status_code = status_code
    result.reason = "Too Many Requests"
    result._content = b'{"value": [{"name": "production"}]}'
    return result


def test_rate_limit_is_retriable():
    stream = object.__new__(dynamicsBcStream)
    stream.path = "/companies"

    with pytest.raises(RetriableAPIError):
        stream.validate_response(response(429))


def test_environment_lookup_uses_the_sdk_retry_decorator(monkeypatch):
    stream = object.__new__(dynamicsBcStream)
    stream.envs_list = None
    stream.path = "/companies"
    decorated = []
    monkeypatch.setattr(dynamicsBcStream, "authenticator", property(lambda _: None))
    monkeypatch.setattr(client.requests, "get", lambda **_: response(200))
    stream.request_decorator = lambda request: decorated.append(request) or request

    assert stream.get_environments_list() == {"value": [{"name": "production"}]}
    assert len(decorated) == 1
