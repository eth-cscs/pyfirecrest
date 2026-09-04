import json
import pytest

from context import firecrest
from werkzeug.wrappers import Response


def auth_handler(request):
    client_id = request.form["client_id"]
    client_secret = request.form["client_secret"]
    if client_id == "valid_id":
        if client_secret == "valid_secret":
            ret = {
                "access_token": "VALID_TOKEN",
                "expires_in": 15,
                "refresh_expires_in": 0,
                "token_type": "Bearer",
                "not-before-policy": 0,
                "scope": "profile firecrest email",
            }
            ret_status = 200
        elif client_secret == "valid_secret_2":
            ret = {
                "access_token": "token_2",
                "expires_in": 15,
                "refresh_expires_in": 0,
                "token_type": "Bearer",
                "not-before-policy": 0,
                "scope": "profile firecrest email",
            }
            ret_status = 200
        else:
            ret = {
                "error": "unauthorized_client",
                "error_description": "Invalid client secret",
            }
            ret_status = 400
    else:
        ret = {
            "error": "invalid_client",
            "error_description": "Invalid client credentials",
        }
        ret_status = 400

    return Response(json.dumps(ret), status=ret_status, content_type="application/json")


def auth_handler_basic(request):
    # client_secret_basic sends the credentials via HTTP Basic auth
    if request.authorization is None:
        ret = {
            "error": "invalid_client",
            "error_description": "Missing client credentials",
        }
        return Response(
            json.dumps(ret), status=400, content_type="application/json"
        )

    client_id = request.authorization.username
    client_secret = request.authorization.password
    if client_id == "valid_id" and client_secret == "valid_secret":
        ret = {
            "access_token": "VALID_TOKEN",
            "expires_in": 15,
            "refresh_expires_in": 0,
            "token_type": "Bearer",
            "not-before-policy": 0,
            "scope": "profile firecrest email",
        }
        ret_status = 200
    else:
        ret = {
            "error": "invalid_client",
            "error_description": "Invalid client credentials",
        }
        ret_status = 400

    return Response(json.dumps(ret), status=ret_status, content_type="application/json")


@pytest.fixture
def auth_server(httpserver):
    httpserver.expect_request("/auth/token").respond_with_handler(auth_handler)
    return httpserver


@pytest.fixture
def auth_server_basic(httpserver):
    httpserver.expect_request("/auth/token").respond_with_handler(auth_handler_basic)
    return httpserver


def test_client_credentials_valid(auth_server):
    auth_obj = firecrest.ClientCredentialsAuth(
        "valid_id", "valid_secret", auth_server.url_for("/auth/token")
    )
    assert auth_obj._min_token_validity == 10
    assert auth_obj.get_access_token() == "VALID_TOKEN"
    # Change the secret differentiate between first and second request
    auth_obj._client_secret = "valid_secret_2"
    assert auth_obj.get_access_token() == "VALID_TOKEN"

    auth_obj = firecrest.ClientCredentialsAuth(
        "valid_id",
        "valid_secret",
        auth_server.url_for("/auth/token"),
        min_token_validity=20,
    )
    assert auth_obj.get_access_token() == "VALID_TOKEN"
    # Change the secret differentiate between first and second request
    auth_obj._client_secret = "valid_secret_2"
    assert auth_obj.get_access_token() == "token_2"


def test_client_credentials_basic_valid(auth_server_basic):
    auth_obj = firecrest.ClientCredentialsAuth(
        "valid_id",
        "valid_secret",
        auth_server_basic.url_for("/auth/token"),
        client_auth_method="client_secret_basic",
    )
    assert auth_obj.get_access_token() == "VALID_TOKEN"


def test_client_credentials_basic_invalid_secret(auth_server_basic):
    auth_obj = firecrest.ClientCredentialsAuth(
        "valid_id",
        "invalid_secret",
        auth_server_basic.url_for("/auth/token"),
        client_auth_method="client_secret_basic",
    )
    with pytest.raises(Exception) as exc_info:
        auth_obj.get_access_token()

    assert "Client credentials error" in str(exc_info.value)


def test_client_credentials_invalid_auth_method():
    with pytest.raises(ValueError):
        firecrest.ClientCredentialsAuth(
            "valid_id",
            "valid_secret",
            "https://auth.example.com/auth/token",
            client_auth_method="invalid_method",
        )


def test_client_credentials_invalid_id(auth_server):
    auth_obj = firecrest.ClientCredentialsAuth(
        "invalid_id", "valid_secret", auth_server.url_for("/auth/token")
    )
    with pytest.raises(Exception) as exc_info:
        auth_obj.get_access_token()

    assert "Client credentials error" in str(exc_info.value)


def test_client_credentials_invalid_secret(auth_server):
    auth_obj = firecrest.ClientCredentialsAuth(
        "valid_id", "invalid_secret", auth_server.url_for("/auth/token")
    )
    with pytest.raises(Exception) as exc_info:
        auth_obj.get_access_token()

    assert "Client credentials error" in str(exc_info.value)


def test_api_key_auth_headers():
    auth = firecrest.ApiKeyAuth("my-key")
    assert auth.auth_headers() == {"X-API-Key": "my-key"}
    assert not hasattr(auth, "get_access_token")


def test_api_key_auth_custom_header():
    auth = firecrest.ApiKeyAuth("my-key", header_name="Api-Key")
    assert auth.auth_headers() == {"Api-Key": "my-key"}


def test_api_key_auth_invalid_args():
    with pytest.raises(ValueError):
        firecrest.ApiKeyAuth("")

    with pytest.raises(ValueError):
        firecrest.ApiKeyAuth("my-key", header_name="")


def test_api_key_auth_repr_hides_key():
    auth = firecrest.ApiKeyAuth("super-secret")
    assert "super-secret" not in repr(auth)


def test_client_credentials_auth_headers(auth_server):
    auth = firecrest.ClientCredentialsAuth(
        "valid_id", "valid_secret", auth_server.url_for("/auth/token")
    )
    assert auth.auth_headers() == {"Authorization": "Bearer VALID_TOKEN"}


def test_token_command_auth_headers():
    auth = firecrest.TokenCommandAuth("echo my-token")
    assert auth.auth_headers() == {"Authorization": "Bearer my-token"}
