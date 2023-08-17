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


@pytest.fixture
def auth_server(httpserver):
    httpserver.expect_request("/auth/token").respond_with_handler(auth_handler)
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
