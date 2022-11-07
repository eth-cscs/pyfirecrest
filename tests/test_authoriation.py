import httpretty
import json
import pytest

from context import firecrest


def auth_callback(request, uri, response_headers):
    client_id = request.parsed_body["client_id"][0]
    client_secret = request.parsed_body["client_secret"][0]
    if client_id == "valid_id":
        if client_secret == "valid_secret":
            ret = {
                "access_token": "token_1",
                "expires_in": 15,
                "refresh_expires_in": 0,
                "token_type": "Bearer",
                "not-before-policy": 0,
                "scope": "profile firecrest email",
            }
            return [200, response_headers, json.dumps(ret)]
        if client_secret == "valid_secret_2":
            ret = {
                "access_token": "token_2",
                "expires_in": 15,
                "refresh_expires_in": 0,
                "token_type": "Bearer",
                "not-before-policy": 0,
                "scope": "profile firecrest email",
            }
            return [200, response_headers, json.dumps(ret)]
        else:
            ret = {
                "error": "unauthorized_client",
                "error_description": "Invalid client secret",
            }
            return [400, response_headers, json.dumps(ret)]
    else:
        ret = {
            "error": "invalid_client",
            "error_description": "Invalid client credentials",
        }
        return [400, response_headers, json.dumps(ret)]


@pytest.fixture(autouse=True)
def setup_callbacks():
    httpretty.enable(allow_net_connect=False, verbose=True)

    httpretty.register_uri(
        httpretty.POST,
        "https://myauth.com/auth/realms/cscs/protocol/openid-connect/token",
        body=auth_callback,
    )

    yield

    httpretty.disable()
    httpretty.reset()


def test_client_credentials_valid():
    auth_obj = firecrest.ClientCredentialsAuth(
        "valid_id",
        "valid_secret",
        "https://myauth.com/auth/realms/cscs/protocol/openid-connect/token",
    )
    assert auth_obj._min_token_validity == 10
    assert auth_obj.get_access_token() == "token_1"
    # Change the secret differentiate between first and second request
    auth_obj._client_secret = "valid_secret_2"
    assert auth_obj.get_access_token() == "token_1"

    auth_obj = firecrest.ClientCredentialsAuth(
        "valid_id",
        "valid_secret",
        "https://myauth.com/auth/realms/cscs/protocol/openid-connect/token",
        min_token_validity=20,
    )
    assert auth_obj.get_access_token() == "token_1"
    # Change the secret differentiate between first and second request
    auth_obj._client_secret = "valid_secret_2"
    assert auth_obj.get_access_token() == "token_2"


def test_client_credentials_invalid_id():
    auth_obj = firecrest.ClientCredentialsAuth(
        "invalid_id",
        "valid_secret",
        "https://myauth.com/auth/realms/cscs/protocol/openid-connect/token",
    )
    with pytest.raises(Exception) as exc_info:
        auth_obj.get_access_token()

    assert "Client credentials error" in str(exc_info.value)


def test_client_credentials_invalid_secret():
    auth_obj = firecrest.ClientCredentialsAuth(
        "valid_id",
        "invalid_secret",
        "https://myauth.com/auth/realms/cscs/protocol/openid-connect/token",
    )
    with pytest.raises(Exception) as exc_info:
        auth_obj.get_access_token()

    assert "Client credentials error" in str(exc_info.value)
