import pytest

from context import firecrest
from typer.testing import CliRunner

from firecrest import __app_name__, __version__, cli


runner = CliRunner()

def test_cli_version():
    result = runner.invoke(cli.app, ["--version"])
    assert result.exit_code == 0
    assert f"FirecREST CLI Version: {__version__}\n" in result.stdout

@pytest.fixture
def client1():
    class ValidAuthorization:
        def get_access_token(self):
            # This token was created in https://jwt.io/ with payload:
            # {
            #     "realm_access": {
            #         "roles": [
            #             "firecrest-sa"
            #         ]
            #     },
            #     "resource_access": {
            #         "bob-client": {
            #             "roles": [
            #             "bob"
            #             ]
            #         }
            #     },
            #     "clientId": "bob-client",
            #     "preferred_username": "service-account-bob-client"
            # }
            return "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyZWFsbV9hY2Nlc3MiOnsicm9sZXMiOlsiZmlyZWNyZXN0LXNhIl19LCJyZXNvdXJjZV9hY2Nlc3MiOnsiYm9iLWNsaWVudCI6eyJyb2xlcyI6WyJib2IiXX19LCJjbGllbnRJZCI6ImJvYi1jbGllbnQiLCJwcmVmZXJyZWRfdXNlcm5hbWUiOiJzZXJ2aWNlLWFjY291bnQtYm9iLWNsaWVudCJ9.XfCXDclEBh7faQrOF2piYdnb7c3AUiCxDesTkNSwpSY"

    return firecrest.Firecrest(
        firecrest_url="http://firecrest.cscs.ch", authorization=ValidAuthorization()
    )


@pytest.fixture
def client2():
    class ValidAuthorization:
        def get_access_token(self):
            # This token was created in https://jwt.io/ with payload:
            # {
            #     "realm_access": {
            #         "roles": [
            #             "other-role"
            #         ]
            #     },
            #     "preferred_username": "alice"
            # }
            return "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyZWFsbV9hY2Nlc3MiOnsicm9sZXMiOlsib3RoZXItcm9sZSJdfSwicHJlZmVycmVkX3VzZXJuYW1lIjoiYWxpY2UifQ.dpo1_F9jkV-RpNGqTaCNLbM-JPMnstDg7mQjzbwDp5g"

    return firecrest.Firecrest(
        firecrest_url="http://firecrest.cscs.ch", authorization=ValidAuthorization()
    )


@pytest.fixture
def client3():
    class ValidAuthorization:
        def get_access_token(self):
            # This token was created in https://jwt.io/ with payload:
            # {
            #     "preferred_username": "eve"
            # }
            return "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJwcmVmZXJyZWRfdXNlcm5hbWUiOiJldmUifQ.SGVPDrJdy8b5jRpxcw9ILLsf8M2ljAYWxiN0A1b_1SE"

    return firecrest.Firecrest(
        firecrest_url="http://firecrest.cscs.ch", authorization=ValidAuthorization()
    )


@pytest.fixture
def invalid_client():
    class ValidAuthorization:
        def get_access_token(self):
            return "INVALID TOKEN"

    return firecrest.Firecrest(
        firecrest_url="http://firecrest.cscs.ch", authorization=ValidAuthorization()
    )


def test_whoami(client1):
    assert client1.whoami() == "bob"


def test_whoami_2(client2):
    assert client2.whoami() == "alice"


def test_whoami_3(client3):
    assert client3.whoami() == "eve"


def test_whoami_invalid_client(invalid_client):
    assert invalid_client.whoami() == None
