import common
import httpretty
import json
import pytest
import re
import test_authoriation as auth

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
def valid_client():
    class ValidAuthorization:
        def get_access_token(self):
            return "VALID_TOKEN"

    return firecrest.Firecrest(
        firecrest_url="http://firecrest.cscs.ch", authorization=ValidAuthorization()
    )


@pytest.fixture
def valid_credentials():
    return [
        "--firecrest-url=http://firecrest.cscs.ch",
        "--client-id=valid_id",
        "--client-secret=valid_secret",
        "--token-url=https://myauth.com/auth/realms/cscs/protocol/openid-connect/token",
    ]


@pytest.fixture
def invalid_client():
    class ValidAuthorization:
        def get_access_token(self):
            return "INVALID TOKEN"

    return firecrest.Firecrest(
        firecrest_url="http://firecrest.cscs.ch", authorization=ValidAuthorization()
    )


def tasks_callback(request, uri, response_headers):
    if request.headers["Authorization"] != "Bearer VALID_TOKEN":
        return [401, response_headers, '{"message": "Bad token; invalid JSON"}']

    ret = {
        "tasks": {
            "taskid_1": {
                "created_at": "2022-08-16T07:18:54",
                "data": "data",
                "description": "description",
                "hash_id": "taskid_1",
                "last_modify": "2022-08-16T07:18:54",
                "service": "storage",
                "status": "114",
                "task_id": "taskid_1",
                "task_url": "TASK_IP/tasks/taskid_1",
                "updated_at": "2022-08-16T07:18:54",
                "user": "username",
            },
            "taskid_2": {
                "created_at": "2022-08-16T07:18:54",
                "data": "data",
                "description": "description",
                "hash_id": "taskid_2",
                "last_modify": "2022-08-16T07:18:54",
                "service": "storage",
                "status": "112",
                "task_id": "taskid_2",
                "task_url": "TASK_IP/tasks/taskid_2",
                "updated_at": "2022-08-16T07:18:54",
                "user": "username",
            },
            "taskid_3": {
                "created_at": "2022-08-16T07:18:54",
                "data": "data",
                "description": "description",
                "hash_id": "taskid_3",
                "last_modify": "2022-08-16T07:18:54",
                "service": "storage",
                "status": "111",
                "task_id": "taskid_3",
                "task_url": "TASK_IP/tasks/taskid_3",
                "updated_at": "2022-08-16T07:18:54",
                "user": "username",
            },
        }
    }
    if uri == "http://firecrest.cscs.ch/tasks/":
        return [200, response_headers, json.dumps(ret)]

    task_id = uri.split("/")[-1]
    if task_id in {"taskid_1", "taskid_2", "taskid_3"}:
        ret = {"task": ret["tasks"][task_id]}
        return [200, response_headers, json.dumps(ret)]
    else:
        ret = {"error": f"Task {task_id} does not exist"}
        return [404, response_headers, json.dumps(ret)]


@pytest.fixture(autouse=True)
def setup_callbacks():
    httpretty.enable(allow_net_connect=False, verbose=True)

    httpretty.register_uri(
        httpretty.GET,
        re.compile(r"http:\/\/firecrest\.cscs\.ch\/tasks.*"),
        body=tasks_callback,
    )

    httpretty.register_uri(
        httpretty.POST,
        "https://myauth.com/auth/realms/cscs/protocol/openid-connect/token",
        body=auth.auth_callback,
    )

    yield

    httpretty.disable()
    httpretty.reset()


def test_whoami(client1):
    assert client1.whoami() == "bob"


def test_whoami_2(client2):
    assert client2.whoami() == "alice"


def test_whoami_3(client3):
    assert client3.whoami() == "eve"


def test_whoami_invalid_client(invalid_client):
    assert invalid_client.whoami() == None


def test_all_tasks(valid_client):
    assert valid_client._tasks() == {
        "taskid_1": {
            "created_at": "2022-08-16T07:18:54",
            "data": "data",
            "description": "description",
            "hash_id": "taskid_1",
            "last_modify": "2022-08-16T07:18:54",
            "service": "storage",
            "status": "114",
            "task_id": "taskid_1",
            "task_url": "TASK_IP/tasks/taskid_1",
            "updated_at": "2022-08-16T07:18:54",
            "user": "username",
        },
        "taskid_2": {
            "created_at": "2022-08-16T07:18:54",
            "data": "data",
            "description": "description",
            "hash_id": "taskid_2",
            "last_modify": "2022-08-16T07:18:54",
            "service": "storage",
            "status": "112",
            "task_id": "taskid_2",
            "task_url": "TASK_IP/tasks/taskid_2",
            "updated_at": "2022-08-16T07:18:54",
            "user": "username",
        },
        "taskid_3": {
            "created_at": "2022-08-16T07:18:54",
            "data": "data",
            "description": "description",
            "hash_id": "taskid_3",
            "last_modify": "2022-08-16T07:18:54",
            "service": "storage",
            "status": "111",
            "task_id": "taskid_3",
            "task_url": "TASK_IP/tasks/taskid_3",
            "updated_at": "2022-08-16T07:18:54",
            "user": "username",
        },
    }


def test_cli_all_tasks(valid_credentials):
    args = valid_credentials + ["tasks", "--no-pager"]
    result = runner.invoke(cli.app, args=args)
    stdout = common.clean_stdout(result.stdout)
    assert result.exit_code == 0
    assert "Task information: 3 results" in stdout
    assert "Task ID  | Status" in stdout
    assert "taskid_1 | 114" in stdout
    assert "taskid_2 | 112" in stdout
    assert "taskid_3 | 111" in stdout


def test_subset_tasks(valid_client):
    # "taskid_4" is not a valid id but it will be silently ignored
    assert valid_client._tasks(["taskid_1", "taskid_3", "taskid_4"]) == {
        "taskid_1": {
            "created_at": "2022-08-16T07:18:54",
            "data": "data",
            "description": "description",
            "hash_id": "taskid_1",
            "last_modify": "2022-08-16T07:18:54",
            "service": "storage",
            "status": "114",
            "task_id": "taskid_1",
            "task_url": "TASK_IP/tasks/taskid_1",
            "updated_at": "2022-08-16T07:18:54",
            "user": "username",
        },
        "taskid_3": {
            "created_at": "2022-08-16T07:18:54",
            "data": "data",
            "description": "description",
            "hash_id": "taskid_3",
            "last_modify": "2022-08-16T07:18:54",
            "service": "storage",
            "status": "111",
            "task_id": "taskid_3",
            "task_url": "TASK_IP/tasks/taskid_3",
            "updated_at": "2022-08-16T07:18:54",
            "user": "username",
        },
    }


def test_cli_subset_tasks(valid_credentials):
    args = valid_credentials + ["tasks", "--no-pager", "taskid_1", "taskid_3"]
    result = runner.invoke(cli.app, args=args)
    stdout = common.clean_stdout(result.stdout)
    assert result.exit_code == 0
    assert "Task information: 2 results" in stdout
    assert "Task ID  | Status" in stdout
    assert "taskid_1 | 114" in stdout
    assert "taskid_3 | 111" in stdout
    assert "taskid_2 | 112" not in stdout


def test_one_task(valid_client):
    assert valid_client._tasks(["taskid_2"]) == {
        "taskid_2": {
            "created_at": "2022-08-16T07:18:54",
            "data": "data",
            "description": "description",
            "hash_id": "taskid_2",
            "last_modify": "2022-08-16T07:18:54",
            "service": "storage",
            "status": "112",
            "task_id": "taskid_2",
            "task_url": "TASK_IP/tasks/taskid_2",
            "updated_at": "2022-08-16T07:18:54",
            "user": "username",
        }
    }


def test_cli_one_task(valid_credentials):
    args = valid_credentials + ["tasks", "--no-pager", "taskid_2"]
    result = runner.invoke(cli.app, args=args)
    stdout = common.clean_stdout(result.stdout)
    assert result.exit_code == 0
    assert "Task information: 1 result" in stdout
    assert "Task ID  | Status" in stdout
    assert "taskid_2 | 112" in stdout
    assert "taskid_1 | 114" not in stdout
    assert "taskid_3 | 111" not in stdout


def test_invalid_task(valid_client):
    with pytest.raises(firecrest.FirecrestException):
        valid_client._tasks(["invalid_id"])


def test_tasks_invalid(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        invalid_client._tasks()
