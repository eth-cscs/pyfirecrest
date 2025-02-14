import common
import json
import pytest
import re
import test_authorisation as auth

from context import firecrest
from typer.testing import CliRunner
from werkzeug.wrappers import Response
from werkzeug.wrappers import Request

from firecrest import __app_name__, __version__, cli


runner = CliRunner()


def test_cli_version():
    result = runner.invoke(cli.app, ["--version"])
    assert result.exit_code == 0
    assert f"FirecREST CLI Version: {__version__}\n" in result.stdout


@pytest.fixture
def client1(fc_server):
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

    return firecrest.v1.Firecrest(
        firecrest_url=fc_server.url_for("/"), authorization=ValidAuthorization()
    )


@pytest.fixture
def client2(fc_server):
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

    return firecrest.v1.Firecrest(
        firecrest_url=fc_server.url_for("/"), authorization=ValidAuthorization()
    )


@pytest.fixture
def client3(fc_server):
    class ValidAuthorization:
        def get_access_token(self):
            # This token was created in https://jwt.io/ with payload:
            # {
            #     "preferred_username": "eve"
            # }
            return "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJwcmVmZXJyZWRfdXNlcm5hbWUiOiJldmUifQ.SGVPDrJdy8b5jRpxcw9ILLsf8M2ljAYWxiN0A1b_1SE"

    return firecrest.v1.Firecrest(
        firecrest_url=fc_server.url_for("/"), authorization=ValidAuthorization()
    )


@pytest.fixture
def valid_client(fc_server):
    class ValidAuthorization:
        def get_access_token(self):
            return "VALID_TOKEN"

    client = firecrest.v1.Firecrest(
        firecrest_url=fc_server.url_for("/"), authorization=ValidAuthorization()
    )
    client.set_api_version("1.16.0")
    return client


@pytest.fixture
def valid_credentials(fc_server, auth_server):
    return [
        f"--firecrest-url={fc_server.url_for('/')}",
        "--client-id=valid_id",
        "--client-secret=valid_secret",
        f"--token-url={auth_server.url_for('/auth/token')}",
        "--api-version=1.16.0",
    ]


@pytest.fixture
def invalid_client(fc_server):
    class InvalidAuthorization:
        def get_access_token(self):
            return "INVALID_TOKEN"

    client = firecrest.v1.Firecrest(
        firecrest_url=fc_server.url_for("/"), authorization=InvalidAuthorization()
    )
    client.set_api_version("1.16.0")
    return client


def tasks_handler(request: Request):
    if request.headers["Authorization"] != "Bearer VALID_TOKEN":
        return Response(
            json.dumps({"message": "Bad token; invalid JSON"}),
            status=401,
            content_type="application/json",
        )

    all_tasks = {
        "taskid_1": {
            "created_at": "2022-08-16T07:18:54",
            "data": "data",
            "description": "description",
            "hash_id": "taskid_1",
            "last_modify": "2022-08-16T07:18:54",
            "service": "storage",
            "status": "114",
            "system": "cluster1",
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
            "system": "cluster1",
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
            "system": "cluster1",
            "task_id": "taskid_3",
            "task_url": "TASK_IP/tasks/taskid_3",
            "updated_at": "2022-08-16T07:18:54",
            "user": "username",
        }
    }
    status_code = 200
    tasks = request.args.get("tasks")

    if tasks:
        tasks = tasks.split(",")
        ret = {
         "tasks": {k: v for k, v in all_tasks.items() if k in tasks}
        }
    else:
        ret = {
         "tasks": all_tasks
        }

    return Response(
        json.dumps(ret), status=status_code, content_type="application/json"
    )


@pytest.fixture
def fc_server(httpserver):
    httpserver.expect_request(
        re.compile("^/tasks"), method="GET"
    ).respond_with_handler(tasks_handler)

    return httpserver


@pytest.fixture
def auth_server(httpserver):
    httpserver.expect_request("/auth/token").respond_with_handler(auth.auth_handler)
    return httpserver


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
            "system": "cluster1",
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
            "system": "cluster1",
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
            "system": "cluster1",
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
            "system": "cluster1",
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
            "system": "cluster1",
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
            "system": "cluster1",
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
    assert valid_client._tasks(["invalid_id"]) == {}


def test_tasks_invalid(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        invalid_client._tasks()
