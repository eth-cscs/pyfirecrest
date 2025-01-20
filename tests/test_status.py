import common
import json
import pytest
import re
import os
import test_authorisation as auth

from context import firecrest
from firecrest import __app_name__, __version__, cli
from typer.testing import CliRunner
from werkzeug.wrappers import Response
from werkzeug.wrappers import Request


runner = CliRunner()


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


@pytest.fixture
def fc_server(httpserver):
    httpserver.expect_request(
        re.compile("^/status/services.*"), method="GET"
    ).respond_with_handler(services_handler)

    httpserver.expect_request(
        re.compile("^/status/systems.*"), method="GET"
    ).respond_with_handler(systems_handler)

    httpserver.expect_request("/status/parameters", method="GET").respond_with_handler(
        parameters_handler
    )

    httpserver.expect_request(
        re.compile("^/status/filesystems.*"), method="GET"
    ).respond_with_handler(filesystems_handler)

    return httpserver


@pytest.fixture
def auth_server(httpserver):
    httpserver.expect_request("/auth/token").respond_with_handler(auth.auth_handler)
    return httpserver


def services_handler(request: Request):
    if request.headers["Authorization"] != "Bearer VALID_TOKEN":
        return Response(
            json.dumps({"message": "Bad token; invalid JSON"}),
            status=401,
            content_type="application/json",
        )

    ret = {
        "description": "List of services with status and description.",
        "out": [
            {
                "description": "server up & flask running",
                "service": "utilities",
                "status": "available",
            },
            {
                "description": "server up & flask running",
                "service": "compute",
                "status": "available",
            },
        ],
    }
    ret_status = 200
    uri = request.url
    if not uri.endswith("/status/services"):
        service = uri.split("/")[-1]
        if service == "utilities":
            ret = ret["out"][0]
        elif service == "compute":
            ret = ret["out"][1]
        else:
            ret = {"description": "Service does not exists"}
            ret_status = 404

    return Response(json.dumps(ret), status=ret_status, content_type="application/json")


def systems_handler(request: Request):
    if request.headers["Authorization"] != "Bearer VALID_TOKEN":
        return Response(
            json.dumps({"message": "Bad token; invalid JSON"}),
            status=401,
            content_type="application/json",
        )

    ret = {
        "description": "List of systems with status and description.",
        "out": [
            {
                "description": "System ready",
                "status": "available",
                "system": "cluster1",
            },
            {
                "description": "System ready",
                "status": "available",
                "system": "cluster2",
            },
        ],
    }
    ret_status = 200
    uri = request.url
    if not uri.endswith("/status/systems"):
        system = uri.split("/")[-1]
        if system == "cluster1":
            ret = {
                "description": "System information",
                "out": {
                    "description": "System ready",
                    "status": "available",
                    "system": "cluster1",
                },
            }
            ret_status = 200
        elif system == "cluster2":
            ret = {
                "description": "System information",
                "out": {
                    "description": "System ready",
                    "status": "available",
                    "system": "cluster2",
                },
            }
            ret_status = 200
        else:
            ret = {"description": "System does not exists."}
            ret_status = 400

    return Response(json.dumps(ret), status=ret_status, content_type="application/json")


def parameters_handler(request: Request):
    if request.headers["Authorization"] != "Bearer VALID_TOKEN":
        return Response(
            json.dumps({"message": "Bad token; invalid JSON"}),
            status=401,
            content_type="application/json",
        )

    ret = {
        "description": "Firecrest's parameters",
        "out": {
            "storage": [
                {"name": "OBJECT_STORAGE", "unit": "", "value": "swift"},
                {
                    "name": "STORAGE_TEMPURL_EXP_TIME",
                    "unit": "seconds",
                    "value": "2592000",
                },
                {"name": "STORAGE_MAX_FILE_SIZE", "unit": "MB", "value": "512000"},
                {
                    "name": "FILESYSTEMS",
                    "unit": "",
                    "value": [{"mounted": ["/fs1"], "system": "cluster1"}],
                },
            ],
            "utilities": [
                {"name": "UTILITIES_MAX_FILE_SIZE", "unit": "MB", "value": "5"},
                {"name": "UTILITIES_TIMEOUT", "unit": "seconds", "value": "5"},
            ],
        },
    }
    return Response(json.dumps(ret), status=200, content_type="application/json")


def filesystems_handler(request: Request):
    if request.headers["Authorization"] != "Bearer VALID_TOKEN":
        return Response(
            json.dumps({"message": "Bad token; invalid JSON"}),
            status=401,
            content_type="application/json",
        )

    ret = {
        "description": "Filesystem information",
        "out": {
            "cluster": [
                {
                    "description": "Users home filesystem",
                    "name": "HOME",
                    "path": "/home",
                    "status": "available",
                    "status_code": 200
                },
                {
                    "description": "Scratch filesystem",
                    "name": "SCRATCH",
                    "path": "/scratch",
                    "status": "not available",
                    "status_code": 400
                }
            ]
        }
    }
    ret_status = 200
    uri = request.url
    if not uri.endswith("/status/filesystems"):
        system = uri.split("/")[-1]
        if system == "cluster":
            ret["description"] = f"Filesystem information for system {system}"
            ret["out"] = ret["out"][system]
        else:
            ret = {"description": f"System '{system}' does not exists."}
            ret_status = 400

    return Response(json.dumps(ret), status=ret_status, content_type="application/json")


def test_all_services(valid_client):
    assert valid_client.all_services() == [
        {
            "description": "server up & flask running",
            "service": "utilities",
            "status": "available",
        },
        {
            "description": "server up & flask running",
            "service": "compute",
            "status": "available",
        },
    ]


def test_cli_all_services(valid_credentials):
    args = valid_credentials + ["services"]
    result = runner.invoke(cli.app, args=args)
    stdout = common.clean_stdout(result.stdout)
    assert result.exit_code == 0
    assert "Service   | Status    | Description" in stdout
    assert "utilities | available | server up & flask running" in stdout
    assert "compute   | available | server up & flask running" in stdout


def test_all_services_invalid(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        invalid_client.all_services()


def test_service(valid_client):
    assert valid_client.service("utilities") == {
        "description": "server up & flask running",
        "service": "utilities",
        "status": "available",
    }


def test_cli_service(valid_credentials):
    args = valid_credentials + ["services", "--name=utilities"]
    result = runner.invoke(cli.app, args=args)
    stdout = common.clean_stdout(result.stdout)
    assert result.exit_code == 0
    assert "Service   | Status    | Description" in stdout
    assert "utilities | available | server up & flask running" in stdout
    assert "compute" not in stdout


def test_invalid_service(valid_client):
    with pytest.raises(firecrest.FirecrestException):
        valid_client.service("invalid_service")


def test_service_invalid(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        invalid_client.service("utilities")


def test_all_systems(valid_client):
    assert valid_client.all_systems() == [
        {"description": "System ready", "status": "available", "system": "cluster1"},
        {"description": "System ready", "status": "available", "system": "cluster2"},
    ]


def test_cli_all_systems(valid_credentials):
    args = valid_credentials + ["systems"]
    result = runner.invoke(cli.app, args=args)
    stdout = common.clean_stdout(result.stdout)
    assert result.exit_code == 0
    assert "System   | Status    | Description" in stdout
    assert "cluster1 | available | System ready" in stdout
    assert "cluster2 | available | System ready" in stdout


def test_all_systems_invalid(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        invalid_client.all_systems()


def test_system(valid_client):
    assert valid_client.system("cluster1") == {
        "description": "System ready",
        "status": "available",
        "system": "cluster1",
    }


def test_cli_system(valid_credentials):
    args = valid_credentials + ["systems", "--name=cluster1"]
    result = runner.invoke(cli.app, args=args)
    stdout = common.clean_stdout(result.stdout)
    assert result.exit_code == 0
    assert "System   | Status    | Description" in stdout
    assert "cluster1 | available | System ready" in stdout
    assert "cluster2" not in stdout


def test_invalid_system(valid_client):
    with pytest.raises(firecrest.FirecrestException):
        valid_client.system("invalid_system")


def test_system_invalid(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        invalid_client.system("cluster1")


def test_parameters(valid_client):
    assert valid_client.parameters() == {
        "storage": [
            {"name": "OBJECT_STORAGE", "unit": "", "value": "swift"},
            {"name": "STORAGE_TEMPURL_EXP_TIME", "unit": "seconds", "value": "2592000"},
            {"name": "STORAGE_MAX_FILE_SIZE", "unit": "MB", "value": "512000"},
            {
                "name": "FILESYSTEMS",
                "unit": "",
                "value": [{"mounted": ["/fs1"], "system": "cluster1"}],
            },
        ],
        "utilities": [
            {"name": "UTILITIES_MAX_FILE_SIZE", "unit": "MB", "value": "5"},
            {"name": "UTILITIES_TIMEOUT", "unit": "seconds", "value": "5"},
        ],
    }


def test_cli_parameters(valid_credentials):
    args = valid_credentials + ["parameters"]
    result = runner.invoke(cli.app, args=args)
    stdout = common.clean_stdout(result.stdout)
    assert result.exit_code == 0
    assert "Storage parameters" in stdout
    assert "Utilities parameters" in stdout


def test_parameters_invalid(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        invalid_client.parameters()


def test_filesystems(valid_client):
    assert valid_client.filesystems() == {
        "cluster": [
            {
                "description": "Users home filesystem",
                "name": "HOME",
                "path": "/home",
                "status": "available",
                "status_code": 200
            },
            {
                "description": "Scratch filesystem",
                "name": "SCRATCH",
                "path": "/scratch",
                "status": "not available",
                "status_code": 400
            }
        ]
    }

    assert valid_client.filesystems(system_name="cluster") == {
        "cluster": [
            {
                "description": "Users home filesystem",
                "name": "HOME",
                "path": "/home",
                "status": "available",
                "status_code": 200
            },
            {
                "description": "Scratch filesystem",
                "name": "SCRATCH",
                "path": "/scratch",
                "status": "not available",
                "status_code": 400
            }
        ]
    }


def test_cli_filesystems(valid_credentials):
    # Clean up the env var that may be set in the environment
    os.environ.pop("FIRECREST_SYSTEM", None)
    args = valid_credentials + ["filesystems"]
    result = runner.invoke(cli.app, args=args)
    stdout = common.clean_stdout(result.stdout)
    assert result.exit_code == 0
    assert "Status of filesystems for `cluster`" in stdout

    args = valid_credentials + ["filesystems", "--system", "cluster"]
    result = runner.invoke(cli.app, args=args)
    stdout = common.clean_stdout(result.stdout)
    assert result.exit_code == 0
    assert "Status of filesystems for `cluster`" in stdout

    args = valid_credentials + ["filesystems", "--system", "invalid_cluster"]
    result = runner.invoke(cli.app, args=args)
    stdout = common.clean_stdout(result.stdout)
    assert result.exit_code == 1


def test_filesystems_invalid(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        invalid_client.filesystems()
