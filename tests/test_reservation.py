import json
import pytest
import re
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

    return firecrest.Firecrest(
        firecrest_url=fc_server.url_for("/"), authorization=ValidAuthorization()
    )


@pytest.fixture
def valid_credentials(fc_server, auth_server):
    return [
        f"--firecrest-url={fc_server.url_for('/')}",
        "--client-id=valid_id",
        "--client-secret=valid_secret",
        f"--token-url={auth_server.url_for('/auth/token')}",
    ]


@pytest.fixture
def invalid_client(fc_server):
    class InvalidAuthorization:
        def get_access_token(self):
            return "INVALID_TOKEN"

    return firecrest.Firecrest(
        firecrest_url=fc_server.url_for("/"), authorization=InvalidAuthorization()
    )


def all_reservations_handler(request: Request):
    if request.headers["Authorization"] != "Bearer VALID_TOKEN":
        return Response(
            json.dumps({"message": "Bad token; invalid JSON"}),
            status=401,
            content_type="application/json",
        )

    ret = {"success": []}
    return Response(json.dumps(ret), status=200, content_type="application/json")


def create_reservation_handler(request: Request):
    if request.headers["Authorization"] != "Bearer VALID_TOKEN":
        return Response(
            json.dumps({"message": "Bad token; invalid JSON"}),
            status=401,
            content_type="application/json",
        )

    ret = {}
    return Response(json.dumps(ret), status=201, content_type="application/json")


def update_reservation_handler(request: Request):
    if request.headers["Authorization"] != "Bearer VALID_TOKEN":
        return Response(
            json.dumps({"message": "Bad token; invalid JSON"}),
            status=401,
            content_type="application/json",
        )

    ret = {}
    return Response(json.dumps(ret), status=200, content_type="application/json")


def delete_reservation_handler(request: Request):
    if request.headers["Authorization"] != "Bearer VALID_TOKEN":
        return Response(
            json.dumps({"message": "Bad token; invalid JSON"}),
            status=401,
            content_type="application/json",
        )

    ret = {}
    return Response(json.dumps(ret), status=204, content_type="application/json")


@pytest.fixture
def fc_server(httpserver):
    httpserver.expect_request("/reservations", method="GET").respond_with_handler(
        all_reservations_handler
    )

    httpserver.expect_request("/reservations", method="POST").respond_with_handler(
        create_reservation_handler
    )

    httpserver.expect_request(
        re.compile("^/reservations/.*"), method="PUT"
    ).respond_with_handler(update_reservation_handler)

    httpserver.expect_request(
        re.compile("^/reservations/.*"), method="DELETE"
    ).respond_with_handler(delete_reservation_handler)

    return httpserver


@pytest.fixture
def auth_server(httpserver):
    httpserver.expect_request("/auth/token").respond_with_handler(auth.auth_handler)
    return httpserver


def test_all_reservations(valid_client):
    assert valid_client.all_reservations("cluster1") == []


def test_cli_all_reservations(valid_credentials):
    args = valid_credentials + ["reservation", "list", "cluster1"]
    result = runner.invoke(cli.app, args=args)
    assert result.exit_code == 0


def test_all_reservations_invalid(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        invalid_client.all_reservations("cluster1")


def test_create_reservation(valid_client):
    valid_client.create_reservation(
        "cluster1",
        "reservation",
        "account",
        "number_of_nodes",
        "node_type",
        "start_time",
        "end_time",
    )


def test_cli_create_reservation(valid_credentials):
    args = valid_credentials + [
        "reservation",
        "create",
        "cluster1",
        "reservation",
        "account",
        "number_of_nodes",
        "node_type",
        "start_time",
        "end_time",
    ]
    result = runner.invoke(cli.app, args=args)
    assert result.exit_code == 0


def test_create_reservation_invalid(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        invalid_client.create_reservation(
            "cluster1",
            "reservation",
            "account",
            "number_of_nodes",
            "node_type",
            "start_time",
            "end_time",
        )


def test_update_reservation(valid_client):
    valid_client.update_reservation(
        "cluster1",
        "reservation",
        "account",
        "number_of_nodes",
        "node_type",
        "start_time",
        "end_time",
    )


def test_cli_update_reservation(valid_credentials):
    args = valid_credentials + [
        "reservation",
        "update",
        "cluster1",
        "reservation",
        "account",
        "number_of_nodes",
        "node_type",
        "start_time",
        "end_time",
    ]
    result = runner.invoke(cli.app, args=args)
    assert result.exit_code == 0


def test_update_reservation_invalid(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        invalid_client.update_reservation(
            "cluster1",
            "reservation",
            "account",
            "number_of_nodes",
            "node_type",
            "start_time",
            "end_time",
        )


def test_delete_reservation(valid_client):
    valid_client.delete_reservation("cluster1", "reservation")


def test_cli_delete_reservation(valid_credentials):
    args = valid_credentials + ["reservation", "delete", "cluster1", "reservation"]
    result = runner.invoke(cli.app, args=args)
    assert result.exit_code == 0


def test_delete_reservation_invalid(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        invalid_client.delete_reservation("cluster1", "reservation")
