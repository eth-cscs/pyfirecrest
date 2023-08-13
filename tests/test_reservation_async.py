import pytest
import re
import test_reservation as basic_reservation

from context import firecrest
from firecrest import __app_name__, __version__
from typer.testing import CliRunner


runner = CliRunner()


@pytest.fixture
def valid_client(fc_server):
    class ValidAuthorization:
        def get_access_token(self):
            return "VALID_TOKEN"

    client = firecrest.AsyncFirecrest(
        firecrest_url=fc_server.url_for("/"), authorization=ValidAuthorization()
    )
    client.time_between_calls = {
        "compute": 0,
        "reservations": 0,
        "status": 0,
        "storage": 0,
        "tasks": 0,
        "utilities": 0,
    }

    return client


@pytest.fixture
def invalid_client(fc_server):
    class InvalidAuthorization:
        def get_access_token(self):
            return "INVALID_TOKEN"

    client = firecrest.AsyncFirecrest(
        firecrest_url=fc_server.url_for("/"), authorization=InvalidAuthorization()
    )
    client.time_between_calls = {
        "compute": 0,
        "reservations": 0,
        "status": 0,
        "storage": 0,
        "tasks": 0,
        "utilities": 0,
    }

    return client


@pytest.fixture
def fc_server(httpserver):
    httpserver.expect_request("/reservations", method="GET").respond_with_handler(
        basic_reservation.all_reservations_handler
    )

    httpserver.expect_request("/reservations", method="POST").respond_with_handler(
        basic_reservation.create_reservation_handler
    )

    httpserver.expect_request(
        re.compile("^/reservations/.*"), method="PUT"
    ).respond_with_handler(basic_reservation.update_reservation_handler)

    httpserver.expect_request(
        re.compile("^/reservations/.*"), method="DELETE"
    ).respond_with_handler(basic_reservation.delete_reservation_handler)

    return httpserver


@pytest.mark.asyncio
async def test_all_reservations(valid_client):
    assert await valid_client.all_reservations("cluster1") == []


@pytest.mark.asyncio
async def test_all_reservations_invalid(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        await invalid_client.all_reservations("cluster1")


@pytest.mark.asyncio
async def test_create_reservation(valid_client):
    await valid_client.create_reservation(
        "cluster1",
        "reservation",
        "account",
        "number_of_nodes",
        "node_type",
        "start_time",
        "end_time",
    )


@pytest.mark.asyncio
async def test_create_reservation_invalid(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        await invalid_client.create_reservation(
            "cluster1",
            "reservation",
            "account",
            "number_of_nodes",
            "node_type",
            "start_time",
            "end_time",
        )


@pytest.mark.asyncio
async def test_update_reservation(valid_client):
    await valid_client.update_reservation(
        "cluster1",
        "reservation",
        "account",
        "number_of_nodes",
        "node_type",
        "start_time",
        "end_time",
    )


@pytest.mark.asyncio
async def test_update_reservation_invalid(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        await invalid_client.update_reservation(
            "cluster1",
            "reservation",
            "account",
            "number_of_nodes",
            "node_type",
            "start_time",
            "end_time",
        )


@pytest.mark.asyncio
async def test_delete_reservation(valid_client):
    await valid_client.delete_reservation("cluster1", "reservation")


@pytest.mark.asyncio
async def test_delete_reservation_invalid(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        await invalid_client.delete_reservation("cluster1", "reservation")
