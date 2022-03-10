import httpretty
import json
import pytest
import re

from context import firecrest


@pytest.fixture
def valid_client():
    class ValidAuthorization:
        def get_access_token(self):
            return "VALID_TOKEN"

    return firecrest.Firecrest(
        firecrest_url="http://firecrest.cscs.ch", authorization=ValidAuthorization()
    )


@pytest.fixture
def invalid_client():
    class InvalidAuthorization:
        def get_access_token(self):
            return "INVALID_TOKEN"

    return firecrest.Firecrest(
        firecrest_url="http://firecrest.cscs.ch", authorization=InvalidAuthorization()
    )


def all_reservations_callback(request, uri, response_headers):
    if request.headers["Authorization"] != "Bearer VALID_TOKEN":
        return [401, response_headers, '{"message": "Bad token; invalid JSON"}']

    ret = {
        'success': []
    }
    return [200, response_headers, json.dumps(ret)]


def create_reservation_callback(request, uri, response_headers):
    if request.headers["Authorization"] != "Bearer VALID_TOKEN":
        return [401, response_headers, '{"message": "Bad token; invalid JSON"}']

    ret = {}
    return [201, response_headers, json.dumps(ret)]


def update_reservation_callback(request, uri, response_headers):
    if request.headers["Authorization"] != "Bearer VALID_TOKEN":
        return [401, response_headers, '{"message": "Bad token; invalid JSON"}']

    ret = {}
    return [200, response_headers, json.dumps(ret)]


def delete_reservation_callback(request, uri, response_headers):
    if request.headers["Authorization"] != "Bearer VALID_TOKEN":
        return [401, response_headers, '{"message": "Bad token; invalid JSON"}']

    ret = {}
    return [204, response_headers, json.dumps(ret)]


@pytest.fixture(autouse=True)
def setup_callbacks():
    httpretty.enable(allow_net_connect=False, verbose=True)

    httpretty.register_uri(
        httpretty.GET,
        "http://firecrest.cscs.ch/reservations",
        body=all_reservations_callback
    )

    httpretty.register_uri(
        httpretty.POST,
        "http://firecrest.cscs.ch/reservations",
        body=create_reservation_callback
    )

    httpretty.register_uri(
        httpretty.PUT,
        re.compile(r"http:\/\/firecrest\.cscs\.ch\/reservations\/.*"),
        body=update_reservation_callback
    )

    httpretty.register_uri(
        httpretty.DELETE,
        re.compile(r"http:\/\/firecrest\.cscs\.ch\/reservations\/.*"),
        body=delete_reservation_callback,
    )

    yield

    httpretty.disable()
    httpretty.reset()


def test_all_reservations(valid_client):
    assert valid_client.all_reservations("cluster1") == []


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
        "end_time"
    )


def test_create_reservation_invalid(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        invalid_client.create_reservation(
        "cluster1",
        "reservation",
        "account",
        "number_of_nodes",
        "node_type",
        "start_time",
        "end_time"
    )


def test_update_reservation(valid_client):
    valid_client.update_reservation(
        "cluster1",
        "reservation",
        "account",
        "number_of_nodes",
        "node_type",
        "start_time",
        "end_time"
    )


def test_update_reservation_invalid(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        invalid_client.update_reservation(
        "cluster1",
        "reservation",
        "account",
        "number_of_nodes",
        "node_type",
        "start_time",
        "end_time"
    )


def test_delete_reservation(valid_client):
    valid_client.delete_reservation("cluster1", 'reservation')


def test_delete_reservation_invalid(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        invalid_client.delete_reservation("cluster1", 'reservation')
