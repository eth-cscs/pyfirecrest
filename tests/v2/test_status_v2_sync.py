import json
import pytest
import re

from context_v2 import SyncFirecrest
from werkzeug.wrappers import Response
from werkzeug.wrappers import Request


def read_json_file(filename):
    with open(filename) as fp:
        data = json.load(fp)

    return data


@pytest.fixture
def valid_client(fc_server):
    class ValidAuthorization:
        def get_access_token(self):
            return "VALID_TOKEN"

    return SyncFirecrest(
        firecrest_url=fc_server.url_for("/"),
        authorization=ValidAuthorization()
    )


@pytest.fixture
def invalid_client(fc_server):
    class InvalidAuthorization:
        def get_access_token(self):
            return "INVALID_TOKEN"

    return SyncFirecrest(
        firecrest_url=fc_server.url_for("/"),
        authorization=InvalidAuthorization()
    )


@pytest.fixture
def fc_server(httpserver):
    httpserver.expect_request(
         re.compile("/status/.*"), method="GET"
     ).respond_with_handler(status_handler)

    return httpserver


def status_handler(request: Request):
    if request.headers["Authorization"] != "Bearer VALID_TOKEN":
        return Response(
            json.dumps({"message": "Bad token; invalid JSON"}),
            status=401,
            content_type="application/json",
        )

    endpoint = request.url.split("/")[-1]
    data = read_json_file(f"v2/responses/{endpoint}.json")

    ret = data["response"]
    ret_status = data["status_code"]

    return Response(json.dumps(ret),
                    status=ret_status,
                    content_type="application/json")


def test_systems(valid_client):
    data = read_json_file("v2/responses/systems.json")
    resp = valid_client.systems()
    assert resp == data["response"]["systems"]


def test_partitions(valid_client):
    data = read_json_file("v2/responses/partitions.json")
    resp = valid_client.partitions("cluster")
    assert resp == data["response"]["partitions"]


def test_nodes(valid_client):
    data = read_json_file("v2/responses/nodes.json")
    resp = valid_client.nodes("cluster")
    assert resp == data["response"]["nodes"]


def test_reservations(valid_client):
    data = read_json_file("v2/responses/reservations.json")
    resp = valid_client.reservations("cluster")
    assert resp == data["response"]["reservations"]
