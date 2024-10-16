import json
import pytest
import re

from context import AsyncFirecrest
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

    return AsyncFirecrest(
        firecrest_url=fc_server.url_for("/"),
        authorization=ValidAuthorization()
    )


@pytest.fixture
def invalid_client(fc_server):
    class InvalidAuthorization:
        def get_access_token(self):
            return "INVALID_TOKEN"

    return AsyncFirecrest(
        firecrest_url=fc_server.url_for("/"),
        authorization=InvalidAuthorization()
    )


@pytest.fixture
def fc_server(httpserver):
    httpserver.expect_request(
         re.compile("^/status/systems.*"), method="GET"
     ).respond_with_handler(systems_handler)
    return httpserver


def systems_handler(request: Request):
    if request.headers["Authorization"] != "Bearer VALID_TOKEN":
        return Response(
            json.dumps({"message": "Bad token; invalid JSON"}),
            status=401,
            content_type="application/json",
        )

    data = read_json_file("responses/systems.json")

    ret = data["response"]
    ret_status = data["status_code"]

    return Response(json.dumps(ret),
                    status=ret_status,
                    content_type="application/json")


@pytest.mark.asyncio
async def test_systems(valid_client):
    data = read_json_file("responses/systems.json")
    assert await valid_client.systems() == data["response"]
