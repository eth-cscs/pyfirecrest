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


httpretty.enable(allow_net_connect=False, verbose=True)


def services_callback(request, uri, response_headers):
    if request.headers["Authorization"] != "Bearer VALID_TOKEN":
        return [401, response_headers, '{"message": "Bad token; invalid JSON"}']

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
    if uri == "http://firecrest.cscs.ch/status/services":
        return [200, response_headers, json.dumps(ret)]

    service = uri.split("/")[-1]
    if service == "utilities":
        ret = ret["out"][0]
        return [200, response_headers, json.dumps(ret)]
    elif service == "compute":
        ret = ret["out"][1]
        return [200, response_headers, json.dumps(ret)]
    else:
        ret = {"description": "Service does not exists"}
        return [404, response_headers, json.dumps(ret)]


def systems_callback(request, uri, response_headers):
    if request.headers["Authorization"] != "Bearer VALID_TOKEN":
        return [401, response_headers, '{"message": "Bad token; invalid JSON"}']

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
    return [200, response_headers, json.dumps(ret)]


httpretty.register_uri(
    httpretty.GET,
    re.compile(r"http:\/\/firecrest\.cscs\.ch\/status\/services.*"),
    body=services_callback,
)

httpretty.register_uri(
    httpretty.GET,
    re.compile(r"http:\/\/firecrest\.cscs\.ch\/status\/systems.*"),
    body=systems_callback,
)


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


def test_all_services_invalid(invalid_client):
    with pytest.raises(Exception):
        invalid_client.all_services()


def test_service(valid_client):
    assert valid_client.service("utilities") == {
        "description": "server up & flask running",
        "service": "utilities",
        "status": "available",
    }


def test_invalid_service(valid_client):
    with pytest.raises(Exception):
        valid_client.service("invalid_service")


def test_service_invalid(invalid_client):
    with pytest.raises(Exception):
        invalid_client.service("utilities")


def test_all_systems(valid_client):
    assert valid_client.all_systems() == [
        {"description": "System ready", "status": "available", "system": "cluster1"},
        {"description": "System ready", "status": "available", "system": "cluster2"},
    ]


def test_all_systems_invalid(invalid_client):
    with pytest.raises(Exception):
        invalid_client.all_systems()
