import json
import re

from context import firecrest
import httpretty
import pytest


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
    if uri == "http://firecrest.cscs.ch/status/systems":
        return [200, response_headers, json.dumps(ret)]

    service = uri.split("/")[-1]
    if service == "cluster1":
        ret = {
            "description": "System information",
            "out": {
                "description": "System ready",
                "status": "available",
                "system": "cluster1",
            },
        }
        return [200, response_headers, json.dumps(ret)]
    elif service == "cluster2":
        ret = {
            "description": "System information",
            "out": {
                "description": "System ready",
                "status": "available",
                "system": "cluster2",
            },
        }
        return [200, response_headers, json.dumps(ret)]
    else:
        ret = {"description": "System does not exists."}
        return [404, response_headers, json.dumps(ret)]


def parameters_callback(request, uri, response_headers):
    if request.headers["Authorization"] != "Bearer VALID_TOKEN":
        return [401, response_headers, '{"message": "Bad token; invalid JSON"}']

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
    return [200, response_headers, json.dumps(ret)]


@pytest.fixture(autouse=True)
def setup_callbacks():
    httpretty.enable(allow_net_connect=False, verbose=True)

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

    httpretty.register_uri(
        httpretty.GET,
        "http://firecrest.cscs.ch/status/parameters",
        body=parameters_callback,
    )

    yield

    httpretty.disable()
    httpretty.reset()


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
    with pytest.raises(firecrest.UnauthorizedException):
        invalid_client.all_services()


def test_service(valid_client):
    assert valid_client.service("utilities") == {
        "description": "server up & flask running",
        "service": "utilities",
        "status": "available",
    }


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


def test_all_systems_invalid(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        invalid_client.all_systems()


def test_system(valid_client):
    assert valid_client.system("cluster1") == {
        "description": "System ready",
        "status": "available",
        "system": "cluster1",
    }


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


def test_parameters_invalid(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        invalid_client.parameters()
