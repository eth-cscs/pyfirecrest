import pytest
import re
import test_status as basic_status

from context import firecrest
from firecrest import __app_name__, __version__


@pytest.fixture
def valid_client(fc_server):
    class ValidAuthorization:
        def get_access_token(self):
            return "VALID_TOKEN"

    client = firecrest.v1.AsyncFirecrest(
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
    client.set_api_version("1.16.0")

    return client


@pytest.fixture
def invalid_client(fc_server):
    class InvalidAuthorization:
        def get_access_token(self):
            return "INVALID_TOKEN"

    client = firecrest.v1.AsyncFirecrest(
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
    client.set_api_version("1.16.0")

    return client


@pytest.fixture
def fc_server(httpserver):
    httpserver.expect_request(
        re.compile("^/status/services.*"), method="GET"
    ).respond_with_handler(basic_status.services_handler)

    httpserver.expect_request(
        re.compile("^/status/systems.*"), method="GET"
    ).respond_with_handler(basic_status.systems_handler)

    httpserver.expect_request("/status/parameters", method="GET").respond_with_handler(
        basic_status.parameters_handler
    )

    httpserver.expect_request(
        re.compile("^/status/filesystems.*"), method="GET"
    ).respond_with_handler(basic_status.filesystems_handler)

    return httpserver


@pytest.mark.asyncio
async def test_all_services(valid_client):
    assert await valid_client.all_services() == [
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


@pytest.mark.asyncio
async def test_all_services_invalid(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        await invalid_client.all_services()


@pytest.mark.asyncio
async def test_service(valid_client):
    assert await valid_client.service("utilities") == {
        "description": "server up & flask running",
        "service": "utilities",
        "status": "available",
    }


@pytest.mark.asyncio
async def test_invalid_service(valid_client):
    with pytest.raises(firecrest.FirecrestException):
        await valid_client.service("invalid_service")


@pytest.mark.asyncio
async def test_service_invalid(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        await invalid_client.service("utilities")


@pytest.mark.asyncio
async def test_all_systems(valid_client):
    assert await valid_client.all_systems() == [
        {"description": "System ready", "status": "available", "system": "cluster1"},
        {"description": "System ready", "status": "available", "system": "cluster2"},
    ]


@pytest.mark.asyncio
async def test_all_systems_invalid(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        await invalid_client.all_systems()


@pytest.mark.asyncio
async def test_system(valid_client):
    assert await valid_client.system("cluster1") == {
        "description": "System ready",
        "status": "available",
        "system": "cluster1",
    }


@pytest.mark.asyncio
async def test_invalid_system(valid_client):
    with pytest.raises(firecrest.FirecrestException):
        await valid_client.system("invalid_system")


@pytest.mark.asyncio
async def test_system_invalid(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        await invalid_client.system("cluster1")


@pytest.mark.asyncio
async def test_parameters(valid_client):
    assert await valid_client.parameters() == {
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


@pytest.mark.asyncio
async def test_parameters_invalid(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        await invalid_client.parameters()


@pytest.mark.asyncio
async def test_filesystems(valid_client):
    assert await valid_client.filesystems() == {
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

    assert await valid_client.filesystems(system_name="cluster") == {
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


@pytest.mark.asyncio
async def test_filesystems_invalid(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        await invalid_client.filesystems()
