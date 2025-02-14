import pytest
import re
import test_extras as basic_extras

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
        "/tasks", method="GET"
    ).respond_with_handler(basic_extras.tasks_handler)

    return httpserver


@pytest.mark.asyncio
async def test_all_tasks(valid_client):
    assert await valid_client._tasks() == {
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


@pytest.mark.asyncio
async def test_subset_tasks(valid_client):
    # "taskid_4" is not a valid id but it will be silently ignored
    assert await valid_client._tasks(["taskid_1", "taskid_3", "taskid_4"]) == {
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


@pytest.mark.asyncio
async def test_one_task(valid_client):
    assert await valid_client._tasks(["taskid_2"]) == {
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


@pytest.mark.asyncio
async def test_invalid_task(valid_client):
    assert await valid_client._tasks(["invalid_id"]) == {}


@pytest.mark.asyncio
async def test_tasks_invalid(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        await invalid_client._tasks()
