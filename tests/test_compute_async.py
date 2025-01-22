import pytest
import re
import test_compute as basic_compute

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
def slurm_script(tmp_path):
    tmp_dir = tmp_path / "script_dir"
    tmp_dir.mkdir()
    script = tmp_dir / "script.sh"
    script.write_text("#!/bin/bash -l\n# Slurm job script\n")
    return script


@pytest.fixture
def non_slurm_script(tmp_path):
    tmp_dir = tmp_path / "script_dir"
    tmp_dir.mkdir()
    script = tmp_dir / "script.sh"
    script.write_text("non slurm script\n")
    return script


@pytest.fixture
def fc_server(httpserver):
    httpserver.expect_request("/compute/jobs/path", method="POST").respond_with_handler(
        basic_compute.submit_path_handler
    )

    httpserver.expect_request(
        "/compute/jobs/upload", method="POST"
    ).respond_with_handler(basic_compute.submit_upload_handler)

    httpserver.expect_request("/compute/acct", method="GET").respond_with_handler(
        basic_compute.sacct_handler
    )

    httpserver.expect_request("/compute/jobs", method="GET").respond_with_handler(
        basic_compute.queue_handler
    )

    httpserver.expect_request(
        re.compile("^/compute/jobs.*"), method="DELETE"
    ).respond_with_handler(basic_compute.cancel_handler)

    httpserver.expect_request(
        "/tasks", method="GET"
    ).respond_with_handler(basic_compute.tasks_handler)

    httpserver.expect_request(
        "/compute/nodes", method="GET"
    ).respond_with_handler(basic_compute.nodes_request_handler)

    httpserver.expect_request(
        "/compute/partitions", method="GET"
    ).respond_with_handler(basic_compute.partitions_request_handler)

    httpserver.expect_request(
        "/compute/reservations", method="GET"
    ).respond_with_handler(basic_compute.reservations_request_handler)

    return httpserver


@pytest.mark.asyncio
async def test_submit_remote(valid_client):
    global submit_path_retry
    submit_path_retry = 0
    assert await valid_client.submit(
        machine="cluster1", job_script="/path/to/workdir/script.sh", local_file=False
    ) == {
        "firecrest_taskid": "submit_path_job_id_default_account",
        "job_data_err": "",
        "job_data_out": "",
        "job_file": "/path/to/workdir/script.sh",
        "job_file_err": "/path/to/workdir/slurm-35335405.out",
        "job_file_out": "/path/to/workdir/slurm-35335405.out",
        "jobid": 35335405,
        "result": "Job submitted",
    }
    submit_path_retry = 0
    assert await valid_client.submit(
        machine="cluster1",
        job_script="/path/to/workdir/script.sh",
        local_file=False,
        account="proj",
    ) == {
        "firecrest_taskid": "submit_path_job_id_proj_account",
        "job_data_err": "",
        "job_data_out": "",
        "job_file": "/path/to/workdir/script.sh",
        "job_file_err": "/path/to/workdir/slurm-35335405.out",
        "job_file_out": "/path/to/workdir/slurm-35335405.out",
        "jobid": 35335406,
        "result": "Job submitted",
    }


@pytest.mark.asyncio
async def test_submit_local(valid_client, slurm_script):
    # Test submission for local script
    global submit_upload_retry
    submit_upload_retry = 0
    assert await valid_client.submit(
        machine="cluster1", job_script=slurm_script, local_file=True
    ) == {
        "firecrest_taskid": "submit_upload_job_id_default_account",
        "job_data_err": "",
        "job_data_out": "",
        "job_file": "/path/to/firecrest/submit_upload_job_id_default_account/script.sh",
        "job_file_err": "/path/to/firecrest/submit_upload_job_id_default_account/slurm-35342667.out",
        "job_file_out": "/path/to/firecrest/submit_upload_job_id_default_account/slurm-35342667.out",
        "jobid": 35342667,
        "result": "Job submitted",
    }
    submit_upload_retry = 0
    assert await valid_client.submit(
        machine="cluster1", job_script=slurm_script, local_file=True, account="proj"
    ) == {
        "firecrest_taskid": "submit_upload_job_id_proj_account",
        "job_data_err": "",
        "job_data_out": "",
        "job_file": "/path/to/firecrest/submit_upload_job_id_proj_account/script.sh",
        "job_file_err": "/path/to/firecrest/submit_upload_job_id_proj_account/slurm-35342667.out",
        "job_file_out": "/path/to/firecrest/submit_upload_job_id_proj_account/slurm-35342667.out",
        "jobid": 35342668,
        "result": "Job submitted",
    }


@pytest.mark.asyncio
async def test_submit_invalid_arguments(valid_client, non_slurm_script):
    with pytest.raises(firecrest.HeaderException):
        await valid_client.submit(
            machine="cluster1",
            job_script="/path/to/non/existent/file",
            local_file=False,
        )

    global submit_path_retry
    submit_path_retry = 0
    with pytest.raises(firecrest.FirecrestException):
        await valid_client.submit(
            machine="cluster1",
            job_script="/path/to/non/slurm/file.sh",
            local_file=False,
        )

    global submit_upload_retry
    submit_upload_retry = 0

    with pytest.raises(firecrest.FirecrestException):
        await valid_client.submit(
            machine="cluster1", job_script=non_slurm_script, local_file=True
        )


@pytest.mark.asyncio
async def test_submit_invalid_machine(valid_client, slurm_script):
    with pytest.raises(firecrest.HeaderException):
        await valid_client.submit(
            machine="cluster2", job_script="/path/to/file", local_file=False
        )

    with pytest.raises(firecrest.HeaderException):
        await valid_client.submit(
            machine="cluster2", job_script=slurm_script, local_file=True
        )


@pytest.mark.asyncio
async def test_submit_invalid_client(invalid_client, slurm_script):
    with pytest.raises(firecrest.UnauthorizedException):
        await invalid_client.submit(
            machine="cluster1", job_script="/path/to/file", local_file=False
        )

    with pytest.raises(firecrest.UnauthorizedException):
        await invalid_client.submit(
            machine="cluster1", job_script=slurm_script, local_file=True
        )


@pytest.mark.asyncio
async def test_poll(valid_client):
    global acct_retry
    acct_retry = 0
    assert await valid_client.poll(
        machine="cluster1",
        jobs=[352, 2, "334"],
        start_time="starttime",
        end_time="endtime",
    ) == [
        {
            "jobid": "352",
            "name": "firecrest_job_test",
            "nodelist": "nid0[6227-6229]",
            "nodes": "3",
            "partition": "normal",
            "start_time": "2021-11-29T16:31:07",
            "state": "COMPLETED",
            "time": "00:48:00",
            "time_left": "2021-11-29T16:31:47",
            "user": "username",
        },
        {
            "jobid": "334",
            "name": "firecrest_job_test2",
            "nodelist": "nid02401",
            "nodes": "1",
            "partition": "normal",
            "start_time": "2021-11-29T16:31:07",
            "state": "COMPLETED",
            "time": "00:17:12",
            "time_left": "2021-11-29T16:31:50",
            "user": "username",
        },
    ]
    acct_retry = 0
    assert await valid_client.poll(machine="cluster1", jobs=[]) == [
        {
            "jobid": "352",
            "name": "firecrest_job_test",
            "nodelist": "nid0[6227-6229]",
            "nodes": "3",
            "partition": "normal",
            "start_time": "2021-11-29T16:31:07",
            "state": "COMPLETED",
            "time": "00:48:00",
            "time_left": "2021-11-29T16:31:47",
            "user": "username",
        }
    ]
    assert await valid_client.poll(machine="cluster1", jobs=["empty"]) == []


@pytest.mark.asyncio
async def test_poll_invalid_arguments(valid_client):
    global acct_retry
    acct_retry = 0

    with pytest.raises(firecrest.FirecrestException):
        await valid_client.poll(machine="cluster1", jobs=["l"])


@pytest.mark.asyncio
async def test_poll_invalid_machine(valid_client):
    with pytest.raises(firecrest.HeaderException):
        await valid_client.poll(machine="cluster2", jobs=[])


@pytest.mark.asyncio
async def test_poll_invalid_client(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        await invalid_client.poll(machine="cluster1", jobs=[])


@pytest.mark.asyncio
async def test_poll_active(valid_client):
    global queue_retry
    queue_retry = 0
    assert await valid_client.poll_active(machine="cluster1", jobs=[352, 2, "334"]) == [
        {
            "job_data_err": "",
            "job_data_out": "",
            "job_file": "(null)",
            "job_file_err": "stderr-file-not-found",
            "job_file_out": "stdout-file-not-found",
            "jobid": "352",
            "name": "interactive",
            "nodelist": "nid02357",
            "nodes": "1",
            "partition": "debug",
            "start_time": "6:38",
            "state": "RUNNING",
            "time": "2022-03-10T10:11:34",
            "time_left": "23:22",
            "user": "username",
        }
    ]
    queue_retry = 0
    assert await valid_client.poll_active(machine="cluster1", jobs=[]) == [
        {
            "job_data_err": "",
            "job_data_out": "",
            "job_file": "(null)",
            "job_file_err": "stderr-file-not-found",
            "job_file_out": "stdout-file-not-found",
            "jobid": "352",
            "name": "interactive",
            "nodelist": "nid02357",
            "nodes": "1",
            "partition": "debug",
            "start_time": "6:38",
            "state": "RUNNING",
            "time": "2022-03-10T10:11:34",
            "time_left": "23:22",
            "user": "username",
        },
        {
            "job_data_err": "",
            "job_data_out": "",
            "job_file": "(null)",
            "job_file_err": "stderr-file-not-found",
            "job_file_out": "stdout-file-not-found",
            "jobid": "356",
            "name": "interactive",
            "nodelist": "nid02351",
            "nodes": "1",
            "partition": "debug",
            "start_time": "6:38",
            "state": "RUNNING",
            "time": "2022-03-10T10:11:34",
            "time_left": "23:22",
            "user": "username",
        },
    ]


@pytest.mark.asyncio
async def test_poll_active_invalid_arguments(valid_client):
    global queue_retry
    queue_retry = 0

    with pytest.raises(firecrest.FirecrestException):
        await valid_client.poll_active(machine="cluster1", jobs=["l"])

    queue_retry = 0
    with pytest.raises(firecrest.FirecrestException):
        # We assume that jobid is too old and is rejected by squeue
        await valid_client.poll_active(machine="cluster1", jobs=["4"])


@pytest.mark.asyncio
async def test_poll_active_invalid_machine(valid_client):
    with pytest.raises(firecrest.HeaderException):
        await valid_client.poll_active(machine="cluster2", jobs=[])


@pytest.mark.asyncio
async def test_poll_active_invalid_client(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        await invalid_client.poll_active(machine="cluster1", jobs=[])


@pytest.mark.asyncio
async def test_cancel(valid_client):
    global cancel_retry
    cancel_retry = 0
    # Make sure this doesn't raise an error
    await valid_client.cancel(machine="cluster1", job_id=35360071)


@pytest.mark.asyncio
async def test_cancel_invalid_arguments(valid_client):
    global cancel_retry
    cancel_retry = 0
    with pytest.raises(firecrest.FirecrestException):
        await valid_client.cancel(machine="cluster1", job_id="k")

    cancel_retry = 0
    with pytest.raises(firecrest.FirecrestException):
        # Jobid 35360072 is from a different user
        await valid_client.cancel(machine="cluster1", job_id="35360072")


@pytest.mark.asyncio
async def test_cancel_invalid_machine(valid_client):
    with pytest.raises(firecrest.HeaderException):
        await valid_client.cancel(machine="cluster2", job_id=35360071)


@pytest.mark.asyncio
async def test_cancel_invalid_client(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        await invalid_client.cancel(machine="cluster1", job_id=35360071)


@pytest.mark.asyncio
async def test_get_nodes(valid_client):
    response = [{
        "ActiveFeatures": ["f7t"],
        "NodeName": "nid001",
        "Partitions": [
            "part01",
            "part02"
        ],
        "State": [
            "IDLE"
        ]
    }]
    assert await valid_client.nodes(machine="cluster1") == response


@pytest.mark.asyncio
async def test_get_nodes_not_impl(valid_client):
    valid_client.set_api_version("1.15.0")
    with pytest.raises(firecrest.NotImplementedOnAPIversion):
        await valid_client.nodes(machine="cluster1")


@pytest.mark.asyncio
async def test_get_nodes_from_list(valid_client):
    response = [{
        "ActiveFeatures": ["f7t"],
        "NodeName": "nid001",
        "Partitions": [
            "part01",
            "part02"
        ],
        "State": [
            "IDLE"
        ]
    }]
    assert await valid_client.nodes(machine="cluster1",
                                    nodes=["nid001"]) == response


@pytest.mark.asyncio
async def test_get_nodes_from_list_not_impl(valid_client):
    valid_client.set_api_version("1.15.0")
    with pytest.raises(firecrest.NotImplementedOnAPIversion):
        await valid_client.nodes(machine="cluster1", nodes=["nid001"])


@pytest.mark.asyncio
async def test_get_nodes_unknown(valid_client):
    with pytest.raises(firecrest.FirecrestException):
        await valid_client.nodes(machine="cluster1",
                                 nodes=["nidunknown"])


@pytest.mark.asyncio
async def test_get_partitions(valid_client):
    response = [
        {
            "Default": "YES",
            "PartitionName": "part01",
            "State": "UP",
            "TotalCPUs": "2",
            "TotalNodes": "1"
        },
        {
            "Default": "NO",
            "PartitionName": "part02",
            "State": "UP",
            "TotalCPUs": "2",
            "TotalNodes": "1"
        },
        {
            "Default": "NO",
            "PartitionName": "xfer",
            "State": "UP",
            "TotalCPUs": "2",
            "TotalNodes": "1"
        }
    ]
    assert await valid_client.partitions(machine="cluster1") == response


@pytest.mark.asyncio
async def test_get_partitions_not_impl(valid_client):
    valid_client.set_api_version("1.15.0")
    with pytest.raises(firecrest.NotImplementedOnAPIversion):
        await valid_client.partitions(machine="cluster1")


@pytest.mark.asyncio
async def test_get_partitions_from_list(valid_client):
    response = [
        {
            "Default": "YES",
            "PartitionName": "part01",
            "State": "UP",
            "TotalCPUs": "2",
            "TotalNodes": "1"
        },
        {
            "Default": "NO",
            "PartitionName": "part02",
            "State": "UP",
            "TotalCPUs": "2",
            "TotalNodes": "1"
        },
        {
            "Default": "NO",
            "PartitionName": "xfer",
            "State": "UP",
            "TotalCPUs": "2",
            "TotalNodes": "1"
        }
    ]
    assert await valid_client.partitions(
        machine="cluster1", partitions=["part01", "part02", "xfer"]
    ) == response


@pytest.mark.asyncio
async def test_get_partitions_from_list_not_impl(valid_client):
    valid_client.set_api_version("1.15.0")
    with pytest.raises(firecrest.NotImplementedOnAPIversion):
        await valid_client.partitions(
            machine="cluster1", partitions=["part01", "part02", "xfer"]
        )


@pytest.mark.asyncio
async def test_get_partitions_unknown(valid_client):
    with pytest.raises(firecrest.FirecrestException):
        await valid_client.partitions(
            machine="cluster1",
            partitions=["invalid_part"]
        )


@pytest.mark.asyncio
async def test_get_reservations(valid_client):
    response = [
        {
            "EndTime": "2024-05-01T15:00:00",
            "Features": "(null)",
            "Nodes": "nid001",
            "ReservationName": "res01",
            "StartTime": "2024-05-01T12:00:00",
            "State": "INACTIVE"
        },
        {
            "EndTime": "2024-06-01T15:00:00",
            "Features": ["f7t1", "f7t2"],
            "Nodes": "nid002",
            "ReservationName": "res04",
            "StartTime": "2024-06-01T12:00:00",
            "State": "INACTIVE"
        }
    ]
    assert await valid_client.reservations(machine="cluster1") == response


@pytest.mark.asyncio
async def test_get_reservations_not_impl(valid_client):
    valid_client.set_api_version("1.15.0")
    with pytest.raises(firecrest.NotImplementedOnAPIversion):
        await valid_client.reservations(machine="cluster1")


