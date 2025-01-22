import pytest
import re
import test_storage as basic_storage

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
        re.compile("^/storage/xfer-internal.*"), method="POST"
    ).respond_with_handler(basic_storage.internal_transfer_handler)

    httpserver.expect_request(
        "/tasks", method="GET"
    ).respond_with_handler(basic_storage.storage_tasks_handler)

    httpserver.expect_request(
        "/storage/xfer-external/download", method="POST"
    ).respond_with_handler(basic_storage.external_download_handler)

    httpserver.expect_request(
        "/storage/xfer-external/upload", method="POST"
    ).respond_with_handler(basic_storage.external_upload_handler)

    return httpserver


@pytest.mark.asyncio
async def test_internal_transfer(valid_client):
    global internal_transfer_retry

    # mv job
    internal_transfer_retry = 0
    assert await valid_client.submit_move_job(
        machine="cluster1",
        source_path="/path/to/source",
        target_path="/path/to/destination",
        job_name="mv-job",
        time="2",
        stage_out_job_id="35363851",
        account="project",
    ) == {
        "job_data_err": "",
        "job_data_out": "",
        "job_file": "/path/to/firecrest/internal_transfer_id/sbatch-job.sh",
        "job_file_err": "/path/to/firecrest/internal_transfer_id/job-35363861.err",
        "job_file_out": "/path/to/firecrest/internal_transfer_id/job-35363861.out",
        "jobid": 35363861,
        "result": "Job submitted",
        "system": "cluster1",
    }

    # cp job
    internal_transfer_retry = 0
    assert await valid_client.submit_copy_job(
        machine="cluster1",
        source_path="/path/to/source",
        target_path="/path/to/destination",
        job_name="mv-job",
        time="2",
        stage_out_job_id="35363851",
        account="project",
    ) == {
        "job_data_err": "",
        "job_data_out": "",
        "job_file": "/path/to/firecrest/internal_transfer_id/sbatch-job.sh",
        "job_file_err": "/path/to/firecrest/internal_transfer_id/job-35363861.err",
        "job_file_out": "/path/to/firecrest/internal_transfer_id/job-35363861.out",
        "jobid": 35363861,
        "result": "Job submitted",
        "system": "cluster1",
    }

    # rsync job
    internal_transfer_retry = 0
    assert await valid_client.submit_rsync_job(
        machine="cluster1",
        source_path="/path/to/source",
        target_path="/path/to/destination",
        job_name="mv-job",
        time="2",
        stage_out_job_id="35363851",
        account="project",
    ) == {
        "job_data_err": "",
        "job_data_out": "",
        "job_file": "/path/to/firecrest/internal_transfer_id/sbatch-job.sh",
        "job_file_err": "/path/to/firecrest/internal_transfer_id/job-35363861.err",
        "job_file_out": "/path/to/firecrest/internal_transfer_id/job-35363861.out",
        "jobid": 35363861,
        "result": "Job submitted",
        "system": "cluster1",
    }

    # rm job
    internal_transfer_retry = 0
    assert await valid_client.submit_delete_job(
        machine="cluster1",
        target_path="/path/to/destination",
        job_name="mv-job",
        time="2",
        stage_out_job_id="35363851",
        account="project",
    ) == {
        "job_data_err": "",
        "job_data_out": "",
        "job_file": "/path/to/firecrest/internal_transfer_id/sbatch-job.sh",
        "job_file_err": "/path/to/firecrest/internal_transfer_id/job-35363861.err",
        "job_file_out": "/path/to/firecrest/internal_transfer_id/job-35363861.out",
        "jobid": 35363861,
        "result": "Job submitted",
        "system": "cluster1",
    }


@pytest.mark.asyncio
async def test_internal_transfer_invalid_machine(valid_client):
    with pytest.raises(firecrest.HeaderException):
        # mv job
        await valid_client.submit_move_job(
            machine="cluster2",
            source_path="/path/to/source",
            target_path="/path/to/destination",
            job_name="mv-job",
            time="2",
            stage_out_job_id="35363851",
            account="project",
        )

    with pytest.raises(firecrest.HeaderException):
        # cp job
        await valid_client.submit_copy_job(
            machine="cluster2",
            source_path="/path/to/source",
            target_path="/path/to/destination",
            job_name="mv-job",
            time="2",
            stage_out_job_id="35363851",
            account="project",
        )

    with pytest.raises(firecrest.HeaderException):
        # rsync job
        await valid_client.submit_rsync_job(
            machine="cluster2",
            source_path="/path/to/source",
            target_path="/path/to/destination",
            job_name="mv-job",
            time="2",
            stage_out_job_id="35363851",
            account="project",
        )

    with pytest.raises(firecrest.HeaderException):
        # rm job
        await valid_client.submit_delete_job(
            machine="cluster2",
            target_path="/path/to/destination",
            job_name="mv-job",
            time="2",
            stage_out_job_id="35363851",
            account="project",
        )


@pytest.mark.asyncio
async def test_internal_transfer_invalid_client(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        # mv job
        await invalid_client.submit_move_job(
            machine="cluster1",
            source_path="/path/to/source",
            target_path="/path/to/destination",
            job_name="mv-job",
            time="2",
            stage_out_job_id="35363851",
            account="project",
        )

    with pytest.raises(firecrest.UnauthorizedException):
        # cp job
        await invalid_client.submit_copy_job(
            machine="cluster1",
            source_path="/path/to/source",
            target_path="/path/to/destination",
            job_name="mv-job",
            time="2",
            stage_out_job_id="35363851",
            account="project",
        )

    with pytest.raises(firecrest.UnauthorizedException):
        # rsync job
        await invalid_client.submit_rsync_job(
            machine="cluster1",
            source_path="/path/to/source",
            target_path="/path/to/destination",
            job_name="mv-job",
            time="2",
            stage_out_job_id="35363851",
            account="project",
        )

    with pytest.raises(firecrest.UnauthorizedException):
        # rm job
        await invalid_client.submit_delete_job(
            machine="cluster1",
            target_path="/path/to/destination",
            job_name="mv-job",
            time="2",
            stage_out_job_id="35363851",
            account="project",
        )


@pytest.mark.asyncio
async def test_external_download(valid_client):
    global external_download_retry
    external_download_retry = 0
    valid_client.set_api_version("1.14.0")
    obj = await valid_client.external_download("cluster1", "/path/to/remote/source")
    assert isinstance(obj, firecrest.v1.AsyncExternalDownload)
    assert obj._task_id == "external_download_id"
    assert obj.client == valid_client


@pytest.mark.asyncio
async def test_external_download_legacy(valid_client):
    global external_download_retry
    external_download_retry = 0
    valid_client.set_api_version("1.13.0")
    obj = await valid_client.external_download("cluster1", "/path/to/remote/sourcelegacy")
    assert isinstance(obj, firecrest.v1.AsyncExternalDownload)
    assert obj._task_id == "external_download_id_legacy"
    assert obj.client == valid_client


@pytest.mark.asyncio
async def test_external_upload(valid_client):
    global external_upload_retry
    external_upload_retry = 0
    obj = await valid_client.external_upload(
        "cluster1", "/path/to/local/source", "/path/to/remote/destination"
    )
    assert isinstance(obj, firecrest.v1.AsyncExternalUpload)
    assert obj._task_id == "external_upload_id"
    assert obj.client == valid_client
