import httpretty
import json
import pytest
import re

from context import firecrest
from firecrest.BasicClient import ExternalUpload, ExternalDownload


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


def internal_transfer_callback(request, uri, response_headers):
    if request.headers["Authorization"] != "Bearer VALID_TOKEN":
        return [401, response_headers, '{"message": "Bad token; invalid JSON"}']

    if request.headers["X-Machine-Name"] != "cluster1":
        response_headers["X-Machine-Does-Not-Exist"] = "Machine does not exist"
        return [
            400,
            response_headers,
            '{"description": "Failed to submit job", "error": "Machine does not exist"}',
        ]

    ret = {
        "success": "Task created",
        "task_id": "internal_transfer_id",
        "task_url": "https://148.187.97.214:8443/tasks/internal_transfer_id",
    }
    status_code = 201
    return [status_code, response_headers, json.dumps(ret)]


def external_download_callback(request, uri, response_headers):
    if request.headers["Authorization"] != "Bearer VALID_TOKEN":
        return [401, response_headers, '{"message": "Bad token; invalid JSON"}']

    # TODO Machine is ignored at this point
    # if request.headers["X-Machine-Name"] != "cluster1":
    #     response_headers["X-Machine-Does-Not-Exist"] = "Machine does not exist"
    #     return [
    #         400,
    #         response_headers,
    #         '{"description": "Failed to submit job", "error": "Machine does not exist"}',
    #     ]

    # I couldn't find a better way to get the params from the request
    if b"sourcePath=%2Fpath%2Fto%2Fremote%2Fsource" in request.body:
        ret = {
            "success": "Task created",
            "task_id": "external_download_id",
            "task_url": "https://148.187.97.214:8443/tasks/external_download_id",
        }
        status_code = 201
    else:
        response_headers["X-Invalid-Path"] = "path is an invalid path"
        ret = {"description": "sourcePath error"}
        status_code = 400

    return [status_code, response_headers, json.dumps(ret)]


def external_upload_callback(request, uri, response_headers):
    if request.headers["Authorization"] != "Bearer VALID_TOKEN":
        return [401, response_headers, '{"message": "Bad token; invalid JSON"}']

    # TODO Machine is ignored at this point
    # if request.headers["X-Machine-Name"] != "cluster1":
    #     response_headers["X-Machine-Does-Not-Exist"] = "Machine does not exist"
    #     return [
    #         400,
    #         response_headers,
    #         '{"description": "Failed to submit job", "error": "Machine does not exist"}',
    #     ]

    # I couldn't find a better way to get the params from the request
    if (
        b"sourcePath=%2Fpath%2Fto%2Flocal%2Fsource" in request.body
        and b"targetPath=%2Fpath%2Fto%2Fremote%2Fdestination" in request.body
    ):
        ret = {
            "success": "Task created",
            "task_id": "external_upload_id",
            "task_url": "https://148.187.97.214:8443/tasks/external_upload_id",
        }
        status_code = 201
    else:
        response_headers["X-Invalid-Path"] = "path is an invalid path"
        ret = {"description": "sourcePath error"}
        status_code = 400

    return [status_code, response_headers, json.dumps(ret)]


# Global variables for tasks
internal_transfer_retry = 0
internal_transfer_result = 1
external_upload_retry = 0
external_upload_result = 3
external_download_retry = 0
external_download_result = 0


def storage_tasks_callback(request, uri, response_headers):
    if request.headers["Authorization"] != "Bearer VALID_TOKEN":
        return [401, response_headers, '{"message": "Bad token; invalid JSON"}']

    global internal_transfer_retry
    global external_download_retry

    taskid = uri.split("/")[-1]
    if taskid == "tasks":
        # TODO: return all tasks
        pass
    elif taskid == "internal_transfer_id":
        if internal_transfer_retry < internal_transfer_result:
            internal_transfer_retry += 1
            ret = {
                "task": {
                    "data": "https://127.0.0.1:5003",
                    "description": "Queued",
                    "hash_id": taskid,
                    "last_modify": "2021-12-04T11:52:10",
                    "service": "compute",
                    "status": "100",
                    "task_id": taskid,
                    "task_url": f"https://148.187.97.214:8443/tasks/{taskid}",
                    "user": "username",
                }
            }
            status_code = 200
        else:
            ret = {
                "task": {
                    "data": {
                        "job_data_err": "",
                        "job_data_out": "",
                        "job_file": f"/path/to/firecrest/{taskid}/sbatch-job.sh",
                        "job_file_err": f"/path/to/firecrest/{taskid}/job-35363861.err",
                        "job_file_out": f"/path/to/firecrest/{taskid}/job-35363861.out",
                        "jobid": 35363861,
                        "result": "Job submitted",
                    },
                    "description": "Finished successfully",
                    "hash_id": taskid,
                    "last_modify": "2021-12-06T13:48:52",
                    "service": "compute",
                    "status": "200",
                    "task_id": "6f514b060ca036917f4194964b6e949c",
                    "task_url": f"https://148.187.97.214:8443/tasks/{taskid}",
                    "user": "username",
                }
            }
            status_code = 200
    elif taskid == "external_download_id":
        if external_download_retry < 1:
            external_download_retry += 1
            ret = {
                "task": {
                    "data": "Queued",
                    "description": "Queued",
                    "hash_id": taskid,
                    "last_modify": "2021-12-04T11:52:10",
                    "service": "storage",
                    "status": "100",
                    "task_id": taskid,
                    "task_url": f"https://148.187.97.214:8443/tasks/{taskid}",
                    "user": "username",
                }
            }
            status_code = 200
        elif external_download_retry < 2:
            external_download_retry += 1
            ret = {
                "task": {
                    "data": "Started upload from filesystem to Object Storage",
                    "description": "Started upload from filesystem to Object Storage",
                    "hash_id": taskid,
                    "last_modify": "2021-12-04T11:52:10",
                    "service": "storage",
                    "status": "116",
                    "task_id": taskid,
                    "task_url": f"https://148.187.97.214:8443/tasks/{taskid}",
                    "user": "username",
                }
            }
            status_code = 200
        elif external_download_retry < 2:
            external_download_retry += 1
            ret = {
                "task": {
                    "data": "https://object_storage_link.com",
                    "description": "Started upload from filesystem to Object Storage",
                    "hash_id": taskid,
                    "last_modify": "2021-12-04T11:52:10",
                    "service": "storage",
                    "status": "117",
                    "task_id": taskid,
                    "task_url": f"https://148.187.97.214:8443/tasks/{taskid}",
                    "user": "username",
                }
            }
            status_code = 200

    return [status_code, response_headers, json.dumps(ret)]


@pytest.fixture(autouse=True)
def setup_callbacks():
    httpretty.enable(allow_net_connect=False, verbose=True)

    httpretty.register_uri(
        httpretty.POST,
        re.compile(r"http:\/\/firecrest\.cscs\.ch\/storage\/xfer-internal.*"),
        body=internal_transfer_callback,
    )

    httpretty.register_uri(
        httpretty.GET,
        re.compile(r"http:\/\/firecrest\.cscs\.ch\/tasks.*"),
        body=storage_tasks_callback,
    )

    httpretty.register_uri(
        httpretty.POST,
        "http://firecrest.cscs.ch/storage/xfer-external/download",
        body=external_download_callback,
    )

    httpretty.register_uri(
        httpretty.POST,
        "http://firecrest.cscs.ch/storage/xfer-external/upload",
        body=external_upload_callback,
    )

    yield

    httpretty.disable()
    httpretty.reset()


def test_internal_transfer(valid_client):
    global internal_transfer_retry

    # mv job
    internal_transfer_retry = 0
    assert valid_client.submit_move_job(
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
    }

    # cp job
    internal_transfer_retry = 0
    assert valid_client.submit_copy_job(
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
    }

    # rsync job
    internal_transfer_retry = 0
    assert valid_client.submit_rsync_job(
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
    }

    # rm job
    internal_transfer_retry = 0
    assert valid_client.submit_delete_job(
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
    }


def test_internal_transfer_invalid_machine(valid_client):
    with pytest.raises(firecrest.HeaderException):
        # mv job
        valid_client.submit_move_job(
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
        valid_client.submit_copy_job(
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
        valid_client.submit_rsync_job(
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
        valid_client.submit_delete_job(
            machine="cluster2",
            target_path="/path/to/destination",
            job_name="mv-job",
            time="2",
            stage_out_job_id="35363851",
            account="project",
        )


def test_internal_transfer_invalid_client(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        # mv job
        invalid_client.submit_move_job(
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
        invalid_client.submit_copy_job(
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
        invalid_client.submit_rsync_job(
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
        invalid_client.submit_delete_job(
            machine="cluster1",
            target_path="/path/to/destination",
            job_name="mv-job",
            time="2",
            stage_out_job_id="35363851",
            account="project",
        )


def test_external_download(valid_client):
    obj = valid_client.external_download("cluster1", "/path/to/remote/source")
    assert isinstance(obj, ExternalDownload)
    assert obj._task_id == "external_download_id"
    assert obj.client == valid_client


def test_external_upload(valid_client):
    obj = valid_client.external_upload(
        "cluster1", "/path/to/local/source", "/path/to/remote/destination"
    )
    assert isinstance(obj, ExternalUpload)
    assert obj._task_id == "external_upload_id"
    assert obj.client == valid_client
