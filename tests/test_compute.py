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


httpretty.enable(allow_net_connect=False, verbose=True)


def submit_path_callback(request, uri, response_headers):
    if request.headers["Authorization"] != "Bearer VALID_TOKEN":
        return [401, response_headers, '{"message": "Bad token; invalid JSON"}']

    if request.headers["X-Machine-Name"] != "cluster1":
        response_headers["X-Machine-Does-Not-Exist"] = "Machine does not exist"
        return [
            400,
            response_headers,
            '{"description": "Failed to submit job", "error": "Machine does not exist"}',
        ]

    target_path = request.parsed_body["targetPath"][0]
    if target_path == "/path/to/workdir/script.sh":
        ret = {
            "success": "Task created",
            "task_id": "submit_path_job_id",
            "task_url": "https://148.187.97.214:8443/tasks/submit_path_job_id",
        }
        status_code = 201
    elif target_path == "/path/to/non/slurm/file.sh":
        ret = {
            "success": "Task created",
            "task_id": "submit_path_job_id_fail",
            "task_url": "https://148.187.97.214:8443/tasks/submit_path_job_id_fail",
        }
        status_code = 201
    else:
        response_headers["X-Invalid-Path"] = f"{target_path} is an invalid path."
        ret = {"description": "Failed to submit job"}
        status_code = 400

    return [status_code, response_headers, json.dumps(ret)]


def submit_upload_callback(request, uri, response_headers):
    if request.headers["Authorization"] != "Bearer VALID_TOKEN":
        return [401, response_headers, '{"message": "Bad token; invalid JSON"}']

    if request.headers["X-Machine-Name"] != "cluster1":
        response_headers["X-Machine-Does-Not-Exist"] = "Machine does not exist"
        return [
            400,
            response_headers,
            '{"description": "Failed to submit job", "error": "Machine does not exist"}',
        ]

    # I couldn't find a better way to get the params from the request
    if b'form-data; name="file"; filename="script.sh"' in request.body:
        if b"#!/bin/bash -l\n" in request.body:
            ret = {
                "success": "Task created",
                "task_id": "submit_upload_job_id",
                "task_url": "https://148.187.97.214:8443/tasks/submit_upload_job_id",
            }
            status_code = 201
        else:
            ret = {
                "success": "Task created",
                "task_id": "submit_upload_job_id_fail",
                "task_url": "https://148.187.97.214:8443/tasks/submit_upload_job_id_fail",
            }
            status_code = 201
    else:
        response_headers["X-Invalid-Path"] = f"path is an invalid path."
        ret = {"description": "Failed to submit job"}
        status_code = 400

    return [status_code, response_headers, json.dumps(ret)]


# Global variables for tasks
submit_path_retry = 0
submit_path_result = 1
submit_upload_retry = 0
submit_upload_result = 1


def tasks_callback(request, uri, response_headers):
    if request.headers["Authorization"] != "Bearer VALID_TOKEN":
        return [401, response_headers, '{"message": "Bad token; invalid JSON"}']

    global submit_path_retry
    global submit_upload_retry

    taskid = uri.split("/")[-1]
    if taskid == "tasks":
        # TODO: return all tasks
        pass
    elif taskid == "submit_path_job_id" or taskid == "submit_path_job_id_fail":
        if submit_path_retry < submit_path_result:
            submit_path_retry += 1
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
        elif taskid == "submit_path_job_id":
            ret = {
                "task": {
                    "data": {
                        "job_data_err": "",
                        "job_data_out": "",
                        "job_file": "/path/to/workdir/script.sh",
                        "job_file_err": "/path/to/workdir/slurm-35335405.out",
                        "job_file_out": "/path/to/workdir/slurm-35335405.out",
                        "jobid": 35335405,
                        "result": "Job submitted",
                    },
                    "description": "Finished successfully",
                    "hash_id": "submit_path_job_id",
                    "last_modify": "2021-12-04T11:52:11",
                    "service": "compute",
                    "status": "200",
                    "task_id": "submit_path_job_id",
                    "task_url": "https://148.187.97.214:8443/tasks/submit_path_job_id",
                    "user": "username",
                }
            }
            status_code = 200
        else:
            ret = {
                "task": {
                    "data": "sbatch: error: This does not look like a batch script...",
                    "description": "Finished with errors",
                    "hash_id": "taskid",
                    "last_modify": "2021-12-04T11:52:11",
                    "service": "compute",
                    "status": "400",
                    "task_id": "taskid",
                    "task_url": f"https://148.187.97.214:8443/tasks/{taskid}",
                    "user": "username",
                }
            }
            status_code = 200
    elif taskid == "submit_upload_job_id" or taskid == "submit_upload_job_id_fail":
        if submit_upload_retry < submit_upload_result:
            submit_upload_retry += 1
            ret = {
                "task": {
                    "data": "Queued",
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
        elif taskid == "submit_upload_job_id":
            ret = {
                "task": {
                    "data": {
                        "job_data_err": "",
                        "job_data_out": "",
                        "job_file": f"/path/to/firecrest/{taskid}/script.sh",
                        "job_file_err": f"/path/to/firecrest/{taskid}/slurm-35342667.out",
                        "job_file_out": f"/path/to/firecrest/{taskid}/slurm-35342667.out",
                        "jobid": 35342667,
                        "result": "Job submitted",
                    },
                    "description": "Finished successfully",
                    "hash_id": taskid,
                    "last_modify": "2021-12-04T11:52:11",
                    "service": "compute",
                    "status": "200",
                    "task_id": taskid,
                    "task_url": f"https://148.187.97.214:8443/tasks/{taskid}",
                    "user": "username",
                }
            }
            status_code = 200
        else:
            ret = {
                "task": {
                    "data": "sbatch: error: This does not look like a batch script...",
                    "description": "Finished with errors",
                    "hash_id": "taskid",
                    "last_modify": "2021-12-04T11:52:11",
                    "service": "compute",
                    "status": "400",
                    "task_id": "taskid",
                    "task_url": f"https://148.187.97.214:8443/tasks/{taskid}",
                    "user": "username",
                }
            }
            status_code = 200

    return [status_code, response_headers, json.dumps(ret)]


# def squeue_callback(request, uri, response_headers):
#     if request.headers["Authorization"] != "Bearer VALID_TOKEN":
#         return [401, response_headers, '{"message": "Bad token; invalid JSON"}']

# def sacct_callback(request, uri, response_headers):
#     if request.headers["Authorization"] != "Bearer VALID_TOKEN":
#         return [401, response_headers, '{"message": "Bad token; invalid JSON"}']

# def cancel_callback(request, uri, response_headers):
#     if request.headers["Authorization"] != "Bearer VALID_TOKEN":
#         return [401, response_headers, '{"message": "Bad token; invalid JSON"}']


httpretty.register_uri(
    httpretty.POST,
    "http://firecrest.cscs.ch/compute/jobs/path",
    body=submit_path_callback,
)

httpretty.register_uri(
    httpretty.POST,
    "http://firecrest.cscs.ch/compute/jobs/upload",
    body=submit_upload_callback,
)

httpretty.register_uri(
    httpretty.GET,
    re.compile(r"http:\/\/firecrest\.cscs\.ch\/tasks.*"),
    body=tasks_callback,
)


def test_submit(valid_client, slurm_script):
    global submit_path_retry
    # Test submission for remote script
    submit_path_retry = 0
    assert valid_client.submit(
        machine="cluster1", job_script="/path/to/workdir/script.sh", local_file=False
    ) == {
        "job_data_err": "",
        "job_data_out": "",
        "job_file": "/path/to/workdir/script.sh",
        "job_file_err": "/path/to/workdir/slurm-35335405.out",
        "job_file_out": "/path/to/workdir/slurm-35335405.out",
        "jobid": 35335405,
        "result": "Job submitted",
    }

    # Test submission for local script
    global submit_upload_retry
    submit_upload_retry = 0
    assert valid_client.submit(
        machine="cluster1", job_script=slurm_script, local_file=True
    ) == {
        "job_data_err": "",
        "job_data_out": "",
        "job_file": "/path/to/firecrest/submit_upload_job_id/script.sh",
        "job_file_err": "/path/to/firecrest/submit_upload_job_id/slurm-35342667.out",
        "job_file_out": "/path/to/firecrest/submit_upload_job_id/slurm-35342667.out",
        "jobid": 35342667,
        "result": "Job submitted",
    }


def test_submit_invalid_arguments(valid_client, non_slurm_script):
    with pytest.raises(firecrest.HeaderException):
        valid_client.submit(
            machine="cluster1",
            job_script="/path/to/non/existent/file",
            local_file=False,
        )

    global submit_path_retry
    submit_path_retry = 0
    with pytest.raises(firecrest.FirecrestException):
        valid_client.submit(
            machine="cluster1",
            job_script="/path/to/non/slurm/file.sh",
            local_file=False,
        )

    global submit_upload_retry
    submit_upload_retry = 0

    with pytest.raises(firecrest.FirecrestException):
        valid_client.submit(
            machine="cluster1", job_script=non_slurm_script, local_file=True
        )


def test_submit_invalid_machine(valid_client, slurm_script):
    with pytest.raises(firecrest.HeaderException):
        valid_client.submit(
            machine="cluster2", job_script="/path/to/file", local_file=False
        )

    with pytest.raises(firecrest.HeaderException):
        valid_client.submit(
            machine="cluster2", job_script=slurm_script, local_file=True
        )


def test_submit_invalid_client(invalid_client, slurm_script):
    with pytest.raises(firecrest.UnauthorizedException):
        invalid_client.submit(
            machine="cluster1", job_script="/path/to/file", local_file=False
        )

    with pytest.raises(firecrest.UnauthorizedException):
        invalid_client.submit(
            machine="cluster1", job_script=slurm_script, local_file=True
        )
