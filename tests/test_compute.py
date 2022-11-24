import common
import httpretty
import json
import pytest
import re
import test_authoriation as auth

from context import firecrest
from firecrest import __app_name__, __version__, cli
from typer.testing import CliRunner


runner = CliRunner()


@pytest.fixture
def valid_client():
    class ValidAuthorization:
        def get_access_token(self):
            return "VALID_TOKEN"

    return firecrest.Firecrest(
        firecrest_url="http://firecrest.cscs.ch", authorization=ValidAuthorization()
    )


@pytest.fixture
def valid_credentials():
    return [
        "--firecrest-url=http://firecrest.cscs.ch",
        "--client-id=valid_id",
        "--client-secret=valid_secret",
        "--token-url=https://myauth.com/auth/realms/cscs/protocol/openid-connect/token",
    ]


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
            "task_url": "TASK_IP/tasks/submit_path_job_id",
        }
        status_code = 201
    elif target_path == "/path/to/non/slurm/file.sh":
        ret = {
            "success": "Task created",
            "task_id": "submit_path_job_id_fail",
            "task_url": "TASK_IP/tasks/submit_path_job_id_fail",
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
                "task_url": "TASK_IP/tasks/submit_upload_job_id",
            }
            status_code = 201
        else:
            ret = {
                "success": "Task created",
                "task_id": "submit_upload_job_id_fail",
                "task_url": "TASK_IP/tasks/submit_upload_job_id_fail",
            }
            status_code = 201
    else:
        response_headers["X-Invalid-Path"] = f"path is an invalid path."
        ret = {"description": "Failed to submit job"}
        status_code = 400

    return [status_code, response_headers, json.dumps(ret)]


def queue_callback(request, uri, response_headers):
    if request.headers["Authorization"] != "Bearer VALID_TOKEN":
        return [401, response_headers, '{"message": "Bad token; invalid JSON"}']

    if request.headers["X-Machine-Name"] != "cluster1":
        response_headers["X-Machine-Does-Not-Exist"] = "Machine does not exist"
        return [
            400,
            response_headers,
            '{ "description": "Failed to retrieve jobs information", "error": "Machine does not exists"}',
        ]

    jobs = request.querystring.get("jobs", [""])[0].split(",")
    if jobs == [""]:
        ret = {
            "success": "Task created",
            "task_id": "queue_full_id",
            "task_url": "TASK_IP/tasks/queue_full_id",
        }
        status_code = 200
    elif jobs == ["352", "2", "334"]:
        ret = {
            "success": "Task created",
            "task_id": "queue_352_2_334_id",
            "task_url": "TASK_IP/tasks/queue_352_2_334_id",
        }
        status_code = 200
    elif jobs == ["l"]:
        ret = {
            "description": "Failed to retrieve job information",
            "error": "l is not a valid job ID",
        }
        status_code = 400
    elif jobs == ["4"]:
        ret = {
            "success": "Task created",
            "task_id": "queue_id_fail",
            "task_url": "TASK_IP/tasks/queue_id_fail",
        }
        status_code = 200

    return [status_code, response_headers, json.dumps(ret)]


def sacct_callback(request, uri, response_headers):
    if request.headers["Authorization"] != "Bearer VALID_TOKEN":
        return [401, response_headers, '{"message": "Bad token; invalid JSON"}']

    if request.headers["X-Machine-Name"] != "cluster1":
        response_headers["X-Machine-Does-Not-Exist"] = "Machine does not exist"
        return [
            400,
            response_headers,
            '{"description": "Failed to retrieve account information", "error": "Machine does not exist"}',
        ]

    jobs = request.querystring.get("jobs", [""])[0].split(",")
    if jobs == [""]:
        ret = {
            "success": "Task created",
            "task_id": "acct_full_id",
            "task_url": "TASK_IP/tasks/acct_full_id",
        }
        status_code = 200
    elif jobs == ["empty"]:
        ret = {
            "success": "Task created",
            "task_id": "acct_empty_id",
            "task_url": "TASK_IP/tasks/acct_empty_id",
        }
        status_code = 200
    elif jobs == ["352", "2", "334"]:
        ret = {
            "success": "Task created",
            "task_id": "acct_352_2_334_id",
            "task_url": "TASK_IP/tasks/acct_352_2_334_id",
        }
        status_code = 200
    elif jobs == ["l"]:
        ret = {
            "success": "Task created",
            "task_id": "acct_352_2_334_id_fail",
            "task_url": "TASK_IP/tasks/acct_352_2_334_id_fail",
        }
        status_code = 200

    return [status_code, response_headers, json.dumps(ret)]


def cancel_callback(request, uri, response_headers):
    if request.headers["Authorization"] != "Bearer VALID_TOKEN":
        return [401, response_headers, '{"message": "Bad token; invalid JSON"}']

    if request.headers["X-Machine-Name"] != "cluster1":
        response_headers["X-Machine-Does-Not-Exist"] = "Machine does not exist"
        return [
            400,
            response_headers,
            '{"description": "Failed to delete job", "error": "Machine does not exist"}',
        ]

    jobid = uri.split("/")[-1]
    if jobid == "35360071":
        ret = {
            "success": "Task created",
            "task_id": "cancel_job_id",
            "task_url": "TASK_IP/tasks/cancel_job_id",
        }
        status_code = 200
    elif jobid == "35360072":
        ret = {
            "success": "Task created",
            "task_id": "cancel_job_id_permission_fail",
            "task_url": "TASK_IP/tasks/cancel_job_id_permission_fail",
        }
        status_code = 200
    else:
        ret = {
            "success": "Task created",
            "task_id": "cancel_job_id_fail",
            "task_url": "TASK_IP/tasks/cancel_job_id_fail",
        }
        status_code = 200

    return [status_code, response_headers, json.dumps(ret)]


# Global variables for tasks
submit_path_retry = 0
submit_path_result = 1
submit_upload_retry = 0
submit_upload_result = 1
acct_retry = 0
acct_result = 1
queue_retry = 0
queue_result = 1
cancel_retry = 0
cancel_result = 1


def tasks_callback(request, uri, response_headers):
    if request.headers["Authorization"] != "Bearer VALID_TOKEN":
        return [401, response_headers, '{"message": "Bad token; invalid JSON"}']

    global submit_path_retry
    global submit_upload_retry
    global acct_retry
    global queue_retry
    global cancel_retry

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
                    "task_url": f"TASK_IP/tasks/{taskid}",
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
                    "task_url": "TASK_IP/tasks/submit_path_job_id",
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
                    "task_url": f"TASK_IP/tasks/{taskid}",
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
                    "task_url": f"TASK_IP/tasks/{taskid}",
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
                    "task_url": f"TASK_IP/tasks/{taskid}",
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
                    "task_url": f"TASK_IP/tasks/{taskid}",
                    "user": "username",
                }
            }
            status_code = 200
    elif (
        taskid == "acct_352_2_334_id"
        or taskid == "acct_352_2_334_id_fail"
        or taskid == "acct_full_id"
        or taskid == "acct_empty_id"
    ):
        if acct_retry < acct_result:
            acct_retry += 1
            ret = {
                "task": {
                    "data": "Queued",
                    "description": "Queued",
                    "hash_id": taskid,
                    "last_modify": "2021-12-04T11:52:10",
                    "service": "compute",
                    "status": "100",
                    "task_id": taskid,
                    "task_url": f"TASK_IP/tasks/{taskid}",
                    "user": "username",
                }
            }
            status_code = 200
        elif taskid == "acct_352_2_334_id":
            ret = {
                "task": {
                    "data": [
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
                    ],
                    "description": "Finished successfully",
                    "hash_id": taskid,
                    "last_modify": "2021-12-06T09:53:48",
                    "service": "compute",
                    "status": "200",
                    "task_id": taskid,
                    "task_url": f"TASK_IP/tasks/{taskid}",
                    "user": "username",
                }
            }
            status_code = 200
        elif taskid == "acct_full_id":
            ret = {
                "task": {
                    "data": [
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
                    ],
                    "description": "Finished successfully",
                    "hash_id": taskid,
                    "last_modify": "2021-12-06T09:53:48",
                    "service": "compute",
                    "status": "200",
                    "task_id": taskid,
                    "task_url": f"TASK_IP/tasks/{taskid}",
                    "user": "username",
                }
            }
            status_code = 200
        elif taskid == "acct_empty_id":
            ret = {
                "task": {
                    "data": {},
                    "description": "Finished successfully",
                    "hash_id": taskid,
                    "last_modify": "2021-12-06T09:53:48",
                    "service": "compute",
                    "status": "200",
                    "task_id": taskid,
                    "task_url": f"TASK_IP/tasks/{taskid}",
                    "user": "username",
                }
            }
            status_code = 200
        else:
            ret = {
                "task": {
                    "data": "sacct: fatal: Bad job/step specified: l",
                    "description": "Finished with errors",
                    "hash_id": taskid,
                    "last_modify": "2021-12-06T09:47:22",
                    "service": "compute",
                    "status": "400",
                    "task_id": taskid,
                    "task_url": f"TASK_IP/tasks/{taskid}",
                    "user": "username",
                }
            }
            status_code = 200
    elif (
        taskid == "queue_352_2_334_id"
        or taskid == "queue_id_fail"
        or taskid == "queue_full_id"
    ):
        if queue_retry < queue_result:
            queue_retry += 1
            ret = {
                "task": {
                    "data": "Queued",
                    "description": "Queued",
                    "hash_id": taskid,
                    "last_modify": "2021-12-04T11:52:10",
                    "service": "compute",
                    "status": "100",
                    "task_id": taskid,
                    "task_url": f"TASK_IP/tasks/{taskid}",
                    "user": "username",
                }
            }
            status_code = 200
        elif taskid == "queue_352_2_334_id":
            ret = {
                "task": {
                    "data": {
                        "0": {
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
                    },
                    "description": "Finished successfully",
                    "hash_id": taskid,
                    "last_modify": "2021-12-06T09:53:48",
                    "service": "compute",
                    "status": "200",
                    "task_id": taskid,
                    "task_url": f"TASK_IP/tasks/{taskid}",
                    "user": "username",
                }
            }
            status_code = 200
        elif taskid == "queue_full_id":
            ret = {
                "task": {
                    "data": {
                        "0": {
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
                        "1": {
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
                    },
                    "description": "Finished successfully",
                    "hash_id": taskid,
                    "last_modify": "2021-12-06T09:53:48",
                    "service": "compute",
                    "status": "200",
                    "task_id": taskid,
                    "task_url": f"TASK_IP/tasks/{taskid}",
                    "user": "username",
                }
            }
            status_code = 200
        else:
            ret = {
                "task": {
                    "data": "slurm_load_jobs error: Invalid job id specified",
                    "description": "Finished with errors",
                    "hash_id": taskid,
                    "last_modify": "2021-12-06T09:47:22",
                    "service": "compute",
                    "status": "400",
                    "task_id": taskid,
                    "task_url": f"TASK_IP/tasks/{taskid}",
                    "user": "username",
                }
            }
            status_code = 200
    elif (
        taskid == "cancel_job_id"
        or taskid == "cancel_job_id_fail"
        or taskid == "cancel_job_id_permission_fail"
    ):
        if cancel_retry < cancel_result:
            cancel_retry += 1
            ret = {
                "task": {
                    "data": "Queued",
                    "description": "Queued",
                    "hash_id": taskid,
                    "last_modify": "2021-12-04T11:52:10",
                    "service": "compute",
                    "status": "100",
                    "task_id": taskid,
                    "task_url": f"TASK_IP/tasks/{taskid}",
                    "user": "username",
                }
            }
            status_code = 200
        elif taskid == "cancel_job_id":
            ret = {
                "task": {
                    "data": "",
                    "description": "Finished successfully",
                    "hash_id": taskid,
                    "last_modify": "2021-12-06T10:42:06",
                    "service": "compute",
                    "status": "200",
                    "task_id": taskid,
                    "task_url": f"TASK_IP/tasks/{taskid}",
                    "user": "username",
                }
            }
            status_code = 200
        elif taskid == "cancel_job_id_permission_fail":
            ret = {
                "task": {
                    "data": "User does not have permission to cancel job",
                    "description": "Finished with errors",
                    "hash_id": taskid,
                    "last_modify": "2021-12-06T10:32:26",
                    "service": "compute",
                    "status": "400",
                    "task_id": taskid,
                    "task_url": f"TASK_IP/tasks/{taskid}",
                    "user": "username",
                }
            }
            status_code = 200
        else:
            ret = {
                "task": {
                    "data": "scancel: error: Invalid job id tg",
                    "description": "Finished with errors",
                    "hash_id": taskid,
                    "last_modify": "2021-12-06T10:39:47",
                    "service": "compute",
                    "status": "400",
                    "task_id": taskid,
                    "task_url": f"TASK_IP/tasks/{taskid}",
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
        "http://firecrest.cscs.ch/compute/jobs/path",
        body=submit_path_callback,
    )

    httpretty.register_uri(
        httpretty.POST,
        "http://firecrest.cscs.ch/compute/jobs/upload",
        body=submit_upload_callback,
    )

    httpretty.register_uri(
        httpretty.GET, "http://firecrest.cscs.ch/compute/acct", body=sacct_callback
    )

    httpretty.register_uri(
        httpretty.GET, "http://firecrest.cscs.ch/compute/jobs", body=queue_callback
    )

    httpretty.register_uri(
        httpretty.DELETE,
        re.compile(r"http:\/\/firecrest\.cscs\.ch\/compute\/jobs.*"),
        body=cancel_callback,
    )

    httpretty.register_uri(
        httpretty.GET,
        re.compile(r"http:\/\/firecrest\.cscs\.ch\/tasks.*"),
        body=tasks_callback,
    )

    httpretty.register_uri(
        httpretty.POST,
        "https://myauth.com/auth/realms/cscs/protocol/openid-connect/token",
        body=auth.auth_callback,
    )

    yield

    httpretty.disable()
    httpretty.reset()


def test_submit_remote(valid_client):
    global submit_path_retry
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


def test_cli_submit_remote(valid_credentials):
    global submit_path_retry
    submit_path_retry = 0
    args = valid_credentials + [
        "submit",
        "cluster1",
        "/path/to/workdir/script.sh",
        "--no-local",
    ]
    result = runner.invoke(cli.app, args=args)
    stdout = common.clean_stdout(result.stdout)
    assert result.exit_code == 0
    assert "'jobid': 35335405" in stdout
    assert "'result': 'Job submitted'" in stdout


def test_submit_local(valid_client, slurm_script):
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


def test_cli_submit_local(valid_credentials, slurm_script):
    global submit_upload_retry
    submit_upload_retry = 0
    args = valid_credentials + ["submit", "cluster1", str(slurm_script)]
    result = runner.invoke(cli.app, args=args)
    stdout = common.clean_stdout(result.stdout)
    assert result.exit_code == 0
    assert "'jobid': 35342667" in stdout
    assert "'result': 'Job submitted'" in stdout


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


def test_poll(valid_client):
    global acct_retry
    acct_retry = 0
    assert valid_client.poll(
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
    assert valid_client.poll(machine="cluster1", jobs=[]) == [
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
    assert valid_client.poll(machine="cluster1", jobs=["empty"]) == []


def test_cli_poll(valid_credentials):
    global acct_retry
    acct_retry = 0
    args = valid_credentials + [
        "poll",
        "cluster1",
        "352",
        "2",
        "334",
        "--start-time=starttime",
        "--end-time=endtime",
    ]
    result = runner.invoke(cli.app, args=args)
    stdout = common.clean_stdout(result.stdout)
    assert result.exit_code == 0
    assert "Accounting data for jobs" in stdout
    assert "352" in stdout
    assert "334" in stdout

    acct_retry = 0
    args = valid_credentials + ["poll", "cluster1"]
    result = runner.invoke(cli.app, args=args)
    stdout = common.clean_stdout(result.stdout)
    assert result.exit_code == 0
    assert "Accounting data for jobs" in stdout
    assert "352" in stdout


def test_poll_invalid_arguments(valid_client):
    global acct_retry
    acct_retry = 0

    with pytest.raises(firecrest.FirecrestException):
        valid_client.poll(machine="cluster1", jobs=["l"])


def test_poll_invalid_machine(valid_client):
    with pytest.raises(firecrest.HeaderException):
        valid_client.poll(machine="cluster2", jobs=[])


def test_poll_invalid_client(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        invalid_client.poll(machine="cluster1", jobs=[])


def test_poll_active(valid_client):
    global queue_retry
    queue_retry = 0
    assert valid_client.poll_active(machine="cluster1", jobs=[352, 2, "334"]) == [
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
    assert valid_client.poll_active(machine="cluster1", jobs=[]) == [
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


def test_cli_poll_active(valid_credentials):
    global queue_retry
    queue_retry = 0
    args = valid_credentials + ["poll-active", "cluster1", "352", "2", "334"]
    result = runner.invoke(cli.app, args=args)
    stdout = common.clean_stdout(result.stdout)
    assert result.exit_code == 0
    assert "Information about jobs in the queue" in stdout
    assert "352" in stdout

    queue_retry = 0
    args = valid_credentials + ["poll-active", "cluster1"]
    result = runner.invoke(cli.app, args=args)
    stdout = common.clean_stdout(result.stdout)
    assert result.exit_code == 0
    assert "Information about jobs in the queue" in stdout
    assert "352" in stdout
    assert "356" in stdout


def test_poll_active_invalid_arguments(valid_client):
    global queue_retry
    queue_retry = 0

    with pytest.raises(firecrest.FirecrestException):
        valid_client.poll_active(machine="cluster1", jobs=["l"])

    queue_retry = 0
    with pytest.raises(firecrest.FirecrestException):
        # We assume that jobid is too old and is rejected by squeue
        valid_client.poll_active(machine="cluster1", jobs=["4"])


def test_poll_active_invalid_machine(valid_client):
    with pytest.raises(firecrest.HeaderException):
        valid_client.poll_active(machine="cluster2", jobs=[])


def test_poll_active_invalid_client(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        invalid_client.poll_active(machine="cluster1", jobs=[])


def test_cancel(valid_client):
    global cancel_retry
    cancel_retry = 0
    # Make sure this doesn't raise an error
    valid_client.cancel(machine="cluster1", job_id=35360071)


def test_cli_cancel(valid_credentials):
    global queue_retry
    queue_retry = 0
    args = valid_credentials + ["cancel", "cluster1", "35360071"]
    result = runner.invoke(cli.app, args=args)
    assert result.exit_code == 0


def test_cancel_invalid_arguments(valid_client):
    global cancel_retry
    cancel_retry = 0
    with pytest.raises(firecrest.FirecrestException):
        valid_client.cancel(machine="cluster1", job_id="k")

    cancel_retry = 0
    with pytest.raises(firecrest.FirecrestException):
        # Jobid 35360072 is from a different user
        valid_client.cancel(machine="cluster1", job_id="35360072")


def test_cancel_invalid_machine(valid_client):
    with pytest.raises(firecrest.HeaderException):
        valid_client.cancel(machine="cluster2", job_id=35360071)


def test_cancel_invalid_client(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        invalid_client.cancel(machine="cluster1", job_id=35360071)
