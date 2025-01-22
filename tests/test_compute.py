import common
import json
import pytest
import re
import test_authorisation as auth

from context import firecrest
from firecrest import __app_name__, __version__, cli
from typer.testing import CliRunner
from werkzeug.wrappers import Response
from werkzeug.wrappers import Request


runner = CliRunner()


@pytest.fixture
def valid_client(fc_server):
    class ValidAuthorization:
        def get_access_token(self):
            return "VALID_TOKEN"

    client = firecrest.v1.Firecrest(
        firecrest_url=fc_server.url_for("/"), authorization=ValidAuthorization()
    )
    client.polling_sleep_times = [0, 0, 0]
    client.set_api_version("1.16.0")
    return client


@pytest.fixture
def valid_credentials(fc_server, auth_server):
    return [
        f"--firecrest-url={fc_server.url_for('/')}",
        "--client-id=valid_id",
        "--client-secret=valid_secret",
        f"--token-url={auth_server.url_for('/auth/token')}",
        "--api-version=1.16.0",
    ]


@pytest.fixture
def invalid_client(fc_server):
    class InvalidAuthorization:
        def get_access_token(self):
            return "INVALID_TOKEN"

    client = firecrest.v1.Firecrest(
        firecrest_url=fc_server.url_for("/"), authorization=InvalidAuthorization()
    )
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


def submit_path_handler(request: Request):
    if request.headers["Authorization"] != "Bearer VALID_TOKEN":
        return Response(
            json.dumps({"message": "Bad token; invalid JSON"}),
            status=401,
            content_type="application/json",
        )

    if request.headers["X-Machine-Name"] != "cluster1":
        return Response(
            json.dumps(
                {
                    "description": "Failed to submit job",
                    "error": "Machine does not exist",
                }
            ),
            status=400,
            headers={"X-Machine-Does-Not-Exist": "Machine does not exist"},
            content_type="application/json",
        )

    target_path = request.form["targetPath"]
    account = request.form.get("account", None)
    extra_headers = None
    if target_path == "/path/to/workdir/script.sh":
        if account is None:
            ret = {
                "success": "Task created",
                "task_id": "submit_path_job_id_default_account",
                "task_url": "TASK_IP/tasks/submit_path_job_id_default_account",
            }
            status_code = 201
        else:
            ret = {
                "success": "Task created",
                "task_id": f"submit_path_job_id_{account}_account",
                "task_url": f"TASK_IP/tasks/submit_path_job_id_{account}_account",
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
        extra_headers = {"X-Invalid-Path": f"{target_path} is an invalid path."}
        ret = {"description": "Failed to submit job"}
        status_code = 400

    return Response(
        json.dumps(ret),
        status=status_code,
        headers=extra_headers,
        content_type="application/json",
    )


def nodes_request_handler(request: Request):
    if not request.query_string or request.query_string == b"nodes=nid001":
        ret = {
            "success": "Task created",
            "task_id": "nodes_info",
            "task_url": "/tasks/nodes_info",
        }
        status_code = 200

    if request.query_string == b"nodes=nidunknown":
        ret = {
            "success": "Task created",
            "task_id": "info_unknown_node",
            "task_url": "/tasks/info_unknown_node",
        }
        status_code = 200

    return Response(
        json.dumps(ret), status=status_code, content_type="application/json"
    )


def reservations_request_handler(request: Request):
    if not request.query_string or request.query_string == b"reservations=res01":
        ret = {
            "success": "Task created",
            "task_id": "reservations_info",
            "task_url": "/tasks/reservations_info",
        }
        status_code = 200

    if request.query_string == b"reservations=invalid_res":
        ret = {
            "success": "Task created",
            "task_id": "info_unknown_reservations",
            "task_url": "/tasks/info_unknown_reservations",
        }
        status_code = 200

    return Response(
        json.dumps(ret), status=status_code, content_type="application/json"
    )


def partitions_request_handler(request: Request):
    if (
        not request.query_string or
        request.query_string == b'partitions=part01%2Cpart02%2Cxfer'
    ):
        ret = {
            "success": "Task created",
            "task_id": "partitions_info",
            "task_url": "/tasks/partitions_info"
        }
        status_code = 200

    if request.query_string == b'partitions=invalid_part':
        ret = {
            "success": "Task created",
            "task_id": "info_unknown_partition",
            "task_url": "/tasks/info_unknown_partition"
        }
        status_code = 200

    return Response(
        json.dumps(ret), status=status_code, content_type="application/json"
    )


def submit_upload_handler(request: Request):
    if request.headers["Authorization"] != "Bearer VALID_TOKEN":
        return Response(
            json.dumps({"message": "Bad token; invalid JSON"}),
            status=401,
            content_type="application/json",
        )

    if request.headers["X-Machine-Name"] != "cluster1":
        return Response(
            json.dumps(
                {
                    "description": "Failed to submit job",
                    "error": "Machine does not exist",
                }
            ),
            status=400,
            headers={"X-Machine-Does-Not-Exist": "Machine does not exist"},
            content_type="application/json",
        )

    extra_headers = None
    if request.files["file"].filename == "script.sh":
        if b"#!/bin/bash -l\n" in request.files["file"].read():
            if request.form.get("account", None) == "proj":
                ret = {
                    "success": "Task created",
                    "task_id": "submit_upload_job_id_proj_account",
                    "task_url": "TASK_IP/tasks/submit_upload_job_id_proj_account",
                }
            else:
                ret = {
                    "success": "Task created",
                    "task_id": "submit_upload_job_id_default_account",
                    "task_url": "TASK_IP/tasks/submit_upload_job_id_default_account",
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
        extra_headers = {"X-Invalid-Path": f"path is an invalid path."}
        ret = {"description": "Failed to submit job"}
        status_code = 400

    return Response(
        json.dumps(ret),
        status=status_code,
        headers=extra_headers,
        content_type="application/json",
    )


def queue_handler(request: Request):
    if request.headers["Authorization"] != "Bearer VALID_TOKEN":
        return Response(
            json.dumps({"message": "Bad token; invalid JSON"}),
            status=401,
            content_type="application/json",
        )

    if request.headers["X-Machine-Name"] != "cluster1":
        return Response(
            json.dumps(
                {
                    "description": "Failed to retrieve jobs information",
                    "error": "Machine does not exists",
                }
            ),
            status=400,
            headers={"X-Machine-Does-Not-Exist": "Machine does not exist"},
            content_type="application/json",
        )

    jobs = request.args.get("jobs", "").split(",")
    if jobs == [""]:
        ret = {
            "success": "Task created",
            "task_id": "queue_full_id",
            "task_url": "TASK_IP/tasks/queue_full_id",
        }
        status_code = 200
    elif set(jobs) == {"352", "2", "334"}:
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

    return Response(
        json.dumps(ret), status=status_code, content_type="application/json"
    )


def sacct_handler(request: Request):
    if request.headers["Authorization"] != "Bearer VALID_TOKEN":
        return Response(
            json.dumps({"message": "Bad token; invalid JSON"}),
            status=401,
            content_type="application/json",
        )

    if request.headers["X-Machine-Name"] != "cluster1":
        return Response(
            json.dumps(
                {
                    "description": "Failed to retrieve account information",
                    "error": "Machine does not exist",
                }
            ),
            status=400,
            headers={"X-Machine-Does-Not-Exist": "Machine does not exist"},
            content_type="application/json",
        )

    jobs = request.args.get("jobs", "").split(",")
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
    elif set(jobs) == {"352", "2", "334"}:
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

    return Response(
        json.dumps(ret), status=status_code, content_type="application/json"
    )


def cancel_handler(request: Request):
    if request.headers["Authorization"] != "Bearer VALID_TOKEN":
        return Response(
            json.dumps({"message": "Bad token; invalid JSON"}),
            status=401,
            content_type="application/json",
        )

    if request.headers["X-Machine-Name"] != "cluster1":
        return Response(
            json.dumps(
                {
                    "description": "Failed to delete job",
                    "error": "Machine does not exist",
                }
            ),
            status=400,
            headers={"X-Machine-Does-Not-Exist": "Machine does not exist"},
            content_type="application/json",
        )

    uri = request.url
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

    return Response(
        json.dumps(ret), status=status_code, content_type="application/json"
    )


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


def tasks_handler(request: Request):
    if request.headers["Authorization"] != "Bearer VALID_TOKEN":
        return Response(
            json.dumps({"message": "Bad token; invalid JSON"}),
            status=401,
            content_type="application/json",
        )

    global submit_path_retry
    global submit_upload_retry
    global acct_retry
    global queue_retry
    global cancel_retry

    taskid = request.args.get("tasks")
    if taskid in (
        "submit_path_job_id_default_account",
        "submit_path_job_id_proj_account",
        "submit_path_job_id_fail",
    ):
        if submit_path_retry < submit_path_result:
            submit_path_retry += 1
            ret = {
                "tasks": {
                    taskid: {
                        "data": "https://127.0.0.1:5003",
                        "description": "Queued",
                        "hash_id": taskid,
                        "last_modify": "2021-12-04T11:52:10",
                        "service": "compute",
                        "status": "100",
                        "system": "cluster1",
                        "task_id": taskid,
                        "task_url": f"TASK_IP/tasks/{taskid}",
                        "user": "username",
                    }
                }
            }
            status_code = 200
        elif taskid in (
            "submit_path_job_id_default_account",
            "submit_path_job_id_proj_account",
        ):
            jobid = (
                35335405 if taskid == "submit_path_job_id_default_account" else 35335406
            )
            ret = {
                "tasks": {
                    taskid: {
                        "data": {
                            "job_data_err": "",
                            "job_data_out": "",
                            "job_file": "/path/to/workdir/script.sh",
                            "job_file_err": "/path/to/workdir/slurm-35335405.out",
                            "job_file_out": "/path/to/workdir/slurm-35335405.out",
                            "jobid": jobid,
                            "result": "Job submitted",
                        },
                        "description": "Finished successfully",
                        "hash_id": taskid,
                        "last_modify": "2021-12-04T11:52:11",
                        "service": "compute",
                        "status": "200",
                        "system": "cluster1",
                        "task_id": taskid,
                        "task_url": f"TASK_IP/tasks/{taskid}",
                        "user": "username",
                    }
                }
            }
            status_code = 200
        else:
            ret = {
                "tasks": {
                    taskid: {
                        "data": "sbatch: error: This does not look like a batch script...",
                        "description": "Finished with errors",
                        "hash_id": "taskid",
                        "last_modify": "2021-12-04T11:52:11",
                        "service": "compute",
                        "status": "400",
                        "system": "cluster1",
                        "task_id": taskid,
                        "task_url": f"TASK_IP/tasks/{taskid}",
                        "user": "username",
                    }
                }
            }
            status_code = 200
    elif taskid in (
        "submit_upload_job_id_default_account",
        "submit_upload_job_id_proj_account",
        "submit_upload_job_id_fail",
    ):
        if submit_upload_retry < submit_upload_result:
            submit_upload_retry += 1
            ret = {
                "tasks": {
                    taskid: {
                        "data": "Queued",
                        "description": "Queued",
                        "hash_id": taskid,
                        "last_modify": "2021-12-04T11:52:10",
                        "service": "compute",
                        "status": "100",
                        "system": "cluster1",
                        "task_id": taskid,
                        "task_url": f"TASK_IP/tasks/{taskid}",
                        "user": "username",
                    }
                }
            }
            status_code = 200
        elif taskid in (
            "submit_upload_job_id_default_account",
            "submit_upload_job_id_proj_account",
        ):
            jobid = (
                35342667
                if taskid == "submit_upload_job_id_default_account"
                else 35342668
            )
            ret = {
                "tasks": {
                    taskid: {
                        "data": {
                            "job_data_err": "",
                            "job_data_out": "",
                            "job_file": f"/path/to/firecrest/{taskid}/script.sh",
                            "job_file_err": f"/path/to/firecrest/{taskid}/slurm-35342667.out",
                            "job_file_out": f"/path/to/firecrest/{taskid}/slurm-35342667.out",
                            "jobid": jobid,
                            "result": "Job submitted",
                        },
                        "description": "Finished successfully",
                        "hash_id": taskid,
                        "last_modify": "2021-12-04T11:52:11",
                        "service": "compute",
                        "status": "200",
                        "system": "cluster1",
                        "task_id": taskid,
                        "task_url": f"TASK_IP/tasks/{taskid}",
                        "user": "username",
                    }
                }
            }
            status_code = 200
        else:
            ret = {
                "tasks": {
                    taskid: {
                        "data": "sbatch: error: This does not look like a batch script...",
                        "description": "Finished with errors",
                        "hash_id": taskid,
                        "last_modify": "2021-12-04T11:52:11",
                        "service": "compute",
                        "status": "400",
                        "system": "cluster1",
                        "task_id": taskid,
                        "task_url": f"TASK_IP/tasks/{taskid}",
                        "user": "username",
                    }
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
                "tasks": {
                    taskid: {
                        "data": "Queued",
                        "description": "Queued",
                        "hash_id": taskid,
                        "last_modify": "2021-12-04T11:52:10",
                        "service": "compute",
                        "status": "100",
                        "system": "cluster1",
                        "task_id": taskid,
                        "task_url": f"TASK_IP/tasks/{taskid}",
                        "user": "username",
                    }
                }
            }
            status_code = 200
        elif taskid == "acct_352_2_334_id":
            ret = {
                "tasks": {
                    taskid: {
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
                        "system": "cluster1",
                        "task_id": taskid,
                        "task_url": f"TASK_IP/tasks/{taskid}",
                        "user": "username",
                    }
                }
            }
            status_code = 200
        elif taskid == "acct_full_id":
            ret = {
                "tasks": {
                    taskid: {
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
                        "system": "cluster1",
                        "task_id": taskid,
                        "task_url": f"TASK_IP/tasks/{taskid}",
                        "user": "username",
                    }
                }
            }
            status_code = 200
        elif taskid == "acct_empty_id":
            ret = {
                "tasks": {
                    taskid: {
                        "data": {},
                        "description": "Finished successfully",
                        "hash_id": taskid,
                        "last_modify": "2021-12-06T09:53:48",
                        "service": "compute",
                        "status": "200",
                        "system": "cluster1",
                        "task_id": taskid,
                        "task_url": f"TASK_IP/tasks/{taskid}",
                        "user": "username",
                    }
                }
            }
            status_code = 200
        else:
            ret = {
                "tasks": {
                    taskid: {
                        "data": "sacct: fatal: Bad job/step specified: l",
                        "description": "Finished with errors",
                        "hash_id": taskid,
                        "last_modify": "2021-12-06T09:47:22",
                        "service": "compute",
                        "status": "400",
                        "system": "cluster1",
                        "task_id": taskid,
                        "task_url": f"TASK_IP/tasks/{taskid}",
                        "user": "username",
                    }
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
                "tasks": {
                    taskid: {
                        "data": "Queued",
                        "description": "Queued",
                        "hash_id": taskid,
                        "last_modify": "2021-12-04T11:52:10",
                        "service": "compute",
                        "status": "100",
                        "system": "cluster1",
                        "task_id": taskid,
                        "task_url": f"TASK_IP/tasks/{taskid}",
                        "user": "username",
                    }
                }
            }
            status_code = 200
        elif taskid == "queue_352_2_334_id":
            ret = {
                "tasks": {
                    taskid: {
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
                        "system": "cluster1",
                        "task_id": taskid,
                        "task_url": f"TASK_IP/tasks/{taskid}",
                        "user": "username",
                    }
                }
            }
            status_code = 200
        elif taskid == "queue_full_id":
            ret = {
                "tasks": {
                    taskid: {
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
                        "system": "cluster1",
                        "task_id": taskid,
                        "task_url": f"TASK_IP/tasks/{taskid}",
                        "user": "username",
                    }
                }
            }
            status_code = 200
        else:
            ret = {
                "tasks": {
                    taskid: {
                        "data": "slurm_load_jobs error: Invalid job id specified",
                        "description": "Finished with errors",
                        "hash_id": taskid,
                        "last_modify": "2021-12-06T09:47:22",
                        "service": "compute",
                        "status": "400",
                        "system": "cluster1",
                        "task_id": taskid,
                        "task_url": f"TASK_IP/tasks/{taskid}",
                        "user": "username",
                    }
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
                "tasks": {
                    taskid: {
                        "data": "Queued",
                        "description": "Queued",
                        "hash_id": taskid,
                        "last_modify": "2021-12-04T11:52:10",
                        "service": "compute",
                        "status": "100",
                        "system": "cluster1",
                        "task_id": taskid,
                        "task_url": f"TASK_IP/tasks/{taskid}",
                        "user": "username",
                    }
                }
            }
            status_code = 200
        elif taskid == "cancel_job_id":
            ret = {
                "tasks": {
                    taskid: {
                        "data": "",
                        "description": "Finished successfully",
                        "hash_id": taskid,
                        "last_modify": "2021-12-06T10:42:06",
                        "service": "compute",
                        "status": "200",
                        "system": "cluster1",
                        "task_id": taskid,
                        "task_url": f"TASK_IP/tasks/{taskid}",
                        "user": "username",
                    }
                }
            }
            status_code = 200
        elif taskid == "cancel_job_id_permission_fail":
            ret = {
                "tasks": {
                    taskid: {
                        "data": "User does not have permission to cancel job",
                        "description": "Finished with errors",
                        "hash_id": taskid,
                        "last_modify": "2021-12-06T10:32:26",
                        "service": "compute",
                        "status": "400",
                        "system": "cluster1",
                        "task_id": taskid,
                        "task_url": f"TASK_IP/tasks/{taskid}",
                        "user": "username",
                    }
                }
            }
            status_code = 200
        else:
            ret = {
                "tasks": {
                    taskid: {
                        "data": "scancel: error: Invalid job id tg",
                        "description": "Finished with errors",
                        "hash_id": taskid,
                        "last_modify": "2021-12-06T10:39:47",
                        "service": "compute",
                        "status": "400",
                        "system": "cluster1",
                        "task_id": taskid,
                        "task_url": f"TASK_IP/tasks/{taskid}",
                        "user": "username",
                    }
                }
            }
            status_code = 200
    elif taskid == "nodes_info":
        ret = {
            "tasks": {
                taskid: {
                    "created_at": "2024-04-16T09:47:06",
                    "data": [
                        {
                            "ActiveFeatures": ["f7t"],
                            "NodeName": "nid001",
                            "Partitions": [
                                "part01",
                                "part02",
                            ],
                            "State": ["IDLE"],
                        }
                    ],
                    "description": "Finished successfully",
                    "hash_id": "nodes_info",
                    "last_modify": "2024-04-16T09:47:06",
                    "service": "compute",
                    "status": "200",
                    "system": "cluster1",
                    "task_id": "nodes_info",
                    "task_url": "/tasks/nodes_info",
                    "updated_at": "2024-04-16T09:47:06",
                    "user": "service-account-firecrest-sample",
                }
            }
        }
        status_code = 200
    elif taskid == "info_unknown_node":
        ret = {
            "tasks": {
                taskid: {
                    "created_at": "2024-04-16T09:56:14",
                    "data": "Node nidunknown not found",
                    "description": "Finished with errors",
                    "hash_id": "info_unknown_node",
                    "last_modify": "2024-04-16T09:56:14",
                    "service": "compute",
                    "status": "400",
                    "system": "cluster1",
                    "task_id": "info_unknown_node",
                    "task_url": "/tasks/info_unknown_node",
                    "updated_at": "2024-04-16T09:56:14",
                    "user": "service-account-firecrest-sample",
                }
            }
        }
        status_code = 200
    elif taskid == "partitions_info":
        ret = {
            "tasks": {
                taskid: {
                    "created_at": "2024-04-24T12:02:25",
                    "data": [
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
                    ],
                    "description": "Finished successfully",
                    "hash_id": taskid,
                    "last_modify": "2024-04-24T12:02:25",
                    "service": "compute",
                    "status": "200",
                    "system": "cluster1",
                    "task_id": taskid,
                    "task_url": f"/tasks/{taskid}",
                    "updated_at": "2024-04-24T12:02:25",
                    "user": "service-account-firecrest-sample"
                }
            }
        }
        status_code = 200
    elif taskid == "info_unknown_partition":
        ret = {
            "tasks": {
                taskid: {
                    "created_at": "2024-04-24T12:17:13",
                    "data": "Partition invalid_part not found",
                    "description": "Finished with errors",
                    "hash_id": taskid,
                    "last_modify": "2024-04-24T12:02:25",
                    "service": "compute",
                    "status": "400",
                    "system": "cluster1",
                    "task_id": taskid,
                    "task_url": f"/tasks/{taskid}",
                    "updated_at": "2024-04-24T12:17:14",
                    "user": "service-account-firecrest-sample"
                }
            }
        }
        status_code = 200
    elif taskid == "reservations_info":
        ret = {
            "tasks": {
                taskid: {
                    "created_at": "2024-04-24T12:02:25",
                    "data": [
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
                    ],
                    "description": "Finished successfully",
                    "hash_id": taskid,
                    "last_modify": "2024-04-24T12:02:25",
                    "service": "compute",
                    "status": "200",
                    "system": "cluster1",
                    "task_id": taskid,
                    "task_url": f"/tasks/{taskid}",
                    "updated_at": "2024-04-24T12:02:25",
                    "user": "service-account-firecrest-sample",
                }
            }
        }
        status_code = 200
    elif taskid == "info_unknown_reservations":
        ret = {
            "tasks": {
                taskid: {
                    "created_at": "2024-04-24T12:17:13",
                    "data": [],
                    "description": "Finished with errors",
                    "hash_id": taskid,
                    "last_modify": "2024-04-24T12:02:25",
                    "service": "compute",
                    "status": "200",
                    "system": "cluster1",
                    "task_id": taskid,
                    "task_url": f"/tasks/{taskid}",
                    "updated_at": "2024-04-24T12:17:14",
                    "user": "service-account-firecrest-sample",
                }
            }
        }
        status_code = 200

    return Response(
        json.dumps(ret), status=status_code, content_type="application/json"
    )


@pytest.fixture
def fc_server(httpserver):
    httpserver.expect_request("/compute/jobs/path", method="POST").respond_with_handler(
        submit_path_handler
    )

    httpserver.expect_request(
        "/compute/jobs/upload", method="POST"
    ).respond_with_handler(submit_upload_handler)

    httpserver.expect_request("/compute/acct", method="GET").respond_with_handler(
        sacct_handler
    )

    httpserver.expect_request("/compute/jobs", method="GET").respond_with_handler(
        queue_handler
    )

    httpserver.expect_request(
        re.compile("^/compute/jobs.*"), method="DELETE"
    ).respond_with_handler(cancel_handler)

    httpserver.expect_request("/tasks", method="GET").respond_with_handler(
        tasks_handler
    )

    httpserver.expect_request("/compute/nodes", method="GET").respond_with_handler(
        nodes_request_handler
    )

    httpserver.expect_request(
        "/compute/reservations", method="GET"
    ).respond_with_handler(reservations_request_handler)

    httpserver.expect_request(
        "/compute/partitions", method="GET"
    ).respond_with_handler(partitions_request_handler)

    return httpserver


@pytest.fixture
def auth_server(httpserver):
    httpserver.expect_request("/auth/token").respond_with_handler(auth.auth_handler)
    return httpserver


def test_submit_remote(valid_client):
    global submit_path_retry
    submit_path_retry = 0
    assert valid_client.submit(
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
    assert valid_client.submit(
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


def test_cli_submit_remote(valid_credentials):
    global submit_path_retry
    submit_path_retry = 0
    args = valid_credentials + [
        "submit",
        "--system",
        "cluster1",
        "/path/to/workdir/script.sh",
        "--no-local",
    ]
    result = runner.invoke(cli.app, args=args)
    stdout = common.clean_stdout(result.stdout)
    assert result.exit_code == 0
    assert '"jobid": 35335405' in stdout
    assert '"result": "Job submitted"' in stdout

    submit_path_retry = 0
    args = valid_credentials + [
        "submit",
        "--system",
        "cluster1",
        "/path/to/workdir/script.sh",
        "--no-local",
        "--account=proj",
    ]
    result = runner.invoke(cli.app, args=args)
    stdout = common.clean_stdout(result.stdout)
    assert result.exit_code == 0
    assert '"jobid": 35335406' in stdout
    assert '"result": "Job submitted"' in stdout


def test_submit_local(valid_client, slurm_script):
    # Test submission for local script
    global submit_upload_retry
    submit_upload_retry = 0
    assert valid_client.submit(
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
    assert valid_client.submit(
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


def test_cli_submit_local(valid_credentials, slurm_script):
    global submit_upload_retry
    submit_upload_retry = 0
    args = valid_credentials + ["submit", "--system", "cluster1", str(slurm_script)]
    result = runner.invoke(cli.app, args=args)
    stdout = common.clean_stdout(result.stdout)
    assert result.exit_code == 0
    assert '"jobid": 35342667' in stdout
    assert '"result": "Job submitted"' in stdout

    submit_upload_retry = 0
    args = valid_credentials + [
        "submit",
        "--system",
        "cluster1",
        str(slurm_script),
        "--account=proj",
    ]
    result = runner.invoke(cli.app, args=args)
    stdout = common.clean_stdout(result.stdout)
    assert result.exit_code == 0
    assert '"jobid": 35342668' in stdout
    assert '"result": "Job submitted"' in stdout


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
        "--system",
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
    args = valid_credentials + ["poll", "--system", "cluster1"]
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
    args = valid_credentials + [
        "poll-active",
        "--system",
        "cluster1",
        "352",
        "2",
        "334",
    ]
    result = runner.invoke(cli.app, args=args)
    stdout = common.clean_stdout(result.stdout)
    assert result.exit_code == 0
    assert "Information about jobs in the queue" in stdout
    assert "352" in stdout

    queue_retry = 0
    args = valid_credentials + ["poll-active", "--system", "cluster1"]
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
    args = valid_credentials + ["cancel", "--system", "cluster1", "35360071"]
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


def test_get_nodes(valid_client):
    response = [
        {
            "ActiveFeatures": ["f7t"],
            "NodeName": "nid001",
            "Partitions": ["part01", "part02"],
            "State": ["IDLE"],
        }
    ]
    assert valid_client.nodes(machine="cluster1") == response


def test_get_nodes_not_impl(valid_client):
    valid_client.set_api_version("1.15.0")
    with pytest.raises(firecrest.NotImplementedOnAPIversion):
        valid_client.nodes(machine="cluster1")


def test_get_nodes_from_list(valid_client):
    response = [
        {
            "ActiveFeatures": ["f7t"],
            "NodeName": "nid001",
            "Partitions": ["part01", "part02"],
            "State": ["IDLE"],
        }
    ]
    assert valid_client.nodes(machine="cluster1", nodes=["nid001"]) == response


def test_get_nodes_from_list_not_impl(valid_client):
    valid_client.set_api_version("1.15.0")
    with pytest.raises(firecrest.NotImplementedOnAPIversion):
        valid_client.nodes(machine="cluster1", nodes=["nid001"])


def test_get_nodes_unknown(valid_client):
    with pytest.raises(firecrest.FirecrestException):
        valid_client.nodes(machine="cluster1", nodes=["nidunknown"])


def test_cli_get_nodes(valid_credentials):
    args = valid_credentials + ["get-nodes", "--system", "cluster1", "nid001"]
    result = runner.invoke(cli.app, args=args)
    stdout = common.clean_stdout(result.stdout)
    assert result.exit_code == 0
    assert "Information about jobs in the queue" in stdout
    assert "nid001" in stdout
    assert "part01, part02" in stdout
    assert "IDLE" in stdout
    assert "f7t" in stdout


def test_get_partitions(valid_client):
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
    assert valid_client.partitions(machine="cluster1") == response


def test_get_partitions_not_impl(valid_client):
    valid_client.set_api_version("1.15.0")
    with pytest.raises(firecrest.NotImplementedOnAPIversion):
        valid_client.partitions(machine="cluster1")


def test_get_partitions_from_list(valid_client):
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
    assert valid_client.partitions(
        machine="cluster1", partitions=["part01", "part02", "xfer"]
    ) == response


def test_get_partitions_from_list_not_impl(valid_client):
    valid_client.set_api_version("1.15.0")
    with pytest.raises(firecrest.NotImplementedOnAPIversion):
        valid_client.partitions(
            machine="cluster1", partitions=["part01", "part02", "xfer"]
        )


def test_get_partitions_unknown(valid_client):
    with pytest.raises(firecrest.FirecrestException):
        valid_client.partitions(
            machine="cluster1",
            partitions=["invalid_part"]
        )


def test_cli_get_partitions(valid_credentials):
    args = (
        valid_credentials +
        ["get-partitions", "--system", "cluster1"]
    )
    result = runner.invoke(cli.app, args=args)
    stdout = common.clean_stdout(result.stdout)
    assert result.exit_code == 0
    assert "Information about partitions in the system" in stdout
    assert "part01" in stdout
    assert "part02" in stdout
    assert "UP" in stdout


def test_get_reservations(valid_client):
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
    assert valid_client.reservations(machine="cluster1") == response


def test_get_reservations_not_impl(valid_client):
    valid_client.set_api_version("1.15.0")
    with pytest.raises(firecrest.NotImplementedOnAPIversion):
        valid_client.reservations(machine="cluster1")


def test_cli_get_reservations(valid_credentials):
    args = valid_credentials + ["get-reservations", "--system", "cluster1"]
    result = runner.invoke(cli.app, args=args)
    stdout = common.clean_stdout(result.stdout)
    assert result.exit_code == 0
    assert "Information about reservations in the system" in stdout
