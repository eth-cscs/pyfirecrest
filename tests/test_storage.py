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


def internal_transfer_handler(request: Request):
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

    ret = {
        "success": "Task created",
        "task_id": "internal_transfer_id",
        "task_url": "TASK_IP/tasks/internal_transfer_id",
    }
    status_code = 201
    return Response(
        json.dumps(ret), status=status_code, content_type="application/json"
    )


def external_download_handler(request: Request):
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
                    "description": "Failed to download file",
                    "error": "Machine does not exist",
                }
            ),
            status=400,
            headers={"X-Machine-Does-Not-Exist": "Machine does not exist"},
            content_type="application/json",
        )

    extra_headers = None
    source_path = request.form.get("sourcePath")
    if source_path == "/path/to/remote/sourcelegacy":
        ret = {
            "success": "Task created",
            "task_id": "external_download_id_legacy",
            "task_url": "TASK_IP/tasks/external_download_id_legacy",
        }
        status_code = 201
    elif source_path == "/path/to/remote/source":
        ret = {
            "success": "Task created",
            "task_id": "external_download_id",
            "task_url": "TASK_IP/tasks/external_download_id",
        }
        status_code = 201
    else:
        extra_headers = {"X-Invalid-Path": "path is an invalid path"}
        ret = {"description": "sourcePath error"}
        status_code = 400

    return Response(
        json.dumps(ret),
        status=status_code,
        headers=extra_headers,
        content_type="application/json",
    )


def external_upload_handler(request: Request):
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
                    "description": "Failed to upload file",
                    "error": "Machine does not exist",
                }
            ),
            status=400,
            headers={"X-Machine-Does-Not-Exist": "Machine does not exist"},
            content_type="application/json",
        )

    source_path = request.form.get("sourcePath")
    target_path = request.form.get("targetPath")
    extra_headers = None
    if (
        source_path == "/path/to/local/source"
        and target_path == "/path/to/remote/destination"
    ):
        ret = {
            "success": "Task created",
            "task_id": "external_upload_id",
            "task_url": "TASK_IP/tasks/external_upload_id",
        }
        status_code = 201
    else:
        extra_headers = {"X-Invalid-Path": "path is an invalid path"}
        ret = {"description": "sourcePath error"}
        status_code = 400

    return Response(
        json.dumps(ret),
        status=status_code,
        headers=extra_headers,
        content_type="application/json",
    )


# Global variables for tasks
internal_transfer_retry = 0
internal_transfer_result = 1
external_upload_retry = 0
external_upload_result = 3
external_download_retry = 0
external_download_result = 0


def storage_tasks_handler(request: Request):
    if request.headers["Authorization"] != "Bearer VALID_TOKEN":
        return Response(
            json.dumps({"message": "Bad token; invalid JSON"}),
            status=401,
            content_type="application/json",
        )

    global internal_transfer_retry
    global external_download_retry
    global external_upload_retry

    taskid = request.args.get("tasks")
    if taskid == "internal_transfer_id":
        if internal_transfer_retry < internal_transfer_result:
            internal_transfer_retry += 1
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
        else:
            ret = {
                "tasks": {
                    taskid: {
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
                        "system": "cluster1",
                        "task_id": "6f514b060ca036917f4194964b6e949c",
                        "task_url": f"TASK_IP/tasks/{taskid}",
                        "user": "username",
                    }
                }
            }
            status_code = 200
    elif taskid == "external_download_id" or taskid == "external_download_id_legacy":
        if external_download_retry < 1:
            external_download_retry += 1
            ret = {
                "tasks": {
                    taskid: {
                        "data": "Queued",
                        "description": "Queued",
                        "hash_id": taskid,
                        "last_modify": "2021-12-04T11:52:10",
                        "service": "storage",
                        "status": "100",
                        "system": "cluster1",
                        "task_id": taskid,
                        "task_url": f"TASK_IP/tasks/{taskid}",
                        "user": "username",
                    }
                }
            }
            status_code = 200
        elif external_download_retry < 2:
            external_download_retry += 1
            ret = {
                "tasks": {
                    taskid: {
                        "data": "Started upload from filesystem to Object Storage",
                        "description": "Started upload from filesystem to Object Storage",
                        "hash_id": taskid,
                        "last_modify": "2021-12-04T11:52:10",
                        "service": "storage",
                        "status": "116",
                        "system": "cluster1",
                        "task_id": taskid,
                        "task_url": f"TASK_IP/tasks/{taskid}",
                        "user": "username",
                    }
                }
            }
            status_code = 200
        elif taskid == "external_download_id_legacy":
            ret = {
                "tasks": {
                    taskid: {
                        "data": "https://object_storage_link_legacy.com",
                        "description": "Started upload from filesystem to Object Storage",
                        "hash_id": taskid,
                        "last_modify": "2021-12-04T11:52:10",
                        "service": "storage",
                        "status": "117",
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
                        "data": {
                            "source": "/path/to/remote/source",
                            "system_name": "machine",
                            "url": "https://object_storage_link.com",
                        },
                        "description": "Started upload from filesystem to Object Storage",
                        "hash_id": taskid,
                        "last_modify": "2021-12-04T11:52:10",
                        "service": "storage",
                        "status": "117",
                        "system": "cluster1",
                        "task_id": taskid,
                        "task_url": f"TASK_IP/tasks/{taskid}",
                        "user": "username",
                    }
                }
            }
            status_code = 200
    elif taskid == "external_upload_id":
        if external_upload_retry < 1:
            external_upload_retry += 1
            ret = {
                "tasks": {
                    taskid: {
                        "data": "Queued",
                        "description": "Queued",
                        "hash_id": taskid,
                        "last_modify": "2021-12-04T11:52:10",
                        "service": "storage",
                        "status": "100",
                        "system": "cluster1",
                        "task_id": taskid,
                        "task_url": f"TASK_IP/tasks/{taskid}",
                        "user": "username",
                    }
                }
            }
            status_code = 200
        elif external_upload_retry < 2:
            external_upload_retry += 1
            ret = {
                "tasks": {
                    taskid: {
                        "created_at": "2022-11-23T09:07:50",
                        "data": {
                            "hash_id": taskid,
                            "msg": "Waiting for Presigned URL to upload file to staging area (OpenStack Swift)",
                            "source": "/path/to/local/source",
                            "status": "110",
                            "system": "cluster1",
                            "system_addr": "machine_addr",
                            "system_name": "cluster1",
                            "target": "/path/to/remote/destination",
                            "trace_id": "trace",
                            "user": "username",
                        },
                        "description": "Waiting for Form URL from Object Storage to be retrieved",
                        "hash_id": taskid,
                        "last_modify": "2022-11-23T09:07:50",
                        "service": "storage",
                        "status": "110",
                        "system": "cluster1",
                        "task_id": "taskid",
                        "task_url": f"TASK_IP/tasks/{taskid}",
                        "updated_at": "2022-11-23T09:07:50",
                        "user": "username",
                    }
                }
            }
            status_code = 200
        else:
            ret = {
                "tasks": {
                    taskid: {
                        "created_at": "2022-11-23T09:18:43",
                        "data": {
                            "hash_id": taskid,
                            "msg": {
                                "command": f"curl --show-error -s -ihttps://object.com/v1/AUTH_auth/username/{taskid}/ -X POST -F max_file_size=536870912000 -F max_file_count=1 -F expires=1671787123 -F signature=sign -F redirect= -F file=@/path/to/local/source ",
                                "parameters": {
                                    "data": {
                                        "expires": 1671787123,
                                        "max_file_count": 1,
                                        "max_file_size": 536870912000,
                                        "redirect": "",
                                        "signature": "sign",
                                    },
                                    "files": "/path/to/local/source",
                                    "headers": {},
                                    "json": {},
                                    "method": "POST",
                                    "params": {},
                                    "url": f"https://object.com/v1/AUTH_auth/username/{taskid}/",
                                },
                            },
                            "source": "/path/to/local/source",
                            "status": "111",
                            "system": "cluster1",
                            "system_addr": "machine_addr",
                            "system_name": "cluster1",
                            "target": "/path/to/remote/destination",
                            "trace_id": "trace",
                            "user": "username",
                        },
                        "description": "Form URL from Object Storage received",
                        "hash_id": taskid,
                        "last_modify": "2022-11-23T09:18:43",
                        "service": "storage",
                        "status": "111",
                        "system": "cluster1",
                        "task_id": taskid,
                        "task_url": f"TASK_IP/tasks/{taskid}",
                        "updated_at": "2022-11-23T09:18:43",
                        "user": "username",
                    }
                }
            }
            status_code = 200

    return Response(
        json.dumps(ret), status=status_code, content_type="application/json"
    )


@pytest.fixture
def fc_server(httpserver):
    httpserver.expect_request(
        re.compile("^/storage/xfer-internal.*"), method="POST"
    ).respond_with_handler(internal_transfer_handler)

    httpserver.expect_request(
        "/tasks", method="GET"
    ).respond_with_handler(storage_tasks_handler)

    httpserver.expect_request(
        "/storage/xfer-external/download", method="POST"
    ).respond_with_handler(external_download_handler)

    httpserver.expect_request(
        "/storage/xfer-external/upload", method="POST"
    ).respond_with_handler(external_upload_handler)

    return httpserver


@pytest.fixture
def auth_server(httpserver):
    httpserver.expect_request("/auth/token").respond_with_handler(auth.auth_handler)
    return httpserver


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
        "system": "cluster1",
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
        "system": "cluster1",
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
        "system": "cluster1",
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
        "system": "cluster1",
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
    global external_download_retry
    external_download_retry = 0
    valid_client.set_api_version("1.14.0")
    obj = valid_client.external_download("cluster1", "/path/to/remote/source")
    assert isinstance(obj, firecrest.v1.ExternalDownload)
    assert obj._task_id == "external_download_id"
    assert obj.client == valid_client


def test_external_download_legacy(valid_client):
    global external_download_retry
    external_download_retry = 0
    valid_client.set_api_version("1.13.0")
    obj = valid_client.external_download("cluster1", "/path/to/remote/sourcelegacy")
    assert isinstance(obj, firecrest.v1.ExternalDownload)
    assert obj._task_id == "external_download_id_legacy"
    assert obj.client == valid_client


def test_cli_external_download(valid_credentials):
    global external_download_retry
    external_download_retry = 0
    args = valid_credentials + [
        "--api-version=1.14.0",
        "download",
        "--type=external",
        "--system",
        "cluster1",
        "/path/to/remote/source",
    ]
    result = runner.invoke(cli.app, args=args)
    stdout = common.clean_stdout(result.stdout)
    assert result.exit_code == 0
    assert (
        "Follow the status of the transfer asynchronously with that task ID:" in stdout
    )
    assert "external_download_id" in stdout
    assert "Download the file from:" in stdout
    assert "https://object_storage_link.com" in stdout


def test_cli_external_download_legacy(valid_credentials):
    global external_download_retry
    external_download_retry = 0
    args = valid_credentials + [
        "--api-version=1.13.0",
        "download",
        "--type=external",
        "--system",
        "cluster1",
        "/path/to/remote/sourcelegacy",
    ]
    result = runner.invoke(cli.app, args=args)
    stdout = common.clean_stdout(result.stdout)
    assert result.exit_code == 0
    assert (
        "Follow the status of the transfer asynchronously with that task ID:" in stdout
    )
    assert "external_download_id_legacy" in stdout
    assert "Download the file from:" in stdout
    assert "https://object_storage_link_legacy.com" in stdout


def test_external_upload(valid_client):
    global external_upload_retry
    external_upload_retry = 0
    obj = valid_client.external_upload(
        "cluster1", "/path/to/local/source", "/path/to/remote/destination"
    )
    assert isinstance(obj, firecrest.v1.ExternalUpload)
    assert obj._task_id == "external_upload_id"
    assert obj.client == valid_client


def test_cli_external_upload(valid_credentials):
    global external_upload_retry
    external_upload_retry = 0
    args = valid_credentials + [
        "upload",
        "--type=external",
        "--system",
        "cluster1",
        "/path/to/local/source",
        "/path/to/remote/destination",
    ]
    result = runner.invoke(cli.app, args=args)
    stdout = common.clean_stdout(result.stdout)
    assert result.exit_code == 0
    assert (
        "Follow the status of the transfer asynchronously with that task ID:" in stdout
    )
    assert "external_upload_id" in stdout
    assert "Necessary information to upload the file in the staging area:" in stdout
    assert "Or simply run the following command to finish the upload:" in stdout
