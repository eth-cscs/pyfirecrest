import common
import json
import pytest
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


def ls_handler(request: Request):
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
                    "description": "Error on ls operation",
                    "error": "Machine does not exist",
                }
            ),
            status=400,
            headers={"X-Machine-Does-Not-Exist": "Machine does not exist"},
            content_type="application/json",
        )

    targetPath = request.args.get("targetPath")
    if targetPath == "/path/to/valid/dir":
        ret = {
            "description": "List of contents",
            "output": [
                {
                    "group": "group",
                    "last_modified": "2021-08-10T15:26:52",
                    "link_target": "",
                    "name": "file.txt",
                    "permissions": "r-xr-xr-x.",
                    "size": "180",
                    "type": "-",
                    "user": "user",
                },
                {
                    "group": "group",
                    "last_modified": "2021-10-07T09:17:01",
                    "link_target": "",
                    "name": "projectdir",
                    "permissions": "rwxr-xr-x",
                    "size": "4096",
                    "type": "d",
                    "user": "user",
                },
            ],
        }
        showhidden = request.args.get("showhidden", False)
        if showhidden:
            ret["output"].append(
                {
                    "group": "group",
                    "last_modified": "2021-11-26T09:34:59",
                    "link_target": "",
                    "name": ".hiddenfile",
                    "permissions": "rwxrwxr-x",
                    "size": "4096",
                    "type": "-",
                    "user": "user",
                }
            )

        return Response(json.dumps(ret), status=200, content_type="application/json")

    if targetPath == "/path/to/invalid/dir":
        extra_headers = {"X-Invalid-Path": "path is an invalid path"}
        ret = {"description": "Error on ls operation"}
        return Response(
            json.dumps(ret),
            status=400,
            headers=extra_headers,
            content_type="application/json",
        )


def mkdir_handler(request: Request):
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
                    "description": "Error on mkdir operation",
                    "error": "Machine does not exist",
                }
            ),
            status=400,
            headers={"X-Machine-Does-Not-Exist": "Machine does not exist"},
            content_type="application/json",
        )

    extra_headers = None
    target_path = request.form["targetPath"]
    p = request.form.get("p", False)
    if target_path == "path/to/valid/dir" or (
        target_path == "path/to/valid/dir/with/p" and p
    ):
        ret = {"description": "Success to mkdir file or directory.", "output": ""}
        status_code = 201
    else:
        extra_headers[
            "X-Invalid-Path"
        ] = "sourcePath and/or targetPath are invalid paths"
        ret = {"description": "Error on mkdir operation"}
        status_code = 400

    return Response(
        json.dumps(ret),
        status=status_code,
        headers=extra_headers,
        content_type="application/json",
    )


def mv_handler(request: Request):
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
                    "description": "Error on rename operation",
                    "error": "Machine does not exist",
                }
            ),
            status=400,
            headers={"X-Machine-Does-Not-Exist": "Machine does not exist"},
            content_type="application/json",
        )

    source_path = request.form["sourcePath"]
    target_path = request.form["targetPath"]
    if (
        source_path == "/path/to/valid/source"
        and target_path == "/path/to/valid/destination"
    ):
        ret = {"description": "Success to rename file or directory.", "output": ""}
        status_code = 200
    else:
        # FIXME: FirecREST updates the response_headers as well, so the error might be HeaderException
        ret = {"description": "Error on rename operation"}
        status_code = 400

    return Response(
        json.dumps(ret), status=status_code, content_type="application/json"
    )


def chmod_handler(request: Request):
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
                    "description": "Error on chmod operation",
                    "error": "Machine does not exist",
                }
            ),
            status=400,
            headers={"X-Machine-Does-Not-Exist": "Machine does not exist"},
            content_type="application/json",
        )

    extra_headers = None
    target_path = request.form["targetPath"]
    mode = request.form["mode"]
    if target_path == "/path/to/valid/file" and mode == "777":
        ret = {
            "description": "Success to chmod file or directory.",
            "output": "mode of '/path/to/valid/file' changed from 0755 (rwxr-xr-x) to 0777 (rwxrwxrwx)",
        }
        status_code = 200
    else:
        # FIXME: FirecREST sets the X-Invalid-Path even when the problem is the mode argument
        extra_headers = {"X-Invalid-Path": "path is an invalid path"}
        ret = {"description": "Error on chmod operation"}
        status_code = 400

    return Response(
        json.dumps(ret),
        status=status_code,
        headers=extra_headers,
        content_type="application/json",
    )


def chown_handler(request: Request):
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
                    "description": "Error on chown operation",
                    "error": "Machine does not exist",
                }
            ),
            status=400,
            headers={"X-Machine-Does-Not-Exist": "Machine does not exist"},
            content_type="application/json",
        )

    extra_headers = None
    target_path = request.form["targetPath"]
    owner = request.form.get("owner", "")
    group = request.form.get("group", "")
    if target_path == "/path/to/file" and owner == "new_owner" and group == "new_group":
        ret = {
            "description": "Success to chown file or directory.",
            "output": "changed ownership of '/path/to/file' from old_owner:old_group to new_owner:new_group",
        }
        status_code = 200
    else:
        extra_headers = {"X-Invalid-Path": "path is an invalid path"}
        ret = {"description": "Error on chown operation"}
        status_code = 400

    return Response(
        json.dumps(ret),
        status=status_code,
        headers=extra_headers,
        content_type="application/json",
    )


def copy_handler(request: Request):
    if request.headers["Authorization"] != "Bearer VALID_TOKEN":
        return Response(
            json.dumps({"message": "Bad token; invalid JSON"}),
            status=401,
            content_type="application/json",
        )

    if request.headers["X-Machine-Name"] != "cluster1":
        return Response(
            json.dumps(
                {"description": "Error on copy operation", "error": "Machine does not exist"}
            ),
            status=400,
            headers={"X-Machine-Does-Not-Exist": "Machine does not exist"},
            content_type="application/json",
        )

    extra_headers = None
    source_path = request.form["sourcePath"]
    target_path = request.form["targetPath"]
    if (
        source_path == "/path/to/valid/source"
        and target_path == "/path/to/valid/destination"
    ):
        ret = {"description": "Success to copy file or directory.", "output": ""}
        status_code = 201
    else:
        extra_headers = {"X-Invalid-Path": "path is an invalid path"}
        ret = {"description": "Error on copy operation"}
        status_code = 400

    return Response(
        json.dumps(ret),
        status=status_code,
        headers=extra_headers,
        content_type="application/json",
    )


def compress_handler(request: Request):
    if request.headers["Authorization"] != "Bearer VALID_TOKEN":
        return Response(
            json.dumps({"message": "Bad token; invalid JSON"}),
            status=401,
            content_type="application/json",
        )

    if request.headers["X-Machine-Name"] != "cluster1":
        return Response(
            json.dumps(
                {"description": "Error on compress operation", "error": "Machine does not exist"}
            ),
            status=400,
            headers={"X-Machine-Does-Not-Exist": "Machine does not exist"},
            content_type="application/json",
        )

    extra_headers = None
    source_path = request.form["sourcePath"]
    target_path = request.form["targetPath"]
    if (
        source_path == "/path/to/valid/source"
        and target_path == "/path/to/valid/destination.tar.gz"
    ):
        ret = {"description": "Success to compress file or directory.", "output": ""}
        status_code = 201
    else:
        extra_headers = {"X-Invalid-Path": "path is an invalid path"}
        ret = {"description": "Error on compress operation"}
        status_code = 400

    return Response(
        json.dumps(ret),
        status=status_code,
        headers=extra_headers,
        content_type="application/json",
    )


def extract_handler(request: Request):
    if request.headers["Authorization"] != "Bearer VALID_TOKEN":
        return Response(
            json.dumps({"message": "Bad token; invalid JSON"}),
            status=401,
            content_type="application/json",
        )

    if request.headers["X-Machine-Name"] != "cluster1":
        return Response(
            json.dumps(
                {"description": "Error on compress operation", "error": "Machine does not exist"}
            ),
            status=400,
            headers={"X-Machine-Does-Not-Exist": "Machine does not exist"},
            content_type="application/json",
        )

    extra_headers = None
    source_path = request.form["sourcePath"]
    target_path = request.form["targetPath"]
    if (
        source_path == "/path/to/valid/source.tar.gz"
        and target_path == "/path/to/valid/destination"
    ):
        ret = {"description": "Success to extract file or directory.", "output": ""}
        status_code = 201
    else:
        extra_headers = {"X-Invalid-Path": "path is an invalid path"}
        ret = {"description": "Error on extract operation"}
        status_code = 400

    return Response(
        json.dumps(ret),
        status=status_code,
        headers=extra_headers,
        content_type="application/json",
    )


def file_type_handler(request: Request):
    if request.headers["Authorization"] != "Bearer VALID_TOKEN":
        return Response(
            json.dumps({"message": "Bad token; invalid JSON"}),
            status=401,
            content_type="application/json",
        )

    if request.headers["X-Machine-Name"] != "cluster1":
        return Response(
            json.dumps(
                {"description": "Error on file operation", "error": "Machine does not exist"}
            ),
            status=400,
            headers={"X-Machine-Does-Not-Exist": "Machine does not exist"},
            content_type="application/json",
        )

    extra_headers = None
    targetPath = request.args.get("targetPath")
    if targetPath == "/path/to/empty/file":
        ret = {"description": "Success to file file or directory.", "output": "empty"}
        status_code = 200
    elif targetPath == "/path/to/directory":
        ret = {
            "description": "Success to file file or directory.",
            "output": "directory",
        }
        status_code = 200
    else:
        extra_headers = {"X-Invalid-Path": "path is an invalid path"}
        ret = {"description": "Error on file operation"}
        status_code = 400

    return Response(
        json.dumps(ret),
        status=status_code,
        headers=extra_headers,
        content_type="application/json",
    )


def stat_handler(request: Request):
    if request.headers["Authorization"] != "Bearer VALID_TOKEN":
        return Response(
            json.dumps({"message": "Bad token; invalid JSON"}),
            status=401,
            content_type="application/json",
        )

    if request.headers["X-Machine-Name"] != "cluster1":
        return Response(
            json.dumps(
                {"description": "Error on file operation", "error": "Machine does not exist"}
            ),
            status=400,
            headers={"X-Machine-Does-Not-Exist": "Machine does not exist"},
            content_type="application/json",
        )

    extra_headers = None
    targetPath = request.args.get("targetPath")
    deref = request.args.get("dereference", False)
    if targetPath == "/path/to/link":
        if deref:
            ret = {
                "description": "Success to stat file or directory.",
                "output": {
                    "atime": 1653660606,
                    "ctime": 1653660606,
                    "dev": 2418024346,
                    "gid": 1000,
                    "ino": 648577914584968738,
                    "mode": 644,
                    "mtime": 1653660606,
                    "nlink": 1,
                    "size": 0,
                    "uid": 25948,
                },
            }
            status_code = 200
        else:
            ret = {
                "description": "Success to stat file or directory.",
                "output": {
                    "atime": 1655197211,
                    "ctime": 1655197211,
                    "dev": 2418024346,
                    "gid": 1000,
                    "ino": 648577971375854279,
                    "mode": 777,
                    "mtime": 1655197211,
                    "nlink": 1,
                    "size": 8,
                    "uid": 25948,
                },
            }
            status_code = 200
    else:
        extra_headers = {"X-Not-Found": "sourcePath not found"}
        ret = {"description": "Error on stat operation"}
        status_code = 400

    return Response(
        json.dumps(ret),
        status=status_code,
        headers=extra_headers,
        content_type="application/json",
    )


def symlink_handler(request: Request):
    if request.headers["Authorization"] != "Bearer VALID_TOKEN":
        return Response(
            json.dumps({"message": "Bad token; invalid JSON"}),
            status=401,
            content_type="application/json",
        )

    if request.headers["X-Machine-Name"] != "cluster1":
        return Response(
            json.dumps(
                {"description": "Error on symlink operation", "error": "Machine does not exist"}
            ),
            status=400,
            headers={"X-Machine-Does-Not-Exist": "Machine does not exist"},
            content_type="application/json",
        )

    extra_headers = None
    target_path = request.form["targetPath"]
    link_path = request.form["linkPath"]
    if target_path == "/path/to/file" and link_path == "/path/to/link":
        ret = {"description": "Success to link file or directory.", "output": ""}
        status_code = 201
    else:
        extra_headers = {"X-Invalid-Path": "path is an invalid path"}
        ret = {"description": "Error on symlink operation"}
        status_code = 400

    return Response(
        json.dumps(ret),
        status=status_code,
        headers=extra_headers,
        content_type="application/json",
    )


def simple_download_handler(request: Request):
    if request.headers["Authorization"] != "Bearer VALID_TOKEN":
        return Response(
            json.dumps({"message": "Bad token; invalid JSON"}),
            status=401,
            content_type="application/json",
        )

    if request.headers["X-Machine-Name"] != "cluster1":
        return Response(
            json.dumps(
                {"description": "Error on download operation", "error": "Machine does not exist"}
            ),
            status=400,
            headers={"X-Machine-Does-Not-Exist": "Machine does not exist"},
            content_type="application/json",
        )

    source_path = request.args.get("sourcePath")
    if source_path == "/path/to/remote/source":
        ret = "Hello!\n"
        status_code = 200
        return Response(ret, status=status_code)
    else:
        extra_headers = {"X-Invalid-Path": "path is an invalid path"}
        ret = {"description": "Error on download operation"}
        status_code = 400
        return Response(
            json.dumps(ret),
            status=status_code,
            headers=extra_headers,
            content_type="application/json",
        )


def simple_upload_handler(request: Request):
    if request.headers["Authorization"] != "Bearer VALID_TOKEN":
        return Response(
            json.dumps({"message": "Bad token; invalid JSON"}),
            status=401,
            content_type="application/json",
        )

    if request.headers["X-Machine-Name"] != "cluster1":
        return Response(
            json.dumps(
                {"description": "Error on download operation", "error": "Machine does not exist"}
            ),
            status=400,
            headers={"X-Machine-Does-Not-Exist": "Machine does not exist"},
            content_type="application/json",
        )

    if request.form["targetPath"] == "/path/to/remote/destination":
        extra_headers = None
        ret = {"description": "File upload successful", "output": ""}
        status_code = 201
    else:
        extra_headers = {"X-Invalid-Path": "path is an invalid path"}
        ret = {"description": "Error on upload operation"}
        status_code = 400

    return Response(
        json.dumps(ret),
        status=status_code,
        headers=extra_headers,
        content_type="application/json",
    )


def simple_delete_handler(request: Request):
    if request.headers["Authorization"] != "Bearer VALID_TOKEN":
        return Response(
            json.dumps({"message": "Bad token; invalid JSON"}),
            status=401,
            content_type="application/json",
        )

    if request.headers["X-Machine-Name"] != "cluster1":
        return Response(
            json.dumps(
                {"description": "Error on download operation", "error": "Machine does not exist"}
            ),
            status=400,
            headers={"X-Machine-Does-Not-Exist": "Machine does not exist"},
            content_type="application/json",
        )

    target_path = request.form["targetPath"]
    if target_path == "/path/to/file":
        extra_headers = None
        ret = {"description": "File delete successful", "output": ""}
        status_code = 204
    else:
        extra_headers = {"X-Invalid-Path": "path is an invalid path"}
        ret = {"description": "Error on delete operation"}
        status_code = 400

    return Response(
        json.dumps(ret),
        status=status_code,
        headers=extra_headers,
        content_type="application/json",
    )


def checksum_handler(request: Request):
    if request.headers["Authorization"] != "Bearer VALID_TOKEN":
        return Response(
            json.dumps({"message": "Bad token; invalid JSON"}),
            status=401,
            content_type="application/json",
        )

    if request.headers["X-Machine-Name"] != "cluster1":
        return Response(
            json.dumps(
                {"description": "Error on checksum operation", "error": "Machine does not exist"}
            ),
            status=400,
            headers={"X-Machine-Does-Not-Exist": "Machine does not exist"},
            content_type="application/json",
        )

    target_path = request.args.get("targetPath")
    if target_path == "/path/to/file":
        extra_headers = None
        ret = {
            "description": "Success to checksum file or directory.",
            "output": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        }
        status_code = 200
    else:
        extra_headers = {"X-Invalid-Path": "path is an invalid path"}
        ret = {"description": "Error on checksum operation"}
        status_code = 400

    return Response(
        json.dumps(ret),
        status=status_code,
        headers=extra_headers,
        content_type="application/json",
    )


def view_handler(request: Request):
    if request.headers["Authorization"] != "Bearer VALID_TOKEN":
        return Response(
            json.dumps({"message": "Bad token; invalid JSON"}),
            status=401,
            content_type="application/json",
        )

    if request.headers["X-Machine-Name"] != "cluster1":
        return Response(
            json.dumps(
                {"description": "Error on head operation", "error": "Machine does not exist"}
            ),
            status=400,
            headers={"X-Machine-Does-Not-Exist": "Machine does not exist"},
            content_type="application/json",
        )

    target_path = request.args.get("targetPath")
    if target_path == "/path/to/file":
        extra_headers = None
        ret = {"description": "Success to head file or directory.", "output": "hello\n"}
        status_code = 200
    else:
        extra_headers = {"X-Invalid-Path": "path is an invalid path"}
        ret = {"description": "Error on head operation"}
        status_code = 400

    return Response(
        json.dumps(ret),
        status=status_code,
        headers=extra_headers,
        content_type="application/json",
    )


def head_tail_handler(request: Request):
    is_tail_req = "tail" in request.url
    if request.headers["Authorization"] != "Bearer VALID_TOKEN":
        return Response(
            json.dumps({"message": "Bad token; invalid JSON"}),
            status=401,
            content_type="application/json",
        )

    if request.headers["X-Machine-Name"] != "cluster1":
        return Response(
            json.dumps(
                {"description": "Error on head operation", "error": "Machine does not exist"}
            ),
            status=400,
            headers={"X-Machine-Does-Not-Exist": "Machine does not exist"},
            content_type="application/json",
        )

    target_path = request.args.get("targetPath")
    lines = request.args.get("lines")
    bytes = request.args.get("bytes")
    if target_path == "/path/to/file":
        extra_headers = None
        if lines and int(lines) < 10:
            result = int(lines) * "hello\n"
        else:
            result = 10 * "hello\n"

        if bytes:
            if is_tail_req:
                result = result[-int(bytes) :]
            else:
                result = result[0 : int(bytes)]

        ret = {"description": "Success to head file.", "output": result}
        status_code = 200
    else:
        extra_headers = {"X-Invalid-Path": "path is an invalid path"}
        ret = {"description": "Error on head operation"}
        status_code = 400

    return Response(
        json.dumps(ret),
        status=status_code,
        headers=extra_headers,
        content_type="application/json",
    )


def whoami_handler(request: Request):
    if request.headers["Authorization"] != "Bearer VALID_TOKEN":
        return Response(
            json.dumps({"message": "Bad token; invalid JSON"}),
            status=401,
            content_type="application/json",
        )

    if request.headers["X-Machine-Name"] != "cluster1":
        return Response(
            json.dumps(
                {"description": "Error on whoami operation", "error": "Machine does not exist"}
            ),
            status=400,
            headers={"X-Machine-Does-Not-Exist": "Machine does not exist"},
            content_type="application/json",
        )

    groups = request.args.get("groups", False)
    if groups:
        ret = {
            "description": "User information",
            "output": {
                "group": {"id": "1000", "name": "group1"},
                "groups": [{"id": "1000", "name": "group1"}, {"id": "1001", "name": "group2"}],
                "user": {"id": "10000", "name": "test_user"},
            }
        }
    else:
        ret = {"description": "Success on whoami operation.", "output": "username"}

    return Response(
        json.dumps(ret),
        status=200,
        content_type="application/json",
    )


@pytest.fixture
def fc_server(httpserver):
    httpserver.expect_request("/utilities/ls", method="GET").respond_with_handler(
        ls_handler
    )

    httpserver.expect_request("/utilities/mkdir", method="POST").respond_with_handler(
        mkdir_handler
    )

    httpserver.expect_request("/utilities/rename", method="PUT").respond_with_handler(
        mv_handler
    )

    httpserver.expect_request("/utilities/chmod", method="PUT").respond_with_handler(
        chmod_handler
    )

    httpserver.expect_request("/utilities/chown", method="PUT").respond_with_handler(
        chown_handler
    )

    httpserver.expect_request("/utilities/copy", method="POST").respond_with_handler(
        copy_handler
    )

    httpserver.expect_request("/utilities/compress", method="POST").respond_with_handler(
        compress_handler
    )

    httpserver.expect_request("/utilities/extract", method="POST").respond_with_handler(
        extract_handler
    )

    httpserver.expect_request("/utilities/file", method="GET").respond_with_handler(
        file_type_handler
    )

    httpserver.expect_request("/utilities/stat", method="GET").respond_with_handler(
        stat_handler
    )

    httpserver.expect_request("/utilities/symlink", method="POST").respond_with_handler(
        symlink_handler
    )

    httpserver.expect_request("/utilities/download", method="GET").respond_with_handler(
        simple_download_handler
    )

    httpserver.expect_request("/utilities/upload", method="POST").respond_with_handler(
        simple_upload_handler
    )

    httpserver.expect_request("/utilities/rm", method="DELETE").respond_with_handler(
        simple_delete_handler
    )

    httpserver.expect_request("/utilities/checksum", method="GET").respond_with_handler(
        checksum_handler
    )

    httpserver.expect_request("/utilities/view", method="GET").respond_with_handler(
        view_handler
    )

    httpserver.expect_request("/utilities/head", method="GET").respond_with_handler(
        head_tail_handler
    )

    httpserver.expect_request("/utilities/tail", method="GET").respond_with_handler(
        head_tail_handler
    )

    httpserver.expect_request("/utilities/whoami", method="GET").respond_with_handler(
        whoami_handler
    )

    return httpserver


@pytest.fixture
def auth_server(httpserver):
    httpserver.expect_request("/auth/token").respond_with_handler(auth.auth_handler)
    return httpserver


def test_list_files(valid_client):
    assert valid_client.list_files("cluster1", "/path/to/valid/dir") == [
        {
            "group": "group",
            "last_modified": "2021-08-10T15:26:52",
            "link_target": "",
            "name": "file.txt",
            "permissions": "r-xr-xr-x.",
            "size": "180",
            "type": "-",
            "user": "user",
        },
        {
            "group": "group",
            "last_modified": "2021-10-07T09:17:01",
            "link_target": "",
            "name": "projectdir",
            "permissions": "rwxr-xr-x",
            "size": "4096",
            "type": "d",
            "user": "user",
        },
    ]

    assert valid_client.list_files(
        "cluster1", "/path/to/valid/dir", show_hidden=True
    ) == [
        {
            "group": "group",
            "last_modified": "2021-08-10T15:26:52",
            "link_target": "",
            "name": "file.txt",
            "permissions": "r-xr-xr-x.",
            "size": "180",
            "type": "-",
            "user": "user",
        },
        {
            "group": "group",
            "last_modified": "2021-10-07T09:17:01",
            "link_target": "",
            "name": "projectdir",
            "permissions": "rwxr-xr-x",
            "size": "4096",
            "type": "d",
            "user": "user",
        },
        {
            "group": "group",
            "last_modified": "2021-11-26T09:34:59",
            "link_target": "",
            "name": ".hiddenfile",
            "permissions": "rwxrwxr-x",
            "size": "4096",
            "type": "-",
            "user": "user",
        },
    ]


def test_cli_list_files(valid_credentials):
    args = valid_credentials + ["ls", "--system", "cluster1", "/path/to/valid/dir"]
    result = runner.invoke(cli.app, args=args)
    stdout = common.clean_stdout(result.stdout)
    assert result.exit_code == 0
    assert "file.txt" in stdout
    assert "projectd" in stdout
    assert ".hiddenf" not in stdout

    args = valid_credentials + ["ls", "--system", "cluster1", "/path/to/valid/dir", "--show-hidden"]
    result = runner.invoke(cli.app, args=args)
    stdout = common.clean_stdout(result.stdout)
    assert result.exit_code == 0
    assert "file.txt" in stdout
    assert "projectd" in stdout
    assert ".hiddenf" in stdout


def test_list_files_invalid_path(valid_client):
    with pytest.raises(firecrest.FirecrestException):
        valid_client.list_files("cluster1", "/path/to/invalid/dir")


def test_list_files_invalid_machine(valid_client):
    with pytest.raises(firecrest.HeaderException):
        valid_client.list_files("cluster2", "/path/to/dir")


def test_list_files_invalid_client(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        invalid_client.list_files("cluster1", "/path/to/dir")


def test_mkdir(valid_client):
    # Make sure these don't raise an error
    valid_client.mkdir("cluster1", "path/to/valid/dir")
    valid_client.mkdir("cluster1", "path/to/valid/dir/with/p", p=True)


def test_cli_mkdir(valid_credentials):
    args = valid_credentials + ["mkdir", "--system", "cluster1", "path/to/valid/dir"]
    result = runner.invoke(cli.app, args=args)
    assert result.exit_code == 0

    args = valid_credentials + ["mkdir", "--system", "cluster1", "path/to/valid/dir/with/p", "-p"]
    result = runner.invoke(cli.app, args=args)
    assert result.exit_code == 0


def test_mkdir_invalid_path(valid_client):
    with pytest.raises(firecrest.FirecrestException):
        valid_client.mkdir("cluster1", "/path/to/invalid/dir")


def test_mkdir_invalid_machine(valid_client):
    with pytest.raises(firecrest.HeaderException):
        valid_client.mkdir("cluster2", "path/to/dir")


def test_mkdir_invalid_client(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        invalid_client.mkdir("cluster1", "path/to/dir")


def test_mv(valid_client):
    # Make sure this doesn't raise an error
    valid_client.mv("cluster1", "/path/to/valid/source", "/path/to/valid/destination")


def test_cli_mv(valid_credentials):
    args = valid_credentials + [
        "mv",
        "--system",
        "cluster1",
        "/path/to/valid/source",
        "/path/to/valid/destination",
    ]
    result = runner.invoke(cli.app, args=args)
    assert result.exit_code == 0


def test_mv_invalid_paths(valid_client):
    with pytest.raises(firecrest.FirecrestException):
        valid_client.mv(
            "cluster1", "/path/to/invalid/source", "/path/to/valid/destination"
        )

    with pytest.raises(firecrest.FirecrestException):
        valid_client.mv(
            "cluster1", "/path/to/valid/source", "/path/to/invalid/destination"
        )


def test_mv_invalid_machine(valid_client):
    with pytest.raises(firecrest.HeaderException):
        valid_client.mv("cluster2", "/path/to/source", "/path/to/destination")


def test_mv_invalid_client(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        invalid_client.mv("cluster1", "/path/to/source", "/path/to/destination")


def test_chmod(valid_client):
    # Make sure this doesn't raise an error
    valid_client.chmod("cluster1", "/path/to/valid/file", "777")


def test_cli_chmod(valid_credentials):
    args = valid_credentials + ["chmod", "--system", "cluster1", "/path/to/valid/file", "777"]
    result = runner.invoke(cli.app, args=args)
    assert result.exit_code == 0


def test_chmod_invalid_arguments(valid_client):
    with pytest.raises(firecrest.FirecrestException):
        valid_client.chmod("cluster1", "/path/to/invalid/file", "777")

    with pytest.raises(firecrest.FirecrestException):
        valid_client.chmod("cluster1", "/path/to/valid/file", "random_string")


def test_chmod_invalid_machine(valid_client):
    with pytest.raises(firecrest.HeaderException):
        valid_client.chmod("cluster2", "/path/to/file", "700")


def test_chmod_invalid_client(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        invalid_client.chmod("cluster1", "/path/to/file", "600")


def test_chown(valid_client):
    # Make sure this doesn't raise an error
    valid_client.chown(
        "cluster1", "/path/to/file", owner="new_owner", group="new_group"
    )
    # Call will immediately return if neither owner and nor group are set
    valid_client.chown("cluster", "path")


def test_cli_chown(valid_credentials):
    args = valid_credentials + [
        "chown",
        "--system",
        "cluster1",
        "/path/to/file",
        "--owner=new_owner",
        "--group=new_group",
    ]
    result = runner.invoke(cli.app, args=args)
    assert result.exit_code == 0


def test_chown_invalid_arguments(valid_client):
    with pytest.raises(firecrest.HeaderException):
        valid_client.chown(
            "cluster1", "/bad/path", owner="new_owner", group="new_group"
        )

    with pytest.raises(firecrest.HeaderException):
        valid_client.chown(
            "cluster1", "/path/to/file", owner="bad_owner", group="new_group"
        )

    with pytest.raises(firecrest.HeaderException):
        valid_client.chown(
            "cluster1", "/path/to/file", owner="new_owner", group="bad_group"
        )


def test_chown_invalid_machine(valid_client):
    with pytest.raises(firecrest.HeaderException):
        valid_client.chown(
            "cluster2", "/path/to/file", owner="new_owner", group="new_group"
        )


def test_chown_invalid_client(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        invalid_client.chown(
            "cluster1", "/path/to/file", owner="new_owner", group="new_group"
        )


def test_copy(valid_client):
    # Make sure this doesn't raise an error
    valid_client.copy("cluster1", "/path/to/valid/source", "/path/to/valid/destination")


def test_cli_copy(valid_credentials):
    args = valid_credentials + [
        "cp",
        "--system",
        "cluster1",
        "/path/to/valid/source",
        "/path/to/valid/destination",
    ]
    result = runner.invoke(cli.app, args=args)
    assert result.exit_code == 0


def test_copy_invalid_arguments(valid_client):
    with pytest.raises(firecrest.HeaderException):
        valid_client.copy(
            "cluster1", "/path/to/invalid/source", "/path/to/valid/destination"
        )

    with pytest.raises(firecrest.HeaderException):
        valid_client.copy(
            "cluster1", "/path/to/valid/source", "/path/to/invalid/destination"
        )


def test_copy_invalid_machine(valid_client):
    with pytest.raises(firecrest.HeaderException):
        valid_client.copy("cluster2", "/path/to/source", "/path/to/destination")


def test_copy_invalid_client(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        invalid_client.copy("cluster1", "/path/to/source", "/path/to/destination")


def test_compress(valid_client):
    # Mostly sure this doesn't raise an error
    assert (
        valid_client.compress(
            "cluster1",
            "/path/to/valid/source",
            "/path/to/valid/destination.tar.gz"
        ) == "/path/to/valid/destination.tar.gz"
    )


def test_compress_not_impl(valid_client):
    valid_client.set_api_version("1.15.0")
    with pytest.raises(firecrest.NotImplementedOnAPIversion):
        valid_client.compress(
            "cluster1",
            "/path/to/valid/source",
            "/path/to/valid/destination.tar.gz"
        )


def test_cli_compress(valid_credentials):
    args = valid_credentials + [
        "compress",
        "--system",
        "cluster1",
        "/path/to/valid/source",
        "/path/to/valid/destination.tar.gz",
    ]
    result = runner.invoke(cli.app, args=args)
    assert result.exit_code == 0


def test_extract(valid_client):
    # Mostly sure this doesn't raise an error
    assert (
        valid_client.extract(
            "cluster1",
            "/path/to/valid/source.tar.gz",
            "/path/to/valid/destination"
        ) == "/path/to/valid/destination"
    )


def test_extract_not_impl(valid_client):
    valid_client.set_api_version("1.15.0")
    with pytest.raises(firecrest.NotImplementedOnAPIversion):
        valid_client.extract(
            "cluster1",
            "/path/to/valid/source.tar.gz",
            "/path/to/valid/destination"
        )


def test_cli_extract(valid_credentials):
    args = valid_credentials + [
        "extract",
        "--system",
        "cluster1",
        "/path/to/valid/source.tar.gz",
        "/path/to/valid/destination",
    ]
    result = runner.invoke(cli.app, args=args)
    assert result.exit_code == 0


def test_file_type(valid_client):
    assert valid_client.file_type("cluster1", "/path/to/empty/file") == "empty"
    assert valid_client.file_type("cluster1", "/path/to/directory") == "directory"


def test_cli_file_type(valid_credentials):
    args = valid_credentials + ["file", "--system", "cluster1", "/path/to/empty/file"]
    result = runner.invoke(cli.app, args=args)
    stdout = common.clean_stdout(result.stdout)
    assert result.exit_code == 0
    assert "empty" in stdout

    args = valid_credentials + ["file", "--system", "cluster1", "/path/to/directory"]
    result = runner.invoke(cli.app, args=args)
    stdout = common.clean_stdout(result.stdout)
    assert result.exit_code == 0
    assert "directory" in stdout


def test_file_type_invalid_arguments(valid_client):
    with pytest.raises(firecrest.HeaderException):
        valid_client.file_type("cluster1", "/path/to/invalid/file")


def test_file_type_invalid_machine(valid_client):
    with pytest.raises(firecrest.HeaderException):
        valid_client.file_type("cluster2", "/path/to/file")


def test_file_type_invalid_client(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        invalid_client.file_type("cluster1", "/path/to/file")


def test_stat(valid_client):
    assert valid_client.stat("cluster1", "/path/to/link") == {
        "atime": 1655197211,
        "ctime": 1655197211,
        "dev": 2418024346,
        "gid": 1000,
        "ino": 648577971375854279,
        "mode": 777,
        "mtime": 1655197211,
        "nlink": 1,
        "size": 8,
        "uid": 25948,
    }
    assert valid_client.stat("cluster1", "/path/to/link", dereference=False) == {
        "atime": 1655197211,
        "ctime": 1655197211,
        "dev": 2418024346,
        "gid": 1000,
        "ino": 648577971375854279,
        "mode": 777,
        "mtime": 1655197211,
        "nlink": 1,
        "size": 8,
        "uid": 25948,
    }
    assert valid_client.stat("cluster1", "/path/to/link", dereference=True) == {
        "atime": 1653660606,
        "ctime": 1653660606,
        "dev": 2418024346,
        "gid": 1000,
        "ino": 648577914584968738,
        "mode": 644,
        "mtime": 1653660606,
        "nlink": 1,
        "size": 0,
        "uid": 25948,
    }


def test_cli_stat(valid_credentials):
    args = valid_credentials + ["stat", "--system", "cluster1", "/path/to/link"]
    result = runner.invoke(cli.app, args=args)
    stdout = common.clean_stdout(result.stdout)
    assert result.exit_code == 0
    assert "ino       | 648577971375854279 | inode number" in stdout

    args = valid_credentials + ["stat", "--system", "cluster1", "/path/to/link", "-L"]
    result = runner.invoke(cli.app, args=args)
    stdout = common.clean_stdout(result.stdout)
    assert result.exit_code == 0
    assert "ino       | 648577914584968738 | inode number" in stdout


def test_stat_invalid_arguments(valid_client):
    with pytest.raises(firecrest.HeaderException):
        valid_client.stat("cluster1", "/path/to/invalid/file")


def test_stat_invalid_machine(valid_client):
    with pytest.raises(firecrest.HeaderException):
        valid_client.stat("cluster2", "/path/to/file")


def test_stat_invalid_client(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        invalid_client.stat("cluster1", "/path/to/file")


def test_symlink(valid_client):
    # Make sure this doesn't raise an error
    valid_client.symlink("cluster1", "/path/to/file", "/path/to/link")


def test_cli_symlink(valid_credentials):
    args = valid_credentials + ["symlink", "--system", "cluster1", "/path/to/file", "/path/to/link"]
    result = runner.invoke(cli.app, args=args)
    assert result.exit_code == 0


def test_symlink_invalid_arguments(valid_client):
    with pytest.raises(firecrest.HeaderException):
        valid_client.symlink("cluster1", "/path/to/invalid/file", "/path/to/link")

    with pytest.raises(firecrest.HeaderException):
        valid_client.symlink("cluster1", "/path/to/file", "/path/to/invalid/link")


def test_symlink_invalid_machine(valid_client):
    with pytest.raises(firecrest.HeaderException):
        valid_client.symlink("cluster2", "/path/to/file", "/path/to/link")


def test_symlink_invalid_client(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        invalid_client.symlink("cluster1", "/path/to/file", "/path/to/link")


def test_simple_download(valid_client, tmp_path):
    tmp_dir = tmp_path / "download_dir"
    tmp_dir.mkdir()
    local_file = tmp_dir / "hello.txt"
    # Make sure this doesn't raise an error
    valid_client.simple_download("cluster1", "/path/to/remote/source", local_file)

    with open(local_file) as f:
        assert f.read() == "Hello!\n"


def test_cli_simple_download(valid_credentials, tmp_path):
    tmp_dir = tmp_path / "download_dir"
    tmp_dir.mkdir()
    local_file = tmp_dir / "hello_cli.txt"
    args = valid_credentials + [
        "download",
        "--system",
        "cluster1",
        "/path/to/remote/source",
        str(local_file),
        "--type=direct",
    ]
    result = runner.invoke(cli.app, args=args)
    assert result.exit_code == 0

    with open(local_file) as f:
        assert f.read() == "Hello!\n"


def test_simple_download_invalid_arguments(valid_client):
    with pytest.raises(firecrest.HeaderException):
        valid_client.simple_download(
            "cluster1", "/path/to/invalid/file", "/path/to/local/destination"
        )


def test_simple_download_invalid_machine(valid_client):
    with pytest.raises(firecrest.HeaderException):
        valid_client.simple_download(
            "cluster2", "/path/to/source", "/path/to/destination"
        )


def test_simple_download_invalid_client(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        invalid_client.simple_download(
            "cluster1", "/path/to/source", "/path/to/destination"
        )


def test_simple_upload(valid_client, tmp_path):
    tmp_dir = tmp_path / "upload_dir"
    tmp_dir.mkdir()
    local_file = tmp_dir / "hello.txt"
    local_file.write_text("hi")
    # Make sure this doesn't raise an error
    valid_client.simple_upload("cluster1", local_file, "/path/to/remote/destination")


def test_cli_simple_upload(valid_credentials, tmp_path):
    tmp_dir = tmp_path / "upload_dir"
    tmp_dir.mkdir()
    local_file = tmp_dir / "hello.txt"
    local_file.write_text("hi")
    args = valid_credentials + [
        "upload",
        "--system",
        "cluster1",
        str(local_file),
        "/path/to/remote/destination",
        "--type=direct",
    ]
    result = runner.invoke(cli.app, args=args)
    # Make sure this doesn't raise an error
    assert result.exit_code == 0


def test_simple_upload_invalid_arguments(valid_client, tmp_path):
    tmp_dir = tmp_path / "download_dir"
    tmp_dir.mkdir()
    local_file = tmp_dir / "hello_invalid.txt"
    local_file.write_text("hi")
    with pytest.raises(firecrest.HeaderException):
        valid_client.simple_upload(
            "cluster1", local_file, "/path/to/invalid/destination"
        )


def test_simple_upload_invalid_machine(valid_client, tmp_path):
    tmp_dir = tmp_path / "download_dir"
    tmp_dir.mkdir()
    local_file = tmp_dir / "hello_invalid.txt"
    local_file.write_text("hi")
    with pytest.raises(firecrest.HeaderException):
        valid_client.simple_upload(
            "cluster2", local_file, "/path/to/remote/destination"
        )


def test_simple_upload_invalid_client(invalid_client, tmp_path):
    tmp_dir = tmp_path / "download_dir"
    tmp_dir.mkdir()
    local_file = tmp_dir / "hello_invalid.txt"
    local_file.write_text("hi")
    with pytest.raises(firecrest.UnauthorizedException):
        invalid_client.simple_upload(
            "cluster1", local_file, "/path/to/remote/destination"
        )


def test_simple_delete(valid_client):
    # Make sure this doesn't raise an error
    valid_client.simple_delete("cluster1", "/path/to/file")


def test_cli_simple_delete(valid_credentials):
    args = valid_credentials + ["rm", "--system", "cluster1", "/path/to/file", "--force"]
    result = runner.invoke(cli.app, args=args)
    # Make sure this doesn't raise an error
    assert result.exit_code == 0


def test_simple_delete_invalid_arguments(valid_client):
    with pytest.raises(firecrest.HeaderException):
        valid_client.simple_delete("cluster1", "/path/to/invalid/file")


def test_simple_delete_invalid_machine(valid_client):
    with pytest.raises(firecrest.HeaderException):
        valid_client.simple_delete("cluster2", "/path/to/file")


def test_simple_delete_invalid_client(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        invalid_client.simple_delete("cluster1", "/path/to/file")


def test_checksum(valid_client):
    assert (
        valid_client.checksum("cluster1", "/path/to/file")
        == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    )


def test_cli_checksum(valid_credentials):
    args = valid_credentials + ["checksum", "--system", "cluster1", "/path/to/file"]
    result = runner.invoke(cli.app, args=args)
    stdout = common.clean_stdout(result.stdout)
    assert result.exit_code == 0
    assert "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" in stdout


def test_checksum_invalid_arguments(valid_client):
    with pytest.raises(firecrest.HeaderException):
        valid_client.checksum("cluster1", "/path/to/invalid/file")


def test_checksum_invalid_machine(valid_client):
    with pytest.raises(firecrest.HeaderException):
        valid_client.checksum("cluster2", "/path/to/file")


def test_checksum_invalid_client(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        invalid_client.checksum("cluster1", "/path/to/file")


def test_view(valid_client):
    assert valid_client.view("cluster1", "/path/to/file") == "hello\n"


def test_head(valid_client):
    assert valid_client.head("cluster1", "/path/to/file") == 10 * "hello\n"
    assert valid_client.head("cluster1", "/path/to/file", lines=2) == 2 * "hello\n"
    assert valid_client.head("cluster1", "/path/to/file", bytes=4) == "hell"


def test_cli_head(valid_credentials):
    args = valid_credentials + ["head", "--system", "cluster1", "/path/to/file"]
    result = runner.invoke(cli.app, args=args)
    stdout = common.clean_stdout(result.stdout)
    assert result.exit_code == 0
    assert stdout.count("hello") == 10

    args = valid_credentials + ["head", "--lines=3", "--system", "cluster1", "/path/to/file"]
    result = runner.invoke(cli.app, args=args)
    stdout = common.clean_stdout(result.stdout)
    assert result.exit_code == 0
    assert stdout.count("hello") == 3

    args = valid_credentials + ["head", "-n", "3", "--system", "cluster1", "/path/to/file"]
    result = runner.invoke(cli.app, args=args)
    stdout = common.clean_stdout(result.stdout)
    assert result.exit_code == 0
    assert stdout.count("hello") == 3

    args = valid_credentials + ["head", "--bytes", "4", "--system", "cluster1", "/path/to/file"]
    result = runner.invoke(cli.app, args=args)
    stdout = common.clean_stdout(result.stdout)
    assert result.exit_code == 0
    assert "hello" not in stdout
    assert "hell" in stdout

    args = valid_credentials + ["head", "-c", "4", "--system", "cluster1", "/path/to/file"]
    result = runner.invoke(cli.app, args=args)
    stdout = common.clean_stdout(result.stdout)
    assert result.exit_code == 0
    assert "hello" not in stdout
    assert "hell" in stdout


def test_tail(valid_client):
    assert valid_client.tail("cluster1", "/path/to/file") == 10 * "hello\n"
    assert valid_client.tail("cluster1", "/path/to/file", lines=2) == 2 * "hello\n"
    assert valid_client.tail("cluster1", "/path/to/file", bytes=5) == "ello\n"


def test_cli_tail(valid_credentials):
    args = valid_credentials + ["tail", "--system", "cluster1", "/path/to/file"]
    result = runner.invoke(cli.app, args=args)
    stdout = common.clean_stdout(result.stdout)
    assert result.exit_code == 0
    assert stdout.count("hello") == 10

    args = valid_credentials + ["tail", "--lines=3", "--system", "cluster1", "/path/to/file"]
    result = runner.invoke(cli.app, args=args)
    stdout = common.clean_stdout(result.stdout)
    assert result.exit_code == 0
    assert stdout.count("hello") == 3

    args = valid_credentials + ["tail", "-n", "3", "--system", "cluster1", "/path/to/file"]
    result = runner.invoke(cli.app, args=args)
    stdout = common.clean_stdout(result.stdout)
    assert result.exit_code == 0
    assert stdout.count("hello") == 3

    args = valid_credentials + ["tail", "--bytes", "4", "--system", "cluster1", "/path/to/file"]
    result = runner.invoke(cli.app, args=args)
    stdout = common.clean_stdout(result.stdout)
    assert result.exit_code == 0
    assert "hello" not in stdout
    assert "llo" in stdout

    args = valid_credentials + ["tail", "-c", "4", "--system", "cluster1", "/path/to/file"]
    result = runner.invoke(cli.app, args=args)
    stdout = common.clean_stdout(result.stdout)
    assert result.exit_code == 0
    assert "hello" not in stdout
    assert "llo" in stdout


def test_view_invalid_arguments(valid_client):
    with pytest.raises(firecrest.HeaderException):
        valid_client.view("cluster1", "/path/to/invalid/file")


def test_view_invalid_machine(valid_client):
    with pytest.raises(firecrest.HeaderException):
        valid_client.view("cluster2", "/path/to/file")


def test_view_invalid_client(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        invalid_client.view("cluster1", "/path/to/file")


def test_whoami(valid_client):
    assert valid_client.whoami("cluster1") == "username"


def test_whoami_invalid_machine(valid_client):
    with pytest.raises(firecrest.HeaderException):
        valid_client.whoami("cluster2")


def test_whoami_invalid_client(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        invalid_client.whoami("cluster1")


def test_cli_whoami(valid_credentials):
    args = valid_credentials + ["whoami", "--system", "cluster1"]
    result = runner.invoke(cli.app, args=args)
    stdout = common.clean_stdout(result.stdout)
    assert result.exit_code == 0
    assert "username" in stdout


def test_groups(valid_client):
    assert valid_client.groups("cluster1") == {
        "group": {"id": "1000", "name": "group1"},
        "groups": [{"id": "1000", "name": "group1"}, {"id": "1001", "name": "group2"}],
        "user": {"id": "10000", "name": "test_user"},
    }


def test_groups_not_impl(valid_client):
    valid_client.set_api_version("1.14.0")
    with pytest.raises(firecrest.NotImplementedOnAPIversion):
        valid_client.groups(machine="cluster1")


def test_groups_invalid_machine(valid_client):
    with pytest.raises(firecrest.HeaderException):
        valid_client.groups("cluster2")


def test_groups_invalid_client(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        invalid_client.groups("cluster1")


def test_cli_id(valid_credentials):
    args = valid_credentials + ["id", "--system", "cluster1"]
    result = runner.invoke(cli.app, args=args)
    stdout = common.clean_stdout(result.stdout)
    assert result.exit_code == 0
    assert "uid=10000(test_user) gid=1000(group1) groups=1000(group1),1001(group2)" in stdout
