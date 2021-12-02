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


httpretty.enable(allow_net_connect=False, verbose=True)


def ls_callback(request, uri, response_headers):
    if request.headers["Authorization"] != "Bearer VALID_TOKEN":
        return [401, response_headers, '{"message": "Bad token; invalid JSON"}']

    if request.headers["X-Machine-Name"] != "cluster1":
        response_headers["X-Machine-Does-Not-Exist"] = "Machine does not exist"
        return [
            400,
            response_headers,
            '{"description": "Error on ls operation", "error": "Machine does not exist"}',
        ]

    print(request.querystring)
    targetPath = request.querystring.get("targetPath", [None])[0]
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
        showhidden = request.querystring.get("showhidden", [False])[0]
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

        return [200, response_headers, json.dumps(ret)]

    if targetPath == "/path/to/invalid/dir":
        response_headers["X-Invalid-Path"] = "path is an invalid path"
        return [400, response_headers, '{"description": "Error on ls operation"}']


def mkdir_callback(request, uri, response_headers):
    if request.headers["Authorization"] != "Bearer VALID_TOKEN":
        return [401, response_headers, '{"message": "Bad token; invalid JSON"}']

    if request.headers["X-Machine-Name"] != "cluster1":
        response_headers["X-Machine-Does-Not-Exist"] = "Machine does not exist"
        return [
            400,
            response_headers,
            '{"description": "Error on ls operation", "error": "Machine does not exist"}',
        ]

    target_path = request.parsed_body["targetPath"][0]
    p = request.parsed_body.get("p", [False])[0]
    if target_path == "path/to/valid/dir" or (
        target_path == "path/to/valid/dir/with/p" and p
    ):
        ret = {"description": "Success to mkdir file or directory.", "output": ""}
        status_code = 201
    else:
        response_headers[
            "X-Invalid-Path"
        ] = "sourcePath and/or targetPath are invalid paths"
        ret = {"description": "Error on mkdir operation"}
        status_code = 400

    return [status_code, response_headers, json.dumps(ret)]


def mv_callback(request, uri, response_headers):
    if request.headers["Authorization"] != "Bearer VALID_TOKEN":
        return [401, response_headers, '{"message": "Bad token; invalid JSON"}']

    if request.headers["X-Machine-Name"] != "cluster1":
        response_headers["X-Machine-Does-Not-Exist"] = "Machine does not exist"
        return [
            400,
            response_headers,
            '{"description": "Error on ls operation", "error": "Machine does not exist"}',
        ]

    source_path = request.parsed_body["sourcePath"][0]
    target_path = request.parsed_body["targetPath"][0]

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

    return [status_code, response_headers, json.dumps(ret)]


def chmod_callback(request, uri, response_headers):
    if request.headers["Authorization"] != "Bearer VALID_TOKEN":
        return [401, response_headers, '{"message": "Bad token; invalid JSON"}']

    if request.headers["X-Machine-Name"] != "cluster1":
        response_headers["X-Machine-Does-Not-Exist"] = "Machine does not exist"
        return [
            400,
            response_headers,
            '{"description": "Error on ls operation", "error": "Machine does not exist"}',
        ]

    target_path = request.parsed_body["targetPath"][0]
    mode = request.parsed_body["mode"][0]

    if target_path == "/path/to/valid/file" and mode == "777":
        ret = {
            "description": "Success to chmod file or directory.",
            "output": "mode of '/path/to/valid/file' changed from 0755 (rwxr-xr-x) to 0777 (rwxrwxrwx)",
        }
        status_code = 200
    else:
        # FIXME: FirecREST sets the X-Invalid-Path even when the problem is the mode argument
        response_headers["X-Invalid-Path"] = "path is an invalid path"
        ret = {"description": "Error on chmod operation"}
        status_code = 400

    return [status_code, response_headers, json.dumps(ret)]


def chown_callback(request, uri, response_headers):
    if request.headers["Authorization"] != "Bearer VALID_TOKEN":
        return [401, response_headers, '{"message": "Bad token; invalid JSON"}']

    if request.headers["X-Machine-Name"] != "cluster1":
        response_headers["X-Machine-Does-Not-Exist"] = "Machine does not exist"
        return [
            400,
            response_headers,
            '{"description": "Error on ls operation", "error": "Machine does not exist"}',
        ]

    target_path = request.parsed_body["targetPath"][0]
    owner = request.parsed_body.get("owner", [""])[0]
    group = request.parsed_body.get("group", [""])[0]

    if target_path == "/path/to/file" and owner == "new_owner" and group == "new_group":
        ret = {
            "description": "Success to chown file or directory.",
            "output": "changed ownership of '/path/to/file' from old_owner:old_group to new_owner:new_group",
        }
        status_code = 200
    else:
        response_headers["X-Invalid-Path"] = "path is an invalid path"
        ret = {"description": "Error on chown operation"}
        status_code = 400

    return [status_code, response_headers, json.dumps(ret)]


httpretty.register_uri(
    httpretty.GET, "http://firecrest.cscs.ch/utilities/ls", body=ls_callback
)

httpretty.register_uri(
    httpretty.POST, "http://firecrest.cscs.ch/utilities/mkdir", body=mkdir_callback
)

httpretty.register_uri(
    httpretty.PUT, "http://firecrest.cscs.ch/utilities/rename", body=mv_callback
)

httpretty.register_uri(
    httpretty.PUT, "http://firecrest.cscs.ch/utilities/chmod", body=chmod_callback
)

httpretty.register_uri(
    httpretty.PUT, "http://firecrest.cscs.ch/utilities/chown", body=chown_callback
)


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
        "cluster1", "/path/to/valid/dir", showhidden=True
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
