import json
import pytest
import re

from context_v2 import Firecrest, UnexpectedStatusException
from werkzeug.wrappers import Response
from werkzeug.wrappers import Request


def read_json_file(filename):
    with open(filename) as fp:
        data = json.load(fp)

    return data


@pytest.fixture
def valid_client(fc_server):
    class ValidAuthorization:
        def get_access_token(self):
            return "VALID_TOKEN"

    return Firecrest(
        firecrest_url=fc_server.url_for("/"),
        authorization=ValidAuthorization()
    )


@pytest.fixture
def invalid_client(fc_server):
    class InvalidAuthorization:
        def get_access_token(self):
            return "INVALID_TOKEN"

    return Firecrest(
        firecrest_url=fc_server.url_for("/"),
        authorization=InvalidAuthorization()
    )


@pytest.fixture
def fc_server(httpserver):
    httpserver.expect_request(
        re.compile("/status/.*"), method="GET"
    ).respond_with_handler(status_handler)

    httpserver.expect_request(
        re.compile("/filesystem/.*"), method="GET"
    ).respond_with_handler(filesystem_handler)

    return httpserver


def status_handler(request: Request):
    if request.headers["Authorization"] != "Bearer VALID_TOKEN":
        return Response(
            json.dumps({"message": "Bad token; invalid JSON"}),
            status=401,
            content_type="application/json",
        )

    endpoint = request.url.split("/")[-1]
    data = read_json_file(f"v2/responses/{endpoint}.json")

    ret = data["response"]
    ret_status = data["status_code"]

    return Response(json.dumps(ret),
                    status=ret_status,
                    content_type="application/json")


def filesystem_handler(request: Request):
    if request.headers["Authorization"] != "Bearer VALID_TOKEN":
        return Response(
            json.dumps({"message": "Bad token; invalid JSON"}),
            status=401,
            content_type="application/json",
        )

    url, params = request.url.split("?")
    endpoint = url.split("/")[-1]

    suffix = ""

    if endpoint == "head":
        if "bytes=8" in params:
            suffix = "_bytes"

        if "skipEnding=true" in params:
            suffix = "_bytes_exclude_trailing"

        if "lines=4" in params and "skipEnding=false" in params:
            suffix = "_lines"

        if "lines=4" in params and "skipEnding=true" in params:
            suffix = "_lines_exclude_trailing"

    if endpoint == "tail":
        if "bytes=8" in params:
            suffix = "_bytes"

        if "skipBeginning=true" in params:
            suffix = "_bytes_exclude_beginning"

        if "lines=4" in params and "skipBeginning=false" in params:
            suffix = "_lines"

        if "lines=4" in params and "skipBeginning=true" in params:
            suffix = "_lines_exclude_beginning"

    if endpoint == "ls":
        if "dereference=true" in params:
            suffix = "_dereference"

        if "showHidden=true" in params:
            suffix = "_hidden"

        if "recursive=true" in params:
            suffix = "_recursive"

        if "numericUid=true" in params:
            suffix = "_uid"

        if "path=/invalid/path" in params:
            suffix = "_invalid_path"

    if endpoint == "stat":
        if "dereference=true" in params:
            suffix = "_dereference"

    data = read_json_file(f"v2/responses/{endpoint}{suffix}.json")

    ret = data["response"]
    ret_status = data["status_code"]

    return Response(json.dumps(ret),
                    status=ret_status,
                    content_type="application/json")


def test_systems(valid_client):
    data = read_json_file("v2/responses/systems.json")
    resp = valid_client.systems()
    assert resp == data["response"]["systems"]


def test_partitions(valid_client):
    data = read_json_file("v2/responses/partitions.json")
    resp = valid_client.partitions("cluster")
    assert resp == data["response"]["partitions"]


def test_nodes(valid_client):
    data = read_json_file("v2/responses/nodes.json")
    resp = valid_client.nodes("cluster")
    assert resp == data["response"]["nodes"]


def test_reservations(valid_client):
    data = read_json_file("v2/responses/reservations.json")
    resp = valid_client.reservations("cluster")
    assert resp == data["response"]["reservations"]


def test_userinfo(valid_client):
    data = read_json_file("v2/responses/userinfo.json")
    resp = valid_client.userinfo("cluster")
    assert resp == data["response"]


def test_head(valid_client):
    data = read_json_file("v2/responses/head.json")
    resp = valid_client.head("cluster", "/path/to/file")
    assert resp == data["response"]["output"]


def test_head_bytes(valid_client):
    data = read_json_file("v2/responses/head_bytes.json")
    resp = valid_client.head("cluster", "/path/to/file", num_bytes=8)
    assert resp == data["response"]["output"]


def test_head_bytes_exclude_trailing(valid_client):
    data = read_json_file("v2/responses/head_bytes_exclude_trailing.json")
    resp = valid_client.head("cluster", "/path/to/file",
                             num_bytes=8, exclude_trailing=True)
    assert resp == data["response"]["output"]


def test_head_lines(valid_client):
    data = read_json_file("v2/responses/head_lines.json")
    resp = valid_client.head("cluster", "/path/to/file",
                             num_lines=4)
    assert resp == data["response"]["output"]


def test_head_lines_exclude_trailing(valid_client):
    data = read_json_file("v2/responses/head_lines_exclude_trailing.json")
    resp = valid_client.head("cluster", "/path/to/file",
                             exclude_trailing=True, num_lines=4)
    assert resp == data["response"]["output"]

def test_head_lines_and_bytes(valid_client):
    with pytest.raises(ValueError) as excinfo:
        valid_client.head("cluster", "/path/to/file", num_bytes=8,
                          num_lines=4)

    assert str(excinfo.value) == (
        "You cannot specify both `num_bytes` and `num_lines`."
    )


def test_tail(valid_client):
    data = read_json_file("v2/responses/tail.json")
    resp = valid_client.tail("cluster", "/path/to/file")
    assert resp == data["response"]["output"]


def test_tail_bytes(valid_client):
    data = read_json_file("v2/responses/tail_bytes.json")
    resp = valid_client.tail("cluster", "/path/to/file", num_bytes=8)
    assert resp == data["response"]["output"]


def test_tail_bytes_exclude_beginning(valid_client):
    data = read_json_file("v2/responses/tail_bytes_exclude_beginning.json")
    resp = valid_client.tail("cluster", "/path/to/file",
                             num_bytes=8, exclude_beginning=True)
    assert resp == data["response"]["output"]


def test_tail_lines(valid_client):
    data = read_json_file("v2/responses/tail_lines.json")
    resp = valid_client.tail("cluster", "/path/to/file",
                             num_lines=4)
    assert resp == data["response"]["output"]


def test_tail_lines_exclude_trailing(valid_client):
    data = read_json_file("v2/responses/tail_lines_exclude_beginning.json")
    resp = valid_client.tail("cluster", "/path/to/file",
                             exclude_beginning=True, num_lines=4)
    assert resp == data["response"]["output"]

def test_tail_lines_and_bytes(valid_client):
    with pytest.raises(ValueError) as excinfo:
        valid_client.tail("cluster", "/path/to/file", num_bytes=8,
                          num_lines=4)

    assert str(excinfo.value) == (
        "You cannot specify both `num_bytes` and `num_lines`."
    )


def test_ls(valid_client):
    data = read_json_file("v2/responses/ls.json")
    resp = valid_client.list_files("cluster", "/home/user")
    assert resp == data["response"]["output"]


def test_ls_dereference(valid_client):
    data = read_json_file("v2/responses/ls_dereference.json")
    resp = valid_client.list_files("cluster", "/home/user",
                                   dereference=True)
    assert resp == data["response"]["output"]


def test_ls_hidden(valid_client):
    data = read_json_file("v2/responses/ls_hidden.json")
    resp = valid_client.list_files("cluster", "/home/user",
                                   show_hidden=True)

    assert resp == data["response"]["output"]


def test_ls_recursive(valid_client):
    data = read_json_file("v2/responses/ls_recursive.json")
    resp = valid_client.list_files("cluster", "/home/user",
                                   recursive=True)

    assert resp == data["response"]["output"]


def test_ls_uid(valid_client):
    data = read_json_file("v2/responses/ls_uid.json")
    resp = valid_client.list_files("cluster", "/home/user",
                                   numeric_uid=True)

    assert resp == data["response"]["output"]


def test_ls_invalid_path(valid_client):
    data = read_json_file("v2/responses/ls_invalid_path.json")
    with pytest.raises(UnexpectedStatusException) as excinfo:
        valid_client.list_files("cluster", "/invalid/path")

    byte_content = excinfo.value.responses[-1].content
    decoded_string = byte_content.decode('utf-8')
    response_dict = json.loads(decoded_string)
    message = response_dict["message"]

    assert str(message) == (
        "ls: cannot access '/invalid/path': No such file or directory"
    )


def test_view(valid_client):
    data = read_json_file("v2/responses/view.json")
    resp = valid_client.view("cluster", "/home/user/file")

    assert resp == data["response"]["output"]


def test_stat(valid_client):
    data = read_json_file("v2/responses/stat.json")
    resp = valid_client.stat("cluster", "/home/user/file")

    assert resp == data["response"]["output"]


def test_stat_dereference(valid_client):
    data = read_json_file("v2/responses/stat_dereference.json")
    resp = valid_client.stat("cluster", "/home/user/file",
                             dereference=True)

    assert resp == data["response"]["output"]


def test_file_type(valid_client):
    data = read_json_file("v2/responses/file.json")
    resp = valid_client.file_type("cluster", "/home/user/file")

    assert resp == data["response"]["output"]


def test_checksum(valid_client):
    data = read_json_file("v2/responses/checksum.json")
    resp = valid_client.checksum("cluster", "/home/user/file")

    assert resp == data["response"]["output"]
