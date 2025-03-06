import json
import pytest
import re

from context_v2 import Firecrest, UnexpectedStatusException
from werkzeug.wrappers import Response
from werkzeug.wrappers import Request
from handlers import (fc_server,
                      filesystem_handler,
                      read_json_file,
                      status_handler,
                      submit_handler)


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


def test_tail_lines_exclude_beginning(valid_client):
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


def test_mkdir(valid_client):
    data = read_json_file("v2/responses/mkdir.json")
    resp = valid_client.mkdir("cluster", "/home/user/file")

    assert resp == data["response"]["output"]


def test_chown(valid_client):
    data = read_json_file("v2/responses/chown.json")
    resp = valid_client.chown("cluster", "/home/user/file",
                              "test1", "users")

    assert resp == data["response"]["output"]


def test_chown_not_permitted(valid_client):
    data = read_json_file("v2/responses/chown_not_permitted.json")
    with pytest.raises(UnexpectedStatusException) as excinfo:
        valid_client.chown("cluster", "/home/test1/xxx",
                           "test1", "users")

    assert str(excinfo.value) == (
        f"last request: 403 {data['response']}: expected status 200"
    )


def test_chmod(valid_client):
    data = read_json_file("v2/responses/chmod.json")
    resp = valid_client.chmod("cluster", "/home/user/xxx",
                              "777")

    assert resp == data["response"]["output"]


def test_rm(valid_client):
    data = read_json_file("v2/responses/rm.json")
    resp = valid_client.rm("cluster", "/home/user/file")

    assert resp == data["response"]# ["output"]


def test_job_info(valid_client):
    data = read_json_file("v2/responses/job_info.json")
    resp = valid_client.job_info("cluster")

    assert resp == data["response"]["jobs"]


def test_job_info_jobid(valid_client):
    data = read_json_file("v2/responses/job_info.json")
    resp = valid_client.job_info("cluster", "1")

    assert resp == data["response"]["jobs"]


def test_job_metadata(valid_client):
    data = read_json_file("v2/responses/job_metadata.json")
    resp = valid_client.job_metadata("cluster", "1")

    assert resp == data["response"]["jobs"]


def test_job_submit(valid_client):
    data = read_json_file("v2/responses/job_submit.json")
    resp = valid_client.submit("cluster", "/path/to/dir",
                               script_str="...")

    assert resp == data["response"]


def test_job_submit_no_script(valid_client):
    with pytest.raises(ValueError) as excinfo:
        valid_client.submit("cluster", "/path/to/dir")

    assert str(excinfo.value) == (
        "Exactly one of the arguments `script_str` or "
        "`script_local_path` must be set."
    )
