import common
import json
import pytest

from context_v2 import Firecrest, UnexpectedStatusException
from handlers import (
    auth_server,
    fc_server,
    read_json_file
)

from firecrest import __app_name__, __version__, cli2 as cli
from typer.testing import CliRunner


runner = CliRunner()


@pytest.fixture
def valid_credentials(fc_server, auth_server):
    return [
        f"--firecrest-url={fc_server.url_for('/')}",
        "--client-id=valid_id",
        "--client-secret=valid_secret",
        f"--token-url={auth_server.url_for('/auth/token')}",
        "--api-version=1.16.0",
    ]


def test_list_files(valid_credentials):
    args = valid_credentials + ["ls", "--system", "cluster1", "/home/user"]
    result = runner.invoke(cli.app, args=args)
    stdout = common.clean_stdout(result.stdout)
    assert result.exit_code == 0
    assert "bin" in stdout
    assert ".hiddenf" not in stdout


def test_list_files_hidden(valid_credentials):
    for alias in ["-a", "--show-hidden"]:
        args = valid_credentials + [
            "ls", "--system", "cluster1", "/home/user", alias
        ]
        result = runner.invoke(cli.app, args=args)
        stdout = common.clean_stdout(result.stdout)
        assert result.exit_code == 0
        assert "bin" in stdout
        assert ".bashrc" in stdout


def test_list_files_recursive(valid_credentials):
    for alias in ["-R", "--recursive"]:
        args = valid_credentials + [
            "ls", "--system", "cluster1", "/home/user", alias
        ]
        result = runner.invoke(cli.app, args=args)
        stdout = common.clean_stdout(result.stdout)
        assert result.exit_code == 0
        assert "/home/test1/bin" in stdout
        assert "/home/test1/bin/file" in stdout


def test_list_files_num_uid_gid(valid_credentials):
    for alias in ["-n", "--numeric-uid-gid"]:
        args = valid_credentials + [
            "ls", "--system", "cluster1", "/home/user", alias
        ]
        result = runner.invoke(cli.app, args=args)
        stdout = common.clean_stdout(result.stdout)
        assert result.exit_code == 0
        assert "bin" in stdout
        assert '"linkTarget": "bin/file"' in stdout


def test_list_files_dereference(valid_credentials):
    args = valid_credentials + [
        "ls", "--system", "cluster1", "/home/user", "-L"
    ]
    result = runner.invoke(cli.app, args=args)
    stdout = common.clean_stdout(result.stdout)
    assert result.exit_code == 0
    assert "bin" in stdout
    assert '"linkTarget": "bin/file"' not in stdout
    assert '"linkTarget": null' in stdout


def test_systems(valid_credentials):
    args = valid_credentials + [
        "systems"
    ]
    result = runner.invoke(cli.app, args=args)
    stdout = common.clean_stdout(result.stdout)
    assert result.exit_code == 0
    assert "cluster-api" in stdout
    assert "cluster-ssh" in stdout


def test_partitions(valid_credentials):
    args = valid_credentials + [
       "get-partitions", "--system", "cluster"
    ]
    result = runner.invoke(cli.app, args=args)
    stdout = common.clean_stdout(result.stdout)
    print(stdout)
    assert result.exit_code == 0
    assert "part01" in stdout
    assert "part02" in stdout
    assert "xfer" in stdout


def test_nodes(valid_credentials):
    args = valid_credentials + [
        "get-nodes", "--system", "cluster"
    ]
    result = runner.invoke(cli.app, args=args)
    stdout = common.clean_stdout(result.stdout)
    assert result.exit_code == 0
    assert "part01" in stdout
    assert "part02" in stdout
    assert "xfer" in stdout
    assert '"sockets": 2' in stdout


def test_reservations(valid_credentials):
    args = valid_credentials + [
        "get-nodes", "--system", "cluster"
    ]
    result = runner.invoke(cli.app, args=args)
    stdout = common.clean_stdout(result.stdout)
    assert result.exit_code == 0
    assert "part01" in stdout
    assert "part02" in stdout
    assert "xfer" in stdout
    assert '"sockets": 2' in stdout


def test_id(valid_credentials):
    args = valid_credentials + [
        "id", "--system", "cluster"
    ]
    result = runner.invoke(cli.app, args=args)
    stdout = common.clean_stdout(result.stdout)
    assert result.exit_code == 0
    assert "fireuser" in stdout
    assert "1000" in stdout


def test_head(valid_credentials):
    args = valid_credentials + [
        "head", "--system", "cluster", "/path/to/file", "--json"
    ]
    result = runner.invoke(cli.app, args=args)
    stdout = common.clean_stdout(result.stdout)
    assert result.exit_code == 0
    assert '"content": "1\\n2\\n3\\n4\\n5\\n6\\n7\\n8\\n9\\n10\\n"' in stdout
    assert '"contentType": "lines"' in stdout
    assert '"startPosition": 0' in stdout
    assert '"endPosition": 10' in stdout

    args = valid_credentials + [
        "head", "--system", "cluster", "/home/user/file"
    ]
    result = runner.invoke(cli.app, args=args)
    stdout = common.clean_stdout(result.stdout)
    assert result.exit_code == 0
    assert "1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n" in stdout


def test_head_bytes(valid_credentials):
    for alias in ["-c", "--bytes"]:
        args = valid_credentials + [
            "head", "--system", "cluster", "/path/to/file", alias, "8", "--json"
        ]
        result = runner.invoke(cli.app, args=args)
        stdout = common.clean_stdout(result.stdout)
        assert result.exit_code == 0
        assert '"content": "1\\n2\\n3\\n4"' in stdout
        assert '"contentType": "bytes"' in stdout
        assert '"startPosition": 0' in stdout
        assert '"endPosition": 7' in stdout


def test_head_bytes_exclude_trailing(valid_credentials):
    for alias in ["-c", "--bytes"]:
        args = valid_credentials + [
            "head",
            "--system",
            "cluster",
            "/path/to/file",
            alias,
            "-8",
            "--json"
        ]
        result = runner.invoke(cli.app, args=args)
        stdout = common.clean_stdout(result.stdout)
        assert result.exit_code == 0
        assert '"content": "1\\n2\\n3\\n4\\n5\\n6\\n7\\n8\\n9\\n10"' in stdout
        assert '"contentType": "bytes"' in stdout
        assert '"startPosition": 0' in stdout
        assert '"endPosition": -7' in stdout


def test_head_lines(valid_credentials):
    for alias in ["-n", "--lines"]:
        args = valid_credentials + [
            "head", "--system", "cluster", "/path/to/file", alias, "4", "--json"
        ]
        result = runner.invoke(cli.app, args=args)
        stdout = common.clean_stdout(result.stdout)
        assert result.exit_code == 0
        assert '"content": "1\\n2\\n3\\n"' in stdout
        assert '"contentType": "lines"' in stdout
        assert '"startPosition": 0' in stdout
        assert '"endPosition": 3' in stdout


def test_head_lines_exclude_trailing(valid_credentials):
    for alias in ["-n", "--lines"]:
        args = valid_credentials + [
            "head", "--system", "cluster", "/path/to/file", alias, "-4", "--json"
        ]
        result = runner.invoke(cli.app, args=args)
        stdout = common.clean_stdout(result.stdout)
        assert result.exit_code == 0
        assert '"1\\n2\\n3\\n4\\n5\\n6\\n7\\n8\\n9\\n"' in stdout
        assert '"contentType": "lines"' in stdout
        assert '"startPosition": 0' in stdout
        assert '"endPosition": -3' in stdout


def test_head_lines_and_bytes(valid_credentials):
    args = valid_credentials + [
        "head",
        "--system",
        "cluster",
        "/path/to/file",
        "--bytes", "-4",
        "--lines", "-4",
        "--json"
    ]
    result = runner.invoke(cli.app, args=args)
    assert result.exit_code == 1
    assert "head: cannot specify both 'bytes' and 'lines'" in result.stdout


def test_tail(valid_credentials):
    args = valid_credentials + [
        "tail", "--system", "cluster", "/path/to/file", "--json"
    ]
    result = runner.invoke(cli.app, args=args)
    stdout = common.clean_stdout(result.stdout)
    assert result.exit_code == 0
    assert '"content": "3\\n4\\n5\\n6\\n7\\n8\\n9\\n10\\n11\\n12\\n"' in stdout
    assert '"contentType": "lines"' in stdout
    assert '"startPosition": 10' in stdout
    assert '"endPosition": -1' in stdout

    args = valid_credentials + [
        "tail", "--system", "cluster", "/home/user/file"
    ]
    result = runner.invoke(cli.app, args=args)
    stdout = common.clean_stdout(result.stdout)
    assert result.exit_code == 0
    assert "3\n4\n5\n6\n7\n8\n9\n10\n11\n12\n" in stdout


def test_tail_bytes(valid_credentials):
    for alias in ["-c", "--bytes"]:
        args = valid_credentials + [
            "tail",
            "--system",
            "cluster",
            "/path/to/file",
            alias,
            "8",
            "--json"
        ]
        result = runner.invoke(cli.app, args=args)
        stdout = common.clean_stdout(result.stdout)
        assert result.exit_code == 0
        assert '"content": "\\n11\\n12\\n"' in stdout
        assert '"contentType": "bytes"' in stdout
        assert '"startPosition": -7' in stdout
        assert '"endPosition": -1' in stdout


def test_tail_bytes_exclude_beginning(valid_credentials):
    for alias in ["-c", "--bytes"]:
        args = valid_credentials + [
            "tail",
            "--system",
            "cluster",
            "/path/to/file",
            alias,
            "+8",
            "--json"
        ]
        result = runner.invoke(cli.app, args=args)
        stdout = common.clean_stdout(result.stdout)
        assert result.exit_code == 0
        assert '"content": "4\\n5\\n6\\n7\\n8\\n9\\n10\\n11\\n12\\n"' in stdout
        assert '"contentType": "bytes"' in stdout
        assert '"startPosition": 7' in stdout
        assert '"endPosition": -1' in stdout


def test_tail_lines(valid_credentials):
    for alias in ["-n", "--lines"]:
        args = valid_credentials + [
            "tail", "--system", "cluster", "/path/to/file", alias, "4", "--json"
        ]
        result = runner.invoke(cli.app, args=args)
        stdout = common.clean_stdout(result.stdout)
        assert result.exit_code == 0
        assert '"content": "10\\n11\\n12\\n"' in stdout
        assert '"contentType": "lines"' in stdout
        assert '"startPosition": -3' in stdout
        assert '"endPosition": -1' in stdout


def test_tail_lines_exclude_beginning(valid_credentials):
    for alias in ["-n", "--lines"]:
        args = valid_credentials + [
            "tail", "--system", "cluster", "/path/to/file", alias, "+4", "--json"
        ]
        result = runner.invoke(cli.app, args=args)
        stdout = common.clean_stdout(result.stdout)
        assert result.exit_code == 0
        assert '"content": "3\\n4\\n5\\n6\\n7\\n8\\n9\\n10\\n11\\n12\\n"' in stdout
        assert '"contentType": "lines"' in stdout
        assert '"startPosition": 3' in stdout
        assert '"endPosition": -1' in stdout


def test_tail_lines_and_bytes(valid_credentials):
    args = valid_credentials + [
        "tail",
        "--system",
        "cluster",
        "/path/to/file",
        "--bytes", "4",
        "--lines", "4",
        "--json"
    ]
    result = runner.invoke(cli.app, args=args)
    assert result.exit_code == 1
    assert "tail: cannot specify both 'bytes' and 'lines'" in result.stdout


# def test_ls(valid_credentials):
#     args = valid_credentials + [
#         "ls",
#         "--system",
#         "cluster",
#         "/home/user",
#         "--json"
#     ]
#     result = runner.invoke(cli.app, args=args)
#     stdout = common.clean_stdout(result.stdout)
#     print(stdout)
#     assert result.exit_code == 0
#     assert "file" in result.stdout


# def test_ls_dereference(valid_client):
#     data = read_json_file("v2/responses/ls_dereference.json")
#     resp = valid_client.list_files("cluster", "/home/user",
#                                    dereference=True)
#     assert resp == data["response"]["output"]


# def test_ls_hidden(valid_client):
#     data = read_json_file("v2/responses/ls_hidden.json")
#     resp = valid_client.list_files("cluster", "/home/user",
#                                    show_hidden=True)

#     assert resp == data["response"]["output"]


# def test_ls_recursive(valid_client):
#     data = read_json_file("v2/responses/ls_recursive.json")
#     resp = valid_client.list_files("cluster", "/home/user",
#                                    recursive=True)

#     assert resp == data["response"]["output"]


# def test_ls_uid(valid_client):
#     data = read_json_file("v2/responses/ls_uid.json")
#     resp = valid_client.list_files("cluster", "/home/user",
#                                    numeric_uid=True)

#     assert resp == data["response"]["output"]


# def test_ls_invalid_path(valid_client):
#     data = read_json_file("v2/responses/ls_invalid_path.json")
#     with pytest.raises(UnexpectedStatusException) as excinfo:
#         valid_client.list_files("cluster", "/invalid/path")

#     byte_content = excinfo.value.responses[-1].content
#     decoded_string = byte_content.decode('utf-8')
#     response_dict = json.loads(decoded_string)
#     message = response_dict["message"]

#     assert str(message) == (
#         "ls: cannot access '/invalid/path': No such file or directory"
#     )


# def test_view(valid_client):
#     data = read_json_file("v2/responses/view.json")
#     resp = valid_client.view("cluster", "/home/user/file")

#     assert resp == data["response"]["output"]


# def test_stat(valid_client):
#     data = read_json_file("v2/responses/stat.json")
#     resp = valid_client.stat("cluster", "/home/user/file")

#     assert resp == data["response"]["output"]


# def test_stat_dereference(valid_client):
#     data = read_json_file("v2/responses/stat_dereference.json")
#     resp = valid_client.stat("cluster", "/home/user/file",
#                              dereference=True)

#     assert resp == data["response"]["output"]


# def test_file_type(valid_client):
#     data = read_json_file("v2/responses/file.json")
#     resp = valid_client.file_type("cluster", "/home/user/file")

#     assert resp == data["response"]["output"]


# def test_checksum(valid_client):
#     data = read_json_file("v2/responses/checksum.json")
#     resp = valid_client.checksum("cluster", "/home/user/file")

#     assert resp == data["response"]["output"]


# def test_mkdir(valid_client):
#     data = read_json_file("v2/responses/mkdir.json")
#     resp = valid_client.mkdir("cluster", "/home/user/file")

#     assert resp == data["response"]["output"]


# def test_chown(valid_client):
#     data = read_json_file("v2/responses/chown.json")
#     resp = valid_client.chown("cluster", "/home/user/file",
#                               "test1", "users")

#     assert resp == data["response"]["output"]


# def test_chown_not_permitted(valid_client):
#     data = read_json_file("v2/responses/chown_not_permitted.json")
#     with pytest.raises(UnexpectedStatusException) as excinfo:
#         valid_client.chown("cluster", "/home/test1/xxx",
#                            "test1", "users")

#     assert str(excinfo.value) == (
#         f"last request: 403 {data['response']}: expected status 200"
#     )


# def test_chmod(valid_client):
#     data = read_json_file("v2/responses/chmod.json")
#     resp = valid_client.chmod("cluster", "/home/user/xxx",
#                               "777")

#     assert resp == data["response"]["output"]


# def test_rm(valid_client):
#     data = read_json_file("v2/responses/rm.json")
#     resp = valid_client.rm("cluster", "/home/user/file")

#     assert resp == data["response"]# ["output"]


# def test_job_info(valid_client):
#     data = read_json_file("v2/responses/job_info.json")
#     resp = valid_client.job_info("cluster")

#     assert resp == data["response"]["jobs"]


# def test_job_info_jobid(valid_client):
#     data = read_json_file("v2/responses/job_info.json")
#     resp = valid_client.job_info("cluster", "1")

#     assert resp == data["response"]["jobs"]


# def test_job_metadata(valid_client):
#     data = read_json_file("v2/responses/job_metadata.json")
#     resp = valid_client.job_metadata("cluster", "1")

#     assert resp == data["response"]["jobs"]


# def test_job_submit(valid_client):
#     data = read_json_file("v2/responses/job_submit.json")
#     resp = valid_client.submit("cluster", "/path/to/dir",
#                                script_str="...")

#     assert resp == data["response"]


# def test_job_submit_no_script(valid_client):
#     with pytest.raises(ValueError) as excinfo:
#         valid_client.submit("cluster", "/path/to/dir")

#     assert str(excinfo.value) == (
#         "Exactly one of the arguments `script_str` or "
#         "`script_local_path` must be set."
#     )
