import common
import pytest

from handlers import (
    auth_server,
    fc_server,
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
    args = valid_credentials + [
        "ls",
        "--system",
        "cluster1",
        "/home/user",
    ]
    result = runner.invoke(cli.app, args=args)
    stdout = common.clean_stdout(result.stdout)
    assert result.exit_code == 0
    assert "bin" in stdout
    assert ".hiddenf" not in stdout


def test_list_files_hidden(valid_credentials):
    for alias in ["-a", "--show-hidden"]:
        args = valid_credentials + [
            "ls",
            "--system",
            "cluster1",
            "/home/user",
            alias,
        ]
        result = runner.invoke(cli.app, args=args)
        stdout = common.clean_stdout(result.stdout)
        assert result.exit_code == 0
        assert "bin" in stdout
        assert ".bashrc" in stdout


def test_list_files_recursive(valid_credentials):
    for alias in ["-R", "--recursive"]:
        args = valid_credentials + [
            "ls",
            "--system",
            "cluster1",
            "/home/user",
            alias,
        ]
        result = runner.invoke(cli.app, args=args)
        stdout = common.clean_stdout(result.stdout)
        assert result.exit_code == 0
        assert "/home/test1/bin" in stdout
        assert "/home/test1/bin/file" in stdout


def test_list_files_num_uid_gid(valid_credentials):
    for alias in ["-n", "--numeric-uid-gid"]:
        args = valid_credentials + [
            "ls",
            "--system",
            "cluster1",
            "/home/user",
            alias,
        ]
        result = runner.invoke(cli.app, args=args)
        stdout = common.clean_stdout(result.stdout)
        assert result.exit_code == 0
        assert "bin" in stdout
        assert '"linkTarget": "bin/file"' in stdout


def test_list_files_dereference(valid_credentials):
    args = valid_credentials + [
        "ls",
        "--system",
        "cluster1",
        "/home/user",
        "-L",
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
    stdout = common.clean_stdout(result.stdout)
    assert result.exit_code == 1
    assert "head: cannot specify both 'bytes' and 'lines'" in stdout


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
    stdout = common.clean_stdout(result.stdout)
    assert result.exit_code == 1
    assert "tail: cannot specify both 'bytes' and 'lines'" in stdout


def test_stat(valid_credentials):
    args = valid_credentials + [
        "stat",
        "--system",
        "cluster",
        "/home/user/file",
    ]
    result = runner.invoke(cli.app, args=args)
    stdout = common.clean_stdout(result.stdout)
    assert result.exit_code == 0
    for fields in [
        "mode", "ino", "dev", "nlink", "uid", "gid",
        "size", "atime", "mtime", "ctime"
    ]:
        assert fields in stdout

    assert '"uid": 0' in stdout


def test_stat_dereference(valid_credentials):
    for alias in ["-L", "--dereference"]:
        args = valid_credentials + [
            "stat",
            "--system",
            "cluster",
            "/home/user/file",
            alias
        ]
        result = runner.invoke(cli.app, args=args)
        stdout = common.clean_stdout(result.stdout)
        assert result.exit_code == 0
        for fields in [
            "mode", "ino", "dev", "nlink", "uid", "gid",
            "size", "atime", "mtime", "ctime"
        ]:
            assert fields in stdout

        assert '"uid": 1000' in stdout


def test_file_type(valid_credentials):
    args = valid_credentials + [
        "file",
        "--system",
        "cluster",
        "/home/user/file",
    ]
    result = runner.invoke(cli.app, args=args)
    stdout = common.clean_stdout(result.stdout)
    assert result.exit_code == 0
    assert "ASCII text" in stdout


def test_checksum(valid_credentials):
    args = valid_credentials + [
        "checksum",
        "--system",
        "cluster",
        "/home/user/file",
    ]
    result = runner.invoke(cli.app, args=args)
    stdout = common.clean_stdout(result.stdout)
    assert result.exit_code == 0
    assert "67149111d45cf106eb92ab5be7ec08179bddea7426ddde7cfe0ae68a7cffce74" in stdout
    assert '"algorithm": "SHA256"' in stdout


def test_mkdir(valid_credentials):
    args = valid_credentials + [
        "mkdir",
        "--system",
        "cluster",
        "/home/user/directory",
    ]
    result = runner.invoke(cli.app, args=args)
    assert result.exit_code == 0


def test_chown(valid_credentials):
    args = valid_credentials + [
        "chown",
        "--system",
        "cluster",
        "/home/user/directory",
    ]
    result = runner.invoke(cli.app, args=args)
    assert result.exit_code == 0


def test_chown_not_permitted(valid_credentials):
    args = valid_credentials + [
        "chown",
        "--system",
        "cluster",
        "/home/test1/xxx",
        "--owner",
        "test1",
        "--group",
        "users"
    ]
    result = runner.invoke(cli.app, args=args)
    stdout = common.clean_stdout(result.stdout)
    assert result.exit_code == 1
    assert "chown: changing ownership" in stdout
    assert "Operation not permitted" in stdout


def test_chmod(valid_credentials):
    args = valid_credentials + [
        "chmod",
        "--system",
        "cluster",
        "/home/test1/xxx",
        "777"
    ]
    result = runner.invoke(cli.app, args=args)
    assert result.exit_code == 0


def test_rm(valid_credentials):
    args = valid_credentials + [
        "rm",
        "--system",
        "cluster",
        "/home/user/file",
        "--force"
    ]
    result = runner.invoke(cli.app, args=args)
    assert result.exit_code == 0


def test_job_info(valid_credentials):
    args = valid_credentials + [
        "job-info",
        "--system",
        "cluster",
    ]
    result = runner.invoke(cli.app, args=args)
    stdout = common.clean_stdout(result.stdout)
    assert result.exit_code == 0
    assert '"jobId": 1,' in stdout
    assert '"name": "allocation"' in stdout


def test_job_metadata(valid_credentials):
    args = valid_credentials + [
        "job-metadata",
        "--system",
        "cluster",
        "26",
    ]
    result = runner.invoke(cli.app, args=args)
    stdout = common.clean_stdout(result.stdout)
    assert result.exit_code == 0
    assert '"jobId": 26' in stdout
    assert '"script": "#!/bin/sh\\npwd"' in stdout
    for field in [
        "jobId",
        "script",
        "standardInput",
        "standardOutput",
        "standardError",
    ]:
        assert field in stdout
