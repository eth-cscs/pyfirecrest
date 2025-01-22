import pytest
import test_utilities as basic_utilities

from context import firecrest

from firecrest import __app_name__, __version__


@pytest.fixture
def valid_client(fc_server):
    class ValidAuthorization:
        def get_access_token(self):
            return "VALID_TOKEN"

    client = firecrest.v1.AsyncFirecrest(
        firecrest_url=fc_server.url_for("/"), authorization=ValidAuthorization()
    )
    client.time_between_calls = {
        "compute": 0,
        "reservations": 0,
        "status": 0,
        "storage": 0,
        "tasks": 0,
        "utilities": 0,
    }
    client.set_api_version("1.16.0")

    return client


@pytest.fixture
def invalid_client(fc_server):
    class InvalidAuthorization:
        def get_access_token(self):
            return "INVALID_TOKEN"

    client = firecrest.v1.AsyncFirecrest(
        firecrest_url=fc_server.url_for("/"), authorization=InvalidAuthorization()
    )
    client.time_between_calls = {
        "compute": 0,
        "reservations": 0,
        "status": 0,
        "storage": 0,
        "tasks": 0,
        "utilities": 0,
    }
    client.set_api_version("1.16.0")

    return client


@pytest.fixture
def fc_server(httpserver):
    httpserver.expect_request("/utilities/ls", method="GET").respond_with_handler(
        basic_utilities.ls_handler
    )

    httpserver.expect_request("/utilities/mkdir", method="POST").respond_with_handler(
        basic_utilities.mkdir_handler
    )

    httpserver.expect_request("/utilities/rename", method="PUT").respond_with_handler(
        basic_utilities.mv_handler
    )

    httpserver.expect_request("/utilities/chmod", method="PUT").respond_with_handler(
        basic_utilities.chmod_handler
    )

    httpserver.expect_request("/utilities/chown", method="PUT").respond_with_handler(
        basic_utilities.chown_handler
    )

    httpserver.expect_request("/utilities/copy", method="POST").respond_with_handler(
        basic_utilities.copy_handler
    )

    httpserver.expect_request("/utilities/compress", method="POST").respond_with_handler(
        basic_utilities.compress_handler
    )

    httpserver.expect_request("/utilities/extract", method="POST").respond_with_handler(
        basic_utilities.extract_handler
    )

    httpserver.expect_request("/utilities/file", method="GET").respond_with_handler(
        basic_utilities.file_type_handler
    )

    httpserver.expect_request("/utilities/stat", method="GET").respond_with_handler(
        basic_utilities.stat_handler
    )

    httpserver.expect_request("/utilities/symlink", method="POST").respond_with_handler(
        basic_utilities.symlink_handler
    )

    httpserver.expect_request("/utilities/download", method="GET").respond_with_handler(
        basic_utilities.simple_download_handler
    )

    httpserver.expect_request("/utilities/upload", method="POST").respond_with_handler(
        basic_utilities.simple_upload_handler
    )

    httpserver.expect_request("/utilities/rm", method="DELETE").respond_with_handler(
        basic_utilities.simple_delete_handler
    )

    httpserver.expect_request("/utilities/checksum", method="GET").respond_with_handler(
        basic_utilities.checksum_handler
    )

    httpserver.expect_request("/utilities/view", method="GET").respond_with_handler(
        basic_utilities.view_handler
    )

    httpserver.expect_request("/utilities/head", method="GET").respond_with_handler(
        basic_utilities.head_tail_handler
    )

    httpserver.expect_request("/utilities/tail", method="GET").respond_with_handler(
        basic_utilities.head_tail_handler
    )

    httpserver.expect_request("/utilities/whoami", method="GET").respond_with_handler(
        basic_utilities.whoami_handler
    )

    return httpserver


@pytest.mark.asyncio
async def test_list_files(valid_client):
    assert await valid_client.list_files("cluster1", "/path/to/valid/dir") == [
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

    assert await valid_client.list_files(
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


@pytest.mark.asyncio
async def test_list_files_invalid_path(valid_client):
    with pytest.raises(firecrest.FirecrestException):
        await valid_client.list_files("cluster1", "/path/to/invalid/dir")


@pytest.mark.asyncio
async def test_list_files_invalid_machine(valid_client):
    with pytest.raises(firecrest.HeaderException):
        await valid_client.list_files("cluster2", "/path/to/dir")


@pytest.mark.asyncio
async def test_list_files_invalid_client(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        await invalid_client.list_files("cluster1", "/path/to/dir")


@pytest.mark.asyncio
async def test_mkdir(valid_client):
    # Make sure these don't raise an error
    await valid_client.mkdir("cluster1", "path/to/valid/dir")
    await valid_client.mkdir("cluster1", "path/to/valid/dir/with/p", p=True)


@pytest.mark.asyncio
async def test_mkdir_invalid_path(valid_client):
    with pytest.raises(firecrest.FirecrestException):
        await valid_client.mkdir("cluster1", "/path/to/invalid/dir")


@pytest.mark.asyncio
async def test_mkdir_invalid_machine(valid_client):
    with pytest.raises(firecrest.HeaderException):
        await valid_client.mkdir("cluster2", "path/to/dir")


@pytest.mark.asyncio
async def test_mkdir_invalid_client(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        await invalid_client.mkdir("cluster1", "path/to/dir")


@pytest.mark.asyncio
async def test_mv(valid_client):
    # Make sure this doesn't raise an error
    await valid_client.mv("cluster1", "/path/to/valid/source", "/path/to/valid/destination")


@pytest.mark.asyncio
async def test_mv_invalid_paths(valid_client):
    with pytest.raises(firecrest.FirecrestException):
        await valid_client.mv(
            "cluster1", "/path/to/invalid/source", "/path/to/valid/destination"
        )

    with pytest.raises(firecrest.FirecrestException):
        await valid_client.mv(
            "cluster1", "/path/to/valid/source", "/path/to/invalid/destination"
        )


@pytest.mark.asyncio
async def test_mv_invalid_machine(valid_client):
    with pytest.raises(firecrest.HeaderException):
        await valid_client.mv("cluster2", "/path/to/source", "/path/to/destination")


@pytest.mark.asyncio
async def test_mv_invalid_client(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        await invalid_client.mv("cluster1", "/path/to/source", "/path/to/destination")


@pytest.mark.asyncio
async def test_chmod(valid_client):
    # Make sure this doesn't raise an error
    await valid_client.chmod("cluster1", "/path/to/valid/file", "777")


@pytest.mark.asyncio
async def test_chmod_invalid_arguments(valid_client):
    with pytest.raises(firecrest.FirecrestException):
        await valid_client.chmod("cluster1", "/path/to/invalid/file", "777")

    with pytest.raises(firecrest.FirecrestException):
        await valid_client.chmod("cluster1", "/path/to/valid/file", "random_string")


@pytest.mark.asyncio
async def test_chmod_invalid_machine(valid_client):
    with pytest.raises(firecrest.HeaderException):
        await valid_client.chmod("cluster2", "/path/to/file", "700")


@pytest.mark.asyncio
async def test_chmod_invalid_client(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        await invalid_client.chmod("cluster1", "/path/to/file", "600")


@pytest.mark.asyncio
async def test_chown(valid_client):
    # Make sure this doesn't raise an error
    await valid_client.chown(
        "cluster1", "/path/to/file", owner="new_owner", group="new_group"
    )
    # Call will immediately return if neither owner and nor group are set
    await valid_client.chown("cluster", "path")


@pytest.mark.asyncio
async def test_chown_invalid_arguments(valid_client):
    with pytest.raises(firecrest.HeaderException):
        await valid_client.chown(
            "cluster1", "/bad/path", owner="new_owner", group="new_group"
        )

    with pytest.raises(firecrest.HeaderException):
        await valid_client.chown(
            "cluster1", "/path/to/file", owner="bad_owner", group="new_group"
        )

    with pytest.raises(firecrest.HeaderException):
        await valid_client.chown(
            "cluster1", "/path/to/file", owner="new_owner", group="bad_group"
        )


@pytest.mark.asyncio
async def test_chown_invalid_machine(valid_client):
    with pytest.raises(firecrest.HeaderException):
        await valid_client.chown(
            "cluster2", "/path/to/file", owner="new_owner", group="new_group"
        )


@pytest.mark.asyncio
async def test_chown_invalid_client(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        await invalid_client.chown(
            "cluster1", "/path/to/file", owner="new_owner", group="new_group"
        )


@pytest.mark.asyncio
async def test_copy(valid_client):
    # Make sure this doesn't raise an error
    await valid_client.copy("cluster1", "/path/to/valid/source", "/path/to/valid/destination")


@pytest.mark.asyncio
async def test_copy_invalid_arguments(valid_client):
    with pytest.raises(firecrest.HeaderException):
        await valid_client.copy(
            "cluster1", "/path/to/invalid/source", "/path/to/valid/destination"
        )

    with pytest.raises(firecrest.HeaderException):
        await valid_client.copy(
            "cluster1", "/path/to/valid/source", "/path/to/invalid/destination"
        )


@pytest.mark.asyncio
async def test_copy_invalid_machine(valid_client):
    with pytest.raises(firecrest.HeaderException):
        await valid_client.copy("cluster2", "/path/to/source", "/path/to/destination")


@pytest.mark.asyncio
async def test_copy_invalid_client(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        await invalid_client.copy("cluster1", "/path/to/source", "/path/to/destination")


@pytest.mark.asyncio
async def test_compress(valid_client):
    # Mostly sure this doesn't raise an error
    assert (
       await valid_client.compress(
            "cluster1",
            "/path/to/valid/source",
            "/path/to/valid/destination.tar.gz"
        ) == "/path/to/valid/destination.tar.gz"
    )


@pytest.mark.asyncio
async def test_compress_not_impl(valid_client):
    valid_client.set_api_version("1.15.0")
    with pytest.raises(firecrest.NotImplementedOnAPIversion):
        await valid_client.compress(
            "cluster1",
            "/path/to/valid/source",
            "/path/to/valid/destination.tar.gz"
        )


@pytest.mark.asyncio
async def test_extract(valid_client):
    # Mostly sure this doesn't raise an error
    assert (
        await valid_client.extract(
            "cluster1",
            "/path/to/valid/source.tar.gz",
            "/path/to/valid/destination"
        ) == "/path/to/valid/destination"
    )


@pytest.mark.asyncio
async def test_extract_not_impl(valid_client):
    valid_client.set_api_version("1.15.0")
    with pytest.raises(firecrest.NotImplementedOnAPIversion):
        await valid_client.extract(
            "cluster1",
            "/path/to/valid/source.tar.gz",
            "/path/to/valid/destination"
        )


@pytest.mark.asyncio
async def test_file_type(valid_client):
    assert await valid_client.file_type("cluster1", "/path/to/empty/file") == "empty"
    assert await valid_client.file_type("cluster1", "/path/to/directory") == "directory"


@pytest.mark.asyncio
async def test_file_type_invalid_arguments(valid_client):
    with pytest.raises(firecrest.HeaderException):
        await valid_client.file_type("cluster1", "/path/to/invalid/file")


@pytest.mark.asyncio
async def test_file_type_invalid_machine(valid_client):
    with pytest.raises(firecrest.HeaderException):
        await valid_client.file_type("cluster2", "/path/to/file")


@pytest.mark.asyncio
async def test_file_type_invalid_client(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        await invalid_client.file_type("cluster1", "/path/to/file")


@pytest.mark.asyncio
async def test_stat(valid_client):
    assert await valid_client.stat("cluster1", "/path/to/link") == {
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
    assert await valid_client.stat("cluster1", "/path/to/link", dereference=False) == {
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
    assert await valid_client.stat("cluster1", "/path/to/link", dereference=True) == {
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


@pytest.mark.asyncio
async def test_stat_invalid_arguments(valid_client):
    with pytest.raises(firecrest.HeaderException):
        await valid_client.stat("cluster1", "/path/to/invalid/file")


@pytest.mark.asyncio
async def test_stat_invalid_machine(valid_client):
    with pytest.raises(firecrest.HeaderException):
        await valid_client.stat("cluster2", "/path/to/file")


@pytest.mark.asyncio
async def test_stat_invalid_client(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        await invalid_client.stat("cluster1", "/path/to/file")


@pytest.mark.asyncio
async def test_symlink(valid_client):
    # Make sure this doesn't raise an error
    await valid_client.symlink("cluster1", "/path/to/file", "/path/to/link")


@pytest.mark.asyncio
async def test_symlink_invalid_arguments(valid_client):
    with pytest.raises(firecrest.HeaderException):
        await valid_client.symlink("cluster1", "/path/to/invalid/file", "/path/to/link")

    with pytest.raises(firecrest.HeaderException):
        await valid_client.symlink("cluster1", "/path/to/file", "/path/to/invalid/link")


@pytest.mark.asyncio
async def test_symlink_invalid_machine(valid_client):
    with pytest.raises(firecrest.HeaderException):
        await valid_client.symlink("cluster2", "/path/to/file", "/path/to/link")


@pytest.mark.asyncio
async def test_symlink_invalid_client(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        await invalid_client.symlink("cluster1", "/path/to/file", "/path/to/link")


@pytest.mark.asyncio
async def test_simple_download(valid_client, tmp_path):
    tmp_dir = tmp_path / "download_dir"
    tmp_dir.mkdir()
    local_file = tmp_dir / "hello.txt"
    # Make sure this doesn't raise an error
    await valid_client.simple_download("cluster1", "/path/to/remote/source", local_file)

    with open(local_file) as f:
        assert f.read() == "Hello!\n"


@pytest.mark.asyncio
async def test_simple_download_invalid_arguments(valid_client):
    with pytest.raises(firecrest.HeaderException):
        await valid_client.simple_download(
            "cluster1", "/path/to/invalid/file", "/path/to/local/destination"
        )


@pytest.mark.asyncio
async def test_simple_download_invalid_machine(valid_client):
    with pytest.raises(firecrest.HeaderException):
        await valid_client.simple_download(
            "cluster2", "/path/to/source", "/path/to/destination"
        )


@pytest.mark.asyncio
async def test_simple_download_invalid_client(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        await invalid_client.simple_download(
            "cluster1", "/path/to/source", "/path/to/destination"
        )


@pytest.mark.asyncio
async def test_simple_upload(valid_client, tmp_path):
    tmp_dir = tmp_path / "upload_dir"
    tmp_dir.mkdir()
    local_file = tmp_dir / "hello.txt"
    local_file.write_text("hi")
    # Make sure this doesn't raise an error
    await valid_client.simple_upload("cluster1", local_file, "/path/to/remote/destination")


@pytest.mark.asyncio
async def test_simple_upload_invalid_arguments(valid_client, tmp_path):
    tmp_dir = tmp_path / "download_dir"
    tmp_dir.mkdir()
    local_file = tmp_dir / "hello_invalid.txt"
    local_file.write_text("hi")
    with pytest.raises(firecrest.HeaderException):
        await valid_client.simple_upload(
            "cluster1", local_file, "/path/to/invalid/destination"
        )


@pytest.mark.asyncio
async def test_simple_upload_invalid_machine(valid_client, tmp_path):
    tmp_dir = tmp_path / "download_dir"
    tmp_dir.mkdir()
    local_file = tmp_dir / "hello_invalid.txt"
    local_file.write_text("hi")
    with pytest.raises(firecrest.HeaderException):
        await valid_client.simple_upload(
            "cluster2", local_file, "/path/to/remote/destination"
        )


@pytest.mark.asyncio
async def test_simple_upload_invalid_client(invalid_client, tmp_path):
    tmp_dir = tmp_path / "download_dir"
    tmp_dir.mkdir()
    local_file = tmp_dir / "hello_invalid.txt"
    local_file.write_text("hi")
    with pytest.raises(firecrest.UnauthorizedException):
        await invalid_client.simple_upload(
            "cluster1", local_file, "/path/to/remote/destination"
        )


@pytest.mark.asyncio
async def test_simple_delete(valid_client):
    # Make sure this doesn't raise an error
    await valid_client.simple_delete("cluster1", "/path/to/file")


@pytest.mark.asyncio
async def test_simple_delete_invalid_arguments(valid_client):
    with pytest.raises(firecrest.HeaderException):
        await valid_client.simple_delete("cluster1", "/path/to/invalid/file")


@pytest.mark.asyncio
async def test_simple_delete_invalid_machine(valid_client):
    with pytest.raises(firecrest.HeaderException):
        await valid_client.simple_delete("cluster2", "/path/to/file")


@pytest.mark.asyncio
async def test_simple_delete_invalid_client(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        await invalid_client.simple_delete("cluster1", "/path/to/file")


@pytest.mark.asyncio
async def test_checksum(valid_client):
    assert (
        await valid_client.checksum("cluster1", "/path/to/file")
        == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    )


@pytest.mark.asyncio
async def test_checksum_invalid_arguments(valid_client):
    with pytest.raises(firecrest.HeaderException):
        await valid_client.checksum("cluster1", "/path/to/invalid/file")


@pytest.mark.asyncio
async def test_checksum_invalid_machine(valid_client):
    with pytest.raises(firecrest.HeaderException):
        await valid_client.checksum("cluster2", "/path/to/file")


@pytest.mark.asyncio
async def test_checksum_invalid_client(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        await invalid_client.checksum("cluster1", "/path/to/file")


@pytest.mark.asyncio
async def test_view(valid_client):
    assert await valid_client.view("cluster1", "/path/to/file") == "hello\n"


@pytest.mark.asyncio
async def test_head(valid_client):
    assert await valid_client.head("cluster1", "/path/to/file") == 10 * "hello\n"
    assert await valid_client.head("cluster1", "/path/to/file", lines=2) == 2 * "hello\n"
    assert await valid_client.head("cluster1", "/path/to/file", bytes=4) == "hell"


@pytest.mark.asyncio
async def test_tail(valid_client):
    assert await valid_client.tail("cluster1", "/path/to/file") == 10 * "hello\n"
    assert await valid_client.tail("cluster1", "/path/to/file", lines=2) == 2 * "hello\n"
    assert await valid_client.tail("cluster1", "/path/to/file", bytes=5) == "ello\n"


@pytest.mark.asyncio
async def test_view_invalid_arguments(valid_client):
    with pytest.raises(firecrest.HeaderException):
        await valid_client.view("cluster1", "/path/to/invalid/file")


@pytest.mark.asyncio
async def test_view_invalid_machine(valid_client):
    with pytest.raises(firecrest.HeaderException):
        await valid_client.view("cluster2", "/path/to/file")


@pytest.mark.asyncio
async def test_view_invalid_client(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        await invalid_client.view("cluster1", "/path/to/file")


@pytest.mark.asyncio
async def test_whoami(valid_client):
    assert await valid_client.whoami("cluster1") == "username"


@pytest.mark.asyncio
async def test_whoami_invalid_machine(valid_client):
    with pytest.raises(firecrest.HeaderException):
        await valid_client.whoami("cluster2")


@pytest.mark.asyncio
async def test_whoami_invalid_client(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        await invalid_client.whoami("cluster1")


@pytest.mark.asyncio
async def test_groups(valid_client):
    assert await valid_client.groups("cluster1") == {
        "group": {"id": "1000", "name": "group1"},
        "groups": [{"id": "1000", "name": "group1"}, {"id": "1001", "name": "group2"}],
        "user": {"id": "10000", "name": "test_user"},
    }


@pytest.mark.asyncio
async def test_groups_not_impl(valid_client):
    valid_client.set_api_version("1.14.0")
    with pytest.raises(firecrest.NotImplementedOnAPIversion):
        await valid_client.groups(machine="cluster1")


@pytest.mark.asyncio
async def test_groups_invalid_machine(valid_client):
    with pytest.raises(firecrest.HeaderException):
        await valid_client.groups("cluster2")


@pytest.mark.asyncio
async def test_groups_invalid_client(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        await invalid_client.groups("cluster1")

