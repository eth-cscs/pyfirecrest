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
        return [400, response_headers, '{"description": "Error on ls operation", "error": "Machine does not exist"}']

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
                    "user": "user"
                },
                {
                    "group": "group",
                    "last_modified": "2021-10-07T09:17:01",
                    "link_target": "",
                    "name": "projectdir",
                    "permissions": "rwxr-xr-x",
                    "size": "4096",
                    "type": "d",
                    "user": "user"
                }
            ],
        }
        showhidden = request.querystring.get("showhidden", [False])[0]
        if showhidden:
            ret["output"].append({
                "group": "group",
                "last_modified": "2021-11-26T09:34:59",
                "link_target": "",
                "name": ".hiddenfile",
                "permissions": "rwxrwxr-x",
                "size": "4096",
                "type": "-",
                "user": "user"
            })

        return [200, response_headers, json.dumps(ret)]

    if targetPath == "/path/to/invalid/dir":
        response_headers["X-Invalid-Path"] = "path is an invalid path"
        return [400, response_headers, '{"description": "Error on ls operation"}']

# httpretty.register_uri(
#     httpretty.GET,
#     re.compile(r"http:\/\/firecrest\.cscs\.ch\/status\/services.*"),
#     body=services_callback,
# )

httpretty.register_uri(
    httpretty.GET,
    "http://firecrest.cscs.ch/utilities/ls",
    body=ls_callback,
)


def test_list_files(valid_client):
    assert valid_client.list_files("cluster1", '/path/to/valid/dir') == [
        {
            "group": "group",
            "last_modified": "2021-08-10T15:26:52",
            "link_target": "",
            "name": "file.txt",
            "permissions": "r-xr-xr-x.",
            "size": "180",
            "type": "-",
            "user": "user"
        },
        {
            "group": "group",
            "last_modified": "2021-10-07T09:17:01",
            "link_target": "",
            "name": "projectdir",
            "permissions": "rwxr-xr-x",
            "size": "4096",
            "type": "d",
            "user": "user"
        }
    ]

    assert valid_client.list_files("cluster1", '/path/to/valid/dir', showhidden=True) == [
        {
            "group": "group",
            "last_modified": "2021-08-10T15:26:52",
            "link_target": "",
            "name": "file.txt",
            "permissions": "r-xr-xr-x.",
            "size": "180",
            "type": "-",
            "user": "user"
        },
        {
            "group": "group",
            "last_modified": "2021-10-07T09:17:01",
            "link_target": "",
            "name": "projectdir",
            "permissions": "rwxr-xr-x",
            "size": "4096",
            "type": "d",
            "user": "user"
        },
        {
            "group": "group",
            "last_modified": "2021-11-26T09:34:59",
            "link_target": "",
            "name": ".hiddenfile",
            "permissions": "rwxrwxr-x",
            "size": "4096",
            "type": "-",
            "user": "user"
        }
    ]


def test_list_files_invalid_path(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        invalid_client.list_files("cluster1", '/path/to/dir')


def test_list_files_invalid_client(invalid_client):
    with pytest.raises(firecrest.UnauthorizedException):
        invalid_client.list_files("cluster1", '/path/to/dir')
