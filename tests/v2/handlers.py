import json
import pytest
import re

from werkzeug.wrappers import Response
from werkzeug.wrappers import Request


def read_json_file(filename):
    with open(filename) as fp:
        data = json.load(fp)

    return data


@pytest.fixture
def auth_server(httpserver):
    httpserver.expect_request("/auth/token").respond_with_handler(auth_handler)
    return httpserver


@pytest.fixture
def fc_server(httpserver):
    httpserver.expect_request(
        re.compile("/status/.*"), method="GET"
    ).respond_with_handler(status_handler)

    for endpoint in ["ls", "view", "tail", "head",
                     "checksum", "file", "stat"]:
        httpserver.expect_request(
            re.compile(rf"/filesystem/.*/{endpoint}"), method="GET"
        ).respond_with_handler(filesystem_handler)

    httpserver.expect_request(
        re.compile(r"/filesystem/.*/mkdir"), method="POST"
    ).respond_with_handler(filesystem_handler)

    for endpoint in ["chown", "chmod"]:
        httpserver.expect_request(
            re.compile(rf"/filesystem/.*/{endpoint}"), method="PUT"
        ).respond_with_handler(filesystem_handler)

    httpserver.expect_request(
        re.compile(r"/filesystem/.*/rm"), method="DELETE"
    ).respond_with_handler(filesystem_handler)

    for endpoint in ["jobs", "metadata"]:
        httpserver.expect_request(
            re.compile(rf"/compute/.*/{endpoint}"), method="GET"
        ).respond_with_handler(filesystem_handler)

    httpserver.expect_request(
        re.compile(r"/compute/.*/jobs"), method="POST"
    ).respond_with_handler(submit_handler)

    return httpserver

def auth_handler(request):
    client_id = request.form["client_id"]
    client_secret = request.form["client_secret"]
    if client_id == "valid_id":
        if client_secret == "valid_secret":
            ret = {
                "access_token": "VALID_TOKEN",
                "expires_in": 15,
                "refresh_expires_in": 0,
                "token_type": "Bearer",
                "not-before-policy": 0,
                "scope": "profile firecrest email",
            }
            ret_status = 200
        elif client_secret == "valid_secret_2":
            ret = {
                "access_token": "token_2",
                "expires_in": 15,
                "refresh_expires_in": 0,
                "token_type": "Bearer",
                "not-before-policy": 0,
                "scope": "profile firecrest email",
            }
            ret_status = 200
        else:
            ret = {
                "error": "unauthorized_client",
                "error_description": "Invalid client secret",
            }
            ret_status = 400
    else:
        ret = {
            "error": "invalid_client",
            "error_description": "Invalid client credentials",
        }
        ret_status = 400

    return Response(json.dumps(ret), status=ret_status, content_type="application/json")


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

    url, *params = request.url.split("?")

    endpoint = url.split("/")[-1]

    suffix = ""

    if endpoint == "head":
        if (request.args.get("path") == "/path/to/file" and
            request.args.get("skipEnding") == "false" and
            request.args.get("bytes") == "8"):
            suffix = "_bytes"

        if (request.args.get("path") == "/path/to/file" and
            request.args.get("skipEnding") == "true" and
            request.args.get("bytes") == "8"):
            suffix = "_bytes_exclude_trailing"

        if (request.args.get("path") == "/path/to/file" and
            request.args.get("skipEnding") == "false" and
            request.args.get("lines") == "4"):
            suffix = "_lines"

        if (request.args.get("path") == "/path/to/file" and
            request.args.get("skipEnding") == "true" and
            request.args.get("lines") == "4"):
            suffix = "_lines_exclude_trailing"

    if endpoint == "tail":
        if (request.args.get("path") == "/path/to/file" and
            request.args.get("skipBeginning") == "false" and
            request.args.get("bytes") == "8"):
            suffix = "_bytes"

        if (request.args.get("path") == "/path/to/file" and
            request.args.get("skipBeginning") == "true" and
            request.args.get("bytes") == "8"):
            suffix = "_bytes_exclude_beginning"

        if (request.args.get("path") == "/path/to/file" and
            request.args.get("skipBeginning") == "false" and
            request.args.get("lines") == "4"):
            suffix = "_lines"

        if (request.args.get("path") == "/path/to/file" and
            request.args.get("skipBeginning") == "true" and
            request.args.get("lines") == "4"):
            suffix = "_lines_exclude_beginning"

    if endpoint == "ls":
        if (request.args.get("path") == "/home/user" and
            request.args.get("showHidden") == "false" and
            request.args.get("recursive") == "false" and
            request.args.get("numericUid") == "false" and
            request.args.get("dereference") == "true"):
            suffix = "_dereference"

        if (request.args.get("path") == "/home/user" and
            request.args.get("showHidden") == "true" and
            request.args.get("recursive") == "false" and
            request.args.get("numericUid") == "false" and
            request.args.get("dereference") == "false"):
            suffix = "_hidden"

        if (request.args.get("path") == "/home/user" and
            request.args.get("showHidden") == "false" and
            request.args.get("recursive") == "true" and
            request.args.get("numericUid") == "false" and
            request.args.get("dereference") == "false"):
            suffix = "_recursive"

        if (request.args.get("path") == "/home/user" and
            request.args.get("showHidden") == "false" and
            request.args.get("recursive") == "false" and
            request.args.get("numericUid") == "true" and
            request.args.get("dereference") == "false"):
            suffix = "_uid"

        if (request.args.get("path") == "/invalid/path" and
            request.args.get("showHidden") == "false" and
            request.args.get("recursive") == "false" and
            request.args.get("numericUid") == "false" and
            request.args.get("dereference") == "false"):
            suffix = "_invalid_path"

    if endpoint == "stat":
        if (request.args.get("path") == "/home/user/file" and
            request.args.get("dereference") == "true"):
            suffix = "_dereference"

    if endpoint == "chown":
        data = json.loads(request.get_data())
        if data == {
            'path': '/home/test1/xxx',
            'owner': 'test1',
            'group': 'users'
        }:
            suffix = "_not_permitted"

    if endpoint == "jobs":
        endpoint = "job"
        suffix = "_info"

    if endpoint == "1":
        endpoint = "job"
        suffix = "_info"

    if endpoint == "metadata":
        endpoint = "job"
        suffix = "_metadata"

    data = read_json_file(f"v2/responses/{endpoint}{suffix}.json")

    ret = data["response"]
    ret_status = data["status_code"]

    return Response(json.dumps(ret),
                    status=ret_status,
                    content_type="application/json")


def submit_handler(request: Request):
    if request.headers["Authorization"] != "Bearer VALID_TOKEN":
        return Response(
            json.dumps({"message": "Bad token; invalid JSON"}),
            status=401,
            content_type="application/json",
        )

    url, *params = request.url.split("?")

    endpoint = url.split("/")[-1]

    suffix = ""

    if endpoint == "jobs":
        endpoint = "job"
        suffix = "_submit"

    data = read_json_file(f"v2/responses/{endpoint}{suffix}.json")

    ret = data["response"]
    ret_status = data["status_code"]

    return Response(json.dumps(ret),
                    status=ret_status,
                    content_type="application/json")
