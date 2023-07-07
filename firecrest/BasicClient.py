#
#  Copyright (c) 2019-2023, ETH Zurich. All rights reserved.
#
#  Please, refer to the LICENSE file in the root directory.
#  SPDX-License-Identifier: BSD-3-Clause
#
from __future__ import annotations

import itertools
import jwt
import logging
import pathlib
import requests
import shlex
import shutil
import subprocess
import sys
import time
import urllib.request

from contextlib import nullcontext
from io import BufferedWriter, BytesIO
from requests.compat import json  # type: ignore
from typing import Any, ContextManager, Optional, overload, Sequence, Tuple, List
from packaging.version import Version, parse

import firecrest.FirecrestException as fe
import firecrest.types as t

if sys.version_info >= (3, 8):
    from typing import Literal
else:
    from typing_extensions import Literal

logger = logging.getLogger(__name__)

# This function is temporarily here
def handle_response(response):
    print("\nResponse status code:")
    print(response.status_code)
    print("\nResponse headers:")
    print(json.dumps(dict(response.headers), indent=4))
    print("\nResponse json:")
    try:
        print(json.dumps(response.json(), indent=4))
    except json.JSONDecodeError:
        print("-")


class ExternalStorage:
    """External storage object.
    """

    _final_states: set[str]

    def __init__(
        self,
        client: Firecrest,
        task_id: str,
        previous_responses: Optional[List[requests.Response]] = None,
    ) -> None:
        previous_responses = [] if previous_responses is None else previous_responses
        self._client = client
        self._task_id = task_id
        self._in_progress = True
        self._status: Optional[str] = None
        self._data = None
        self._object_storage_data = None
        self._sleep_time = itertools.cycle([1, 5, 10])
        self._responses = previous_responses

    @property
    def client(self) -> Firecrest:
        """Returns the client that will be used to get information for the task.
        """
        return self._client

    @property
    def task_id(self) -> str:
        """Returns the FirecREST task ID that is associated with this transfer.
        """
        return self._task_id

    def _update(self) -> None:
        if self._status not in self._final_states:
            task = self._client._task_safe(self._task_id, self._responses)
            self._status = task["status"]
            self._data = task["data"]
            logger.info(f"Task {self._task_id} has status {self._status}")
            if not self._object_storage_data:
                if self._status == "111":
                    self._object_storage_data = task["data"]["msg"]
                elif self._status == "117":
                    self._object_storage_data = task["data"]

    @property
    def status(self) -> str:
        """Returns status of the task that is associated with this transfer.

        :calls: GET `/tasks/{taskid}`
        """
        self._update()
        return self._status  # type: ignore

    @property
    def in_progress(self) -> bool:
        """Returns `False` when the transfer has been completed (succesfully or with errors), otherwise `True`.

        :calls: GET `/tasks/{taskid}`
        """
        self._update()
        return self._status not in self._final_states

    @property
    def data(self) -> Optional[dict]:
        """Returns the task information from the latest response.

        :calls: GET `/tasks/{taskid}`
        """
        self._update()
        return self._data

    @property
    def object_storage_data(self):
        """Returns the necessary information for the external transfer.
        The call is blocking and in cases of large file transfers it might take a long time.

        :calls: GET `/tasks/{taskid}`
        :rtype: dictionary or string
        """
        if not self._object_storage_data:
            self._update()

        while not self._object_storage_data:
            t = next(self._sleep_time)
            logger.info(f"Sleeping for {t} sec")
            time.sleep(t)
            self._update()

        return self._object_storage_data


class ExternalUpload(ExternalStorage):
    """
    This class handles the external upload from a file.

    Tracks the progress of the upload through the status of the associated task.
    Final states: *114* and *115*.

    +--------+--------------------------------------------------------------------+
    | Status | Description                                                        |
    +========+====================================================================+
    | 110    | Waiting for Form URL from Object Storage to be retrieved           |
    +--------+--------------------------------------------------------------------+
    | 111    | Form URL from Object Storage received                              |
    +--------+--------------------------------------------------------------------+
    | 112    | Object Storage confirms that upload to Object Storage has finished |
    +--------+--------------------------------------------------------------------+
    | 113    | Download from Object Storage to server has started                 |
    +--------+--------------------------------------------------------------------+
    | 114    | Download from Object Storage to server has finished                |
    +--------+--------------------------------------------------------------------+
    | 115    | Download from Object Storage error                                 |
    +--------+--------------------------------------------------------------------+

    :param client: FirecREST client associated with the transfer
    :param task_id: FirecrREST task associated with the transfer
    """

    def __init__(
        self,
        client: Firecrest,
        task_id: str,
        previous_responses: Optional[List[requests.Response]] = None,
    ) -> None:
        previous_responses = [] if previous_responses is None else previous_responses
        super().__init__(client, task_id, previous_responses)
        self._final_states = {"114", "115"}
        logger.info(f"Creating ExternalUpload object for task {task_id}")

    def finish_upload(self) -> None:
        """Finish the upload process.
        This call will upload the file to the staging area.
        Check with the method `status` or `in_progress` to see the status of the transfer.
        The transfer from the staging area to the systems's filesystem can take several seconds to start to start.
        """
        c = self.object_storage_data["command"]  # typer: ignore
        # LOCAL FIX FOR MAC
        # c = c.replace("192.168.220.19", "localhost")
        logger.info(f"Uploading the file to the staging area with the command: {c}")
        command = subprocess.run(
            shlex.split(c), stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        if command.returncode != 0:
            exc = Exception(
                f"Failed to finish upload with error: {command.stderr.decode('utf-8')}"
            )
            logger.critical(exc)
            raise exc


class ExternalDownload(ExternalStorage):
    """
    This class handles the external download from a file.

    Tracks the progress of the download through the status of the associated task.
    Final states: *117* and *118*.

    +--------+--------------------------------------------------------------------+
    | Status | Description                                                        |
    +========+====================================================================+
    | 116    | Started upload from filesystem to Object Storage                   |
    +--------+--------------------------------------------------------------------+
    | 117    | Upload from filesystem to Object Storage has finished successfully |
    +--------+--------------------------------------------------------------------+
    | 118    | Upload from filesystem to Object Storage has finished with errors  |
    +--------+--------------------------------------------------------------------+

    :param client: FirecREST client associated with the transfer
    :param task_id: FirecrREST task associated with the transfer
    """

    def __init__(
        self,
        client: Firecrest,
        task_id: str,
        previous_responses: Optional[List[requests.Response]] = None,
    ) -> None:
        previous_responses = [] if previous_responses is None else previous_responses
        super().__init__(client, task_id, previous_responses)
        self._final_states = {"117", "118"}
        logger.info(f"Creating ExternalDownload object for task {task_id}")

    def invalidate_object_storage_link(self) -> None:
        """Invalidate the temporary URL for downloading.

        :calls: POST `/storage/xfer-external/invalidate`
        """
        self._client._invalidate(self._task_id)

    @property
    def object_storage_link(self) -> str:
        """Get the direct download url for the file. The response from the FirecREST api
        changed after version 1.13.0, so make sure to set to older version, if you are
        using an older deployment.

        :calls: GET `/tasks/{taskid}`
        """
        if self._client._api_version > Version("1.13.0"):
            return self.object_storage_data["url"]
        else:
            return self.object_storage_data

    def finish_download(self, target_path: str | pathlib.Path | BufferedWriter) -> None:
        """Finish the download process. The response from the FirecREST api changed after
        version 1.13.0, so make sure to set to older version, if you are using an older
        deployment.

        :param target_path: the local path to save the file

        :calls: GET `/tasks/{taskid}`
        """
        url = self.object_storage_link
        logger.info(f"Downloading the file from {url} and saving to {target_path}")
        # LOCAL FIX FOR MAC
        # url = url.replace("192.168.220.19", "localhost")
        context: ContextManager[BufferedWriter] = (
            open(target_path, "wb")  # type: ignore
            if isinstance(target_path, str) or isinstance(target_path, pathlib.Path)
            else nullcontext(target_path)
        )
        with urllib.request.urlopen(url) as response, context as out_file:
            shutil.copyfileobj(response, out_file)


class Firecrest:
    """
    This is the basic class you instantiate to access the FirecREST API v1.
    Necessary parameters are the firecrest URL and an authorization object.

    :param firecrest_url: FirecREST's URL
    :param authorization: the authorization object. This object is responsible of handling the credentials and the only requirement for it is that it has a method get_access_token() that returns a valid access token.
    :param verify: either a boolean, in which case it controls whether requests will verify the server’s TLS certificate, or a string, in which case it must be a path to a CA bundle to use
    :param sa_role: this corresponds to the `F7T_AUTH_ROLE` configuration parameter of the site. If you don't know how FirecREST is setup it's better to leave the default.
    """

    TOO_MANY_REQUESTS_CODE = 429

    def _retry_requests(func):
        def wrapper(*args, **kwargs):
            client = args[0]
            num_retries = 0
            resp = func(*args, **kwargs)
            while True:
                if resp.status_code != client.TOO_MANY_REQUESTS_CODE:
                    break
                elif (
                    client.num_retries_rate_limit is not None
                    and num_retries >= client.num_retries_rate_limit
                ):
                    logger.debug(
                        f"Rate limit is reached and the request has "
                        f"been retried already {num_retries} times"
                    )
                    break
                else:
                    reset = resp.headers.get(
                        "Retry-After",
                        default=resp.headers.get("RateLimit-Reset", default=10),
                    )
                    logger.info(
                        f"Rate limit is reached, will sleep for "
                        f"{reset} seconds and try again"
                    )
                    reset = int(reset)
                    time.sleep(reset)
                    resp = func(*args, **kwargs)
                    num_retries += 1

            return resp

        return wrapper

    def __init__(
        self,
        firecrest_url: str,
        authorization: Any,
        verify: Optional[str | bool] = None,
        sa_role: str = "firecrest-sa",
    ) -> None:
        self._firecrest_url = firecrest_url
        self._authorization = authorization
        # This should be used only for blocking operations that require multiple requests,
        # not for external upload/download
        self._current_method_requests: List[requests.Response] = []
        self._verify = verify
        self._sa_role = sa_role
        #: This attribute will be passed to all the requests that will be made.
        #: How many seconds to wait for the server to send data before giving up.
        #: After that time a `requests.exceptions.Timeout` error will be raised.
        #:
        #: It can be a float or a tuple. More details here: https://requests.readthedocs.io.
        self.timeout: Optional[float | Tuple[float, float] | Tuple[float, None]] = None
        #: Number of retries in case the rate limit is reached. When it is set to `None`, the
        #: client will keep trying until it gets a different status code than 429.
        self.num_retries_rate_limit: Optional[int] = None
        self._api_version: Version = parse("1.13.0")

    def set_api_version(self, api_version: str) -> None:
        """Set the version of the api of firecrest. By default it will be assumed that you are
        using version 1.13.0 or compatible. The version is parsed by the `packaging` library.
        """
        self._api_version = parse(api_version)

    @_retry_requests  # type: ignore
    def _get_request(
        self, endpoint, additional_headers=None, params=None
    ) -> requests.Response:
        url = f"{self._firecrest_url}{endpoint}"
        headers = {"Authorization": f"Bearer {self._authorization.get_access_token()}"}
        if additional_headers:
            headers.update(additional_headers)

        logger.info(f"Making GET request to {endpoint}")
        resp = requests.get(
            url=url,
            headers=headers,
            params=params,
            verify=self._verify,
            timeout=self.timeout,
        )
        return resp

    @_retry_requests  # type: ignore
    def _post_request(
        self, endpoint, additional_headers=None, data=None, files=None
    ) -> requests.Response:
        url = f"{self._firecrest_url}{endpoint}"
        headers = {"Authorization": f"Bearer {self._authorization.get_access_token()}"}
        if additional_headers:
            headers.update(additional_headers)

        logger.info(f"Making POST request to {endpoint}")
        resp = requests.post(
            url=url,
            headers=headers,
            data=data,
            files=files,
            verify=self._verify,
            timeout=self.timeout,
        )
        return resp

    @_retry_requests  # type: ignore
    def _put_request(
        self, endpoint, additional_headers=None, data=None
    ) -> requests.Response:
        url = f"{self._firecrest_url}{endpoint}"
        headers = {"Authorization": f"Bearer {self._authorization.get_access_token()}"}
        if additional_headers:
            headers.update(additional_headers)

        logger.info(f"Making PUT request to {endpoint}")
        resp = requests.put(
            url=url,
            headers=headers,
            data=data,
            verify=self._verify,
            timeout=self.timeout,
        )
        return resp

    @_retry_requests  # type: ignore
    def _delete_request(
        self, endpoint, additional_headers=None, data=None
    ) -> requests.Response:
        url = f"{self._firecrest_url}{endpoint}"
        headers = {"Authorization": f"Bearer {self._authorization.get_access_token()}"}
        if additional_headers:
            headers.update(additional_headers)

        logger.info(f"Making DELETE request to {endpoint}")
        resp = requests.delete(
            url=url,
            headers=headers,
            data=data,
            verify=self._verify,
            timeout=self.timeout,
        )
        return resp

    @overload
    def _json_response(
        self,
        responses: List[requests.Response],
        expected_status_code: int,
        allow_none_result: Literal[False] = ...,
    ) -> dict:
        ...

    @overload
    def _json_response(
        self,
        responses: List[requests.Response],
        expected_status_code: int,
        allow_none_result: Literal[True],
    ) -> Optional[dict]:
        ...

    def _json_response(
        self,
        responses: List[requests.Response],
        expected_status_code: int,
        allow_none_result: bool = False,
    ):
        # Will examine only the last response
        response = responses[-1]
        status_code = response.status_code
        # handle_response(response)
        exc: fe.FirecrestException
        for h in fe.ERROR_HEADERS:
            if h in response.headers:
                logger.critical(f"Header '{h}' is included in the response")
                exc = fe.HeaderException(responses)
                logger.critical(exc)
                raise exc

        if status_code == 401:
            logger.critical(f"Status of the response is 401")
            exc = fe.UnauthorizedException(responses)
            logger.critical(exc)
            raise exc
        elif status_code == 404:
            logger.critical(f"Status of the response is 404")
            exc = fe.NotFound(responses)
            logger.critical(exc)
            raise exc
        elif status_code >= 400:
            logger.critical(f"Status of the response is {status_code}")
            exc = fe.FirecrestException(responses)
            logger.critical(exc)
            raise exc
        elif status_code != expected_status_code:
            logger.critical(
                f"Unexpected status of last request {status_code}, it should have been {expected_status_code}"
            )
            exc = fe.UnexpectedStatusException(responses, expected_status_code)
            logger.critical(exc)
            raise exc

        try:
            ret = response.json()
        except json.decoder.JSONDecodeError:
            if allow_none_result:
                ret = None
            else:
                exc = fe.NoJSONException(responses)
                logger.critical(exc)
                raise exc

        return ret

    def _tasks(
        self,
        task_ids: Optional[List[str]] = None,
        responses: Optional[List[requests.Response]] = None,
    ) -> dict[str, t.Task]:
        """Return a dictionary of FirecREST tasks and their last update.
        When `task_ids` is an empty list or contains more than one element the
        `/tasks` endpoint will be called. Otherwise `/tasks/{taskid}`.
        When the `/tasks` is called the method will not give an error for invalid IDs,
        but `/tasks/{taskid}` will raise an exception.

        :param task_ids: list of task IDs. When empty all tasks are returned.
        :param responses: list of responses that are associated with these tasks (only relevant for error)
        :calls: GET `/tasks` or `/tasks/{taskid}`
        """
        task_ids = [] if task_ids is None else task_ids
        responses = [] if responses is None else responses
        endpoint = "/tasks/"
        if len(task_ids) == 1:
            endpoint += task_ids[0]

        resp = self._get_request(endpoint=endpoint)
        responses.append(resp)
        taskinfo = self._json_response(responses, 200)
        if len(task_ids) == 0:
            return taskinfo["tasks"]
        elif len(task_ids) == 1:
            return {task_ids[0]: taskinfo["task"]}
        else:
            return {k: v for k, v in taskinfo["tasks"].items() if k in task_ids}

    def _task_safe(
        self, task_id: str, responses: Optional[List[requests.Response]] = None
    ) -> t.Task:
        if responses is None:
            responses = self._current_method_requests

        task = self._tasks([task_id], responses)[task_id]
        status = int(task["status"])
        exc: fe.FirecrestException
        if status == 115:
            logger.critical("Task has error status code 115")
            exc = fe.StorageUploadException(responses)
            logger.critical(exc)
            raise exc

        if status == 118:
            logger.critical("Task has error status code 118")
            exc = fe.StorageDownloadException(responses)
            logger.critical(exc)
            raise exc

        if status >= 400:
            logger.critical(f"Task has error status code {status}")
            exc = fe.FirecrestException(responses)
            logger.critical(exc)
            raise exc

        return task

    def _invalidate(
        self, task_id: str, responses: Optional[List[requests.Response]] = None
    ):
        responses = [] if responses is None else responses
        resp = self._post_request(
            endpoint="/storage/xfer-external/invalidate",
            additional_headers={"X-Task-Id": task_id},
        )
        responses.append(resp)
        return self._json_response(responses, 201, allow_none_result=True)

    def _poll_tasks(self, task_id: str, final_status, sleep_time):
        logger.info(f"Polling task {task_id} until status is {final_status}")
        resp = self._task_safe(task_id)
        while resp["status"] < final_status:
            t = next(sleep_time)
            logger.info(
                f'Status of {task_id} is {resp["status"]}, sleeping for {t} sec'
            )
            time.sleep(t)
            resp = self._task_safe(task_id)

        logger.info(f'Status of {task_id} is {resp["status"]}')
        return resp["data"]

    # Status
    def all_services(self) -> List[t.Service]:
        """Returns a list containing all available micro services with a name, description, and status.

        :calls: GET `/status/services`
        """
        resp = self._get_request(endpoint="/status/services")
        return self._json_response([resp], 200)["out"]

    def service(self, service_name: str) -> t.Service:
        """Returns information about a micro service.
        Returns the name, description, and status.

        :param service_name: the service name
        :calls: GET `/status/services/{service_name}`
        """
        resp = self._get_request(endpoint=f"/status/services/{service_name}")
        return self._json_response([resp], 200)  # type: ignore

    def all_systems(self) -> List[t.System]:
        """Returns a list containing all available systems and response status.

        :calls: GET `/status/systems`
        """
        resp = self._get_request(endpoint="/status/systems")
        return self._json_response([resp], 200)["out"]

    def system(self, system_name: str) -> t.System:
        """Returns information about a system.
        Returns the name, description, and status.

        :param system_name: the system name
        :calls: GET `/status/systems/{system_name}`
        """
        resp = self._get_request(endpoint=f"/status/systems/{system_name}")
        return self._json_response([resp], 200)["out"]

    def parameters(self) -> t.Parameters:
        """Returns configuration parameters of the FirecREST deployment that is associated with the client.

        :calls: GET `/status/parameters`
        """
        resp = self._get_request(endpoint="/status/parameters")
        return self._json_response([resp], 200)["out"]

    # Utilities
    def list_files(
        self, machine: str, target_path: str, show_hidden: bool = False
    ) -> List[t.LsFile]:
        """Returns a list of files in a directory.

        :param machine: the machine name where the filesystem belongs to
        :param target_path: the absolute target path
        :param show_hidden: show hidden files
        :calls: GET `/utilities/ls`
        """
        params: dict[str, Any] = {"targetPath": f"{target_path}"}
        if show_hidden is True:
            params["showhidden"] = show_hidden

        resp = self._get_request(
            endpoint="/utilities/ls",
            additional_headers={"X-Machine-Name": machine},
            params=params,
        )
        return self._json_response([resp], 200)["output"]

    def mkdir(self, machine: str, target_path: str, p: Optional[bool] = None) -> None:
        """Creates a new directory.

        :param machine: the machine name where the filesystem belongs to
        :param target_path: the absolute target path
        :param p: no error if existing, make parent directories as needed
        :calls: POST `/utilities/mkdir`
        """
        data: dict[str, Any] = {"targetPath": target_path}
        if p:
            data["p"] = p

        resp = self._post_request(
            endpoint="/utilities/mkdir",
            additional_headers={"X-Machine-Name": machine},
            data=data,
        )
        self._json_response([resp], 201)

    def mv(self, machine: str, source_path: str, target_path: str) -> None:
        """Rename/move a file, directory, or symlink at the `source_path` to the `target_path` on `machine`'s filesystem.

        :param machine: the machine name where the filesystem belongs to
        :param source_path: the absolute source path
        :param target_path: the absolute target path
        :calls: PUT `/utilities/rename`
        """
        resp = self._put_request(
            endpoint="/utilities/rename",
            additional_headers={"X-Machine-Name": machine},
            data={"targetPath": target_path, "sourcePath": source_path},
        )
        self._json_response([resp], 200)

    def chmod(self, machine: str, target_path: str, mode: str) -> None:
        """Changes the file mod bits of a given file according to the specified mode.

        :param machine: the machine name where the filesystem belongs to
        :param target_path: the absolute target path
        :param mode: same as numeric mode of linux chmod tool
        :calls: PUT `/utilities/chmod`
        """
        resp = self._put_request(
            endpoint="/utilities/chmod",
            additional_headers={"X-Machine-Name": machine},
            data={"targetPath": target_path, "mode": mode},
        )
        self._json_response([resp], 200)

    def chown(
        self,
        machine: str,
        target_path: str,
        owner: Optional[str] = None,
        group: Optional[str] = None,
    ) -> None:
        """Changes the user and/or group ownership of a given file.
        If only owner or group information is passed, only that information will be updated.

        :param machine: the machine name where the filesystem belongs to
        :param target_path: the absolute target path
        :param owner: owner ID for target
        :param group: group ID for target
        :calls: PUT `/utilities/chown`
        """
        if owner is None and group is None:
            return

        data = {"targetPath": target_path}
        if owner:
            data["owner"] = owner

        if group:
            data["group"] = group

        resp = self._put_request(
            endpoint="/utilities/chown",
            additional_headers={"X-Machine-Name": machine},
            data=data,
        )
        self._json_response([resp], 200)

    def copy(self, machine: str, source_path: str, target_path: str) -> None:
        """Copies file from `source_path` to `target_path`.

        :param machine: the machine name where the filesystem belongs to
        :param source_path: the absolute source path
        :param target_path: the absolute target path
        :calls: POST `/utilities/copy`
        """
        resp = self._post_request(
            endpoint="/utilities/copy",
            additional_headers={"X-Machine-Name": machine},
            data={"targetPath": target_path, "sourcePath": source_path},
        )
        self._json_response([resp], 201)

    def file_type(self, machine: str, target_path: str) -> str:
        """Uses the `file` linux application to determine the type of a file.

        :param machine: the machine name where the filesystem belongs to
        :param target_path: the absolute target path
        :calls: GET `/utilities/file`
        """
        resp = self._get_request(
            endpoint="/utilities/file",
            additional_headers={"X-Machine-Name": machine},
            params={"targetPath": target_path},
        )
        return self._json_response([resp], 200)["output"]

    def stat(
        self, machine: str, target_path: str, dereference: bool = False
    ) -> t.StatFile:
        """Uses the stat linux application to determine the status of a file on the machine's filesystem.
        The result follows: https://docs.python.org/3/library/os.html#os.stat_result.

        :param machine: the machine name where the filesystem belongs to
        :param target_path: the absolute target path
        :param dereference: follow link (default False)
        :calls: GET `/utilities/stat`
        """
        params: dict[str, Any] = {"targetPath": target_path}
        if dereference:
            params["dereference"] = dereference

        resp = self._get_request(
            endpoint="/utilities/stat",
            additional_headers={"X-Machine-Name": machine},
            params=params,
        )
        return self._json_response([resp], 200)["output"]

    def symlink(self, machine: str, target_path: str, link_path: str) -> None:
        """Creates a symbolic link.

        :param machine: the machine name where the filesystem belongs to
        :param target_path: the absolute path that the symlink will point to
        :param link_path: the absolute path to the new symlink
        :calls: POST `/utilities/symlink`
        """
        resp = self._post_request(
            endpoint="/utilities/symlink",
            additional_headers={"X-Machine-Name": machine},
            data={"targetPath": target_path, "linkPath": link_path},
        )
        self._json_response([resp], 201)

    def simple_download(
        self, machine: str, source_path: str, target_path: str | pathlib.Path | BytesIO
    ) -> None:
        """Blocking call to download a small file.
        The maximun size of file that is allowed can be found from the parameters() call.

        :param machine: the machine name where the filesystem belongs to
        :param source_path: the absolute source path
        :param target_path: the target path in the local filesystem or binary stream
        :calls: GET `/utilities/download`
        """
        resp = self._get_request(
            endpoint="/utilities/download",
            additional_headers={"X-Machine-Name": machine},
            params={"sourcePath": source_path},
        )
        self._json_response([resp], 200, allow_none_result=True)
        context: ContextManager[BytesIO] = (
            open(target_path, "wb")  # type: ignore
            if isinstance(target_path, str) or isinstance(target_path, pathlib.Path)
            else nullcontext(target_path)
        )
        with context as f:
            f.write(resp.content)

    def simple_upload(
        self,
        machine: str,
        source_path: str | pathlib.Path | BytesIO,
        target_path: str,
        filename: Optional[str] = None,
    ) -> None:
        """Blocking call to upload a small file.
        The file that will be uploaded will have the same name as the source_path.
        The maximum size of file that is allowed can be found from the parameters() call.

        :param machine: the machine name where the filesystem belongs to
        :param source_path: the source path of the file or binary stream
        :param target_path: the absolute target path of the directory where the file will be uploaded
        :param filename: naming target file to filename (default is same as the local one)
        :calls: POST `/utilities/upload`
        """
        context: ContextManager[BytesIO] = (
            open(source_path, "rb")  # type: ignore
            if isinstance(source_path, str) or isinstance(source_path, pathlib.Path)
            else nullcontext(source_path)
        )
        with context as f:
            # Set filename
            if filename is not None:
                f = (filename, f)  # type: ignore

            resp = self._post_request(
                endpoint="/utilities/upload",
                additional_headers={"X-Machine-Name": machine},
                data={"targetPath": target_path},
                files={"file": f},
            )

        self._json_response([resp], 201)

    def simple_delete(self, machine: str, target_path: str) -> None:
        """Blocking call to delete a small file.

        :param machine: the machine name where the filesystem belongs to
        :param target_path: the absolute target path
        :calls: DELETE `/utilities/rm`
        """
        resp = self._delete_request(
            endpoint="/utilities/rm",
            additional_headers={"X-Machine-Name": machine},
            data={"targetPath": target_path},
        )
        self._json_response([resp], 204, allow_none_result=True)

    def checksum(self, machine: str, target_path: str) -> str:
        """Calculate the SHA256 (256-bit) checksum of a specified file.

        :param machine: the machine name where the filesystem belongs to
        :param target_path: the absolute target path
        :calls: GET `/utilities/checksum`
        """
        resp = self._get_request(
            endpoint="/utilities/checksum",
            additional_headers={"X-Machine-Name": machine},
            params={"targetPath": target_path},
        )
        return self._json_response([resp], 200)["output"]

    def head(
        self,
        machine: str,
        target_path: str,
        bytes: Optional[int] = None,
        lines: Optional[int] = None,
    ) -> str:
        """Display the beginning of a specified file.
        By default 10 lines will be returned.
        Bytes and lines cannot be specified simultaneously.
        The final result will be smaller than `UTILITIES_MAX_FILE_SIZE` bytes.
        This variable is available in the parameters command.

        :param machine: the machine name where the filesystem belongs to
        :param target_path: the absolute target path
        :param lines: the number of lines to be displayed
        :param bytes: the number of bytes to be displayed
        :calls: GET `/utilities/head`
        """
        resp = self._get_request(
            endpoint="/utilities/head",
            additional_headers={"X-Machine-Name": machine},
            params={"targetPath": target_path, "lines": lines, "bytes": bytes},
        )
        return self._json_response([resp], 200)["output"]

    def tail(
        self,
        machine: str,
        target_path: str,
        bytes: Optional[int] = None,
        lines: Optional[int] = None,
    ) -> str:
        """Display the last part of a specified file.
        By default 10 lines will be returned.
        Bytes and lines cannot be specified simultaneously.
        The final result will be smaller than `UTILITIES_MAX_FILE_SIZE` bytes.
        This variable is available in the parameters command.

        :param machine: the machine name where the filesystem belongs to
        :param target_path: the absolute target path
        :param lines: the number of lines to be displayed
        :param bytes: the number of bytes to be displayed
        :calls: GET `/utilities/head`
        """
        resp = self._get_request(
            endpoint="/utilities/tail",
            additional_headers={"X-Machine-Name": machine},
            params={"targetPath": target_path, "lines": lines, "bytes": bytes},
        )
        return self._json_response([resp], 200)["output"]

    def view(self, machine: str, target_path: str) -> str:
        """View the content of a specified file.
        The final result will be smaller than `UTILITIES_MAX_FILE_SIZE` bytes.
        This variable is available in the parameters command.

        :param machine: the machine name where the filesystem belongs to
        :param target_path: the absolute target path
        :calls: GET `/utilities/checksum`
        """
        resp = self._get_request(
            endpoint="/utilities/view",
            additional_headers={"X-Machine-Name": machine},
            params={"targetPath": target_path},
        )
        return self._json_response([resp], 200)["output"]

    def whoami(self, machine=None) -> Optional[str]:
        """Returns the username that FirecREST will be using to perform the other calls.
        In the case the machine name is passed in the arguments, a call is made to the respective endpoint and the command whoami is run on the machine.
        Otherwise, the library decodes the token and will return `None` if the token is not valid.

        :calls: GET `/utilities/whoami`
        """
        if machine:
            resp = self._get_request(
                endpoint="/utilities/whoami",
                additional_headers={"X-Machine-Name": machine},
            )
            return self._json_response([resp], 200)["output"]

        try:
            decoded = jwt.decode(
                self._authorization.get_access_token(),
                options={"verify_signature": False},
            )
            try:
                if self._sa_role in decoded["realm_access"]["roles"]:
                    clientId = decoded["clientId"]
                    username = decoded["resource_access"][clientId]["roles"][0]
                    return username

                return decoded["preferred_username"]
            except KeyError:
                return decoded["preferred_username"]

        except Exception:
            # Invalid token, cannot retrieve username
            return None

    # Compute
    def _submit_request(self, machine: str, job_script, local_file, account=None):
        if local_file:
            with open(job_script, "rb") as f:
                if account:
                    data = {"account": account}
                else:
                    data = None

                resp = self._post_request(
                    endpoint="/compute/jobs/upload",
                    additional_headers={"X-Machine-Name": machine},
                    files={"file": f},
                    data=data,
                )
        else:
            data = {"targetPath": job_script}
            if account:
                data["account"] = account

            resp = self._post_request(
                endpoint="/compute/jobs/path",
                additional_headers={"X-Machine-Name": machine},
                data=data,
            )

        self._current_method_requests.append(resp)
        return self._json_response(self._current_method_requests, 201)

    def _squeue_request(self, machine: str, jobs=None):
        jobs = [] if jobs is None else jobs
        params = {}
        if jobs:
            params = {"jobs": ",".join([str(j) for j in jobs])}

        resp = self._get_request(
            endpoint="/compute/jobs",
            additional_headers={"X-Machine-Name": machine},
            params=params,
        )
        self._current_method_requests.append(resp)
        return self._json_response(self._current_method_requests, 200)

    def _acct_request(self, machine: str, jobs=None, start_time=None, end_time=None):
        jobs = [] if jobs is None else jobs
        params = {}
        if jobs:
            params["jobs"] = ",".join(jobs)

        if start_time:
            params["starttime"] = start_time

        if end_time:
            params["endtime"] = end_time

        resp = self._get_request(
            endpoint="/compute/acct",
            additional_headers={"X-Machine-Name": machine},
            params=params,
        )
        self._current_method_requests.append(resp)
        return self._json_response(self._current_method_requests, 200)

    def submit(
        self,
        machine: str,
        job_script: str,
        local_file: Optional[bool] = True,
        account: Optional[str] = None,
    ) -> t.JobSubmit:
        """Submits a batch script to SLURM on the target system

        :param machine: the machine name where the scheduler belongs to
        :param job_script: the path of the script (if it's local it can be relative path, if it is on the machine it has to be the absolute path)
        :param local_file: batch file can be local (default) or on the machine's filesystem
        :param account: submit the job with this project account
        :calls: POST `/compute/jobs/upload` or POST `/compute/jobs/path`

                GET `/tasks/{taskid}`
        """
        self._current_method_requests = []
        json_response = self._submit_request(machine, job_script, local_file, account)
        logger.info(f"Job submission task: {json_response['task_id']}")
        return self._poll_tasks(
            json_response["task_id"], "200", itertools.cycle([1, 5, 10])
        )

    def poll(
        self,
        machine: str,
        jobs: Optional[Sequence[str | int]] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
    ) -> List[t.JobAcct]:
        """Retrieves information about submitted jobs.
        This call uses the `sacct` command.

        :param machine: the machine name where the scheduler belongs to
        :param jobs: list of the IDs of the jobs
        :param start_time: Start time (and/or date) of job's query. Allowed formats are HH:MM[:SS] [AM|PM] MMDD[YY] or MM/DD[/YY] or MM.DD[.YY] MM/DD[/YY]-HH:MM[:SS] YYYY-MM-DD[THH:MM[:SS]]
        :param end_time: End time (and/or date) of job's query. Allowed formats are HH:MM[:SS] [AM|PM] MMDD[YY] or MM/DD[/YY] or MM.DD[.YY] MM/DD[/YY]-HH:MM[:SS] YYYY-MM-DD[THH:MM[:SS]]
        :calls: GET `/compute/acct`

                GET `/tasks/{taskid}`
        """
        self._current_method_requests = []
        if isinstance(jobs, str):
            logger.warning(
                f"`jobs` is meant to be a list, not a string. Will poll for jobs: {[str(j) for j in jobs]}"
            )

        jobids = [str(j) for j in jobs] if jobs else []
        json_response = self._acct_request(machine, jobids, start_time, end_time)
        logger.info(f"Job polling task: {json_response['task_id']}")
        res = self._poll_tasks(
            json_response["task_id"], "200", itertools.cycle([1, 5, 10])
        )
        # When there is no job in the sacct output firecrest will return an empty dictionary instead of list
        if isinstance(res, dict):
            return list(res.values())
        else:
            return res

    def poll_active(
        self, machine: str, jobs: Optional[Sequence[str | int]] = None
    ) -> List[t.JobQueue]:
        """Retrieves information about active jobs.
        This call uses the `squeue -u <username>` command.

        :param machine: the machine name where the scheduler belongs to
        :param jobs: list of the IDs of the jobs
        :calls: GET `/compute/jobs`

                GET `/tasks/{taskid}`
        """
        self._current_method_requests = []
        if isinstance(jobs, str):
            logger.warning(
                f"`jobs` is meant to be a list, not a string. Will poll for jobs: {[str(j) for j in jobs]}"
            )

        jobs = jobs if jobs else []
        jobids = [str(j) for j in jobs]
        json_response = self._squeue_request(machine, jobids)
        logger.info(f"Job active polling task: {json_response['task_id']}")
        dict_result = self._poll_tasks(
            json_response["task_id"], "200", itertools.cycle([1, 5, 10])
        )
        return list(dict_result.values())

    def cancel(self, machine: str, job_id: str | int) -> str:
        """Cancels running job.
        This call uses the `scancel` command.

        :param machine: the machine name where the scheduler belongs to
        :param job_id: the ID of the job
        :calls: DELETE `/compute/jobs/{job_id}`

                GET `/tasks/{taskid}`
        """
        self._current_method_requests = []
        resp = self._delete_request(
            endpoint=f"/compute/jobs/{job_id}",
            additional_headers={"X-Machine-Name": machine},
        )
        self._current_method_requests.append(resp)
        json_response = self._json_response(self._current_method_requests, 200)
        logger.info(f"Job cancellation task: {json_response['task_id']}")
        return self._poll_tasks(
            json_response["task_id"], "200", itertools.cycle([1, 5, 10])
        )

    # Storage
    def _internal_transfer(
        self,
        endpoint,
        machine,
        source_path,
        target_path,
        job_name,
        time,
        stage_out_job_id,
        account,
    ):
        data = {"targetPath": target_path}
        if source_path:
            data["sourcePath"] = source_path

        if job_name:
            data["jobName"] = job_name

        if time:
            data["time"] = time

        if stage_out_job_id:
            data["stageOutJobId"] = stage_out_job_id

        if account:
            data["account"] = account

        resp = self._post_request(
            endpoint=endpoint, additional_headers={"X-Machine-Name": machine}, data=data
        )
        self._current_method_requests.append(resp)
        return self._json_response(self._current_method_requests, 201)

    def submit_move_job(
        self,
        machine: str,
        source_path: str,
        target_path: str,
        job_name: Optional[str] = None,
        time: Optional[str] = None,
        stage_out_job_id: Optional[str] = None,
        account: Optional[str] = None,
    ) -> t.JobSubmit:
        """Move files between internal CSCS file systems.
        Rename/Move source_path to target_path.
        Possible to stage-out jobs providing the SLURM ID of a production job.
        More info about internal transfer: https://user.cscs.ch/storage/data_transfer/internal_transfer/

        :param machine: the machine name where the scheduler belongs to
        :param source_path: the absolute source path
        :param target_path: the absolute target path
        :param job_name: job name
        :param time: limit on the total run time of the job. Acceptable time formats 'minutes', 'minutes:seconds', 'hours:minutes:seconds', 'days-hours', 'days-hours:minutes' and 'days-hours:minutes:seconds'.
        :param stage_out_job_id: transfer data after job with ID {stage_out_job_id} is completed
        :param account: name of the bank account to be used in SLURM. If not set, system default is taken.
        :calls: POST `/storage/xfer-internal/mv`

                GET `/tasks/{taskid}`
        """
        self._current_method_requests = []
        endpoint = "/storage/xfer-internal/mv"
        json_response = self._internal_transfer(
            endpoint,
            machine,
            source_path,
            target_path,
            job_name,
            time,
            stage_out_job_id,
            account,
        )
        logger.info(f"Job submission task: {json_response['task_id']}")
        return self._poll_tasks(
            json_response["task_id"], "200", itertools.cycle([1, 5, 10])
        )

    def submit_copy_job(
        self,
        machine: str,
        source_path: str,
        target_path: str,
        job_name: Optional[str] = None,
        time: Optional[str] = None,
        stage_out_job_id: Optional[str] = None,
        account: Optional[str] = None,
    ) -> t.JobSubmit:
        """Copy files between internal CSCS file systems.
        Copy source_path to target_path.
        Possible to stage-out jobs providing the SLURM Id of a production job.
        More info about internal transfer: https://user.cscs.ch/storage/data_transfer/internal_transfer/

        :param machine: the machine name where the scheduler belongs to
        :param source_path: the absolute source path
        :param target_path: the absolute target path
        :param job_name: job name
        :param time: limit on the total run time of the job. Acceptable time formats 'minutes', 'minutes:seconds', 'hours:minutes:seconds', 'days-hours', 'days-hours:minutes' and 'days-hours:minutes:seconds'.
        :param stage_out_job_id: transfer data after job with ID {stage_out_job_id} is completed
        :param account: name of the bank account to be used in SLURM. If not set, system default is taken.
        :calls: POST `/storage/xfer-internal/cp`

                GET `/tasks/{taskid}`
        """
        self._current_method_requests = []
        endpoint = "/storage/xfer-internal/cp"
        json_response = self._internal_transfer(
            endpoint,
            machine,
            source_path,
            target_path,
            job_name,
            time,
            stage_out_job_id,
            account,
        )
        logger.info(f"Job submission task: {json_response['task_id']}")
        return self._poll_tasks(
            json_response["task_id"], "200", itertools.cycle([1, 5, 10])
        )

    def submit_rsync_job(
        self,
        machine: str,
        source_path: str,
        target_path: str,
        job_name: Optional[str] = None,
        time: Optional[str] = None,
        stage_out_job_id: Optional[str] = None,
        account: Optional[str] = None,
    ) -> t.JobSubmit:
        """Transfer files between internal CSCS file systems.
        Transfer source_path to target_path.
        Possible to stage-out jobs providing the SLURM Id of a production job.
        More info about internal transfer: https://user.cscs.ch/storage/data_transfer/internal_transfer/

        :param machine: the machine name where the scheduler belongs to
        :param source_path: the absolute source path
        :param target_path: the absolute target path
        :param job_name: job name
        :param time: limit on the total run time of the job. Acceptable time formats 'minutes', 'minutes:seconds', 'hours:minutes:seconds', 'days-hours', 'days-hours:minutes' and 'days-hours:minutes:seconds'.
        :param stage_out_job_id: transfer data after job with ID {stage_out_job_id} is completed
        :param account: name of the bank account to be used in SLURM. If not set, system default is taken.
        :calls: POST `/storage/xfer-internal/rsync`

                GET `/tasks/{taskid}`
        """
        self._current_method_requests = []
        endpoint = "/storage/xfer-internal/rsync"
        json_response = self._internal_transfer(
            endpoint,
            machine,
            source_path,
            target_path,
            job_name,
            time,
            stage_out_job_id,
            account,
        )
        logger.info(f"Job submission task: {json_response['task_id']}")
        return self._poll_tasks(
            json_response["task_id"], "200", itertools.cycle([1, 5, 10])
        )

    def submit_delete_job(
        self,
        machine: str,
        target_path: str,
        job_name: Optional[str] = None,
        time: Optional[str] = None,
        stage_out_job_id: Optional[str] = None,
        account: Optional[str] = None,
    ) -> t.JobSubmit:
        """Remove files in internal CSCS file systems.
        Remove file in target_path.
        Possible to stage-out jobs providing the SLURM Id of a production job.
        More info about internal transfer: https://user.cscs.ch/storage/data_transfer/internal_transfer/

        :param machine: the machine name where the scheduler belongs to
        :param target_path: the absolute target path
        :param job_name: job name
        :param time: limit on the total run time of the job. Acceptable time formats 'minutes', 'minutes:seconds', 'hours:minutes:seconds', 'days-hours', 'days-hours:minutes' and 'days-hours:minutes:seconds'.
        :param stage_out_job_id: transfer data after job with ID {stage_out_job_id} is completed
        :param account: name of the bank account to be used in SLURM. If not set, system default is taken.
        :calls: POST `/storage/xfer-internal/rm`

                GET `/tasks/{taskid}`
        """
        self._current_method_requests = []
        endpoint = "/storage/xfer-internal/rm"
        json_response = self._internal_transfer(
            endpoint,
            machine,
            None,
            target_path,
            job_name,
            time,
            stage_out_job_id,
            account,
        )
        logger.info(f"Job submission task: {json_response['task_id']}")
        return self._poll_tasks(
            json_response["task_id"], "200", itertools.cycle([1, 5, 10])
        )

    def external_upload(
        self, machine: str, source_path: str, target_path: str
    ) -> ExternalUpload:
        """Non blocking call for the upload of larger files.

        :param machine: the machine where the filesystem belongs to
        :param source_path: the source path in the local filesystem
        :param target_path: the target path in the machine's filesystem
        :returns: an ExternalUpload object
        """
        resp = self._post_request(
            endpoint="/storage/xfer-external/upload",
            additional_headers={"X-Machine-Name": machine},
            data={"targetPath": target_path, "sourcePath": source_path},
        )
        json_response = self._json_response([resp], 201)["task_id"]
        return ExternalUpload(self, json_response, [resp])

    def external_download(self, machine: str, source_path: str) -> ExternalDownload:
        """Non blocking call for the download of larger files.

        :param machine: the machine where the filesystem belongs to
        :param source_path: the source path in the local filesystem
        :returns: an ExternalDownload object
        """
        resp = self._post_request(
            endpoint="/storage/xfer-external/download",
            additional_headers={"X-Machine-Name": machine},
            data={"sourcePath": source_path},
        )
        return ExternalDownload(
            self, self._json_response([resp], 201)["task_id"], [resp]
        )

    # Reservation
    def all_reservations(self, machine: str) -> List[dict]:
        """List all active reservations and their status

        :param machine: the machine name
        :calls: GET `/reservations`
        """
        resp = self._get_request(
            endpoint="/reservations", additional_headers={"X-Machine-Name": machine}
        )
        return self._json_response([resp], 200)["success"]

    def create_reservation(
        self,
        machine: str,
        reservation: str,
        account: str,
        number_of_nodes: str,
        node_type: str,
        start_time: str,
        end_time: str,
    ) -> None:
        """Creates a new reservation with {reservation} name for a given SLURM groupname

        :param machine: the machine name
        :param reservation: the reservation name
        :param account: the account in SLURM to which the reservation is made for
        :param number_of_nodes: number of nodes needed for the reservation
        :param node_type: type of node
        :param start_time: start time for reservation (YYYY-MM-DDTHH:MM:SS)
        :param end_time: end time for reservation (YYYY-MM-DDTHH:MM:SS)
        :calls: POST `/reservations`
        """
        data = {
            "reservation": reservation,
            "account": account,
            "numberOfNodes": number_of_nodes,
            "nodeType": node_type,
            "starttime": start_time,
            "endtime": end_time,
        }
        resp = self._post_request(
            endpoint="/reservations",
            additional_headers={"X-Machine-Name": machine},
            data=data,
        )
        self._json_response([resp], 201)

    def update_reservation(
        self,
        machine: str,
        reservation: str,
        account: str,
        number_of_nodes: str,
        node_type: str,
        start_time: str,
        end_time: str,
    ) -> None:
        """Updates an already created reservation named {reservation}

        :param machine: the machine name
        :param reservation: the reservation name
        :param account: the account in SLURM to which the reservation is made for
        :param number_of_nodes: number of nodes needed for the reservation
        :param node_type: type of node
        :param start_time: start time for reservation (YYYY-MM-DDTHH:MM:SS)
        :param end_time: end time for reservation (YYYY-MM-DDTHH:MM:SS)
        :calls: PUT `/reservations/{reservation}`
        """
        data = {
            "account": account,
            "numberOfNodes": number_of_nodes,
            "nodeType": node_type,
            "starttime": start_time,
            "endtime": end_time,
        }
        resp = self._put_request(
            endpoint=f"/reservations/{reservation}",
            additional_headers={"X-Machine-Name": machine},
            data=data,
        )
        self._json_response([resp], 200)

    def delete_reservation(self, machine: str, reservation: str) -> None:
        """Deletes an already created reservation named {reservation}

        :param machine: the machine name
        :param reservation: the reservation name
        :calls: DELETE `/reservations/{reservation}`
        """
        resp = self._delete_request(
            endpoint=f"/reservations/{reservation}",
            additional_headers={"X-Machine-Name": machine},
        )
        self._json_response([resp], 204, allow_none_result=True)
