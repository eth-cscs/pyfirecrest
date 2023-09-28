#
#  Copyright (c) 2019-2023, ETH Zurich. All rights reserved.
#
#  Please, refer to the LICENSE file in the root directory.
#  SPDX-License-Identifier: BSD-3-Clause
#
from __future__ import annotations

import asyncio
import httpx
from io import BytesIO
import jwt
import logging
import pathlib
import requests
import sys
import time

from contextlib import nullcontext
from typing import Any, ContextManager, Optional, overload, Sequence, Tuple, List
from requests.compat import json  # type: ignore
from packaging.version import Version, parse

import firecrest.FirecrestException as fe
import firecrest.types as t
from firecrest.AsyncExternalStorage import AsyncExternalUpload, AsyncExternalDownload


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


class ComputeTask:
    """Helper object for blocking methods that require multiple requests"""

    def __init__(
        self,
        client: AsyncFirecrest,
        task_id: str,
        previous_responses: Optional[List[requests.Response]] = None,
    ) -> None:
        self._responses = [] if previous_responses is None else previous_responses
        self._client = client
        self._task_id = task_id

    async def poll_task(self, final_status):
        logger.info(f"Polling task {self._task_id} until status is {final_status}")
        resp = await self._client._task_safe(self._task_id, self._responses)
        while resp["status"] < final_status:
            # The rate limit is handled by the async client so no need to
            # add a `sleep` here
            resp = await self._client._task_safe(self._task_id, self._responses)

        logger.info(f'Status of {self._task_id} is {resp["status"]}')
        return resp["data"]


class AsyncFirecrest:
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
        async def wrapper(*args, **kwargs):
            client = args[0]
            num_retries = 0
            resp = await func(*args, **kwargs)
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
                    reset = int(reset)
                    microservice = kwargs["endpoint"].split("/")[1]
                    client = args[0]
                    logger.info(
                        f"Rate limit in `{microservice}` is reached, next "
                        f"request will be possible in {reset} sec"
                    )
                    retry_time = time.time() + reset
                    if retry_time > client._next_request_ts[microservice]:
                        client._next_request_ts[microservice] = retry_time

                    resp = await func(*args, **kwargs)
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
        self._verify = verify  # TODO: not supported in httpx
        self._sa_role = sa_role
        #: This attribute will be passed to all the requests that will be made.
        #: How many seconds to wait for the server to send data before giving up.
        #: After that time a `requests.exceptions.Timeout` error will be raised.
        #:
        #: It can be a float or a tuple. More details here: https://www.python-httpx.org/advanced/#fine-tuning-the-configuration.
        self.timeout: Any = None
        # type is Any because of some incompatibility between https and requests library

        #: Number of retries in case the rate limit is reached. When it is set to `None`, the
        #: client will keep trying until it gets a different status code than 429.
        self.num_retries_rate_limit: Optional[int] = None
        self._api_version: Version = parse("1.13.1")
        self._session = httpx.AsyncClient()

        #: Seconds between requests in each microservice
        self.time_between_calls: dict[str, float] = {  # TODO more detailed docs
            "compute": 5,
            "reservations": 5,
            "status": 5,
            "storage": 5,
            "tasks": 5,
            "utilities": 5,
        }
        self._next_request_ts: dict[str, float] = {
            "compute": 0,
            "reservations": 0,
            "status": 0,
            "storage": 0,
            "tasks": 0,
            "utilities": 0,
        }
        self._locks = {
            "compute": asyncio.Lock(),
            "reservations": asyncio.Lock(),
            "status": asyncio.Lock(),
            "storage": asyncio.Lock(),
            "tasks": asyncio.Lock(),
            "utilities": asyncio.Lock(),
        }

        # The following objects are used to "merge" requests in the same endpoints,
        # for example requests to tasks or polling for jobs
        self._polling_ids: dict[str, set] = {"compute": set(), "tasks": set()}
        self._polling_results: dict[str, List] = {"compute": [], "tasks": []}
        self._polling_events: dict[str, Optional[asyncio.Event]] = {
            "compute": None,
            "tasks": None,
        }

    def set_api_version(self, api_version: str) -> None:
        """Set the version of the api of firecrest. By default it will be assumed that you are
        using version 1.13.1 or compatible. The version is parsed by the `packaging` library.
        """
        self._api_version = parse(api_version)

    @_retry_requests  # type: ignore
    async def _get_request(
        self, endpoint, additional_headers=None, params=None
    ) -> httpx.Response:
        microservice = endpoint.split("/")[1]
        url = f"{self._firecrest_url}{endpoint}"

        async def _merged_get(event):
            await self._stall_request(microservice)
            async with self._locks[microservice]:
                results = self._polling_results[microservice]
                ids = self._polling_ids[microservice].copy()
                self._polling_events[microservice] = None
                self._polling_ids[microservice] = set()
                comma_sep_par = "tasks" if microservice == "tasks" else "jobs"
                if ids == {"*"}:
                    if comma_sep_par in params:
                        del params[comma_sep_par]
                else:
                    params[comma_sep_par] = ",".join(ids)

                headers = {
                    "Authorization": f"Bearer {self._authorization.get_access_token()}"
                }
                if additional_headers:
                    headers.update(additional_headers)

                logger.info(f"Making GET request to {endpoint}")
                resp = await self._session.get(
                    url=url, headers=headers, params=params, timeout=self.timeout
                )

                self._next_request_ts[microservice] = (
                    time.time() + self.time_between_calls[microservice]
                )

                results.append(resp)
                event.set()

            return

        if microservice == "tasks" or endpoint in ("/compute/jobs", "/compute/acct"):
            async with self._locks[microservice]:
                if self._polling_ids[microservice] != {"*"}:
                    comma_sep_par = "tasks" if microservice == "tasks" else "jobs"
                    if comma_sep_par not in params:
                        self._polling_ids[microservice] = {"*"}
                    else:
                        task_ids = params[comma_sep_par].split(",")
                        self._polling_ids[microservice].update(task_ids)

                if self._polling_events[microservice] is None:
                    self._polling_events[microservice] = asyncio.Event()
                    my_event = self._polling_events[microservice]
                    self._polling_results[microservice] = []
                    my_result = self._polling_results[microservice]
                    waiter = True
                    task = asyncio.create_task(_merged_get(my_event))
                else:
                    waiter = False
                    my_event = self._polling_events[microservice]
                    my_result = self._polling_results[microservice]

            if waiter:
                await task

            await my_event.wait()  # type: ignore
            resp = my_result[0]
            return resp

        # Otherwise just do what you usually do
        async with self._locks[microservice]:
            await self._stall_request(microservice)
            headers = {
                "Authorization": f"Bearer {self._authorization.get_access_token()}"
            }
            if additional_headers:
                headers.update(additional_headers)

            logger.info(f"Making GET request to {endpoint}")
            resp = await self._session.get(
                url=url, headers=headers, params=params, timeout=self.timeout
            )
            self._next_request_ts[microservice] = (
                time.time() + self.time_between_calls[microservice]
            )

        return resp

    @_retry_requests  # type: ignore
    async def _post_request(
        self, endpoint, additional_headers=None, data=None, files=None
    ) -> httpx.Response:
        microservice = endpoint.split("/")[1]
        url = f"{self._firecrest_url}{endpoint}"
        async with self._locks[microservice]:
            await self._stall_request(microservice)
            headers = {
                "Authorization": f"Bearer {self._authorization.get_access_token()}"
            }
            if additional_headers:
                headers.update(additional_headers)

            logger.info(f"Making POST request to {endpoint}")
            resp = await self._session.post(
                url=url, headers=headers, data=data, files=files, timeout=self.timeout
            )
            self._next_request_ts[microservice] = (
                time.time() + self.time_between_calls[microservice]
            )

        return resp

    @_retry_requests  # type: ignore
    async def _put_request(
        self, endpoint, additional_headers=None, data=None
    ) -> httpx.Response:
        microservice = endpoint.split("/")[1]
        url = f"{self._firecrest_url}{endpoint}"
        async with self._locks[microservice]:
            await self._stall_request(microservice)
            headers = {
                "Authorization": f"Bearer {self._authorization.get_access_token()}"
            }
            if additional_headers:
                headers.update(additional_headers)

            logger.info(f"Making PUT request to {endpoint}")
            resp = await self._session.put(
                url=url, headers=headers, data=data, timeout=self.timeout
            )
            self._next_request_ts[microservice] = (
                time.time() + self.time_between_calls[microservice]
            )

        return resp

    @_retry_requests  # type: ignore
    async def _delete_request(
        self, endpoint, additional_headers=None, data=None
    ) -> requests.Response:
        microservice = endpoint.split("/")[1]
        url = f"{self._firecrest_url}{endpoint}"
        async with self._locks[microservice]:
            await self._stall_request(microservice)
            headers = {
                "Authorization": f"Bearer {self._authorization.get_access_token()}"
            }
            if additional_headers:
                headers.update(additional_headers)

            logger.info(f"Making DELETE request to {endpoint}")
            # TODO: httpx doesn't support data in delete so we will have to
            # keep using the `requests` package for this
            resp = requests.delete(
                url=url, headers=headers, data=data, timeout=self.timeout
            )
            self._next_request_ts[microservice] = (
                time.time() + self.time_between_calls[microservice]
            )

        return resp

    async def _stall_request(self, microservice: str) -> None:
        if self._next_request_ts[microservice] is not None:
            while time.time() <= self._next_request_ts[microservice]:
                logger.debug(
                    f"`{microservice}` microservice has received too many requests. "
                    f"Going to sleep for "
                    f"~{self._next_request_ts[microservice] - time.time()} sec"
                )
                await asyncio.sleep(self._next_request_ts[microservice] - time.time())

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

    async def _tasks(
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
        endpoint = "/tasks"
        params = {}
        if task_ids:
            params = {"tasks": ",".join([str(j) for j in task_ids])}

        resp = await self._get_request(endpoint=endpoint, params=params)
        responses.append(resp)
        taskinfo = self._json_response(responses, 200)
        if len(task_ids) == 0:
            return taskinfo["tasks"]
        else:
            return {k: v for k, v in taskinfo["tasks"].items() if k in task_ids}

    async def _task_safe(
        self, task_id: str, responses: Optional[List[requests.Response]] = None
    ) -> t.Task:
        if responses is None:
            responses = self._current_method_requests

        task = (await self._tasks([task_id], responses))[task_id]
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

    async def _invalidate(
        self, task_id: str, responses: Optional[List[requests.Response]] = None
    ):
        responses = [] if responses is None else responses
        resp = await self._post_request(
            endpoint="/storage/xfer-external/invalidate",
            additional_headers={"X-Task-Id": task_id},
        )
        responses.append(resp)
        return self._json_response(responses, 201, allow_none_result=True)

    async def _poll_tasks(self, task_id: str, final_status, sleep_time):
        logger.info(f"Polling task {task_id} until status is {final_status}")
        resp = await self._task_safe(task_id)
        while resp["status"] < final_status:
            t = next(sleep_time)
            logger.info(
                f'Status of {task_id} is {resp["status"]}, sleeping for {t} sec'
            )
            await asyncio.sleep(t)
            resp = await self._task_safe(task_id)

        logger.info(f'Status of {task_id} is {resp["status"]}')
        return resp["data"]

    # Status
    async def all_services(self) -> List[t.Service]:
        """Returns a list containing all available micro services with a name, description, and status.

        :calls: GET `/status/services`
        """
        resp = await self._get_request(endpoint="/status/services")
        return self._json_response([resp], 200)["out"]

    async def service(self, service_name: str) -> t.Service:
        """Returns information about a micro service.
        Returns the name, description, and status.

        :param service_name: the service name
        :calls: GET `/status/services/{service_name}`
        """
        resp = await self._get_request(endpoint=f"/status/services/{service_name}")
        return self._json_response([resp], 200)  # type: ignore

    async def all_systems(self) -> List[t.System]:
        """Returns a list containing all available systems and response status.

        :calls: GET `/status/systems`
        """
        resp = await self._get_request(endpoint="/status/systems")
        return self._json_response([resp], 200)["out"]

    async def system(self, system_name: str) -> t.System:
        """Returns information about a system.
        Returns the name, description, and status.

        :param system_name: the system name
        :calls: GET `/status/systems/{system_name}`
        """
        resp = await self._get_request(endpoint=f"/status/systems/{system_name}")
        return self._json_response([resp], 200)["out"]

    async def parameters(self) -> t.Parameters:
        """Returns configuration parameters of the FirecREST deployment that is associated with the client.

        :calls: GET `/status/parameters`
        """
        resp = await self._get_request(endpoint="/status/parameters")
        return self._json_response([resp], 200)["out"]

    # Utilities
    async def list_files(
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

        resp = await self._get_request(
            endpoint="/utilities/ls",
            additional_headers={"X-Machine-Name": machine},
            params=params,
        )
        return self._json_response([resp], 200)["output"]

    async def mkdir(
        self, machine: str, target_path: str, p: Optional[bool] = None
    ) -> None:
        """Creates a new directory.

        :param machine: the machine name where the filesystem belongs to
        :param target_path: the absolute target path
        :param p: no error if existing, make parent directories as needed
        :calls: POST `/utilities/mkdir`
        """
        data: dict[str, Any] = {"targetPath": target_path}
        if p:
            data["p"] = p

        resp = await self._post_request(
            endpoint="/utilities/mkdir",
            additional_headers={"X-Machine-Name": machine},
            data=data,
        )
        self._json_response([resp], 201)

    async def mv(self, machine: str, source_path: str, target_path: str) -> None:
        """Rename/move a file, directory, or symlink at the `source_path` to the `target_path` on `machine`'s filesystem.

        :param machine: the machine name where the filesystem belongs to
        :param source_path: the absolute source path
        :param target_path: the absolute target path
        :calls: PUT `/utilities/rename`
        """
        resp = await self._put_request(
            endpoint="/utilities/rename",
            additional_headers={"X-Machine-Name": machine},
            data={"targetPath": target_path, "sourcePath": source_path},
        )
        self._json_response([resp], 200)

    async def chmod(self, machine: str, target_path: str, mode: str) -> None:
        """Changes the file mod bits of a given file according to the specified mode.

        :param machine: the machine name where the filesystem belongs to
        :param target_path: the absolute target path
        :param mode: same as numeric mode of linux chmod tool
        :calls: PUT `/utilities/chmod`
        """
        resp = await self._put_request(
            endpoint="/utilities/chmod",
            additional_headers={"X-Machine-Name": machine},
            data={"targetPath": target_path, "mode": mode},
        )
        self._json_response([resp], 200)

    async def chown(
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

        resp = await self._put_request(
            endpoint="/utilities/chown",
            additional_headers={"X-Machine-Name": machine},
            data=data,
        )
        self._json_response([resp], 200)

    async def copy(self, machine: str, source_path: str, target_path: str) -> None:
        """Copies file from `source_path` to `target_path`.

        :param machine: the machine name where the filesystem belongs to
        :param source_path: the absolute source path
        :param target_path: the absolute target path
        :calls: POST `/utilities/copy`
        """
        resp = await self._post_request(
            endpoint="/utilities/copy",
            additional_headers={"X-Machine-Name": machine},
            data={"targetPath": target_path, "sourcePath": source_path},
        )
        self._json_response([resp], 201)

    async def file_type(self, machine: str, target_path: str) -> str:
        """Uses the `file` linux application to determine the type of a file.

        :param machine: the machine name where the filesystem belongs to
        :param target_path: the absolute target path
        :calls: GET `/utilities/file`
        """
        resp = await self._get_request(
            endpoint="/utilities/file",
            additional_headers={"X-Machine-Name": machine},
            params={"targetPath": target_path},
        )
        return self._json_response([resp], 200)["output"]

    async def stat(
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

        resp = await self._get_request(
            endpoint="/utilities/stat",
            additional_headers={"X-Machine-Name": machine},
            params=params,
        )
        return self._json_response([resp], 200)["output"]

    async def symlink(self, machine: str, target_path: str, link_path: str) -> None:
        """Creates a symbolic link.

        :param machine: the machine name where the filesystem belongs to
        :param target_path: the absolute path that the symlink will point to
        :param link_path: the absolute path to the new symlink
        :calls: POST `/utilities/symlink`
        """
        resp = await self._post_request(
            endpoint="/utilities/symlink",
            additional_headers={"X-Machine-Name": machine},
            data={"targetPath": target_path, "linkPath": link_path},
        )
        self._json_response([resp], 201)

    async def simple_download(
        self, machine: str, source_path: str, target_path: str | pathlib.Path | BytesIO
    ) -> None:
        """Blocking call to download a small file.
        The maximun size of file that is allowed can be found from the parameters() call.

        :param machine: the machine name where the filesystem belongs to
        :param source_path: the absolute source path
        :param target_path: the target path in the local filesystem or binary stream
        :calls: GET `/utilities/download`
        """
        resp = await self._get_request(
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

    async def simple_upload(
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

            resp = await self._post_request(
                endpoint="/utilities/upload",
                additional_headers={"X-Machine-Name": machine},
                data={"targetPath": target_path},
                files={"file": f},
            )

        self._json_response([resp], 201)

    async def simple_delete(self, machine: str, target_path: str) -> None:
        """Blocking call to delete a small file.

        :param machine: the machine name where the filesystem belongs to
        :param target_path: the absolute target path
        :calls: DELETE `/utilities/rm`
        """
        resp = await self._delete_request(
            endpoint="/utilities/rm",
            additional_headers={"X-Machine-Name": machine},
            data={"targetPath": target_path},
        )
        self._json_response([resp], 204, allow_none_result=True)

    async def checksum(self, machine: str, target_path: str) -> str:
        """Calculate the SHA256 (256-bit) checksum of a specified file.

        :param machine: the machine name where the filesystem belongs to
        :param target_path: the absolute target path
        :calls: GET `/utilities/checksum`
        """
        resp = await self._get_request(
            endpoint="/utilities/checksum",
            additional_headers={"X-Machine-Name": machine},
            params={"targetPath": target_path},
        )
        return self._json_response([resp], 200)["output"]

    async def head(
        self,
        machine: str,
        target_path: str,
        bytes: Optional[str] = None,
        lines: Optional[str] = None,
        skip_ending: Optional[bool] = False,
    ) -> str:
        """Display the beginning of a specified file.
        By default 10 lines will be returned.
        Bytes and lines cannot be specified simultaneously.
        The final result will be smaller than `UTILITIES_MAX_FILE_SIZE` bytes.
        This variable is available in the parameters command.

        :param machine: the machine name where the filesystem belongs to
        :param target_path: the absolute target path
        :param bytes: the number of bytes to be displayed
        :param lines: the number of lines to be displayed
        :param skip_ending: the output will be the whole file, without the last NUM bytes/lines of each file. NUM should be specified in the respective argument through `bytes` or `lines`. Equivalent to passing -NUM to the `head` command.
        :calls: GET `/utilities/head`
        """
        resp = await self._get_request(
            endpoint="/utilities/head",
            additional_headers={"X-Machine-Name": machine},
            params={
                "targetPath": target_path,
                "lines": lines,
                "bytes": bytes,
                "skip_ending": skip_ending,
            },
        )
        return self._json_response([resp], 200)["output"]

    async def tail(
        self,
        machine: str,
        target_path: str,
        bytes: Optional[str] = None,
        lines: Optional[str] = None,
        skip_beginning: Optional[bool] = False,
    ) -> str:
        """Display the last part of a specified file.
        By default 10 lines will be returned.
        Bytes and lines cannot be specified simultaneously.
        The final result will be smaller than `UTILITIES_MAX_FILE_SIZE` bytes.
        This variable is available in the parameters command.

        :param machine: the machine name where the filesystem belongs to
        :param target_path: the absolute target path
        :param bytes: the number of bytes to be displayed
        :param lines: the number of lines to be displayed
        :param skip_beginning: the output will start with byte/line NUM of each file. NUM should be specified in the respective argument through `bytes` or `lines`. Equivalent to passing +NUM to the `tail` command.
        :calls: GET `/utilities/head`
        """
        resp = await self._get_request(
            endpoint="/utilities/tail",
            additional_headers={"X-Machine-Name": machine},
            params={
                "targetPath": target_path,
                "lines": lines,
                "bytes": bytes,
                "skip_beginning": skip_beginning,
            },
        )
        return self._json_response([resp], 200)["output"]

    async def view(self, machine: str, target_path: str) -> str:
        """View the content of a specified file.
        The final result will be smaller than `UTILITIES_MAX_FILE_SIZE` bytes.
        This variable is available in the parameters command.

        :param machine: the machine name where the filesystem belongs to
        :param target_path: the absolute target path
        :calls: GET `/utilities/checksum`
        """
        resp = await self._get_request(
            endpoint="/utilities/view",
            additional_headers={"X-Machine-Name": machine},
            params={"targetPath": target_path},
        )
        return self._json_response([resp], 200)["output"]

    async def whoami(self, machine=None) -> Optional[str]:
        """Returns the username that FirecREST will be using to perform the other calls.
        In the case the machine name is passed in the arguments, a call is made to the respective endpoint and the command whoami is run on the machine.
        Otherwise, the library decodes the token and will return `None` if the token is not valid.

        :calls: GET `/utilities/whoami`
        """
        if machine:
            resp = await self._get_request(
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
    async def _submit_request(self, machine: str, job_script, local_file, account=None):
        if local_file:
            with open(job_script, "rb") as f:
                if account:
                    data = {"account": account}
                else:
                    data = None

                resp = await self._post_request(
                    endpoint="/compute/jobs/upload",
                    additional_headers={"X-Machine-Name": machine},
                    files={"file": f},
                    data=data,
                )
        else:
            data = {"targetPath": job_script}
            if account:
                data["account"] = account

            resp = await self._post_request(
                endpoint="/compute/jobs/path",
                additional_headers={"X-Machine-Name": machine},
                data=data,
            )

        return self._json_response([resp], 201)

    async def _squeue_request(self, machine: str, jobs=None):
        jobs = [] if jobs is None else jobs
        params = {}
        if jobs:
            params = {"jobs": ",".join([str(j) for j in jobs])}

        resp = await self._get_request(
            endpoint="/compute/jobs",
            additional_headers={"X-Machine-Name": machine},
            params=params,
        )
        return self._json_response([resp], 200)

    async def _acct_request(
        self, machine: str, jobs=None, start_time=None, end_time=None
    ):
        jobs = [] if jobs is None else jobs
        params = {}
        if jobs:
            params["jobs"] = ",".join(jobs)

        if start_time:
            params["starttime"] = start_time

        if end_time:
            params["endtime"] = end_time

        resp = await self._get_request(
            endpoint="/compute/acct",
            additional_headers={"X-Machine-Name": machine},
            params=params,
        )
        return self._json_response([resp], 200)

    async def submit(
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
        if local_file:
            with open(job_script, "rb") as f:
                if account:
                    data = {"account": account}
                else:
                    data = None

                resp = await self._post_request(
                    endpoint="/compute/jobs/upload",
                    additional_headers={"X-Machine-Name": machine},
                    files={"file": f},
                    data=data,
                )
        else:
            data = {"targetPath": job_script}
            if account:
                data["account"] = account

            resp = await self._post_request(
                endpoint="/compute/jobs/path",
                additional_headers={"X-Machine-Name": machine},
                data=data,
            )

        json_response = self._json_response([resp], 201)
        logger.info(f"Job submission task: {json_response['task_id']}")
        t = ComputeTask(self, json_response["task_id"], [resp])

        # Inject taskid in the result
        result = await t.poll_task("200")
        result["firecrest_taskid"] = json_response["task_id"]
        return result

    async def poll(
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
        jobids = [str(j) for j in jobs] if jobs else []
        params = {}
        if jobids:
            params["jobs"] = ",".join(jobids)

        if start_time:
            params["starttime"] = start_time

        if end_time:
            params["endtime"] = end_time

        resp = await self._get_request(
            endpoint="/compute/acct",
            additional_headers={"X-Machine-Name": machine},
            params=params,
        )
        json_response = self._json_response([resp], 200)
        logger.info(f"Job polling task: {json_response['task_id']}")
        t = ComputeTask(self, json_response["task_id"], [resp])
        res = await t.poll_task("200")
        # When there is no job in the sacct output firecrest will return an empty dictionary instead of list
        if isinstance(res, dict):
            return list(res.values())
        elif jobids:
            # Filter since the request may have been merged with others
            ret = [i for i in res if i["jobid"] in jobids]
            return ret
        else:
            return res

    async def poll_active(
        self, machine: str, jobs: Optional[Sequence[str | int]] = None
    ) -> List[t.JobQueue]:
        """Retrieves information about active jobs.
        This call uses the `squeue -u <username>` command.

        :param machine: the machine name where the scheduler belongs to
        :param jobs: list of the IDs of the jobs
        :calls: GET `/compute/jobs`

                GET `/tasks/{taskid}`
        """
        jobs = jobs if jobs else []
        jobids = {str(j) for j in jobs}
        params = {}
        if jobs:
            params = {"jobs": ",".join([str(j) for j in jobids])}

        resp = await self._get_request(
            endpoint="/compute/jobs",
            additional_headers={"X-Machine-Name": machine},
            params=params,
        )
        json_response = self._json_response([resp], 200)
        logger.info(f"Job active polling task: {json_response['task_id']}")
        t = ComputeTask(self, json_response["task_id"], [resp])
        dict_result = await t.poll_task("200")
        if len(jobids):
            ret = [i for i in dict_result.values() if i["jobid"] in jobids]
        else:
            ret = list(dict_result.values())

        return ret

    async def cancel(self, machine: str, job_id: str | int) -> str:
        """Cancels running job.
        This call uses the `scancel` command.

        :param machine: the machine name where the scheduler belongs to
        :param job_id: the ID of the job
        :calls: DELETE `/compute/jobs/{job_id}`

                GET `/tasks/{taskid}`
        """
        resp = await self._delete_request(
            endpoint=f"/compute/jobs/{job_id}",
            additional_headers={"X-Machine-Name": machine},
        )
        json_response = self._json_response([resp], 200)
        logger.info(f"Job cancellation task: {json_response['task_id']}")
        t = ComputeTask(self, json_response["task_id"], [resp])
        return await t.poll_task("200")

    # Storage
    async def _internal_transfer(
        self,
        endpoint,
        machine,
        source_path,
        target_path,
        job_name,
        time,
        stage_out_job_id,
        account,
        ret_response,
    ):
        data = {"targetPath": target_path}
        if source_path:
            data["sourcePath"] = source_path

        if job_name:
            data["jobname"] = job_name

        if time:
            data["time"] = time

        if stage_out_job_id:
            data["stageOutJobId"] = stage_out_job_id

        if account:
            data["account"] = account

        resp = await self._post_request(
            endpoint=endpoint, additional_headers={"X-Machine-Name": machine}, data=data
        )
        ret_response.append(resp)
        return self._json_response([resp], 201)

    async def submit_move_job(
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
        resp: List[requests.Response] = []
        endpoint = "/storage/xfer-internal/mv"
        json_response = await self._internal_transfer(
            endpoint,
            machine,
            source_path,
            target_path,
            job_name,
            time,
            stage_out_job_id,
            account,
            resp,
        )
        logger.info(f"Job submission task: {json_response['task_id']}")
        t = ComputeTask(self, json_response["task_id"], resp)
        return await t.poll_task("200")

    async def submit_copy_job(
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
        resp: List[requests.Response] = []
        endpoint = "/storage/xfer-internal/cp"
        json_response = await self._internal_transfer(
            endpoint,
            machine,
            source_path,
            target_path,
            job_name,
            time,
            stage_out_job_id,
            account,
            resp,
        )
        logger.info(f"Job submission task: {json_response['task_id']}")
        t = ComputeTask(self, json_response["task_id"], resp)
        return await t.poll_task("200")

    async def submit_rsync_job(
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
        resp: List[requests.Response] = []
        endpoint = "/storage/xfer-internal/rsync"
        json_response = await self._internal_transfer(
            endpoint,
            machine,
            source_path,
            target_path,
            job_name,
            time,
            stage_out_job_id,
            account,
            resp,
        )
        logger.info(f"Job submission task: {json_response['task_id']}")
        t = ComputeTask(self, json_response["task_id"], resp)
        return await t.poll_task("200")

    async def submit_delete_job(
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
        resp: List[requests.Response] = []
        endpoint = "/storage/xfer-internal/rm"
        json_response = await self._internal_transfer(
            endpoint,
            machine,
            None,
            target_path,
            job_name,
            time,
            stage_out_job_id,
            account,
            resp,
        )
        logger.info(f"Job submission task: {json_response['task_id']}")
        t = ComputeTask(self, json_response["task_id"], resp)
        return await t.poll_task("200")

    async def external_upload(
        self, machine: str, source_path: str, target_path: str
    ) -> AsyncExternalUpload:
        """Non blocking call for the upload of larger files.

        :param machine: the machine where the filesystem belongs to
        :param source_path: the source path in the local filesystem
        :param target_path: the target path in the machine's filesystem
        :returns: an ExternalUpload object
        """
        resp = await self._post_request(
            endpoint="/storage/xfer-external/upload",
            additional_headers={"X-Machine-Name": machine},
            data={"targetPath": target_path, "sourcePath": source_path},
        )
        json_response = self._json_response([resp], 201)["task_id"]
        return AsyncExternalUpload(self, json_response, [resp])

    async def external_download(
        self, machine: str, source_path: str
    ) -> AsyncExternalDownload:
        """Non blocking call for the download of larger files.

        :param machine: the machine where the filesystem belongs to
        :param source_path: the source path in the local filesystem
        :returns: an ExternalDownload object
        """
        resp = await self._post_request(
            endpoint="/storage/xfer-external/download",
            additional_headers={"X-Machine-Name": machine},
            data={"sourcePath": source_path},
        )
        return AsyncExternalDownload(
            self, self._json_response([resp], 201)["task_id"], [resp]
        )

    # Reservation
    async def all_reservations(self, machine: str) -> List[dict]:
        """List all active reservations and their status

        :param machine: the machine name
        :calls: GET `/reservations`
        """
        resp = await self._get_request(
            endpoint="/reservations", additional_headers={"X-Machine-Name": machine}
        )
        return self._json_response([resp], 200)["success"]

    async def create_reservation(
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
        resp = await self._post_request(
            endpoint="/reservations",
            additional_headers={"X-Machine-Name": machine},
            data=data,
        )
        self._json_response([resp], 201)

    async def update_reservation(
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
        resp = await self._put_request(
            endpoint=f"/reservations/{reservation}",
            additional_headers={"X-Machine-Name": machine},
            data=data,
        )
        self._json_response([resp], 200)

    async def delete_reservation(self, machine: str, reservation: str) -> None:
        """Deletes an already created reservation named {reservation}

        :param machine: the machine name
        :param reservation: the reservation name
        :calls: DELETE `/reservations/{reservation}`
        """
        resp = await self._delete_request(
            endpoint=f"/reservations/{reservation}",
            additional_headers={"X-Machine-Name": machine},
        )
        self._json_response([resp], 204, allow_none_result=True)
