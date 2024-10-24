#
#  Copyright (c) 2024, ETH Zurich. All rights reserved.
#
#  Please, refer to the LICENSE file in the root directory.
#  SPDX-License-Identifier: BSD-3-Clause
#
from __future__ import annotations

import httpx
import json
import logging
import pathlib
import ssl

from contextlib import nullcontext
from io import BytesIO
from packaging.version import Version, parse
from typing import Any, ContextManager, Optional, List

from firecrest.utilities import (
    parse_retry_after, slurm_state_completed, time_block
)
from firecrest.FirecrestException import UnexpectedStatusException


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


class AsyncFirecrest:
    """
    This is the basic class you instantiate to access the FirecREST API v2.
    Necessary parameters are the firecrest URL and an authorization object.

    :param firecrest_url: FirecREST's URL
    :param authorization: the authorization object. This object is responsible
                          of handling the credentials and the only requirement
                          for it is that it has a method get_access_token()
                          that returns a valid access token.
    :param verify: either a boolean, in which case it controls whether
                   requests will verify the server’s TLS certificate,
                   or a string, in which case it must be a path to a CA bundle
                   to use
    """

    TOO_MANY_REQUESTS_CODE = 429

    def __init__(
        self,
        firecrest_url: str,
        authorization: Any,
        verify: str | bool | ssl.SSLContext = True,
    ) -> None:
        self._firecrest_url = firecrest_url
        self._authorization = authorization
        self._verify = verify
        #: This attribute will be passed to all the requests that will be made.
        #: How many seconds to wait for the server to send data before giving
        # up. After that time a `requests.exceptions.Timeout` error will be
        # raised.
        #:
        #: It can be a float or a tuple. More details here:
        # https://www.python-httpx.org/advanced/#fine-tuning-the-configuration.
        self.timeout: Any = None
        # type is Any because of some incompatibility between httpx and
        # requests library

        #: Disable all logging from the client.
        self.disable_client_logging: bool = False
        #: Number of retries in case the rate limit is reached. When it is set
        # to `None`, the client will keep trying until it gets a different
        # status code than 429.
        self.num_retries_rate_limit: Optional[int] = None
        self._api_version: Version = parse("2.0.0")
        self._session = httpx.AsyncClient(verify=self._verify)

    def set_api_version(self, api_version: str) -> None:
        """Set the version of the api of firecrest. By default it will be
        assumed that you are using version 2.0.0 or compatible. The version is
        parsed by the `packaging` library.
        """
        self._api_version = parse(api_version)

    async def close_session(self) -> None:
        """Close the httpx session"""
        await self._session.aclose()

    async def create_new_session(self) -> None:
        """Create a new httpx session"""
        if not self._session.is_closed:
            await self._session.aclose()

        self._session = httpx.AsyncClient(verify=self._verify)

    @property
    def is_session_closed(self) -> bool:
        """Check if the httpx session is closed"""
        return self._session.is_closed

    def log(self, level: int, msg: Any) -> None:
        """Log a message with the given level on the client logger.
        """
        if not self.disable_client_logging:
            logger.log(level, msg)

    # @_retry_requests  # type: ignore
    async def _get_request(
        self,
        endpoint,
        additional_headers=None,
        params=None
    ) -> httpx.Response:
        url = f"{self._firecrest_url}{endpoint}"
        headers = {
            "Authorization": f"Bearer {self._authorization.get_access_token()}"
        }
        if additional_headers:
            headers.update(additional_headers)

        self.log(logging.DEBUG, f"Making GET request to {endpoint}")
        with time_block(f"GET request to {endpoint}", logger):
            resp = await self._session.get(
                url=url, headers=headers, params=params, timeout=self.timeout
            )

        return resp

    # @_retry_requests  # type: ignore
    async def _post_request(
        self, endpoint, additional_headers=None, data=None, files=None
    ) -> httpx.Response:
        url = f"{self._firecrest_url}{endpoint}"
        headers = {
            "Authorization": f"Bearer {self._authorization.get_access_token()}"
        }
        if additional_headers:
            headers.update(additional_headers)

        self.log(logging.DEBUG, f"Making POST request to {endpoint}")
        with time_block(f"POST request to {endpoint}", logger):
            resp = await self._session.post(
                url=url, headers=headers, data=data, files=files, timeout=self.timeout
            )

        return resp

    # @_retry_requests  # type: ignore
    async def _put_request(
        self, endpoint, additional_headers=None, data=None
    ) -> httpx.Response:
        url = f"{self._firecrest_url}{endpoint}"
        headers = {
            "Authorization": f"Bearer {self._authorization.get_access_token()}"
        }
        if additional_headers:
            headers.update(additional_headers)

        self.log(logging.DEBUG, f"Making PUT request to {endpoint}")
        with time_block(f"PUT request to {endpoint}", logger):
            resp = await self._session.put(
                url=url, headers=headers, data=data, timeout=self.timeout
            )

        return resp

    # @_retry_requests  # type: ignore
    async def _delete_request(
        self, endpoint, additional_headers=None, data=None
    ) -> httpx.Response:
        url = f"{self._firecrest_url}{endpoint}"
        headers = {
            "Authorization": f"Bearer {self._authorization.get_access_token()}"
        }
        if additional_headers:
            headers.update(additional_headers)

        self.log(logging.INFO, f"Making DELETE request to {endpoint}")
        with time_block(f"DELETE request to {endpoint}", logger):
            # httpx doesn't support data in the `delete` method so we will
            # have to use the generic `request` method
            # https://www.python-httpx.org/compatibility/#request-body-on-http-methods
            resp = await self._session.request(
                method="DELETE",
                url=url,
                headers=headers,
                data=data,
                timeout=self.timeout,
            )

        return resp

    def _json_response(
        self,
        response: httpx.Response,
        expected_status_code: int
    ) -> dict:
        status_code = response.status_code
        # handle_response(response)
        if status_code != expected_status_code:
            self.log(
                logging.DEBUG,
                f"Unexpected status of last request {status_code}, it "
                f"should have been {expected_status_code}"
            )
            raise UnexpectedStatusException(
                [response], expected_status_code
            )

        return response.json()

    async def systems(self) -> List[dict]:
        """Returns available systems.

        :calls: GET `/status/systems`
        """
        resp = await self._get_request(endpoint="/status/systems")
        return resp.json()['systems']

    async def nodes(
        self,
        system_name: str
    ) -> List[dict]:
        """Returns nodes of the system.

        :param system_name: the system name where the nodes belong to
        :calls: GET `/status/{system_name}/nodes`
        """
        resp = await self._get_request(
            endpoint=f"/status/{system_name}/nodes"
        )
        return self._json_response(resp, 200)['nodes']

    async def reservations(
        self,
        system_name: str
    ) -> List[dict]:
        """Returns reservations defined in the system.

        :param system_name: the system name where the reservations belong to
        :calls: GET `/status/{system_name}/reservations`
        """
        resp = await self._get_request(
            endpoint=f"/status/{system_name}/reservations"
        )
        return self._json_response(resp, 200)['reservations']

    async def partitions(
        self,
        system_name: str
    ) -> List[dict]:
        """Returns partitions defined in the scheduler of the system.

        :param system_name: the system name where the partitions belong to
        :calls: GET `/status/{system_name}/partitions`
        """
        resp = await self._get_request(
            endpoint=f"/status/{system_name}/partitions"
        )
        return self._json_response(resp, 200)["partitions"]

    async def list_files(
        self,
        system_name: str,
        path: str,
        show_hidden: bool = False,
        recursive: bool = False,
        numeric_uid: bool = False,
        dereference: bool = False
    ) -> List[dict]:
        """Returns a list of files in a directory.

        :param system_name: the system name where the filesystem belongs to
        :param path: the absolute target path
        :param show_hidden: Show hidden files
        :param recursive: recursively list directories encountered
        :param dereference: when showing file information for a symbolic link,
                            show information for the file the link references
                            rather than for the link itself
        :calls: GET `/filesystem/{system_name}/ops/ls`
        """
        resp = await self._get_request(
            endpoint=f"/filesystem/{system_name}/ops/ls",
            params={
                "path": path,
                "showHidden": show_hidden,
                "recursive": recursive,
                "numericUid": numeric_uid,
                "followLinks": dereference
            }
        )
        return self._json_response(resp, 200)["output"]

    async def head(
        self,
        system_name: str,
        path: str,
        num_bytes: Optional[int] = None,
        num_lines: Optional[int] = None,
        exclude_trailing: bool = False,
    ) -> List[dict]:
        """Display the beginning of a specified file.
        By default 10 lines will be returned.
        `num_bytes` and `num_lines` cannot be specified simultaneously.

        :param system_name: the system name where the filesystem belongs to
        :param path: the absolute target path of the file
        :param num_bytes: the output will be the first NUM bytes of each file
        :param num_lines: the output will be the first NUM lines of each file
        :param exclude_trailing: the output will be the whole file, without
                                 the last NUM bytes/lines of each file. NUM
                                 should be specified in the respective
                                 argument through ``bytes`` or ``lines``.
        :calls: GET `/filesystem/{system_name}/ops/head`
        """
        # Validate that num_bytes and num_lines are not passed together
        if num_bytes is not None and num_lines is not None:
            raise ValueError(
                "You cannot specify both `num_bytes` and `num_lines`."
            )

        # If `exclude_trailing` is passed, either `num_bytes` or `num_lines`
        # must be passed
        if exclude_trailing and num_bytes is None and num_lines is None:
            raise ValueError(
                "`exclude_trailing` requires either `num_bytes` or "
                "`num_lines` to be specified.")

        params = {
            "path": path,
            "skipEnding": exclude_trailing
        }
        if num_bytes is not None:
            params["bytes"] = num_bytes

        if num_lines is not None:
            params["lines"] = num_lines

        resp = await self._get_request(
            endpoint=f"/filesystem/{system_name}/ops/head",
            params=params
        )
        return self._json_response(resp, 200)['output']

    async def tail(
        self,
        system_name: str,
        path: str,
        num_bytes: Optional[int] = None,
        num_lines: Optional[int] = None,
        exclude_beginning: bool = False,  # Changed to exclude_beginning
    ) -> List[dict]:
        """Display the ending of a specified file.
        By default, 10 lines will be returned.
        `num_bytes` and `num_lines` cannot be specified simultaneously.

        :param system_name: the system name where the filesystem belongs to
        :param path: the absolute target path of the file
        :param num_bytes: The output will be the last NUM bytes of each file
        :param num_lines: The output will be the last NUM lines of each file
        :param exclude_beginning: The output will be the whole file, without
                                  the first NUM bytes/lines of each file. NUM
                                  should be specified in the respective
                                  argument through ``num_bytes`` or
                                  ``num_lines``.
        :calls: GET `/filesystem/{system_name}/ops/tail`
        """
        # Ensure `num_bytes` and `num_lines` are not passed together
        if num_bytes is not None and num_lines is not None:
            raise ValueError(
                "You cannot specify both `num_bytes` and `num_lines`."
            )

        # If `exclude_beginning` is passed, either `num_bytes` or `num_lines` must be passed
        if exclude_beginning and num_bytes is None and num_lines is None:
            raise ValueError(
                "`exclude_beginning` requires either `num_bytes` or "
                "`num_lines` to be specified."
            )

        params = {
            "path": path,
            "skipBeginning": exclude_beginning
        }
        if num_bytes is not None:
            params["bytes"] = num_bytes

        if num_lines is not None:
            params["lines"] = num_lines

        resp = await self._get_request(
            endpoint=f"/filesystem/{system_name}/ops/tail",
            params=params
        )
        return self._json_response(resp, 200)['output']

    async def view(
        self,
        system_name: str,
        path: str,
    ) -> List[dict]:
        """
        View full file content (up to 5MB files)

        :param system_name: the system name where the filesystem belongs to
        :param path: the absolute target path
        :calls: GET `/filesystem/{system_name}/ops/view`
        """
        params: dict[str, str] = {"path": f"{path}"}

        resp = await self._get_request(
            endpoint=f"/filesystem/{system_name}/ops/view",
            params=params
        )
        return self._json_response(resp, 200)

    async def checksum(
        self,
        system_name: str,
        path: str,
    ) -> List[dict]:
        """
        Calculate the SHA256 (256-bit) checksum of a specified file.

        :param system_name: the system name where the filesystem belongs to
        :param path: the absolute target path
        :calls: GET `/filesystem/{system_name}/ops/checksum`
        """
        params: dict[str, str] = {"path": f"{path}"}

        resp = await self._get_request(
            endpoint=f"/filesystem/{system_name}/ops/checksum",
            params=params
        )
        return self._json_response(resp, 200)

    async def file_type(
        self,
        system_name: str,
        path: str,
    ) -> List[dict]:
        """
        Uses the `file` linux application to determine the type of a file.

        :param system_name: the system name where the filesystem belongs to
        :param path: the absolute target path
        :calls: GET `/filesystem/{system_name}/ops/checksum`
        """
        params: dict[str, str] = {"path": f"{path}"}

        resp = await self._get_request(
            endpoint=f"/filesystem/{system_name}/ops/file",
            params=params
        )
        return self._json_response(resp, 200)

    async def chmod(
        self,
        system_name: str,
        path: str,
        mode: str
    ) -> List[dict]:
        """Changes the file mod bits of a given file according to the specified mode.

        :param system_name: the system name where the filesystem belongs to
        :param path: the absolute target path
        :param mode: same as numeric mode of linux chmod tool
        :calls: PUT `/filesystem/{system_name}/ops/chmod`
        """
        params: dict[str, str] = {
            "path": f"{path}",
            "mode": f"{mode}"
        }
        resp = await self._put_request(
            endpoint=f"/filesystem/{system_name}/ops/chmod",
            params=params
        )
        return self._json_response(resp, 200)

    async def chown(
        self,
        system_name: str,
        path: str,
        owner: str,
        group: str
    ) -> List[dict]:
        """Changes the user and/or group ownership of a given file.
        If only owner or group information is passed, only that information will be updated.

        :param system_name: the system name where the filesystem belongs to
        :param path: the absolute target path
        :param owner: owner ID for target
        :param group: group ID for target
        :calls: PUT `/filesystem/{system_name}/ops/chown`
        """
        params: dict[str, str] = {"path": f"{path}"}
        if owner:
            params["owner"] = owner

        if group:
            params["group"] = group

        resp = await self._put_request(
            endpoint=f"/filesystem/{system_name}/ops/chown",
            params=params
        )
        return self._json_response(resp, 200)


    async def stat(
        self,
        system_name: str,
        path: str,
        dereference: bool,
    ) -> List[dict]:
        """
        Uses the stat linux application to determine the status of a file on the system's filesystem.
        The result follows: https://docs.python.org/3/library/os.html#os.stat_result.

        :param system_name: the system name where the filesystem belongs to
        :param path: the absolute target path
        :param dereference: follow symbolic links
        :calls: GET `/filesystem/{system_name}/ops/checksum`
        """
        params: dict[str, str] = {"path": f"{path}"}

        if dereference is True:
            params["dereference"] = dereference

        resp = await self._get_request(
            endpoint=f"/filesystem/{system_name}/ops/stat",
            params=params
        )
        return self._json_response(resp, 200)

    async def upload(
        self,
        system_name: str,
        source_path: str,
        target_path: str,
        filename: Optional[str] = None,
    ) -> List[dict]:
        """Blocking call to upload a small file.
        The file that will be uploaded will have the same name as the
        source_path.

        :param system_name: the system name where the filesystem belongs to
        :param source_path: the source path of the file or binary stream
        :param target_path: the absolute target path of the directory where
                            the file will be uploaded
        :param filename: naming target file to filename (default is same as
                         the local one)
        :calls: POST `/filesystem/{system_name}/transfer/upload`
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

            params: dict[str, str] = {
                "fileName": f,
                "targetPath": f"{target_path}"
            }

        resp = await self._post_request(
            endpoint=f"/filesystem/{system_name}/transfer/upload",
            params=params
        )
        return self._json_response(resp, 200)

    async def download(
        self,
        system_name: str,
        source_path: str,
        target_path: str,
    ) -> List[dict]:
        """Blocking call to download a small file.

        :param system_name: the system name where the filesystem belongs to
        :param source_path: the absolute source path of the file or binary
                            stream
        :param target_path: the target path in the local filesystem or binary
                            stream
        :calls: POST `/filesystem/{system_name}/transfer/upload`
        """
        params: dict[str, str] = {"source_path": f"{sourcePath}"}
        resp = await self._post_request(
            endpoint=f"/filesystem/{system_name}/transfer/upload",
            params=params
        )
        self._json_response([resp], 200, allow_none_result=True)
        context: ContextManager[BytesIO] = (
            open(target_path, "wb")  # type: ignore
            if isinstance(target_path, str) or isinstance(target_path, pathlib.Path)
            else nullcontext(target_path)
        )
        with context as f:
            f.write(resp.content)

    async def mv(
        self,
        system_name: str,
        source_path: str,
        target_path: str
    ) -> List[dict]:
        """Rename/move a file, directory, or symlink at the `source_path` to
        the `target_path` on `system_name`'s filesystem.
        When successful, the method returns a string with the new path of the file.

        :param system_name: the system name where the filesystem belongs to
        :param source_path: the absolute source path
        :param target_path: the absolute target path
        :calls: POST `/filesystem/{system_name}/transfer/mv`
        """
        params: dict[str, str] = {
            "sourcePath": f"{source_path}",
            "targetPath": f"{target_path}"
        }
        resp = await self._post_request(
            endpoint=f"/filesystem/{system_name}/transfer/mv",
            params=params
        )
        return self._json_response(resp, 200)

    async def cp(
        self,
        system_name: str,
        source_path: str,
        target_path: str
    ) -> List[dict]:
        """Copies file from `source_path` to `target_path`.
        When successful, the method returns a string with the path of the
        newly created file.

        :param system_name: the system name where the filesystem belongs to
        :param source_path: the absolute source path
        :param target_path: the absolute target path
        :calls: POST `/filesystem/{system_name}/transfer/cp`
        """
        params: dict[str, str] = {
            "sourcePath": f"{source_path}",
            "targetPath": f"{target_path}"
        }
        resp = await self._post_request(
            endpoint=f"/filesystem/{system_name}/transfer/cp",
            params=params
        )
        return self._json_response(resp, 200)

    async def rm(
        self,
        system_name: str,
        path: str,
        target_path: str
    ) -> List[dict]:
        """Blocking call to delete a small file.

        :param system_name: the system name where the filesystem belongs to
        :param path: the absolute target path
        :calls: DELETE `/filesystem/{system_name}/transfer/rm`
        """
        params: dict[str, str] = {"path": f"{path}"}
        resp = await self._post_request(
            endpoint=f"/filesystem/{system_name}/transfer/rm",
            params=params
        )
        return self._json_response(resp, 200)
