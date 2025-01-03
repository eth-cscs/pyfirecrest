#
#  Copyright (c) 2024, ETH Zurich. All rights reserved.
#
#  Please, refer to the LICENSE file in the root directory.
#  SPDX-License-Identifier: BSD-3-Clause
#
from __future__ import annotations

import aiofiles
import httpx
import json
import logging
import os
import pathlib
import ssl
import time

from contextlib import contextmanager
from io import BytesIO
from packaging.version import Version, parse
from typing import Any, Optional, List

from firecrest.utilities import (
    slurm_state_completed, time_block
)
from firecrest.FirecrestException import (
    FirecrestException,
    TransferJobFailedException,
    UnexpectedStatusException
)


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


def sleep_generator():
    yield 0.2  # First yield 2 seconds because the api takes time to update
    value = 0.5
    while True:
        yield value
        value *= 2   # Double the value for each iteration


class Firecrest:
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
        self._session = httpx.Client(verify=self._verify)

    def set_api_version(self, api_version: str) -> None:
        """Set the version of the api of firecrest. By default it will be
        assumed that you are using version 2.0.0 or compatible. The version is
        parsed by the `packaging` library.
        """
        self._api_version = parse(api_version)

    def close_session(self) -> None:
        """Close the httpx session"""
        self._session.close()

    def create_new_session(self) -> None:
        """Create a new httpx session"""
        if not self._session.is_closed:
            self._session.close()

        self._session = httpx.Client(verify=self._verify)

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
    def _get_request(
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
            resp = self._session.get(
                url=url, headers=headers, params=params, timeout=self.timeout
            )

        return resp

    # @_retry_requests  # type: ignore
    def _post_request(
        self, endpoint, additional_headers=None, params=None, data=None, files=None
    ) -> httpx.Response:
        url = f"{self._firecrest_url}{endpoint}"
        headers = {
            "Authorization": f"Bearer {self._authorization.get_access_token()}"
        }
        if additional_headers:
            headers.update(additional_headers)

        self.log(logging.DEBUG, f"Making POST request to {endpoint}")
        with time_block(f"POST request to {endpoint}", logger):
            resp = self._session.post(
                url=url,
                headers=headers,
                params=params,
                data=data,
                files=files,
                timeout=self.timeout
            )

        return resp

    # @_retry_requests  # type: ignore
    def _put_request(
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
            resp = self._session.put(
                url=url, headers=headers, data=data, timeout=self.timeout
            )

        return resp

    # @_retry_requests  # type: ignore
    def _delete_request(
        self, endpoint, additional_headers=None, params=None, data=None
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
            resp = self._session.request(
                method="DELETE",
                url=url,
                headers=headers,
                params=params,
                data=data,
                timeout=self.timeout,
            )

        return resp

    def _check_response(
        self,
        response: httpx.Response,
        expected_status_code: int,
        return_json: bool = True
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

        return response.json() if return_json and status_code != 204 else {}

    def systems(self) -> List[dict]:
        """Returns available systems.

        :calls: GET `/status/systems`
        """
        resp = self._get_request(endpoint="/status/systems")
        return resp.json()['systems']

    def nodes(
        self,
        system_name: str
    ) -> List[dict]:
        """Returns nodes of the system.

        :param system_name: the system name where the nodes belong to
        :calls: GET `/status/{system_name}/nodes`
        """
        resp = self._get_request(
            endpoint=f"/status/{system_name}/nodes"
        )
        return self._check_response(resp, 200)['nodes']

    def reservations(
        self,
        system_name: str
    ) -> List[dict]:
        """Returns reservations defined in the system.

        :param system_name: the system name where the reservations belong to
        :calls: GET `/status/{system_name}/reservations`
        """
        resp = self._get_request(
            endpoint=f"/status/{system_name}/reservations"
        )
        return self._check_response(resp, 200)['reservations']

    def partitions(
        self,
        system_name: str
    ) -> List[dict]:
        """Returns partitions defined in the scheduler of the system.

        :param system_name: the system name where the partitions belong to
        :calls: GET `/status/{system_name}/partitions`
        """
        resp = self._get_request(
            endpoint=f"/status/{system_name}/partitions"
        )
        return self._check_response(resp, 200)["partitions"]

    def userinfo(
        self,
        system_name: str
    ) -> dict:
        """Returns user and groups information.

        :calls: GET `/status/{system_name}/userinfo`
        """
        resp = self._get_request(
             endpoint=f"/status/{system_name}/userinfo"
        )
        return self._check_response(resp, 200)

    def list_files(
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
        resp = self._get_request(
            endpoint=f"/filesystem/{system_name}/ops/ls",
            params={
                "path": path,
                "showHidden": show_hidden,
                "recursive": recursive,
                "numericUid": numeric_uid,
                "dereference": dereference
            }
        )
        return self._check_response(resp, 200)["output"]

    def head(
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

        resp = self._get_request(
            endpoint=f"/filesystem/{system_name}/ops/head",
            params=params
        )
        return self._check_response(resp, 200)['output']

    def tail(
        self,
        system_name: str,
        path: str,
        num_bytes: Optional[int] = None,
        num_lines: Optional[int] = None,
        exclude_beginning: bool = False,
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

        # If `exclude_beginning` is passed, either `num_bytes` or `num_lines`
        # must be passed
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

        resp = self._get_request(
            endpoint=f"/filesystem/{system_name}/ops/tail",
            params=params
        )
        return self._check_response(resp, 200)['output']

    def view(
        self,
        system_name: str,
        path: str,
    ) -> str:
        """
        View full file content (up to 5MB files)

        :param system_name: the system name where the filesystem belongs to
        :param path: the absolute target path of the file
        :calls: GET `/filesystem/{system_name}/ops/view`
        """
        resp = self._get_request(
            endpoint=f"/filesystem/{system_name}/ops/view",
            params={"path": path}
        )
        return self._check_response(resp, 200)["output"]

    def checksum(
        self,
        system_name: str,
        path: str,
    ) -> dict:
        """
        Calculate the SHA256 (256-bit) checksum of a specified file.

        :param system_name: the system name where the filesystem belongs to
        :param path: the absolute target path of the file
        :calls: GET `/filesystem/{system_name}/ops/checksum`
        """
        resp = self._get_request(
            endpoint=f"/filesystem/{system_name}/ops/checksum",
            params={"path": path}
        )
        return self._check_response(resp, 200)["output"]

    def file_type(
        self,
        system_name: str,
        path: str,
    ) -> str:
        """
        Uses the `file` linux application to determine the type of a file.

        :param system_name: the system name where the filesystem belongs to
        :param path: the absolute target path of the file
        :calls: GET `/filesystem/{system_name}/ops/file`
        """
        resp = self._get_request(
            endpoint=f"/filesystem/{system_name}/ops/file",
            params={"path": path}
        )
        return self._check_response(resp, 200)["output"]

    def chmod(
        self,
        system_name: str,
        path: str,
        mode: str
    ) -> dict:
        """Changes the file mod bits of a given file according to the
        specified mode.

        :param system_name: the system name where the filesystem belongs to
        :param path: the absolute target path of the file
        :param mode: same as numeric mode of linux chmod tool
        :calls: PUT `/filesystem/{system_name}/ops/chmod`
        """
        data: dict[str, str] = {
            "path": path,
            "mode": mode
        }
        resp = self._put_request(
            endpoint=f"/filesystem/{system_name}/ops/chmod",
            data=json.dumps(data)
        )
        return self._check_response(resp, 200)["output"]

    def chown(
        self,
        system_name: str,
        path: str,
        owner: str,
        group: str
    ) -> dict:
        """Changes the user and/or group ownership of a given file.
        If only owner or group information is passed, only that information
        will be updated.

        :param system_name: the system name where the filesystem belongs to
        :param path: the absolute target path of the file
        :param owner: owner ID for target
        :param group: group ID for target
        :calls: PUT `/filesystem/{system_name}/ops/chown`
        """
        data: dict[str, str] = {
            "path": path,
            "owner": owner,
            "group": group
        }
        resp = self._put_request(
            endpoint=f"/filesystem/{system_name}/ops/chown",
            data=json.dumps(data)
        )
        return self._check_response(resp, 200)["output"]

    def stat(
        self,
        system_name: str,
        path: str,
        dereference: bool = False,
    ) -> dict:
        """
        Uses the stat linux application to determine the status of a file on
        the system's filesystem. The result follows:
        https://docs.python.org/3/library/os.html#os.stat_result.

        :param system_name: the system name where the filesystem belongs to
        :param path: the absolute target path
        :param dereference: follow symbolic links
        :calls: GET `/filesystem/{system_name}/ops/stat`
        """
        resp = self._get_request(
            endpoint=f"/filesystem/{system_name}/ops/stat",
            params={
                "path": path,
                "dereference": dereference
            }
        )
        return self._check_response(resp, 200)["output"]

    def symlink(
        self,
        system_name: str,
        source_path: str,
        link_path: str,
    ) -> dict:
        """Create a symbolic link.

        :param system_name: the system name where the filesystem belongs to
        :param source_path: the absolute path to the file the link points to
        :param link_path: the absolute path to the symlink

        :calls: POST `/filesystem/{system_name}/ops/symlink`
        """
        resp = self._post_request(
            endpoint=f"/filesystem/{system_name}/ops/symlink",
            data=json.dumps({
                "sourcePath": source_path,
                "linkPath": link_path
            })
        )
        return self._check_response(resp, 201)

    def mkdir(
        self,
        system_name: str,
        path: str,
        create_parents: bool = False
    ) -> dict:
        """Create a directory.

        :param system_name: the system name where the filesystem belongs to
        :param path: the absolute path to the new directory
        :param create_parents: create intermediate parent directories

        :calls: POST `/filesystem/{system_name}/ops/mkdir`
        """
        resp = self._post_request(
            endpoint=f"/filesystem/{system_name}/ops/mkdir",
            data=json.dumps({
                "sourcePath": path,
                "parent": create_parents
            })
        )
        return self._check_response(resp, 201)

    def mv(
        self,
        system_name: str,
        source_path: str,
        target_path: str,
        blocking: bool = False
    ) -> dict:
        """Rename/move a file, directory, or symlink at the `source_path` to
        the `target_path` on `system_name`'s filesystem.
        This operation runs in a job.

        :param system_name: the system name where the filesystem belongs to
        :param source_path: the absolute source path
        :param target_path: the absolute target path
        :param blocking: whether to wait for the job to complete

        :calls: POST `/filesystem/{system_name}/transfer/mv`
        """
        data: dict[str, str] = {
            "sourcePath": source_path,
            "targetPath": target_path
        }
        resp = self._post_request(
            endpoint=f"/filesystem/{system_name}/transfer/mv",
            data=json.dumps(data)
        )
        job_info = self._check_response(resp, 201)

        if blocking:
            self._wait_for_transfer_job(job_info)

        return job_info

    def compress(
        self,
        system_name: str,
        source_path: str,
        target_path: str,
        dereference: bool = False
    ) -> None:
        """Compress a directory or file.

        :param system_name: the system name where the filesystem belongs to
        :param source_path: the absolute path to source directory
        :param target_path: the absolute path to the newly created
                            compressed file
        :param dereference: dereference links when compressing
        :calls: POST `/filesystem/{system_name}/ops/compress`
        """
        resp = self._post_request(
            endpoint=f"/filesystem/{system_name}/ops/compress",
            data=json.dumps({
                "source_path": source_path,
                "target_path": target_path,
                "dereference": dereference
            })
        )
        self._check_response(resp, 204)

    def extract(
        self,
        system_name: str,
        source_path: str,
        target_path: str,
    ) -> None:
        """Extract tar gzip archives.

        :param system_name: the system name where the filesystem belongs to
        :param source_path: the absolute path to the archive
        :param target_path: the absolute path to target directory
        :calls: POST `/filesystem/{system_name}/ops/extract`
        """
        resp = self._post_request(
            endpoint=f"/filesystem/{system_name}/ops/extract",
            data=json.dumps({
                "source_path": source_path,
                "target_path": target_path
            })
        )
        self._check_response(resp, 204)

    def _wait_for_transfer_job(self, job_info):
        job_id = job_info["transferJob"]["jobId"]
        system_name = job_info["transferJob"]["system"]
        for i in sleep_generator():
            try:
                job = self.job_info(system_name, job_id)
            except FirecrestException as e:
                if e.responses[-1].status_code == 404 and "Job not found" in e.responses[-1].json()['message']:
                    time.sleep(i)
                    continue

            state = job[0]["state"]["current"]
            if isinstance(state, list):
                state = ",".join(state)

            if slurm_state_completed(state):
                break

            time.sleep(i)

        # TODO: Check if the job was successful

        stdout_file = self.view(system_name, job_info["transferJob"]["logs"]["outputLog"])
        if (
            "Files were successfully" not in stdout_file and
            "File was successfully" not in stdout_file and
            "Multipart file upload successfully completed" not in stdout_file
        ):
            raise TransferJobFailedException(job_info)

    def cp(
        self,
        system_name: str,
        source_path: str,
        target_path: str,
        blocking: bool = False
    ) -> dict:
        """Copies file from `source_path` to `target_path`.

        :param system_name: the system name where the filesystem belongs to
        :param source_path: the absolute source path
        :param target_path: the absolute target path
        :param blocking: whether to wait for the job to complete
        :calls: POST `/filesystem/{system_name}/transfer/cp`
        """
        data: dict[str, str] = {
            "sourcePath": source_path,
            "targetPath": target_path
        }

        resp = self._post_request(
            endpoint=f"/filesystem/{system_name}/transfer/cp",
            data=json.dumps(data)
        )
        job_info = self._check_response(resp, 201)

        if blocking:
            self._wait_for_transfer_job(job_info)

        return job_info

    def rm(
        self,
        system_name: str,
        path: str,
        blocking: bool = False
    ) -> dict:
        """Delete a file.

        :param system_name: the system name where the filesystem belongs to
        :param path: the absolute target path
        :calls: DELETE `/filesystem/{system_name}/transfer/rm`
        """
        resp = self._delete_request(
            endpoint=f"/filesystem/{system_name}/transfer/rm",
            params={"path": path}
        )
        # self._check_response(resp, 204)

        job_info = self._check_response(resp, 200)

        if blocking:
            self._wait_for_transfer_job(job_info)

        return job_info

    def upload(
        self,
        system_name: str,
        local_file: str | pathlib.Path | BytesIO,
        directory: str,
        filename: str,
        blocking: bool = False
    ) -> dict:
        """Upload a file to the system. The user uploads a file to the
        staging area Object storage) of FirecREST and it will be moved
        to the target directory in a job.

        :param system_name: the system name where the filesystem belongs to
        :param local_file: the local file's path to be uploaded (can be
                           relative)
        :param source_path: the absolut target path of the directory where the
                            file will be uploaded
        :param filename: the name of the file in the target directory
        :param blocking: whether to wait for the job to complete
        :calls: POST `/filesystem/{system_name}/transfer/upload`
        """
        # TODO check if the file exists locally

        resp = self._post_request(
            endpoint=f"/filesystem/{system_name}/transfer/upload",
            data=json.dumps({
                "source_path": directory,
                "fileName": filename
            })
        )

        transfer_info = self._check_response(resp, 201)
        # Upload the file
        # FIXME
        with open(local_file, "rb") as f: # type: ignore
            data = f.read()  # TODO this will fail for large files
            self._session.put(
                url=transfer_info["uploadUrl"],
                data=data  # type: ignore
            )

        if blocking:
            self._wait_for_transfer_job(transfer_info)

        return transfer_info

    def download(
        self,
        system_name: str,
        source_path: str,
        target_path: str,
        blocking: bool = False
    ) -> dict:
        """Download a file from the remote system.

        :param system_name: the system name where the filesystem belongs to
        :param source_path: the absolute source path of the file
        :param target_path: the target path in the local filesystem (can
                            be relative path)
        :param blocking: whether to wait for the job to complete
        :calls: POST `/filesystem/{system_name}/transfer/upload`
        """
        resp = self._post_request(
            endpoint=f"/filesystem/{system_name}/transfer/download",
            data=json.dumps({
                "source_path": source_path,
            })
        )

        transfer_info = self._check_response(resp, 201)
        if blocking:
            self._wait_for_transfer_job(transfer_info)

            # Download the file
            with open(target_path, "wb") as f:
                # TODO this will fail for large files
                resp = self._session.get(
                    url=transfer_info["downloadUrl"],
                )
                f.write(resp.content)

        return transfer_info

    def submit(
        self,
        system_name: str,
        working_dir: str,
        script_str: Optional[str] = None,
        script_path: Optional[str] = None,
        env_vars: Optional[dict[str, str]] = None,
    ) -> dict:
        """Submit a job.

        :param system_name: the system name where the filesystem belongs to
        :param working_dir: the working directory of the job
        :param script_str: the job script
        :param script_path: path to the job script
        :param env_vars: environment variables to be set before running the
                         job
        :calls: POST `/compute/{system_name}/jobs`
        """

        if sum(arg is not None for arg in [script_str, script_path]) != 1:
            self.log(
                logging.ERROR,
                "Exactly one of the arguments `script_str` or `script_path` "
                "must be set."
            )
            raise ValueError(
                "Exactly one of the arguments `script_str` or `script_path` "
                "must be set."
            )

        if script_path:
            if not os.path.isfile(script_path):
                raise FileNotFoundError(f"Script file not found: {script_path}")
            with open(script_path) as file:
                script_str = file.read()

        data: dict[str, dict[str, Any]] = {
            "job": {
                "script": script_str,
                "working_directory": working_dir
            }
        }
        if env_vars:
            data["job"]["env"] = env_vars

        resp = self._post_request(
            endpoint=f"/compute/{system_name}/jobs",
            data=json.dumps(data)
        )
        return self._check_response(resp, 201)

    def job_info(
        self,
        system_name: str,
        jobid: Optional[str] = None
    ) -> dict:
        """Get job information. When the job is not specified, it will return
        all the jobs.

        :param system_name: the system name where the filesystem belongs to
        :param job: the ID of the job
        :calls: GET `/compute/{system_name}/jobs` or
                GET `/compute/{system_name}/jobs/{job}`
        """
        url = f"/compute/{system_name}/jobs"
        url = f"{url}/{jobid}" if jobid else url

        resp = self._get_request(
            endpoint=url,
        )
        return self._check_response(resp, 200)["jobs"]

    def job_metadata(
        self,
        system_name: str,
        jobid: str,
    ) -> dict:
        """Get job metadata.

        :param system_name: the system name where the filesystem belongs to
        :param jobid: the ID of the job
        :calls: GET `/compute/{system_name}/jobs/{jobid}/metadata`
        """
        resp = self._get_request(
            endpoint=f"/compute/{system_name}/jobs/{jobid}/metadata",
        )
        return self._check_response(resp, 200)['jobs']

    def cancel_job(
        self,
        system_name: str,
        jobid: str,
    ) -> dict:
        """Cancel a job.

        :param system_name: the system name where the filesystem belongs to
        :param jobid: the ID of the job to be cancelled
        :calls: DELETE `/compute/{system_name}/jobs/{jobid}`
        """
        resp = self._delete_request(
            endpoint=f"/compute/{system_name}/jobs/{jobid}",
        )
        return self._check_response(resp, 204)

    def attach_to_job(
        self,
        system_name: str,
        jobid: str,
        command: str,
    ) -> dict:
        """Attach a process to a job.

        :param system_name: the system name where the filesystem belongs to
        :param jobid: the ID of the job
        :param command: the command to be executed
        :calls: PUT `/compute/{system_name}/jobs/{jobid}/attach`
        """
        resp = self._put_request(
            endpoint=f"/compute/{system_name}/jobs/{jobid}/attach",
            data=json.dumps({"command": command})
        )
        return self._check_response(resp, 204)
