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
import os
import pathlib
import ssl
import time

from packaging.version import Version, parse
from typing import Any, Optional, List

from firecrest.utilities import (
    parse_retry_after,
    part_checksum_xml,
    sched_state_completed,
    time_block,
)
from firecrest.FirecrestException import (
    FirecrestException,
    JobTimeoutException,
    MultipartUploadException,
    TransferJobFailedException,
    TransferJobTimeoutException,
    UnexpectedStatusException,
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
    yield 0.2
    value = 0.5
    while True:
        yield value
        # Double the value for each iteration, up to 2 minutes
        if value < 60:
            value *= 2


class ExternalUpload:
    def __init__(self, client, transfer_info, local_file):
        self._client = client
        self._local_file = local_file
        self._transfer_info = transfer_info
        self._all_tags = []
        # Chunk size for the multipart upload. Default is 64MB.
        self.chunk_size = 64 * 1024 * 1024  # 64MB
        self._total_file_size = os.path.getsize(local_file)

    @property
    def transfer_data(self):
        return self._transfer_info

    def upload_file_to_stage(self):
        urls = self._transfer_info["partsUploadUrls"]
        # TODO: maybe we should run this in parallel
        for index, upload_url in enumerate(urls):
            self._upload_part(upload_url, index)

        # S3 complains when tags are not sorted
        self._all_tags.sort(key=lambda x: x['PartNumber'])
        checksum = part_checksum_xml(self._all_tags)
        self._complete_upload(
            checksum
        )

    def wait_for_transfer_job(self, timeout=None):
        self._client._wait_for_transfer_job(
            self._transfer_info,
            timeout=timeout
        )

    def _upload_part(self, url, index):
        chunk_size = self._transfer_info["maxPartSize"]

        def chunk_reader(f, c):
            i = 0
            while True:
                next_chunk = c if i + c <= chunk_size else chunk_size - i
                i += next_chunk
                data = f.read(next_chunk)
                if not data:
                    break

                yield data

        self._client.log(
            logging.DEBUG,
            f"Uploading part {index + 1} to {url}"
        )
        start = index * chunk_size
        if start + chunk_size > self._total_file_size:
            content_length = self._total_file_size - start
        else:
            content_length = chunk_size

            with open(self._local_file, "rb") as f:
                f.seek(start)
                resp = self._client._session.put(
                    url=url,
                    content=chunk_reader(f, self.chunk_size),
                    timeout=None,
                    headers={
                        "Content-Length": str(content_length)
                    }
                )

        if resp.status_code >= 400:
            raise MultipartUploadException(
                self._transfer_info,
                f"Failed to upload part {index + 1}: "
                f"{resp.status_code}: {resp.text}"
            )

        self._client.log(
            logging.DEBUG,
            f"Uploaded part {index + 1} to {url}"
        )
        e_tag = resp.headers['ETag']
        self._all_tags.append({
            'ETag': e_tag,
            'PartNumber': index + 1
        })

    def _complete_upload(self, checksum):
        url = self._transfer_info["completeUploadUrl"]
        self._client.log(
            logging.DEBUG,
            f"Finishing upload of file {self._local_file} to {url}"
        )
        resp = self._client._session.post(
            url=url,
            data=checksum
        )
        if resp.status_code >= 400:
            raise MultipartUploadException(
                self._transfer_info,
                f"Failed to finish upload: {resp.status_code}: {resp.text}"
            )


class ExternalDownload:
    def __init__(self, client, transfer_info, file_path):
        self._client = client
        self._transfer_info = transfer_info
        self._file_path = file_path
        # Chunk size for the multipart download. Default is 64MB.
        self.chunk_size = 64 * 1024 * 1024  # 64MB

    @property
    def transfer_data(self):
        return self._transfer_info

    def download_file_from_stage(self, file_path=None):
        file_name = file_path or self._file_path
        self._client.log(
            logging.DEBUG,
            f"Downloading file from {self._transfer_info['downloadUrl']} "
            f"to {file_name}"
        )

        with self._client._session.stream(
            "GET",
            self._transfer_info["downloadUrl"]
        ) as resp:
            resp.raise_for_status()

            with open(file_name, "wb") as f:
                for chunk in resp.iter_bytes(
                    chunk_size=self.chunk_size
                ):
                    f.write(chunk)

        self._client.log(
            logging.DEBUG,
            f"Downloaded file from {self._transfer_info['downloadUrl']} "
            f"to {file_name}"
        )

    def wait_for_transfer_job(self, timeout=None):
        self._client._wait_for_transfer_job(
            self._transfer_info,
            timeout=timeout
        )


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
    MAX_DIRECT_UPLOAD_SIZE = 1048576

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
                    client.log(
                        logging.DEBUG,
                        f"Rate limit is reached and the request has "
                        f"been retried already {num_retries} times"
                    )
                    break
                else:
                    reset = resp.headers.get(
                        "Retry-After",
                        default=resp.headers.get(
                            "RateLimit-Reset", default=10
                        ),
                    )
                    reset = parse_retry_after(reset, client.log)
                    client.log(
                        logging.INFO,
                        f"Rate limit is reached, will sleep for "
                        f"{reset} seconds and try again"
                    )
                    time.sleep(reset)
                    resp = func(*args, **kwargs)
                    num_retries += 1

            return resp

        return wrapper

    def __init__(
        self,
        firecrest_url: str,
        authorization: Any,
        verify: str | bool | ssl.SSLContext = True,
    ) -> None:
        self._firecrest_url = firecrest_url.rstrip('/')
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

    @_retry_requests  # type: ignore
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

    @_retry_requests  # type: ignore
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

    @_retry_requests  # type: ignore
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

    @_retry_requests  # type: ignore
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

    def server_version(self) -> str | None:
        """Returns the exact API version of the FirecREST server.

        :calls: GET `/status/systems`
        """
        resp = self._get_request(endpoint="/status/systems")
        if resp.headers.get("f7t-appversion") == "2.x.x":
            return "2"
        elif (
            resp.headers.get("f7t-appversion")
        ):
            return resp.headers["f7t-appversion"]
        else:
            return None

    def systems(self) -> List[dict]:
        """Returns available systems.

        :calls: GET `/status/systems`
        """
        resp = self._get_request(endpoint="/status/systems")
        return self._check_response(resp, 200)['systems']

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
        :numeric_uid: list numeric user and group IDs
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
    ) -> dict:
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
    ) -> dict:
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
        return self._check_response(resp, 201)["output"]

    def mv(
        self,
        system_name: str,
        source_path: str,
        target_path: str,
        account: Optional[str] = None,
        blocking: bool = True,
        timeout: Optional[float] = None
    ) -> dict:
        """Rename/move a file, directory, or symlink at the `source_path` to
        the `target_path` on `system_name`'s filesystem.
        This operation runs in a job.

        :param system_name: the system name where the filesystem belongs to
        :param source_path: the absolute source path
        :param target_path: the absolute target path
        :param account: the account to be used for the transfer job
        :param blocking: whether to wait for the job to complete
        :param timeout: the maximum time to wait for the job to complete

        :calls: POST `/filesystem/{system_name}/transfer/mv`
        """
        data: dict[str, str] = {
            "sourcePath": source_path,
            "targetPath": target_path,
        }
        if account is not None:
            data["account"] = account

        resp = self._post_request(
            endpoint=f"/filesystem/{system_name}/transfer/mv",
            data=json.dumps(data)
        )
        job_info = self._check_response(resp, 201)

        if blocking:
            self._wait_for_transfer_job(
                job_info,
                timeout=timeout
            )

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

    def _wait_for_transfer_job(
        self,
        job_info: dict,
        timeout: Optional[float] = None
    ) -> None:
        job_id = job_info["transferJob"]["jobId"]
        system_name = job_info["transferJob"]["system"]
        try:
            self.wait_for_job(
                system_name,
                job_id,
                timeout=timeout
            )
        except JobTimeoutException:
            self.log(
                logging.DEBUG,
                f"Transfer job {job_id} timed out"
            )
            raise TransferJobTimeoutException(job_info)

        try:
            # If job is cancelled before it starts running, the log file will
            # not be available
            stdout_file = self.view(
                system_name,
                job_info["transferJob"]["logs"]["outputLog"]
            )
        except FirecrestException as e:
            if (
                e.responses[-1].status_code == 404 and
                "No such file or directory" in e.responses[-1].json()['message']
            ):
                raise TransferJobFailedException(
                    job_info, file_not_found=True
                )
            else:
                raise e

        if (
            "Files were successfully" not in stdout_file and
            "File was successfully" not in stdout_file and
            "Multipart file upload successfully completed" not in stdout_file
        ):
            raise TransferJobFailedException(job_info)

    def wait_for_job(
        self,
        system_name: str,
        job_id: str,
        timeout: Optional[float] = None,
        not_found_timeout: float = 20.0
    ) -> List[Any]:
        """Wait for a job to complete. When the job is completed, it will
        return the job information.
        :param system_name: the system name where the filesystem belongs to
        :param job_id: the ID of the job to wait for
        :param timeout: the maximum time to wait for the job to complete
                        in seconds. If the timeout is set and the job is not
                        completed within this time, it will be cancelled.
        :param not_found_timeout: the maximum time to wait for the job to be
                                  available in the scheduler, in seconds.
                                  Slurm or other scheduler will need a few
                                  seconds before updating the database. If
                                  the job is not found within this time, the
                                  function will raise an exception.
        :calls: GET `/jobs/{system_name}/{job_id}`
        """
        if timeout:
            timeout_time = time.time() + timeout

        not_found_timeout_time = (
            time.time() + not_found_timeout
        )

        self.log(
            logging.DEBUG,
            f"Waiting for job {job_id}."
        )

        def check_timeout(sleep_time):
            if not timeout:
                return

            if time.time() + sleep_time > timeout_time:
                self.log(
                    logging.DEBUG,
                    f"Timeout is about to be reached, while waiting for "
                    f"job {job_id}. Will cancel the job to stop the transfer."
                )

                try:
                    self.cancel_job(system_name, job_id)
                except FirecrestException as e:
                    self.log(
                        logging.DEBUG,
                        f"Failed to cancel job {job_id}: {e}"
                    )

                raise JobTimeoutException(job_id)

        for i in sleep_generator():
            try:
                job = self.job_info(system_name, job_id)
            except FirecrestException as e:
                if (
                    e.responses[-1].status_code == 404 and
                    "Job not found" in e.responses[-1].json()['message']
                ):
                    if time.time() > not_found_timeout_time:
                        raise e

                    self.log(
                        logging.DEBUG,
                        f"Job {job_id} information is not yet available, will "
                        f"sleep for {i} seconds."
                    )
                    check_timeout(i)
                    time.sleep(i)
                    continue

            state = job[0]["status"]["state"]
            if isinstance(state, list):
                state = ",".join(state)

            if sched_state_completed(state):
                self.log(
                    logging.DEBUG,
                    f"Job {job_id} is completed with state: {state}."
                )
                break

            self.log(
                logging.DEBUG,
                f"Job {job_id} state is {state}. Will sleep for {i} seconds."
            )
            check_timeout(i)
            time.sleep(i)

        return job

    def cp(
        self,
        system_name: str,
        source_path: str,
        target_path: str,
        dereference: bool = False,
        account: Optional[str] = None,
        blocking: bool = True,
        timeout: Optional[float] = None
    ) -> dict:
        """Copies file from `source_path` to `target_path`.

        :param system_name: the system name where the filesystem belongs to
        :param source_path: the absolute source path
        :param target_path: the absolute target path
        :param dereference: dereference links when copying
        :param blocking: whether to wait for the job to complete
        :param account: the account to be used for the transfer job
        :param timeout: the maximum time to wait for the job to complete
        :calls: POST `/filesystem/{system_name}/transfer/cp`
        """
        # TODO: dereference is supported only after version 2.2.8 of the API
        data: dict[str, Any] = {
            "sourcePath": source_path,
            "targetPath": target_path,
            "dereference": dereference,
        }
        if account is not None:
            data["account"] = account

        resp = self._post_request(
            endpoint=f"/filesystem/{system_name}/transfer/cp",
            data=json.dumps(data)
        )
        job_info = self._check_response(resp, 201)

        if blocking:
            self._wait_for_transfer_job(
                job_info,
                timeout=timeout
            )

        return job_info

    def rm(
        self,
        system_name: str,
        path: str,
        account: Optional[str] = None,
        blocking: bool = True,
        timeout: Optional[float] = None
    ) -> Optional[dict]:
        """Delete a file.
        First the client will try to delete the file directly, and if it
        fails with a timeout, it will launch a job to delete the file.

        :param system_name: the system name where the filesystem belongs to
        :param path: the absolute target path
        :param account: the account to be used for the transfer job  (only
                        relevant when the file is not deleted directly)
        :param blocking: whether to wait for the job to complete (only
                         relevant when the file is not deleted directly)
        :param timeout: the maximum time to wait for the job to complete
        :calls: DELETE `/filesystem/{system_name}/ops/rm`

                DELETE `/filesystem/{system_name}/transfer/rm`

                GET `/jobs/{system_name}/{job_id}`
        """
        params = {
            "path": path,
        }
        try:
            resp = self._delete_request(
                endpoint=f"/filesystem/{system_name}/ops/rm",
                params=params
            )
            self._check_response(resp, 204)
            return None
        except FirecrestException as e:
            if e.responses[-1].status_code == 408:
                self.log(
                    logging.DEBUG,
                    f"The command for the deletion of file {path} "
                    f"got a timeout, a job will be launched to "
                    f"delete the file."
                )
            elif e.responses[-1].status_code == 500:
                try:
                    json_resp = e.responses[-1].json()
                    if (
                        "message" in json_resp and
                        "exit status:124" in json_resp["message"]
                    ):
                        self.log(
                            logging.DEBUG,
                            f"The command for the deletion of file {path} "
                            f"got a timeout, a job will be launched to "
                            f"delete the file."
                        )
                    else:
                        raise e

                except json.JSONDecodeError:
                    raise e
            else:
                raise e

        if account is not None:
            params["account"] = account

        resp = self._delete_request(
            endpoint=f"/filesystem/{system_name}/transfer/rm",
            params=params
        )

        job_info = self._check_response(resp, 200)
        if blocking:
            self._wait_for_transfer_job(
                job_info,
                timeout=timeout
            )

        return job_info

    def upload(
        self,
        system_name: str,
        local_file: str | pathlib.Path,
        directory: str,
        filename: str,
        account: Optional[str] = None,
        blocking: bool = True
    ) -> Optional[ExternalUpload]:
        """Upload a file to the system. Small files will be
        uploaded directly to FirecREST and will be immediately available.
        The function will return `None` in this case.
        Large files will be uploaded in parts to the
        staging area of FirecREST and then moved to the target directory in a
        job. The function will return the transfer job information in this
        case.

        :param system_name: the system name where the filesystem belongs to
        :param local_file: the local file's path to be uploaded (can be
                           relative)
        :param directory: the absolut target path of the directory where the
                          file will be uploaded
        :param filename: the name of the file in the target directory
        :param account: the account to be used for the transfer job (only
                        relevant when the file is larger than
                        `MAX_DIRECT_UPLOAD_SIZE`)
        :param blocking: whether to wait for the job to complete (only
                         relevant when the file is larger than
                         `MAX_DIRECT_UPLOAD_SIZE`)
        :calls: POST `/filesystem/{system_name}/transfer/upload`
        """
        if not os.path.isfile(local_file):
            raise FileNotFoundError(f"File not found: {local_file}")

        local_file_size = os.path.getsize(local_file)
        if local_file_size < self.MAX_DIRECT_UPLOAD_SIZE:
            self.log(
                logging.DEBUG,
                f"File ({local_file}) will be directly uploaded to the "
                f"target directory, since it's {local_file_size} bytes."
            )
            with open(local_file, "rb") as f:
                file_content = f.read()
                resp = self._post_request(
                    endpoint=f"/filesystem/{system_name}/ops/upload",
                    params={
                        "path": directory,
                    },
                    files={"file": (filename, file_content)}
                )
                self._check_response(resp, 204)
                return None

        self.log(
            logging.DEBUG,
            f"File ({local_file}) needs to be uploaded in parts to the "
            f"stage area of FirecREST and then moved to the "
            f"target directory, since it's {local_file_size} bytes."
        )
        data = {
            "source_path": directory,
            "fileName": filename,
            'fileSize': local_file_size,
        }
        if account is not None:
            data["account"] = account

        resp = self._post_request(
            endpoint=f"/filesystem/{system_name}/transfer/upload",
            data=json.dumps(data)
        )

        transfer_info = self._check_response(resp, 201)
        ext_upload = ExternalUpload(
            client=self,
            transfer_info=transfer_info,
            local_file=local_file,
        )

        if blocking:
            self.log(
                logging.DEBUG,
                f"Blocking until ({local_file}) is transfered to the "
                f"filesystem."
            )
            # Upload the file in parts
            ext_upload.upload_file_to_stage()

            # Wait for the file to be available in the target directory
            ext_upload.wait_for_transfer_job()

        return ext_upload

    def download(
        self,
        system_name: str,
        source_path: str,
        target_path: str,
        account: Optional[str] = None,
        blocking: bool = True
    ) -> Optional[ExternalDownload]:
        """Download a file from the remote system.

        :param system_name: the system name where the filesystem belongs to
        :param source_path: the absolute source path of the file
        :param target_path: the target path in the local filesystem (can
                            be relative path)
        :param account: the account to be used for the transfer job (only
                        relevant when the file is larger than
                        `MAX_DIRECT_UPLOAD_SIZE`)
        :param blocking: whether to wait for the job to complete
        :calls: POST `/filesystem/{system_name}/transfer/download`
        """
        # Check if the file is small enough to be downloaded directly
        try:
            file_info = self.stat(system_name, source_path)
            file_size = file_info["size"]
        except FirecrestException as e:
            if (
                e.responses[-1].status_code == 404 and
                "No such file or directory" in e.responses[-1].json()['message']
            ):
                raise FileNotFoundError(
                    f"File not found: {source_path} on system {system_name}"
                )
            else:
                raise e

        if file_size < self.MAX_DIRECT_UPLOAD_SIZE:
            self.log(
                logging.DEBUG,
                f"File ({source_path}) will be directly downloaded to the "
                f"target directory, since it's {file_size} bytes."
            )
            self.log(
                logging.DEBUG,
                "Arguments `account` and `blocking` will be ignored."
            )
            resp = self._get_request(
                endpoint=f"/filesystem/{system_name}/ops/download",
                params={
                    "path": source_path,
                }
            )
            self._check_response(resp, 200, return_json=False)

            with open(target_path, "wb") as f:
                f.write(resp.content)

            return None

        data = {
            "source_path": source_path,
        }
        if account is not None:
            data["account"] = account

        resp = self._post_request(
            endpoint=f"/filesystem/{system_name}/transfer/download",
            data=json.dumps(data)
        )

        transfer_info = self._check_response(resp, 201)
        download_obj = ExternalDownload(
            client=self,
            transfer_info=transfer_info,
            file_path=target_path
        )
        if blocking:
            self.log(
                logging.DEBUG,
                f"Blocking until ({source_path}) is transfered to the "
                f"filesystem."
            )
            download_obj.wait_for_transfer_job()
            download_obj.download_file_from_stage()

        return download_obj

    def submit(
        self,
        system_name: str,
        working_dir: str,
        script_str: Optional[str] = None,
        script_local_path: Optional[str] = None,
        script_remote_path: Optional[str] = None,
        env_vars: Optional[dict[str, str]] = None,
        account: Optional[str] = None
    ) -> dict:
        """Submit a job.

        :param system_name: the system name where the filesystem belongs to
        :param working_dir: the working directory of the job
        :param script_str: the job script
        :param script_local_path: path to the job script
        :param script_remote_path: path to the job script on the remote
                                   filesystem
        :param env_vars: environment variables to be set before running the
                         job
        :param account: the account to be used for the job
        :calls: POST `/compute/{system_name}/jobs`
        """

        if sum(
            arg is not None for arg in [
                script_str,
                script_local_path,
                script_remote_path
            ]
        ) != 1:
            raise ValueError(
                "Exactly one of the arguments `script_str`, "
                "`script_local_path` or `script_remote_path` must "
                "be set."
            )

        data: dict = {
            "job": {
                "working_directory": working_dir
            }
        }
        if script_remote_path:
            # TODO: Check that the version of the api supports this (added in
            # 2.2.6)

            if not script_remote_path.startswith("/"):
                raise ValueError(
                    "The `script_remote_path` must be an absolute path."
                )

            data["job"]["scriptPath"] = script_remote_path

        else:
            if script_local_path:
                if not os.path.isfile(script_local_path):
                    raise FileNotFoundError(
                        f"Script file not found: {script_local_path}"
                    )
                with open(script_local_path) as file:
                    script_str = file.read()

            data["job"]["script"] = script_str

        if env_vars:
            data["job"]["env"] = env_vars

        if account:
            # TODO: Check that the version of the api supports this (added in
            # 2.2.6)
            data["job"]["account"] = account

        resp = self._post_request(
            endpoint=f"/compute/{system_name}/jobs",
            data=json.dumps(data)
        )
        return self._check_response(resp, 201)

    def job_info(
        self,
        system_name: str,
        jobid: Optional[str] = None,
        allusers: bool = False
    ) -> list:
        """Get job information. When the job is not specified, it will return
        all the jobs.

        :param system_name: the system name where the filesystem belongs to
        :param jobid: the ID of the job
        :param allusers: whether to return jobs of all users or only the
                         current user
        :calls: GET `/compute/{system_name}/jobs` or
                GET `/compute/{system_name}/jobs/{job}`
        """
        url = f"/compute/{system_name}/jobs"
        url = f"{url}/{jobid}" if jobid else url

        # TODO: Check version compatibility for `allusers` parameter
        # It was added in FirecREST 2.2.7

        resp = self._get_request(
            endpoint=url,
            params={"allusers": allusers}
        )
        result_jobs = self._check_response(resp, 200)["jobs"]
        return result_jobs if result_jobs is not None else []

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
