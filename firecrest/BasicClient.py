#
#  Copyright (c) 2019-2022, ETH Zurich. All rights reserved.
#
#  Please, refer to the LICENSE file in the root directory.
#  SPDX-License-Identifier: BSD-3-Clause
#
import itertools
import jwt
import logging
import pathlib
import requests
import shlex
import shutil
import subprocess
import time
import urllib.request

import firecrest.FirecrestException as fe

from contextlib import nullcontext
from requests.compat import json


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

    def __init__(self, client, task_id, previous_responses=None):
        previous_responses = [] if previous_responses is None else previous_responses
        self._client = client
        self._task_id = task_id
        self._in_progress = True
        self._status = None
        self._data = None
        self._object_storage_data = None
        self._sleep_time = itertools.cycle([1, 5, 10])
        self._responses = previous_responses

    @property
    def client(self):
        """Returns the client that will be used to get information for the task.

        :rtype: Firecrest Object
        """
        return self._client

    @property
    def task_id(self):
        """Returns the FirecREST task ID that is associated with this transfer.

        :rtype: string
        """
        return self._task_id

    def _update(self):
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
    def status(self):
        """Returns status of the task that is associated with this transfer.

        :calls: GET `/tasks/{taskid}`
        :rtype: string
        """
        self._update()
        return self._status

    @property
    def in_progress(self):
        """Returns `False` when the transfer has been completed (succesfully or with errors), otherwise `True`.

        :calls: GET `/tasks/{taskid}`
        :rtype: boolean
        """
        self._update()
        return self._status not in self._final_states

    @property
    def data(self):
        """Returns the task information from the latest response.

        :calls: GET `/tasks/{taskid}`
        :rtype: dictionary
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
    :type client: Firecrest
    :param task_id: FirecrREST task associated with the transfer
    :type task_id: string
    """

    def __init__(self, client, task_id, previous_responses=None):
        previous_responses = [] if previous_responses is None else previous_responses
        super().__init__(client, task_id, previous_responses)
        self._final_states = {"114", "115"}
        logger.info(f"Creating ExternalUpload object for task {task_id}")

    def finish_upload(self):
        """Finish the upload process.
        This call will upload the file to the staging area.
        Check with the method `status` or `in_progress` to see the status of the transfer.
        The transfer from the staging area to the systems's filesystem can take several seconds to start to start.

        :rtype: None
        """
        c = self.object_storage_data["command"]
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
    :type client: Firecrest
    :param task_id: FirecrREST task associated with the transfer
    :type task_id: string
    """

    def __init__(self, client, task_id, previous_responses=None):
        previous_responses = [] if previous_responses is None else previous_responses
        super().__init__(client, task_id, previous_responses)
        self._final_states = {"117", "118"}
        logger.info(f"Creating ExternalDownload object for task {task_id}")

    def invalidate_object_storage_link(self):
        """Invalidate the temporary URL for downloading.

        :calls: POST `/storage/xfer-external/invalidate`
        :rtype: None
        """
        self._client._invalidate(self._task_id)

    def finish_download(self, target_path):
        """Finish the download process.

        :param target_path: the local path to save the file
        :type target_path: string or binary stream
        :rtype: None
        """
        url = self.object_storage_data
        logger.info(f"Downloading the file from {url} and saving to {target_path}")
        # LOCAL FIX FOR MAC
        # url = url.replace("192.168.220.19", "localhost")
        context = (
            open(target_path, "wb")
            if isinstance(target_path, str)
            or isinstance(target_path, pathlib.PosixPath)
            else nullcontext(target_path)
        )
        with urllib.request.urlopen(url) as response, context as out_file:
            shutil.copyfileobj(response, out_file)


class Firecrest:
    """
    This is the basic class you instantiate to access the FirecREST API v1.
    Necessary parameters are the firecrest URL and an authorization object.
    This object is responsible of handling the credentials and the only
    requirement for it is that it has a method get_access_token() that returns
    a valid access token.

    :param firecrest_url: FirecREST's URL
    :type firecrest_url: string
    :param authorization: the authorization object
    :type authorization: object
    :param verify: either a boolean, in which case it controls whether requests will verify the server’s TLS certificate, or a string, in which case it must be a path to a CA bundle to use (default True)
    :type verify: boolean or string, optional
    :param sa_role: this corresponds to the `F7T_AUTH_ROLE` configuration parameter of the site. If you don't know how FirecREST is setup it's better to leave the default.
    :type sa_role: string, optional
    """

    def __init__(
        self, firecrest_url, authorization, verify=None, sa_role="firecrest-sa"
    ):
        self._firecrest_url = firecrest_url
        self._authorization = authorization
        # This should be used only for blocking operations that require multiple requests,
        # not for external upload/download
        self._current_method_requests = []
        self._verify = verify
        self._sa_role = sa_role
        #: It will be passed to all the requests that will be made.
        #: How many seconds to wait for the server to send data before giving up.
        #: After that time a `requests.exceptions.Timeout` error will be raised.
        #:
        #: It can be a float or a tuple. More details here: https://requests.readthedocs.io.
        self.timeout = None

    def _get_request(self, endpoint, additional_headers=None, params=None):
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

    def _post_request(self, endpoint, additional_headers=None, data=None, files=None):
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

    def _put_request(self, endpoint, additional_headers=None, data=None):
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

    def _delete_request(self, endpoint, additional_headers=None, data=None):
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

    def _json_response(self, responses, expected_status_code):
        # Will examine only the last response
        response = responses[-1]
        status_code = response.status_code
        # handle_response(response)
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
            ret = None

        return ret

    def _tasks(self, task_ids=None, responses=None):
        """Return a dictionary of FirecREST tasks and their last update.
        When `task_ids` is an empty list or contains more than one element the
        `/tasks` endpoint will be called. Otherwise `/tasks/{taskid}`.
        When the `/tasks` is called the method will not give an error for invalid IDs,
        but `/tasks/{taskid}` will raise an exception.

        :param task_ids: list of task IDs. When empty all tasks are returned.
        :type task_ids: list
        :param responses: list of responses that are associated with these tasks (only relevant for error)
        :type responses: list
        :calls: GET `/tasks` or `/tasks/{taskid}`
        :rtype: dictionary
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

    def _task_safe(self, task_id, responses=None):
        if responses is None:
            responses = self._current_method_requests

        task = self._tasks([task_id], responses)[task_id]
        status = int(task["status"])
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

    def _invalidate(self, task_id, responses=None):
        responses = [] if responses is None else responses
        resp = self._post_request(
            endpoint="/storage/xfer-external/invalidate",
            additional_headers={"X-Task-Id": task_id},
        )
        responses.append(resp)
        return self._json_response(responses, 201)

    def _poll_tasks(self, task_id, final_status, sleep_time):
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
    def all_services(self):
        """Returns a list containing all available micro services with a name, description, and status.

        :calls: GET `/status/services`
        :rtype: list of dictionaries (one for each service)
        """
        resp = self._get_request(endpoint="/status/services")
        return self._json_response([resp], 200)["out"]

    def service(self, service_name):
        """Returns information about a micro service.
        Returns the name, description, and status.

        :param service_name: the service name
        :type service_name: string
        :calls: GET `/status/services/{service_name}`
        :rtype: list of dictionaries (one for each service)
        """
        resp = self._get_request(endpoint=f"/status/services/{service_name}")
        return self._json_response([resp], 200)

    def all_systems(self):
        """Returns a list containing all available systems and response status.

        :calls: GET `/status/systems`
        :rtype: list of dictionaries (one for each system)
        """
        resp = self._get_request(endpoint="/status/systems")
        return self._json_response([resp], 200)["out"]

    def system(self, system_name):
        """Returns information about a system.
        Returns the name, description, and status.

        :param system_name: the system name
        :type system_name: string
        :calls: GET `/status/systems/{system_name}`
        :rtype: list of dictionaries (one for each system)
        """
        resp = self._get_request(endpoint=f"/status/systems/{system_name}")
        return self._json_response([resp], 200)["out"]

    def parameters(self):
        """Returns list of parameters that can be configured in environment files.

        :calls: GET `/status/parameters`
        :rtype: list of parameters
        """
        resp = self._get_request(endpoint="/status/parameters")
        return self._json_response([resp], 200)["out"]

    # Utilities
    def list_files(self, machine, target_path, show_hidden=None):
        """Returns a list of files in a directory.

        :param machine: the machine name where the filesystem belongs to
        :type machine: string
        :param target_path: the absolute target path
        :type target_path: string
        :param show_hidden: show hidden files
        :type show_hidden: boolean, optional
        :calls: GET `/utilities/ls`
        :rtype: list of files
        """
        params = {"targetPath": f"{target_path}"}
        if show_hidden == True:
            params["showhidden"] = show_hidden

        resp = self._get_request(
            endpoint="/utilities/ls",
            additional_headers={"X-Machine-Name": machine},
            params=params,
        )
        return self._json_response([resp], 200)["output"]

    def mkdir(self, machine, target_path, p=None):
        """Creates a new directory.

        :param machine: the machine name where the filesystem belongs to
        :type machine: string
        :param target_path: the absolute target path
        :type target_path: string
        :param p: no error if existing, make parent directories as needed
        :type p: boolean, optional
        :calls: POST `/utilities/mkdir`
        :rtype: None
        """
        data = {"targetPath": target_path}
        if p:
            data["p"] = p

        resp = self._post_request(
            endpoint="/utilities/mkdir",
            additional_headers={"X-Machine-Name": machine},
            data=data,
        )
        self._json_response([resp], 201)

    def mv(self, machine, source_path, target_path):
        """Rename/move a file, directory, or symlink at the `source_path` to the `target_path` on `machine`'s filesystem.

        :param machine: the machine name where the filesystem belongs to
        :type machine: string
        :param source_path: the absolute source path
        :type source_path: string
        :param target_path: the absolute target path
        :type target_path: string
        :calls: PUT `/utilities/rename`
        :rtype: None
        """
        resp = self._put_request(
            endpoint="/utilities/rename",
            additional_headers={"X-Machine-Name": machine},
            data={"targetPath": target_path, "sourcePath": source_path},
        )
        self._json_response([resp], 200)

    def chmod(self, machine, target_path, mode):
        """Changes the file mod bits of a given file according to the specified mode.

        :param machine: the machine name where the filesystem belongs to
        :type machine: string
        :param target_path: the absolute target path
        :type target_path: string
        :param mode: same as numeric mode of linux chmod tool
        :type mode: string
        :calls: PUT `/utilities/chmod`
        :rtype: None
        """
        resp = self._put_request(
            endpoint="/utilities/chmod",
            additional_headers={"X-Machine-Name": machine},
            data={"targetPath": target_path, "mode": mode},
        )
        self._json_response([resp], 200)

    def chown(self, machine, target_path, owner=None, group=None):
        """Changes the user and/or group ownership of a given file.
        If only owner or group information is passed, only that information will be updated.

        :param machine: the machine name where the filesystem belongs to
        :type machine: string
        :param target_path: the absolute target path
        :type target_path: string
        :param owner: owner ID for target
        :type owner: string, optional
        :param group: group ID for target
        :type group: string, optional
        :calls: PUT `/utilities/chown`
        :rtype: None
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

    def copy(self, machine, source_path, target_path):
        """Copies file from `source_path` to `target_path`.

        :param machine: the machine name where the filesystem belongs to
        :type machine: string
        :param source_path: the absolute source path
        :type source_path: string
        :param target_path: the absolute target path
        :type target_path: string
        :calls: POST `/utilities/copy`
        :rtype: None
        """
        resp = self._post_request(
            endpoint="/utilities/copy",
            additional_headers={"X-Machine-Name": machine},
            data={"targetPath": target_path, "sourcePath": source_path},
        )
        self._json_response([resp], 201)

    def file_type(self, machine, target_path):
        """Uses the `file` linux application to determine the type of a file.

        :param machine: the machine name where the filesystem belongs to
        :type machine: string
        :param target_path: the absolute target path
        :type target_path: string
        :calls: GET `/utilities/file`
        :rtype: string
        """
        resp = self._get_request(
            endpoint="/utilities/file",
            additional_headers={"X-Machine-Name": machine},
            params={"targetPath": target_path},
        )
        return self._json_response([resp], 200)["output"]

    def stat(self, machine, target_path, dereference=False):
        """Uses the stat linux application to determine the status of a file on the machine's filesystem.
        The result follows: https://docs.python.org/3/library/os.html#os.stat_result.

        :param machine: the machine name where the filesystem belongs to
        :type machine: string
        :param target_path: the absolute target path
        :type target_path: string
        :param dereference: follow link (default False)
        :type dereference: boolean, optional
        :calls: GET `/utilities/stat`
        :rtype: string
        """
        params = {"targetPath": target_path}
        if dereference:
            params["dereference"] = dereference

        resp = self._get_request(
            endpoint="/utilities/stat",
            additional_headers={"X-Machine-Name": machine},
            params=params,
        )
        return self._json_response([resp], 200)["output"]

    def symlink(self, machine, target_path, link_path):
        """Creates a symbolic link.

        :param machine: the machine name where the filesystem belongs to
        :type machine: string
        :param target_path: the absolute path that the symlink will point to
        :type target_path: string
        :param link_path: the absolute path to the new symlink
        :type link_path: string
        :calls: POST `/utilities/symlink`
        :rtype: None
        """
        resp = self._post_request(
            endpoint="/utilities/symlink",
            additional_headers={"X-Machine-Name": machine},
            data={"targetPath": target_path, "linkPath": link_path},
        )
        self._json_response([resp], 201)

    def simple_download(self, machine, source_path, target_path):
        """Blocking call to download a small file.
        The maximun size of file that is allowed can be found from the parameters() call.

        :param machine: the machine name where the filesystem belongs to
        :type machine: string
        :param source_path: the absolute source path
        :type source_path: string
        :param target_path: the target path in the local filesystem or binary stream
        :type target_path: string or binary stream
        :calls: GET `/utilities/download`
        :rtype: None
        """
        resp = self._get_request(
            endpoint="/utilities/download",
            additional_headers={"X-Machine-Name": machine},
            params={"sourcePath": source_path},
        )
        self._json_response([resp], 200)
        context = (
            open(target_path, "wb")
            if isinstance(target_path, str)
            or isinstance(target_path, pathlib.PosixPath)
            else nullcontext(target_path)
        )
        with context as f:
            f.write(resp.content)

    def simple_upload(self, machine, source_path, target_path, filename=None):
        """Blocking call to upload a small file.
        The file that will be uploaded will have the same name as the source_path.
        The maximum size of file that is allowed can be found from the parameters() call.

        :param machine: the machine name where the filesystem belongs to
        :type machine: string
        :param source_path: the source path of the file or binary stream
        :type source_path: string or binary stream
        :param target_path: the absolute target path of the directory where the file will be uploaded
        :type target_path: string
        :param filename: naming target file to filename (default is same as the local one)
        :type filename: string
        :calls: POST `/utilities/upload`
        :rtype: None
        """
        context = (
            open(source_path, "rb")
            if isinstance(source_path, str)
            or isinstance(source_path, pathlib.PosixPath)
            else nullcontext(source_path)
        )
        with context as f:
            # Set filename
            if filename is not None:
                f = (filename, f)

            resp = self._post_request(
                endpoint="/utilities/upload",
                additional_headers={"X-Machine-Name": machine},
                data={"targetPath": target_path},
                files={"file": f},
            )

        self._json_response([resp], 201)

    def simple_delete(self, machine, target_path):
        """Blocking call to delete a small file.

        :param machine: the machine name where the filesystem belongs to
        :type machine: string
        :param target_path: the absolute target path
        :type target_path: string
        :calls: DELETE `/utilities/rm`
        :rtype: None
        """
        resp = self._delete_request(
            endpoint="/utilities/rm",
            additional_headers={"X-Machine-Name": machine},
            data={"targetPath": target_path},
        )
        self._json_response([resp], 204)

    def checksum(self, machine, target_path):
        """Calculate the SHA256 (256-bit) checksum of a specified file.

        :param machine: the machine name where the filesystem belongs to
        :type machine: string
        :param target_path: the absolute target path
        :type target_path: string
        :calls: GET `/utilities/checksum`
        :rtype: string
        """
        resp = self._get_request(
            endpoint="/utilities/checksum",
            additional_headers={"X-Machine-Name": machine},
            params={"targetPath": target_path},
        )
        return self._json_response([resp], 200)["output"]

    def view(self, machine, target_path):
        """View the content of a specified file.

        :param machine: the machine name where the filesystem belongs to
        :type machine: string
        :param target_path: the absolute target path
        :type target_path: string
        :calls: GET `/utilities/checksum`
        :rtype: string
        """
        resp = self._get_request(
            endpoint="/utilities/view",
            additional_headers={"X-Machine-Name": machine},
            params={"targetPath": target_path},
        )
        return self._json_response([resp], 200)["output"]

    def whoami(self):
        """Returns the username that FirecREST will be using to perform the other calls.
        Will return `None` if the token is not valid.

        :rtype: string or None
        """

        # FIXME This needs to be added as an endpoint in FirecREST,
        # now it's making a guess and it could be wrong.
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
    def _submit_request(self, machine, job_script, local_file):
        if local_file:
            with open(job_script, "rb") as f:
                resp = self._post_request(
                    endpoint="/compute/jobs/upload",
                    additional_headers={"X-Machine-Name": machine},
                    files={"file": f},
                )
        else:
            resp = self._post_request(
                endpoint="/compute/jobs/path",
                additional_headers={"X-Machine-Name": machine},
                data={"targetPath": job_script},
            )

        self._current_method_requests.append(resp)
        return self._json_response(self._current_method_requests, 201)

    def _squeue_request(self, machine, jobs=None):
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

    def _acct_request(self, machine, jobs=None, start_time=None, end_time=None):
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

    def submit(self, machine, job_script, local_file=True):
        """Submits a batch script to SLURM on the target system

        :param machine: the machine name where the scheduler belongs to
        :type machine: string
        :param job_script: the path of the script (if it's local it can be relative path, if it is on the machine it has to be the absolute path)
        :type job_script: string
        :param local_file: batch file can be local (default) or on the machine's filesystem
        :type local_file: boolean, optional
        :calls: POST `/compute/jobs/upload` or POST `/compute/jobs/path`

                GET `/tasks/{taskid}`
        :rtype: dictionary
        """
        self._current_method_requests = []
        json_response = self._submit_request(machine, job_script, local_file)
        logger.info(f"Job submission task: {json_response['task_id']}")
        return self._poll_tasks(
            json_response["task_id"], "200", itertools.cycle([1, 5, 10])
        )

    def poll(self, machine, jobs=None, start_time=None, end_time=None):
        """Retrieves information about submitted jobs.
        This call uses the `sacct` command.

        :param machine: the machine name where the scheduler belongs to
        :type machine: string
        :param jobs: list of the IDs of the jobs
        :type jobs: list of strings/integers, optional
        :param start_time: Start time (and/or date) of job's query. Allowed formats are HH:MM[:SS] [AM|PM] MMDD[YY] or MM/DD[/YY] or MM.DD[.YY] MM/DD[/YY]-HH:MM[:SS] YYYY-MM-DD[THH:MM[:SS]]
        :type start_time: string, optional
        :param end_time: End time (and/or date) of job's query. Allowed formats are HH:MM[:SS] [AM|PM] MMDD[YY] or MM/DD[/YY] or MM.DD[.YY] MM/DD[/YY]-HH:MM[:SS] YYYY-MM-DD[THH:MM[:SS]]
        :type end_time: string, optional
        :calls: GET `/compute/acct`

                GET `/tasks/{taskid}`
        :rtype: dictionary
        """
        self._current_method_requests = []
        jobs = jobs if jobs else []
        jobids = [str(j) for j in jobs]
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

    def poll_active(self, machine, jobs=None):
        """Retrieves information about active jobs.
        This call uses the `squeue -u <username>` command.

        :param machine: the machine name where the scheduler belongs to
        :type machine: string
        :param jobs: list of the IDs of the jobs
        :type jobs: list of strings/integers, optional
        :calls: GET `/compute/jobs`

                GET `/tasks/{taskid}`
        :rtype: dictionary
        """
        self._current_method_requests = []
        jobs = jobs if jobs else []
        jobids = [str(j) for j in jobs]
        json_response = self._squeue_request(machine, jobids)
        logger.info(f"Job active polling task: {json_response['task_id']}")
        dict_result = self._poll_tasks(
            json_response["task_id"], "200", itertools.cycle([1, 5, 10])
        )
        return list(dict_result.values())

    def cancel(self, machine, job_id):
        """Cancels running job.
        This call uses the `scancel` command.

        :param machine: the machine name where the scheduler belongs to
        :type machine: string
        :param job_id: the absolute target path (default [])
        :type job_id: list of strings/integers, optional
        :calls: DELETE `/compute/jobs/{job_id}`

                GET `/tasks/{taskid}`
        :rtype: dictionary
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
            data["jobname"] = job_name

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
        machine,
        source_path,
        target_path,
        job_name=None,
        time=None,
        stage_out_job_id=None,
        account=None,
    ):
        """Move files between internal CSCS file systems.
        Rename/Move source_path to target_path.
        Possible to stage-out jobs providing the SLURM ID of a production job.
        More info about internal transfer: https://user.cscs.ch/storage/data_transfer/internal_transfer/

        :param machine: the machine name where the scheduler belongs to
        :type machine: string
        :param source_path: the absolute source path
        :type source_path: string
        :param target_path: the absolute target path
        :type target_path: string,
        :param job_name: job name
        :type job_name: string, optional
        :param time: limit on the total run time of the job. Acceptable time formats 'minutes', 'minutes:seconds', 'hours:minutes:seconds', 'days-hours', 'days-hours:minutes' and 'days-hours:minutes:seconds'.
        :type time: string, optional
        :param stage_out_job_id: transfer data after job with ID {stage_out_job_id} is completed
        :type stage_out_job_id: string, optional
        :param account: name of the bank account to be used in SLURM. If not set, system default is taken.
        :type account: string, optional
        :calls: POST `/storage/xfer-internal/mv`

                GET `/tasks/{taskid}`
        :rtype: dictionary with the jobid of the submitted job
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
        machine,
        source_path,
        target_path,
        job_name=None,
        time=None,
        stage_out_job_id=None,
        account=None,
    ):
        """Copy files between internal CSCS file systems.
        Copy source_path to target_path.
        Possible to stage-out jobs providing the SLURM Id of a production job.
        More info about internal transfer: https://user.cscs.ch/storage/data_transfer/internal_transfer/

        :param machine: the machine name where the scheduler belongs to
        :type machine: string
        :param source_path: the absolute source path
        :type source_path: string
        :param target_path: the absolute target path
        :type target_path: string,
        :param job_name: job name
        :type job_name: string, optional
        :param time: limit on the total run time of the job. Acceptable time formats 'minutes', 'minutes:seconds', 'hours:minutes:seconds', 'days-hours', 'days-hours:minutes' and 'days-hours:minutes:seconds'.
        :type time: string, optional
        :param stage_out_job_id: transfer data after job with ID {stage_out_job_id} is completed
        :type stage_out_job_id: string, optional
        :param account: name of the bank account to be used in SLURM. If not set, system default is taken.
        :type account: string, optional
        :calls: POST `/storage/xfer-internal/cp`

                GET `/tasks/{taskid}`
        :rtype: dictionary with the jobid of the submitted job
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
        machine,
        source_path,
        target_path,
        job_name=None,
        time=None,
        stage_out_job_id=None,
        account=None,
    ):
        """Transfer files between internal CSCS file systems.
        Transfer source_path to target_path.
        Possible to stage-out jobs providing the SLURM Id of a production job.
        More info about internal transfer: https://user.cscs.ch/storage/data_transfer/internal_transfer/

        :param machine: the machine name where the scheduler belongs to
        :type machine: string
        :param source_path: the absolute source path
        :type source_path: string
        :param target_path: the absolute target path
        :type target_path: string,
        :param job_name: job name
        :type job_name: string, optional
        :param time: limit on the total run time of the job. Acceptable time formats 'minutes', 'minutes:seconds', 'hours:minutes:seconds', 'days-hours', 'days-hours:minutes' and 'days-hours:minutes:seconds'.
        :type time: string, optional
        :param stage_out_job_id: transfer data after job with ID {stage_out_job_id} is completed
        :type stage_out_job_id: string, optional
        :param account: name of the bank account to be used in SLURM. If not set, system default is taken.
        :type account: string, optional
        :calls: POST `/storage/xfer-internal/rsync`

                GET `/tasks/{taskid}`
        :rtype: dictionary with the jobid of the submitted job
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
        machine,
        target_path,
        job_name=None,
        time=None,
        stage_out_job_id=None,
        account=None,
    ):
        """Remove files in internal CSCS file systems.
        Remove file in target_path.
        Possible to stage-out jobs providing the SLURM Id of a production job.
        More info about internal transfer: https://user.cscs.ch/storage/data_transfer/internal_transfer/

        :param machine: the machine name where the scheduler belongs to
        :type machine: string
        :param target_path: the absolute target path
        :type target_path: string,
        :param job_name: job name
        :type job_name: string, optional
        :param time: limit on the total run time of the job. Acceptable time formats 'minutes', 'minutes:seconds', 'hours:minutes:seconds', 'days-hours', 'days-hours:minutes' and 'days-hours:minutes:seconds'.
        :type time: string, optional
        :param stage_out_job_id: transfer data after job with ID {stage_out_job_id} is completed
        :type stage_out_job_id: string, optional
        :param account: name of the bank account to be used in SLURM. If not set, system default is taken.
        :type account: string, optional
        :calls: POST `/storage/xfer-internal/rm`

                GET `/tasks/{taskid}`
        :rtype: dictionary with the jobid of the submitted job
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

    def external_upload(self, machine, source_path, target_path):
        """Non blocking call for the upload of larger files.

        :param machine: the machine where the filesystem belongs to
        :type machine: string
        :param source_path: the source path in the local filesystem
        :type source_path: string
        :param target_path: the target path in the machine's filesystem
        :type target_path: string
        :returns: an ExternalUpload object
        :rtype: ExternalUpload
        """
        resp = self._post_request(
            endpoint="/storage/xfer-external/upload",
            additional_headers={"X-Machine-Name": machine},
            data={"targetPath": target_path, "sourcePath": source_path},
        )
        json_response = self._json_response([resp], 201)["task_id"]
        return ExternalUpload(self, json_response, [resp])

    def external_download(self, machine, source_path):
        """Non blocking call for the download of larger files.

        :param machine: the machine where the filesystem belongs to
        :type machine: string
        :param source_path: the source path in the local filesystem
        :type source_path: string
        :returns: an ExternalDownload object
        :rtype: ExternalDownload
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
    def all_reservations(self, machine):
        """List all active reservations and their status

        :param machine: the machine name
        :type machine: string
        :calls: GET `/reservations`
        :rtype: list of dictionaries (one for each reservation)
        """
        resp = self._get_request(
            endpoint="/reservations", additional_headers={"X-Machine-Name": machine}
        )
        return self._json_response([resp], 200)["success"]

    def create_reservation(
        self,
        machine,
        reservation,
        account,
        number_of_nodes,
        node_type,
        start_time,
        end_time,
    ):
        """Creates a new reservation with {reservation} name for a given SLURM groupname

        :param machine: the machine name
        :type machine: string
        :param reservation: the reservation name
        :type reservation: string
        :param account: the account in SLURM to which the reservation is made for
        :type account: string
        :param number_of_nodes: number of nodes needed for the reservation
        :type number_of_nodes: string
        :param node_type: type of node
        :type node_type: string
        :param start_time: start time for reservation (YYYY-MM-DDTHH:MM:SS)
        :type start_time: string
        :param end_time: end time for reservation (YYYY-MM-DDTHH:MM:SS)
        :type end_time: string
        :calls: POST `/reservations`
        :rtype: None
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
        machine,
        reservation,
        account,
        number_of_nodes,
        node_type,
        start_time,
        end_time,
    ):
        """Updates an already created reservation named {reservation}

        :param machine: the machine name
        :type machine: string
        :param reservation: the reservation name
        :type reservation: string
        :param account: the account in SLURM to which the reservation is made for
        :type account: string
        :param number_of_nodes: number of nodes needed for the reservation
        :type number_of_nodes: string
        :param node_type: type of node
        :type node_type: string
        :param start_time: start time for reservation (YYYY-MM-DDTHH:MM:SS)
        :type start_time: string
        :param end_time: end time for reservation (YYYY-MM-DDTHH:MM:SS)
        :type end_time: string
        :calls: PUT `/reservations/{reservation}`
        :rtype: None
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

    def delete_reservation(self, machine, reservation):
        """Deletes an already created reservation named {reservation}

        :param machine: the machine name
        :type machine: string
        :param reservation: the reservation name
        :type reservation: string
        :calls: DELETE `/reservations/{reservation}`
        :rtype: None
        """
        resp = self._delete_request(
            endpoint=f"/reservations/{reservation}",
            additional_headers={"X-Machine-Name": machine},
        )
        self._json_response([resp], 204)
