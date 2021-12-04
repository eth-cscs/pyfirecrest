#
#  Copyright (c) 2019-2021, ETH Zurich. All rights reserved.
#
#  Please, refer to the LICENSE file in the root directory.
#  SPDX-License-Identifier: BSD-3-Clause
#
import itertools
import json
from typing import List

import jwt
import requests
import shlex
import shutil
import subprocess
import time
import urllib.request

import firecrest.FirecrestException as fe
from firecrest.response_types import Utilities_Ls_Item


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

    def __init__(self, client: 'Firecrest', task_id, previous_responses=[]):
        self._client = client
        self._task_id = task_id
        self._in_progress = True
        self._status = None
        self._data = None
        self._object_storage_data = None
        self._sleep_time = itertools.cycle([1, 5, 10])
        self._responses = previous_responses

    @property
    def client(self) -> 'Firecrest':
        """Returns the client that will be used to get information for the task.
        """
        return self._client

    def _update(self):
        if self._status not in self._final_states:
            task = self._client._tasks(self._task_id, self._responses)
            self._status = task["status"]
            self._data = task["data"]
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
        return self._status

    @property
    def in_progress(self) -> bool:
        """Returns `False` when the transfer has been completed (succesfully or with errors), otherwise `True`.

        :calls: GET `/tasks/{taskid}`
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
            time.sleep(next(self._sleep_time))
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

    def __init__(self, client, task_id, previous_responses=[]):
        super().__init__(client, task_id, previous_responses)
        self._final_states = {"114", "115"}

    def finish_upload(self) -> None:
        """Finish the upload process.
        This call will upload the file to the staging area.
        Check with the method `status` or `in_progress` to see the status of the transfer.
        The transfer from the staging area to the systems's filesystem can take several seconds to start to start.
        """
        c = self.object_storage_data["command"]
        # LOCAL FIX FOR MAC
        # c = c.replace("192.168.220.19", "localhost")
        command = subprocess.run(
            shlex.split(c), stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        if command.returncode != 0:
            raise Exception(
                f"failed to finish upload with error: {command.stderr.decode('utf-8')}"
            )


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

    def __init__(self, client, task_id, previous_responses=[]):
        super().__init__(client, task_id, previous_responses)
        self._final_states = {"117", "118"}

    def invalidate_object_storage_link(self):
        """Invalidate the temporary URL for downloading.

        :calls: POST `/storage/xfer-external/invalidate`
        :rtype: None
        """
        self._client._invalidate(self._task_id)

    def finish_download(self, targetname):
        """Finish the download process.

        :param targetname: the local path to save the file
        :type targetname: string
        :rtype: None
        """
        url = self.object_storage_data
        # LOCAL FIX FOR MAC
        # url = url.replace("192.168.220.19", "localhost")
        with urllib.request.urlopen(url) as response, open(
            targetname, "wb"
        ) as out_file:
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
        self._current_method_requests = []
        self._verify = verify
        self._sa_role = sa_role

    def _json_response(self, responses, expected_status_code):
        # Will examine only the last response
        response = responses[-1]
        status_code = response.status_code
        # handle_response(response)
        for h in fe.ERROR_HEADERS:
            if h in response.headers:
                raise fe.HeaderException(responses)

        if status_code == 401:
            raise fe.UnauthorizedException(responses)
        elif status_code >= 400:
            raise fe.FirecrestException(responses)
        elif status_code != expected_status_code:
            raise fe.UnexpectedStatusException(responses, expected_status_code)

        try:
            ret = response.json()
        except json.decoder.JSONDecodeError:
            ret = None

        return ret

    def _tasks(self, taskid=None, responses=None):
        if responses is None:
            responses = self._current_method_requests

        url = f"{self._firecrest_url}/tasks/"
        if taskid:
            url += taskid

        headers = {f"Authorization": f"Bearer {self._authorization.get_access_token()}"}
        resp = requests.get(url=url, headers=headers, verify=self._verify)
        responses.append(resp)
        taskinfo = self._json_response(responses, 200)
        status = int(taskinfo["task"]["status"])
        if status == 115:
            raise fe.StorageUploadException(responses)

        if status == 118:
            raise fe.StorageDownloadException(responses)

        if status >= 400:
            raise fe.FirecrestException(responses)

        return taskinfo["task"]

    def _invalidate(self, taskid, responses=[]):
        url = f"{self._firecrest_url}/storage/xfer-external/invalidate"
        headers = {
            "Authorization": f"Bearer {self._authorization.get_access_token()}",
            "X-Task-Id": taskid,
        }
        resp = requests.post(url=url, headers=headers, verify=self._verify)
        responses.append(resp)
        return self._json_response(responses, 201)

    def _poll_tasks(self, taskid, final_status, sleep_time):
        resp = self._tasks(taskid)
        while resp["status"] < final_status:
            time.sleep(next(sleep_time))
            resp = self._tasks(taskid)

        return resp["data"]

    # Status
    def all_services(self):
        """Returns a list containing all available micro services with a name, description, and status.

        :calls: GET `/status/services`
        :rtype: list of dictionaries (one for each service)
        """
        url = f"{self._firecrest_url}/status/services"
        headers = {f"Authorization": f"Bearer {self._authorization.get_access_token()}"}
        resp = requests.get(url=url, headers=headers, verify=self._verify)
        return self._json_response([resp], 200)["out"]

    def service(self, servicename):
        """Returns information about a micro service.
        Returns the name, description, and status.

        :param servicename: the service name
        :type servicename: string
        :calls: GET `/status/services/{servicename}`
        :rtype: list of dictionaries (one for each service)
        """
        url = f"{self._firecrest_url}/status/services/{servicename}"
        headers = {f"Authorization": f"Bearer {self._authorization.get_access_token()}"}
        resp = requests.get(url=url, headers=headers, verify=self._verify)
        return self._json_response([resp], 200)

    def all_systems(self):
        """Returns a list containing all available systems and response status.

        :calls: GET `/status/systems`
        :rtype: list of dictionaries (one for each system)
        """
        url = f"{self._firecrest_url}/status/systems"
        headers = {f"Authorization": f"Bearer {self._authorization.get_access_token()}"}
        resp = requests.get(url=url, headers=headers, verify=self._verify)
        return self._json_response([resp], 200)["out"]

    def system(self, systemname):
        """Returns information about a system.
        Returns the name, description, and status.

        :param systemname: the system name
        :type systemname: string
        :calls: GET `/status/systems/{systemname}`
        :rtype: list of dictionaries (one for each system)
        """
        url = f"{self._firecrest_url}/status/systems/{systemname}"
        headers = {f"Authorization": f"Bearer {self._authorization.get_access_token()}"}
        resp = requests.get(url=url, headers=headers, verify=self._verify)
        return self._json_response([resp], 200)["out"]

    def parameters(self):
        """Returns list of parameters that can be configured in environment files.

        :calls: GET `/status/parameters`
        :rtype: list of parameters
        """
        url = f"{self._firecrest_url}/status/parameters"
        headers = {f"Authorization": f"Bearer {self._authorization.get_access_token()}"}
        resp = requests.get(url=url, headers=headers, verify=self._verify)
        return self._json_response([resp], 200)["out"]

    # Utilities
    def list_files(self, machine: str, targetPath: str, showhidden: bool = False) -> List[Utilities_Ls_Item]:
        """Returns a list of files in a directory.

        :param machine: the machine name where the filesystem belongs to
        :param targetPath: the absolute target path
        :type targetPath: string
        :param showhidden: show hidden files
        :type showhidden: boolean, optional
        :calls: GET `/utilities/ls`
        :rtype: list of files
        """
        url = f"{self._firecrest_url}/utilities/ls"
        headers = {
            "Authorization": f"Bearer {self._authorization.get_access_token()}",
            "X-Machine-Name": machine,
        }
        params = {"targetPath": f"{targetPath}"}
        if showhidden is True:
            params["showhidden"] = showhidden

        resp = requests.get(
            url=url, headers=headers, params=params, verify=self._verify
        )
        return self._json_response([resp], 200)["output"]

    def mkdir(self, machine: str, targetPath, p=None):
        """Creates a new directory.

        :param machine: the machine name where the filesystem belongs to
        :param targetPath: the absolute target path
        :type targetPath: string
        :param p: no error if existing, make parent directories as needed
        :type p: boolean, optional
        :calls: POST `/utilities/mkdir`
        :rtype: None
        """
        url = f"{self._firecrest_url}/utilities/mkdir"
        headers = {
            "Authorization": f"Bearer {self._authorization.get_access_token()}",
            "X-Machine-Name": machine,
        }
        data = {"targetPath": targetPath}
        if p:
            data["p"] = p

        resp = requests.post(url=url, headers=headers, data=data, verify=self._verify)
        self._json_response([resp], 201)

    def mv(self, machine: str, sourcePath, targetPath):
        """Rename/move a file, directory, or symlink at the `sourcePath` to the `targetPath` on `machine`'s filesystem.

        :param machine: the machine name where the filesystem belongs to
        :param sourcePath: the absolute source path
        :type sourcePath: string
        :param targetPath: the absolute target path
        :type targetPath: string
        :calls: PUT `/utilities/rename`
        :rtype: None
        """
        url = f"{self._firecrest_url}/utilities/rename"
        headers = {
            "Authorization": f"Bearer {self._authorization.get_access_token()}",
            "X-Machine-Name": machine,
        }
        data = {"targetPath": targetPath, "sourcePath": sourcePath}
        resp = requests.put(url=url, headers=headers, data=data, verify=self._verify)
        self._json_response([resp], 200)

    def chmod(self, machine: str, targetPath, mode):
        """Changes the file mod bits of a given file according to the specified mode.

        :param machine: the machine name where the filesystem belongs to
        :param targetPath: the absolute target path
        :type targetPath: string
        :param mode: same as numeric mode of linux chmod tool
        :type mode: string
        :calls: PUT `/utilities/chmod`
        :rtype: None
        """
        url = f"{self._firecrest_url}/utilities/chmod"
        headers = {
            "Authorization": f"Bearer {self._authorization.get_access_token()}",
            "X-Machine-Name": machine,
        }
        data = {"targetPath": targetPath, "mode": mode}
        resp = requests.put(url=url, headers=headers, data=data, verify=self._verify)
        self._json_response([resp], 200)

    def chown(self, machine: str, targetPath, owner=None, group=None):
        """Changes the user and/or group ownership of a given file.
        If only owner or group information is passed, only that information will be updated.

        :param machine: the machine name where the filesystem belongs to
        :param targetPath: the absolute target path
        :type targetPath: string
        :param owner: owner username for target
        :type owner: string, optional
        :param group: group username for target
        :type group: string, optional
        :calls: PUT `/utilities/chown`
        :rtype: None
        """
        if owner is None and group is None:
            return

        url = f"{self._firecrest_url}/utilities/chown"
        headers = {
            "Authorization": f"Bearer {self._authorization.get_access_token()}",
            "X-Machine-Name": machine,
        }
        data = {"targetPath": targetPath}
        if owner:
            data["owner"] = owner

        if group:
            data["group"] = group

        resp = requests.put(url=url, headers=headers, data=data, verify=self._verify)
        self._json_response([resp], 200)

    def copy(self, machine: str, sourcePath, targetPath):
        """Copies file from `sourcePath` to `targetPath`.

        :param machine: the machine name where the filesystem belongs to
        :param sourcePath: the absolute source path
        :type sourcePath: string
        :param targetPath: the absolute target path
        :type targetPath: string
        :calls: POST `/utilities/copy`
        :rtype: None
        """
        url = f"{self._firecrest_url}/utilities/copy"
        headers = {
            "Authorization": f"Bearer {self._authorization.get_access_token()}",
            "X-Machine-Name": machine,
        }
        data = {"targetPath": targetPath, "sourcePath": sourcePath}
        resp = requests.post(url=url, headers=headers, data=data, verify=self._verify)
        self._json_response([resp], 201)

    def file_type(self, machine: str, targetPath) -> str:
        """Uses the `file` linux application to determine the type of a file.

        :param machine: the machine name where the filesystem belongs to
        :param targetPath: the absolute target path
        :type targetPath: string
        :calls: GET `/utilities/file`
        :rtype: string
        """
        url = f"{self._firecrest_url}/utilities/file"
        headers = {
            "Authorization": f"Bearer {self._authorization.get_access_token()}",
            "X-Machine-Name": machine,
        }
        params = {"targetPath": targetPath}
        resp = requests.get(
            url=url, headers=headers, params=params, verify=self._verify
        )
        return self._json_response([resp], 200)["output"]

    def symlink(self, machine: str, targetPath, linkPath):
        """Creates a symbolic link.

        :param machine: the machine name where the filesystem belongs to
        :param targetPath: the absolute path that the symlink will point to
        :type targetPath: string
        :param symlink: the absolute path to the new symlink
        :type symlink: string
        :calls: POST `/utilities/symlink`
        :rtype: None
        """
        url = f"{self._firecrest_url}/utilities/symlink"
        headers = {
            "Authorization": f"Bearer {self._authorization.get_access_token()}",
            "X-Machine-Name": machine,
        }
        data = {"targetPath": targetPath, "linkPath": linkPath}
        resp = requests.post(url=url, headers=headers, data=data, verify=self._verify)
        self._json_response([resp], 201)

    def simple_download(self, machine: str, sourcePath, targetPath):
        """Blocking call to download a small file.
        The maximun size of file that is allowed can be found from the parameters() call.

        :param machine: the machine name where the filesystem belongs to
        :param sourcePath: the absolute source path
        :type sourcePath: string
        :param targetPath: the absolute target path
        :type targetPath: string
        :calls: GET `/utilities/download`
        :rtype: None
        """

        url = f"{self._firecrest_url}/utilities/download"
        headers = {
            "Authorization": f"Bearer {self._authorization.get_access_token()}",
            "X-Machine-Name": machine,
        }
        params = {"sourcePath": sourcePath}
        resp = requests.get(
            url=url, headers=headers, params=params, verify=self._verify
        )
        self._json_response([resp], 200)
        with open(targetPath, "wb") as f:
            f.write(resp.content)

    def simple_upload(self, machine: str, sourcePath, targetPath):
        """Blocking call to upload a small file.
        The maximum size of file that is allowed can be found from the parameters() call.

        :param machine: the machine name where the filesystem belongs to
        :param sourcePath: the absolute source path
        :type sourcePath: string
        :param targetPath: the absolute target path
        :type targetPath: string
        :calls: POST `/utilities/upload`
        :rtype: None
        """

        url = f"{self._firecrest_url}/utilities/upload"
        headers = {
            "Authorization": f"Bearer {self._authorization.get_access_token()}",
            "X-Machine-Name": machine,
        }
        with open(sourcePath, "rb") as f:
            data = {"targetPath": targetPath}
            files = {"file": f}
            resp = requests.post(
                url=url, headers=headers, data=data, files=files, verify=self._verify
            )

        self._json_response([resp], 201)

    def simple_delete(self, machine: str, targetPath):
        """Blocking call to delete a small file.

        :param machine: the machine name where the filesystem belongs to
        :param targetPath: the absolute target path
        :type targetPath: string
        :calls: DELETE `/utilities/rm`
        :rtype: None
        """

        url = f"{self._firecrest_url}/utilities/rm"
        headers = {
            "Authorization": f"Bearer {self._authorization.get_access_token()}",
            "X-Machine-Name": machine,
        }
        data = {"targetPath": targetPath}
        resp = requests.delete(url=url, headers=headers, data=data, verify=self._verify)
        self._json_response([resp], 204)

    def checksum(self, machine: str, targetPath):
        """Calculate the SHA256 (256-bit) checksum of a specified file.

        :param machine: the machine name where the filesystem belongs to
        :param targetPath: the absolute target path
        :type targetPath: string
        :calls: GET `/utilities/checksum`
        :rtype: string
        """
        url = f"{self._firecrest_url}/utilities/checksum"
        headers = {
            "Authorization": f"Bearer {self._authorization.get_access_token()}",
            "X-Machine-Name": machine,
        }
        params = {"targetPath": targetPath}
        resp = requests.get(
            url=url, headers=headers, params=params, verify=self._verify
        )
        return self._json_response([resp], 200)["output"]

    def view(self, machine: str, targetPath):
        """View the content of a specified file.

        :param machine: the machine name where the filesystem belongs to
        :param targetPath: the absolute target path
        :type targetPath: string
        :calls: GET `/utilities/checksum`
        :rtype: string
        """
        url = f"{self._firecrest_url}/utilities/view"
        headers = {
            "Authorization": f"Bearer {self._authorization.get_access_token()}",
            "X-Machine-Name": machine,
        }
        params = {"targetPath": targetPath}
        resp = requests.get(
            url=url, headers=headers, params=params, verify=self._verify
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
            decoded = jwt.decode(self._authorization.get_access_token(), verify=False)
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
    def _submit_request(self, machine: str, job_script, local_file):
        headers = {
            "Authorization": f"Bearer {self._authorization.get_access_token()}",
            "X-Machine-Name": machine,
        }
        if local_file:
            url = f"{self._firecrest_url}/compute/jobs/upload"
            with open(job_script, "rb") as f:
                files = {"file": f}
                resp = requests.post(
                    url=url, headers=headers, files=files, verify=self._verify
                )
        else:
            url = f"{self._firecrest_url}/compute/jobs/path"
            data = {"targetPath": job_script}
            resp = requests.post(
                url=url, headers=headers, data=data, verify=self._verify
            )

        self._current_method_requests.append(resp)
        return self._json_response(self._current_method_requests, 201)

    def _squeue_request(self, machine: str, jobs=[]):
        url = f"{self._firecrest_url}/compute/jobs"
        headers = {
            "Authorization": f"Bearer {self._authorization.get_access_token()}",
            "X-Machine-Name": machine,
        }
        params = {}
        if jobs:
            params = {"jobs": ",".join([str(j) for j in jobs])}

        resp = requests.get(
            url=url, headers=headers, params=params, verify=self._verify
        )
        self._current_method_requests.append(resp)
        return self._json_response(self._current_method_requests, 200)

    def _acct_request(self, machine: str, jobs=[], starttime=None, endtime=None):
        url = f"{self._firecrest_url}/compute/acct"
        headers = {
            "Authorization": f"Bearer {self._authorization.get_access_token()}",
            "X-Machine-Name": machine,
        }
        params = {}
        if jobs:
            params["jobs"] = ",".join(jobs)

        if starttime:
            params["starttime"] = starttime

        if endtime:
            params["endtime"] = endtime

        resp = requests.get(
            url=url, headers=headers, params=params, verify=self._verify
        )
        self._current_method_requests.append(resp)
        return self._json_response(self._current_method_requests, 200)

    def submit(self, machine: str, job_script, local_file=True):
        """Submits a batch script to SLURM on the target system

        :param machine: the machine name where the scheduler belongs to
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
        return self._poll_tasks(
            json_response["task_id"], "200", itertools.cycle([1, 5, 10])
        )

    def poll(self, machine: str, jobs=[], starttime=None, endtime=None):
        """Retrieves information about submitted jobs.
        This call uses the `sacct` command.

        :param machine: the machine name where the scheduler belongs to
        :param jobs: list of the IDs of the jobs (default [])
        :type jobs: list of strings/integers, optional
        :param starttime: Start time (and/or date) of job's query. Allowed formats are HH:MM[:SS] [AM|PM] MMDD[YY] or MM/DD[/YY] or MM.DD[.YY] MM/DD[/YY]-HH:MM[:SS] YYYY-MM-DD[THH:MM[:SS]]
        :type starttime: string, optional
        :param endtime: End time (and/or date) of job's query. Allowed formats are HH:MM[:SS] [AM|PM] MMDD[YY] or MM/DD[/YY] or MM.DD[.YY] MM/DD[/YY]-HH:MM[:SS] YYYY-MM-DD[THH:MM[:SS]]
        :type endtime: string, optional
        :calls: GET `/compute/acct`

                GET `/tasks/{taskid}`
        :rtype: dictionary
        """
        self._current_method_requests = []
        jobids = [str(j) for j in jobs]
        json_response = self._acct_request(machine, jobids, starttime, endtime)
        return self._poll_tasks(
            json_response["task_id"], "200", itertools.cycle([1, 5, 10])
        )

    def cancel(self, machine: str, jobid):
        """Retrieves information about submitted jobs.
        This call uses the `scancel` command.

        :param machine: the machine name where the scheduler belongs to
        :param jobid: the absolute target path (default [])
        :type jobid: list of strings/integers, optional
        :calls: DELETE `/compute/jobs/{jobid}`

                GET `/tasks/{taskid}`
        :rtype: dictionary
        """
        self._current_method_requests = []
        url = f"{self._firecrest_url}/compute/jobs/{jobid}"
        headers = {
            "Authorization": f"Bearer {self._authorization.get_access_token()}",
            "X-Machine-Name": machine,
        }
        resp = requests.delete(url=url, headers=headers)
        self._current_method_requests.append(resp)
        json_response = self._json_response(self._current_method_requests, 200)
        return self._poll_tasks(
            json_response["task_id"], "200", itertools.cycle([1, 5, 10])
        )

    # Storage
    def _internal_transfer(
        self,
        url,
        machine,
        sourcePath,
        targetPath,
        jobname,
        time,
        stageOutJobId,
        account,
    ):
        headers = {
            "Authorization": f"Bearer {self._authorization.get_access_token()}",
            "X-Machine-Name": machine,
        }
        data = {"targetPath": targetPath}
        if sourcePath:
            data["sourcePath"] = sourcePath

        if jobname:
            data["jobname"] = jobname

        if time:
            data["time"] = time

        if stageOutJobId:
            data["stageOutJobId"] = stageOutJobId

        if account:
            data["account"] = account

        resp = requests.post(url=url, headers=headers, data=data, verify=self._verify)
        self._current_method_requests.append(resp)
        return self._json_response(self._current_method_requests, 201)

    def submit_move_job(
        self,
        machine: str,
        sourcePath,
        targetPath,
        jobname=None,
        time=None,
        stageOutJobId=None,
        account=None,
    ):
        """Move files between internal CSCS file systems.
        Rename/Move sourcePath to targetPath.
        Possible to stage-out jobs providing the SLURM Id of a production job.
        More info about internal transfer: https://user.cscs.ch/storage/data_transfer/internal_transfer/

        :param machine: the machine name where the scheduler belongs to
        :param sourcePath: the absolute source path
        :type sourcePath: string
        :param targetPath: the absolute target path
        :type targetPath: string,
        :param jobname: job name
        :type jobname: string, optional
        :param time: limit on the total run time of the rename. Acceptable time formats 'minutes', 'minutes:seconds', 'hours:minutes:seconds', 'days-hours', 'days-hours:minutes' and 'days-hours:minutes:seconds'. Note: for stage-in queue a slurm xfer job.
        :type time: string, optional
        :param stageOutJobId: transfer data after job with ID {stageOutJobId} is completed
        :type stageOutJobId: string, optional
        :param account: name of the bank account to be used in SLURM. If not set, system default is taken.
        :type account: string, optional
        :calls: POST `/storage/xfer-internal/mv`

                GET `/tasks/{taskid}`
        :rtype: dictionary with the jobid of the submitted job
        """
        self._current_method_requests = []
        url = f"{self._firecrest_url}/storage/xfer-internal/mv"
        json_response = self._internal_transfer(
            url, machine, sourcePath, targetPath, jobname, time, stageOutJobId, account
        )
        return self._poll_tasks(
            json_response["task_id"], "200", itertools.cycle([1, 5, 10])
        )

    def submit_copy_job(
        self,
        machine,
        sourcePath,
        targetPath,
        jobname=None,
        time=None,
        stageOutJobId=None,
        account=None,
    ):
        """Copy files between internal CSCS file systems.
        Copy sourcePath to targetPath.
        Possible to stage-out jobs providing the SLURM Id of a production job.
        More info about internal transfer: https://user.cscs.ch/storage/data_transfer/internal_transfer/

        :param machine: the machine name where the scheduler belongs to
        :param sourcePath: the absolute source path
        :type sourcePath: string
        :param targetPath: the absolute target path
        :type targetPath: string,
        :param jobname: job name
        :type jobname: string, optional
        :param time: limit on the total run time of the rename. Acceptable time formats 'minutes', 'minutes:seconds', 'hours:minutes:seconds', 'days-hours', 'days-hours:minutes' and 'days-hours:minutes:seconds'. Note: for stage-in queue a slurm xfer job.
        :type time: string, optional
        :param stageOutJobId: transfer data after job with ID {stageOutJobId} is completed
        :type stageOutJobId: string, optional
        :param account: name of the bank account to be used in SLURM. If not set, system default is taken.
        :type account: string, optional
        :calls: POST `/storage/xfer-internal/cp`

                GET `/tasks/{taskid}`
        :rtype: dictionary with the jobid of the submitted job
        """
        self._current_method_requests = []
        url = f"{self._firecrest_url}/storage/xfer-internal/cp"
        json_response = self._internal_transfer(
            url, machine, sourcePath, targetPath, jobname, time, stageOutJobId, account
        )
        return self._poll_tasks(
            json_response["task_id"], "200", itertools.cycle([1, 5, 10])
        )

    def submit_rsync_job(
        self,
        machine,
        sourcePath,
        targetPath,
        jobname=None,
        time=None,
        stageOutJobId=None,
        account=None,
    ):
        """Transfer files between internal CSCS file systems.
        Transfer sourcePath to targetPath.
        Possible to stage-out jobs providing the SLURM Id of a production job.
        More info about internal transfer: https://user.cscs.ch/storage/data_transfer/internal_transfer/

        :param machine: the machine name where the scheduler belongs to
        :param sourcePath: the absolute source path
        :type sourcePath: string
        :param targetPath: the absolute target path
        :type targetPath: string,
        :param jobname: job name
        :type jobname: string, optional
        :param time: limit on the total run time of the rename. Acceptable time formats 'minutes', 'minutes:seconds', 'hours:minutes:seconds', 'days-hours', 'days-hours:minutes' and 'days-hours:minutes:seconds'. Note: for stage-in queue a slurm xfer job.
        :type time: string, optional
        :param stageOutJobId: transfer data after job with ID {stageOutJobId} is completed
        :type stageOutJobId: string, optional
        :param account: name of the bank account to be used in SLURM. If not set, system default is taken.
        :type account: string, optional
        :calls: POST `/storage/xfer-internal/rsync`

                GET `/tasks/{taskid}`
        :rtype: dictionary with the jobid of the submitted job
        """
        self._current_method_requests = []
        url = f"{self._firecrest_url}/storage/xfer-internal/rsync"
        json_response = self._internal_transfer(
            url, machine, sourcePath, targetPath, jobname, time, stageOutJobId, account
        )
        return self._poll_tasks(
            json_response["task_id"], "200", itertools.cycle([1, 5, 10])
        )

    def submit_delete_job(
        self,
        machine,
        targetPath,
        jobname=None,
        time=None,
        stageOutJobId=None,
        account=None,
    ):
        """Remove files in internal CSCS file systems.
        Remove file in targetPath.
        Possible to stage-out jobs providing the SLURM Id of a production job.
        More info about internal transfer: https://user.cscs.ch/storage/data_transfer/internal_transfer/

        :param machine: the machine name where the scheduler belongs to
        :param targetPath: the absolute target path
        :type targetPath: string,
        :param jobname: job name
        :type jobname: string, optional
        :param time: limit on the total run time of the rename. Acceptable time formats 'minutes', 'minutes:seconds', 'hours:minutes:seconds', 'days-hours', 'days-hours:minutes' and 'days-hours:minutes:seconds'. Note: for stage-in queue a slurm xfer job.
        :type time: string, optional
        :param stageOutJobId: transfer data after job with ID {stageOutJobId} is completed
        :type stageOutJobId: string, optional
        :param account: name of the bank account to be used in SLURM. If not set, system default is taken.
        :type account: string, optional
        :calls: POST `/storage/xfer-internal/rm`

                GET `/tasks/{taskid}`
        :rtype: dictionary with the jobid of the submitted job
        """
        self._current_method_requests = []
        url = f"{self._firecrest_url}/storage/xfer-internal/rm"
        json_response = self._internal_transfer(
            url, machine, None, targetPath, jobname, time, stageOutJobId, account
        )
        return self._poll_tasks(
            json_response["task_id"], "200", itertools.cycle([1, 5, 10])
        )

    def external_upload(self, machine: str, sourcePath, targetPath):
        """Non blocking call for the upload of larger files.

        :param machine: the machine where the filesystem belongs to
        :param sourcePath: the source path in the local filesystem
        :type sourcePath: string
        :param targetPath: the target path in the machine's filesystem
        :type targetPath: string
        :returns: an ExternalDownload object
        :rtype: ExternalDownload
        """
        self._current_method_requests = []
        url = f"{self._firecrest_url}/storage/xfer-external/upload"
        headers = {
            "Authorization": f"Bearer {self._authorization.get_access_token()}",
            "X-Machine-Name": machine,  # will not be taken into account yet
        }
        data = {"targetPath": targetPath, "sourcePath": sourcePath}
        resp = requests.post(url=url, headers=headers, data=data, verify=self._verify)
        json_response = self._json_response([resp], 201)["task_id"]
        return ExternalUpload(self, json_response, [resp])

    def external_download(self, machine: str, sourcePath):
        """Non blocking call for the download of larger files.

        :param machine: the machine where the filesystem belongs to
        :param sourcePath: the source path in the local filesystem
        :type sourcePath: string
        :param targetPath: the target path in the machine's filesystem
        :type targetPath: string
        :returns: an ExternalDownload object
        :rtype: ExternalDownload
        """
        self._current_method_requests = []
        url = f"{self._firecrest_url}/storage/xfer-external/download"
        headers = {
            "Authorization": f"Bearer {self._authorization.get_access_token()}",
            "X-Machine-Name": machine,  # will not be taken into account yet
        }
        data = {"sourcePath": sourcePath}
        resp = requests.post(url=url, headers=headers, data=data, verify=self._verify)
        return ExternalDownload(
            self, self._json_response([resp], 201)["task_id"], [resp]
        )

    # Reservation
    def all_reservations(self, machine):
        """List all active reservations and their status

        :param machine: the machine name
        :calls: GET `/reservations`
        :rtype: list of dictionaries (one for each reservation)
        """
        url = f"{self._firecrest_url}/reservations"
        headers = {
            "Authorization": f"Bearer {self._authorization.get_access_token()}",
            "X-Machine-Name": machine,
        }
        resp = requests.get(url=url, headers=headers, verify=self._verify)
        return self._json_response([resp], 200)["success"]

    def create_reservation(
        self, machine: str, reservation, account, numberOfNodes, nodeType, starttime, endtime
    ):
        """Creates a new reservation with {reservation} name for a given SLURM groupname

        :param machine: the machine name
        :param reservation: the reservation name
        :type reservation: string
        :param account: the account in SLURM to which the reservation is made for
        :type account: string
        :param numberOfNodes: number of nodes needed for the reservation
        :type numberOfNodes: string
        :param nodeType: type of node
        :type nodeType: string
        :param starttime: start time for reservation (YYYY-MM-DDTHH:MM:SS)
        :type starttime: string
        :param endtime: end time for reservation (YYYY-MM-DDTHH:MM:SS)
        :type endtime: string
        :calls: POST `/reservations`
        :rtype: None
        """
        url = f"{self._firecrest_url}/reservations"
        headers = {
            "Authorization": f"Bearer {self._authorization.get_access_token()}",
            "X-Machine-Name": machine,
        }
        data = {
            "reservation": reservation,
            "account": account,
            "numberOfNodes": numberOfNodes,
            "nodeType": nodeType,
            "starttime": starttime,
            "endtime": endtime,
        }

        resp = requests.post(url=url, headers=headers, data=data, verify=self._verify)
        self._json_response([resp], 201)

    def update_reservation(
        self, machine: str, reservation, account, numberOfNodes, nodeType, starttime, endtime
    ):
        """Updates an already created reservation named {reservation}

        :param machine: the machine name
        :param reservation: the reservation name
        :type reservation: string
        :param account: the account in SLURM to which the reservation is made for
        :type account: string
        :param numberOfNodes: number of nodes needed for the reservation
        :type numberOfNodes: string
        :param nodeType: type of node
        :type nodeType: string
        :param starttime: start time for reservation (YYYY-MM-DDTHH:MM:SS)
        :type starttime: string
        :param endtime: end time for reservation (YYYY-MM-DDTHH:MM:SS)
        :type endtime: string
        :calls: PUT `/reservations/{reservation}`
        :rtype: None
        """
        url = f"{self._firecrest_url}/reservations/{reservation}"
        headers = {
            "Authorization": f"Bearer {self._authorization.get_access_token()}",
            "X-Machine-Name": machine,
        }
        data = {
            "account": account,
            "numberOfNodes": numberOfNodes,
            "nodeType": nodeType,
            "starttime": starttime,
            "endtime": endtime,
        }
        resp = requests.put(url=url, headers=headers, data=data, verify=self._verify)
        self._json_response([resp], 200)

    def delete_reservation(self, machine: str, reservation):
        """Deletes an already created reservation named {reservation}

        :param machine: the machine name
        :param reservation: the reservation name
        :type reservation: string
        :calls: DELETE `/reservations/{reservation}`
        :rtype: None
        """

        url = f"{self._firecrest_url}/reservations/{reservation}"
        headers = {
            "Authorization": f"Bearer {self._authorization.get_access_token()}",
            "X-Machine-Name": machine,
        }
        resp = requests.delete(url=url, headers=headers, verify=self._verify)
        self._json_response([resp], 204)
