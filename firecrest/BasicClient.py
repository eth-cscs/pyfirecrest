import json
import requests
import itertools
import time

import subprocess
import shlex
import sys

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
    def __init__(self, client, task_id):
        self.client = client
        self._task_id = task_id
        self._in_progress = True
        self._status = None
        self._data = None
        self._object_storage_data = None
        self._sleep_time = itertools.cycle([1, 5, 10])

    def _update(self):
        if self._status not in self._final_states:
            task = self.client._tasks(self._task_id)
            self._status = task["status"]
            self._data = task["data"]
            if not self._object_storage_data:
                if self._status == '111':
                    self._object_storage_data = task["data"]["msg"]
                elif self._status == '117':
                    self._object_storage_data = task["data"]

    @property
    def in_progress(self):
        self._update()
        return self._status not in self._final_states

    @property
    def status(self):
        self._update()
        return self._status

    @property
    def data(self):
        self._update()
        return self._data

class ExternalUpload(ExternalStorage):
    """
    This class handles the external upload from a file.

    Attributes:
        status (int): Tracks the progress of the upload.
                      "110" : "Waiting for Form URL from Object Storage to be retrieved"
                      "111" : "Form URL from Object Storage received"
                      "112" : "Object Storage confirms that upload to Object Storage has finished"
                      "113" : "Download from Object Storage to server has started"
                      "114" : "Download from Object Storage to server has finished"
                      "115" : "Download from Object Storage error"
    """

    def __init__(self, client, task_id):
        super().__init__(client, task_id)
        self._final_states = {'114', '115'}

    @property
    def object_storage_data(self):
        if not self._object_storage_data:
            self._update()

        while not self._object_storage_data:
            time.sleep(next(self._sleep_time))
            self._update()

        return self._object_storage_data

    def finish_upload(self):
        link = self.object_storage_data
        subprocess.run(shlex.split(link["command"]), stdout=subprocess.PIPE)


class ExternalDownload(ExternalStorage):
    # download process states:
    # ST_UPL_BEG = "116" # on download process: start upload from filesystem to Object Storage
    # ST_UPL_END = "117" # on download process: upload from filesystem to Object Storage is finished
    # ST_UPL_ERR = "118" # on download process: upload from filesystem to Object Storage is erroneous

    def __init__(self, client, task_id):
        super().__init__(client, task_id)
        self._final_states = {'117', '118'}

    @property
    def object_storage_data(self):
        if not self._object_storage_data:
            self._update()

        while not self._object_storage_data:
            time.sleep(next(self._sleep_time))
            self._update()

        return self._object_storage_data

    def invalidate_object_storage_link(self):
        self.client._invalidate(self._task_id)

    def finish_download(self, targetname):
        url = self.object_storage_data
        # TODO: doesn't work yet

class Firecrest:
    """Stores all the client information.
    """
    def __init__(self, firecrest_url=None, authentication=None):
        self._firecrest_url = firecrest_url
        self._authentication = authentication

    def _json_response(self, response, expected_status_code):
        status_code = response.status_code
        # handle_response(response)
        if status_code >= 400:
            raise Exception(f'Status code: {str(status_code)} {repr(response.json())}')
        elif status_code != expected_status_code:
            raise Exception(f'status_code ({status_code}) != expected_status_code ({expected_status_code})')

        return response.json()

    def _tasks(self, taskid = None):
        url = f"{self._firecrest_url}/tasks/"
        if taskid:
            url += taskid
        headers = {f"Authorization": f"Bearer {self._authentication.get_access_token()}"}
        resp = requests.get(url=url, headers=headers)

        return self._json_response(resp, 200)["task"]

    def _invalidate(self, taskid):
        url = f"{self._firecrest_url}/storage/xfer-external/invalidate"
        headers = {
            "Authorization": f"Bearer {self._authentication.get_access_token()}",
            "X-Task-Id": taskid
        }
        resp = requests.post(url=url, headers=headers)

        return self._json_response(resp, 201)

    def _poll_tasks(self, taskid, final_status, sleep_time):
        resp = self._tasks(taskid)
        while resp['status'] < final_status:
            time.sleep(next(sleep_time))
            resp = self._tasks(taskid)

        return resp['data']

    # Status
    def all_services(self):
        url = f"{self._firecrest_url}/status/services"
        headers = {f"Authorization": f"Bearer {self._authentication.get_access_token()}"}
        resp = requests.get(url=url, headers=headers)

        return self._json_response(resp, 200)["out"]

    def service(self, servicename):
        url = f"{self._firecrest_url}/status/services/{servicename}"
        headers = {f"Authorization": f"Bearer {self._authentication.get_access_token()}"}
        resp = requests.get(url=url, headers=headers)

        return self._json_response(resp, 200)
        # return self._json_response(resp, 200)["out"]

    def all_systems(self):
        url = f"{self._firecrest_url}/status/systems"
        headers = {f"Authorization": f"Bearer {self._authentication.get_access_token()}"}
        resp = requests.get(url=url, headers=headers)

        return self._json_response(resp, 200)["out"]

    def system(self, systemsname):
        url = f"{self._firecrest_url}/status/systems/{systemsname}"
        headers = {f"Authorization": f"Bearer {self._authentication.get_access_token()}"}
        resp = requests.get(url=url, headers=headers)

        return self._json_response(resp, 200)["out"]

    def parameters(self):
        url = f"{self._firecrest_url}/status/parameters"
        headers = {f"Authorization": f"Bearer {self._authentication.get_access_token()}"}
        resp = requests.get(url=url, headers=headers)

        return self._json_response(resp, 200)["out"]

    # Utilities
    def list_files(self, machine, targetPath, showhidden=None):
        url = f"{self._firecrest_url}/utilities/ls"
        headers = {
            "Authorization": f"Bearer {self._authentication.get_access_token()}",
            "X-Machine-Name": machine,
        }
        params = {
            "targetPath": f"{targetPath}"
        }
        if showhidden:
            params["showhidden"] = showhidden

        resp = requests.get(
            url=url,
            headers=headers,
            params=params,
        )

        return self._json_response(resp, 200)["output"]

    def mkdir(self, machine, targetPath, p=None):
        url = f"{self._firecrest_url}/utilities/mkdir"
        headers = {
            "Authorization": f"Bearer {self._authentication.get_access_token()}",
            "X-Machine-Name": machine,
        }
        data = {"targetPath": targetPath}
        if p:
            data["p"] = p

        resp = requests.post(
            url=url,
            headers=headers,
            data=data,
        )

        self._json_response(resp, 201)

    def mv(self, machine, sourcePath, targetPath):
        url = f"{self._firecrest_url}/utilities/rename"
        headers = {
            "Authorization": f"Bearer {self._authentication.get_access_token()}",
            "X-Machine-Name": machine,
        }
        data = {
            "targetPath": targetPath,
            "sourcePath": sourcePath
        }

        resp = requests.put(
            url=url,
            headers=headers,
            data=data,
        )

        self._json_response(resp, 200)

    def chmod(self, machine, targetPath, mode):
        url = f"{self._firecrest_url}/utilities/chmod"
        headers = {
            "Authorization": f"Bearer {self._authentication.get_access_token()}",
            "X-Machine-Name": machine,
        }
        data = {
            "targetPath": targetPath,
            "mode": mode
        }

        resp = requests.put(
            url=url,
            headers=headers,
            data=data,
        )

        self._json_response(resp, 200)

    def chown(self, machine, targetPath, owner=None, group=None):
        if owner is None and group is None:
            return

        url = f"{self._firecrest_url}/utilities/chown"
        headers = {
            "Authorization": f"Bearer {self._authentication.get_access_token()}",
            "X-Machine-Name": machine,
        }
        data = {
            "targetPath": targetPath
        }
        if owner:
            data["owner"] = owner

        if group:
            data["group"] = group

        resp = requests.put(
            url=url,
            headers=headers,
            data=data,
        )

        self._json_response(resp, 200)

    def copy(self, machine, sourcePath, targetPath):
        url = f"{self._firecrest_url}/utilities/copy"
        headers = {
            "Authorization": f"Bearer {self._authentication.get_access_token()}",
            "X-Machine-Name": machine,
        }
        data = {
            "targetPath": targetPath,
            "sourcePath": sourcePath
        }

        resp = requests.post(
            url=url,
            headers=headers,
            data=data,
        )

        self._json_response(resp, 201)

    def file_type(self, machine, targetPath):
        url = f"{self._firecrest_url}/utilities/file"
        headers = {
            "Authorization": f"Bearer {self._authentication.get_access_token()}",
            "X-Machine-Name": machine,
        }
        params = {
            "targetPath": targetPath
        }

        resp = requests.get(
            url=url,
            headers=headers,
            params=params,
        )

        return self._json_response(resp, 200)["out"]

    def symlink(self, machine, targetPath, linkPath):
        url = f"{self._firecrest_url}/utilities/symlink"
        headers = {
            "Authorization": f"Bearer {self._authentication.get_access_token()}",
            "X-Machine-Name": machine,
        }
        data = {
            "targetPath": targetPath,
            "linkPath": linkPath
        }

        resp = requests.post(
            url=url,
            headers=headers,
            data=data,
        )

        self._json_response(resp, 201)

    def simple_download(self, machine, sourcePath, targetPath):
        """Blocking call to download a small file.

        The size of file that is allowed can be found from the parameters() call.

        sourcePath is in the machine's filesystem
        targetPath is in the local filesystem
        """

        url = f"{self._firecrest_url}/utilities/download"
        headers = {
            "Authorization": f"Bearer {self._authentication.get_access_token()}",
            "X-Machine-Name": machine,
        }
        params = {
            "sourcePath": sourcePath
        }

        resp = requests.get(
            url=url,
            headers=headers,
            params=params,
        )

        if resp.status_code == 200:
            with open(targetPath, "wb") as f:
                f.write(resp.content)
        else:
            raise Exception('Status code: '+str(resp.status_code)+' '+repr(resp.json()))

    def simple_upload(self, machine, sourcePath, targetPath):
        """Blocking call to upload a small file.

        The size of file that is allowed can be found from the parameters() call.

        sourcePath: in the local filesystem
        targetPath: in the machine's filesystem
        """

        url = f"{self._firecrest_url}/utilities/upload"
        headers = {
            "Authorization": f"Bearer {self._authentication.get_access_token()}",
            "X-Machine-Name": machine,
        }

        with open(sourcePath, "rb") as f:
            data = {
                "targetPath": targetPath,
            }
            files = {
                "file": f
            }

            resp = requests.post(
                url=url,
                headers=headers,
                data=data,
                files=files
            )

        self._json_response(resp, 201)

    def simple_delete(self, machine, targetPath):
        """Blocking call to delete a small file.

        The size of file that is allowed can be found from the parameters() call.
        """

        url = f"{self._firecrest_url}/utilities/rm"
        headers = {
            "Authorization": f"Bearer {self._authentication.get_access_token()}",
            "X-Machine-Name": machine,
        }
        data={
            'targetPath': targetPath
        }

        resp = requests.delete(
            url=url,
            headers=headers,
            data=data,
        )

        assert resp.status_code == 204

    def checksum(self, machine, targetPath):
        url = f"{self._firecrest_url}/utilities/checksum"
        headers = {
            "Authorization": f"Bearer {self._authentication.get_access_token()}",
            "X-Machine-Name": machine,
        }
        params = {
            "targetPath": targetPath
        }

        resp = requests.get(
            url=url,
            headers=headers,
            params=params,
        )

        return self._json_response(resp, 200)
        # return self._json_response(resp, 200)["out"]

    def view(self, machine, targetPath):
        url = f"{self._firecrest_url}/utilities/view"
        headers = {
            "Authorization": f"Bearer {self._authentication.get_access_token()}",
            "X-Machine-Name": machine,
        }
        params = {
            "targetPath": targetPath
        }

        resp = requests.get(
            url=url,
            headers=headers,
            params=params,
        )

        return self._json_response(resp, 200)
        # return self._json_response(resp, 200)["out"]

    # Compute
    def _submit_request(self, machine, job_script):
        url = f"{self._firecrest_url}/compute/jobs/upload"
        headers = {
            "Authorization": f"Bearer {self._authentication.get_access_token()}",
            "X-Machine-Name": machine,
        }

        with open(job_script, "rb") as f:
            files = {
                "file": f
            }

            resp = requests.post(
                url=url,
                headers=headers,
                files=files,
            )

        return self._json_response(resp, 201)

    def _squeue_request(self, machine, jobs = []):
        url = f"{self._firecrest_url}/compute/jobs"
        headers = {
            "Authorization": f"Bearer {self._authentication.get_access_token()}",
            "X-Machine-Name": machine,
        }
        params = {}
        if jobs:
            params = {
                "jobs": ",".join([str(j) for j in jobs])
            }

        resp = requests.get(
            url=url,
            headers=headers,
            params=params
        )

        return self._json_response(resp, 200)

    def _acct_request(self, machine, jobs = []):
        url = f"{self._firecrest_url}/compute/acct"
        headers = {
            "Authorization": f"Bearer {self._authentication.get_access_token()}",
            "X-Machine-Name": machine,
        }
        params = {}
        if jobs:
            params = {
                "jobs": ",".join([str(j) for j in jobs])
            }

        resp = requests.get(
            url=url,
            headers=headers,
            params=params
        )

        return self._json_response(resp, 200)

    def submit_job(self, machine, job_script):
        json_response = self._submit_request(machine, job_script)
        return self._poll_tasks(json_response["task_id"], '200', itertools.cycle([1, 5, 10]))

    def poll(self, machine, jobs = []):
        jobids = [str(j) for j in jobs]
        if not jobids:
            return {}

        json_response = self._acct_request(machine, jobids)
        return self._poll_tasks(json_response["task_id"], '200', itertools.cycle([1, 5, 10]))

    def cancel(self, machine, jobid):
        url = f"{self._firecrest_url}/compute/jobs/{jobid}"
        headers = {
            "Authorization": f"Bearer {self._authentication.get_access_token()}",
            "X-Machine-Name": machine,
        }

        resp = requests.delete(
            url=url,
            headers=headers
        )

        json_response = self._json_response(resp, 200)
        return self._poll_tasks(json_response["task_id"], '200', itertools.cycle([1, 5, 10]))

    # Storage
    def external_upload(self, machine, sourcePath, targetPath):
        """Non blocking call for the upload of larger files.

        # TODO: Should briefly explain the non-blocking process

        Parameters
        ----------
        machine: str
            The machine where the filesystem belongs to
        sourcePath : str
            The source path in the local filesystem
        targetPath: str
            The target path in the machine's filesystem

        Returns
        -------
        ExternalDownload
            Returns an ExternalDownload object
        """
        url = f"{self._firecrest_url}/storage/xfer-external/upload"
        headers = {
            "Authorization": f"Bearer {self._authentication.get_access_token()}",
            "X-Machine-Name": machine, # will not be taken into account yet
        }
        data = {
            "targetPath": targetPath,
            "sourcePath": sourcePath
        }

        resp = requests.post(
            url=url,
            headers=headers,
            data=data,
        )
        try:
            json_response = self._json_response(resp, 201)["task_id"]
            return ExternalUpload(self, json_response)
        except:
            # TODO: handle errors
            print('TODOOO')
            return None

    def external_download(self, machine, sourcePath):
        """Non blocking call for the download of larger files.

        # TODO: Should briefly explain the non-blocking process

        Parameters
        ----------
        machine: str
            The machine where the filesystem belongs to
        sourcePath : str
            The source path in the local filesystem
        targetPath: str
            The target path in the machine's filesystem

        Returns
        -------
        ExternalDownload
            Returns an ExternalDownload object
        """
        url = f"{self._firecrest_url}/storage/xfer-external/download"
        headers = {
            "Authorization": f"Bearer {self._authentication.get_access_token()}",
            "X-Machine-Name": machine,
        }
        data = {
            "sourcePath": sourcePath
        }

        resp = requests.post(
            url=url,
            headers=headers,
            data=data,
        )

        return ExternalDownload(self, self._json_response(resp, 201)["task_id"])

    def _internal_transfer(self, url, machine, sourcePath, targetPath, jobname, time, stageOutJobId):
        headers = {
            "Authorization": f"Bearer {self._authentication.get_access_token()}",
            "X-Machine-Name": machine,
        }
        data = {
            "targetPath": targetPath
        }
        if sourcePath:
            data['sourcePath'] = sourcePath

        if jobname:
            data['jobname'] = jobname

        if time:
            data['time'] = time

        if stageOutJobId:
            data['stageOutJobId'] = stageOutJobId

        resp = requests.post(
            url=url,
            headers=headers,
            data=data,
        )

        return self._json_response(resp, 201)

    def submit_move_job(self, machine, sourcePath, targetPath, jobname=None, time=None, stageOutJobId=None):
        url = f"{self._firecrest_url}/storage/xfer-internal/mv"
        json_response = self._internal_transfer(url, machine, sourcePath, targetPath, jobname, time, stageOutJobId)
        return self._poll_tasks(json_response["task_id"], '200', itertools.cycle([1, 5, 10]))

    def submit_copy_job(self, machine, sourcePath, targetPath, jobname=None, time=None, stageOutJobId=None):
        url = f"{self._firecrest_url}/storage/xfer-internal/cp"
        json_response = self._internal_transfer(url, machine, sourcePath, targetPath, jobname, time, stageOutJobId)
        return self._poll_tasks(json_response["task_id"], '200', itertools.cycle([1, 5, 10]))

    def submit_rsync_job(self, machine, sourcePath, targetPath, jobname=None, time=None, stageOutJobId=None):
        url = f"{self._firecrest_url}/storage/xfer-internal/rsync"
        json_response = self._internal_transfer(url, machine, sourcePath, targetPath, jobname, time, stageOutJobId)
        return self._poll_tasks(json_response["task_id"], '200', itertools.cycle([1, 5, 10]))

    def submit_delete_job(self, machine, targetPath, jobname=None, time=None, stageOutJobId=None):
        url = f"{self._firecrest_url}/storage/xfer-internal/rm"
        json_response = self._internal_transfer(url, machine, None, targetPath, jobname, time, stageOutJobId)
        return self._poll_tasks(json_response["task_id"], '200', itertools.cycle([1, 5, 10]))
