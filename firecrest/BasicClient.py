#
#  Copyright (c) 2019-2023, ETH Zurich. All rights reserved.
#
#  Please, refer to the LICENSE file in the root directory.
#  SPDX-License-Identifier: BSD-3-Clause
#
from __future__ import annotations

import asyncio
from io import BytesIO
import logging
import pathlib
import sys
from typing import Optional, Sequence, List, Union

import firecrest.FirecrestException as fe
import firecrest.types as t
from firecrest.AsyncClient import AsyncFirecrest

from requests.compat import json  # type: ignore

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


def make_sync(func):
    def wrapper(*args, **kwargs):
        return asyncio.run(func(*args, **kwargs))

    return wrapper


class Firecrest(AsyncFirecrest):
    """
    This is the basic class you instantiate to access the FirecREST API v1.
    Necessary parameters are the firecrest URL and an authorization object.

    :param firecrest_url: FirecREST's URL
    :param authorization: the authorization object. This object is responsible of handling the credentials and the only requirement for it is that it has a method get_access_token() that returns a valid access token.
    :param verify: either a boolean, in which case it controls whether requests will verify the server’s TLS certificate, or a string, in which case it must be a path to a CA bundle to use
    :param sa_role: this corresponds to the `F7T_AUTH_ROLE` configuration parameter of the site. If you don't know how FirecREST is setup it's better to leave the default.
    """

    # @make_sync
    # def _tasks(
    #     self,
    #     task_ids: Optional[List[str]] = None,
    #     responses: Optional[List[requests.Response]] = None,
    # ) -> dict[str, t.Task]:
    #     """Return a dictionary of FirecREST tasks and their last update.
    #     When `task_ids` is an empty list or contains more than one element the
    #     `/tasks` endpoint will be called. Otherwise `/tasks/{taskid}`.
    #     When the `/tasks` is called the method will not give an error for invalid IDs,
    #     but `/tasks/{taskid}` will raise an exception.

    #     :param task_ids: list of task IDs. When empty all tasks are returned.
    #     :param responses: list of responses that are associated with these tasks (only relevant for error)
    #     :calls: GET `/tasks` or `/tasks/{taskid}`
    #     """
    #     return asyncio.run(super()._tasks(task_ids, responses))

    # @make_sync
    # def _invalidate(
    #     self, task_id: str, responses: Optional[List[requests.Response]] = None
    # ):
    #     return super()._invalidate(task_id, responses)

    # Status
    @make_sync
    def all_services(self) -> List[t.Service]:
        """Returns a list containing all available micro services with a name, description, and status.

        :calls: GET `/status/services`
        """
        return super().all_services()

    @make_sync
    def service(self, service_name: str) -> t.Service:
        """Returns information about a micro service.
        Returns the name, description, and status.

        :param service_name: the service name
        :calls: GET `/status/services/{service_name}`
        """
        return super().service(service_name)

    @make_sync
    def all_systems(self) -> List[t.System]:
        """Returns a list containing all available systems and response status.

        :calls: GET `/status/systems`
        """
        return super().all_systems()

    @make_sync
    def system(self, system_name: str) -> t.System:
        """Returns information about a system.
        Returns the name, description, and status.

        :param system_name: the system name
        :calls: GET `/status/systems/{system_name}`
        """
        return super().system(system_name)

    @make_sync
    def parameters(self) -> t.Parameters:
        """Returns configuration parameters of the FirecREST deployment that is associated with the client.

        :calls: GET `/status/parameters`
        """
        return super().parameters()

    # Utilities
    @make_sync
    def list_files(
        self, machine: str, target_path: str, show_hidden: bool = False
    ) -> List[t.LsFile]:
        """Returns a list of files in a directory.

        :param machine: the machine name where the filesystem belongs to
        :param target_path: the absolute target path
        :param show_hidden: show hidden files
        :calls: GET `/utilities/ls`
        """
        return super().list_files(machine, target_path, show_hidden)

    @make_sync
    def mkdir(self, machine: str, target_path: str, p: Optional[bool] = None) -> None:
        """Creates a new directory.

        :param machine: the machine name where the filesystem belongs to
        :param target_path: the absolute target path
        :param p: no error if existing, make parent directories as needed
        :calls: POST `/utilities/mkdir`
        """
        return super().mkdir(machine, target_path, p)

    @make_sync
    def mv(self, machine: str, source_path: str, target_path: str) -> None:
        """Rename/move a file, directory, or symlink at the `source_path` to the `target_path` on `machine`'s filesystem.

        :param machine: the machine name where the filesystem belongs to
        :param source_path: the absolute source path
        :param target_path: the absolute target path
        :calls: PUT `/utilities/rename`
        """
        return super().mv(machine, source_path, target_path)

    @make_sync
    def chmod(self, machine: str, target_path: str, mode: str) -> None:
        """Changes the file mod bits of a given file according to the specified mode.

        :param machine: the machine name where the filesystem belongs to
        :param target_path: the absolute target path
        :param mode: same as numeric mode of linux chmod tool
        :calls: PUT `/utilities/chmod`
        """
        return super().chmod(machine, target_path, mode)

    make_sync

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
        return super().chown(machine, target_path, owner, group)

    @make_sync
    def copy(self, machine: str, source_path: str, target_path: str) -> None:
        """Copies file from `source_path` to `target_path`.

        :param machine: the machine name where the filesystem belongs to
        :param source_path: the absolute source path
        :param target_path: the absolute target path
        :calls: POST `/utilities/copy`
        """
        return super().copy(machine, source_path, target_path)

    @make_sync
    def file_type(self, machine: str, target_path: str) -> str:
        """Uses the `file` linux application to determine the type of a file.

        :param machine: the machine name where the filesystem belongs to
        :param target_path: the absolute target path
        :calls: GET `/utilities/file`
        """
        return super().copy(machine, target_path)

    @make_sync
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
        return super().stat(machine, target_path, dereference)

    @make_sync
    def symlink(self, machine: str, target_path: str, link_path: str) -> None:
        """Creates a symbolic link.

        :param machine: the machine name where the filesystem belongs to
        :param target_path: the absolute path that the symlink will point to
        :param link_path: the absolute path to the new symlink
        :calls: POST `/utilities/symlink`
        """
        return super().symlink(machine, target_path, link_path)

    @make_sync
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
        return super().simple_download(machine, source_path, target_path)

    @make_sync
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
        return super().simple_upload(machine, source_path, target_path, filename)

    @make_sync
    def simple_delete(self, machine: str, target_path: str) -> None:
        """Blocking call to delete a small file.

        :param machine: the machine name where the filesystem belongs to
        :param target_path: the absolute target path
        :calls: DELETE `/utilities/rm`
        """
        return super().simple_delete(machine, target_path)

    @make_sync
    def checksum(self, machine: str, target_path: str) -> str:
        """Calculate the SHA256 (256-bit) checksum of a specified file.

        :param machine: the machine name where the filesystem belongs to
        :param target_path: the absolute target path
        :calls: GET `/utilities/checksum`
        """
        return super().checksum(machine, target_path)

    @make_sync
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
        return super().head(machine, target_path, bytes, lines)

    @make_sync
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
        return super().tail(machine, target_path, bytes, lines)

    @make_sync
    def view(self, machine: str, target_path: str) -> str:
        """View the content of a specified file.
        The final result will be smaller than `UTILITIES_MAX_FILE_SIZE` bytes.
        This variable is available in the parameters command.

        :param machine: the machine name where the filesystem belongs to
        :param target_path: the absolute target path
        :calls: GET `/utilities/checksum`
        """
        return super().view(machine, target_path)

    @make_sync
    def whoami(self) -> Optional[str]:
        """Returns the username that FirecREST will be using to perform the other calls.
        Will return `None` if the token is not valid.
        """
        return super().whoami()

    @make_sync
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
        return super().submit(machine, job_script, local_file, account)

    @make_sync
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
        return super().poll(machine, jobs, start_time, end_time)

    @make_sync
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
        return super().poll_active(machine, jobs)

    @make_sync
    def cancel(self, machine: str, job_id: str | int) -> str:
        """Cancels running job.
        This call uses the `scancel` command.

        :param machine: the machine name where the scheduler belongs to
        :param job_id: the ID of the job
        :calls: DELETE `/compute/jobs/{job_id}`

                GET `/tasks/{taskid}`
        """
        return super().cancel(machine, job_id)

    # Storage
    @make_sync
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
        return super().submit_move_job(
            machine, source_path, target_path, job_name, time, stage_out_job_id, account
        )

    @make_sync
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
        return super().submit_copy_job(
            machine, source_path, target_path, job_name, time, stage_out_job_id, account
        )

    @make_sync
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
        return super().submit_rsync_job(
            machine, source_path, target_path, job_name, time, stage_out_job_id, account
        )

    @make_sync
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
        return super().submit_delete_job(
            machine, target_path, job_name, time, stage_out_job_id, account
        )

    @make_sync
    def external_upload(
        self, machine: str, source_path: str, target_path: str
    ) -> ExternalUpload:
        """Non blocking call for the upload of larger files.

        :param machine: the machine where the filesystem belongs to
        :param source_path: the source path in the local filesystem
        :param target_path: the target path in the machine's filesystem
        :returns: an ExternalUpload object
        """
        return super().external_upload(machine, source_path, target_path)

    @make_sync
    def external_download(self, machine: str, source_path: str) -> ExternalDownload:
        """Non blocking call for the download of larger files.

        :param machine: the machine where the filesystem belongs to
        :param source_path: the source path in the local filesystem
        :returns: an ExternalDownload object
        """
        return super().external_download(machine, source_path)

    # Reservation
    @make_sync
    def all_reservations(self, machine: str) -> List[dict]:
        """List all active reservations and their status

        :param machine: the machine name
        :calls: GET `/reservations`
        """
        return super().all_reservations(machine)

    @make_sync
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
        return super().create_reservation(
            machine,
            reservation,
            account,
            number_of_nodes,
            node_type,
            start_time,
            end_time,
        )

    @make_sync
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
        return super().update_reservation(
            machine,
            reservation,
            account,
            number_of_nodes,
            node_type,
            start_time,
            end_time,
        )

    @make_sync
    def delete_reservation(self, machine: str, reservation: str) -> None:
        """Deletes an already created reservation named {reservation}

        :param machine: the machine name
        :param reservation: the reservation name
        :calls: DELETE `/reservations/{reservation}`
        """
        return super().delete_reservation(machine, reservation)
