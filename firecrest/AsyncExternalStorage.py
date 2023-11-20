#
#  Copyright (c) 2019-2023, ETH Zurich. All rights reserved.
#
#  Please, refer to the LICENSE file in the root directory.
#  SPDX-License-Identifier: BSD-3-Clause
#
from __future__ import annotations

import asyncio
from io import BufferedWriter
import itertools
import logging
import pathlib
import requests
import shutil
import sys
from typing import ContextManager, Optional, List, TYPE_CHECKING
import urllib.request
from packaging.version import Version

if TYPE_CHECKING:
    from firecrest.AsyncClient import AsyncFirecrest

from contextlib import nullcontext
from requests.compat import json  # type: ignore

if sys.version_info >= (3, 8):
    from typing import Literal
else:
    from typing_extensions import Literal

logger = logging.getLogger(__name__)


class AsyncExternalStorage:
    """External storage object."""

    _final_states: set[str]

    def __init__(
        self,
        client: AsyncFirecrest,
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
        self._responses = previous_responses

    @property
    def client(self) -> AsyncFirecrest:
        """Returns the client that will be used to get information for the task."""
        return self._client

    @property
    def task_id(self) -> str:
        """Returns the FirecREST task ID that is associated with this transfer."""
        return self._task_id

    async def _update(self) -> None:
        if self._status not in self._final_states:
            task = await self._client._task_safe(self._task_id, self._responses)
            self._status = task["status"]
            self._data = task["data"]
            logger.info(f"Task {self._task_id} has status {self._status}")
            if not self._object_storage_data:
                if self._status == "111":
                    self._object_storage_data = task["data"]["msg"]
                elif self._status == "117":
                    self._object_storage_data = task["data"]

    @property
    async def status(self) -> str:
        """Returns status of the task that is associated with this transfer.

        :calls: GET `/tasks/{taskid}`
        """
        await self._update()
        return self._status  # type: ignore

    @property
    async def in_progress(self) -> bool:
        """Returns `False` when the transfer has been completed (succesfully or with errors), otherwise `True`.

        :calls: GET `/tasks/{taskid}`
        """
        await self._update()
        return self._status not in self._final_states

    @property
    async def data(self) -> Optional[dict]:
        """Returns the task information from the latest response.

        :calls: GET `/tasks/{taskid}`
        """
        await self._update()
        return self._data

    @property
    async def object_storage_data(self):
        """Returns the necessary information for the external transfer.
        The call is blocking and in cases of large file transfers it might take a long time.

        :calls: GET `/tasks/{taskid}`
        :rtype: dictionary or string
        """
        if not self._object_storage_data:
            await self._update()

        while not self._object_storage_data:
            # No need for extra sleeping here, since the async client handles
            # the rate of requests anyway
            await self._update()

        return self._object_storage_data


class AsyncExternalUpload(AsyncExternalStorage):
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
        client: AsyncFirecrest,
        task_id: str,
        previous_responses: Optional[List[requests.Response]] = None,
    ) -> None:
        previous_responses = [] if previous_responses is None else previous_responses
        super().__init__(client, task_id, previous_responses)
        self._final_states = {"114", "115"}
        logger.info(f"Creating ExternalUpload object for task {task_id}")

    async def finish_upload(self) -> None:
        """Finish the upload process.
        This call will upload the file to the staging area.
        Check with the method `status` or `in_progress` to see the status of the transfer.
        The transfer from the staging area to the systems's filesystem can take several seconds to start to start.
        """
        c = (await self.object_storage_data)["command"]  # typer: ignore
        # LOCAL FIX FOR MAC
        # c = c.replace("192.168.220.19", "localhost")
        logger.info(f"Uploading the file to the staging area with the command: {c}")
        proc = await asyncio.create_subprocess_shell(
            c, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )

        _, stderr = await proc.communicate()
        if proc.returncode != 0:
            exc = Exception(
                f"Failed to finish upload with error: {stderr.decode('utf-8')}"
            )
            logger.critical(exc)
            raise exc


class AsyncExternalDownload(AsyncExternalStorage):
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
        client: AsyncFirecrest,
        task_id: str,
        previous_responses: Optional[List[requests.Response]] = None,
    ) -> None:
        previous_responses = [] if previous_responses is None else previous_responses
        super().__init__(client, task_id, previous_responses)
        self._final_states = {"117", "118"}
        logger.info(f"Creating ExternalDownload object for task {task_id}")

    async def invalidate_object_storage_link(self) -> None:
        """Invalidate the temporary URL for downloading.

        :calls: POST `/storage/xfer-external/invalidate`
        """
        await self._client._invalidate(self._task_id)

    @property
    async def object_storage_link(self) -> str:
        """Get the direct download url for the file. The response from the FirecREST api
        changed after version 1.13.0, so make sure to set to older version, if you are
        using an older deployment.

        :calls: GET `/tasks/{taskid}`
        """
        if self._client._api_version > Version("1.13.0"):
            return await self.object_storage_data["url"]
        else:
            return await self.object_storage_data

    async def finish_download(
        self, target_path: str | pathlib.Path | BufferedWriter
    ) -> None:
        """Finish the download process.

        :param target_path: the local path to save the file
        """
        url = await self.object_storage_data
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
