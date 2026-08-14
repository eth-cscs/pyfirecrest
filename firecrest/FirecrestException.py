#
#  Copyright (c) 2019-2023, ETH Zurich. All rights reserved.
#
#  Please, refer to the LICENSE file in the root directory.
#  SPDX-License-Identifier: BSD-3-Clause
#
import json

from firecrest.tracing import current_correlation_id


ERROR_HEADERS = {
    "X-A-Directory",
    "X-Error",
    "X-Invalid-Path",
    "X-Machine-Does-Not-Exist",
    "X-Machine-Not-Available",
    "X-Not-A-Directory",
    "X-Not-Found",
    "X-Permission-Denied",
    "X-Timeout",
}


def _tracing_suffix(request_id=None, correlation_id=None):
    """Format the tracing IDs for an exception message. Returns an empty
    string when no ID is available.
    """
    ids = []
    if request_id:
        ids.append(f"X-Request-ID: {request_id}")

    if correlation_id:
        ids.append(f"X-Correlation-ID: {correlation_id}")

    return f" [{', '.join(ids)}]" if ids else ""


class FirecrestException(Exception):
    """Base class for exceptions raised when using PyFirecREST."""

    def __init__(self, responses):
        super().__init__()
        self._responses = responses

    @property
    def responses(self):
        return self._responses

    @property
    def request_id(self):
        """The `X-Request-ID` header of the last request, or `None` when
        the request didn't carry one.
        """
        if not self._responses:
            return None

        return self._responses[-1].request.headers.get("X-Request-ID")

    @property
    def correlation_id(self):
        """The `X-Correlation-ID` header of the last request, or `None`
        when the request didn't carry one.
        """
        if not self._responses:
            return None

        return self._responses[-1].request.headers.get("X-Correlation-ID")

    def __str__(self):
        try:
            last_json_response = self._responses[-1].json()
        except json.decoder.JSONDecodeError:
            last_json_response = None

        return (
            f"last request: {self._responses[-1].status_code} "
            f"{last_json_response}"
            f"{_tracing_suffix(self.request_id, self.correlation_id)}"
        )


class NotFound(FirecrestException):
    """Exception raised by an invalid path"""

    def __str__(self):
        return f"{super().__str__()}: FirecREST endpoint not found"


class UnauthorizedException(FirecrestException):
    """Exception raised by an unauthorized request"""

    def __str__(self):
        return f"{super().__str__()}: unauthorized request"


class ClientsCredentialsException(FirecrestException):
    """Exception raised by the request to the authorization server"""

    def __str__(self):
        return f"{super().__str__()}: Client credentials error"


class HeaderException(FirecrestException):
    """Exception raised by a request with an error header"""

    def __str__(self):
        s = f"{super().__str__()}: "
        for h in ERROR_HEADERS:
            if h in self._responses[-1].headers:
                s += self._responses[-1].headers[h]
                break

        return s


class UnexpectedStatusException(FirecrestException):
    """Exception raised when a request gets an unexpected status"""

    def __init__(self, responses, expected_status_code):
        super().__init__(responses)
        self._expected_status_code = expected_status_code

    def __str__(self):
        return f"{super().__str__()}: expected status {self._expected_status_code}"


class NoJSONException(FirecrestException):
    """Exception raised when JSON in not included in the response"""

    def __str__(self):
        return f"{super().__str__()}: JSON is not included in the response"


class StorageDownloadException(FirecrestException):
    """Exception raised by a failed external download"""


class StorageUploadException(FirecrestException):
    """Exception raised by a failed external upload"""


class PollingIterException(Exception):
    """Exception raised when the polling iterator is exhausted"""

    def __init__(self, task_id):
        self._task_id = task_id

    def __str__(self):
        return (
            f"polling iterator for task {self._task_id} "
            f"is exhausted. Update `polling_sleep_times` of the client "
            f"to increase the number of polling attempts."
        )


class TransferJobFailedException(Exception):
    """Exception raised when the polling iterator is exhausted"""

    def __init__(self, transfer_job_info, file_not_found=False):
        self._transfer_job_info = transfer_job_info
        self._file_not_found = file_not_found
        self._correlation_id = current_correlation_id.get()

    @property
    def correlation_id(self):
        """The `X-Correlation-ID` header of the requests of the transfer
        operation, or `None` when no ID was set when the exception was
        raised.
        """
        return self._correlation_id

    def __str__(self):
        if self._file_not_found:
            return (
                f"Logs for transfer job not found. Maybe the job was "
                f"cancelled. Check the transfer job for more information: "
                f"{self._transfer_job_info['transferJob']}"
                f"{_tracing_suffix(correlation_id=self._correlation_id)}"
            )

        return (
            f"Transfer job failed. Check the log files for more "
            f"information: {self._transfer_job_info['transferJob']}"
            f"{_tracing_suffix(correlation_id=self._correlation_id)}"
        )


class JobTimeoutException(Exception):
    """Exception when the job exceeds the user-defined timeout"""

    def __init__(self, jobid):
        self._jobid = jobid

    def __str__(self):
        return (
            f"Job {self._jobid} has exceeded the user-defined timeout."
        )


class TransferJobTimeoutException(TransferJobFailedException):
    """Exception when the transfer job exceeds the user-defined timeout"""

    def __str__(self):
        return (
            f"Transfer job has exceeded the user-defined timeout. "
            f"Transfer job was cancelled: "
            f"{self._transfer_job_info['transferJob']}."
            f"{_tracing_suffix(correlation_id=self._correlation_id)}"
        )


class MultipartUploadException(Exception):
    """Exception raised when a multipart upload fails"""

    def __init__(self, transfer_job_info, msg=None):
        self._transfer_job_info = transfer_job_info
        self._msg = msg
        self._correlation_id = current_correlation_id.get()

    @property
    def correlation_id(self):
        """The `X-Correlation-ID` header of the requests of the transfer
        operation, or `None` when no ID was set when the exception was
        raised.
        """
        return self._correlation_id

    def __str__(self):
        ret = f"{self._msg}: " if self._msg else ""
        ret += (
            f"Multipart upload failed. Transfer info: "
            f"({self._transfer_job_info})"
            f"{_tracing_suffix(correlation_id=self._correlation_id)}"
        )
        return ret


class NotImplementedOnAPIversion(Exception):
    """Exception raised when a feature is not developed yet for the current API version"""
