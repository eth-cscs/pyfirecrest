#
#  Copyright (c) 2019-2021, ETH Zurich. All rights reserved.
#
#  Please, refer to the LICENSE file in the root directory.
#  SPDX-License-Identifier: BSD-3-Clause
#
import json


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


class FirecrestException(Exception):
    """Base class for exceptions raised when using PyFirecREST.
    """

    def __init__(self, responses):
        super().__init__()
        self._responses = responses

    @property
    def responses(self):
        return self._responses

    def __str__(self):
        try:
            last_json_response = self._responses[-1].json()
        except json.decoder.JSONDecodeError:
            last_json_response = None

        return f"last request: {self._responses[-1].status_code} {last_json_response}"


class NotFound(FirecrestException):
    """Exception raised by an invalid path
    """

    def __str__(self):
        return f"{super().__str__()}: FirecREST endpoint not found"


class UnauthorizedException(FirecrestException):
    """Exception raised by an unauthorized request
    """

    def __str__(self):
        return f"{super().__str__()}: unauthorized request"


class HeaderException(FirecrestException):
    """Exception raised by a request with an error header
    """

    def __str__(self):
        s = f"{super().__str__()}: "
        for h in ERROR_HEADERS:
            if h in self._responses[-1].headers:
                s += self._responses[-1].headers[h]
                break

        return s


class UnexpectedStatusException(FirecrestException):
    """Exception raised when a request gets an unexpected status
    """

    def __init__(self, responses, expected_status_code):
        super().__init__(responses)
        self._expected_status_code = expected_status_code

    def __str__(self):
        return f"{super().__str__()}: expected status {self._expected_status_code}"


class StorageDownloadException(FirecrestException):
    """Exception raised by a failed external download
    """


class StorageUploadException(FirecrestException):
    """Exception raised by a failed external upload
    """
