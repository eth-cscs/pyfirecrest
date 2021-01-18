#
#  Copyright (c) 2019-2021, ETH Zurich. All rights reserved.
#
#  Please, refer to the LICENSE file in the root directory.
#  SPDX-License-Identifier: BSD-3-Clause
#
import json


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


class UnauthorizedException(FirecrestException):
    """Exception raised with an invalid token
    """

    def __str__(self):
        return f"{super().__str__()}: unauthorized request"


class InvalidPathException(FirecrestException):
    """Exception raised with an invalid token
    """

    def __str__(self):
        s = f"{super().__str__()}: "
        if "X-Invalid-Path" in self._responses[-1].headers:
            s += self._responses[-1].headers["X-Invalid-Path"]
        else:
            s += "invalid path"

        return s


class PermissionDeniedException(FirecrestException):
    """Exception raised with an invalid token
    """

    def __str__(self):
        s = f"{super().__str__()}: "
        if "X-Invalid-Path" in self._responses[-1].headers:
            s += self._responses[-1].headers["X-Invalid-Path"]
        else:
            s += "permission denied"

        return s


class UnexpectedStatusException(FirecrestException):
    """Exception raised when a request gets an unexpected status
    """

    def __init__(self, responses, expected_status_code):
        super().__init__(responses)
        self._expected_status_code = expected_status_code

    def __str__(self):
        return f"{super().__str__()}: expected status {self._expected_status_code}"
