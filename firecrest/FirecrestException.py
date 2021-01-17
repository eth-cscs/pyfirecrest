#
#  Copyright (c) 2019-2021, ETH Zurich. All rights reserved.
#
#  Please, refer to the LICENSE file in the root directory.
#  SPDX-License-Identifier: BSD-3-Clause
#
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
        return f'last request status: {self._responses[-1].status_code}'


class UnauthorizedException(FirecrestException):
    """Exception raised with an invalid token
    """

    def __str__(self):
        return f'unauthorized request: {self._responses[-1].status_code} {self._responses[-1].json()}'


class InvalidPathException(FirecrestException):
    """Exception raised with an invalid token
    """

    def __str__(self):
        return f'cannot open (No such file or directory)'


class PermissionDeniedException(FirecrestException):
    """Exception raised with an invalid token
    """

    def __str__(self):
        return f'cannot open (Permission denied)'