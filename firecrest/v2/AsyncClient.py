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
import ssl

from typing import Any, List, Optional
from packaging.version import Version, parse

import firecrest.FirecrestException as fe

from firecrest.utilities import (
    parse_retry_after, slurm_state_completed, time_block
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


class AsyncFirecrest:
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

    def __init__(
        self,
        firecrest_url: str,
        authorization: Any,
        verify: str | bool | ssl.SSLContext = True,
    ) -> None:
        self._firecrest_url = firecrest_url
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
        self._session = httpx.AsyncClient(verify=self._verify)

    def set_api_version(self, api_version: str) -> None:
        """Set the version of the api of firecrest. By default it will be
        assumed that you are using version 2.0.0 or compatible. The version is
        parsed by the `packaging` library.
        """
        self._api_version = parse(api_version)

    async def close_session(self) -> None:
        """Close the httpx session"""
        await self._session.aclose()

    async def create_new_session(self) -> None:
        """Create a new httpx session"""
        if not self._session.is_closed:
            await self._session.aclose()

        self._session = httpx.AsyncClient(verify=self._verify)

    @property
    def is_session_closed(self) -> bool:
        """Check if the httpx session is closed"""
        return self._session.is_closed

    def log(self, level: int, msg: Any) -> None:
        """Log a message with the given level on the client logger.
        """
        if not self.disable_client_logging:
            logger.log(level, msg)

    # @_retry_requests  # type: ignore
    async def _get_request(
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
            resp = await self._session.get(
                url=url, headers=headers, params=params, timeout=self.timeout
            )

        return resp

    # @_retry_requests  # type: ignore
    async def _post_request(
        self, endpoint, additional_headers=None, data=None, files=None
    ) -> httpx.Response:
        url = f"{self._firecrest_url}{endpoint}"
        headers = {
            "Authorization": f"Bearer {self._authorization.get_access_token()}"
        }
        if additional_headers:
            headers.update(additional_headers)

        self.log(logging.DEBUG, f"Making POST request to {endpoint}")
        with time_block(f"POST request to {endpoint}", logger):
            resp = await self._session.post(
                url=url, headers=headers, data=data, files=files, timeout=self.timeout
            )

        return resp

    # @_retry_requests  # type: ignore
    async def _put_request(
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
            resp = await self._session.put(
                url=url, headers=headers, data=data, timeout=self.timeout
            )

        return resp

    # @_retry_requests  # type: ignore
    async def _delete_request(
        self, endpoint, additional_headers=None, data=None
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
            resp = await self._session.request(
                method="DELETE",
                url=url,
                headers=headers,
                data=data,
                timeout=self.timeout,
            )

        return resp

    def _json_response(
        self,
        response: httpx.Response,
        expected_status_code: int
    ) -> dict:
        status_code = response.status_code
        # handle_response(response)
        for h in fe.ERROR_HEADERS:
            if h in response.headers:
                self.log(
                    logging.DEBUG,
                    f"Header '{h}' is included in the response"
                )
                raise fe.HeaderException([response])

        if status_code == 401:
            self.log(
                logging.DEBUG,
                "Status of the response is 401"
            )
            raise fe.UnauthorizedException([response])
        elif status_code == 404:
            self.log(
                logging.DEBUG,
                "Status of the response is 404"
            )
            raise fe.NotFound([response])
        elif status_code >= 400:
            self.log(
                logging.DEBUG,
                f"Status of the response is {status_code}"
            )
            raise fe.FirecrestException([response])
        elif status_code != expected_status_code:
            self.log(
                logging.CRITICAL,
                f"Unexpected status of last request {status_code}, it "
                f"should have been {expected_status_code}"
            )
            raise fe.UnexpectedStatusException(
                [response], expected_status_code
            )

        return response.json()

    async def systems(self) -> List[dict]:
        """Returns available systems.

        :calls: GET `/status/systems`
        """
        resp = await self._get_request(endpoint="/status/systems")
        return resp.json()
