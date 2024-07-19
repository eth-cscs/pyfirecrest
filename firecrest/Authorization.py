#
#  Copyright (c) 2019-2023, ETH Zurich. All rights reserved.
#
#  Please, refer to the LICENSE file in the root directory.
#  SPDX-License-Identifier: BSD-3-Clause
#
import logging
import requests
import time

import firecrest.FirecrestException as fe

from datetime import datetime
from requests.compat import json  # type: ignore
from typing import Optional, Tuple, Union

logger = logging.getLogger(__name__)


class ClientCredentialsAuth:
    """
    Client Credentials Authorization class.

    :param client_id: name of the client as registered in the authorization server
    :type client_id: string
    :param client_secret: secret associated to the client
    :type client_secret: string
    :param token_uri: URI of the token request in the authorization server (e.g. https://auth.your-server.com/auth/realms/cscs/protocol/openid-connect/token)
    :type token_uri: string
    :param min_token_validity: reuse OIDC token until {min_token_validity} sec before the expiration time (by default 10). Since the token will be checked by different microservices, setting more time in min_token_validity will ensure that the token doesn't expire in the middle of the request.
    :type min_token_validity: float
    """

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        token_uri: str,
        min_token_validity: int = 10,
    ):
        self._client_id = client_id
        self._client_secret = client_secret
        self._token_uri = token_uri
        self._access_token = None
        self._token_expiration_ts = None
        self._min_token_validity = min_token_validity
        #: It will be passed to all the requests that will be made.
        #: How many seconds to wait for the server to send data before giving up.
        #: After that time a `requests.exceptions.Timeout` error will be raised.
        #:
        #: It can be a float or a tuple. More details here: https://requests.readthedocs.io.
        self.timeout: Optional[
            Union[float, Tuple[float, float], Tuple[float, None]]
        ] = None
        #: Disable all logging from this authorization object.
        self.disable_client_logging: bool = False

    def _log(self, level: int, msg: str) -> None:
        """Log a message with the given level on the client logger.
        """
        if not self.disable_client_logging:
            logger.log(level, msg)

    def get_access_token(self) -> str:
        """Returns an access token to be used for accessing resources.
        If the request fails the token will be None
        """

        # Make sure that the access token has at least {min_token_validity} sec left before
        # it expires, otherwise make a new request
        if (
            self._access_token
            and self._token_expiration_ts
            and time.time() <= (self._token_expiration_ts - self._min_token_validity)
        ):
            self._log(
                logging.INFO,
                f"Reusing token, will renew after {datetime.fromtimestamp(self._token_expiration_ts - self._min_token_validity)}"
            )
            return self._access_token

        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        data = {
            "grant_type": "client_credentials",
            "client_id": self._client_id,
            "client_secret": self._client_secret,
        }
        resp = requests.post(
            self._token_uri, headers=headers, data=data, timeout=self.timeout
        )
        try:
            resp_json = resp.json()
        except json.JSONDecodeError:
            resp_json = ""

        if not resp.ok:
            self._log(
                logging.CRITICAL,
                f"Could not obtain token: {fe.ClientsCredentialsException([resp])}"
            )
            raise fe.ClientsCredentialsException([resp])

        self._access_token = resp_json["access_token"]
        self._token_expiration_ts = time.time() + resp_json["expires_in"]
        assert self._token_expiration_ts is not None
        self._log(
            logging.INFO,
            f"Token expires at {datetime.fromtimestamp(self._token_expiration_ts)}"
        )
        return self._access_token
