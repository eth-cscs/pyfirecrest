#
#  Copyright (c) 2019-2021, ETH Zurich. All rights reserved.
#
#  Please, refer to the LICENSE file in the root directory.
#  SPDX-License-Identifier: BSD-3-Clause
#
import requests
import time


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

    def __init__(self, client_id, client_secret, token_uri, min_token_validity=10):
        self._client_id = client_id
        self._client_secret = client_secret
        self._token_uri = token_uri
        self._access_token = None
        self._token_expiration_ts = None
        self._min_token_validity = min_token_validity

    def get_access_token(self):
        """Returns an access token to be used for accessing resources.
        If the request fails the token will be None

        :rtype: string
        """

        # Make sure that the access token has at least {min_token_validity} sec left before
        # it expires, otherwise make a new request
        if (
            self._access_token
            and self._token_expiration_ts
            and time.time() <= (self._token_expiration_ts - self._min_token_validity)
        ):
            return self._access_token

        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        data = {
            "grant_type": "client_credentials",
            "client_id": self._client_id,
            "client_secret": self._client_secret,
        }
        resp = requests.post(self._token_uri, headers=headers, data=data)
        resp_json = resp.json()
        if not resp.ok:
            raise Exception(
                f"Request to {self._token_uri} failed with "
                f"status code {resp.status_code}: {resp_json}"
            )

        self._access_token = resp_json["access_token"]
        self._token_expiration_ts = time.time() + resp_json["expires_in"]
        return self._access_token
