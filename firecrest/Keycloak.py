#
#  Copyright (c) 2019-2020, ETH Zurich. All rights reserved.
#
#  Please, refer to the LICENSE file in the root directory.
#  SPDX-License-Identifier: BSD-3-Clause
#
import requests
from requests.models import Response

import logging
from logging.handlers import TimedRotatingFileHandler
from functools import wraps


class ClientCredentialsAuthorization(object):
    """
    ClientCredentialsAuthorization class.
    Allows login and control the token validation and refresh workflow for an account in keycloak (grant_type=client_credentials)

    :param client_id: name of the client as registered in keycloak server
    :type client_id: string
    :param client_secret: secret delivered by keycloak administrator at registry
    :type client_secret: string
    :param token_uri: URI of the token request in the keycloak server (https://auth.your-server.com/auth/realms/cscs/protocol/openid-connect/token)
    :type token_uri: string
    :param debug: activates (de-activates) output of the logs (default is False)
    :type token_uri: boolean, optional
    """

    def __init__(self, client_id, client_secret, token_uri, debug=False):
        """Constructor of a ClientCredentialsAuthorization object.
        """
        self._client_secret = client_secret
        self._client_id = client_id
        self._token_uri = token_uri
        self._debug = debug
        self._tokens = {"access_token": None}

        if debug:
            logging.basicConfig(level=logging.DEBUG)
        else:
            logging.basicConfig(level=logging.INFO)

    def get_access_token(self):
        """Returns an access token to be used for accessing resources.

        :rtype: string
        """
        return self._tokens["access_token"]

    def is_token_valid(self, access_token):
        """Checks if an access token is still valid

        :param access_token: access token to be validated
        :type access_token: string
        :rtype: boolean
        """
        if self._debug:
            logging.debug("Checks if access token is valid")

        url = f"{self._token_uri}/introspect"
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        data = {
            "client_id": self._client_id,
            "client_secret": self._client_secret,
            "token": access_token,
            "token_type_hint": "access_token",
        }

        try:
            resp = requests.post(url, data=data, headers=headers)
            if self._debug:
                logging.debug(f"Status code: {resp.status_code}")
                logging.debug(f"Response from {url}: {resp.json()}")

            if resp.ok:
                active = resp.json()["active"]

                if active:
                    if self._debug:
                        logging.debug("Token is active")

                    return True

                if self._debug:
                    logging.debug("Token no longer valid")

                return False

            return False
        except Exception as e:
            if self._debug:
                logging.error(f"Error calling keycloak: {type(e)}")
                logging.error(f"{e}")
            return False

    # returns access token from client_id & client_secret
    def get_kc_tokens(self):
        """Returns a new access and refresh token from scratch

        :rtype: dictionary of {"access_token": <access_token>}
        """
        if self._debug:
            logging.debug("Getting new access token")

        # curl -X POST -H "Content-Type: application/x-www-form-urlencoded" \
        # -d 'grant_type=client_credentials&client_id=CLIENT_ID&client_secret=CLIENT_SECRET_KEY' \
        # TOKEN_URI

        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        data = {
            "grant_type": "client_credentials",
            "client_id": self._client_id,
            "client_secret": self._client_secret,
        }

        try:
            resp = requests.post(self._token_uri, headers=headers, data=data)

            if self._debug:
                logging.debug(f"Status code: {resp.status_code}")
                logging.debug(f"Response from {self._token_uri}: {resp.json()}")

            if not resp.ok:
                if self._debug:
                    logging.error("Invalid autentication")
                return None
        except Exception as e:
            if self._debug:
                logging.error(f"Error calling Keycloak: {type(e)}")
                logging.error(f"{e}")
            return None

        access_token = resp.json()["access_token"]
        return {"access_token": access_token}

    # decorator
    # use:
    # @keycloak_utils.account_login
    # def login():
    #   ...
    def account_login(self, func):
        @wraps(func)
        def wrapper_account_login(*args, **kwargs):
            # Checks if there is already an access token and if it's still valid
            if self._tokens["access_token"] != None and self.is_token_valid(
                self._tokens["access_token"]
            ):
                if self._debug:
                    logging.debug("Access token is still valid")

                return func(*args, **kwargs)

            # otherwise, brand new tokens are needed from the scratch
            tokens = self.get_kc_tokens()

            if tokens:
                self._tokens["access_token"] = tokens["access_token"]
                return func(*args, **kwargs)

            # if failed, then can be logged in KC
            logging.error("Can't logging with keycloak server")
            return {"error": "Can't login with keycloak server"}, 401

        return wrapper_account_login
