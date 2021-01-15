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


class ClientCredentialsAuthentication(object):
    """
    KeycloakServiceAccount class.
    Allows login and control the token validation and refresh workflow for a service account in keycloak (grant_type=client_credentials)
    ...

    Attributes
    -----------
    client_id : str, mandatory
        name of the client as registered in keycloak server
    client_secret : str, mandatory
        secret delivered by keycloak administrator at register
    token_uri : str, mandatory
        URI of the token request in the keycloak server (https://auth.your-server.com/auth/realms/cscs/protocol/openid-connect/token)
    debug : bool, optional
        activates (de-activates) output of the logs (default is False)
    Methods
    -------
    get_access_token():
        returns an access token
    get_kc_tokens():
        returns a list containing access and refresh token
    get_kc_tokens_from_refresh(refresh_token)
        returns a list containing access and refresh token using the refresh token
    is_token_valud(access_token)
        returns True if access_token is valid. Otherwise, returns False.
    service_account_loggin(func)
        decorator to be used in functions to login in a service account keycloak
    """

    def __init__(self, client_id, client_secret, token_uri, debug=False):
        """
        Constructor of a KeycloakServiceAccount object.

        Parameters
        -----------
        client_id : str, mandatory
            name of the client as registered in keycloak server
        client_secret : str, mandatory
            secret delivered by keycloak administrator at register
        token_uri : str, mandatory
            URI of the token request in the keycloak server (https://auth.your-server.com/auth/realms/cscs/protocol/openid-connect/token)
        debug : bool, optional
            activates (de-activates) output of the logs (default is False)
        Returns
        --------
        None
        """

        self.CLIENT_SECRET = client_secret
        self.CLIENT_ID = client_id

        # URI for keycloak CSCS
        self.TOKEN_URI = token_uri

        # False if not info for debug purposes is needed
        self.debug = debug

        # token objects
        self.TOKENS = {"access_token": None, "refresh_token": None}

        if debug:
            logging.basicConfig(level=logging.DEBUG)
        else:
            logging.basicConfig(level=logging.INFO)

    def get_access_token(self):
        """
        Returns an access token.
        Parameters
        ----------
            None
        Returns
        -------
            access_token (str) : access token to be used for accessing resources
        """
        return self.TOKENS["access_token"]

    # Returns True if keycloak access_token is valid
    def is_token_valid(self, access_token):
        """
        Checks if an access token is still valid
        Parameters
        ----------
            access_token: str, mandatory
                keycloak access token to be validated
        Returns
        -------
            is_valid (bool) : True if it's valid, or False in other case
        """

        if self.debug:
            logging.debug("Checks if access token is valid")
        url = f"{self.TOKEN_URI}/introspect"

        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        data = {
            "client_id": self.CLIENT_ID,
            "client_secret": self.CLIENT_SECRET,
            "token": access_token,
            "token_type_hint": "access_token",
        }

        try:
            resp = requests.post(url, data=data, headers=headers)

            if self.debug:
                logging.debug(f"Response from {url}: {resp.json()}")

            if resp.ok:
                active = resp.json()["active"]

                if active:
                    if self.debug:
                        logging.debug("Token is active")
                    return True
                if self.debug:
                    logging.debug("Token no longer valid")
                return False

            return False
        except Exception as e:
            if self.debug:
                logging.error(f"Error calling keycloak: {type(e)}")
                logging.error(f"{e}")
            return False

    # use refresh_token to get a new access_token
    def get_kc_tokens_from_refresh(self, refresh_token):
        """
        Returns an access token from a refresh token
        Parameters
        ----------
            refresh_token : str, mandatory
                keycloak refresh token to use in order of fetch a new access token
        Returns
        -------
            {"refresh_token":refresh_token, "access_token": access_token} (dict) : refresh and access token in a dictionary format
        """

        # curl -X POST -H "Content-Type: application/x-www-form-urlencoded" -H "cache-control: no-cache" \
        #               -H "accept: application/x-www-form-urlencoded" \
        # -d "grant_type=refresh_token&client_id=CLIENT_ID&client_secret=CLIENT_SECRET_KEY&refresh_token=refresh_token" \
        # "TOKEN_URI"

        if self.debug:
            logging.debug("Getting new access token from refresh token")

        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        data = {
            "grant_type": "refresh_token",
            "client_id": self.CLIENT_ID,
            "client_secret": self.CLIENT_SECRET,
            "refresh_token": refresh_token,
        }

        try:
            resp = requests.post(self.TOKEN_URI, headers=headers, data=data)

            if self.debug:
                logging.debug(f"Response from {self.TOKEN_URI}: {resp.json()}")

            if not resp.ok:
                if self.debug:
                    logging.error(f"Error: {resp.json()['error_description']}")
                return None
        except Exception as e:
            if self.debug:
                logging.error(f"Error calling Keycloak: {type(e)}")
                logging.error(f"{e}")
            return None

        access_token = resp.json()["access_token"]
        refresh_token = resp.json()["refresh_token"]

        return {"refresh_token": refresh_token, "access_token": access_token}

    # returns acces and refresh token from client_id & client_secret
    # this should be the first when loging in the application
    def get_kc_tokens(self):
        """
        Returns a new access and refresh token from the scratch
        Parameters
        ----------
            None
        Returns
        -------
            {"refresh_token":refresh_token, "access_token": access_token} (dict) : refresh and access token in a dictionary format
        """
        if self.debug:
            logging.debug("Getting new access & refresh tokens")

        # curl -X POST -H "Content-Type: application/x-www-form-urlencoded" \
        # -d 'grant_type=client_credentials&client_id=CLIENT_ID&client_secret=CLIENT_SECRET_KEY' \
        # TOKEN_URI

        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        data = {
            "grant_type": "client_credentials",
            "client_id": self.CLIENT_ID,
            "client_secret": self.CLIENT_SECRET,
        }

        try:
            resp = requests.post(self.TOKEN_URI, headers=headers, data=data)

            if self.debug:
                logging.debug(f"Response from {self.TOKEN_URI}: {resp.json()}")
                logging.debug(f"Status code: {resp.status_code}")

            if not resp.ok:
                if self.debug:
                    logging.error("Invalid autentication")
                return None
        except Exception as e:
            if self.debug:
                logging.error(f"Error calling Keycloak: {type(e)}")
                logging.error(f"{e}")
            return None

        access_token = resp.json()["access_token"]
        refresh_token = resp.json()["refresh_token"]

        return {"refresh_token": refresh_token, "access_token": access_token}

    # decorator
    # use:
    # @keycloak_utils.service_account_login
    # def login():
    #   ...
    def service_account_login(self, func):
        @wraps(func)
        def wrapper_service_account_login(*args, **kwargs):
            # checks if access_token is in session object:
            if self.TOKENS["access_token"] != None:
                if self.debug:
                    logging.debug("Session was initiated at least once")

                # if it is, then checks validity
                if self.is_token_valid(self.TOKENS["access_token"]):
                    if self.debug:
                        logging.debug("Access token is still valid")
                    return func(*args, **kwargs)

                # otherwise, will try to get it from refresh token:
                if self.debug:
                    logging.debug("Access token not valid")
                tokens = self.get_kc_tokens_from_refresh(self.TOKENS["refresh_token"])
                if tokens != None:
                    # if response from function is not None, then refresh token is still valid
                    self.TOKENS["access_token"] = tokens["access_token"]
                    self.TOKENS["refresh_token"] = tokens["refresh_token"]
                    return func(*args, **kwargs)

            # otherwise, brand new tokens are needed from the scratch
            tokens = self.get_kc_tokens()

            if tokens != None:
                self.TOKENS["access_token"] = tokens["access_token"]
                self.TOKENS["refresh_token"] = tokens["refresh_token"]
                return func(*args, **kwargs)
            # if failed, then can be logged in KC
            logging.error("Can't logging with keycloak server")

            return {"error": "Can't login with keycloak server"}, 401

        return wrapper_service_account_login
