Authorization
=============

For every request to FirecREST you need to have a valid access token.
This will enable your application to have access to the requested resources.

The pyFirecREST Authorization Object
------------------------------------

You can take care of the access token by yourself any way you want, or even better use a python library to take care of this for you, depending on the grant type.
What pyFirecREST will need in the end is only a python object with the method ``get_access_token()``, that when called will provide a valid access token.

Let's say for example you have somehow obtained a long-lasting access token.
The Authorization class you would need to make and give to Firecrest would look like this:

.. code-block:: Python

    class MyAuthorizationClass:
        def __init__(self):
            pass

        def get_access_token(self):
            return <TOKEN>

If this is your case you can move on to the next session, where you can see how to use this Authorization object, in order to make your requests.

If you want to use the Client Credentials authorization grant, you can use the ``ClientCredentialsAuth`` class from pyFirecREST and setup the authorization object like this:

.. code-block:: Python

    import firecrest as f7t

    keycloak = f7t.ClientCredentialsAuth(
        <client_id>, <client_secret>, <token_uri>
    )

Device Authorization Grant Workflow
-----------------------------------

When your application cannot open a browser or embed a user-agent, you can use the OAuth 2.0 Device Authorization Grant.
In this flow, your app is responsible for:
 1. Requesting a device code & user code from the token endpoint
 2. Instructing the user to visit a verification URL and enter the code
 3. Polling the token endpoint until the user has authorized
 4. Caching and refreshing the access token for future calls

This workflow is not handled by pyFirecREST directly, but you can implement it using the `requests` library or any other HTTP client of your choice.
The `Authorization` object you hand to pyFirecREST needs the `get_access_token()` method, but it would also have orchestrate to workflow behind the scenes.

Here we provide an example implementation using `requests`.
We chose to inform the user about the verification URL and user code by simply printing it, but you should adapt this to the needs of your application.

.. code-block:: python

    import time
    import requests

    class DeviceAuthorizationAuth:
        def __init__(self, client_id, device_uri, token_uri, scope=None):
            self.client_id   = client_id
            self.device_uri  = device_uri
            self.token_uri   = token_uri
            self.scope       = scope or "openid"
            self._token_data = None

        def _start_device_flow(self):
            resp = requests.post(self.device_uri, data={
                "client_id": self.client_id,
                "scope":     self.scope,
            })
            resp.raise_for_status()
            return resp.json()

        def _poll_for_token(self, device_code, interval, expires_in):
            deadline = time.time() + expires_in
            while time.time() < deadline:
                resp = requests.post(self.token_uri, data={
                    "grant_type":  "urn:ietf:params:oauth:grant-type:device_code",
                    "device_code": device_code,
                    "client_id":   self.client_id,
                })
                if resp.status_code == 200:
                    return resp.json()

                time.sleep(interval)
            raise RuntimeError("User did not authorize in time")

        def get_access_token(self):
            # If we already have a (non-expired) token, reuse it
            if self._token_data and time.time() < self._token_data["expires_at"]:
                return self._token_data["access_token"]

            # 1) Start device flow
            flow = self._start_device_flow()
            print(f"Go to {flow['verification_uri']} and enter code {flow['user_code']}")

            # 2) Poll until the user completes authorization
            token = self._poll_for_token(
                device_code=flow["device_code"],
                interval=flow.get("interval", 5),
                expires_in=flow.get("expires_in", 600),
            )

            # 3) Compute absolute expiry & cache
            token["expires_at"] = time.time() + token["expires_in"]
            self._token_data = token
            return token["access_token"]

    # Usage with pyFirecREST
    import firecrest as f7t

    auth = DeviceAuthorizationAuth(
        client_id="YOUR_CLIENT_ID",
        device_uri="https://auth.example.com/device",
        token_uri="https://auth.example.com/token",
        scope="openid profile",
    )
    firecrest = f7t.FirecrestClient(url="https://api.firecrest.example.com", auth=auth)


In a similar way you can reuse other packages to support different grant types, like `Flask-OIDC <https://flask-oidc.readthedocs.io/>`__.
