# PyFirecREST

This is a simple python wrapper for the [FirecREST API](https://github.com/eth-cscs/firecrest).

### How to install
- Through [PyPI](https://test.pypi.org/project/pyfirecrest/):

  ```
  pip install -i https://test.pypi.org/simple/ pyfirecrest
  ```

### How to use
To run a simple example you can use the testbuild from the demo environment [here](https://github.com/eth-cscs/firecrest/tree/master/deploy/demo).
The configuration corresponds to the service account `firecrest-sample`.

```python
import firecrest as f7c

# Configuration parameters for the Authorization Object
client_id = "firecrest-sample"
client_secret = "b391e177-fa50-4987-beaf-e6d33ca93571"
token_uri = "http://localhost:8080/auth/realms/kcrealm/protocol/openid-connect/token"

# Create a keycloak service account object
keycloak = f7c.ClientCredentialsAuthorization(
    client_id, client_secret, token_uri, debug=False
)


class MyKeycloakServiceAccount:
    def __init__(self):
        pass

    @keycloak.service_account_login
    def get_access_token(self):
        return keycloak.get_access_token()


# Setup the client for the specific service account
client = fc.Firecrest(
    firecrest_url="http://localhost:8000", authorization=MyKeycloakServiceAccount()
)

try:
    parameters = client.parameters()
    print(f"Firecrest parameters: {parameters}")
except fc.FirecrestException as e:
    # When the error comes from the responses to a firecrest request you will get a
    # `FirecrestException` and from this you can examine the http responses yourself
    # through the `responses` property
    print(e)
    print(e.responses)
except Exception as e:
    # You might also get regular exceptions in some cases. For example when you are
    # trying to upload a file that doesn't exist in your local filesystem.
    pass
```