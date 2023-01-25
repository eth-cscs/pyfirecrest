# PyFirecREST

This is a simple python wrapper for the [FirecREST API](https://github.com/eth-cscs/firecrest).

### How to install
- Through [PyPI](https://pypi.org/project/pyfirecrest/):

  ```
  python3 -m pip install pyfirecrest
  ```

### How to use it as a python package
The full documentation of pyFirecREST is in [this page](https://pyfirecrest.readthedocs.io) but you can get an idea from the following example.
This is how you can use the testbuild from the demo environment [here](https://github.com/eth-cscs/firecrest/tree/master/deploy/demo).
The configuration corresponds to the account `firecrest-sample`.

```python
import firecrest as f7t

# Configuration parameters for the Authorization Object
client_id = "firecrest-sample"
client_secret = "b391e177-fa50-4987-beaf-e6d33ca93571"
token_uri = "http://localhost:8080/auth/realms/kcrealm/protocol/openid-connect/token"

# Create an authorization object with Client Credentials authorization grant
keycloak = f7t.ClientCredentialsAuth(
    client_id, client_secret, token_uri
)

# Setup the client for the specific account
client = f7t.Firecrest(
    firecrest_url="http://localhost:8000", authorization=keycloak
)

try:
    parameters = client.parameters()
    print(f"Firecrest parameters: {parameters}")
except f7t.FirecrestException as e:
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

### How to use it from the terminal

After version pyFirecREST comes together with a CLI but for now it can only be used with the `f7t.ClientCredentialsAuth` authentication class.

Assuming you are using the same client  can start by setting as environment variables the following, but you:
```bash
export FIRECREST_CLIENT_ID=firecrest-sample
export FIRECREST_CLIENT_SECRET=b391e177-fa50-4987-beaf-e6d33ca93571
export FIRECREST_URL=http://localhost:8000
export AUTH_TOKEN_URL=http://localhost:8080/auth/realms/kcrealm/protocol/openid-connect/token
```

After that you can explore the capabilities of the CLI with the `--help` in :
```bash
firecrest --help
firecrest ls --help
firecrest submit --help
firecrest upload --help
firecrest download --help
firecrest submit-template --help
```

Some basic examples:
```bash
# Get the available systems
firecrest systems

# Get the parameters of different microservices of FirecREST
firecrest parameters

# List files of directory
firecrest ls cluster1 /home

# Submit a job
firecrest submit cluster script.sh

# Upload a "small" file (you can check the maximum size in `UTILITIES_MAX_FILE_SIZE` from the `parameters` command)
firecrest upload --type=direct cluster local_file.txt /path/to/cluster/fs

# Upload a "large" file
firecrest upload --type=direct cluster local_file.txt /path/to/cluster/fs
# You will have to finish the upload with a second command that will be given in the output
```
