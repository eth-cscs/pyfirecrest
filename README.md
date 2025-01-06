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
client = f7t.v1.Firecrest(
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

After version 1.3.0 pyFirecREST comes together with a CLI but for now it can only be used with the `f7t.ClientCredentialsAuth` authentication class.

Assuming you are using the same client, you can start by setting as environment variables:
```bash
export FIRECREST_CLIENT_ID=firecrest-sample
export FIRECREST_CLIENT_SECRET=b391e177-fa50-4987-beaf-e6d33ca93571
export FIRECREST_URL=http://localhost:8000
export AUTH_TOKEN_URL=http://localhost:8080/auth/realms/kcrealm/protocol/openid-connect/token
```

After that you can explore the capabilities of the CLI with the `--help` option:
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
# Get the parameters of different microservices of FirecREST
firecrest parameters

# Get the available systems
firecrest systems

# Set the environment variable to specify the name of the system
export FIRECREST_SYSTEM="cluster"

# List files of directory
firecrest ls /home

# Submit a job
firecrest submit script.sh

# Upload a "small" file (you can check the maximum size in `UTILITIES_MAX_FILE_SIZE` from the `parameters` command)
firecrest upload --type=direct local_file.txt /path/to/cluster/fs

# Upload a "large" file
firecrest upload --type=external local_file.txt /path/to/cluster/fs
# You will have to finish the upload with a second command that will be given in the output
```
