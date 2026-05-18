How to use the CLI
==================

After version 1.3.0, pyFirecREST comes together with a CLI. It supports both FirecREST v1 and v2, and defaults to **v2**.

You always need to set ``FIRECREST_URL`` with the URL for the FirecREST instance you are using.
For authentication, choose one of the two modes below.

Client credentials
------------------

Set ``FIRECREST_CLIENT_ID``, ``FIRECREST_CLIENT_SECRET``, and ``AUTH_TOKEN_URL`` (or pass them as ``--client-id``, ``--client-secret``, and ``--token-url``):

.. code-block:: bash

    export FIRECREST_URL=https://firecrest.example.com
    export FIRECREST_CLIENT_ID=my-client
    export FIRECREST_CLIENT_SECRET=my-secret
    export AUTH_TOKEN_URL=https://auth.example.com/auth/.../openid-connect/token

Token command
-------------

Set ``FIRECREST_TOKEN_COMMAND`` to a shell command whose stdout is the bearer token (or pass ``--token-command``).
The command is re-run on each request, so token refresh is handled automatically:

.. code-block:: bash

    export FIRECREST_URL=https://firecrest.example.com
    export FIRECREST_TOKEN_COMMAND="my-org-cli auth token"

    # Or inline:
    firecrest --token-command "my-org-cli auth token" systems

.. note::

    ``--token-command`` is mutually exclusive with ``--client-id`` / ``--client-secret`` / ``--token-url``.

FirecREST cli examples for v2
-----------------------------

After that you can explore the capabilities of the CLI with the `--help` option:

.. code-block:: bash

    firecrest --help
    firecrest ls --help
    firecrest submit --help
    firecrest upload --help
    firecrest download --help

Some basic examples:

.. code-block:: bash

    # Get the available systems
    firecrest systems

    # Set the environment variable to specify the name of the system
    export FIRECREST_SYSTEM=cluster1

    # Get the user and group information for the current user on the selected system
    firecrest id

    # List files of directory
    firecrest ls /home

    # Submit a job
    firecrest submit --working-dir /home/user script.sh

    # Upload a file to the cluster filesystem
    firecrest upload local_file.txt /path/to/cluster/fs remote_file.txt
