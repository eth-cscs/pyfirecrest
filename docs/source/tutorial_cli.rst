How to use the CLI
==================

After version 1.3.0, pyFirecREST comes together with a CLI but for now it can only be used with the ``ClientCredentialsAuth`` authentication class.

.. attention::

    The CLI currently only supports FirecREST v1. Support for v2 is planned for the next release.

You will need to set the environment variables ``FIRECREST_CLIENT_ID``, ``FIRECREST_CLIENT_SECRET`` and ``AUTH_TOKEN_URL`` to set up the Client Credentials client, as well as ``FIRECREST_URL`` with the URL for the FirecREST instance you are using.

After that you can explore the capabilities of the CLI with the `--help` option:

.. code-block:: bash

    firecrest --help
    firecrest ls --help
    firecrest submit --help
    firecrest upload --help
    firecrest download --help
    firecrest submit-template --help

Some basic examples:

.. code-block:: bash

    # Get the available systems
    firecrest systems

    # Set the environment variable to specify the name of the system
    export FIRECREST_SYSTEM=cluster1

    # Get the parameters of different microservices of FirecREST
    firecrest parameters

    # List files of directory
    firecrest ls /home

    # Submit a job
    firecrest submit script.sh

    # Upload a "small" file (you can check the maximum size in `UTILITIES_MAX_FILE_SIZE` from the `parameters` command)
    firecrest upload --type=direct local_file.txt /path/to/cluster/fs

    # Upload a "large" file
    firecrest upload --type=external local_file.txt /path/to/cluster/fs
    # You will have to finish the upload with a second command that will be given in the output
