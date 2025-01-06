Tutorial for FirecREST v2
=========================

This tutorial will guide you through the basic functionalities of v2 of the FirecREST API.
Since the API of FirecREST v2 has some important differences the python client, cannot be the same as the one for FirecREST v1.

Your starting point to use pyFirecREST will be the creation of a `FirecREST` object.
This is simply a mini client that, in cooperation with the authorization object, will take care of the necessary requests that need to be made and handle the responses.

If you want to understand how to setup your authorization object have a look at the previous section.
For this tutorial we will assume the simplest kind of authorization class, where the same token will always be used.

.. code-block:: Python

    import firecrest as fc

    class MyAuthorizationClass:
        def __init__(self):
            pass

        def get_access_token(self):
            return <TOKEN>

    # Setup the client with the appropriate URL and the authorization class
    client = fc.v2.Firecrest(firecrest_url=<firecrest_url>, authorization=MyAuthorizationClass())


Simple blocking requests
------------------------

Most of the methods of the FirecREST object require a simple http request to FirecREST.
With the client we just created here are a couple of examples of listing the files of a directory or getting all the available systems of FirecREST.


Getting all the available systems
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A good starting point is to retrieve the list of systems available in FirecREST. This validates your token and helps you choose a target system for future requests.

.. code-block:: Python

    systems = client.systems()
    print(systems)

Systems is going to be a list of systems and their properties, and you will have to choose from one of them.
This is an example of the output:

.. code-block:: json

    [
        {
            "name": "cluster",
            "host": "cluster.alps.cscs.ch",
            "sshPort": 22,
            "sshCertEmbeddedCmd": true,
            "scheduler": {
                "type": "slurm",
                "version": "24.05.4",
                "apiUrl": null,
                "apiVersion": null
            },
            "servicesHealth": [
                {
                    "serviceType": "scheduler",
                    "lastChecked": "2025-01-06T11:09:29.975235Z",
                    "latency": 0.6163430213928223,
                    "healthy": true,
                    "message": null,
                    "nodes": {
                        "available": 280,
                        "total": 600
                    }
                },
                {
                    "serviceType": "ssh",
                    "lastChecked": "2025-01-06T11:09:29.951104Z",
                    "latency": 0.5919253826141357,
                    "healthy": true,
                    "message": null
                },
                {
                    "serviceType": "filesystem",
                    "lastChecked": "2025-01-06T11:09:29.955848Z",
                    "latency": 0.5964689254760742,
                    "healthy": true,
                    "message": null,
                    "path": "/capstor/scratch/cscs"
                },
                {
                    "serviceType": "filesystem",
                    "lastChecked": "2025-01-06T11:09:29.955997Z",
                    "latency": 0.59639573097229,
                    "healthy": true,
                    "message": null,
                    "path": "/users"
                },
                {
                    "serviceType": "filesystem",
                    "lastChecked": "2025-01-06T11:09:29.955792Z",
                    "latency": 0.5958302021026611,
                    "healthy": true,
                    "message": null,
                    "path": "/capstor/store/cscs"
                }
            ],
            "probing": {
                "interval": 300,
                "timeout": 10,
                "maxLatency": null,
                "maxLoad": null
            },
            "fileSystems": [
                {
                    "path": "/capstor/scratch/cscs",
                    "dataType": "scratch",
                    "defaultWorkDir": true
                },
                {
                    "path": "/users",
                    "dataType": "users",
                    "defaultWorkDir": false
                },
                {
                    "path": "/capstor/store/cscs",
                    "dataType": "store",
                    "defaultWorkDir": false
                }
            ],
            "datatransferJobsDirectives": [
                "#SBATCH --nodes=1",
                "#SBATCH --time=0-00:15:00"
            ],
            "timeouts": {
                "sshConnection": 5,
                "sshLogin": 5,
                "sshCommandExecution": 5
            }
        }
    ]

Listing files in a directory
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Let's say you want to list the directory in the filesystem of a machine called "cluster".
You can get a list of the files, with all the usual properties that ls provides (size, type, permissions etc).

.. code-block:: Python

    files = client.list_files("cluster", "/home/test_user")
    print(files)

The output will be something like this:

.. code-block:: json

    [
        {
            "group": "test_user",
            "lastModified": "2020-04-11T14:53:11",
            "linkTarget": "",
            "name": "test_directory",
            "permissions": "rwxrwxr-x",
            "size": "4096",
            "type": "d",
            "user": "test_user"
        },
        {
            "group": "test_user",
            "lastModified": "2020-04-11T14:14:23",
            "linkTarget": "",
            "name": "test_file.txt",
            "permissions": "rw-rw-r--",
            "size": "10",
            "type": "-",
            "user": "test_user"
        }
    ]

Interact with the scheduler
^^^^^^^^^^^^^^^^^^^^^^^^^^^

FirecREST v2 simplifies job submission, monitoring, and cancellation. These operations now require only a single API request.
As a result the pyFirecREST client has been simplified and the user can interact with the scheduler in a more efficient way.

This is how can make a simple job submission, when the batch script is on your local filesystem:

.. code-block:: Python

    job = client.submit("cluster", working_directory="/home/test_user", script_local_path="script.sh")
    print(job)

For a successful submission the output would look like this.

.. code-block:: json

    {
        "jobid": 42,
    }

In FirecREST v2, the user selects the working directory where the job will be submitted from.

.. Transfer of large files
.. -----------------------

.. TODO
