Tutorial
========

Your starting point to use pyFirecREST will be the creation of a FirecREST object.
This is simply a mini client that, in cooperation with the authorization object, will take care of the necessary requests that need to be made and handle the responses.

If you want to understand how to setup your authorization object have a look at the previous section.
For this tutorial we will assume the simplest kind of authorization class, where the same token will always be used.

.. code-block:: Python

    import firecrest as f7t

    class MyAuthorizationClass:
        def __init__(self):
            pass

        def get_access_token(self):
            return <TOKEN>

    # Setup the client with the appropriate URL and the authorization class
    client = f7t.Firecrest(firecrest_url=<firecrest_url>, authorization=MyAuthorizationClass())


Simple blocking requests
------------------------

Most of the methods of the FirecREST object require a simple http request to FirecREST.
With the client we just created here are a couple of examples of listing the files of a directory or getting all the available systems of FirecREST.


Getting all the available systems
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A good start, to make sure your token is valid, is to get the names of all the available systems, where FirecREST can give you access.
This will definitely be useful in the future.

.. code-block:: Python

    systems = client.all_systems()
    print(systems)

Systems is going to be a list of systems and their properties, and you will have to choose from one of them.
This is an example of the output:

.. code-block:: json

    [
        {
            "description": "System ready",
            "status": "available",
            "system": "cluster"
        },
        {
            "description": "System ready",
            "status": "available",
            "system": "cluster2"
        }
    ]

Listing files in a directory
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Let's say you want to list the directory in the filesystem of a machine called "cluster".
You can get a list of the files, with all the usual properties that ls provides (size, type, permissions etc).

.. code-block:: Python

    files = client.list_files("cluster", "/home/myuser")
    print(files)

The output will be something like this:

.. code-block:: json

    [
        {
            "group": "test_user",
            "last_modified": "2020-04-11T14:53:11",
            "link_target": "",
            "name": "test_directory",
            "permissions": "rwxrwxr-x",
            "size": "4096",
            "type": "d",
            "user": "test_user"
        },
        {
            "group": "test_user",
            "last_modified": "2020-04-11T14:14:23",
            "link_target": "",
            "name": "test_file.txt",
            "permissions": "rw-rw-r--",
            "size": "10",
            "type": "-",
            "user": "test_user"
        }
    ]

Methods that will make more than one requests
---------------------------------------------

Some methods of this client will be blocking, but will require at least two requests to FirecREST to return the results.
One example of this is job submission, which you would call simply as follows:

.. code-block:: Python

    job = client.submit("cluster", "script.sh")
    print(job)

For a successful submission the output would look like this.

.. code-block:: json

    {
        "jobid": 42,
        "result": "Job submitted"
    }

All requests that involve the scheduler will create a FirecREST task and be part of an internal queue.
In order to get the results from the scheduler, more requests have to be made.
This method hides the multiple requests and will be blocking, but you can find more information about the job submission `here <https://firecrest.readthedocs.io/en/latest/tutorial.html#upload-a-small-file-with-the-blocking-call>`__.

Transfer of large files
-----------------------

For larger files the user cannot directly upload/download a file to/from FirecREST.
A staging area will be used and the process will require multiple requests from the user.

External Download
^^^^^^^^^^^^^^^^^

For example in the external download process, the requested file will first have to be moved to the staging area.
**This could take a long time in case of a large file.**
When this process finishes, FirecREST will have created a dedicated space for this file and the user can download the file locally as many times as he wants.
You can follow this process with the status codes of the task:

+--------+--------------------------------------------------------------------+
| Status | Description                                                        |
+========+====================================================================+
| 116    | Started upload from filesystem to Object Storage                   |
+--------+--------------------------------------------------------------------+
| 117    | Upload from filesystem to Object Storage has finished successfully |
+--------+--------------------------------------------------------------------+
| 118    | Upload from filesystem to Object Storage has finished with errors  |
+--------+--------------------------------------------------------------------+

In code it would look like this:

.. code-block:: Python

    # This call will only start the transfer of the file to the staging area
    down_obj = client.external_download("cluster", "/remote/path/to/the/file")

    # You can follow the progress of the transfer through the status property
    print(down_obj.status)

    # As soon as down_obj.status is 117 we can proceed with the download to a local file
    down_obj.finish_download("my_local_file")

You can download the file as many times as you want from the staging area.
In case you want to get directly the link in the staging area you can call ``object_storage_data`` and finish the download in your prefered way.

The methods ``finish_download`` and ``object_storage_data`` are blocking, and they will keep making requests to FirecREST until the status of the task is ``117`` or ``118``.
You could also use the ``status`` property of the object to poll with your prefered rate for task progress, before calling them.

Finally, when you finish your download it would be more safe to invalidate the link to the staging area, with the ``invalidate_object_storage_link`` method.

External Upload
^^^^^^^^^^^^^^^

The case of external upload is very similar.
To upload a file you would have to ask for the link in the staging area and upload the file there.
**Even after uploading the file there, it will take some time for the file to appear in the filesystem.**
You can alway follow the status of the task with the ``status`` method and when the file has been successfully uploaded the status of the task will be 114.

+--------+--------------------------------------------------------------------+
| Status | Description                                                        |
+========+====================================================================+
| 110    | Waiting for Form URL from Object Storage to be retrieved           |
+--------+--------------------------------------------------------------------+
| 111    | Form URL from Object Storage received                              |
+--------+--------------------------------------------------------------------+
| 112    | Object Storage confirms that upload to Object Storage has finished |
+--------+--------------------------------------------------------------------+
| 113    | Download from Object Storage to server has started                 |
+--------+--------------------------------------------------------------------+
| 114    | Download from Object Storage to server has finished                |
+--------+--------------------------------------------------------------------+
| 115    | Download from Object Storage error                                 |
+--------+--------------------------------------------------------------------+

The simplest way to do the uploading through pyFirecREST is as follows:

.. code-block:: Python

    # This call will only create the link to Object Storage
    up_obj = client.external_upload("cluster", "/path/to/local/file", "/remote/path/to/filesystem")

    # As soon as down_obj.status is 111 we can proceed with the upload of local file to the staging area
    down_obj.finish_upload()

    # You can follow the progress of the transfer through the status property
    print(up_obj.status)

But, as before, you can get the necessary components for the upload from the ``object_storage_data`` property.
You can get the link, as well as all the necessary arguments for the request to Object Storage and the full command you could perform manually from the terminal.

Handling of errors
------------------

The methods of the Firecrest, ExternalUpload and ExternalDownload objects can raise exceptions in case something goes wrong.
When the error comes from the response of some request pyFirecREST will raise ``FirecrestException``.
In these cases you can manually examine all the responses from the requests in order to get more information, when the message is not informative enough.
These responses are from the requests package of python and you can get all types of useful information from it, like the status code, the json response, the headers and more.
Here is an example of the code that will handle those failures.

.. code-block:: Python

    try:
        parameters = client.parameters()
        print(f"Firecrest parameters: {parameters}")
    except fc.FirecrestException as e:
        # You can just print the exception to get more information about the type of error,
        # for example an invalid or expired token.
        print(e)
        # Or you can manually examine the responses.
        print(e.responses[-1])
    except Exception as e:
        # You might also get regular exceptions in some cases. For example when you are
        # trying to upload a file that doesn't exist in your local filesystem.
        pass
