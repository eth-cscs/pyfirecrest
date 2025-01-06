How to catch and debug errors
=============================

The methods of the ``Firecrest``, ``ExternalUpload`` and ``ExternalDownload`` objects will raise exceptions in case something goes wrong.
The same happens for their asynchronous counterparts.
When the error comes from the response of some request pyFirecREST will raise ``FirecrestException``.
In these cases you can manually examine all the responses from the requests in order to get more information, when the message is not informative enough.
These responses are from the requests package of python and you can get all types of useful information from it, like the status code, the json response, the headers and more.
Here is an example of the code that will handle those failures.

.. code-block:: Python

    import firecrest as fc


    try:
        files = client.list_files("cluster", "/home/test_user")
        print(f"List of files: {files}")
    except fc.FirecrestException as e:
        # You can just print the exception to get more information about the type of error,
        # for example an invalid or expired token.
        print(e)
        # Or you can manually examine the responses.
        print(e.responses[-1])
        print(e.responses[-1].status_code)
        print(e.responses[-1].body)
    except Exception as e:
        # You might also get regular exceptions in some cases. For example when you are
        # trying to upload a file that doesn't exist in your local filesystem.
        print(f"A different exception was encountered: {e}")
