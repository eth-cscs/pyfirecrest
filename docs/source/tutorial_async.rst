How to use the asynchronous API
===============================

In this tutorial, we will explore the asynchronous API of the pyFirecREST library.
Asynchronous programming is a powerful technique that allows you to write more efficient and responsive code by handling concurrent tasks without blocking the main execution flow.
This capability is particularly valuable when dealing with time-consuming operations such as network requests, I/O operations, or interactions with external services.

In order to take advantage of the asynchronous client you may need to make many changes in your existing code, so the effort is worth it when you develop a code from the start or if you need to make a large number of requests.
You could submit hundreds or thousands of jobs, set a reasonable rate and pyFirecREST will handle it in the background without going over the request rate limit or overflowing the system.

If you are already familiar with the synchronous version of pyFirecREST, you will find it quite straightforward to adapt to the asynchronous paradigm.

We will be going through an example that will use the `asyncio library <https://docs.python.org/3/library/asyncio.html>`__.
First you will need to create an ``AsyncFirecrest`` object, instead of the simple ``Firecrest`` object.

.. code-block:: Python

    client = fc.v1.AsyncFirecrest(
        firecrest_url=<firecrest_url>,
        authorization=MyAuthorizationClass()
    )

As you can see in the reference, the methods of ``AsyncFirecrest`` have the same name as the ones from the simple client, with the same arguments and types, but you will need to use the async/await syntax when you call them.

Here is an example of the calls we saw in the previous section:

.. code-block:: Python

    # Getting all the systems
    systems = await client.all_systems()
    print(systems)

    # Getting the files of a directory
    files = await client.list_files("cluster", "/home/test_user")
    print(files)

    # Submit a job
    job = await client.submit("cluster", script_local_path="script.sh")
    print(job)


The uploads and downloads work as before but you have to keep in mind which methods are coroutines.

.. code-block:: Python

    # Download
    down_obj = await client.external_download("cluster", "/remote/path/to/the/file")
    status = await down_obj.status
    print(status)
    await down_obj.finish_download("my_local_file")

    # Upload
    up_obj = await client.external_upload("cluster", "/path/to/local/file", "/remote/path/to/filesystem")
    await up_obj.finish_upload()
    status = await up_obj.status
    print(status)


Here is a more complete example for how you could use the asynchronous client:


.. code-block:: Python

    import firecrest
    import asyncio
    import logging


    # Setup variables before running the script
    client_id = ""
    client_secret = ""
    token_uri = ""
    firecrest_url = ""

    machine = ""
    local_script_path = ""

    # This is simply setup for logging, you can ignore it
    logger = logging.getLogger("simple_example")
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s - %(message)s", datefmt="%H:%M:%S")
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    async def workflow(client, i):
        logger.info(f"{i}: Starting workflow")
        job = await client.submit(machine, script_local_path=local_script_path)
        logger.info(f"{i}: Submitted job with jobid: {job['jobid']}")
        while True:
            poll_res = await client.poll_active(machine, [job["jobid"]])
            if len(poll_res) < 1:
                logger.info(f"{i}: Job {job['jobid']} is no longer active")
                break

            logger.info(f"{i}: Job {job['jobid']} status: {poll_res[0]['state']}")
            await asyncio.sleep(30)

        output = await client.view(machine, job["job_file_out"])
        logger.info(f"{i}: job output: {output}")


    async def main():
        auth = firecrest.ClientCredentialsAuth(client_id, client_secret, token_uri)
        client = firecrest.AsyncFirecrest(firecrest_url, authorization=auth)

        # Set up the desired polling rate for each microservice. The float number
        # represents the number of seconds between consecutive requests in each
        # microservice.
        client.time_between_calls = {
            "compute": 5,
            "reservations": 5,
            "status": 5,
            "storage": 5,
            "tasks": 5,
            "utilities": 5,
        }

        workflows = [workflow(client, i) for i in range(5)]
        await asyncio.gather(*workflows)


    asyncio.run(main())
