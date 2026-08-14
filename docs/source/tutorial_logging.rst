
Enable logging in your python code
==================================

The simplest way to enable logging in your code would be to add this in the beginning of your file:

.. code-block:: Python

    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s:%(name)s:%(message)s",
    )

pyFirecREST has all of it's messages in `INFO` level. If you want to avoid messages from other packages, you can do the following:

.. code-block:: Python

    import logging

    logging.basicConfig(
        level=logging.WARNING,
        format="%(levelname)s:%(name)s:%(message)s",
    )
    logging.getLogger("firecrest").setLevel(logging.INFO)

Tracing the requests to FirecREST
=================================

Every request that the v2 clients make to FirecREST carries an ``X-Request-ID`` header with a unique ID, which is also logged in `DEBUG` level. You can use it to match a request from your client logs to the FirecREST server logs.

On top of that, all requests carry an ``X-Correlation-ID`` header that groups requests belonging to the same logical operation. By default every method call of the client gets its own auto-generated correlation ID, so, for example, all the requests made by a single call of ``upload()`` will share one ID. External transfer objects returned by ``upload()`` and ``download()`` keep the correlation ID of the call that created them, so the polling of the transfer job can be traced as part of the same operation.

When a request fails, the raised ``FirecrestException`` includes both IDs of the last request in its message. They are also available programmatically as ``e.request_id`` and ``e.correlation_id``, and the exceptions raised by failed external transfers carry the ``e.correlation_id`` of the transfer operation.

If your application already has its own correlation or trace ID, you can pass it to the client with the :func:`firecrest.correlation_id` context manager. All the requests made inside the context will carry your ID:

.. code-block:: Python

    import firecrest

    with firecrest.correlation_id("my-workflow-1234"):
        client.mkdir("cluster", "/home/user/my-workflow")
        client.submit("cluster", script_local_path="script.sh")

When no ID is passed, a random one is generated for the context:

.. code-block:: Python

    with firecrest.correlation_id() as cid:
        print(f"correlation ID of the workflow: {cid}")
        client.mkdir("cluster", "/home/user/my-workflow")
        client.submit("cluster", script_local_path="script.sh")
