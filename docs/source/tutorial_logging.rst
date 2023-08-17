
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
