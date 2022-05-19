Authorization
=============

For every request to FirecREST you need to have a valid access token.
This will enable your application to have access to the requested resources.


.. Supported grant types
.. ---------------------

.. Implicit
.. ^^^^^^^^

.. Client Credentials
.. ^^^^^^^^^^^^^^^^^^

The pyFirecREST Authorization Object
------------------------------------

You can take care of the access token by yourself any way you want, or even better use a python library to take care of this for you, depending on the grant type.
What pyFirecREST will need in the end is only a python object with the method ``get_access_token()``, that when called will provide a valid access token.

Let's say for example you have somehow obtained a long-lasting access token.
The Authorization class you would need to make and give to Firecrest would look like this:

.. code-block:: Python

    class MyAuthorizationClass:
        def __init__(self):
            pass

        def get_access_token(self):
            return <TOKEN>

If this is your case you can move on to the next session, where you can see how to use this Authorization object, in order to make your requests.

If you want to use the Client Credentials authorization grant, you can use the ``ClientCredentialsAuth`` class from pyFirecREST and setup the authorization object like this:

.. code-block:: Python

    import firecrest as f7t

    keycloak = f7t.ClientCredentialsAuth(
        <client_id>, <client_secret>, <token_uri>
    )

In a similar way you can reuse other packages to support different grant types, like `Flask-OIDC <https://flask-oidc.readthedocs.io/>`__.
