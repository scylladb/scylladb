=========
Web Shell
=========

.. warning:: The Web Shell is still experimental, it may change without notice and there may be bugs.

Administrative API designed with interactive usage and convenience in mind. Not designed for high performance or data-heavy workloads, for these, use the :doc:`CQL </cql/index>` or :doc:`Alternator </alternator/alternator/>` protocols instead.
Allows for executing CQL commands and queries against a ScyllaDB cluster, with any HTTP(S) client, like `curl <https://curl.se>`_ or the python `requests library <https://requests.readthedocs.io/en/latest/>`_.
A simple web interface is also provided for convenience, with a similar look and feel to that of :doc:`CQLSH </cql/cqlsh>`.

Local Access
^^^^^^^^^^^^

By default the Web Shell is available on the same IP address as the :doc:`REST API </operating-scylla/rest>`, which is localhost, and using ``HTTP``.
This is for local access only, meant for development, or another avenue administrator access from the local node.
Do not enable ``HTTP`` on a public IP address in a production environment! This will result in credentials and possibly sensitive user-data being transmitted on the network in unencrypted form.

Remote Access
^^^^^^^^^^^^^

Using `HTTPs` is strongly recommended for any remote access.
To use Web Shell via `HTTPs`, enable the `HTTPs` listener on a public IP address, using the ``webshell_https_address``, ``webshell_https_port``, and ``webshell_https_encryption_options`` configuration options.

Example: enable Web Shell remote access on public IP ``172.17.0.1`` and port ``10002`` with ``HTTPs`` via self-signed certificate:

.. code-block:: yaml

    webshell_https_address: 172.17.0.1
    webshell_https_port: 10002
    webshell_https_encryption_options:
        certificate: /path/to/mycert.crt
        keyfile: /path/to/mycert.key

To generate a self signed certificate for testing purposes, you can use the :doc:`generate self-signed certificate </operating-scylla/security/generate-certificate>` guide.
For production systems, it is recommended to use a certificate signed by a trusted Certificate Authority.

.. _webshell-mutual-tls:

HTTPs with mTLS
~~~~~~~~~~~~~~~

In addition to encrypting the connection, the ``HTTPs`` server can ask clients to present a TLS client certificate (mutual TLS, or mTLS).
This is controlled by the ``webshell_https_client_auth`` configuration option, which can be changed at runtime. It accepts one of the following values:

* ``none`` (default) - client certificates are not requested.
* ``request`` - a client certificate is requested but not required. Clients that present a valid certificate can be authenticated by it (see below); clients that do not can still log in with a username and password. Use this mode to support both the web interface (username/password) and scripts or ``curl`` (certificate) on the same port.
* ``require`` - a client certificate is required. The TLS handshake is rejected for clients that do not present a valid certificate.

For the server to validate client certificates, set the ``truststore`` option in ``webshell_https_encryption_options`` to the PEM-encoded certificate of the Certificate Authority that signed the client certificates.

The client certificate can also be used for `authentication <webshell-auth_>_`.

Example: enable mutual TLS in ``request`` mode, validating client certificates against a CA:

.. code-block:: yaml

    webshell_https_address: 172.17.0.1
    webshell_https_port: 10002
    webshell_https_client_auth: request
    webshell_https_encryption_options:
        certificate: /path/to/server.crt
        keyfile: /path/to/server.key
        truststore: /path/to/ca.crt

.. _webshell-auth:

Authentication
^^^^^^^^^^^^^^

Using the Web Shell requires authenticating first, as dictated by the :doc:`authentication configuration </operating-scylla/security/authentication/>`.

Authentication happens via the `login endpoint <webshell-login-endpoint_>`_). A successful login returns a session cookie which has to be included with all requests.

The Web Shell support all valid ScyllaDB authentication methods. The recommended configuration is the :doc:`CertificateOrPasswordAuthenticator </operating-scylla/security/certificate-or-password-authentication/>`.
This allows using `mTLS <webshell-mutual-tls_>`_ for scripts and ``curl``, with fall-back to password-based authentication for browsers (which cannot use mTLS).

Endpoints
^^^^^^^^^

The Web Shell backend is implemented as a handful of ``HTTP`` endpoints.
Design goals:

* Simple text protocol: requests and responses are JSON.
* Stateful sessions with defined lifetime, identified by the session id cookie.
* Sessions are authenticated and all queries pass authorization checks.
* Distinct error codes for different error types.
* Secure by default: ``HTTP`` server listens only on localhost ``HTTPs`` server available for remote access.

Non goals:

* High performance.
* High concurrency.
* Low latency.
* Low resource overhead.
* Rich feature set.
* Efficiency.

Requests sent to the endpoints are subject to concurrency control:

* Maximum of 16 concurrent requests processed, further requests are queued to wait their turn.
* Maximum of 16 waiting requests, further requests are rejected with ``503 Service Unavaliable`` status code.
* Maximum of 1 request per session being processed, further requests for the same session are queued to wait their turn. Due to the above, a maximum of 16 waiters are possible in total accross all sessions.
* Maximum of 32 sessions, further login attemps are rejected with ``503 Service Unavaliable`` status code.

All endpoints use JSON for request and response bodies, unless otherwise noted. Some endpoints do not require a request body.
All endpoints that use JSON, use the following response schema:

.. code-block:: json

    {
      "response": "<respose-data>"
    }

The response data is a string for most endpoints, but it can be any JSON value: both ``/option`` endpoints respond with an object, and so does ``POST /query`` when the output format is ``JSON``.
Error responses always carry a message string, whatever the endpoint responds with on success.

Some endpoints may have additional fields in their responses.

Every response from every endpoint is a JSON object with a ``response`` member, with ``Content-Type: application/json``, whatever the status code.
That includes errors that never reach an endpoint: a request to a path that does not exist is answered with ``404 Not Found``, and one that names an endpoint with a method it does not accept with ``405 Method Not Allowed`` and an ``Allow`` header.
The only exception is ``GET /``, which serves the files of the web interface and answers with their content type instead.


GET /
~~~~~

Serves static resources, like the files that make up the web interface.
No authentication required.
This endpoint doesn't return JSON, but serves the file contents directly instead.

The resources for the web interface are embedded in the server binary.
It is possible to provide an alternate set of resources by using the ``webshell_resource_manifest_path`` configuration option, pointing it to a resource manifest file (``webshell.resources``).
This is mainly useful for development purposes, to be able to serve modified versions of the web interface files without having to recompile the server binary.

.. _webshell-login-endpoint:

POST /login
~~~~~~~~~~~

Creates a new session, returns a session id as a response cookie.
The client is expected to re-send the session id cookie in subsequent requests, otherwise their requests against endpoints that require authentication will be rejected with ``401 Unauthorized`` status code.
Sessions have a TTL of 10 minutes. After this much inactivity, the session is terminated and the session id is invalidated.
Any request to any endpoint refreshes the session TTL.

Login credentials should be sent in the request body, with the following schema:

.. code-block:: json

    {
      "username": "<username>",
      "password": "<password>"
    }


If the server has anonymous access enabled, the request body should be empty. Such empty login request can be used to poll for whether anonymous access is enabled.

When `mutual TLS <webshell-mutual-tls_>`_ is enabled and the client presents a valid certificate over ``HTTPs``, the session is authenticated from the certificate and the request body is ignored.
If the presented certificate cannot be mapped to a role, the request is rejected with ``400 Bad Request``.
If no certificate is presented (only possible in ``request`` mode), the server falls back to username/password authentication from the request body.

This endpoint is idempotent, invoking with an already valid session id cookie is not an error, the endpoint will return ``200 OK``, in this case the request body is ignored.

Response status codes:

* ``200 OK`` - session created successfully or already exists, session id cookie returned.
* ``400 Bad Request`` - missing, badly formed or invalid credentials.
* ``500 Internal Error`` - generic internal error, most likely a bug.
* ``504 Service Unavailable`` - too many requests or too many sessions.

POST /logout
~~~~~~~~~~~~

Terminates the session identified by the session id cookie.
Request body is ignored.

This endpoint is idempotent, invoking with no session id cookie or an invalid session id cookie is not an error, the endpoint will return ``200 OK``.

Response status codes:

* ``200 OK`` - session terminated successfully or did not exist.
* ``500 Internal Error`` - generic internal error, most likely a bug.
* ``504 Service Unavailable`` - too many requests or too many sessions.

POST /query
~~~~~~~~~~~

Executes a CQL query. The query should be sent in the request body, with the following schema:

.. code-block:: json

    {
      "query": "<query>",
      "paging_state": "<paging_state>",
      "options": {"<option>": "<value>"}
    }

The ``paging_state`` field is optional and should only be provided when fetghing subsequent pages of a paged query result.
When a query is paged, each response will contain a ``paging_state`` field, which should be sent back verbating in the next request to get the next page of results.
If no ``paging_state`` field is provided, the query is executed from the beginning.

Requires an authenticated session and the logged in user to have permission to execute the query.

Certain aspects of query execution can be controlled via session options, which are stored as part of the session state and can be manipulated via the `/option endpoint <webshell-option-endpoint_>`_, see `session options <webshell-session-options_>`_ for details.

The ``options`` field is optional and overrides those session options for this one request, without changing the session itself.
Its members are the option names and values described under `session options <webshell-session-options_>`_.
An option that is not mentioned, or is mentioned with a ``null`` value, keeps the value it has in the session.
An unrecognized option name is rejected with ``400 Bad Request``, rather than ignored.

This is how a client gets a result formatted the way it needs from a session that is configured differently - most usefully a machine-readable one, by overriding ``output_format`` to ``JSON`` for a single query, while a human keeps reading ``TEXT`` results in the same session:

.. code-block:: json

    {
      "query": "SELECT keyspace_name, table_name FROM system_schema.tables",
      "options": {"output_format": "JSON"}
    }

The response body schema is as follows:

.. code-block:: json

    {
      "response": "<query-result>",
      "paging_state": "<paging_state>",
      "trace_session_id": "<tracing-session-id>"
    }

The query-result is formatted according to the ``OUTPUT FORMAT`` and ``EXPANDED`` session options. If ``OUTPUT FORMAT`` is ``JSON``, the query result is simply included in the response, without quoting.
The ``paging_state`` is a ``base64`` encoded blob, which should be re-sent on the next query request, to request the next page of the query.
If a different query is sent or if the ``paging_state`` is not included in the next request, the query will restart from the first page.

When tracing is enabled, the response will contain a ``trace_session_id`` field, which can be used to fetch the trace details, either via a direct query against ``system_traces.sessions`` and ``system_traces.events`` tables, or via the ``SHOW SESSION <tracing-session-id>`` command (see `session commands <webshell-session-commands_>`_).

Response status codes:

* ``400 Bad Request`` - bad query.
* ``401 Unauthorized`` - user not logged in (e.g. no valid session id).
* ``403 Fordbidden`` - user is logged in but doesn't have permissions to run the query.
* ``500 Internal Error`` - generic internal error, most likely a bug.
* ``504 Service Unavailable`` - too many requests.

.. _webshell-command-endpoint:

POST /command
~~~~~~~~~~~~~

Handles commands. Request body should contain a single command with the following schema:

.. code-block:: json

    {
      "command": "<command>",
      "arguments": "[<arg1>, <arg2>, ...]"
    }

.. _webshell-session-commands:

session commands
""""""""""""""""
* ``HELP`` - show a help about available commands and options.
* ``SHOW SESSION [<tracing-session-id>]`` - show tracing session events for the provided tracing session id.

.. _webshell-option-endpoint:

POST /option
~~~~~~~~~~~~~

Changes a session option. Option values are stored as part of the session state and affect subsequent queries executed in the context of the same session.
Reading an option is a separate endpoint, `GET /option <webshell-option-read-endpoint_>`_; this one only changes them.
Request body should contain a single option, with the following schema:

.. code-block:: json

    {
      "option": "<option>",
      "arguments": "[<arg1>, <arg2>, ...]"
    }

Both fields are required, and ``arguments`` must not be empty - a request with no arguments is rejected with ``400 Bad Request`` rather than treated as a read.

The response is an object holding the option's new value, using the member names and types described below, never a sentence - it is up to the client to render those values for a human, if it has one:

.. code-block:: json

    {
      "response": {"page_size": 100}
    }

The resulting value is reported because the request does not always determine it: ``PAGING ON``, for one, resolves to a server-side default page size that the client would otherwise have to guess.

This endpoint is idempotent, setting an option to the value it already has does not result in an error, the endpoint will just return the usual response.

Response status codes:

* ``200 OK`` - option changed successfully, new value returned.
* ``400 Bad Request`` - unrecognized option, missing or bad arguments for it.
* ``401 Unauthorized`` - user not logged in (e.g. no valid session id).
* ``500 Internal Error`` - generic internal error, most likely a bug.
* ``504 Service Unavailable`` - too many requests.

.. _webshell-option-read-endpoint:

GET /option
~~~~~~~~~~~

Reports session options. This endpoint never changes anything; changing an option is `POST /option <webshell-option-endpoint_>`_.

With no query parameters, every option is reported:

.. code-block:: console

    curl -b cookies http://127.0.0.1:10001/option

.. code-block:: json

    {
      "response": {
        "consistency": "ONE",
        "expand": false,
        "output_format": "TEXT",
        "page_size": 100,
        "serial_consistency": "SERIAL",
        "tracing": false
      }
    }

With an ``option`` query parameter, only that one is reported.
The name is the same one ``POST /option`` takes, so it can contain a space, which has to be percent-encoded as ``%20`` or written as ``+``:

.. code-block:: console

    curl -b cookies 'http://127.0.0.1:10001/option?option=serial%20consistency'

.. code-block:: json

    {
      "response": {"serial_consistency": "SERIAL"}
    }

This is what makes the options authoritative for a client that did not set them itself - one that has just re-attached to an existing session, for instance, where ``POST /login`` answers "Already logged in" and the options are whatever they were left at.

Response status codes:

* ``200 OK`` - option or options reported.
* ``400 Bad Request`` - unrecognized option.
* ``401 Unauthorized`` - user not logged in (e.g. no valid session id).
* ``500 Internal Error`` - generic internal error, most likely a bug.
* ``504 Service Unavailable`` - too many requests.

.. _webshell-session-options:

session options
"""""""""""""""

Options are addressed by name, CQLSH style, in the ``option`` field of ``POST /option`` and in the ``option`` query parameter of ``GET /option``:

* ``CONSISTENCY <level>`` - set default consistency level for queries (default: ``ONE``).
* ``EXPAND [ON|OFF]`` - enable/disable expanded (vertical) output (default: ``OFF``).
* ``OUTPUT FORMAT [TEXT|JSON]`` - set output format (default: ``TEXT``).
* ``PAGING [ON|OFF|<number>]`` - enable/disable/limit result paging (default: ``100``).
* ``SERIAL CONSISTENCY <level>`` - set default serial consistency level for queries (default: ``SERIAL``).
* ``TRACING [ON|OFF]`` - enable/disable query tracing (default: ``OFF``).

The same options appear as members of a JSON object when either ``/option`` endpoint reports them, and when ``POST /query`` accepts them as per-request overrides in its ``options`` field.
There, each option is named after the value it carries rather than after the CQLSH command, and has its natural JSON type.
String values are case-insensitive.

* ``consistency`` (string) - ``CONSISTENCY``. One of ``ANY``, ``ONE``, ``TWO``, ``THREE``, ``QUORUM``, ``ALL``, ``LOCAL_QUORUM``, ``EACH_QUORUM``, ``SERIAL``, ``LOCAL_SERIAL`` or ``LOCAL_ONE``.
* ``expand`` (boolean) - ``EXPAND``.
* ``output_format`` (string) - ``OUTPUT FORMAT``. Either ``TEXT`` or ``JSON``.
* ``page_size`` (integer) - ``PAGING``. A value of ``0`` or less disables paging, so ``PAGING OFF`` is reported as ``0``.
* ``serial_consistency`` (string) - ``SERIAL CONSISTENCY``. Either ``SERIAL`` or ``LOCAL_SERIAL``.
* ``tracing`` (boolean) - ``TRACING``.
