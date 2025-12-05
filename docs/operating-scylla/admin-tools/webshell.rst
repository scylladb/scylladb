==================
ScyllaDB Web Shell
==================

.. warning:: The Web Shell is still experimental, it may change without notice and there may be bugs.

Administrative API designed with interactive usage and convenience in mind. Not designed for high performance or data-heavy workloads, for these, use the `CQL </cql/>`_ or `Alternator </alternator/alternator/>`_ protocols instead.
Allows for executing CQL commands and queries against a ScyllaDB cluster, with as little as simple HTTP client, like `curl <https://curl.se>`_ or the python `requests library <https://requests.readthedocs.io/en/latest/>`_.
A simple web interface is also provided for convenience, with a similar look and feel to that of `CQLSH </cql/cqlsh>`_.

By default the Web Shell is available on the same IP address as the `REST API </operating-scylla/rest>`_, which is localhost, and using ``HTTP``.
This is for security reasons, as ``HTTP`` is not encrypted and therefore not safe for remote access.

To use Web Shell remotely, enable it on a public IP address and use ``HTTPs``, using the ``webshell_https_address``, ``webshell_https_port``, and ``webshell_https_encryption_options`` configuration options.

Example: enable Web Shell remote access on public IP ``172.17.0.1`` and port ``10002`` with ``HTTPs`` via self-signed certificate:

.. code-block:: yaml

    webshell_https_address: 172.17.0.1
    webshell_https_port: 10002
    webshell_https_encryption_options:
        certificate: /path/to/mycert.crt
        keyfile: /path/to/mycert.key

To generate a self signed certificate for testing purposes, you can use the `generate self-signed certificate </operating-scylla/security/generate-certificate>`_ guide.
For production systems, it is recommended to use a certificate signed by a trusted Certificate Authority.

Backend
-------

The Web Shell backend is implemented as a handful of ``HTTP`` endpoints.
Design goals:

* Simple text protocol.
* Requestsa and responses are JSON.
* Session metadata is stored in ``HTTP`` cookies.
* Distinct error codes for different error types.
* Stateful sessions with defined lifetime, identified by the session id cookie.
* Secure by default: ``HTTP`` server listens only on localhost ``HTTPs`` server available for remote access.
* Sessions are authenticated and all queries pass authorization checks.

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

Endpoints
^^^^^^^^^

All endpoints use JSON for request and response bodies, unless otherwise noted. Some endpoints do not require a request body.
All endpoints that use JSON, use the following response schema:

.. code-block:: json

    {
      "response": "<respose-data>"
    }

Some endpoints may have additional fields in their responses.


GET /
~~~~~

Serves static resources, like the files that make up the web interface.
No authentication required.
This endpoint doesn't return JSON, but serves the file contents directly instead.

The resources for the web interface are embedded in the server binary.
It is possible to provide an alternate set of resources by using the ``webshell_resource_manifest_path`` configuration option, pointing it to a resource manifest file (``webshell.resources``).
This is mainly useful for development purposes, to be able to serve modified versions of the web interface files without having to recompile the server binary.

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

This endpoint is idempontent, invoking with an already valid session id cookie is not an error, the endpoint will return ``200 OK``, in this case the request body is ignored.

Response status codes:

* ``200 OK`` - session created successfully or already exists, session id cookie returned.
* ``400 Bad Request`` - missing, badly formed or invalid credentials.
* ``500 Internal Error`` - generic internal error, most likely a bug.
* ``504 Service Unavailable`` - too many requests or too many sessions.

POST /logout
~~~~~~~~~~~~

Terminates the session identified by the session id cookie.
Request body is ignored.

This endpoint is idempontent, invoking with no session id cookie or an invalid session id cookie is not an error, the endpoint will return ``200 OK``.

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
      "paging_state": "<paging_state>"
    }

The ``paging_state`` field is optional and should only be provided when fetghing subsequent pages of a paged query result.
When a query is paged, each response will contain a ``paging_state`` field, which should be sent back verbating in the next request to get the next page of results.
If no ``paging_state`` field is provided, the query is executed from the beginning.

Requires an authenticated session and the logged in user to have permission to execute the query.

Certain aspects of query execution can be controlled via session options, which are stored as part of the session state and can be manipulated via the `/option endpoint <webshell-option-endpoint_>`_, see `session options <webshell-session-options_>`_ for details.

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

Handles session options. Option values are stored as part of the session state and affect subsequent queries executed in the context of the same session.
Request body should contain a single option, with the following schema:

.. code-block:: json

    {
      "option": "<option>",
      "arguments": "[<arg1>, <arg2>, ...]"
    }


.. _webshell-session-options:

session options
"""""""""""""""
* ``CONSISTENCY LEVEL`` - controls the consistency level of the query, default is ``ONE``.
* ``EXPAND`` - enable/disable expanded (vertical) output, with no args show current setting (default: ``OFF``).
* ``OUTPUT FORMAT [TEXT|JSON]`` - set output format, with no args show current setting (default: ``TEXT``).
* ``PAGING [ON|OFF|<number>]`` - enable/disable/limit result paging, with no args show current setting (default: ``100``).
* ``SERIAL CONSISTENCY [<level>]`` - set default serial consistency level for queries, with no args show current setting (default: ``SERIAL``).
* ``TRACING [ON|OFF]`` - enable/disable query tracing, with no args show current setting (default: ``OFF``).
