==================
ScyllaDB Web Shell
==================

.. warning:: The Web Shell is still experimental, it may change without notice and there may be bugs.

Administrative API designed with interactive usage and convenience in mind. Not designed for high performance or data-heavy workloads, for these, use the `CQL </cql/>`_ or `Alternator </alternator/alternator/>`_ protocols instead.
Allows for executing CQL commands and queries against a ScyllaDB cluster, with as little as simple HTTP client, like `curl <https://curl.se>`_ or the python `requests library <https://requests.readthedocs.io/en/latest/>`_.
A simple web interface is also provided for convenience, with a similar look and feel to that of `CQLSH </cql/cqlsh>`_.

Backend
-------

The Web Shell backend is implemented as a handful of ``HTTP`` endpoints.
Design goals:

* Simple text protocol.
* Input and output are plain text, sent in request and response bodies.
* Additional options and metadata are sent as ``HTTP`` cookies.
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

GET /
~~~~~

Serves static resources, like the files that make up the web interface.
No authentication required.

POST /login
~~~~~~~~~~~

Creates a new session, returns a session id as a response cookie.
The client is expected to re-send the session id cookie in subsequent requests, otherwise their requests against endpoints that require authentication will be rejected with ``401 Unauthorized`` status code.
Sessions have a TTL of 10 minutes. After this much inactivity, the session is terminated and the session id is invalidated.
Any request to any endpoint refreshes the session TTL.

Login credentials should be sent in the request body, in the form of ``${username}\n${password}``.
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

Executes a CQL query. The query should be sent in the request body.
Requires an authenticated session and the logged in user to have permission to execute the query.

Certain aspects of query execution can be controlled via session options, which are stored as part of the session state and can be maninulated via the `/command endpoint <webshell-command-endpoint_>`_, see `session options <webshell-session-options_>`_ for details.

When paging is enabled, the response will contain a ``paging_state`` cookie, which should be sent back to the server in the next request to get the next page of results. The client can iterate until an empty ``paging_state`` cookie is returned, which indicates that there are no more results. The client can cancel paging, by sending an empty ``paging_state`` cookie in the next request, or by sending a different query.
When tracing is enabled, the response will contain a ``trace_session_id`` cookie, which can be used to fetch the trace details, either via a direct query against ``system_traces.sessions`` and ``system_traces.events`` tables, or via the ``SHOW SESSION <tracing-session-id>`` command (see `session commands <webshell-session-commands_>`_).

Response body contains the query result, formatted as plain text or json, dependin on the ``OUTPUT FORMAT`` session options.

Response status codes:

* ``400 Bad Request`` - bad query.
* ``401 Unauthorized`` - user not logged in (e.g. no valid session id).
* ``403 Fordbidden`` - user is logged in but doesn't have permissions to run the query.
* ``500 Internal Error`` - generic internal error, most likely a bug.
* ``504 Service Unavailable`` - too many requests.

.. _webshell-command-endpoint:

POST /command
~~~~~~~~~~~~~

Handles session commands and options. Option values are stored as part of the session state and affect subsequent queries executed in the context of the same session.
Request body should contain a single command. The response body contains the result of executing the command, if any.

.. _webshell-session-options:

session options
"""""""""""""""
* ``CONSISTENCY LEVEL`` - controls the consistency level of the query, default is ``ONE``.
* ``EXPAND`` - enable/disable expanded (vertical) output, with no args show current setting (default: ``OFF``).
* ``OUTPUT FORMAT [TEXT|JSON]`` - set output format, with no args show current setting (default: ``TEXT``).
* ``PAGING [ON|OFF|<number>]`` - enable/disable/limit result paging, with no args show current setting (default: ``100``).
* ``SERIAL CONSISTENCY [<level>]`` - set default serial consistency level for queries, with no args show current setting (default: ``SERIAL``).
* ``TRACING [ON|OFF]`` - enable/disable query tracing, with no args show current setting (default: ``OFF``).

.. _webshell-session-commands:

session commands
""""""""""""""""
* ``HELP`` - show a help about available commands and options.
* ``SHOW [SESSION <tracing-session-id>]`` - show tracing session events for the provided tracing session id.
