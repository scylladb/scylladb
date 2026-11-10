=======================================
Web Shell :label-caution:`Experimental`
=======================================

The Web Shell is an administrative API designed for interactive use and convenience. It is not intended for high-performance or data-intensive workloads.
Use the :doc:`CQL </cql/index>` or :doc:`Alternator </alternator/alternator/>` protocols for those workloads.

The Web Shell lets you run CQL queries against a ScyllaDB cluster through any HTTP(S) client, such as `curl <https://curl.se>`_ or the Python `requests library <https://requests.readthedocs.io/en/latest/>`_.
It also provides a simple web interface with a look and feel similar to :doc:`CQLSH </cql/cqlsh>`.

Enabling the Web Shell
^^^^^^^^^^^^^^^^^^^^^^

The Web Shell is served on the ``webshell`` listeners of the ``listeners`` :doc:`cluster config </cql/cluster-config>` option, see :doc:`Protocol Listeners </operating-scylla/listeners>`.
It is disabled by default, as there are no such listeners by default.

.. note::

   A non-empty ``listeners`` option replaces the per-protocol listener options of ``scylla.yaml`` (``native_transport_port`` and friends), for all protocols.
   So when enabling the Web Shell, list the CQL (and Alternator) listeners the nodes are to serve too.

Example: enable the Web Shell over ``HTTP`` on localhost, port 10010, alongside the default CQL listeners:

.. code-block:: cql

   ALTER CLUSTER WITH listeners = {
       cql: {protocol: 'cql', port: 9042, shard_aware_listener: 'cql_shard_aware'},
       cql_shard_aware: {protocol: 'cql', port: 19042, shard_aware: true},
       webshell: {protocol: 'webshell', port: 10010}};

The ``address`` of a ``webshell`` listener defaults to ``localhost``.
Plain ``HTTP`` is for local access only: for development, or as an additional avenue for administrator access from the local node.
Do not serve ``HTTP`` on a public IP address in a production environment! This will result in credentials and possibly sensitive user-data being transmitted on the network in unencrypted form.

Using ``HTTPS`` is strongly recommended for any remote access.
Example: serve the Web Shell over ``HTTPS`` on the node's address in the ``10.0.0.0/8`` network, port 10011:

.. code-block:: cql

   ALTER CLUSTER WITH listeners = {
       cql: {protocol: 'cql', port: 9042, shard_aware_listener: 'cql_shard_aware'},
       cql_shard_aware: {protocol: 'cql', port: 19042, shard_aware: true},
       webshell_https: {protocol: 'webshell', address: '10.0.0.0/8', port: 10011,
                        tls: {certificate: '/etc/scylla/webshell.crt', keyfile: '/etc/scylla/webshell.key'}}};

``tls: true`` uses ``client_encryption_options``, the encryption options of CQL.
To generate a self signed certificate for testing purposes, you can use the :doc:`generate self-signed certificate </operating-scylla/security/generate-certificate>` guide.
For production systems, it is recommended to use a certificate signed by a trusted Certificate Authority.

The listeners can be changed at runtime, the Web Shell starts serving the new listeners without a restart.

.. _webshell-auth:

Authentication
^^^^^^^^^^^^^^

Each request is authenticated on its own, as dictated by the :doc:`authentication configuration </operating-scylla/security/authentication/>`.
A client is authenticated by either:

* Its TLS client certificate, see :ref:`mutual TLS <webshell-mutual-tls>`. This requires an authenticator which can map certificates to roles, like ``CertificateOrPasswordAuthenticator``; with other authenticators, such as ``PasswordAuthenticator``, the certificate is ignored.
* The username and password in the ``Authorization`` header of the request, with the ``Basic`` scheme (`RFC 7617 <https://datatracker.ietf.org/doc/html/rfc7617>`_). This is what ``curl --user <username>:<password>`` sends.

When the authenticator doesn't require authentication, the requests need no credentials.

A request which fails authentication is rejected with ``401 Unauthorized``.
The ``WWW-Authenticate`` header of the rejection deliberately doesn't name the ``Basic`` scheme, so that browsers don't prompt for, and then remember, credentials on their own. The web interface asks for them instead.

Authentication is repeated for every request, so it adds overhead to each one: password authentication involves computing a password hash, and an LDAP authenticator a round-trip to the LDAP server.
This is fine for the interactive and administrative use the Web Shell is meant for.

The recommended authenticator is the :doc:`CertificateOrPasswordAuthenticator </operating-scylla/security/certificate-or-password-authentication/>`.
This allows using :ref:`mTLS <webshell-mutual-tls>` for scripts and ``curl``, with fall-back to password-based authentication for browsers.

.. _webshell-mutual-tls:

Mutual TLS
~~~~~~~~~~

An ``HTTPS`` listener can ask clients to present a TLS client certificate (mutual TLS, or mTLS), with the ``require_client_auth`` key of its TLS options:

* ``false`` (default) - client certificates are not requested.
* ``optional`` - a client certificate is requested but not required. Clients that present a valid certificate are authenticated by it; clients that do not are authenticated by the ``Authorization`` header. Use this mode to support both the web interface (username/password) and scripts or ``curl`` (certificate) on the same listener.
* ``true`` - a client certificate is required. The TLS handshake is rejected for clients that do not present a valid certificate.

For the server to validate client certificates, set the ``truststore`` option to the PEM-encoded certificate of the Certificate Authority that signed the client certificates.

A client certificate is mapped to a role by the authenticator. If the client presents a certificate which the authenticator cannot map to a role, the request is rejected, there is no fall-back to the ``Authorization`` header.

Example:

.. code-block:: cql

   ALTER CLUSTER WITH listeners = {
       ...,
       webshell_https: {protocol: 'webshell', address: '172.17.0.1', port: 10011,
                        tls: {certificate: '/path/to/server.crt', keyfile: '/path/to/server.key',
                              truststore: '/path/to/ca.crt', require_client_auth: 'optional'}}};

Endpoints
^^^^^^^^^

The Web Shell has a single API endpoint, ``POST /query``, which executes a single CQL query.
Every other ``GET`` request is answered with the files of the web interface, which is served on ``/``.

Design goals:

* Simple: requests and responses are JSON. Each request carries everything needed to execute it: credentials, query and the options of the query.
* Requests are authenticated and all queries pass authorization checks.
* Distinct status codes for different error types.
* Secure by default: there are no listeners by default, a listener defaults to localhost.

Non goals:

* High performance.
* High concurrency.
* Low latency.
* Low resource overhead.
* Rich feature set.

Requests are subject to concurrency control. The limits apply to each shard separately, a request counts against the limits of the shard it arrives on:

* At most 16 requests are processed concurrently, further requests are queued to wait their turn.
* At most 16 requests wait, further requests are rejected with ``429 Too Many Requests``, with a ``Retry-After`` header.

The requests are listed in the ``system.clients`` virtual table, with ``client_type`` ``webshell``, both while they wait for their turn (``connection_stage`` ``ESTABLISHED``) and while they are processed (``AUTHENTICATING``, then ``READY``), including the writing of the response. ``driver_name`` is ``webshell: `` followed by the ``User-Agent`` header of the request, ``username`` is the authenticated user.

All responses, except the files of the web interface, are JSON objects, with ``Content-Type: application/json``.
Error responses carry the error message in the ``error`` member:

.. code-block:: json

    {"error": "<message>"}

Requests made by web pages of other origins are rejected with ``403 Forbidden``: requests carrying an ``Origin`` header which doesn't match their ``Host`` header. Browsers send the ``Origin`` header with such requests, other clients usually don't send it at all.

A request to a path that does not exist is answered with ``404 Not Found``, and a ``GET /query`` with ``405 Method Not Allowed``.

POST /query
~~~~~~~~~~~

Executes a CQL query. The request body is a JSON object:

.. code-block:: json

    {
      "query": "<query>",
      "keyspace": "<keyspace>",
      "consistency": "<consistency level>",
      "serial_consistency": "<serial consistency level>",
      "page_size": 100,
      "paging_state": "<paging state>",
      "timeout_ms": 1000,
      "tracing": false
    }

Only ``query`` is mandatory. Members that are not recognized are rejected, with ``400 Bad Request``.

* ``query`` - the CQL query to execute. A single statement, without the terminating ``;``.
* ``keyspace`` - the keyspace of the query, used for table names which name no keyspace. It is what ``USE <keyspace>`` sets for the following queries of a CQL connection, a ``USE`` sent to the Web Shell applies only to itself.
* ``consistency`` - the consistency level of the query, case-insensitive. One of ``ANY``, ``ONE``, ``TWO``, ``THREE``, ``QUORUM``, ``ALL``, ``LOCAL_QUORUM``, ``EACH_QUORUM``, ``SERIAL``, ``LOCAL_SERIAL`` or ``LOCAL_ONE``. Default: ``ONE``.
* ``serial_consistency`` - the serial consistency level of the query, case-insensitive. Either ``SERIAL`` or ``LOCAL_SERIAL``. Default: ``SERIAL``.
* ``page_size`` - the maximum number of rows returned in the response. Between 1 and 1000, default: 100. Paging cannot be turned off.
* ``paging_state`` - the ``paging_state`` of the previous response, to fetch the next page of the same query. If not provided, the query is executed from the beginning.
* ``timeout_ms`` - the timeout of the query, in milliseconds. It can only lower the timeouts configured for the server (``read_request_timeout_in_ms`` and friends), not raise them. The timeout of the user's :doc:`service level </features/workload-prioritization>`, if it has one, takes precedence.
* ``tracing`` - trace the query, see :doc:`tracing </using-scylla/tracing>`. Default: ``false``.

The query is executed in the scheduling group of the service level of the user.

The response of a successful query is a JSON object, with the following members:

.. code-block:: json

    {
      "trace_session_id": "<tracing session id>",
      "warnings": ["<warning>"],
      "set_keyspace": "<keyspace>",
      "schema_change": {"change": "CREATED", "target": "TABLE", "keyspace": "<keyspace>", "arguments": ["<table>"]},
      "column_types": [{"name": "<name>", "type": "<cql type>"}],
      "paging_state": "<paging state>",
      "result_set": [{"<name>": "<value>"}]
    }

Members which do not apply to the query are left out:

* ``trace_session_id`` - the id of the tracing session, when the query was traced. Use it to fetch the details of the trace from the ``system_traces.sessions`` and ``system_traces.events`` tables.
* ``warnings`` - the warnings of the query, if there are any.
* ``set_keyspace`` - the keyspace switched to, for ``USE <keyspace>``. To use it for the following queries, send it in their ``keyspace`` member.
* ``schema_change`` - the schema change made by the query, for schema altering statements. ``change`` is one of ``CREATED``, ``UPDATED`` or ``DROPPED``. ``target`` is one of ``KEYSPACE``, ``TABLE``, ``TYPE``, ``FUNCTION`` or ``AGGREGATE``. ``arguments`` holds the name of the target (and for functions and aggregates, the types of their arguments), if the target is not a keyspace.
* ``column_types`` - the columns of the result, with their name and CQL type, for queries which return rows.
* ``paging_state`` - an opaque, ``base64`` encoded blob, when the query has more pages. Send it with the same query, to fetch the next page.
* ``result_set`` - the rows of the result, for queries which return rows. Each row is a JSON object, with the column names as keys. The values are formatted the same as with ``SELECT JSON``: an absent value is ``null``, collections, tuples and user defined types are JSON arrays or objects, blobs are hexadecimal strings, and so on.

Response status codes:

* ``200 OK`` - the query was executed successfully.
* ``400 Bad Request`` - invalid request or query.
* ``401 Unauthorized`` - the request failed authentication.
* ``403 Forbidden`` - the user doesn't have permission to execute the query, or the request was made by a web page of another origin.
* ``413 Payload Too Large`` - the request body is larger than 1MB.
* ``429 Too Many Requests`` - too many requests, either for the Web Shell or for the node.
* ``500 Internal Server Error`` - the query failed, or an internal error, most likely a bug.
* ``501 Not Implemented`` - the query has to be executed on another node, which the Web Shell doesn't support (e.g. some queries of strongly consistent tables). The response names that node, in the ``target_node`` member, so the query can be retried on its Web Shell: ``{"error": "<message>", "target_node": {"host_id": "<host id>", "address": "<address>"}}``. The ``address`` is left out if it is not known.
* ``503 Service Unavailable`` - not enough replicas are alive to execute the query with the requested consistency level, or the server is shutting down.
* ``504 Gateway Timeout`` - the query timed out.

Examples
~~~~~~~~

Execute a query with ``curl``:

.. code-block:: console

    $ curl --user cassandra:cassandra http://127.0.0.1:10010/query \
        -d '{"query": "SELECT keyspace_name, table_name FROM system_schema.tables WHERE keyspace_name = '"'"'system_schema'"'"'", "page_size": 2}'
    {"column_types":[{"name":"keyspace_name","type":"text"},{"name":"table_name","type":"text"}],"paging_state":"...","result_set":[{"keyspace_name":"system_schema","table_name":"aggregates"},{"keyspace_name":"system_schema","table_name":"columns"}]}

Fetch all pages of a query with Python:

.. code-block:: python

    import requests

    def query(query, **options):
        rows = []
        while True:
            response = requests.post("http://127.0.0.1:10010/query", auth=("cassandra", "cassandra"),
                                     json={"query": query, **options})
            response.raise_for_status()
            result = response.json()
            rows += result.get("result_set", [])
            if "paging_state" not in result:
                return rows
            options["paging_state"] = result["paging_state"]

The web interface
^^^^^^^^^^^^^^^^^

The web interface is served on ``/``, open it in a browser, e.g. http://127.0.0.1:10010/ for the first example above.
It asks for a username and a password, unless the cluster allows anonymous access. The credentials are kept in the memory of the page and sent with every request, reloading the page signs out.

Type CQL statements, terminated by ``;``, to execute them. Type ``HELP`` for the list of commands and options.
Options, like the consistency level or the page size, are kept by the web interface, which sends them with every query.
