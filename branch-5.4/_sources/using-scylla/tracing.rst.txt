Tracing
-------



Tracing is a ScyllaDB tool meant to help debugging and analyzing internal flows in the server.  There are three types of tracing you can use with Scylla:

* **User Defined CQL query** - One example of such a flow is CQL request processing. By placing a flag inside a CQL query, you can start tracing.
* **Probalistic Tracing** randomly chooses a request to be traced with some defined probability.
* **Slow Query Logging** - records queries with handling time above the specified threshold.

.. note:: If you're planning to use either **probabilistic tracing** or **slow query logging** (see below), it's advisable to change the ``replication_factor`` of the  ``system_traces`` keyspace to ``ALL``:
          
          If you use ``NetworkTopologyStrategy``, we recommend using a replication factor for each datacenter equal to the number 
          of nodes in that datacenter. Alternatively, you can use ``EverywhereReplicationStrategy``. See :doc:`How to Safely Increase the Replication Factor </kb/rf-increase>` for details on changing the RF.

          Such configuration will speed up reads and writes (if combined with a reasonable consistency level, such as ONE or LOCAL_ONE).

User Defined CQL Query Tracing 
..............................

Enabling
^^^^^^^^

While inside a ``cqlsh`` prompt you can enable tracing for this session with the command ``TRACING ON|OFF``.

.. code-block:: cql

   cqlsh> TRACING ON

Returns

.. code-block:: cql

   Now Tracing is enabled

Resume your normal activities, such as adding information into a table. For example:

.. code-block:: cql

   cqlsh> INSERT into keyspace1.standard1 (key, "C0") VALUES (0x12345679, bigintAsBlob(123456));

Returns

.. code-block:: cql

   Tracing session: 227aff60-4f21-11e6-8835-000000000000

   activity                                                                                         | timestamp                  | source    | source_elapsed
   -------------------------------------------------------------------------------------------------+----------------------------+-----------+----------------
   Execute CQL3 query                                                                               | 2016-07-21 11:57:21.238000 | 127.0.0.2 | 0
   Parsing a statement [shard 1]                                                                    | 2016-07-21 11:57:21.238335 | 127.0.0.2 | 1
   Processing a statement [shard 1]                                                                 | 2016-07-21 11:57:21.238405 | 127.0.0.2 | 71
   Creating write handler for token: 2309717968349690594 natural: {127.0.0.1} pending: {} [shard 1] | 2016-07-21 11:57:21.238433 | 127.0.0.2 | 99
   Creating write handler with live: {127.0.0.1} dead: {} [shard 1]                                 | 2016-07-21 11:57:21.238439 | 127.0.0.2 | 105
   Sending a mutation to /127.0.0.1 [shard 1]                                                       | 2016-07-21 11:57:21.238490 | 127.0.0.2 | 156
   Message received from /127.0.0.2 [shard 0]                                                       | 2016-07-21 11:57:21.238562 | 127.0.0.1 | 17
   Sending mutation_done to /127.0.0.2 [shard 0]                                                    | 2016-07-21 11:57:21.238658 | 127.0.0.1 | 113
   Mutation handling is done [shard 0]                                                              | 2016-07-21 11:57:21.238675 | 127.0.0.1 | 130
   Got a response from /127.0.0.1 [shard 1]                                                         | 2016-07-21 11:57:21.238950 | 127.0.0.2 | 616
   Mutation successfully completed [shard 1]                                                        | 2016-07-21 11:57:21.238958 | 127.0.0.2 | 624
   Done processing - preparing a result [shard 1]                                                   | 2016-07-21 11:57:21.238962 | 127.0.0.2 | 628
   Request complete

**NOTE:** ``source_elapsed`` starts over on ``127.0.0.1`` when execution gets there.

Viewing
^^^^^^^
The raw tracing data can be queried as below (scroll to view):

.. code-block:: none

   cqlsh> select * from system_traces.sessions where session_id=227aff60-4f21-11e6-8835-000000000000;

Returns

.. code-block:: none

   session_id                            | client    | command | coordinator | duration | parameters                                                                                                                                                                                                                       | request            | started_at
   --------------------------------------+-----------+---------+-------------+----------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------+---------------------------------
   227aff60-4f21-11e6-8835-000000000000  | 127.0.0.1 | QUERY   | 127.0.0.2   | 639      | {'consistency_level': 'ONE', 'page_size': '100', 'query': 'INSERT into keyspace1.standard1 (key, "C0") VALUES (0x12345679, bigintAsBlob(123456));', 'serial_consistency_level': 'SERIAL', 'user_timestamp': '1469091441238107'}  | Execute CQL3 query | 2016-07-21 08:57:21.238000+0000

   (1 rows)
   cqlsh> select * from system_traces.events where session_id=227aff60-4f21-11e6-8835-000000000000;

   session_id                            | event_id                             | activity                                                                               | source    | source_elapsed | thread
   --------------------------------------+--------------------------------------+----------------------------------------------------------------------------------------+-----------+----------------+--------
   227aff60-4f21-11e6-8835-000000000000  | 227b0c74-4f21-11e6-8835-000000000000 | Parsing a statement                                                                    | 127.0.0.2 | 1              | shard 1
   227aff60-4f21-11e6-8835-000000000000  | 227b0f34-4f21-11e6-8835-000000000000 | Processing a statement                                                                 | 127.0.0.2 | 71             | shard 1
   227aff60-4f21-11e6-8835-000000000000  | 227b1047-4f21-11e6-8835-000000000000 | Creating write handler for token: 2309717968349690594 natural: {127.0.0.1} pending: {} | 127.0.0.2 | 99             | shard 1
   227aff60-4f21-11e6-8835-000000000000  | 227b1087-4f21-11e6-8835-000000000000 | Creating write handler with live: {127.0.0.1} dead: {}                                 | 127.0.0.2 | 105            | shard 1
   227aff60-4f21-11e6-8835-000000000000  | 227b1284-4f21-11e6-8835-000000000000 | Sending a mutation to /127.0.0.1                                                       | 127.0.0.2 | 156            | shard 1
   227aff60-4f21-11e6-8835-000000000000  | 227b1559-4f21-11e6-bf08-000000000000 | Message received from /127.0.0.2                                                       | 127.0.0.1 | 17             | shard 0
   227aff60-4f21-11e6-8835-000000000000  | 227b1915-4f21-11e6-bf08-000000000000 | Sending mutation_done to /127.0.0.2                                                    | 127.0.0.1 | 113            | shard 0
   227aff60-4f21-11e6-8835-000000000000  | 227b19bd-4f21-11e6-bf08-000000000000 | Mutation handling is done                                                              | 127.0.0.1 | 130            | shard 0
   227aff60-4f21-11e6-8835-000000000000  | 227b247e-4f21-11e6-8835-000000000000 | Got a response from /127.0.0.1                                                         | 127.0.0.2 | 616            | shard 1
   227aff60-4f21-11e6-8835-000000000000  | 227b24ca-4f21-11e6-8835-000000000000 | Mutation successfully completed                                                        | 127.0.0.2 | 624            | shard 1
   227aff60-4f21-11e6-8835-000000000000  | 227b24f2-4f21-11e6-8835-000000000000 | Done processing - preparing a result                                                   | 127.0.0.2 | 628            | shard 1

   (11 rows)

Storing
^^^^^^^

Traces from ``cqlsh`` are stored in the ``system_traces`` keyspace for 24 hours. This setting cannot be changed. 

.. _tracing-probabilistic-tracing:

Probabilistic Tracing
.....................

Tracing implies a significant performance penalty on a cluster when enabled. Therefore, if tracing is required for some ongoing workload, it is undesirable to enable it for every request but rather for some (small) portion of requests. This can be done using  **probabilistic tracing**, which randomly chooses a request to be traced with some defined probability.

Enabling
^^^^^^^^
To trace 0.01% of all queries to one coordinator node, you can set a probabilistic tracing with the probability ``0.0001``:

.. code-block:: console

   nodetool settraceprobability 0.0001
   
To set the probabilistic tracing for the entire cluster, use the same command on *all* nodes.

Viewing	
^^^^^^^

If we need trace points for a specific session, we can query the ``events`` table for a given session's id. For example:

 .. code-block:: cql

   SELECT * from system_traces.sessions where session_id = 141ab010-d994-11e7-899e-000000000002;

Storing 
^^^^^^^
Traces are stored in the ``system_traces`` keyspace for 24 hours. This setting cannot be changed. The keyspace consists of two tables with a replication factor of 2:

* ``sessions`` table contains a single row for each tracing session.
* ``events`` table contains a single row for each trace point.

Traces are created in the context of a **tracing session**. For instance, if we trace an ``INSERT`` CQL command, a tracing session with a unique ID (``session_id`` column in the tables mentioned above) will be created, and all trace points hit during its execution will be stored in a context of this session.  This defines the format in which tracing data is stored.

``sessions`` table column descriptions
======================================

* ``session_id``: ID of this tracing session.
* ``command``: currently, this can only have a *QUERY* value.
* ``client``:  address of the client that sent this query.
* ``coordinator``: address of the coordinator node that received this query from the client.
* ``duration``:  the total duration of this tracing session in microseconds
* ``parameters``: this map contains string pairs that describe the query. This may include *query string* or *consistency level*.
* ``request``: a short string describing the current query, like "Execute CQL3 query".
* ``request_size``: size of the request (available from Scylla 3.0).
* ``response_size``: size of the response (available from Scylla 3.0).
* ``started_at``: a timestamp taken when the tracing session has begun.

``events`` table column descriptions
====================================

* ``session_id``: ID of this tracing session.
* ``event_id``: ID of this specific trace entry.
* ``activity``: a trace message.
* ``source``: address of a node where the trace entry has been created.
* ``scylla_parent_id``: ID of a parent span.
* ``scylla_span_id``: the ID of a span that sent an RPC that created the current span.
* ``source_elapsed``: a number of microseconds passed since the beginning of the tracing session on a specific node (see examples above).
* ``thread``: currently, this contains a number of the shard on which this trace point has been taken.
    
Slow Query Logging
..................

Often in real life installations, one of the most important parameters of the system is the longest response time. Naturally, the shorter it is, the better. Therefore, capturing a request that takes a long time and understanding why it took it so long is a very critical and challenging task.

**Slow query logging** will greatly ease debugging related to long requests. When enabled, it records queries with handling time above the specified threshold. As a result, there will be a new record created in ``system_traces.node_slow_log`` table. All tracing records created in the context of the current query on a coordinator node will also be written. In addition, if handling on a given replica takes too long, traces will be stored.

Enabling and configuring
^^^^^^^^^^^^^^^^^^^^^^^^

Slow query logging is disabled by default. A REST API allows configuring and querying the configuration of the feature. 

To set the parameters, run:

.. code-block:: console

   curl -X POST --header "Content-Type: application/json" --header "Accept: application/json" "http://<Node's address>:10000/storage_service/slow_query?enable=<true|false>&ttl=<in seconds>&threshold=<threshold in microseconds>"

For example, to disable the feature on a node with the address ``127.0.0.1``, set the ``ttl`` to ``8600`` and the threshold to ``10000``:

.. code-block:: console

   curl -X POST --header "Content-Type: application/json" --header "Accept: application/json" "http://127.0.0.1:10000/storage_service/slow_query?enable=false&ttl=8600&threshold=10000"

To get the current configuration, run:

.. code-block:: console

   curl -X GET --header "Content-Type: application/json" --header "Accept: application/json" "http://<Node's address>:10000/storage_service/slow_query"

After the ``POST`` command above, the query and result will look as below:

.. code-block:: console

   curl -X GET --header "Content-Type: application/json" --header "Accept: application/json" "http://127.0.0.1:10000/storage_service/slow_query"
   {"threshold": 10000, "enable": false, "ttl": 8600}

Viewing
^^^^^^^

Two time series helper tables were introduced that will help simplify the querying of traces.

``sessions_time_idx`` is for querying regular traces. Another table, the ``node_slow_log_time_idx`` table, is for querying slow query records.

``sessions_time_idx`` and ``node_slow_log_time`` table column descriptions
==========================================================================

* ``minute``: the minute, from epoch time, from when the record was taken.
* ``started_at``: a timestamp taken when the tracing session has begun.
* ``session_id``: the corresponding tracing session ID.
* ``start_time``: time when the query was initiated.
* ``node_ip``: address of a coordinator node.
* ``shard``: shard ID on a coordinator, where the query has been handled.

With these tables, one may get the relevant traces using a query like the one below:

.. code-block:: cql

   SELECT * from system_traces.sessions_time_idx where minutes in ('2016-09-07 16:56:00-0700') and started_at > '2016-09-07 16:56:30-0700';

Storing 
^^^^^^^

Slow query logging results are stored in the ``node_slow_log`` table for 24 hours. This setting cannot be changed.

``node_slow_log`` table column descriptions
===========================================

* ``start_time`` and ``date``: time when the query was initiated.
* ``node_ip``: address of a coordinator node.
* ``shard``: shard ID on a coordinator, where the query has been handled.
* ``command``: the query command, e.g. ``select * from my_ks.my_cf``.
* ``duration``: the duration query handling in microseconds.
* ``parameters``: query parameters like a parameters column in a ``system_traces.sessions`` table.
* ``session_id``: the corresponding tracing session ID.
* ``source ip``: address of the client that sent this query.
* ``table_names``: a list of tables used for this query, where applicable.
* ``username``: a user name used for authentication with this query.

Lightweight slow-queries logging mode
............................................

Natural desire is to run database with slow query tracing mode always enabled.
But the implementation can't detect early if the request will be slow before
it got processed so it has to record all the tracing events before making
a decision. Recording all the tracing events with all of its parameters during
the request execution implies sufficient overhead. This lightweight mode or
fast slow-queries tracing mode offers a solution to this problem allowing
low-overhead slow queries tracing.

The solution
^^^^^^^^^^^^

The *"lightweight slow-query logging"* is a ScyllaDB feature mode that is going to
ease the debugging related to the long requests even further. It minimizes
the tracing session related overhead to its minimum allowing it to be always
enabled.

In a nutshell, this mode tracks only CQL statement and related request
parameters. It effectively omits all the tracing events during the processing.

When enabled, it will work in the same way `slow query tracing` does besides
that it will omit recording all the tracing events. So that it will not
populate data to the `system_traces.events` table but it will populate
trace session records for slow queries to all the rest: `system_traces.sessions`,
`system_traces.node_slow_log`, etc.

Other tracing modes work as usual with that mode enabled.

How to enable and configure
^^^^^^^^^^^^^^^^^^^^^^^^^^^

By default lightweight slow query logging is disabled.

There is a REST API that allows configuring and querying the current
configuration of this feature.

To request current state of the tracing run:

.. code-block:: console

    $ curl http://<node address>:10000/storage_service/slow_query

    {"enable": false, "ttl": 86400, "threshold": 500000, "fast": false}

To enable lightweight slow-queries tracing run:

.. code-block:: console

    $ curl --request POST --header "Content-Type: application/json" --header "Accept: application/json" "http://<node address>:10000/storage_service/slow_query?enable=true&fast=true"

Normal slow query tracing can be enabled with:

.. code-block:: console

    $ curl --request POST --header "Content-Type: application/json" --header "Accept: application/json" "http://<node address>:10000/storage_service/slow_query?enable=true&fast=false"

Performance
^^^^^^^^^^^

We have found out that the lightweight slow-queries tracing implies about 10 times
less overhead on the requests processing than the normal slow query tracing
in the best case hot path (100% cache hit rate prepared statements reads
of a single row on 100% util shard).

In real production workloads we expect the effects to be almost completely
invisible.

Large Partition Tracing
.......................

When we use `Slow Query Logging`_ trying to identify the source of high latencies due to heavy queueing, we have to deal with the problem of “collateral damage”. 
All requests are going to have a long latency because their latency will consist of the queue latency and their handling latency. 
Therefore all of them are likely going to hit the Slow Query threshold and get logged.

If queueing is caused by some particularly heavy request, we would like to be able to filter this request from those that got logged due to a long queueing. 
We have recently added tools that would help us do that:

New columns were added to `system_traces.sessions`_ (available from Scylla 3.0)

* ``request_size``
* ``response_size``

.. _`Slow Query Logging`: #slow-query-logging

.. _`system_traces.sessions`: #sessions-table-column-descriptions

.. _tracing-catching-a-hot-partition:

Catching a Hot Partition
........................

After we started storing EXECUTE parameters in the ``system_traces.sessions`` we can now perform certain analytics tasks given a probabilistic traces recording, for instance, we can detect operations on hot partitions.

If we want to check if we have a hot partition, then we can record a slice of a workload using probabilistic tracing.

For example:

.. code-block:: cql

  nodetool settraceprobability 0.01

Analyze the key distribution - get all entries from ``system_traces.sessions``.

For example:

.. code-block:: cql

   SELECT * FROM system_traces.sessions

Count how many queries of the type you are looking for (SELECT, INSERT, DROP, etc.) with the same key you used.
Compare it to the total amount of requests of the corresponding type and make your conclusion.

.. _tracing-collecting-tracing-data:

Collecting Tracing Data
.......................

When submitting a request for support, it is helpful for us if you include your tracing data in the request.
This procedure can also be used to collect tracing data in order to view which queries are currently running.

**Before You Begin**

* Verify that you have enough disk space for the tracing file. This is purely dependent on how long you ran the trace for.
  For example, if you run the tracing for five minutes with 0.1 percentile, the file should be a few MB, but if you run it for four hours the file will be several GB.
* If you want to save this file to a specific directory, verify that it exists and that you have permission to write to it.
* This procedure saves the tracing data to the directory you are currently in unless specified. Use ``pwd`` in your terminal to verify your location before beginning.


**Procedure**

#. Add to the cqlsh parameters file the following:

   .. code-block:: none

      [copy]
      DELIMITER=;
      HEADER=true

#. Dump the contents of the system_traces.sessions and events tables to a file on disk. The file will be in the directory you are currently in.

   .. code-block:: none

      CONSISTENCY LOCAL_ONE;
      COPY system_traces.sessions TO 'sessions.out' WITH HEADER = TRUE;
      COPY system_traces.events TO 'events.out' WITH HEADER = TRUE;

   Alternatively, specify the location to send the dump file to. Change the path below to suit your needs.

   .. code-block:: none

      CONSISTENCY LOCAL_ONE;
      COPY system_traces.sessions TO '/tmp/tracing/sessions.out' WITH HEADER = TRUE;
      COPY system_traces.events TO '/tmp/tracing/events.out' WITH HEADER = TRUE;


If you are sending this data to Scylla for help, follow the directions in :ref:`How to Report a Scylla Problem <report-performance-problem>`.
