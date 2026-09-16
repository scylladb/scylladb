=========================
ScyllaDB Diagnostic Tools
=========================

ScyllaDB has a wide selection of tools and information sources available for diagnosing problems.
This document covers both built-in and standalone tools that can help you diagnose a problem with ScyllaDB.
This document focuses on enumerating the available tools and information sources, rather than providing a guide on how to diagnose generic or specific issues.

Logs
----

The most obvious source of information to find out more about why ScyllaDB is misbehaving.
On production systems, ScyllaDB logs to syslog; thus logs can usually be viewed via ``journalctl``.
See `Logging </getting-started/logging/>`_ on more information on how to access the logs.


ScyllaDB has the following log levels: ``trace``, ``debug``, ``info``, ``warn``, ``error``.
By default only logs with level ``info`` or above are logged. Some administrators might even set this to ``warn`` to reduce the amount of logs.
ScyllaDB has many different loggers, usually there is one for each subsystem or module.
You can change the log-level of a certain logger to ``debug`` or ``trace``, to get more visibility into the respective subsystem.
This can be done in one of the following ways:

* configuration file (``scylla.yaml``):

  .. code-block:: yaml

    logger_log_level:
        mylogger: debug

* command line:

  .. code-block:: shell

    --logger-log-level mylogger=debug

* nodetool:

  .. code-block:: shell

    $ nodetool setlogginglevel mylogger debug

* REST API:

  .. code-block:: shell

    $ scylla-api-client system logger/{name} POST --name mylogger --level debug

The first two methods require a restart, the latter two work at runtime.
Note that setting the log-level of even a single logger to ``debug`` or below might generate a huge amount of log traffic.
Try to time the log-level bump to when an event of interest start and revert it quickly afterward to avoid saturating your log.

Monitoring
----------

ScyllaDB has a comprehensive monitoring and alerting solution, displaying the different counters from ScyllaDB and the underlying OS, as well as alerts for common problems and pitfalls.
Note that by default monitoring shows information about the entire cluster and even when selecting a certain node, it aggregates the counters from the node's shards.
Sometimes counter values aggregated over all the shards or even over multiple or all nodes is what you want.
Just be aware of the aggregation and know that you can always select the nodes and shards of interest, or display counters by node and shard (disable aggregation).
See `ScyllaDB Monitoring Stack <https://monitoring.docs.scylladb.com/stable/>`_ for more details.

Tracing
-------

Tracing allows you to retrieve the internal log of events happening in the context of a single query.
Therefore, tracing is only useful to diagnose problems related to a certain query and cannot be used to diagnose generic problems.
That said, when it comes to diagnosing problems with a certain query, tracing is an excellent tool, allowing you to have a peek at what happens when that query is processed, including the timestamp of each event.
For more details, see `Tracing </using-scylla/tracing>`_.

Nodetool
--------

Although ``nodetool`` is primarily an administration tool, it has various commands that retrieve and display useful information about the state of a certain ScyllaDB node.
Look for commands with "stats", "info", "describe", "get", "histogram" in their names.
For a comprehensive list of all available nodetool commands, see the `Nodetool Reference </operating-scylla/nodetool>`_.

REST API
--------

ScyllaDB has a REST API which is a superset of all ``nodetool`` commands, in the sense that it is the backend serving all of them.
It has many more endpoints, many of which can supply valuable information about the internal state of ScyllaDB.
For more information, see `REST API </operating-scylla/rest>`_.

System Tables
-------------

ScyllaDB has various internal system tables containing valuable information on its state.
Some of these are virtual tables, tables whose content is derived from in-memory state, rather than on-disk storage as is the case for regular tables. Virtual tables look like and act like regular tables.
For a complete list of all internal tables (including virtual ones), see `System Keyspace <https://github.com/scylladb/scylladb/blob/master/docs/dev/system_keyspace.md>`_.

Other Tools
-----------

ScyllaDB has various other tools, mainly to work with sstables.
If you are diagnosing a problem that is related to sstables misbehaving or being corrupt, you may find these useful:

* `sstabledump </operating-scylla/admin-tools/sstabledump/>`_
* `Scylla SStable </operating-scylla/admin-tools/scylla-sstable/>`_
* `Scylla Types </operating-scylla/admin-tools/scylla-types/>`_

GDB
---

The ultimate tool to extract any information from live ScyllaDB processes or coredumps.
However, it requires intimate knowledge of ScyllaDB internals to be useful.
For more details on how to debug scylla, see `Debugging <https://github.com/scylladb/scylladb/blob/master/docs/dev/debugging.md>`_.
