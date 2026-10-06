======================================
Integrate ScyllaDB with LibreDB Studio
======================================

`LibreDB Studio <https://github.com/libredb/libredb-studio>`_ is an open source (MIT) web-based SQL IDE.
It runs as a container image (``ghcr.io/libredb/libredb-studio:latest``), through ``npx @libredb/studio``, or from a Helm chart.

LibreDB Studio connects to ScyllaDB through its Apache Cassandra connection type, thanks to ScyllaDB's compatibility with
Cassandra at the CQL binary protocol level. The connection uses the DataStax Node.js driver (``cassandra-driver``).

Connecting to ScyllaDB
----------------------

In the LibreDB Studio connection dialog, choose **Apache Cassandra** and fill in:

* The host and the port (9042 by default).
* **Local Data Center**, which the driver requires: the data center name that ``nodetool status`` shows, for example ``datacenter1``
  on a default single-node container.
* **Keyspace**, optional, to pin the session to one keyspace.
* A username and password, if authentication is enabled on the cluster.

Then select **Test Connection** and **Establish Connection**.

Tested features
---------------

The LibreDB Studio project tested this integration against ScyllaDB 2026.2.4 on a single-node container, with
Apache Cassandra 5.0.9 as the baseline in the same pass. The CQL editor and the object browser were also checked
against ScyllaDB 2025.1.14.

* The CQL editor runs statements and returns a result grid. Eighteen CQL types, including ``bigint``, ``varint``,
  ``decimal``, ``duration``, ``blob``, ``inet``, ``date``, ``time``, ``uuid``, ``timestamp`` and the collection types,
  read back identically to the Cassandra baseline.
* The object browser lists keyspaces and, for each keyspace, its tables, materialized views, columns and secondary
  indexes, read from ``system_schema``.
* Errors are classified the same way as on Cassandra, because LibreDB Studio reads the driver's error code rather
  than the server's message text.

For the measured details, see the LibreDB Studio
`wire-compatible engines table <https://github.com/libredb/libredb-studio/blob/main/docs/providers/README.md#wire-compatible-engines>`_
and the `Cassandra provider reference <https://github.com/libredb/libredb-studio/blob/main/docs/providers/cassandra.md>`_.
