============
CQL Commands
============

.. toctree::
   :maxdepth: 1
   :hidden:

   commands/toppartitions

ScyllaDB runs built-in administrative commands over CQL with the ``EXECUTE COMMAND``
statement. The syntax follows Cassandra's CEP-38 (CQL Management API).

``EXECUTE COMMAND``
-------------------

.. code-block:: cql

   EXECUTE COMMAND <name> [WITH <parameter> = <value> AND ...];

Parameters are named and can be given in any order. Values are ordinary terms, so they can
be bind markers in a prepared statement or function calls. A command returns its result as rows.

Cluster-wide execution is planned; until then the ``local = true`` parameter is mandatory: it
runs the command on the node that receives the statement. The ``host_id`` parameter is reserved
for selecting the node to run on and is currently rejected.

Commands are superuser-only unless a command's page says otherwise, so they need authentication
and authorization enabled: with the default ``AllowAllAuthenticator`` every session is anonymous
and gets ``Unauthorized``.

A running command ends early, with an error, when the client closes its connection or the node
shuts down.

``DESCRIBE COMMANDS``
---------------------

``DESCRIBE COMMANDS`` lists every command parameter, one row per parameter, with its CQL type,
whether it is mandatory, and a usage line (``create_statement``), which is what cqlsh prints.
If a keyspace named ``commands`` exists, or an object named ``commands`` exists in the current
keyspace, ``DESCRIBE COMMANDS`` describes that object instead, as it did before.

Commands
--------

* :doc:`toppartitions </cql/commands/toppartitions>`: samples the hottest partitions, like ``nodetool toppartitions``.
