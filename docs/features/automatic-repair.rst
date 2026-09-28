.. _automatic-repair:

Automatic Repair
================

Traditionally, launching :doc:`repairs </operating-scylla/procedures/maintenance/repair>` in a ScyllaDB cluster is left to an external process, typically done via `Scylla Manager <https://manager.docs.scylladb.com/stable/repair/index.html>`_.

Automatic repair offers built-in scheduling in ScyllaDB itself. If the time since the last repair is greater than the configured repair interval, ScyllaDB will start a repair for the :doc:`tablet table </architecture/tablets>` automatically.
Repairs are spread over time and among nodes and shards, to avoid load spikes or any adverse effects on user workloads.

Configuring with CQL
--------------------

Automatic repair is controlled by two :doc:`cluster configuration options </cql/cluster-config>`:

* ``auto_repair_enabled`` (boolean): whether tablets of a table are scheduled for automatic repair.
* ``auto_repair_threshold_in_seconds`` (integer, seconds): a tablet becomes eligible for automatic repair when this
  much time has passed since its last repair. ``0`` disables time-based automatic repair at that scope.

Both can be set for the whole cluster, for a keyspace, or for a single table. A value set for a table
overrides the keyspace value, which overrides the cluster value:

.. code-block:: cql

   ALTER CLUSTER WITH auto_repair_enabled = true;
   ALTER CLUSTER WITH auto_repair_threshold_in_seconds = 86400;
   ALTER KEYSPACE ks WITH auto_repair_enabled = false;
   ALTER TABLE ks.important WITH auto_repair_enabled = true AND auto_repair_threshold_in_seconds = 3600;

This enables automatic repair for every tablet table with a repair period of 1 day, turns it off for
keyspace ``ks``, and turns it back on for ``ks.important`` with a repair period of 1 hour. The setting is
stored in the cluster schema, so it applies to every node without editing per-node configuration files.

Setting an option to ``NULL`` removes it from that scope, so the next broader scope applies again:

.. code-block:: cql

   ALTER TABLE ks.important WITH auto_repair_threshold_in_seconds = NULL;

When an option is set at any scope, ``DESCRIBE TABLE`` and ``DESCRIBE KEYSPACE`` show its effective
value and which scope it comes from.

Deprecated scylla.yaml options
------------------------------

Earlier releases configured automatic repair through two options in ``scylla.yaml``:

.. code-block:: yaml

    auto_repair_enabled_default: true
    auto_repair_threshold_default_in_seconds: 86400

These options are deprecated and will be removed in a future release. They still apply when no
scope sets the corresponding CQL option, and a node that has them set logs a warning at startup.
A value set with ``ALTER CLUSTER``, ``ALTER KEYSPACE`` or ``ALTER TABLE`` always takes precedence
over them. While the value comes from ``scylla.yaml``, ``DESCRIBE`` shows nothing for the option;
query ``system.config`` on each node to see it.

To migrate, run the equivalent statements once from any node and then remove the options from
``scylla.yaml`` on every node:

.. code-block:: cql

   ALTER CLUSTER WITH auto_repair_enabled = true;
   ALTER CLUSTER WITH auto_repair_threshold_in_seconds = 86400;

To disable automatic repair everywhere, run ``ALTER CLUSTER WITH auto_repair_enabled = false``.

Automatic repair relies on :doc:`Incremental Repair </features/incremental-repair>` and as such it only works with :doc:`tablet </architecture/tablets>` tables.
