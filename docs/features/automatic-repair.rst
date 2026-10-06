.. _automatic-repair:

Automatic Repair
================

Traditionally, launching :doc:`repairs </operating-scylla/procedures/maintenance/repair>` in a ScyllaDB cluster is left to an external process, typically done via `Scylla Manager <https://manager.docs.scylladb.com/stable/repair/index.html>`_.

Automatic repair offers built-in scheduling in ScyllaDB itself. If the time since the last repair is greater than the configured repair interval, ScyllaDB will start a repair for the :doc:`tablet table </architecture/tablets>` automatically.
Repairs are spread over time and among nodes and shards, to avoid load spikes or any adverse effects on user workloads.

Enabling automatic repair
-------------------------

Whether automatic repair runs for a table is controlled by the ``auto_repair_enabled`` cluster
configuration option, which can be set per table, per keyspace, or for the whole cluster:

.. code-block:: cql

    ALTER CLUSTER WITH auto_repair_enabled = true;
    ALTER KEYSPACE ks WITH auto_repair_enabled = false;
    ALTER TABLE ks.tbl WITH auto_repair_enabled = true;

The most specific setting wins: a table-level value overrides the keyspace it belongs to, which in
turn overrides the cluster-wide value. Setting a level to ``null`` removes its value, so that the
next level up applies again.

When no level sets the option, the value of ``auto_repair_enabled_default`` in ``scylla.yaml``
applies:

.. code-block:: yaml

    auto_repair_enabled_default: true

Because this is the last-resort default rather than part of the override chain, it has to be set on
each node, to an identical value. Prefer ``ALTER CLUSTER`` to set a cluster-wide value.

Repair interval
---------------

The repair period is configured in ``scylla.yaml``, on each node, to an identical value:

.. code-block:: yaml

    auto_repair_threshold_default_in_seconds: 86400

The example above gives a repair period of 1 day.

Automatic repair relies on :doc:`Incremental Repair </features/incremental-repair>` and as such it only works with :doc:`tablet </architecture/tablets>` tables.
