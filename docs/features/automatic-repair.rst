.. _automatic-repair:

Automatic Repair
================

Traditionally, launching `repairs </operating-scylla/procedures/maintenance/repair>`_ in a ScyllaDB cluster is left to an external process, typically done via `Scylla Manager <https://manager.docs.scylladb.com/stable/repair/index.html>`_.

Automatic repair offers built-in scheduling in ScyllaDB itself. If the time since the last repair is greater than the configured repair interval, ScyllaDB will start a repair for the tablet `tablet </architecture/tablets>`_ automatically.
Repairs are spread over time and among nodes and shards, to avoid load spikes or any adverse effects on user workloads.

To enable automatic repair, add this to the configuration (``scylla.yaml``):

.. code-block:: yaml

    auto_repair_enabled_default: true
    auto_repair_threshold_default_in_seconds: 86400

This will enable automatic repair for all tables with a repair period of 1 day. This configuration has to be set on each node, to an identical value.
More featureful configuration methods will be implemented in the future.

To disable, set ``auto_repair_enabled_default: false``.

Automatic repair relies on `Incremental Repair </features/incremental-repair>`_ and as such it only works with `tablet </architecture/tablets>`_ tables.
