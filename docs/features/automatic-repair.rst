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

Size-based trigger
------------------

A tablet can also be repaired before its repair interval elapses, once enough of its data has
accumulated without being repaired.

The main reason to do so is read amplification. :doc:`Incremental Repair
</features/incremental-repair>` keeps a tablet's repaired and unrepaired SSTables apart, and
the two sets are never compacted together, so a read has to merge across both. The more
unrepaired data a tablet accumulates relative to what has already been repaired, the more
SSTables a read touches. Repairing the tablet lets the two sets be compacted into one again.

This matters most for overwrite-heavy and delete-heavy workloads, where the separation also
holds back compaction itself: a newer version of a row, or a tombstone, cannot be compacted
against the older data it supersedes while the two sit on opposite sides of the split. Setting a
size-based threshold is recommended for such workloads. It also helps when a table takes writes
unevenly, since a tablet that has just absorbed a large amount of new data is repaired promptly
rather than waiting out the full interval.

The trigger is off by default and is enabled by setting a fraction, using the same per table,
per keyspace or cluster-wide forms as ``auto_repair_enabled``:

.. code-block:: cql

    ALTER TABLE ks.tbl WITH auto_repair_threshold_size_fraction = 0.1;

ScyllaDB then repairs a tablet once the average unrepaired size of its replicas reaches that
fraction of their average total size — in the example above, once 10% of the tablet is
unrepaired. Setting the fraction back to ``0`` disables the trigger, leaving only the repair
interval.

A second option guards tablets that hold very little data, so that a nearly empty tablet is not
repaired over and over for a handful of bytes:

.. code-block:: cql

    ALTER TABLE ks.tbl WITH auto_repair_threshold_min_size_in_bytes = 10485760;

Below this much unrepaired data per tablet the fraction is not consulted at all, and the tablet
waits for the repair interval or for a manual repair. It defaults to 10 MiB, as in the example
above.

The two options combine with the repair interval in the usual way, so a cluster-wide interval
can stand while one table is repaired on size alone:

.. code-block:: cql

    ALTER CLUSTER WITH auto_repair_threshold_in_seconds = 86400;
    ALTER TABLE ks.tbl WITH auto_repair_threshold_in_seconds = 0
                        AND auto_repair_threshold_size_fraction = 0.1;

The two triggers are independent, and whichever condition is met first starts the repair.

Repairing a tablet resets the repair interval for it, whichever trigger started the repair —
including one requested through the API. The next time-based repair of that tablet is
therefore due one interval after the last repair *began*, not one interval after the last
time-based one. A repair restricted to particular replicas with ``hosts_filter`` or
``dcs_filter`` is the exception: it leaves the interval untouched, since it did not repair
every replica.

Both options become settable once the cluster enables the ``CLUSTER_CONFIG_REGISTRY_V1``
feature, and the trigger additionally needs every replica of a tablet to report its unrepaired
size, which requires ``TABLET_UNREPAIRED_LOAD_STATS``. While a cluster is being upgraded,
tablets with a replica on a node that does not yet support it are repaired on the repair
interval alone. The trigger is also inactive where Incremental Repair is not available, since
repair would not then mark any data as repaired.

Automatic repair relies on :doc:`Incremental Repair </features/incremental-repair>` and as such it only works with :doc:`tablet </architecture/tablets>` tables.
