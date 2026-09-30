Automatic Scrub
===============

Disks can silently corrupt data and corruption can hide in unused sstables.
ScyllaDB offers a way to periodically scrub sstables and detect corruption.
Automatic scrub checks sstables for corruption one at a time for each shard.
This operation is not read only and may rewrite sstables.
This feature only covers local sstables, object-storage sstables are skipped.

Which sstables are scrubbed
---------------------------

An sstable is considered validated if, within the scrub period, it was:

- written (this includes being created by compaction) or streamed, as file streaming validates
  component digests, or
- selected for automatic scrub and subsequently validated with
  :doc:`scrub </operating-scylla/nodetool-commands/scrub>` in either ``VALIDATE`` or ``ABORT``
  mode.

The timestamp of the last validation is stored in the scylla-metadata component as ``scrub_time``.
Manual :doc:`nodetool scrub </operating-scylla/nodetool-commands/scrub>` operations in ``VALIDATE``
mode do not update the scrub time and remain read-only.
When corruption is detected, invalid sstables are placed into
:ref:`quarantine <sstable-quarantine>`. Once quarantined, an sstable will not be eligible for
automatic scrub.

For sstables with the scylla-metadata component and component digests present, the ``VALIDATE``
mode for scrub will be used and scrub time will be updated using component rewrite. For other
sstables, scrub in ``ABORT`` mode will be used. Consequently, automatic scrub will rewrite
eligible sstables so that both the scylla-metadata component and component digests are present.
Additionally, if an sstable was not validated for at least half the scrub period, regular
compactions will verify component digests. Regardless of the scrub time, if corruption is found
in the sstable during a regular compaction, the input sstables will be quarantined.

Setting the scrub period
------------------------

Automatic scrub is controlled by the ``auto_scrub_period_hours``
:doc:`cluster configuration option </cql/cluster-config>`. To enable it for the whole cluster:

.. code-block:: cql

    ALTER CLUSTER WITH auto_scrub_period_hours = 24;

To disable it, set it to ``0`` (the default). The option can also be set for a single
keyspace or table: a value set on a ``TABLE`` overrides the one set on its ``KEYSPACE``,
which overrides the ``CLUSTER`` value. See :doc:`Cluster configuration </cql/cluster-config>`
for how scopes are resolved.

.. code-block:: cql

    ALTER KEYSPACE ks WITH auto_scrub_period_hours = 12;
    ALTER TABLE ks.tbl WITH auto_scrub_period_hours = 6;

Use ``auto_scrub_period_hours = NULL`` to remove a value and fall back to the broader scope.
