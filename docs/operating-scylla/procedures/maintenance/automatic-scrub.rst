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
It is only present in sstables written or rewritten by ScyllaDB versions which include automatic scrub.
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
in the sstable during a regular compaction, the malformed input sstable will be quarantined.

.. caution::

   Sstables without a ``scrub_time`` value are considered not validated. Additionally, if they
   lack component digests, they will be rewritten by scrub in ``ABORT`` mode.
   Consequently, enabling automatic scrub for the first time may cause a large number of
   sstable rewrites.

Pacing
------

Automatic scrub is paced: the ``auto_scrub_io_throughput_mb_per_sec_per_shard``
:doc:`cluster configuration option </cql/cluster-config>` limits the average rate at which each
shard goes over its sstables. The default is 10 MiB/s per shard. To change it:

.. code-block:: cql

    ALTER CLUSTER WITH auto_scrub_io_throughput_mb_per_sec_per_shard = 20;

Use ``0`` to disable the limit. The option can only be set for the whole cluster.

The limit applies to the average, not to the scrub of a single sstable: an sstable is scrubbed
at full speed, so that it becomes available to compaction again as soon as possible. Automatic
scrub works in bursts: once an hour, the shard picks the sstables which were validated the
longest time ago, about as many as the limit allows for an hour (36 GiB with the default limit),
and scrubs them one after the other. An sstable is always scrubbed whole: after scrubbing more
than the limit allows for an hour, the shard skips the following hours to make up for it.

Pacing spreads out the work when many sstables are due at the same time, most notably when
automatic scrub is enabled on a table which already has data, or after an upgrade from a version
which doesn't store the ``scrub_time``. It also gives regular compaction time to replace such
sstables before automatic scrub gets to them.

The limit also bounds how much data a shard can validate in a scrub period: with the default
limit, about 0.82 TiB per day, or 2.5 TiB in 72 hours. A shard which holds more data than that
validates its sstables less often than the scrub period asks for; raise the limit or the scrub
period in this case.

Setting the scrub period
------------------------

Automatic scrub is controlled by the ``auto_scrub_period_seconds``
:doc:`cluster configuration option </cql/cluster-config>`. To enable it for the whole cluster:

.. code-block:: cql

    ALTER CLUSTER WITH auto_scrub_period_seconds = 86400;

The period is given in seconds and must be at least ``3600`` (one hour). To disable it,
set it to ``0`` (the default). The option can also be set for a single
keyspace or table: a value set on a ``TABLE`` overrides the one set on its ``KEYSPACE``,
which overrides the ``CLUSTER`` value. See :doc:`Cluster configuration </cql/cluster-config>`
for how scopes are resolved.

.. code-block:: cql

    ALTER KEYSPACE ks WITH auto_scrub_period_seconds = 43200;
    ALTER TABLE ks.tbl WITH auto_scrub_period_seconds = 21600;

Use ``auto_scrub_period_seconds = 0`` to disable automatic scrub even if a broader scope enabled it.
Use ``auto_scrub_period_seconds = NULL`` to remove a value and fall back to the broader scope.
