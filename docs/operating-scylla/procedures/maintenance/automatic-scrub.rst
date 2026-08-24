Automatic Scrub
===============

Disks can silently corrupt data and corruption can hide in unused sstables.
ScyllaDB offers a way to periodically scrub sstables and detect corruption.

Automatic scrub validates sstables one at a time for each shard. As the validation time is stored inside the sstable, this operation is not read only and may rewrite sstables.
When corruption is detected, invalid sstables are placed into quarantine. Quarantined sstables are excluded from automatic scrub.

For sstables with the scylla-metadata component and component digests present,
the validate mode for scrub will be used and scrub time will be updated using
component rewrite.

For other sstables, scrub in abort mode will be used. Consequently,
automatic scrub will rewrite eligible sstables so that both the scylla-metadata
component and component digests are present.

Additionally, if an sstable was not validated for at least half the scrub
period, regular compactions will verify component digests.

This feature only covers local sstables, object-storage sstables are skipped.

To enable automatic scrub, add this to the configuration (``scylla.yaml``):

.. code-block:: yaml

    auto_scrub_period_seconds: 86400

To disable, set ``auto_scrub_period_seconds: 0`` (default).
