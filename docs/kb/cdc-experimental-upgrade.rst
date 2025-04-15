===============================
Upgrading from experimental CDC
===============================

If you used CDC in ScyllaDB 4.2 or earlier by enabling the experimental feature and you upgrade to 4.3, you must perform additional steps for CDC to work properly.

First, if you enabled CDC on any table (using ``with cdc = { ... }``), you should stop all writes to this table. Then disable CDC before the upgrade:

.. code-block:: none

    alter table ks.t with cdc = {'enabled': false};

.. caution::

   This will delete the CDC log table associated with this table - in this example, ``ks.t_scylla_cdc_log``.

This should work even if you already upgraded, but preferably disable CDC on all tables before the upgrade.

After disabling CDC and finishing the upgrade you can safely re-enable it.

The next step is running ``nodetool checkAndRepairCdcStreams``. Up to this point, ScyllaDB may have periodically reported the following errors in its logs:

.. code-block:: none

    cdc - Could not retrieve CDC streams with timestamp {...} upon gossip event. Reason: "std::runtime_error (Could not find CDC generation with timestamp ... in distributed system tables (current time: ...), even though some node gossiped about it.)". Action: not retrying.

After running the ``nodetool`` command the errors should not appear anymore. If they do, please open an issue.
