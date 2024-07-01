.. note:: Execute the following commands one node at the time, moving to the next node only **after** the upgrade procedure completed successfully.

.. warning::

   If you are using CDC and upgrading ScyllaDB 4.3 to 4.4, please review the API updates in :doc:`querying CDC streams </using-scylla/cdc/cdc-querying-streams>` and :doc:`CDC stream generations </using-scylla/cdc/cdc-stream-generations>`.
   In particular, you should update applications that use CDC according to :ref:`CDC Upgrade notes <scylla-4-3-to-4-4-upgrade>` **before** upgrading the cluster to 4.4.

   If you are using CDC and upgrading from pre 4.3 version to 4.3, note the :doc:`upgrading from experimental CDC </kb/cdc-experimental-upgrade>`.

