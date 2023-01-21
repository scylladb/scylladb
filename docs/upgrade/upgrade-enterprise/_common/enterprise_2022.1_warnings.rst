.. note:: The note is only useful when CDC is GA supported in the target ScyllaDB. Execute the following commands one node at a time, moving to the next node only **after** the upgrade procedure completed successfully.

.. warning::

   If you are using CDC and upgrading ScyllaDB 2021.1 to 2022.1, please review the API updates in :doc:`querying CDC streams </using-scylla/cdc/cdc-querying-streams>` and :doc:`CDC stream generations </using-scylla/cdc/cdc-stream-generations>`.
   In particular, you should update applications that use CDC according to :ref:`CDC Upgrade notes <scylla-4-3-to-4-4-upgrade>` **before** upgrading the cluster to 2022.1.
