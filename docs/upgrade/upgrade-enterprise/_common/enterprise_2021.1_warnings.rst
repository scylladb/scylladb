.. note:: The note is only useful when CDC is GA supported in the target Scylla. Execute the following commands one node at the time, moving to the next node only **after** the upgrade procedure completed successfully.

.. warning::

   If you are using CDC and upgrading Scylla 2020.1 to 2021.1, please review the API updates in :doc:`querying CDC streams </using-scylla/cdc/cdc-querying-streams>` and :doc:`CDC stream generations </using-scylla/cdc/cdc-stream-generations>`.
   In particular, you should update applications that use CDC according to :ref:`CDC Upgrade notes <scylla-4-3-to-4-4-upgrade>` **before** upgrading the cluster to 2021.1.

   If you are using CDC and upgrading from pre 2020.1 version to 2020.1, note the :doc:`upgrading from experimental CDC </kb/cdc-experimental-upgrade>`.

.. include:: /upgrade/upgrade-enterprise/_common/mv_si_rebuild_warning.rst