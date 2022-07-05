.. include:: /upgrade/upgrade-enterprise/_common/gossip_generation_bug_warning.rst

.. note::

   Scylla Enterprise 2019.1.6 added a new configuration to restrict the memory usage cartesian product IN queries.
   If you are using IN in SELECT operations and hitting a *"cartesian product size ... is greater than maximum"* error, you can either update the query (recommended) or bypass the warning temporarily by adding the following parameters to *scylla.yaml*: 

   * *max_clustering_key_restrictions_per_query: 1000*
   * *max_partition_key_restrictions_per_query: 1000*

   The higher the values, the more likely you will hit an out of memory issue.

             
.. note::

   Scylla Enterprise 2019.1.8 added a new configuration to restrict the memory usage of reverse queries.
   If you are using reverse queries and hitting an error *"Aborting reverse partition read because partition ... is larger than the maximum safe size of ... for reversible partitions"* see the :doc:`reverse queries FAQ section </troubleshooting/reverse-queries>`.
