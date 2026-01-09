

Scylla Metric Update - Scylla 4.1 to 4.2
========================================

.. toctree::
   :maxdepth: 2
   :hidden:

Scylla 4.2 Dashboards are available as part of the latest |mon_root|.

The following metrics are new in Scylla 4.2
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* *scylla_compaction_manager_backlog* : Holds the sum of the compaction backlog for all tables in the system.
* *scylla_sstables_index_page_cache_bytes* : Total number of bytes cached in the index page cache
* *scylla_sstables_index_page_cache_evictions* : Total number of index page cache pages which have been evicted
* *scylla_sstables_index_page_cache_hits* : Index page cache requests which were served from cache
* *scylla_sstables_index_page_cache_misses* : Index page cache requests which had to perform I/O
* *scylla_sstables_index_page_cache_populations* : Total number of index page cache pages which were inserted into the cache
* *scylla_sstables_pi_cache_block_count* : Number of promoted index blocks currently cached
* *scylla_sstables_pi_cache_bytes* : Number of bytes currently used by cached promoted index blocks
* *scylla_sstables_pi_cache_evictions* : Number of promoted index blocks which got evicted
* *scylla_sstables_pi_cache_hits_l0* : Number of requests for promoted index block in state l0 which didn't have to go to the page cache
* *scylla_sstables_pi_cache_hits_l1* : Number of requests for promoted index block in state l1 which didn't have to go to the page cache
* *scylla_sstables_pi_cache_hits_l2* : Number of requests for promoted index block in state l2 which didn't have to go to the page cache
* *scylla_sstables_pi_cache_misses_l0* : Number of requests for promoted index block in state l0 which had to go to the page cache
* *scylla_sstables_pi_cache_misses_l1* : Number of requests for promoted index block in state l1 which had to go to the page cache
* *scylla_sstables_pi_cache_misses_l2* : Number of requests for promoted index block in state l2 which had to go to the page cache
* *scylla_sstables_pi_cache_populations* : Number of promoted index blocks which got inserted
* *scylla_storage_proxy_coordinator_cas_background* : Number of running Paxos operations still after a result was alredy returned
* *scylla_storage_proxy_coordinator_cas_foreground* : Number of running Paxos operations that did not yet produce a result
* *scylla_storage_proxy_coordinator_cas_total_operations* : Number of total paxos operations executed (reads and writes)

The following metrics are removed in Scylla 4.2
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* scylla_lsa_segments_migrated
