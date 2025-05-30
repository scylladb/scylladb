Nodetool compactionhistory
==========================

**compactionhistory** - Provides the history of compaction operations.

For example:


``nodetool compactionhistory``


Example output:

.. code-block:: yaml

   id                                     shard_id keyspace_name columnfamily_name compaction_name started_at              compacted_at            bytes_in bytes_out rows_merged   sstables_in                                                                                                                                                              sstables_out                                                                          total_tombstone_purge_attempt total_tombstone_purge_failure_due_to_overlapping_with_memtable total_tombstone_purge_failure_due_to_overlapping_with_uncompacting_sstable
   17536e70-5358-11e6-9d5f-000000000000          0 ks            test              Compact         2024-12-17T16:19:55.914 2024-12-17T16:39:55.914    11000      5583 {1: 78, 2: 6} [{generation: 2a5db691-bc8d-11ef-a5f9-bbbda77f5688, origin: memtable, size: 5468}, {generation: 28c58a60-bc8d-11ef-a5f9-bbbda77f5688, origin: memtable, size: 5532}]     [{generation: 2a5fb260-bc8d-11ef-a5f9-bbbda77f5688, origin: compaction, size: 5583}]  3                             0                                                              1
   4e164760-5354-11e6-9d5f-000000000000          1 ks            test              Compact         2024-12-17T16:19:55.913 2024-12-17T16:39:55.913    10919      5490 {1: 56, 2: 2} [{generation: 2a5db691-bc8d-11ef-ad72-bbbca77f5688, origin: memtable, size: 5433}, {generation: 28c58a60-bc8d-11ef-ad72-bbbca77f5688, origin: memtable, size: 5486}]     [{generation: 2a5f8b50-bc8d-11ef-ad72-bbbca77f5688, origin: compaction, size: 5490}]  1                             0                                                              0
   17abc7a0-5358-11e6-9710-000000000001          0 system        raft              Compact         2024-12-17T16:19:52.570 2024-12-17T16:39:52.570   102199     96181 {1: 20, 2: 3} [{generation: 285b08c0-bc8d-11ef-a5f9-bbbda77f5688, origin: memtable, size: 13683}, {generation: 2798ca30-bc8d-11ef-a5f9-bbbda77f5688, origin: compaction, size: 88516}] [{generation: 28614a50-bc8d-11ef-a5f9-bbbda77f5688, origin: compaction, size: 96181}] 1                             0                                                              0
   18537950-5358-11e6-be35-000000000002          0 system_schema columns           Compact         2024-12-17T16:19:52.563 2024-12-17T16:39:52.563    25478     19882 {1: 506}      [{generation: 28545200-bc8d-11ef-a5f9-bbbda77f5688, origin: memtable, size: 5744}, {generation: 279a02b0-bc8d-11ef-a5f9-bbbda77f5688, origin: compaction, size: 19734}]  [{generation: 286011d0-bc8d-11ef-a5f9-bbbda77f5688, origin: compaction, size: 19882}] 15                            0                                                              0
   19452c50-5358-11e6-8491-000000000003          0 system        topology          Compact         2024-12-17T16:19:52.555 2024-12-17T16:39:52.555    22197     14236 {2: 4}        [{generation: 00000000-0000-1000-0000-000000000002, origin: memtable, size: 9296}, {generation: 285ba501-bc8d-11ef-a5f9-bbbda77f5688, origin: memtable, size: 12901}]    [{generation: 285e3d10-bc8d-11ef-a5f9-bbbda77f5688, origin: compaction, size: 14236}] 0                             0                                                              0


Explanation:

==========================================================================  ======================================================================================================
Parameter                                                                   Description
==========================================================================  ======================================================================================================
id                                                                          id of the compaction
--------------------------------------------------------------------------  ------------------------------------------------------------------------------------------------------
shard_id                                                                    id of the shard the compaction was executed on
--------------------------------------------------------------------------  ------------------------------------------------------------------------------------------------------
keyspace_name                                                               keyspace name that the compaction performed on
--------------------------------------------------------------------------  ------------------------------------------------------------------------------------------------------
columnfamily_name                                                           columnfamily name that the compaction performed on
--------------------------------------------------------------------------  ------------------------------------------------------------------------------------------------------
compaction_type                                                             type of compaction, e.g. major, regular, reshard, cleanup, etc.
--------------------------------------------------------------------------  ------------------------------------------------------------------------------------------------------
started_at                                                                  timestamp when the compaction started
--------------------------------------------------------------------------  ------------------------------------------------------------------------------------------------------
compacted_at                                                                timestamp when the compaction completed
--------------------------------------------------------------------------  ------------------------------------------------------------------------------------------------------
bytes_in                                                                    SSTable size before the compaction
--------------------------------------------------------------------------  ------------------------------------------------------------------------------------------------------
bytes_out                                                                   SSTable size after the compaction
--------------------------------------------------------------------------  ------------------------------------------------------------------------------------------------------
rows_merged                                                                 histogram of merged data (takes into account clustering rows, static rows, range tombstones, etc.)
--------------------------------------------------------------------------  ------------------------------------------------------------------------------------------------------
sstables_in                                                                 list of sstables that were the input of the compaction
--------------------------------------------------------------------------  ------------------------------------------------------------------------------------------------------
sstables_out                                                                list of sstables that were the output of the compaction (usually one; with ICS/LCS it can be more)
--------------------------------------------------------------------------  ------------------------------------------------------------------------------------------------------
total_tombstone_purge_attempt                                               total amount of tombstones that were candidates for garbage-collection
--------------------------------------------------------------------------  ------------------------------------------------------------------------------------------------------
total_tombstone_purge_failure_due_to_overlapping_with_memtable              purgeable tombstone which couldn't be collected because memtable contains possibly shadowed data
--------------------------------------------------------------------------  ------------------------------------------------------------------------------------------------------
total_tombstone_purge_failure_due_to_overlapping_with_uncompacting_sstable  purgeable tombstone which couldn't be collected because other sstables contains possibly shadowed data
==========================================================================  ======================================================================================================


.. include:: nodetool-index.rst
