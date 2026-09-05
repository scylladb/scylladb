Nodetool gossipinfo
===================

**gossipinfo** - Shows the gossip information for the cluster.

For example:

.. code-block:: shell

   nodetool gossipinfo

Example output

.. code-block:: shell

   generation:1525599034
   heartbeat:229478
   X1:RANGE_TOMBSTONES,LARGE_PARTITIONS,COUNTERS,DIGEST_MULTIPARTITION_READ,CORRECT_COUNTER_ORDER,SCHEMA_TABLES_V3,CORRECT_NON_COMPOUND_RANGE_TOMBSTONES
   RPC_ADDRESS:172.17.0.2
   NET_VERSION:0
   X3:3
   DC:datacenter1
  X2:cycling.race_times:0.000000;system_traces.node_slow_log_time_idx:0.000000;system_traces.node_slow_log:0.000000;system_traces.events:0.000000;system_traces.sessions_time_idx:0.000000;nba.omg:0.000000;system_traces.sessions:0.000000;nba.race_times:0.000000;
   RACK:rack1
   LOAD:1.05442e+06
   RELEASE_VERSION:3.0.8
   SCHEMA:cd38cba0-57e1-35c7-a845-ebb02070d13b
   HOST_ID:01df8fc9-ddc4-4179-ab4c-a1b2223ad84e
   STATUS:NORMAL,983672177588923514

.. include:: nodetool-index.rst
