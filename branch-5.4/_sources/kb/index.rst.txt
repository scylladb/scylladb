Knowledge Base
==============

.. toctree::
   :maxdepth: 2
   :hidden:
   :glob:

   /kb/*
   
.. panel-box::
  :title: Planning and Setup
  :id: "getting-started"
  :class: my-panel 

  * :doc:`Scylla Seed Nodes </kb/seed-nodes>` - Introduction on the purpose and role of Seed Nodes in Scylla as well as configuration tips.
  * :doc:`Compaction </kb/compaction>` - To free up disk space and speed up reads, Scylla must do compaction operations.
  * :doc:`DPDK mode </kb/dpdk-hardware>` - Learn to select and configure networking for DPDK mode
  * :doc:`POSIX networking for Scylla </kb/posix>` - Scylla's POSIX mode works on all physical and virtual network devices and is useful for development work.
  * :doc:`System Limits </kb/system-limits>` - Outlines the system limits which should be set or removed
  * :doc:`Run Scylla as a custom user:group </kb/custom-user>` - Configure the Scylla and supporting services to run as a custom user:group.
  * :doc:`How to Set up a Swap Space Using a File </kb/set-up-swap>` - Outlines the steps you need to take to set up a swap space.


.. panel-box::
  :title: Scylla under the hood
  :id: "getting-started"
  :class: my-panel 

  * :doc:`Gossip in Scylla </kb/gossip>` - Scylla, like Cassandra, uses a type of protocol called “gossip” to exchange metadata about the identities of nodes in a cluster. Here's how it works behind the scenes.
  * :doc:`Scylla consistency quiz for administrators </kb/quiz-administrators>` - How much do you know about NoSQL, from the administrator point of view?
  * :doc:`Scylla Memory Usage </kb/memory-usage>` - Short explanation how Scylla manages memory
  * :doc:`Scylla Nodes are Unresponsive </kb/unresponsive-nodes>` - How to handle swap in Scylla
  * :doc:`CQL Query Does Not Display Entire Result Set </kb/cqlsh-more>` - What to do when a CQL query doesn't display the entire result set.
  * :doc:`Snapshots and Disk Utilization </kb/disk-utilization>` - How snapshots affect disk utilization
  * :doc:`Scylla Snapshots </kb/snapshots>` - What Scylla snapshots are, what they are used for, and how they get created and removed.
  * :doc:`How does Scylla LWT Differ from Apache Cassandra ? </kb/lwt-differences>` - How does Scylla's implementation of lightweight transactions differ from Apache Cassandra?
  * :doc:`If a query does not reveal enough results </kb/cqlsh-results>`
  * :doc:`How to Change gc_grace_seconds for a Table </kb/gc-grace-seconds>` - How to change the ``gc_grace_seconds`` parameter and prevent data resurrection.
  * :doc:`How to flush old tombstones from a table </kb/tombstones-flush>` - How to remove old tombstones from SSTables.
  * :doc:`Increase Cache to Avoid Non-paged Queries </kb/increase-permission-cache>` - How to increase the ``permissions_cache_max_entries`` setting.
  * :doc:`How to Safely Increase the Replication Factor </kb/rf-increase>`
  * :doc:`Facts about TTL, Compaction, and gc_grace_seconds <ttl-facts>`

  **Note**: The KB article for social readers has been *removed*. Instead, please look at lessons on `Scylla University <https://university.scylladb.com/>`_ or the `Care Pet example <https://care-pet.docs.scylladb.com/master/>`_


.. panel-box::
  :title: Configuring and Integrating Scylla
  :id: "getting-started"
  :class: my-panel

  * :doc:`NTP configuration for Scylla </kb/ntp>` - Scylla depends on an accurate system clock. Learn to configure NTP for your data store and applications.
  * :doc:`Scylla and Spark integration </kb/scylla-and-spark-integration>` - How to run an example Spark application that uses Scylla to store data?
  * :doc:`Map CPUs to Scylla Shards </kb/map-cpu>` - Mapping between CPUs and Scylla shards
  * :doc:`Customizing CPUSET </kb/customizing-cpuset>`
  * :doc:`Recreate RAID devices </kb/raid-device>` - How to recreate your RAID devices without running scylla-setup
  * :doc:`Configure Scylla Networking with Multiple NIC/IP Combinations </kb/yaml-address>` - examples for setting the different IP addresses in scylla.yaml
  * :doc:`Updating the Mode in perftune.yaml After a ScyllaDB Upgrade </kb/perftune-modes-sync>`
  * :doc:`Kafka Sink Connector Quickstart </using-scylla/integrations/kafka-connector>`
  * :doc:`Kafka Sink Connector Configuration </using-scylla/integrations/sink-config>`


.. panel-box::
  :title: Analyzing Scylla
  :id: "getting-started"
  :class: my-panel 
   
  * :doc:`Using the perf utility with Scylla </kb/use-perf>` - Using the perf utility to analyze Scylla
  * :doc:`Debug your database with Flame Graphs </kb/flamegraph>` - How to setup and run a Flame Graph
  * :doc:`Decoding Stack Traces </kb/decode-stack-trace>` - How to decode stack traces in Scylla Logs
  * :doc:`Counting all rows in a table </kb/count-all-rows>` - Why counting all rows in a table often leads to a timeout












