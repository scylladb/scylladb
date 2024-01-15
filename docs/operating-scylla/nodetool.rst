
========
Nodetool
========

.. toctree::
   :maxdepth: 1
   :hidden:

   nodetool-commands/cfhistograms
   nodetool-commands/cfstats
   nodetool-commands/checkandrepaircdcstreams
   nodetool-commands/cleanup
   nodetool-commands/clearsnapshot
   nodetool-commands/compactionhistory
   nodetool-commands/compactionstats
   nodetool-commands/compact
   nodetool-commands/decommission
   nodetool-commands/describecluster
   nodetool-commands/describering
   nodetool-commands/disableautocompaction
   nodetool-commands/disablebackup
   nodetool-commands/disablebinary
   nodetool-commands/disablegossip
   nodetool-commands/drain
   nodetool-commands/enbleautocompaction   
   nodetool-commands/enablebackup
   nodetool-commands/enablebinary
   nodetool-commands/enablegossip
   nodetool-commands/flush
   nodetool-commands/getendpoints
   nodetool-commands/gettraceprobability
   nodetool-commands/gossipinfo
   nodetool-commands/help
   nodetool-commands/info
   nodetool-commands/listsnapshots
   nodetool-commands/proxyhistograms
   nodetool-commands/rebuild
   nodetool-commands/refresh
   nodetool-commands/removenode
   nodetool-commands/repair
   nodetool-commands/resetlocalschema
   nodetool-commands/ring
   nodetool-commands/scrub
   nodetool-commands/settraceprobability
   nodetool-commands/setlogginglevel
   nodetool-commands/snapshot
   nodetool-commands/statusbackup
   nodetool-commands/statusbinary
   nodetool-commands/statusgossip
   nodetool-commands/status
   Nodetool stop compaction <nodetool-commands/stop>
   nodetool-commands/tablestats
   nodetool-commands/toppartitions
   nodetool-commands/upgradesstables
   nodetool-commands/viewbuildstatus
   nodetool-commands/version

The ``nodetool`` utility provides a simple command-line interface to the following exposed operations and attributes. Scylla’s nodetool is a fork of `the Apache Cassandra nodetool <https://cassandra.apache.org/doc/latest/tools/nodetool/nodetool.html>`_ with the same syntax and a subset of the operations.

.. _nodetool-generic-options:

Nodetool generic options
========================
All options are supported:



* ``-p <port>`` or ``--port <port>`` - Remote JMX agent port number.

* ``-pp`` or ``--print-port`` - Operate in 4.0 mode with hosts disambiguated by port number.

* ``-pw <password>`` or ``--password <password>`` - Remote JMX agent password.

* ``-pwf <passwordFilePath>`` or ``--password-file <passwordFilePath>`` - Path to the JMX password file.

* ``-u <username>`` or ``--username <username>`` - Remote JMX agent username.

* ``--`` - Separates command-line options from the list of argument(useful when an argument might be mistaken for a command-line option).

Supported Nodetool operations
=============================
Operations that are not listed below are currently not available.

* :doc:`cfhistograms <nodetool-commands/cfhistograms/>` - Provides statistics about a table, including number of SSTables, read/write latency, partition size and column count.
* :doc:`cfstats </operating-scylla/nodetool-commands/cfstats/>` - Provides in-depth diagnostics regard table.
* :doc:`checkandrepaircdcstreams </operating-scylla/nodetool-commands/checkandrepaircdcstreams/>` - Checks and fixes CDC streams.
* :doc:`cleanup </operating-scylla/nodetool-commands/cleanup/>` - Triggers the immediate cleanup of keys no longer belonging to a node.
* :doc:`clearsnapshot </operating-scylla/nodetool-commands/clearsnapshot/>` - This command removes snapshots.
* :doc:`compactionhistory </operating-scylla/nodetool-commands/compactionhistory/>` - Provides the history of compactions.
* :doc:`compactionstats </operating-scylla/nodetool-commands/compactionstats/>`- Print statistics on compactions.
* :doc:`compact </operating-scylla/nodetool-commands/compact/>`- Force a (major) compaction on one or more column families.
* :doc:`decommission </operating-scylla/nodetool-commands/decommission/>` - Decommission the node.
* :doc:`describecluster </operating-scylla/nodetool-commands/describecluster/>` - Print the name, snitch, partitioner and schema version of a cluster.
* :doc:`describering </operating-scylla/nodetool-commands/describering/>` - :code:`<keyspace>`- Shows the partition ranges of a given keyspace.
* :doc:`disableautocompaction </operating-scylla/nodetool-commands/disableautocompaction/>` - Disable automatic compaction of a keyspace or table.
* :doc:`disablebackup </operating-scylla/nodetool-commands/disablebackup/>` - Disable incremental backup.
* :doc:`disablebinary </operating-scylla/nodetool-commands/disablebinary/>` - Disable native transport (binary protocol).
* :doc:`disablegossip </operating-scylla/nodetool-commands/disablegossip/>` - Disable gossip (effectively marking the node down).
* :doc:`drain </operating-scylla/nodetool-commands/drain/>` - Drain the node (stop accepting writes and flush all column families).
* :doc:`enableautocompaction </operating-scylla/nodetool-commands/enbleautocompaction/>` - Enable automatic compaction of a keyspace or table.
* :doc:`enablebackup </operating-scylla/nodetool-commands/enablebackup/>` - Enable incremental backup.
* :doc:`enablebinary </operating-scylla/nodetool-commands/enablebinary/>` - Re-enable native transport (binary protocol).
* :doc:`enablegossip </operating-scylla/nodetool-commands/enablegossip/>` - Re-enable gossip.
* :doc:`flush </operating-scylla/nodetool-commands/flush/>` - Flush one or more column families.
* :doc:`getendpoints <nodetool-commands/getendpoints/>` :code:`<keyspace>` :code:`<table>` :code:`<key>`- Print the end points that owns the key.
* **getlogginglevels** - Get the runtime logging levels.
* :doc:`gettraceprobability </operating-scylla/nodetool-commands/gettraceprobability>` - Displays the current trace probability value. 0 is disabled 1 is enabled.
* :doc:`gossipinfo </operating-scylla/nodetool-commands/gossipinfo/>` - Shows the gossip information for the cluster.
* :doc:`help </operating-scylla/nodetool-commands/help/>` - Display list of available nodetool commands.
* :doc:`info </operating-scylla/nodetool-commands/info/>` - Print node information
* :doc:`listsnapshots </operating-scylla/nodetool-commands/listsnapshots/>` - Lists all the snapshots along with the size on disk and true size.
* **move** :code:`<new token>`- Move node on the token ring to a new token
* **netstats** - Print network information on provided host (connecting node by default)
* :doc:`proxyhistograms </operating-scylla/nodetool-commands/proxyhistograms/>` - Print statistic histograms for network operations
* :doc:`rebuild </operating-scylla/nodetool-commands/rebuild/>` :code:`[<src-dc-name>]`- Rebuild data by streaming from other nodes
* :doc:`refresh </operating-scylla/nodetool-commands/refresh/>`- Load newly placed SSTables to the system without restart
* :doc:`removenode </operating-scylla/nodetool-commands/removenode/>`- Remove node with the provided ID
* :doc:`repair <nodetool-commands/repair/>`  :code:`<keyspace>` :code:`<table>` - Repair one or more tables
* :doc:`resetlocalschema </operating-scylla/nodetool-commands/resetlocalschema/>` - Reset the node's local schema.
* :doc:`ring <nodetool-commands/ring/>` - The nodetool ring command display the token ring information.
* :doc:`scrub </operating-scylla/nodetool-commands/scrub>` :code:`[-m mode] [--no-snapshot] <keyspace> [<table>...]` - Scrub the SSTable files in the specified keyspace or table(s)
* :doc:`setlogginglevel</operating-scylla/nodetool-commands/setlogginglevel>` - sets the logging level threshold for Scylla classes
* :doc:`settraceprobability </operating-scylla/nodetool-commands/settraceprobability/>` ``<value>`` - Sets the probability for tracing a request. race probability value
* :doc:`snapshot </operating-scylla/nodetool-commands/snapshot>` :code:`[-t tag] [-cf column_family] <keyspace>`  - Take a snapshot of specified keyspaces or a snapshot of the specified table.
* :doc:`statusbackup </operating-scylla/nodetool-commands/statusbackup/>` - Status of incremental backup.
* :doc:`statusbinary </operating-scylla/nodetool-commands/statusbinary/>` - Status of native transport (binary protocol).
* :doc:`statusgossip </operating-scylla/nodetool-commands/statusgossip/>` - Status of gossip.
* :doc:`status </operating-scylla/nodetool-commands/status/>` - Print cluster information.
* :doc:`stop </operating-scylla/nodetool-commands/stop/>` - Stop compaction operation.
* **tablehistograms** see :doc:`cfhistograms <nodetool-commands/cfhistograms/>`
* :doc:`tablestats </operating-scylla/nodetool-commands/tablestats/>` - Provides in-depth diagnostics regard table. 
* :doc:`toppartitions </operating-scylla/nodetool-commands/toppartitions/>` - Samples cluster writes and reads and reports the most active partitions in a specified table and time frame.
* :doc:`upgradesstables </operating-scylla/nodetool-commands/upgradesstables>` - Upgrades each table that is not running the latest Scylla version, by rewriting SSTables.
* :doc:`viewbuildstatus </operating-scylla/nodetool-commands/viewbuildstatus/>` - Shows the progress of a materialized view build.
* :doc:`version </operating-scylla/nodetool-commands/version>` - Print the DB version.

.. include:: /rst_include/apache-copyrights.rst
