===========================
Scylla Open Source Features
===========================

.. toctree::
   :maxdepth: 2
   :hidden:

   Lightweight Transactions </using-scylla/lwt/>
   Global Secondary Indexes </using-scylla/secondary-indexes/>
   Local Secondary Indexes </using-scylla/local-secondary-indexes/>
   Materialized Views </using-scylla/materialized-views/>
   Counters </using-scylla/counters/>
   Change Data Capture </using-scylla/cdc/index>
   
   
.. panel-box::
  :title: Scylla Open Source 5.x Features
  :id: "getting-started"
  :class: my-panel

  The following are **new features** for Scylla Open Source 5.x:
  
  * :doc:`Strongly consistent schema management based on the Raft consensus algorithm </architecture/raft/>`. With 
    the implementation of Raft, schema changes in ScyllaDB are safe, including concurrent schema updates. 
    This feature is experimental in version 5.0 and needs to be explicitly enabled.

  * :doc:`Scylla SStable tool </operating-scylla/admin-tools/scylla-sstable/>` - An admin tool that allows you to examine the content of SStables by performing operations such as dumping the content of SStables, generating a histogram, validating the content of SStables, and more.
  * :doc:`Scylla Types tool </operating-scylla/admin-tools/scylla-types/>` - An admin tool that allows you to examine raw values obtained from SStables, logs, coredumps, etc., by printing, validating or comparing the values.
  * :doc:`Virtual Tables </operating-scylla/admin-tools/virtual-tables/>` - Tables that retrieve system-level information by generating their contents on-the-fly when queried.

		* Virtual table for configuration - ``system.config``, allows you to query and update configuration over CQL.
		* Virtual tables for exposing system-level information, such as cluster status, version-related information, etc.
		 
  * :ref:`Automatic management of tombstone garbage collection <ddl-tombstones-gc>` - 
    An automatic mechanism replaces the ``gc_grace_seconds`` option to ensure better database consistency 
    by removing a tombstone only after the range that covers the tombstone is repaired. This feature is 
    experimental in version 5.0 and needs to be explicitly enabled by configuring the ``tombstone_gc`` option.

  * :doc:`Expiration service in Scylla Alternator </alternator/compatibility>` - Support 
    for DynamoDB's TTL feature for detecting and deleting expired items in the table. This feature is experimental in version 5.0 

  See the `Release Notes <https://www.scylladb.com/product/release-notes/>`_ for more information. 

.. panel-box::
  :title: Scylla Open Source 4.x Features
  :id: "getting-started"
  :class: my-panel

  The following are **new features** for Scylla Open Source 4.x
  
  * :doc:`Lightweight Transactions (LWT) </using-scylla/lwt>` - Allows you to create and manipulate data according to a specified condition. :ref:`Lightweight Transactions  CQL <IF>` Reference.
  * :doc:`Scylla Alternator: an Amazon DynamoDB™-compatible API </using-scylla/alternator/index>`
  * :doc:`Change Data Capture (CDC) </using-scylla/cdc/index>` query the history of all changes made to the table. **GA** in Scylla 4.3. In versions prior to Scylla Open Source 4.3, CDC is an experimental feature and you need to enable it in order to use it.
      
.. panel-box::
  :title: Scylla Open Source 3.x Features
  :id: "getting-started"
  :class: my-panel
      
  The following are **new features** for Scylla Open Source 3.3

  * :doc:`Clients Table </troubleshooting/clients-table>` real-time information on CQL clients connected to the Scylla cluster.
    
  The following are **new features** for Scylla Open Source 3.2.x 
     
  * :ref:`IPv6 <ipv6_addresses>` support for client-to-node, node-to-node, Manager to node, and Monitor to node communication - Scylla now supports IPv6 Global Scope Addresses for all IPs: seeds, listen_address, broadcast_address etc. This functionality is available for Scylla Manager in Scylla Manager 2.0 and in Scylla Monitor in 3.0.
  * :ref:`Like Operator <like-operator>` - when used on ``SELECT`` statements informs Scylla that you are looking for a pattern match. The expression ‘column LIKE pattern’ yields true only if the entire column value matches the pattern.
  * :ref:`Group Results <group-by-clause>` - using the CQL ``GROUP BY`` option you can condense into a single row all selected rows that share the same values for a set of columns. 
  * :ref:`Non-Frozen UDTs <udts>` - User Defined Types that are not in a collection do not have to be frozen. UDTs in a collection must be frozen. 
  * :ref:`Auto-expanding Replication Factor <replication-strategy>` -  allows you to set a single replication factor for all Data Centers, including all existing Data Centers.
  * :ref:`Open range deletions <open-range-deletions>` -  deletes rows based on an open-ended request (>, <, >=, =<, etc.)
  
  The following are **new features** for Scylla Open Source 3.1.x
    
  * :ref:`CQL Per Partition Limit <limit-clause>` - This new per partition limit further allows you to set the number of partitions returned as a result. You can mix both row limits and per partition limits in the same CQL statement.
  * :doc:`Local Secondary Indexes </using-scylla/local-secondary-indexes>` - More efficient Secondary Index searches when the base table and index share the same partition key. 
  * :ref:`BYPASS CACHE <bypass-cache>` - This CQL command introduced in Scylla Enterprise 2019.1.1, now available in open source, informs the database that the data being read is unlikely to be read again in the near future, and also was unlikely to have been read in the near past; therefore no attempt should be made to read it from the cache or to populate the cache with the data. 
  * :doc:`Large Cell / Collection Detector </troubleshooting/large-rows-large-cells-tables>` - Makes finding large partitions easier.
  * :doc:`Nodetool toppartitions </operating-scylla/nodetool-commands/toppartitions>` - Samples cluster writes and reads and reports the most active partitions in a specified table and time frame.
  * :ref:`Row-level Repair <row-level-repair>` - :doc:`nodetool repair </operating-scylla/nodetool-commands/repair>` operation now uses row-level repair, adding additional granularity with repairs. 
  * :ref:`ALLOW FILTERING CQL Command <allow-filtering>` that allows for server side data filtering that is not based on the primary key. Introduced in 3.0, this feature was further enhanced in version 3.1.

  The following are **new features** for Scylla Open Source 3.0.x

  * :doc:`Scylla Materialized Views </using-scylla/materialized-views>` - An alternate view table for finding a partition by the value of another column. Experimental in prior versions, this feature was made production ready in Scylla Open Source 3.0.
  * :doc:`Global Secondary Indexes </using-scylla/secondary-indexes>` - A mechanism for allowing efficient searches on non-partition keys using Materialized Views. Experimental in prior versions, this feature was made production ready in Scylla Open Source 3.0.
  * :doc:`Hinted Handoff </architecture/anti-entropy/hinted-handoff>` - ensures availability and consistency
  * :doc:`SSTable 3.0 </architecture/sstable/sstable3/index>` - new SSTable format
  * :ref:`ALLOW FILTERING CQL Command <allow-filtering>` that allows for server side data filtering that is not based on the primary key. 

  More information in the `Release Notes <https://www.scylladb.com/product/release-notes/>`_.

