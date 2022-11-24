===========================
Scylla Enterprise Features
===========================

.. toctree::
   :maxdepth: 2
   :hidden:

   Workload Prioritization </using-scylla/workload-prioritization/>
   In-memory tables </using-scylla/in-memory/>
   Global Secondary Indexes </using-scylla/secondary-indexes/>
   Local Secondary Indexes </using-scylla/local-secondary-indexes/>
   Materialized Views </using-scylla/materialized-views/>
   Counters </using-scylla/counters/>
   Change Data Capture </using-scylla/cdc/index>
   LDAP Authentication </operating-scylla/security/ldap-authentication>
   LDAP Authorization </operating-scylla/security/ldap-authorization>


.. panel-box::
  :title: Coming Soon to Scylla Enterprise
  :id: "getting-started"
  :class: my-panel

  The following features are scheduled for an **upcoming release** of Scylla Enterprise. To see which release, read the `Release Notes <https://www.scylladb.com/product/release-notes/>`_.

  * :doc:`LDAP Role Management </operating-scylla/security/ldap-authorization>`
  * :doc:`LDAP Authentication </operating-scylla/security/ldap-authentication>`

.. panel-box::
  :title: Scylla Enterprise 2022.1 Features
  :id: "getting-started"
  :class: my-panel

  The following are **new features** in ScyllaDB Enterprise 2022.1.x:

  * :doc:`Virtual Tables </operating-scylla/admin-tools/virtual-tables/>` - Tables that retrieve system-level information by generating their contents on-the-fly when queried.

		* Virtual table for configuration - ``system.config``, allows you to query and update configuration over CQL.
		* Virtual tables for exposing system-level information, such as cluster status, version-related information, etc.
	
  See the `Release Notes <https://www.scylladb.com/product/release-notes/>`_ for more information. 

.. panel-box::
  :title: Scylla Enterprise 2021.1 Features
  :id: "getting-started"
  :class: my-panel

  The following are **new features** for Scylla Scylla Enterprise 2021.1.x:

  * :ref:`Space Amplification Goal (SAG) <SAG>` for ICS -  new CQL option to set a Space Amplification Goal (SAG) in Incremental Compaction Strategy (ICS).
  * Scylla Unified Installer - Scylla is now available as an all-in-one binary tar file. You can download the tar file from the `Scylla Download Center <https://www.scylladb.com/download/?platform=tar>`_.
  * :doc:`Change Data Capture (CDC) </using-scylla/cdc/index>` query the history of all changes made to the table. From *Scylla Enterprise 2021.1.1*


  Read the `Release Notes <https://www.scylladb.com/product/release-notes/>`_ for more information.

.. panel-box::
  :title: Scylla Enterprise 2020.1 Features
  :id: "getting-started"
  :class: my-panel

  The following are **new features** for Scylla Scylla Enterprise 2020.1.x:
  
  * :doc:`Lightweight Transactions (LWT) </using-scylla/lwt>` - Allows you to create and manipulate data according to a specified condition. :ref:`Lightweight Transactions  CQL <IF>` Reference.
  * :doc:`Scylla Alternator: an Amazon DynamoDB™-compatible API </using-scylla/alternator/index>`
  * :ref:`Group Results <group-by-clause>` - using the CQL ``GROUP BY`` option you can condense into a single row all selected rows that share the same values for a set of columns. 
  * :ref:`Like Operator <like-operator>` - when used on ``SELECT`` statements informs Scylla that you are looking for a pattern match. The expression ‘column LIKE pattern’ yields true only if the entire column value matches the pattern.
  * :ref:`Open range deletions <open-range-deletions>` -  deletes rows based on an open-ended request (>, <, >=, =<, etc.)
  * :ref:`Auto-expanding Replication Factor <replication-strategy>` -  allows you to set a single replication factor for all Data Centers, including all existing Data Centers.
  * :ref:`Non-Frozen UDTs <udts>` - User Defined Types that are not in a collection do not have to be frozen. UDTs in a collection must be frozen. 
  * :ref:`CQL Per Partition Limit <limit-clause>` - This new per partition limit further allows you to set the number of partitions returned as a result. You can mix both row limits and per partition limits in the same CQL statement.
  * :ref:`BYPASS CACHE <bypass-cache>` - This CQL command introduced in Scylla Enterprise 2019.1.1, now available in open source, informs the database that the data being read is unlikely to be read again in the near future, and also was unlikely to have been read in the near past; therefore no attempt should be made to read it from the cache or to populate the cache with the data.     
  
  Read the `Release Notes <https://www.scylladb.com/product/release-notes/>`_ for more information.

.. panel-box::
  :title: Scylla Enterprise 2019.1 Features
  :id: "getting-started"
  :class: my-panel

  The following are **new features** for Scylla Scylla Enterprise 2019.1.x:

  * :ref:`Incremental Compaction Strategy <incremental-compaction-strategy-ics>` - (version 2019.1.4) - significantly lowers SA (size amplification) for workloads which run STCS and should be used instead of STCS.
  * :ref:`IPv6 <ipv6_addresses>` (version 2019.1.4) support for client-to-node, node-to-node, Manager to node, and Monitoring to node communication - Scylla now supports IPv6 Global Scope Addresses for all IPs: seeds, listen_address, broadcast_address etc. This functionality is available for Scylla Manager in Scylla Manager 2.0.
  * :doc:`Workload Prioritization </using-scylla/workload-prioritization>` - Grant a level of service to roles in your organization. 
  * :doc:`Scylla Materialized Views </using-scylla/materialized-views>` - An alternate view table for finding a partition by the value of another column.
  * :doc:`Global Secondary Indexes </using-scylla/secondary-indexes>` - A mechanism for allowing efficient searches on non-partition keys using Materialized Views.
  * :doc:`Local Secondary Indexes </using-scylla/local-secondary-indexes>` - More efficient Secondary Index searches when the base table and index share the same partition key.
  * :ref:`ALLOW FILTERING CQL Command <allow-filtering>` that allows for server side data filtering that is not based on the primary key.
  * :doc:`Hinted Handoff </architecture/anti-entropy/hinted-handoff>` - ensures availability and consistency
  * :doc:`SSTable 3.0 </architecture/sstable/sstable3/index>` - new SSTable format
  * `Full (multi-partition) <https://www.scylladb.com/2018/11/01/more-efficient-range-scan-paging-with-scylla-3-0/>`_ blog describing improvements for fulll scans.
  * :doc:`Role Based Access Control (RBAC) </operating-scylla/security/rbac-usecase>` - compatible with Apache Cassandra 3.x using CQL commands to :ref:`grant roles <cql-security>` to users in an organization. 
  * :ref:`GoogleCloudSnitch <googlecloudsnitch>`- optimized for use with GCE instances
  * `Large Partitions Support <https://www.scylladb.com/2018/09/11/large-partitions-support-scylla-2-3/>`_ - Scylla supports large partitions. 
  * :doc:`Encryption at Rest </operating-scylla/security/encryption-at-rest>` - protects your data persisted in storage or backup.
  * :ref:`BYPASS CACHE <bypass-cache>` - This CQL command informs the database that the data being read is unlikely to be read again in the near future, and also was unlikely to have been read in the near past; therefore no attempt should be made to read it from the cache or to populate the cache with the data. 

  Read the `Release Notes <https://www.scylladb.com/product/release-notes/>`_ for more information. 

.. panel-box::
  :title: Scylla Enterprise 2018.1 Features
  :id: "getting-started"
  :class: my-panel

  The following are **new features** for Scylla Scylla Enterprise 2018.1.x

  * :doc:`Scylla Auditing Guide </operating-scylla/security/auditing>` -  allows administrators to know which users performed what action at what time.
  * :doc:`Scylla in-memory tables </using-scylla/in-memory/>` - an alternative table type for storing data in RAM.
  * :doc:`Scylla Counters </using-scylla/counters/>` - A data type for counting
  * :ref:`Time Window Compaction Strategy <time-window-compactionstrategy-twcs>` - a replacement compaction strategy for Date-Tiered Compaction Strategy, refined for time-series data.
  * `Heat Weighted Load Balancing <https://www.scylladb.com/2017/09/21/scylla-heat-weighted-load-balancing/>`_ a blog entry which investigates what happens if one of the nodes loses its cache and the solution Heat Weighted Load Balancing.

  Read the `Release Notes <https://www.scylladb.com/product/release-notes/>`_ for more information.
