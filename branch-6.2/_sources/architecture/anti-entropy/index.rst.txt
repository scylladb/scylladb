ScyllaDB Anti-Entropy
=====================

.. toctree::
   :hidden:
   :glob:

   ScyllaDB Hinted Handoff <hinted-handoff/>
   ScyllaDB Read Repair <read-repair/>
   ScyllaDB Repair </operating-scylla/procedures/maintenance/repair/>
    

ScyllaDB replicates data according to :term:`eventual consistency<Eventual Consistency>`.  This means that, in ScyllaDB, when considering the :term:`CAP Theorem<CAP Theorem>`, availability and partition tolerance are considered a higher priority over consistency. Although ScyllaDB’s tunable consistency allows users to make a tradeoff between availability and consistency,  ScyllaDB’s :term:`consistency level<Consistency Level (CL)>` is tunable per query.

However, over time, there can be a number of reasons for data inconsistencies, including:

1. a down node;
2. a network partition;
3. dropped mutations;
4. process crashes (before a flush);
5. a replica that cannot write due to being out of resources;
6. file corruption.

To mitigate :term:`entropy<Entropy>`, or data inconsistency, ScyllaDB uses a few different processes.  The goal of ScyllaDB :term:`anti-entropy<Anti-Entropy>` - based on that of Apache Cassandra  -  is to compare data on all replicas, synchronize data between all replicas,  and, finally,  ensure each replica has the most recent data.

Anti-entropy measures include *write-time* changes such as :term:`hinted handoff<Hinted Handoff>`, *read-time* changes such as :term:`read repair<Read Repair>`, and finally, periodic maintenance via :term:`repair<Repair>`.

* :doc:`ScyllaDB Hinted Handoff <hinted-handoff/>` - High-Level view of ScyllaDB Hinted Handoff
* :doc:`ScyllaDB Read Repair <read-repair/>` - High-Level view of ScyllaDB Read Repair
* :doc:`ScyllaDB Repair </operating-scylla/procedures/maintenance/repair/>` - Description of ScyllaDB Repair

Also learn more in the `Cluster Management, Repair and ScyllaDB Manager lesson <https://university.scylladb.com/courses/scylla-operations/lessons/cluster-management-repair-and-scylla-manager/topic/cluster-management-repair-and-scylla-manager/>`_ on ScyllaDB University.

.. include:: /rst_include/apache-copyrights.rst
