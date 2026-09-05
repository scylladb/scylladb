Scylla Anti-Entropy
===================

.. toctree::
   :hidden:
   :glob:

   Scylla Hinted Handoff <hinted-handoff/>
   Scylla Read Repair <read-repair/>
   Scylla Repair </operating-scylla/procedures/maintenance/repair/>
    

Scylla replicates data according to :term:`eventual consistency<Eventual Consistency>`.  This means that, in Scylla, when considering the :term:`CAP Theorem<CAP Theorem>`, availability and partition tolerance are considered a higher priority over consistency. Although Scylla’s tunable consistency allows users to make a tradeoff between availability and consistency,  Scylla’s :term:`consistency level<Consistency Level (CL)>` is tunable per query.

However, over time, there can be a number of reasons for data inconsistencies, including:

1. a down node;
2. a network partition;
3. dropped mutations;
4. process crashes (before a flush);
5. a replica that cannot write due to being out of resources;
6. file corruption.

To mitigate :term:`entropy<Entropy>`, or data inconsistency, Scylla uses a few different processes.  The goal of Scylla :term:`anti-entropy<Anti-Entropy>` - based on that of Apache Cassandra  -  is to compare data on all replicas, synchronize data between all replicas,  and, finally,  ensure each replica has the most recent data.

Anti-entropy measures include *write-time* changes such as :term:`hinted handoff<Hinted Handoff>`, *read-time* changes such as :term:`read repair<Read Repair>`, and finally, periodic maintenance via :term:`repair<Repair>`.

* :doc:`Scylla Hinted Handoff <hinted-handoff/>` - High-Level view of Scylla Hinted Handoff
* :doc:`Scylla Read Repair <read-repair/>` - High-Level view of Scylla Read Repair
* :doc:`Scylla Repair </operating-scylla/procedures/maintenance/repair/>` - Description of Scylla Repair

Also learn more in the `Cluster Management, Repair and Scylla Manager lesson <https://university.scylladb.com/courses/scylla-operations/lessons/cluster-management-repair-and-scylla-manager/topic/cluster-management-repair-and-scylla-manager/>`_ on Scylla University.

.. include:: /rst_include/apache-copyrights.rst
