===============================
Production Readiness Guidelines
===============================

The goal of this document is to have a checklist that production customers can use to make sure their
deployment adheres to ScyllaDB’s recommendations.
Before deploying to production you should follow up on each of the main bullets described below to verify they comply with the
recommendations provided. Click the links for more information on each topic.


Before You Begin
----------------

Pre-Deployment Requirements
===========================

* :doc:`ScyllaDB System Requirements</getting-started/system-requirements/>` - verify your instances, system, OS, etc are supported by ScyllaDB for production machines.
* :doc:`ScyllaDB Getting Started </getting-started/index>`

Choose a Compaction Strategy
============================

Each workload may require a specific strategy. Refer to :doc:`Choose a Compaction Strategy </architecture/compaction/compaction-strategies>` for details.

Resiliency
----------

When rolling out to production it is important to make sure your data is recoverable and your database can function anytime there is a power or equipment failure.

Replication Factors
===================

Verify the :term:`Replication Factor <Replication Factor (RF)>` is set properly **for each keyspace**.

We recommend using an :abbr:`RF (Replication Factor)` of **at least** three.

If you have a multi-datacenter architecture we recommend to have ``RF=3`` on each DC.

For additional information:

* Read more about :doc:`ScyllaDB Fault Tolerance </architecture/architecture-fault-tolerance/>`
* Take a course at `ScyllaDB University on RF <https://university.scylladb.com/courses/scylla-essentials-overview/lessons/high-availability/topic/fault-tolerance-replication-factor/>`_.

Consistency Levels
==================

Verify the :term:`Consistency Level (CL) <Consistency Level (CL)>` is set properly **for each table**.

We recommend using :code:`LOCAL_QUORUM` across **the cluster and DCs**

For additional information:

* Refer to :doc:`ScyllaDB Fault Tolerance </architecture/architecture-fault-tolerance/>`
* Watch a :doc:`Demo </architecture/console-CL-full-demo/>`
* Take a course at `ScyllaDB University on CL <https://university.scylladb.com/courses/scylla-essentials-overview/lessons/high-availability/topic/consistency-level/>`_

Gossip Configuration
====================

#. Choose the correct Snitch.

   **Always use** :code:`GossipingPropertyFileSnitch` or :code:`Ec2MultiRegionSnitch`
   **Do Not** use SimpleStrategy on any production machine, even if you only have a single DC.

   For additional information:

   * Refer to :doc:`Gossip in ScyllaDB </kb/gossip/>`
   * Follow the :doc:`How to Switch Snitches </operating-scylla/procedures/config-change/switch-snitch/>` procedure if required.
   * Take a course at `ScyllaDB University on Gossip <https://university.scylladb.com/courses/scylla-essentials-overview/lessons/architecture/topic/gossip/>`_

#. Use the correct Data Replication strategy.

   Use :code:`NetworkTopologyStrategy` replication-strategy as it supports multi-DC for your keyspaces.

Performance
-----------

Verify you have run `scylla_setup` in order to tune ScyllaDB to your hardware.

If you are running on a physical hardware please take a look into the following configuration files:

* perftune.yaml_
* cpuset.conf_

perftune.yaml
=============

If you have more than 8 cores or 16 vcpu **always use** :code:`mode: sq_split`

cpuset.conf
===========
Make sure that the configuration in ``/etc/scylla.d/cpuset.conf`` corresponds to ``sq_split`` and that the  hyperthreads of physical core 0 are excluded from the CPU list.

Compression
-----------

.. note: Compression trades CPU for networking so this trade-off may be expensive for you and may not be beneficial.

Inter-node Compression
======================

Enable Inter-node Compression by editing the ScyllaDB Configuration file (/etc/scylla.yaml).

:code:`internode_compression: all`

For additional information, see the Admin Guide :ref:`Inter-node Compression <internode-compression>` section.

Driver Compression
==================

This refers to compressing traffic between the client and ScyllaDB.
Verify your client driver is using compressed traffic when connected to ScyllaDB.
As compression is driver settings dependent, please check your client driver manual or :doc:`ScyllaDB Drivers </using-scylla/drivers/index>`.


Connectivity
------------

Drivers Settings
================

* Use shard aware drivers wherever possible. :doc:`ScyllaDB Drivers </using-scylla/drivers/index>` (not third-party drivers) are shard aware.
* Configure connection pool - open more connections (>3 per shard) and/Or more clients. See `this blog <https://www.scylladb.com/2019/11/20/maximizing-performance-via-concurrency-while-minimizing-timeouts-in-distributed-databases/>`_.

Management
----------

You must use both ScyllaDB Manager and ScyllaDB Monitor.

ScyllaDB Manager
================

ScyllaDB Manager enables centralized cluster administration and database
automation such as repair, backup, and health-check.

Repair
......

Run repairs preferably once a week and run them exclusively from ScyllaDB Manager.
Refer to `Repair a Cluster <https://manager.docs.scylladb.com/branch-2.2/repair/index.html>`_

Backup and Restore
..................

We recommend the following:

* Run a full weekly backup from ScyllaDB Manager
* Run a daily backup from ScyllaDB Manager
* Check the bucket used for restore. This can be done by performing a `restore <https://manager.docs.scylladb.com/branch-2.2/restore/index.html>`_ and making sure the data is valid. This action should be done once a month, or more frequently if needed. Ask our Support team to help you with this.
* Save backup to a bucket supported by ScyllaDB Manager.

For additional information:

* `Backup <https://manager.docs.scylladb.com/branch-2.2/backup/index.html>`_
* `Restore a Backup <https://manager.docs.scylladb.com/branch-2.2/restore/index.html>`_

ScyllaDB Monitoring Stack
============================

ScyllaDB Monitoring Stack helps you monitor everything about your ScyllaDB cluster. ScyllaDB Support team
usually asks for your monitoring metrics when you open a ticket.

See `ScyllaDB Monitoring Stack <https://monitoring.docs.scylladb.com/stable/>`_ for details.

Configuration Management
========================

.. caution:: **All** configuration settings for **all** nodes in the **same cluster** should be **identical or coherent**.

Using tools such as Ansible, Chef, Puppet, Salt, or Juju are recommended.

See this `article <https://www.softwaretestinghelp.com/top-5-software-configuration-management-tools/>`_ for more information.

Security
--------

Use the following guidelines to keep your data and database secure.

* Enable :doc:`Authentication </operating-scylla/security/authentication/>`
* Create Roles for all users and use :doc:`RBAC </operating-scylla/security/rbac-usecase/>` with or without LDAP (coming soon).
* Use Encryption in Transit :doc:`between nodes </operating-scylla/security/node-node-encryption/>` and :doc:`client to node </operating-scylla/security/client-node-encryption/>`.
* Refer to the :doc:`Security Checklist </operating-scylla/security/security-checklist/>` for more information.


HA Testing
----------

HA testing in single DC - for example:

#. Shutdown one node from the cluster (Or scylla service if on the cloud) for 30 min.
#. Turn it back on.

HA testing in multi DC - for example:

#. Disconnect one DC from the other by stopping scylla service on all of these DC
   nodes.
#. Reconnect the DC.

Additional Topics
-----------------
* :doc:`Add a Node </operating-scylla/procedures/cluster-management/add-node-to-cluster/>`
* `Repair <https://manager.docs.scylladb.com/branch-2.2/repair/index.html>`_
* :doc:`Cleanup </operating-scylla/nodetool-commands/cleanup/>`
* Tech Talk: `How to be successful with ScyllaDB <https://www.scylladb.com/tech-talk/how-to-be-successful-with-scylla/>`_
