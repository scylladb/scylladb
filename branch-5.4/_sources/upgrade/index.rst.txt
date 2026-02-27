=================
Upgrade ScyllaDB
=================

.. toctree::
   :titlesonly:
   :hidden:

   ScyllaDB Versioning <scylladb-versioning>
   ScyllaDB Open Source Upgrade <upgrade-opensource/index>
   ScyllaDB Open Source to ScyllaDB Enterprise Upgrade <upgrade-to-enterprise/index>
   ScyllaDB Image <ami-upgrade>
   ScyllaDB Enterprise <https://enterprise.docs.scylladb.com/stable/upgrade/upgrade-enterprise/index.html>

Overview
---------

ScyllaDB upgrade is a rolling procedure - it does not require a full cluster shutdown and is performed without any 
downtime or disruption of service.

To ensure a successful upgrade, follow the :ref:`documented upgrade procedures <upgrade-upgrade-procedures>` tested by ScyllaDB. This means that:

* You should perform the upgrades consecutively - to each successive X.Y version, **without skipping any major or minor version**.
* Before you upgrade to the next version, the whole cluster (each node) must be upgraded to the previous version.
* You cannot perform an upgrade by replacing the nodes in the cluster with new nodes with a different ScyllaDB version. You should never add a new node with a different version to a cluster - if you :doc:`add a node </operating-scylla/procedures/cluster-management/add-node-to-cluster>`, it must have the same X.Y.Z (major.minor.patch) version as the other nodes in the cluster.

Example
========

The following example shows the upgrade path for a 3-node cluster from version 4.3 to version 4.6: 

#. Upgrade all three nodes to version 4.4.
#. Upgrade all three nodes to version 4.5.
#. Upgrade all three nodes to version 4.6.


Upgrading to each patch version by following the Maintenance Release Upgrade Guide 
is optional. However, we recommend upgrading to the latest patch release for your version before upgrading to a new version. 
For example, upgrade to patch 4.4.8 before upgrading to version 4.5.

.. _upgrade-upgrade-procedures:

Procedures for Upgrading ScyllaDB
-----------------------------------

* :doc:`Upgrade ScyllaDB Open Source <upgrade-opensource/index>`

* :doc:`Upgrade from ScyllaDB Open Source to Scylla Enterprise <upgrade-to-enterprise/index>`

* :doc:`Upgrade ScyllaDB Image <ami-upgrade>`

* `Upgrade ScyllaDB Enterprise <https://enterprise.docs.scylladb.com/stable/upgrade/upgrade-enterprise/index.html>`_





