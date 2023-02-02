=================
Upgrade ScyllaDB
=================

.. toctree::
   :titlesonly:
   :hidden:

   ScyllaDB Enterprise <upgrade-enterprise/index>
   ScyllaDB Open Source <upgrade-opensource/index>
   ScyllaDB Open Source to ScyllaDB Enterprise <upgrade-to-enterprise/index>
   ScyllaDB AMI <ami-upgrade>

Overview
---------

ScyllaDB upgrade is a rolling procedure - it does not require a full cluster shutdown and is performed without any 
downtime or disruption of service.

To ensure a successful upgrade, follow the :ref:`documented upgrade procedures <upgrade-upgrade-procedures>` tested by ScyllaDB. This means that:

* You should perform the upgrades consecutively - to each successive X.Y version, **without skipping any major or minor version**.
* Before you upgrade to the next version, the whole cluster (each node) must be upgraded to the previous version.

Example
========

The following example shows the upgrade path for a 3-node cluster from version 4.3 to version 4.6: 

#. Upgrade all three nodes to version 4.4.
#. Upgrade all three nodes to version 4.5.
#. Upgrade all three nodes to version 4.6.

.. _upgrade-upgrade-procedures:

Procedures for Upgrading ScyllaDB
-----------------------------------

* :doc:`Upgrade ScyllaDB Enterprise <upgrade-enterprise/index>`

* :doc:`Upgrade ScyllaDB Open Source <upgrade-opensource/index>`

* :doc:`Upgrade from ScyllaDB Open Source to Scylla Enterprise <upgrade-to-enterprise/index>`

* :doc:`Upgrade ScyllaDB AMI <ami-upgrade>`





