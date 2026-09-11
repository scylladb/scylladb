======================================================================
Upgrade Guide - Scylla 1.5 to 1.6 for Red Hat Enterprise 7 or CentOS 7
======================================================================

This document is a step by step procedure for upgrading from Scylla 1.5 to Scylla 1.6, and rollback to 1.5 if required.


Applicable versions
===================
This guide covers upgrading Scylla from the following versions: 1.5.x to Scylla version 1.6.y, on the following platforms:

* Red Hat Enterprise Linux, version 7 and later
* CentOS, version 7 and later
* No longer provide packages for Fedora

Upgrade Procedure
=================

.. include:: /upgrade/_common/warning.rst

A Scylla upgrade is a rolling procedure which does not require full cluster shutdown. For each of the nodes in the cluster, serially (i.e. one at a time), you will:

* drain node and backup the data
* check your current release
* backup configuration file
* stop Scylla
* download and install new Scylla packages
* start Scylla
* validate that the upgrade was successful

Apply the following procedure **serially** on each node. Do not move to the next node before validating the node is up and running with the new version.

**During** the rolling upgrade it is highly recommended:

* Not to use new 1.6 features
* Not to run administration functions, like repairs, refresh, rebuild or add or remove nodes
* Not to apply schema changes

Upgrade steps
=============
Drain node and backup the data
------------------------------
Before any major procedure, like an upgrade, it is recommended to backup all the data to an external device. In Scylla, backup is done using the ``nodetool snapshot`` command. For **each** node in the cluster, run the following command:

.. code:: sh

   nodetool drain
   nodetool snapshot

Take note of the directory name that nodetool gives you, and copy all the directories having this name under ``/var/lib/scylla`` to a backup device.

When the upgrade is complete (all nodes), the snapshot should be removed by ``nodetool clearsnapshot -t <snapshot>``, or you risk running out of space.

Backup configuration file
-------------------------
.. code:: sh

   sudo cp -a /etc/scylla/scylla.yaml /etc/scylla/scylla.yaml.backup-1.5

Stop Scylla
-----------
.. code:: sh

   sudo systemctl stop scylla-server

Download and install the new release
------------------------------------
Before upgrading, check what version you are running now using ``rpm -qa | grep scylla-server``. You should use the same version in case you want to :ref:`rollback <upgrade-1.5-1.6-rpm-rollback-procedure>` the upgrade. If you are not running a 1.5.x version, stop right here! This guide only covers 1.5.x to 1.6.y upgrades.

To upgrade:

1. Update the `Scylla RPM repo <http://www.scylladb.com/download/centos_rpm>`_ to **1.6**
2. install

.. code:: sh

   sudo yum update scylla\* -y

Start the node
--------------
.. code:: sh

   sudo systemctl start scylla-server

Validate
--------
1. Check cluster status with ``nodetool status`` and make sure **all** nodes, including the one you just upgraded, are in UN status.
2. Use ``curl -X GET "http://localhost:10000/storage_service/scylla_release_version"`` to check scylla version.
3. Use ``journalctl _COMM=scylla`` to check there are no new errors in the log.
4. Check again after 2 minutes, to validate no new issues are introduced.

Once you are sure the node upgrade is successful, move to the next node in the cluster.

.. _upgrade-1.5-1.6-rpm-rollback-procedure:

Rollback Procedure
==================

.. include:: /upgrade/_common/warning_rollback.rst

The following procedure describes a rollback from Scylla release 1.6.x to 1.5.y. Apply this procedure if an upgrade from 1.5 to 1.6 failed before completing on all nodes. Use this procedure only for nodes you upgraded to 1.6

Scylla rollback is a rolling procedure which does **not** require full cluster shutdown.
For each of the nodes rollback to 1.5, you will:

* drain the node and stop Scylla
* retrieve the old Scylla packages
* restore the configuration file
* restart Scylla
* validate the rollback success

Apply the following procedure **serially** on each node. Do not move to the next node before validating the node is up and running with the new version.

Rollback steps
==============
Gracefully shutdown Scylla
--------------------------
.. code:: sh

   nodetool drain
   sudo systemctl stop scylla-server

Download and install the new release
------------------------------------
1. Remove the old repo file.

.. code:: sh

   sudo rm -rf /etc/yum.repos.d/scylla.repo

2. Update the `Scylla RPM repo <http://www.scylladb.com/download/centos_rpm>`_ to **1.5**
3. Install

.. code:: sh

   sudo yum clean all
   sudo yum downgrade scylla\* -y

Restore the configuration file
------------------------------

.. code:: sh

   sudo rm -rf /etc/scylla/scylla.yaml
   sudo cp -a /etc/scylla/scylla.yaml.backup-1.5 /etc/scylla/scylla.yaml

Start the node
--------------

.. code:: sh

   sudo systemctl start scylla-server

Validate
--------
Check upgrade instruction above for validation. Once you are sure the node rollback is successful, move to the next node in the cluster.
