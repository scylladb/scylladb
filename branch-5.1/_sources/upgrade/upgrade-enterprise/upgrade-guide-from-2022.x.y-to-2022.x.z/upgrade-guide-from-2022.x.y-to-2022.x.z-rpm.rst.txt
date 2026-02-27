=====================================================================================================
Upgrade Guide - ScyllaDB Enterprise 2022.x.y to 2022.x.z for Red Hat Enterprise 7, 8 or CentOS 7, 8
=====================================================================================================

This document is a step-by-step procedure for upgrading from ScyllaDB Enterprise 2022.x.y to 2022.x.z.


Applicable versions
===================
This guide covers upgrading ScyllaDB Enterprise from version 2022.x.y to 2022.x.z, on the following platforms:

* Red Hat Enterprise Linux 7 and later
* CentOS 7 and later

Packages for Fedora are no longer provided.

Upgrade Procedure
=================

A ScyllaDB upgrade is a rolling procedure which does not require a full cluster shutdown. For each of the nodes in the cluster, you will:

* Drain the node and backup the data
* Check your current release
* Backup the configuration file
* Stop ScyllaDB
* Download and install the new ScyllaDB packages
* Start ScyllaDB
* Validate that the upgrade was successful

Apply the following procedure **serially** on each node. Do not move to the next node before validating that the node that you upgraded is up and running the new version.

**During** the rolling upgrade, it is highly recommended:

* Not to use new 2022.x.z features.
* Not to run administration functions, like repairs, refresh, rebuild or add or remove nodes.
* Not to apply schema changes.

Upgrade Steps
=============

Drain the node and backup the data
-----------------------------------
Before any major procedure, like an upgrade, it is recommended to backup all the data to an external device. In ScyllaDB, backup is done using the ``nodetool snapshot`` command. For **each** node in the cluster, run the following command:

.. code:: sh

   nodetool drain
   nodetool snapshot

Take note of the directory name that nodetool gives you, and copy all the directories having this name under ``/var/lib/scylla`` to a backup device.

When the upgrade is completed on all nodes, the snapshot should be removed with the ``nodetool clearsnapshot -t <snapshot>`` command, or you risk running out of space.

Backup the configuration file
------------------------------

.. code:: sh

   sudo cp -a /etc/scylla/scylla.yaml /etc/scylla/scylla.yaml.backup-2021.x.z

Stop ScyllaDB
--------------

.. code:: sh

   sudo systemctl stop scylla-server

Download and install the new release
------------------------------------
Before upgrading, check what version you are running now using ``rpm -qa | grep scylla-enterprise-server``. You should use the same version in case you want to :ref:`rollback <upgrade-2022.x.y-to-2022.x.z-rpm-rollback-procedure>` the upgrade. If you are not running a 2022.x.y version, stop right here! This guide only covers 2022.x.y to 2022.x.z upgrades.

To upgrade:

#. Update the `ScyllaDB Enterprise RPM repo <https://www.scylladb.com/customer-portal/?product=ent&platform=centos7&version=stable-release-2021.1>`_ to **2022.x**.
#. Install:

    .. code:: sh

       sudo yum clean all
       sudo yum update scylla\* -y

Start the node
--------------

.. code:: sh

   sudo systemctl start scylla-server

Validate
--------
1. Check cluster status with ``nodetool status`` and make sure **all** nodes, including the one you just upgraded, are in UN status.
2. Use ``curl -X GET "http://localhost:10000/storage_service/scylla_release_version"`` to check the ScyllaDB version.
3. Use ``journalctl _COMM=scylla`` to check there are no new errors in the log.
4. Check again after 2 minutes to validate no new issues are introduced.

Once you are sure the node upgrade is successful, move to the next node in the cluster.

.. _upgrade-2022.x.y-to-2022.x.z-rpm-rollback-procedure:

Rollback Procedure
==================

.. include:: /upgrade/_common/warning_rollback.rst

The following procedure describes a rollback from ScyllaDB Enterprise release 2022.x.z to 2022.x.y. Apply this procedure if an upgrade from 2022.x.y to 2022.x.z failed before completing on all nodes. Use this procedure only for nodes you upgraded to 2022.x.z.

ScyllaDB rollback is a rolling procedure which does **not** require a full cluster shutdown.
For each of the nodes rollback to 2022.x.y, you will:

* Drain the node and stop ScyllaDB
* Downgrade to the previous release
* Restore the configuration file
* Restart ScyllaDB
* Validate the rollback success

Apply the following procedure **serially** on each node. Do not move to the next node before validating that the node is up and running the new version.


Rollback Steps
==============

Gracefully shutdown ScyllaDB
-----------------------------

.. code:: sh

   nodetool drain
   sudo systemctl stop scylla-server

Downgrade to the previous release
-----------------------------------

Install:

.. code:: sh

   sudo yum downgrade scylla\*-2022.x.y-\* -y

Restore the configuration file
------------------------------

.. code:: sh

   sudo rm -rf /etc/scylla/scylla.yaml
   sudo cp -a /etc/scylla/scylla.yaml.backup-2022.x.z /etc/scylla/scylla.yaml

Start the node
--------------

.. code:: sh

   sudo systemctl start scylla-server

Validate
--------
Check the upgrade instruction above for validation. Once you are sure the node rollback is successful, move to the next node in the cluster.
