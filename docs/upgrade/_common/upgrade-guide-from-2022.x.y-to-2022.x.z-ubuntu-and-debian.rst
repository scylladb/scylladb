======================================================================
Upgrade Guide - ScyllaDB Enterprise 2022.x.y to 2022.x.z for |OS|
======================================================================

This document is a step-by-step procedure for upgrading from ScyllaDB Enterprise 2022.x.y to 2022.x.z.


Applicable versions
===================
This guide covers upgrading ScyllaDB Enterprise from version 2022.x.y to 2022.x.z on the following platform:

* |OS|

Upgrade Procedure
=================
.. include:: /upgrade/upgrade-enterprise/_common/enterprise_2022.1_warnings.rst

A ScyllaDB Enterprise upgrade is a rolling procedure which does **not** require a full cluster shutdown.
For each of the nodes in the cluster, you will:

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
------------------------------------
Before any major procedure, like an upgrade, it is recommended to backup all the data to an external device. In ScyllaDB, backup is done using the ``nodetool snapshot`` command. For **each** node in the cluster, run the following command:

.. code:: sh

   nodetool drain
   nodetool snapshot

Take note of the directory name that nodetool gives you, and copy all the directories having this name under ``/var/lib/scylla`` to a backup device.

When the upgrade is completed on all nodes, the snapshot should be removed with the ``nodetool clearsnapshot -t <snapshot>`` command, or you risk running out of space.

Backup the configuration file
-------------------------------

.. code:: sh

   sudo cp -a /etc/scylla/scylla.yaml /etc/scylla/scylla.yaml.backup-2022.x.z

Backup more config files.

.. code:: sh

   for conf in $(cat /var/lib/dpkg/info/scylla-*server.conffiles /var/lib/dpkg/info/scylla-*conf.conffiles /var/lib/dpkg/info/scylla-*jmx.conffiles | grep -v init ); do sudo cp -v $conf $conf.backup-2.1; done

Gracefully stop the node
------------------------

.. code:: sh

   sudo service scylla-enterprise-server stop

Download and install the new release
------------------------------------
Before upgrading, check what version you are running now using ``dpkg -s scylla-enterprise-server``. You should use the same version in case you want to |ROLLBACK|_ the upgrade. If you are not running a 2022.x.y version, stop right here! This guide only covers 2022.x.y to 2022.x.z upgrades.

To upgrade:

#. Update the |APT|_ to **2022.x**.
#. Install:

    .. code:: sh

       sudo apt-get clean all
       sudo apt-get update
       sudo apt-get dist-upgrade scylla-enterprise

Answer ‘y’ to the first two questions.

Start the node
--------------

.. code:: sh

   sudo service scylla-enterprise-server start

Validate
--------
1. Check cluster status with ``nodetool status`` and make sure **all** nodes, including the one you just upgraded, are in UN status.
2. Use ``curl -X GET "http://localhost:10000/storage_service/scylla_release_version"`` to check the ScyllaDB version.
3. Check scylla-enterprise-server log (by ``journalctl _COMM=scylla``) and ``/var/log/syslog`` to validate there are no errors.
4. Check again after 2 minutes to validate no new issues are introduced.

Once you are sure the node upgrade is successful, move to the next node in the cluster.

Rollback Procedure
==================

.. include:: /upgrade/_common/warning_rollback.rst

The following procedure describes a rollback from ScyllaDB Enterprise release 2022.x.z to 2022.x.y. Apply this procedure if an upgrade from 2022.x.y to 2022.x.z failed before completing on all nodes. Use this procedure only for nodes you upgraded to 2022.x.z.

ScyllaDB rollback is a rolling procedure which does **not** require a full cluster shutdown.
For each of the nodes rollback to 2022.x.y, you will:

* Gracefully shutdown ScyllaDB
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
   sudo service scylla-enterprise-server stop

Downgrade to the previous release
----------------------------------

Install:

.. code:: sh

   sudo apt-get install scylla-enterprise=2022.x.y\* scylla-enterprise-server=2022.x.y\* scylla-enterprise-jmx=2022.x.y\* scylla-enterprise-tools=2022.x.y\* scylla-enterprise-tools-core=2022.x.y\* scylla-enterprise-kernel-conf=2022.x.y\* scylla-enterprise-conf=2022.x.y\* scylla-enterprise-python3=2022.x.y\*
   sudo apt-get install scylla-enterprise-machine-image=2022.x.y\*  # only execute on AMI instance

Answer ‘y’ to the first two questions.

Restore the configuration file
------------------------------

.. code:: sh

   sudo rm -rf /etc/scylla/scylla.yaml
   sudo cp -a /etc/scylla/scylla.yaml.backup-2022.x.z /etc/scylla/scylla.yaml

Restore more config files.

.. code:: sh

   for conf in $(cat /var/lib/dpkg/info/scylla-*server.conffiles /var/lib/dpkg/info/scylla-*conf.conffiles /var/lib/dpkg/info/scylla-*jmx.conffiles | grep -v init ); do sudo cp -v $conf.backup-2.1 $conf; done
   sudo systemctl daemon-reload

Start the node
--------------

.. code:: sh

   sudo service scylla-enterprise-server start

Validate
--------
Check the upgrade instruction above for validation. Once you are sure the node rollback is successful, move to the next node in the cluster.
