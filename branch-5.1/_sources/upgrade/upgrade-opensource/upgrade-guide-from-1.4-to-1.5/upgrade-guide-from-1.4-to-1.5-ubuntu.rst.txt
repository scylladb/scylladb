=============================================================================
Upgrade Guide - Scylla 1.4 to 1.5 for Ubuntu 14.04 or 16.04
=============================================================================

This document is a step by step procedure for upgrading from Scylla 1.4 to Scylla 1.5, and rollback to 1.4 if required.


Applicable versions
===================
This guide covers upgrading Scylla from the following versions: 1.4.x to Scylla version 1.5.y on the following platform:

* Ubuntu 14.04
* Ubuntu 16.04

Upgrade Procedure
=================

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

* Not to use new 1.5 features
* Not to run administration functions, like repairs, refresh, rebuild or add or remove nodes
* Not to apply schema changes

Drain node and backup the data
------------------------------
Before any major procedure, like an upgrade, it is recommended to backup all the data to an external device. In Scylla, backup is done using the ``nodetool snapshot`` command. For **each** node in the cluster, run the following command:

.. code:: sh

   nodetool drain
   nodetool snapshot

Take note of the directory name that nodetool gives you, and copy all the directories having this name under ``/var/lib/scylla`` to a backup device.

When the upgrade is complete (all nodes), the snapshot should be removed by ``nodetool clearsnapshot -t <snapshot>``, or you risk running out of space.

Upgrade steps
=============
Backup configuration file
-------------------------

.. code:: sh

   sudo cp -a /etc/scylla/scylla.yaml /etc/scylla/scylla.yaml.backup-1.4

Gracefully stop the node
------------------------

.. code:: sh

   sudo service scylla-server stop

Download and install the new release
------------------------------------
Before upgrading, check what version you are running now using ``dpkg -s scylla-server``. You should use the same version in case you want to :ref:`rollback <upgrade-1.4-1.5-ubuntu-rollback-procedure>` the upgrade. If you are not running a 1.4.x version, stop right here! This guide only covers 1.4.x to 1.5.y upgrades.

To upgrade:

1. Update the `Scylla deb repo <http://www.scylladb.com/download/#fndtn-deb>`_ to **1.5**
2. Install

.. code:: sh

   sudo apt-get update
   sudo apt-get dist-upgrade scylla

Answer ‘y’ to the first two questions.

Start the node
--------------

.. code:: sh

   sudo service scylla-server start

Validate
--------
1. Check cluster status with ``nodetool status`` and make sure **all** nodes, including the one you just upgraded, are in UN status.
2. Use ``curl -X GET "http://localhost:10000/storage_service/scylla_release_version"`` to check scylla version.
3. Check scylla-server log (check ``/var/log/upstart/scylla-server.log`` for Ubuntu 14.04, execute ``journalctl _COMM=scylla`` for Ubuntu 16.04) and ``/var/log/syslog`` to validate there are no errors.
4. Check again after 2 minutes, to validate no new issues are introduced.

Once you are sure the node upgrade is successful, move to the next node in the cluster.

.. _upgrade-1.4-1.5-ubuntu-rollback-procedure:

Rollback Procedure
==================
The following procedure describes a rollback from Scylla release 1.5.x to 1.4.y. Apply this procedure if an upgrade from 1.4 to 1.5 failed before completing on all nodes. Use this procedure only for nodes you upgraded to 1.5


Scylla rollback is a rolling procedure which does **not** require full cluster shutdown.
For each of the nodes rollback to 1.4, you will:

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
   sudo service scylla-server stop

download and install the old release
------------------------------------
1. Remove the old repo file.

.. code:: sh

   sudo rm -rf /etc/apt/sources.list.d/scylla.list

2. Update the `Scylla deb repo <http://www.scylladb.com/download/#fndtn-deb>`_ to **1.4**
3. install

.. code:: sh

   sudo apt-get update
   sudo apt-get remove scylla\* -y
   sudo apt-get install scylla

Answer ‘y’ to the first two questions.

Restore the configuration file
------------------------------
.. code:: sh

   sudo rm -rf /etc/scylla/scylla.yaml
   sudo cp -a /etc/scylla/scylla.yaml.backup-1.4 /etc/scylla/scylla.yaml

Start the node
--------------

.. code:: sh

   sudo service scylla-server start

Validate
--------
Check upgrade instruction above for validation. Once you are sure the node rollback is successful, move to the next node in the cluster.
