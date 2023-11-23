=============================================================================
Upgrade Guide - |SCYLLA_NAME| |SRC_VERSION| to |NEW_VERSION| for |OS|
=============================================================================

This document is a step by step procedure for upgrading from |SCYLLA_NAME| |SRC_VERSION| to |SCYLLA_NAME| |NEW_VERSION|, and rollback to version |SRC_VERSION| if required.

Applicable Versions
===================
This guide covers upgrading |SCYLLA_NAME| |SRC_VERSION| to |SCYLLA_NAME| |NEW_VERSION| on |OS|.
See :doc:`OS Support by Platform and Version </getting-started/os-support>` for information about supported versions.

Upgrade Procedure
=================

A ScyllaDB upgrade is a rolling procedure which does **not** require full cluster shutdown.
For each of the nodes in the cluster, you will:

* Check the cluster's schema
* Drain the node and backup the data
* Backup the configuration file
* Stop ScyllaDB
* Download and install new ScyllaDB packages
* Start ScyllaDB
* Validate that the upgrade was successful

Apply the following procedure **serially** on each node. Do not move to the next node before validating that the node you upgraded is up and running the new version.


**During** the rolling upgrade, it is highly recommended:

* Not to use the new |NEW_VERSION| features
* Not to run administration functions, like repairs, refresh, rebuild or add or remove nodes. See `sctool <https://manager.docs.scylladb.com/stable/sctool/>`_ for suspending ScyllaDB Manager (only available for ScyllaDB Enterprise) scheduled or running repairs.
* Not to apply schema changes

.. note:: Before upgrading, make sure to use the latest `ScyllaDB Montioring <https://monitoring.docs.scylladb.com/>`_ stack.

Upgrade Steps
=============
Check the cluster schema
-------------------------
Make sure that all nodes have the schema synced before the upgrade. The upgrade will fail if there is a schema disagreement between nodes.

.. code:: sh

       nodetool describecluster

Drain the nodes and backup the data
-----------------------------------
Before any major procedure, like an upgrade, it is recommended to backup all the data to an external device. In Scylla, backup is done using the ``nodetool snapshot`` command. For **each** node in the cluster, run the following command:

.. code:: sh

   nodetool drain
   nodetool snapshot

Take note of the directory name that nodetool gives you, and copy all the directories having that name under ``/var/lib/scylla`` to a backup device.

When the upgrade is completed on all nodes, remove the snapshot with the ``nodetool clearsnapshot -t <snapshot>`` command to prevent running out of space.

Backup the configuration file
------------------------------

Back up the ``scylla.yaml`` configuration file and the ScyllaDB packages
in case you need to rollback the upgrade.

.. code:: sh

   sudo cp -a /etc/scylla/scylla.yaml /etc/scylla/scylla.yaml.backup
   sudo cp /etc/apt/sources.list.d/scylla.list ~/scylla.list-backup

Gracefully stop the node
------------------------

.. code:: sh

   sudo service scylla-server stop

Download and install the new release
------------------------------------
Before upgrading, check what version you are running now using ``dpkg -s scylla-server``. You should use the same version in case you want to |ROLLBACK|_ the upgrade. If you are not running a |SRC_VERSION|.x version, stop right here! This guide only covers |SRC_VERSION|.x to |NEW_VERSION|.y upgrades.


