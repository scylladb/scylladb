Upgrade Guide - |SCYLLA_NAME| |SRC_VERSION| to |NEW_VERSION| for |OS|
======================================================================

This document is a step-by-step procedure for upgrading from |SCYLLA_NAME| |FROM| to |SCYLLA_NAME| |TO|, and rollback to 2021.1 if required.


Applicable Versions
------------------------
This guide covers upgrading |SCYLLA_NAME| from version |FROM| to version |TO| on |OS|. See :doc:`OS Support by Platform and Version </getting-started/os-support>` for information about supported versions.


Upgrade Procedure
----------------------------

.. note::
   Apply the following procedure **serially** on each node. Do not move to the next node before validating the node is up and running the new version.

A ScyllaDB upgrade is a rolling procedure which does **not** require full cluster shutdown.
For each of the nodes in the cluster, you will:

* Drain node and backup the data.
* Check your current release.
* Backup configuration file.
* Stop ScyllaDB.
* Download and install new ScyllaDB packages.
* Start ScyllaDB.
* Validate that the upgrade was successful.


**During** the rolling upgrade it is highly recommended:

* Not to use new |TO| features.
* Not to run administration functions, like repairs, refresh, rebuild or add or remove nodes.
* Not to apply schema changes.

Upgrade steps
-------------------------------
Drain node and backup the data
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Before any major procedure, like an upgrade, it is recommended to backup all the data to an external device. In ScyllaDB, backup is done using the ``nodetool snapshot`` command. For **each** node in the cluster, run the following command:

.. code:: sh

   nodetool drain
   nodetool snapshot

Take note of the directory name that nodetool gives you, and copy all the directories having this name under ``/var/lib/scylla`` to a backup device.

When the upgrade is complete (all nodes), the snapshot should be removed by ``nodetool clearsnapshot -t <snapshot>``, or you risk running out of space.


Backup configuration file
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code:: sh

   sudo cp -a /etc/scylla/scylla.yaml /etc/scylla/scylla.yaml.backup-5.x.z

Gracefully stop the node
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code:: sh

   sudo service scylla-server stop

Download and install the new release
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Before upgrading, check what version you are running now using ``dpkg -s scylla-server``. You should use the same version in case you want to |ROLLBACK|_ the upgrade. If you are not running a |FROM| version, stop right here! This guide only covers |FROM| to |TO| upgrades.
