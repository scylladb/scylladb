======================================================================
Upgrade Guide - |SCYLLA_NAME| |SRC_VERSION| to |NEW_VERSION| for |OS|
======================================================================

This document is a step-by-step procedure for upgrading from |SCYLLA_NAME| |FROM| to |SCYLLA_NAME| |TO| (where "z" is the :ref:`latest available version <faq-pinning>`), and rollback to 2021.1 if required.

Applicable versions
===================
This guide covers upgrading |SCYLLA_NAME| from version |FROM| to version |TO| on |OS|. See :doc:`OS Support by Platform and Version </getting-started/os-support>` for information about supported versions.


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

   sudo service scylla-server stop

Download and install the new release
------------------------------------
Before upgrading, check what version you are running now using ``dpkg -s scylla-enterprise-server``. You should use the same version in case you want to |ROLLBACK|_ the upgrade. If you are not running a 2022.x.y version, stop right here! This guide only covers 2022.x.y to 2022.x.z upgrades.
