=============================================================================
Upgrade Guide - |SCYLLA_NAME| |SRC_VERSION| to |NEW_VERSION| for |OS|
=============================================================================

This document is a step by step procedure for upgrading from |SCYLLA_NAME| |SRC_VERSION| to |SCYLLA_NAME| |NEW_VERSION|, and rollback to |SRC_VERSION| if required.

..
  Relevant and tested for Ubuntu 20.04. Remove from other OSes and versions.

There are two upgrade alternatives: you can upgrade ScyllaDB simultaneously updating 3rd party and OS packages (recommended for Ubuntu 20.04), or upgrade ScyllaDB without updating any external packages.

Applicable versions
===================
This guide covers upgrading Scylla from version |SRC_VERSION|.x or later to |SCYLLA_NAME| version |NEW_VERSION|.y on the following platform:

* |OS|

Upgrade Procedure
=================

A Scylla upgrade is a rolling procedure which does **not** require full cluster shutdown.
For each of the nodes in the cluster, you will:

* Check cluster schema
* Drain node and backup the data
* Backup configuration file
* Stop Scylla
* Download and install new Scylla packages
* Start Scylla
* Validate that the upgrade was successful

Apply the following procedure **serially** on each node. Do not move to the next node before validating that the node you upgraded is up and running with the new version.


**During** the rolling upgrade, it is highly recommended:

* Not to use new |NEW_VERSION| features
* Not to run administration functions, like repairs, refresh, rebuild or add or remove nodes. See `here <https://manager.docs.scylladb.com/stable/sctool/>`_ for suspending Scylla Manager (only available Scylla Enterprise) scheduled or running repairs.
* Not to apply schema changes

.. note:: Before upgrading, make sure to use the latest `Scylla Montioring <https://monitoring.docs.scylladb.com/>`_ stack.

Upgrade steps
=============
Check the cluster schema
-------------------------
Make sure that all nodes have the schema synched before the upgrade. The upgrade will fail if there is any disagreement between the nodes.

.. code:: sh

       nodetool describecluster

Drain the nodes and backup the data
-----------------------------------
Before any major procedure, like an upgrade, it is recommended to backup all the data to an external device. In Scylla, backup is done using the ``nodetool snapshot`` command. For **each** node in the cluster, run the following command:

.. code:: sh

   nodetool drain
   nodetool snapshot

Take note of the directory name that nodetool gives you, and copy all the directories having this name under ``/var/lib/scylla`` to a backup device.

When the upgrade is complete on all nodes, the snapshot should be removed by ``nodetool clearsnapshot -t <snapshot>`` to prevent running out of space.

Backup configuration file
-------------------------

.. code:: sh

   sudo cp -a /etc/scylla/scylla.yaml /etc/scylla/scylla.yaml.backup-src

Gracefully stop the node
------------------------

.. code:: sh

   sudo service scylla-server stop

Download and install the new release
------------------------------------
Before upgrading, check what version you are running now using ``dpkg -s scylla-server``. You should use the same version in case you want to |ROLLBACK|_ the upgrade. If you are not running a |SRC_VERSION|.x version, stop right here! This guide only covers |SRC_VERSION|.x to |NEW_VERSION|.y upgrades.

**To upgrade ScyllaDB:**

1. Update the |SCYLLA_REPO|_ to |NEW_VERSION|

2. Install

.. code-block::
   
   sudo apt-get clean all
   sudo apt-get update
   sudo apt-get dist-upgrade |PKG_NAME|


Answer ‘y’ to the first two questions.

**To upgrade ScyllaDB and update 3rd party and OS packages:**

.. include:: /upgrade/_common/upgrade-image.rst


.. note::

   Alternator users upgrading from Scylla 4.0 to 4.1 need to set :doc:`default isolation level </upgrade/upgrade-opensource/upgrade-guide-from-4.0-to-4.1/alternator>`.

