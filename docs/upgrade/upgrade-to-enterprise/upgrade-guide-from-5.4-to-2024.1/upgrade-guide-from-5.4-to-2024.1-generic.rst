.. |SCYLLA_NAME| replace:: ScyllaDB

.. |SRC_VERSION| replace:: 5.4
.. |NEW_VERSION| replace:: 2024.1

.. |DEBIAN_SRC_REPO| replace:: Debian
.. _DEBIAN_SRC_REPO: http://www.scylladb.com/download/?platform=debian-10&version=scylla-5.4

.. |UBUNTU_SRC_REPO| replace:: Ubuntu
.. _UBUNTU_SRC_REPO: https://www.scylladb.com/download/?platform=ubuntu-20.04&version=scylla-5.4

.. |SCYLLA_DEB_SRC_REPO| replace:: ScyllaDB deb repo (|DEBIAN_SRC_REPO|_, |UBUNTU_SRC_REPO|_)

.. |SCYLLA_RPM_SRC_REPO| replace:: ScyllaDB rpm repo
.. _SCYLLA_RPM_SRC_REPO: https://www.scylladb.com/download/?platform=centos&version=scylla-5.4

.. |DEBIAN_NEW_REPO| replace:: Debian
.. _DEBIAN_NEW_REPO: https://www.scylladb.com/customer-portal/?product=ent&platform=debian-10&version=stable-release-2024.1

.. |UBUNTU_NEW_REPO| replace:: Ubuntu
.. _UBUNTU_NEW_REPO: https://www.scylladb.com/customer-portal/?product=ent&platform=ubuntu-20.04&version=stable-release-2024.1

.. |SCYLLA_DEB_NEW_REPO| replace:: ScyllaDB deb repo (|DEBIAN_NEW_REPO|_, |UBUNTU_NEW_REPO|_)

.. |SCYLLA_RPM_NEW_REPO| replace:: ScyllaDB rpm repo
.. _SCYLLA_RPM_NEW_REPO: https://www.scylladb.com/customer-portal/?product=ent&platform=centos7&version=stable-release-2024.1

.. |ROLLBACK| replace:: rollback
.. _ROLLBACK: ./#rollback-procedure

.. |SCYLLA_METRICS| replace:: ScyllaDB Enterprise Metrics Update - ScyllaDB Enterprise 5.4 to 2024.1
.. _SCYLLA_METRICS: ../metric-update-5.4-to-2024.1

=============================================================================
Upgrade Guide - |SCYLLA_NAME| |SRC_VERSION| to |NEW_VERSION|
=============================================================================

This document is a step-by-step procedure for upgrading from |SCYLLA_NAME| |SRC_VERSION| 
to |SCYLLA_NAME| Enterpise |NEW_VERSION|, and rollback to version |SRC_VERSION| if required.

This guide covers upgrading ScyllaDB on Red Hat Enterprise Linux (RHEL) CentOS, Debian, 
and Ubuntu. See :doc:`OS Support by Platform and Version </getting-started/os-support>` 
for information about supported versions.

This guide also applies when you're upgrading ScyllaDB Enterprise official image on EC2, 
GCP, or Azure.

Upgrade Procedure
=================

A ScyllaDB upgrade is a rolling procedure that does **not** require full cluster shutdown.
For each of the nodes in the cluster, you will:

* Check that the cluster's schema is synchronized
* Drain the node and backup the data
* Backup the configuration file
* Stop ScyllaDB
* Download and install new ScyllaDB packages
* Start ScyllaDB
* Validate that the upgrade was successful


.. caution:: 

   Apply the procedure **serially** on each node. Do not move to the next node before 
   validating that the node you upgraded is up and running the new version.

**During** the rolling upgrade, it is highly recommended:

* Not to use the new |NEW_VERSION| features.
* Not to run administration functions, like repairs, refresh, rebuild, or add or remove 
  nodes. See `sctool <https://manager.docs.scylladb.com/stable/sctool/>`_ for suspending 
  ScyllaDB Manager's scheduled or running repairs.
* Not to apply schema changes.

.. note:: 
   
   Before upgrading, make sure to use the latest 
   `ScyllaDB Monitoring <https://monitoring.docs.scylladb.com/>`_ stack.

Upgrade Steps
=============

Check the cluster schema
-------------------------
Make sure that all nodes have the schema synchronized before upgrade. The upgrade 
procedure will fail if there is a schema disagreement between nodes.

.. code:: sh

   nodetool describecluster

Drain the nodes and backup the data
-----------------------------------

Before any major procedure, like an upgrade, it is recommended to backup all 
the data to an external device. In ScyllaDB, you can backup the data using 
the ``nodetool snapshot`` command. For **each** node in the cluster, run 
the following command:

.. code:: sh

   nodetool drain
   nodetool snapshot

Take note of the directory name that nodetool gives you, and copy all the directories 
having that name under ``/var/lib/scylla`` to a backup device.

When the upgrade is completed on all nodes, remove the snapshot with the 
``nodetool clearsnapshot -t <snapshot>`` command to prevent running out of space.

Backup the configuration file
------------------------------

.. code:: sh

   sudo cp -a /etc/scylla/scylla.yaml /etc/scylla/scylla.yaml.backup-src

Gracefully stop the node
------------------------

.. code:: sh

   sudo service scylla-server stop

Download and install the new release
------------------------------------

Before upgrading, check what version you are running now using ``scylla --version``. 
You should use the same version as this version in case you want to |ROLLBACK|_ 
the upgrade. If you are not running a |SRC_VERSION|.x version, stop right here! 
This guide only covers |SRC_VERSION|.x to |NEW_VERSION|.y upgrades.

.. tabs::

   .. group-tab:: Debian/Ubuntu

        **To upgrade ScyllaDB:**

        #. Update the |SCYLLA_DEB_NEW_REPO| to |NEW_VERSION|
        #. Configure Java 1.8:

            .. code-block:: console

               sudo apt-get update
               sudo apt-get install -y openjdk-8-jre-headless
               sudo update-java-alternatives -s java-1.8.0-openjdk-amd64

        #. Install the new ScyllaDB version:

            .. code-block:: console

               sudo apt-get clean all
               sudo apt-get update
               sudo apt-get remove scylla\*
               sudo apt-get install scylla-enterprise
               sudo systemctl daemon-reload

        Answer ‘y’ to the first two questions.

   .. group-tab:: RHEL/CentOS

        **To upgrade ScyllaDB:**

        #. Update the |SCYLLA_RPM_NEW_REPO|_  to |NEW_VERSION|.
        #. Install the new ScyllaDB version:

            .. code:: sh

               sudo yum clean all
               sudo rm -rf /var/cache/yum
               sudo yum remove scylla\*
               sudo yum install scylla-enterprise

**Installing the New Version on Cloud**

If you’re using the ScyllaDB official image (recommended), see the *Debian/Ubuntu* 
tab for upgrade instructions. If you’re using your own image and hae installed ScyllaDB 
packages for Ubuntu or Debian, you need to apply an extended upgrade procedure:

#. Update the ScyllaDB deb repo (see above).
#. Configure Java 1.8 (see above).
#. Install the new ScyllaDB version with the additional 
   ``scylla-enterprise-machine-image`` package:

   .. code::

      sudo apt-get clean all
      sudo apt-get update
      sudo apt-get dist-upgrade scylla-enterprise
      sudo apt-get dist-upgrade scylla-enterprise-machine-image

#. Run ``scylla_setup`` without running ``io_setup``.
#. Run ``sudo /opt/scylladb/scylla-machine-image/scylla_cloud_io_setup``.

Start the node
--------------

.. code:: sh

   sudo service scylla-server start

Validate
--------
#. Check cluster status with ``nodetool status`` and make sure **all** nodes, including 
   the one you just upgraded, are in ``UN`` status.
#. Use ``curl -X GET "http://localhost:10000/storage_service/scylla_release_version"`` 
   to check the ScyllaDB version. Validate that the version matches the one you upgraded to.
#. Check scylla-server log (using ``journalctl _COMM=scylla``) and ``/var/log/syslog`` 
   to validate there are no new errors in the log.
#. Check again after two minutes to validate that no new issues are introduced.

Once you are sure the node upgrade was successful, move to the next node in the cluster.

Rollback Procedure
==================

.. include:: /upgrade/_common/warning_rollback.rst

The following procedure describes a rollback from |SCYLLA_NAME| |NEW_VERSION|.x to 
|SRC_VERSION|.y. Apply this procedure if an upgrade from |SRC_VERSION| to |NEW_VERSION| 
failed before completing on all nodes. Use this procedure only for nodes you upgraded 
to |NEW_VERSION|.

.. warning::

   The rollback procedure can only be applied if some nodes have **not** been upgraded 
   to |NEW_VERSION| yet. As soon as the last node in the rolling upgrade procedure is 
   started with |NEW_VERSION|, rollback becomes impossible. At that point, the only way 
   to restore a cluster to |SRC_VERSION| is by restoring it from backup.

ScyllaDB rollback is a rolling procedure that does **not** require a full cluster shutdown.
For each of the nodes you rollback to |SRC_VERSION| you will:

* Drain the node and stop ScyllaDB
* Retrieve the old ScyllaDB packages
* Restore the configuration file
* Restore system tables
* Reload systemd configuration
* Restart ScyllaDB
* Validate the rollback success

Apply the following procedure **serially** on each node. Do not move to the next node before 
validating that the rollback was successful and the node is up and running the old version.

Rollback Steps
==============

Drain and gracefully stop the node
----------------------------------

.. code:: sh

   nodetool drain
   sudo service scylla-server stop

Download and install the old release
------------------------------------

.. tabs::

   .. group-tab:: Debian/Ubuntu

        #. Remove the old repo file.

            .. code:: sh

               sudo rm -rf /etc/apt/sources.list.d/scylla.list

        #. Update the |SCYLLA_DEB_SRC_REPO| to |SRC_VERSION|.
        #. Install:

            .. code-block::

               sudo apt-get update
               sudo apt-get remove scylla\* -y
               sudo apt-get install scylla

        Answer ‘y’ to the first two questions.

   .. group-tab:: RHEL/CentOS

        #. Remove the old repo file.

            .. code:: sh

               sudo rm -rf /etc/yum.repos.d/scylla.repo

        #. Update the |SCYLLA_RPM_SRC_REPO|_  to |SRC_VERSION|.
        #. Install:

            .. code:: console

               sudo yum clean all
               sudo yum remove scylla\*
               sudo yum install scylla

.. note::
  
   If you are running a ScyllaDB Enterprise official image (for EC2 AMI, GCP, or Azure), follow the instructions for Ubuntu.

Restore the configuration file
------------------------------

.. code:: sh

   sudo rm -rf /etc/scylla/scylla.yaml
   sudo cp -a /etc/scylla/scylla.yaml.backup-src | /etc/scylla/scylla.yaml

Restore system tables
---------------------

Restore all tables of **system** and **system_schema** from the previous snapshot because 
|NEW_VERSION| uses a different set of system tables. 
See :doc:`Restore from a Backup and Incremental Backup </operating-scylla/procedures/backup-restore/restore/>` 
for reference.

.. code:: console

    
    cd /var/lib/scylla/data/keyspace_name/table_name-UUID/
    sudo find . -maxdepth 1 -type f  -exec sudo rm -f "{}" +
    cd /var/lib/scylla/data/keyspace_name/table_name-UUID/snapshots/<snapshot_name>/
    sudo cp -r * /var/lib/scylla/data/keyspace_name/table_name-UUID/
    sudo chown -R scylla:scylla /var/lib/scylla/data/keyspace_name/table_name-UUID/

Reload systemd configuration
----------------------------

You must reload the unit file if the systemd unit file is changed.

.. code:: sh

   sudo systemctl daemon-reload

Start the node
--------------

.. code:: sh

   sudo service scylla-server start

Validate
--------

Check the upgrade instructions above for validation. Once you are sure the node rollback 
is successful, move to the next node in the cluster.