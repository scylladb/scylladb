.. |SCYLLA_NAME| replace:: ScyllaDB

.. |SRC_VERSION| replace:: 6.x.y
.. |NEW_VERSION| replace:: 6.x.z

.. |MINOR_VERSION| replace:: 6.x

.. |SCYLLA_DEB_NEW_REPO| replace:: ScyllaDB deb repo
.. _SCYLLA_DEB_NEW_REPO: https://www.scylladb.com/download/#open-source

.. |SCYLLA_RPM_NEW_REPO| replace:: ScyllaDB Enterprise rpm repo
.. _SCYLLA_RPM_NEW_REPO: https://www.scylladb.com/download/#open-source

=============================================================================
Upgrade Guide - |SCYLLA_NAME| |SRC_VERSION| to |NEW_VERSION|
=============================================================================

This document is a step-by-step procedure for upgrading from |SCYLLA_NAME| |SRC_VERSION| 
to |SCYLLA_NAME| |NEW_VERSION| (where "z" is the latest available version).

Applicable Versions
===================

This guide covers upgrading ScyllaDB on Red Hat Enterprise Linux (RHEL), CentOS, Debian, 
and Ubuntu. See :doc:`OS Support by Platform and Version </getting-started/os-support>` for 
information about supported versions.

This guide also applies when you're upgrading ScyllaDB Enterprise official image on EC2, GCP, 
or Azure.

Upgrade Procedure
=================

.. note::
   Apply the following procedure **serially** on each node. Do not move to the next node
   before validating the node is up and running the new version.

A ScyllaDB upgrade is a rolling procedure which does **not** require full cluster shutdown.
For each of the nodes in the cluster, you will:

* Drain node and backup the data.
* Check your current release.
* Backup configuration file.
* Stop ScyllaDB.
* Download and install new ScyllaDB packages.
* Start ScyllaDB.
* Validate that the upgrade was successful.

**Before** upgrading, check what version you are running now using ``scylla --version``. 
You should use the same version in case you want to rollback the upgrade.

**During** the rolling upgrade it is highly recommended:

* Not to use new |NEW_VERSION| features.
* Not to run administration functions, like repairs, refresh, rebuild or add or remove nodes.
* Not to apply schema changes.

Upgrade steps
=============

Drain node and backup the data
------------------------------
Before any major procedure, like an upgrade, it is recommended to backup all the data to
an external device. In ScyllaDB, backup is done using the ``nodetool snapshot`` command.
For **each** node in the cluster, run the following command:

.. code:: sh

   nodetool drain
   nodetool snapshot

Take note of the directory name that nodetool gives you, and copy all
the directories having this name under ``/var/lib/scylla`` to a backup device.

When the upgrade is complete (all nodes), the snapshot should be removed by
``nodetool clearsnapshot -t <snapshot>``, or you risk running out of space.

Backup the configuration file
------------------------------

Back up the scylla.yaml configuration file and the ScyllaDB packages in case
you need to rollback the upgrade.

.. tabs::

   .. group-tab:: Debian/Ubuntu

      .. code:: sh

         sudo cp -a /etc/scylla/scylla.yaml /etc/scylla/scylla.yaml.backup
         sudo cp /etc/apt/sources.list.d/scylla.list ~/scylla.list-backup


   .. group-tab:: RHEL/CentOS

      .. code:: sh 
         
         sudo cp -a /etc/scylla/scylla.yaml /etc/scylla/scylla.yaml.backup
         sudo cp /etc/yum.repos.d/scylla.repo ~/scylla.repo-backup

Gracefully stop the node
------------------------

.. code:: sh

   sudo service scylla-server stop


Download and install the new release
------------------------------------

.. tabs::

   .. group-tab:: Debian/Ubuntu
        
        #. Update the |SCYLLA_DEB_NEW_REPO|_ to |MINOR_VERSION|.
        #. Install:
        
            .. code:: sh

               sudo apt-get clean all
               sudo apt-get update
               sudo apt-get dist-upgrade scylla

            Answer ‘y’ to the first two questions.
        
   .. group-tab:: RHEL/CentOS

        #. Update the |SCYLLA_RPM_NEW_REPO|_ to |MINOR_VERSION|.
        #. Install:

            .. code:: sh

               sudo yum clean all
               sudo yum update scylla\* -y

   .. group-tab:: EC2/GCP/Azure Ubuntu Image

        If you're using the ScyllaDB official image (recommended), see 
        the **Debian/Ubuntu** tab for upgrade instructions.

        If you're using your own image and installed ScyllaDB packages for 
        Ubuntu or Debian, you need to apply an extended upgrade procedure:

        #. Update the |SCYLLA_DEB_NEW_REPO|_ to |MINOR_VERSION|.
        #. Install the new ScyllaDB version with the additional ``scylla-machine-image`` package:

            .. code-block:: console

               sudo apt-get clean all
               sudo apt-get update
               sudo apt-get dist-upgrade scylla
               sudo apt-get dist-upgrade scylla-machine-image

        #. Run ``scylla_setup`` without ``running io_setup``.
        #. Run ``sudo /opt/scylladb/scylla-machine-image/scylla_cloud_io_setup``.

Start the node
--------------

.. code:: sh

   sudo service start scylla-server

Validate
--------
1. Check cluster status with ``nodetool status`` and make sure **all** nodes, including the one you just upgraded, are in UN status.
2. Use ``curl -X GET "http://localhost:10000/storage_service/scylla_release_version"`` to check the ScyllaDB version.
3. Use ``journalctl _COMM=scylla`` to check there are no new errors in the log.
4. Check again after 2 minutes, to validate no new issues are introduced.

Once you are sure the node upgrade is successful, move to the next node in the cluster.

Rollback Procedure
==================

The following procedure describes a rollback from ScyllaDB release |NEW_VERSION| to |SRC_VERSION|.
Apply this procedure if an upgrade from |SRC_VERSION| to |NEW_VERSION| failed before
completing on all nodes. Use this procedure only for nodes you upgraded to |NEW_VERSION|.

.. caution::
   
   Apply the procedure **serially** on each node. Do not move to the next node
   before validating the node is up and running with the new version.

ScyllaDB rollback is a rolling procedure which does **not** require full cluster shutdown.
For each of the nodes to rollback to |SRC_VERSION|, you will:

* Drain the node and stop ScyllaDB.
* Downgrade to previous release.
* Restore the configuration file.
* Restart ScyllaDB.
* Validate the rollback success.

Rollback steps
==============

Gracefully shutdown ScyllaDB
-----------------------------

.. code:: sh

   nodetool drain
   sudo service stop scylla-server

Downgrade to the previous release
----------------------------------

.. tabs::

   .. group-tab:: Debian/Ubuntu

        Install:

            .. code-block:: console
               :substitutions:

               sudo apt-get install scylla=|SRC_VERSION|\* scylla-server=|SRC_VERSION|\* scylla-tools=|SRC_VERSION|\* scylla-tools-core=|SRC_VERSION|\* scylla-kernel-conf=|SRC_VERSION|\* scylla-conf=|SRC_VERSION|\*
               sudo apt-get install scylla-machine-image=|SRC_VERSION|\*  # only execute on AMI instance


        Answer ‘y’ to the first two questions.

   .. group-tab:: RHEL/CentOS

       Install:

        .. code-block:: console
           :substitutions:

            sudo yum downgrade scylla\*-|SRC_VERSION|-\* -y


Restore the configuration file
------------------------------

.. code:: sh
   
   sudo rm -rf /etc/scylla/scylla.yaml
   sudo cp -a /etc/scylla/scylla.yaml.backup /etc/scylla/scylla.yaml

Start the node
--------------

.. code:: sh

   sudo service scylla-server start

Validate
--------
Check upgrade instruction above for validation. Once you are sure the node
rollback is successful, move to the next node in the cluster.
