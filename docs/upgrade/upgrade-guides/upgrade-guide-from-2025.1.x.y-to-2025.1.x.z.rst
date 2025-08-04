.. |SCYLLA_NAME| replace:: ScyllaDB

.. |SRC_VERSION| replace:: 2025.1.x.y
.. |NEW_VERSION| replace:: 2025.1.x.z

.. |MINOR_VERSION| replace:: 2025.1.x

==========================================================================
Upgrade - |SCYLLA_NAME| |SRC_VERSION| to |NEW_VERSION| (Patch Upgrades)
==========================================================================

This document describes a step-by-step procedure for upgrading from
|SCYLLA_NAME| |SRC_VERSION|  to |SCYLLA_NAME| |NEW_VERSION| (where "z" is
the latest available version), and rolling back to version |SRC_VERSION|
if necessary.

This guide covers upgrading ScyllaDB on Red Hat Enterprise Linux (RHEL),
CentOS, Debian, and Ubuntu.
See :doc:`OS Support by Platform and Version </getting-started/os-support>`
for information about supported versions.

It also applies to the ScyllaDB official image on EC2, GCP, or Azure.

Upgrade Procedure
=================

.. note::
   Apply the following procedure **serially** on each node. Do not move to the next
   node before validating that the node is up and running the new version.

A ScyllaDB upgrade is a rolling procedure that does **not** require a full cluster
shutdown. For each of the nodes in the cluster, you will:

#. Drain the node and back up the data.
#. Backup configuration file.
#. Stop ScyllaDB.
#. Download and install new ScyllaDB packages.
#. Start ScyllaDB.
#. Validate that the upgrade was successful.

**Before** upgrading, check which version you are running now using
``scylla --version``. Note the current version in case you want to roll back
the upgrade.

**During** the rolling upgrade it is highly recommended:

* Not to use new |NEW_VERSION| features.
* Not to run administration functions, like repairs, refresh, rebuild or add
  or remove nodes. See
  `sctool <https://manager.docs.scylladb.com/stable/sctool/>`_ for suspending
  ScyllaDB Manager's scheduled or running repairs.
* Not to apply schema changes.

Upgrade Steps
=============

Back up the data
------------------------------

Back up all the data to an external device. We recommend using
`ScyllaDB Manager <https://manager.docs.scylladb.com/stable/backup/index.html>`_
to create backups.

Alternatively, you can use the ``nodetool snapshot`` command.
For **each** node in the cluster, run the following:

.. code:: sh

   nodetool drain
   nodetool snapshot

Take note of the directory name that nodetool gives you, and copy all
the directories with this name under ``/var/lib/scylla`` to a backup device.

When the upgrade is completed on all nodes, remove the snapshot with the 
``nodetool clearsnapshot -t <snapshot>`` command to prevent running out of space.

Back up the configuration file
------------------------------

Back up the ``scylla.yaml`` configuration file and the ScyllaDB packages
in case you need to roll back the upgrade.

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

You don’t need to update the ScyllaDB DEB or RPM repo when you upgrade to
a patch release.

.. tabs::

   .. group-tab:: Debian/Ubuntu

        To install a patch version on Debian or Ubuntu, run:
        
        .. code:: sh
            
            sudo apt-get clean all
            sudo apt-get update
            sudo apt-get dist-upgrade scylla

        Answer ‘y’ to the first two questions.
        
   .. group-tab:: RHEL/CentOS

        To install a patch version on RHEL or CentOS, run:

        .. code:: sh
            
            sudo yum clean all
            sudo yum update scylla\* -y

   .. group-tab:: EC2/GCP/Azure Ubuntu Image

        If you're using the ScyllaDB official image (recommended), see 
        the **Debian/Ubuntu** tab for upgrade instructions.

        If you're using your own image and have installed ScyllaDB packages for 
        Ubuntu or Debian, you need to apply an extended upgrade procedure:

        #. Install the new ScyllaDB version with the additional
           ``scylla-machine-image`` package:

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
#. Check cluster status with ``nodetool status`` and make sure **all** nodes,
   including the one you just upgraded, are in UN status.
#. Use ``curl -X GET "http://localhost:10000/storage_service/scylla_release_version"``
   to check the ScyllaDB version.
#. Use ``journalctl _COMM=scylla`` to check there are no new errors in the log.
#. Check again after 2 minutes to validate that no new issues are introduced.

Once you are sure the node upgrade is successful, move to the next node in
the cluster.

Rollback Procedure
==================

The following procedure describes a rollback from ScyllaDB release
|NEW_VERSION| to |SRC_VERSION|. Apply this procedure if an upgrade from
|SRC_VERSION| to |NEW_VERSION| failed before completing on all nodes. 

* Use this procedure only on nodes you upgraded to |NEW_VERSION|.
* Execute the following commands one node at a time, moving to the next node only
  after the rollback procedure is completed successfully.

ScyllaDB rollback is a rolling procedure that does **not** require a full
cluster shutdown. For each of the nodes to roll back to |SRC_VERSION|, you will:

#. Drain the node and stop ScyllaDB.
#. Downgrade to the previous release.
#. Restore the configuration file.
#. Restart ScyllaDB.
#. Validate the rollback success.

Rollback Steps
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

        To downgrade to |SRC_VERSION| on Debian or Ubuntu, run:
    
        .. code-block:: console
            :substitutions:

            sudo apt-get install scylla=|SRC_VERSION|\* scylla-server=|SRC_VERSION|\* scylla-tools=|SRC_VERSION|\* scylla-tools-core=|SRC_VERSION|\* scylla-kernel-conf=|SRC_VERSION|\* scylla-conf=|SRC_VERSION|\*


        Answer ‘y’ to the first two questions.

   .. group-tab:: RHEL/CentOS
    
        To downgrade to |SRC_VERSION| on RHEL or CentOS, run:

        .. code-block:: console
            :substitutions:

            sudo yum downgrade scylla\*-|SRC_VERSION|-\* -y

   .. group-tab:: EC2/GCP/Azure Ubuntu Image

        If you’re using the ScyllaDB official image (recommended), see
        the **Debian/Ubuntu** tab for upgrade instructions.

        If you’re using your own image and have installed ScyllaDB packages for
        Ubuntu or Debian, you need to additionally downgrade
        the ``scylla-machine-image`` package.

        .. code-block:: console
            :substitutions:

            sudo apt-get install scylla=|SRC_VERSION|\* scylla-server=|SRC_VERSION|\* scylla-tools=|SRC_VERSION|\* scylla-tools-core=|SRC_VERSION|\* scylla-kernel-conf=|SRC_VERSION|\* scylla-conf=|SRC_VERSION|\*
            sudo apt-get install scylla-machine-image=|SRC_VERSION|\*


        Answer ‘y’ to the first two questions.


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