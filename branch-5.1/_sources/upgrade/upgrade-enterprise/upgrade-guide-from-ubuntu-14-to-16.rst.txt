=====================================
Upgrade Guide - Ubuntu 14.04 to 16.04
=====================================

Ubuntu 14.04 will be end of life in April 2019, this document is a step by step procedure for Scylla users to upgrade Ubuntu 14.04 to 16.04.


Applicable versions
===================

* OS: Ubuntu 14.04
* Scylla: 2018.1 

Upgrade Procedure
=================

.. include:: /upgrade/_common/warning.rst

The upgrade is a rolling procedure which does not require full cluster shutdown. For each of the nodes in the cluster, serially (i.e. one at a time), you will:

* Check cluster schema
* Drain node and backup the data
* Backup configuration file
* Stop Scylla
* Upgrade OS to latest 14.04
* Clean scylla-gcc73 packages to avoid conflict
* Upgrade OS to 16.04
* Upgrade Scylla packages
* Start Scylla
* Validate that the upgrade was successful

Apply the following procedure **serially** on each node. Do not move to the next node before validating the node is up and running with the new version.

**During** the rolling upgrade, it is highly recommended to:

* Not to use new 2018.1 features
* Not to run administration functions, like repairs, refresh, rebuild or add or remove nodes
* Not to apply schema changes

Upgrade steps
=============
Check cluster schema
--------------------
Make sure that all nodes have the schema synched prior to upgrade, we won't survive an upgrade that has schema disagreement between nodes.

.. code:: sh

       nodetool describecluster

Drain node and backup the data
------------------------------
Before any major procedure, like an upgrade, it is recommended to backup all the data to an external device. In Scylla, backup is done using the ``nodetool snapshot`` command. For **each** node in the cluster, run the following command:

.. code:: sh

   nodetool drain
   nodetool snapshot

Take note of the directory name that nodetool gives you, and copy all the directories having this name under ``/var/lib/scylla`` to a backup device.

When the upgrade is complete (all nodes), the snapshot should be removed by ``nodetool clearsnapshot -t <snapshot>``, or you risk running out of space.

Backup configuration file
-------------------------
.. code:: sh

   for conf in $(cat /var/lib/dpkg/info/scylla-*server.conffiles /var/lib/dpkg/info/scylla-*conf.conffiles /var/lib/dpkg/info/scylla-*jmx.conffiles | grep -v init ); do sudo cp -v $conf $conf.backup-2.3; done

Gracefully stop the node
------------------------

.. code:: sh

   sudo service scylla-server stop

Upgrade OS to latest 14.04
--------------------------

.. code:: sh

    sudo apt-get clean all
    sudo apt-get update
    sudo apt-get upgrade

Clean scylla-gcc73 packages to avoid conflict
---------------------------------------------

Related issue: https://github.com/scylladb/scylla/issues/4164

.. code:: sh

    sudo apt-get remove scylla-gcc73-libstdc++6

Upgrade OS to 16.04
-------------------

This step will take some time and will require manual interaction.

.. code:: sh

   sudo apt-get update
   sudo do-release-upgrade
   sudo reboot

Upgrade Scylla packages
-----------------------

Update Scylla repo to repo for Ubuntu 16.04, `Scylla RPM repo <https://www.scylladb.com/enterprise-download/ubuntu-16-04/>`

.. code:: sh

  sudo apt-get dist-upgrade scylla-enterprise


Start the node
--------------

.. code:: sh

   sudo systemctl start scylla-server

Validate
--------
1. Check cluster status with ``nodetool status`` and make sure **all** nodes, including the one you just upgraded, are in UN status.
2. Use ``curl -X GET "http://localhost:10000/storage_service/scylla_release_version"`` to check scylla version.
3. Check scylla-server log (execute ``journalctl _COMM=scylla`` for Ubuntu 16.04) and ``/var/log/syslog`` to validate there are no errors.
4. Check again after 2 minutes, to validate no new issues are introduced.

Once you are sure the node upgrade is successful, move to the next node in the cluster.
