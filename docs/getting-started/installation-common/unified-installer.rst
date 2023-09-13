.. |SCYLLADB_VERSION| replace:: 5.2

.. update the version folder URL below (variables won't work):
    https://downloads.scylladb.com/downloads/scylla/relocatable/scylladb-5.2/

====================================================
Install ScyllaDB Without root Privileges
====================================================

This document covers installing, uninstalling, and upgrading ScyllaDB using Unified Installer. 
Unified Installer is recommended when you do not have root privileges to the server.
If you have root privileges, we recommend installing ScyllaDB with 
:doc:`ScyllaDB Web Installer </getting-started/installation-common/scylla-web-installer/>`
or by downloading the OS-specific packages (RPMs and DEBs) and installing them with 
the package manager (dnf and apt).

Prerequisites
---------------
Ensure your platform is supported by the ScyllaDB version you want to install. 
See :doc:`OS Support </getting-started/os-support>` for information about supported Linux distributions and versions.

Note that if you're on CentOS 7, only root offline installation is supported.

Download and Install
-----------------------

#. Download the latest tar.gz file for ScyllaDB |SCYLLADB_VERSION| (x86 or ARM) from https://downloads.scylladb.com/downloads/scylla/relocatable/scylladb-5.2/.
#. Uncompress the downloaded package.

   The following example shows the package for ScyllaDB 5.2.4 (x86):

   .. code:: console

    tar xvfz scylla-unified-5.2.4-0.20230623.cebbf6c5df2b.x86_64.tar.gz

#. Install OpenJDK 8 or 11.

   The following example shows Java installation on a CentOS-like system:

   .. code:: console
    
    sudo yum install -y java-11-openjdk-headless

   For root offline installation on Debian-like systems, two additional packages, ``xfsprogs`` 
   and ``mdadm``, should be installed to be used in RAID setup.

#. Install ScyllaDB as a user with non-root privileges:

   .. code:: console

    ./install.sh --nonroot --python3 ~/scylladb/python3/bin/python3

Configure and Run ScyllaDB
----------------------------

#. Run the ScyllaDB setup script:

   .. code:: console

    ~/scylladb/sbin/scylla_setup

#. Start ScyllaDB:

   .. code:: console

    systemctl --user start scylla-server

#. Verify that ScyllaDB is running:

   .. code:: console

    systemctl --user status scylla-server

Now you can start using ScyllaDB. Here are some tools you may find useful.


Run nodetool:

.. code:: console

    ~/scylladb/share/cassandra/bin/nodetool status

Run cqlsh:

.. code:: console

    ~/scylladb/share/cassandra/bin/cqlsh 

Run cassandra-stress:

.. code:: console

    ~/scylladb/share/cassandra/bin/cassandra-stress write

.. note::

    You can avoid adding the extended prefix to the commands by exporting the binary directories to PATH:

    ``export PATH=$PATH:~/scylladb/python3/bin:~/scylladb/share/cassandra/bin/:~/scylladb/bin:~/scylladb/sbin``


Upgrade/ Downgrade/ Uninstall
---------------------------------

.. _unified-installed-upgrade:

Upgrade
=========

The unified package is based on a binary package; it’s not a RPM / DEB packages, so it doesn’t upgrade or downgrade by yum / apt. To upgrade ScyllaDB, run the ``install.sh`` script.

Root install:

.. code:: sh

    ./install.sh --upgrade

Nonroot install

.. code:: sh

    ./install.sh --upgrade --nonroot

.. note:: The installation script does not upgrade scylla-jmx and scylla-tools. You will have to upgrade them separately. 

Uninstall
===========

Root uninstall:

.. code:: sh

    sudo ./uninstall.sh

Nonroot uninstall

.. code:: sh

    ./uninstall.sh --nonroot


Downgrade
===========

To downgrade to your original ScyllaDB version, use the Uninstall_ procedure, then install the original ScyllaDB version. 

Next Steps
------------

* :doc:`Configure ScyllaDB </getting-started/system-configuration>`
* Manage your clusters with `ScyllaDB Manager <https://manager.docs.scylladb.com/>`_
* Monitor your cluster and data with `ScyllaDB Monitoring <https://monitoring.docs.scylladb.com/>`_
* Get familiar with ScyllaDB’s :doc:`command line reference guide </operating-scylla/nodetool>`.
* Learn about ScyllaDB at `ScyllaDB University <https://university.scylladb.com/>`_
