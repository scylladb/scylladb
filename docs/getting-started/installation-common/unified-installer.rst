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
See `OS Support <https://docs.scylladb.com/stable/versioning/os-support-per-version.html>`_
for information about supported Linux distributions and versions.

.. _unified-installer-download:

Download and Install
-----------------------

#. Download the latest tar.gz file for ScyllaDB version (x86 or ARM) from ``https://downloads.scylladb.com/downloads/scylla/relocatable/scylladb-<version>/``.

   **Example** for version 2025.1:
   
   - Go to https://downloads.scylladb.com/downloads/scylla/relocatable/scylladb-2025.1/
   - Download the ``scylla-unified`` file for the patch version you want to
     install. For example, to install 2025.1.9 (x86), download
     ``scylla-unified-2025.1.9-0.20251010.6c539463bbda.x86_64.tar.gz``.

#. Uncompress the downloaded package.

   **Example** for version 2025.1.9 (x86) (downloaded in the previous step):

   .. code::

    tar xvfz scylla-unified-2025.1.9-0.20251010.6c539463bbda.x86_64.tar.gz

#. (Root offline installation only) For root offline installation on Debian-like
   systems, two additional packages, ``xfsprogs`` and ``mdadm``, should be
   installed to be used in RAID setup.

#. Install ScyllaDB as a user with non-root privileges:

   .. code:: console

    ./install.sh --nonroot

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

    ~/scylladb/bin/nodetool nodetool status

Run cqlsh:

.. code:: console

    ~/scylladb/bin/cqlsh 


.. note::

    You can avoid adding the extended prefix to the commands by exporting the binary directories to PATH:

    ``export PATH=$PATH:~/scylladb/python3/bin:~/scylladb/share/cassandra/bin/:~/scylladb/bin:~/scylladb/sbin``


Upgrade/ Downgrade/ Uninstall
---------------------------------

.. _unified-installed-upgrade:

Upgrade
=========

The unified package is based on a binary package; it's not an RPM or DEB package, so you can't upgrade or downgrade
it with yum or apt. To upgrade ScyllaDB, run the ``install.sh`` script from the package of the new version.

Before you upgrade, see :doc:`Upgrade ScyllaDB </upgrade/index>` for the supported upgrade paths and the
recommendations to follow during the upgrade.

Upgrade the nodes one at a time (rolling upgrade). Perform the following steps on **each** node in the cluster,
and move on to the next node only after the upgraded node is up and running.

#. Drain the node and back up the data:

   .. code:: sh

    nodetool drain
    nodetool snapshot

   For a nonroot install, run ``~/scylladb/bin/nodetool`` instead of ``nodetool``.

   Take note of the directory name that nodetool gives you, and copy all the directories with this name
   to a backup device. The directories are located under ``/var/lib/scylla/data`` (root install) or
   ``~/scylladb/data`` (nonroot install).

#. Back up the ``scylla.yaml`` configuration file, located in ``/etc/scylla/`` (root install) or
   ``~/scylladb/etc/scylla/`` (nonroot install).

#. Stop the node:

   Root install:

   .. code:: sh

    sudo systemctl stop scylla-server

   Nonroot install:

   .. code:: sh

    systemctl --user stop scylla-server

#. Download and extract the unified package of the new version, as described in
   :ref:`Download and Install <unified-installer-download>`. Then, run the ``install.sh`` script from
   the extracted directory:

   Root install:

   .. code:: sh

    ./install.sh --upgrade

   Nonroot install:

   .. code:: sh

    ./install.sh --upgrade --nonroot

#. Start the node:

   Root install:

   .. code:: sh

    sudo systemctl start scylla-server

   Nonroot install:

   .. code:: sh

    systemctl --user start scylla-server

#. Validate the upgrade. Run ``nodetool status`` to verify that the node is up (``UN``), and check
   the ScyllaDB logs for errors.

When the upgrade is completed on all nodes, remove the snapshot with the
``nodetool clearsnapshot -t <snapshot>`` command to prevent running out of space.

.. note:: The installation script does not upgrade scylla-tools. You will have to upgrade them separately. 

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