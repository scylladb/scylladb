============================================
Upgrade Guide - Scylla 1.0 to 1.1 for Ubuntu
============================================
This document is a step by step procedure for upgrading from Scylla 1.0
to Scylla 1.1, and rollback to 1.0 if required.


Applicable versions
-------------------

This guide covers upgrading Scylla from the following versions: 1.0.x to
Scylla version 1.1.y on the following platform:

-  Ubuntu 14.04

Upgrade Procedure
-----------------

A Scylla upgrade is a rolling procedure which does not require full cluster shutdown. For each of the nodes in the cluster, serially (i.e. one at a time), you will:

-  backup the data
-  check your current release
-  backup configuration file
-  download and install new Scylla packages
-  gracefully restart Scylla
-  validate that the upgrade was successful

Apply the following procedure **serially** on each node. Do not move to
the next node before validating the node is up and running with the new
version.

**During** the rolling upgrade it is highly recommended:

-  Not to use new 1.1 features
-  Not to run administration functions, like repairs, refresh, rebuild
   or add or remove nodes
-  Not to apply schema changes

Backup the data
~~~~~~~~~~~~~~~

Before any major procedure, like an upgrade, it is recommended to backup
all the data to an external device. In Scylla, backup is done using the
``nodetool snapshot`` command. For **each** node in the cluster, run the
following command:

.. code:: sh

    nodetool snapshot

Take note of the directory name that nodetool gives you, and copy all
the directories having this name under ``/var/lib/scylla`` to a backup
device.

Upgrade steps
-------------

Backup configuration file
~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: sh

    sudo cp -a /etc/scylla/scylla.yaml /etc/scylla/scylla.yaml.backup-1.0
    sudo rm -rf /etc/apt/sources.list.d/scylla.list

Download and install the new release
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Before upgrading, check what version you are running now using
``dpkg -s scylla-server``. You should use the same version in case you
want to :ref:`rollback <upgrade-1.0-1.1-ubuntu-rollback-procedure>`
the upgrade. If you are not running a 1.0.x version, stop right here!
This guide only covers 1.0.x to 1.1.y upgrades.

To upgrade:

1. Update the `Scylla deb repo <http://www.scylladb.com/download/#fndtn-deb>`_ to
   **1.1**
2. Install

.. code:: sh

    sudo apt-get update
    sudo apt-get upgrade  scylla-server scylla-jmx scylla-tools

Answer ‘y’ to the first two questions and 'n' when asked to overwrite
``scylla.yaml``.

Gracefully restart the node
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: sh

    nodetool drain
    sudo service scylla-server restart

Validate
~~~~~~~~

1. Check cluster status with ``nodetool status`` and make sure **all**
   nodes, including the one you just upgraded, are in UN status.
2. Check ``/var/log/upstart/scylla-server.log`` and ``/var/log/syslog``
   to validate there are no errors.
3. Check again after 2 minutes, to validate no new issues are
   introduced.

Once you are sure the node upgrade is successful, move to the next node
in the cluster.

.. _upgrade-1.0-1.1-ubuntu-rollback-procedure:

Rollback Procedure
------------------

The following procedure describes a rollback from Scylla release 1.1.x
to 1.0.y. Apply this procedure if an upgrade from 1.0 to 1.1 failed
before completing on all nodes. Use this procedure only for nodes you
upgraded to 1.1

Scylla rollback is a rolling procedure which does **not** require full
cluster shutdown. For each of the nodes rollback to 1.0, you will:

-  retrieve the old Scylla packages
-  drain the node
-  restore the configuration file
-  restart Scylla
-  validate the rollback success

Apply the following procedure **serially** on each node. Do not move to
the next node before validating the node is up and running with the new
version.

Rollback steps
--------------

download and install the old release
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. Remove the old repo file.

.. code:: sh

    sudo rm -rf /etc/apt/sources.list.d/scylla.list

2. Update the `Scylla deb repo <http://www.scylladb.com/download/#fndtn-deb>`_ to
   **1.0**
3. install

::

    sudo apt-get update
    sudo apt-get remove --assume-yes scylla-server scylla-jmx scylla-tools
    sudo apt-get install scylla-server scylla-jmx scylla-tools

Answer ‘y’ to the first two questions and 'n' when asked to overwrite
``scylla.yaml``.

Gracefully shutdown Scylla
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: sh

    nodetool drain
    sudo systemctl stop scylla-server.service

Restore the configuration file
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: sh

    sudo rm -rf /etc/scylla/scylla.yaml
    sudo cp -a /etc/scylla/scylla.yaml.backup-1.0 /etc/scylla/scylla.yaml

Start the node
~~~~~~~~~~~~~~

.. code:: sh

    sudo service scylla-server start

Validate
~~~~~~~~

Check upgrade instruction above for validation. Once you are sure the
node rollback is successful, move to the next node in the cluster.
