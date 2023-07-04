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

For installation without root privileges, follow the instructions on `Scylla Download Center <https://www.scylladb.com/download/?platform=tar>`_.

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
