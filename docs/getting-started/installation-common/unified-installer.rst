====================================================
ScyllaDB Unified Installer (relocatable executable)
====================================================

This document covers how to install, uninstall, and upgrade using the Scylla Unified Installer. The Unified Installer is recommended to be used when you do not have root privileges to the server.
If you have root privileges, it is recommended to download the OS specific packages (RPMs and DEBs) and install them with the package manager (dnf and apt).

Supported distros
=================

See :doc:`OS Support </getting-started/os-support>` for information about supported Linux distributions and versions.

Note that if you're on CentOS 7, only root offline install is supported.

Download and install
====================

For installation without root privileges, follow the instructions on `Scylla Download Center <https://www.scylladb.com/download/?platform=tar>`_

Upgrade / Downgrade/ Uninstall
==============================

.. _unified-installed-upgrade:

Upgrade
-------

The unified package is based on a binary package; it’s not a RPM / DEB packages, so it doesn’t upgrade or downgrade by yum / apt. Currently, only install.sh of scylla supports the upgrade.

Root install:

.. code:: sh

    ./install.sh --upgrade

Nonroot install

.. code:: sh

    ./install.sh --upgrade --nonroot

.. note:: the installation script does not upgrade scylla-jmx and scylla-tools. You will have to do this separately. 

Uninstall
---------

Root uninstall:

.. code:: sh

    sudo ./uninstall.sh

Nonroot uninstall

.. code:: sh

    ./uninstall.sh --nonroot


Downgrade
---------

To downgrade to your original Scylla version, use the Uninstall_ procedure above and then install the original Scylla packages. 
