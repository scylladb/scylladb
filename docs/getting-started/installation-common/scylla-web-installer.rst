==================================
ScyllaDB Web Installer for Linux
==================================

ScyllaDB Web Installer is a platform-agnostic installation script you can run with ``curl`` to install ScyllaDB on Linux.

See :doc:`Install ScyllaDB Linux Packages </getting-started/install-scylla/install-on-linux/>` for information on manually installing ScyllaDB with platform-specific installation packages.

Prerequisites
--------------

Ensure that your platform is supported by the ScyllaDB version you want to install. 
See :doc:`OS Support by Platform and Version </getting-started/os-support/>`.

Install ScyllaDB with Web Installer
---------------------------------------
To install ScyllaDB with Web Installer, run:

.. code:: console

    curl -sSf get.scylladb.com/server | sudo bash

By default, running the script installs the latest official version of ScyllaDB. 

You can run the command with the ``-h`` or ``--help`` flag to print information about the script.

Installing a Non-default Version
---------------------------------------

You can install a version other than the default.

Versions 2025.1 and Later
==============================

Run the command with the ``--scylla-version`` option to specify the version
you want to install.

**Example**

.. code:: console
  
  curl -sSf get.scylladb.com/server | sudo bash -s -- --scylla-version 2025.1.1


Versions Earlier than 2025.1
================================

To install a supported version of *ScyllaDB Enterprise*, run the command with:

* ``--scylla-product scylla-enterprise`` to specify that you want to install
  ScyllaDB Entrprise.
* ``--scylla-version`` to specify the version you want to install.

For example:

.. code:: console
  
  curl -sSf get.scylladb.com/server | sudo bash -s -- --scylla-product scylla-enterprise --scylla-version 2024.1

To install a supported version of *ScyllaDB Open Source*, run the command with
the ``--scylla-version`` option to specify the version you want to install.

For example:

.. code:: console
  
  curl -sSf get.scylladb.com/server | sudo bash -s -- --scylla-version 6.2.1

.. include:: /getting-started/_common/setup-after-install.rst