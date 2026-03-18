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

You can install a version other than the default. To get the list of supported
release versions, run:

.. code:: console
  
  curl -sSf get.scylladb.com/server | sudo bash -s -- --list-active-releases


To install a non-default version, run the command with the ``--scylla-version``
option to specify the version you want to install.

**Example**

.. code-block:: console
  :substitutions:
  
  curl -sSf get.scylladb.com/server | sudo bash -s -- --scylla-version |CURRENT_VERSION|


.. include:: /getting-started/_common/setup-after-install.rst