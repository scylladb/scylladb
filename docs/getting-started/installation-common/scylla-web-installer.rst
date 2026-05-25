==================================
ScyllaDB Web Installer for Linux
==================================

ScyllaDB Web Installer is a platform-agnostic installation script you can run with ``curl`` to install ScyllaDB on Linux.

See :doc:`Install ScyllaDB Linux Packages </getting-started/install-scylla/install-on-linux/>` for information on manually installing ScyllaDB with platform-specific installation packages.

Prerequisites
--------------

Ensure that your platform is supported by the ScyllaDB version you want to install. 
See `OS Support by Platform and Version <https://docs.scylladb.com/stable/versioning/os-support-per-version.html>`_.

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


Installing via Tarball
---------------------------------------

The Web Installer supports tarball installation as an alternative
to the default package manager-based installation. This is useful for installing ScyllaDB
on Linux distributions that are not officially supported via packages, or when you prefer
not to use the system package manager or when you do not have superuser privileges.

To install ScyllaDB using the tarball, run:

.. code:: console

    curl -sSf get.scylladb.com/server | sudo bash -s -- --tarball

You can combine ``--tarball`` with ``--scylla-version`` to install a specific version:

.. code:: console

    curl -sSf get.scylladb.com/server | sudo bash -s -- --tarball --scylla-version |CURRENT_VERSION|

On Linux distributions not supported via native packages, the Web Installer
automatically falls back to tarball installation.

See :doc:`Install ScyllaDB Without root Privileges </getting-started/installation-common/unified-installer>` for more information on the tarball-based (unified) installer.

Install ScyllaDB Manager with Web Installer
----------------------------------------------

You can use the Web Installer to install `ScyllaDB Manager <https://manager.docs.scylladb.com/>`_, which provides automated backup, repair, and cluster management capabilities.

To install ScyllaDB Manager, run:

.. code:: console

    curl -sSf get.scylladb.com/manager | sudo bash

By default, this installs the latest official version of ScyllaDB Manager, including both
``scylla-manager-server`` and ``scylla-manager-client`` packages.

To install a specific version, use the ``--manager-version`` option:

.. code:: console

    curl -sSf get.scylladb.com/manager | sudo bash -s -- --manager-version 3.10

Supported platforms for ScyllaDB Manager installation via Web Installer:

* Debian / Ubuntu
* RHEL / CentOS / Rocky Linux / Amazon Linux

For full ScyllaDB Manager documentation, configuration, and usage instructions,
see `ScyllaDB Manager Docs <https://manager.docs.scylladb.com/>`_.

.. include:: /getting-started/_common/setup-after-install.rst
