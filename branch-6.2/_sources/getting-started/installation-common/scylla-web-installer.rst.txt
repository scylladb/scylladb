==================================
ScyllaDB Web Installer for Linux
==================================

ScyllaDB Web Installer is a platform-agnostic installation script you can run with ``curl`` to install ScyllaDB on Linux.

See `ScyllaDB Download Center <https://www.scylladb.com/download/#core>`_ for information on manually installing ScyllaDB with platform-specific installation packages.

Prerequisites
--------------

Ensure that your platform is supported by the ScyllaDB version you want to install. 
See :doc:`OS Support by Platform and Version </getting-started/os-support/>`.

Install ScyllaDB with Web Installer
---------------------------------------
To install ScyllaDB with Web Installer, run:

.. code:: console

    curl -sSf get.scylladb.com/server | sudo bash

By default, running the script installs the latest official version of ScyllaDB Open Source. You can use the following 
options to install a different version or ScyllaDB Enterprise:

.. list-table::
   :widths:  20 25 55
   :header-rows: 1

   * - Option
     - Acceptable values
     - Description
   * - ``--scylla-product``
     - ``scylla`` | ``scylla-enterprise``
     - Specifies the ScyllaDB product to install: Open Source (``scylla``) or Enterprise (``scylla-enterprise``)  The default is ``scylla``.
   * - ``--scylla-version``
     - ``<version number>``
     - Specifies the ScyllaDB version to install. You can specify the major release (``x.y``) to install the latest patch for that version or a specific patch release (``x.y.x``). The default is the latest official version.

You can run the command with the ``-h`` or ``--help`` flag to print information about the script.

Examples
===========

Installing ScyllaDB Open Source 6.0.1:

.. code:: console

    curl -sSf get.scylladb.com/server | sudo bash -s -- --scylla-version 6.0.1

Installing the latest patch release for ScyllaDB Open Source 6.0:

.. code:: console

    curl -sSf get.scylladb.com/server | sudo bash -s -- --scylla-version 6.0

Installing ScyllaDB Enterprise 2024.1:

.. code:: console

    curl -sSf get.scylladb.com/server | sudo bash -s -- --scylla-product scylla-enterprise --scylla-version 2024.1

.. include:: /getting-started/_common/setup-after-install.rst