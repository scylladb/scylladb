=====================
ScyllaDB CQL Drivers
=====================

.. toctree::
   :titlesonly:
   :hidden:

   scylla-python-driver
   scylla-java-driver
   scylla-go-driver
   scylla-gocqlx-driver
   scylla-cpp-driver
   scylla-rust-driver

ScyllaDB Drivers
-----------------

The following ScyllaDB drivers are available:

* :doc:`Python Driver</using-scylla/drivers/cql-drivers/scylla-python-driver>`
* :doc:`Java Driver </using-scylla/drivers/cql-drivers/scylla-java-driver>`
* :doc:`Go Driver </using-scylla/drivers/cql-drivers/scylla-go-driver>`
* :doc:`Go Extension </using-scylla/drivers/cql-drivers/scylla-gocqlx-driver>`
* :doc:`C++ Driver </using-scylla/drivers/cql-drivers/scylla-cpp-driver>`
* `CPP-over-Rust Driver <https://github.com/scylladb/cpp-rust-driver>`_
* :doc:`Rust Driver </using-scylla/drivers/cql-drivers/scylla-rust-driver>`
* `C# Driver <https://csharp-driver.docs.scylladb.com/>`_

We recommend using ScyllaDB drivers. All ScyllaDB drivers are shard-aware and provide additional 
benefits over third-party drivers.

ScyllaDB supports the CQL binary protocol version 3, so any Apache Cassandra/CQL driver that implements 
the same version works with ScyllaDB.

CDC Integration with ScyllaDB Drivers
-------------------------------------------

The following table specifies which ScyllaDB drivers include a library for
:doc:`CDC </features/cdc/cdc-intro>`.

.. list-table:: 
   :widths: 40 60
   :header-rows: 1

   * - ScyllaDB Driver
     - CDC Connector
   * - :doc:`Python </using-scylla/drivers/cql-drivers/scylla-python-driver>`
     - |x| 
   * - :doc:`Java </using-scylla/drivers/cql-drivers/scylla-java-driver>`
     - |v|
   * - :doc:`Go </using-scylla/drivers/cql-drivers/scylla-go-driver>`
     - |v|
   * - :doc:`Go Extension </using-scylla/drivers/cql-drivers/scylla-gocqlx-driver>`
     - |x| 
   * - :doc:`C++ </using-scylla/drivers/cql-drivers/scylla-cpp-driver>`
     - |x| 
   * - `CPP-over-Rust Driver <https://github.com/scylladb/cpp-rust-driver>`_
     - |x|
   * - :doc:`Rust </using-scylla/drivers/cql-drivers/scylla-rust-driver>`
     - |v|
   * - `C# Driver <https://csharp-driver.docs.scylladb.com/>`_
     - |x| 

Support for Tablets
-------------------------

The following table specifies which ScyllaDB drivers support
:doc:`tablets </architecture/tablets>` and since which version.

.. list-table:: 
   :widths: 30 35 35
   :header-rows: 1

   * - ScyllaDB Driver
     - Support for Tablets
     - Since Version
   * - :doc:`Python</using-scylla/drivers/cql-drivers/scylla-python-driver>`
     - |v| 
     - 3.26.5
   * - :doc:`Java </using-scylla/drivers/cql-drivers/scylla-java-driver>`
     - |v| 
     - 4.18.0 (Java Driver 4.x)

       3.11.5.2 (Java Driver 3.x)
   * - :doc:`Go </using-scylla/drivers/cql-drivers/scylla-go-driver>`
     - |v|
     - 1.13.0
   * - :doc:`Go Extension </using-scylla/drivers/cql-drivers/scylla-gocqlx-driver>`
     - |x| 
     - N/A
   * - :doc:`C++ </using-scylla/drivers/cql-drivers/scylla-cpp-driver>`
     - |x| 
     - N/A
   * - `CPP-over-Rust Driver <https://github.com/scylladb/cpp-rust-driver>`_
     - |v|
     - All versions
   * - :doc:`Rust </using-scylla/drivers/cql-drivers/scylla-rust-driver>`
     - |v| 
     - 0.13.0
   * - `C# Driver <https://csharp-driver.docs.scylladb.com/>`_
     - |v|
     - All versions

Driver Support Policy
-------------------------------

We support the **two most recent minor releases** of our drivers.

* We test and validate the latest two minor versions.
* We typically patch only the latest minor release.

We recommend staying up to date with the latest supported versions to receive
updates and fixes.

At a minimum, upgrade your driver when upgrading to a new ScyllaDB version
to ensure compatibility between the driver and the database.

Third-party Drivers
----------------------

You can find the third-party driver documentation on the GitHub pages for each driver:

* `DataStax Java Driver <https://github.com/datastax/java-driver/>`_
* `DataStax Python Driver <https://github.com/datastax/python-driver/>`_
* `DataStax C# Driver <https://github.com/datastax/csharp-driver/>`_
* `DataStax Ruby Driver <https://github.com/datastax/ruby-driver/>`_
* `DataStax Node.js Driver <https://github.com/datastax/nodejs-driver/>`_
* `DataStax C++ Driver <https://github.com/datastax/cpp-driver/>`_
* `DataStax PHP Driver (Supported versions: 7.1)  <https://github.com/datastax/php-driver>`_
* `He4rt PHP Driver (Supported versions: 8.1 and 8.2)  <https://github.com/he4rt/scylladb-php-driver/>`_
* `Scala Phantom Project <https://github.com/outworkers/phantom>`_
* `Xandra Elixir Driver <https://github.com/lexhide/xandra>`_
* `Exandra Elixir Driver <https://github.com/vinniefranco/exandra>`_

Learn about ScyllaDB Drivers on ScyllaDB University
----------------------------------------------------

  The free `Using ScyllaDB Drivers course <https://university.scylladb.com/courses/using-scylla-drivers/>`_ 
  on ScyllaDB University covers the use of drivers in multiple languages to interact with a ScyllaDB 
  cluster. The languages covered include Java, CPP, Rust, Golang, Python, Node.JS, Scala, and others.
