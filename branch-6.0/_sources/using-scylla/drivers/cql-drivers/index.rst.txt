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

We recommend using ScyllaDB drivers. All ScyllaDB drivers are shard-aware and provide additional 
benefits over third-party drivers.

ScyllaDB supports the CQL binary protocol version 3, so any Apache Cassandra/CQL driver that implements 
the same version works with Scylla.

The following table lists the available ScyllaDB drivers, specifying which support
`ScyllaDB Cloud Serversless <https://cloud.docs.scylladb.com/stable/serverless/index.html>`_ 
or include a library for :doc:`CDC </using-scylla/cdc/cdc-intro>`.

.. list-table:: 
   :widths: 30 35 35 
   :header-rows: 1

   * - 
     - ScyllaDB Driver
     - CDC Connector
   * - :doc:`Python</using-scylla/drivers/cql-drivers/scylla-python-driver>`
     - |v| 
     - |x| 
   * - :doc:`Java </using-scylla/drivers/cql-drivers/scylla-java-driver>`
     - |v| 
     - |v|
   * - :doc:`Go </using-scylla/drivers/cql-drivers/scylla-go-driver>`
     - |v| 
     - |v|
   * - :doc:`Go Extension </using-scylla/drivers/cql-drivers/scylla-gocqlx-driver>`
     - |v|
     - |x| 
   * - :doc:`C++ </using-scylla/drivers/cql-drivers/scylla-cpp-driver>`
     - |v|
     - |x| 
   * - :doc:`Rust </using-scylla/drivers/cql-drivers/scylla-rust-driver>`
     - |v| 
     - |v| 


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

Learn about ScyllaDB Drivers on ScyllaDB University
----------------------------------------------------

  The free `Using ScyllaDB Drivers course <https://university.scylladb.com/courses/using-scylla-drivers/>`_ 
  on ScyllaDB University covers the use of drivers in multiple languages to interact with a ScyllaDB 
  cluster. The languages covered include Java, CPP, Rust, Golang, Python, Node.JS, Scala, and others.
