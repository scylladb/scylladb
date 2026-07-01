===================
Scylla CQL Drivers
===================

.. toctree::
   :titlesonly:
   :hidden:

   scylla-python-driver
   scylla-java-driver
   scylla-go-driver
   scylla-gocqlx-driver
   scylla-cpp-driver
   scylla-rust-driver

All Scylla Drivers are Shard-Aware and provide additional benefits over third-party drivers.

It is always recommended to use the Scylla drivers over third-party drivers.

Scylla supports the CQL binary protocol version 3, so any Apache Cassandra/CQL driver that implements the same version, works with Scylla.



.. panel-box::
  :title: Scylla Drivers
  :id: "getting-started"
  :class: my-panel

  All Scylla Drivers are Shard-Aware and provide additional benefits over third-party drivers.

  * :doc:`Scylla Python Driver </using-scylla/drivers/cql-drivers/scylla-python-driver>`
  * :doc:`Scylla Java Driver </using-scylla/drivers/cql-drivers/scylla-java-driver>` - now includes a library for CDC
  * :doc:`Scylla Go Driver </using-scylla/drivers/cql-drivers/scylla-go-driver>` - now includes a library for CDC
  * :doc:`Scylla Go Extension Driver </using-scylla/drivers/cql-drivers/scylla-gocqlx-driver>`
  * :doc:`Scylla C++ Driver </using-scylla/drivers/cql-drivers/scylla-cpp-driver>`
  * :doc:`Scylla Rust Driver </using-scylla/drivers/cql-drivers/scylla-rust-driver>` - available for preview

.. panel-box::
  :title: Third-party Drivers
  :id: "getting-started"
  :class: my-panel

  You can find the third-party driver documentation on the GitHub pages for each driver:

  * `DataStax Java Driver <https://github.com/datastax/java-driver/>`_
  * `DataStax Python Driver <https://github.com/datastax/python-driver/>`_
  * `DataStax C# Driver <https://github.com/datastax/csharp-driver/>`_
  * `DataStax Ruby Driver <https://github.com/datastax/ruby-driver/>`_
  * `DataStax Node.js Driver <https://github.com/datastax/nodejs-driver/>`_
  * `DataStax C++ Driver <https://github.com/datastax/cpp-driver/>`_
  * `DataStax PHP Driver  <https://github.com/datastax/php-driver/>`_
  * `Rust <https://github.com/AlexPikalov/cdrs>`_
  * `Scala Phantom Project <https://github.com/outworkers/phantom>`_

.. panel-box::
  :title: Lessons on Scylla University
  :id: "getting-started"
  :class: my-panel

  The free `Using Scylla Drivers course <https://university.scylladb.com/courses/using-scylla-drivers/>`_ on Scylla University covers the use of drivers in multiple languages to interact with a Scylla cluster. Some of the languages covered include: Java, CPP, Rust, Golang, Python, Node.JS, Scala and others.
