# ScyllaDB CQL Drivers

## ScyllaDB Drivers

We recommend using ScyllaDB drivers. All ScyllaDB drivers are shard-aware and provide additional
benefits over third-party drivers.

ScyllaDB supports the CQL binary protocol version 3, so any Apache Cassandra/CQL driver that implements
the same version works with ScyllaDB.

The following table lists the available ScyllaDB drivers, specifying which support
[ScyllaDB Cloud Serversless](https://cloud.docs.scylladb.com/stable/serverless/index.html)
or include a library for [CDC](https://opensource.docs.scylladb.com/branch-6.1/using-scylla/cdc/cdc-intro.md).

|                                                                                                                          | ScyllaDB Driver                                           | CDC Connector                                              |
|--------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------|------------------------------------------------------------|
| [Python](https://opensource.docs.scylladb.com/branch-6.1/using-scylla/drivers/cql-drivers/scylla-python-driver.md)       | <i class="inline-icon icon-check" aria-hidden="true"></i> | <i class="inline-icon icon-cancel" aria-hidden="true"></i> |
| [Java](https://opensource.docs.scylladb.com/branch-6.1/using-scylla/drivers/cql-drivers/scylla-java-driver.md)           | <i class="inline-icon icon-check" aria-hidden="true"></i> | <i class="inline-icon icon-check" aria-hidden="true"></i>  |
| [Go](https://opensource.docs.scylladb.com/branch-6.1/using-scylla/drivers/cql-drivers/scylla-go-driver.md)               | <i class="inline-icon icon-check" aria-hidden="true"></i> | <i class="inline-icon icon-check" aria-hidden="true"></i>  |
| [Go Extension](https://opensource.docs.scylladb.com/branch-6.1/using-scylla/drivers/cql-drivers/scylla-gocqlx-driver.md) | <i class="inline-icon icon-check" aria-hidden="true"></i> | <i class="inline-icon icon-cancel" aria-hidden="true"></i> |
| [C++](https://opensource.docs.scylladb.com/branch-6.1/using-scylla/drivers/cql-drivers/scylla-cpp-driver.md)             | <i class="inline-icon icon-check" aria-hidden="true"></i> | <i class="inline-icon icon-cancel" aria-hidden="true"></i> |
| [Rust](https://opensource.docs.scylladb.com/branch-6.1/using-scylla/drivers/cql-drivers/scylla-rust-driver.md)           | <i class="inline-icon icon-check" aria-hidden="true"></i> | <i class="inline-icon icon-check" aria-hidden="true"></i>  |

## Third-party Drivers

You can find the third-party driver documentation on the GitHub pages for each driver:

* [DataStax Java Driver](https://github.com/datastax/java-driver/)
* [DataStax Python Driver](https://github.com/datastax/python-driver/)
* [DataStax C# Driver](https://github.com/datastax/csharp-driver/)
* [DataStax Ruby Driver](https://github.com/datastax/ruby-driver/)
* [DataStax Node.js Driver](https://github.com/datastax/nodejs-driver/)
* [DataStax C++ Driver](https://github.com/datastax/cpp-driver/)
* [DataStax PHP Driver (Supported versions: 7.1)](https://github.com/datastax/php-driver)
* [He4rt PHP Driver (Supported versions: 8.1 and 8.2)](https://github.com/he4rt/scylladb-php-driver/)
* [Scala Phantom Project](https://github.com/outworkers/phantom)
* [Xandra Elixir Driver](https://github.com/lexhide/xandra)

## Learn about ScyllaDB Drivers on ScyllaDB University

> The free [Using ScyllaDB Drivers course](https://university.scylladb.com/courses/using-scylla-drivers/)
> on ScyllaDB University covers the use of drivers in multiple languages to interact with a ScyllaDB
> cluster. The languages covered include Java, CPP, Rust, Golang, Python, Node.JS, Scala, and others.
