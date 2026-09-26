# ScyllaDB Rust Driver

The ScyllaDB Rust driver is a client-side, shard-aware driver written in pure Rust with a fully async API using Tokio.
Optimized for ScyllaDB, the driver is also compatible with Apache Cassandra®.

![image](using-scylla/drivers/cql-drivers/images/monster-rust.png)

**To download and install the driver**, visit the [Github project](https://github.com/scylladb/scylla-rust-driver).

Read the [Documentation](https://rust-driver.docs.scylladb.com).

## Using CDC with Rust

When writing applications, you can use ScyllaDB’s [Rust CDC Library](https://github.com/scylladb/scylla-cdc-rust)
to simplify writing applications that read from ScyllaDB’s CDC.

Use [Rust CDC Library](https://github.com/scylladb/scylla-cdc-rust) to read
[ScyllaDB’s CDC](https://opensource.docs.scylladb.com/branch-5.4/using-scylla/cdc/index.md) update streams.
