# Nodetool version

**version** - Displays the Apache Cassandra version which your version of ScyllaDB is most compatible with, not your current ScyllaDB version.
To display the ScyllaDB version, refer to [Check your current version of ScyllaDB](https://opensource.docs.scylladb.com/branch-6.1/operating-scylla/admin.md#check-your-current-version-of-scylla).
To display additional compatibility metrics, such as CQL spec version, refer to [SHOW VERSION](https://opensource.docs.scylladb.com/branch-6.1/cql/cqlsh.md#cqlsh-show-version).

For example:

```sh
nodetool version
```

Returns (your results may be different):

```none
ReleaseVersion: 3.0.8
```

[Nodetool Reference](https://opensource.docs.scylladb.com/branch-6.1/operating-scylla/nodetool.md)
