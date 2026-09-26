# Nodetool version

**version** - Displays the Apache Cassandra version which your version of Scylla is most compatible with, not your current Scylla version.
To display the Scylla version, refer to [Check your current version of Scylla](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/admin.md#check-your-current-version-of-scylla).
To display additional compatibility metrics, such as CQL spec version, refer to [SHOW VERSION](https://opensource.docs.scylladb.com/branch-5.2/cql/cqlsh.md#cqlsh-show-version).

For example:

```sh
nodetool version
```

Returns (your results may be different):

```none
ReleaseVersion: 3.0.8
```

[Nodetool Reference](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/nodetool.md)
