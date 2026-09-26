# CQL Command `COPY FROM` fails - field larger than the field limit

This troubleshooting guide describes what to do when Scylla fails to import data using the CQL `COPY FROM` command

## Problem

When trying to use the CQL command `COPY FROM`, the following error message is displayed:

```console
Failed to import XXX rows: Error - field larger than field limit (131072),  given up after Y attempts
```

## Solution

1. Locate your `.cqlshrc` file, which is usually under `$HOME`.
2. Add the following line to that file:

```console
[csv]
field_size_limit = 1000000000
```

1. Optionally, adjust `field_size_limit` according to fit.
2. Try again

Additional References

[Troubleshoot](https://opensource.docs.scylladb.com/branch-6.0/troubleshooting/index.md)

[CQL Reference](https://opensource.docs.scylladb.com/branch-6.0/cql/index.md)
