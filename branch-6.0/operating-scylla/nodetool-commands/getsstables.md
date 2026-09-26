# Nodetool getsstables

**getsstables** `[-hf | --hex-format] <keyspace> <table> <key>` - Gets the SStables that contain the given key.

## Syntax

```console
nodetool getsstables [-hf | --hex-format] <keyspace> <table> <key>
```

## Options

| Parameter         | Description                                  |
|-------------------|----------------------------------------------|
| -hf / –hex-format | Specify the key in hexadecimal string format |

## Examples

For example:

```shell
nodetool getsstables ks tbl mykey
```

Example output:

```shell
/var/lib/scylla/data/ks/tbl-3ca78460d61611eea0b49524e39553c0/me-3gec_0mu7_5az0024x96bfm476r6-big-Data.db
/var/lib/scylla/data/ks/tbl-3ca78460d61611eea0b49524e39553c0/me-3gec_0mu7_7bz0024x96bfm476r6-big-Data.db
```

## See Also

[Nodetool Reference](https://opensource.docs.scylladb.com/branch-6.0/operating-scylla/nodetool.md)
