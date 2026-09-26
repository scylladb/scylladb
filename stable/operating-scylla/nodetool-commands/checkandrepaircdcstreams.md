# Nodetool checkAndRepairCdcStreams

Checks if CDC streams reflect the current cluster topology, and regenerates them if they don’t.

#### WARNING
Do not use this operation while performing other administrative tasks, such as
bootstrapping or decommissioning a node.

## Usage

```console
nodetool checkAndRepairCdcStreams
```

## See Also

[Change Data Capture (CDC)](https://opensource.docs.scylladb.com/stable/features/cdc/index.md)

[Upgrading from experimental CDC](https://opensource.docs.scylladb.com/stable/kb/cdc-experimental-upgrade.md)

[Nodetool Reference](https://opensource.docs.scylladb.com/stable/operating-scylla/nodetool.md)
