# Failed Decommission

This article describes the troubleshooting procedure when node decommission fails.

During decommissioning, the streaming process starts, and the node streams its data to the other nodes in the Scylla cluster.
The process may fail if the node fails to read from the HDD or a network problem occurs.

## Problem

The node is stuck in the decommission status.

## How to Verify

Run the `nodetool status` command to check the node status. The expected result is `UL` (Up Leaving).

Check the node status from the other nodes in the cluster to verify that the status of the decommissioned node status is `UL`.

#### NOTE
The `nodetool netstats` command does not show ongoing streaming.

The following error message will appear in the [logs](/getting-started/logging/):

```shell
nodetool: Scylla API server HTTP POST to URL '/storage_service/decommission' failed: stream_ranges failed
```

## Solution

1. Restart the node you are decommissioning.
   > Supported OS
   > ```shell
   > sudo systemctl restart scylla-server
   > ```

   > Docker
   > ```shell
   > docker exec -it some-scylla supervisorctl restart scylla
   > ```

   > (without restarting *some-scylla* container)
2. Run the `nodetool status` command to verify the node is in `UN` status.
3. Run `nodetool decommission` again.

[Troubleshoot](https://opensource.docs.scylladb.com/branch-5.2/troubleshooting/index.md)
