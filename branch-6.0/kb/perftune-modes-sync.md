# Updating the Mode in perftune.yaml After a ScyllaDB Upgrade

In versions 5.1 (ScyllaDB Open Source) and 2022.2 (ScyllaDB Enterprise), we improved ScyllaDB’s performance by [removing the rx_queues_count from the mode
condition](https://github.com/scylladb/seastar/pull/949). As a result, ScyllaDB operates in
the `sq_split` mode instead of the `mq` mode (see [Seastar Perftune](https://opensource.docs.scylladb.com/branch-6.0/operating-scylla/admin-tools/perftune.md) for information about the modes).
If you upgrade from an earlier version of ScyllaDB, your cluster’s existing nodes may use the `mq` mode,
while new nodes will use the `sq_split` mode. As using different modes across one cluster is not recommended,
you should change the configuration to ensure that the `sq_split` mode is used on all nodes.

This section describes how to update the perftune.yaml file to configure the `sq_split` mode on all nodes.

## Procedure

The examples below assume that you are using the default locations for storing data and the scylla.yaml file,
and that your NIC is `eth5`.

1. Backup your old configuration.
   ```console
   sudo mv /etc/scylla.d/cpuset.conf /etc/scylla.d/cpuset.conf.old
   sudo mv /etc/scylla.d/perftune.yaml /etc/scylla.d/perftune.yaml.old
   ```
2. Create a new configuration.
   ```console
   sudo scylla_sysconfig_setup --nic eth5 --homedir /var/lib/scylla --confdir /etc/scylla
   ```

   A new `/etc/scylla.d/cpuset.conf` will be generated on the output.
3. Compare the contents of the newly generated `/etc/scylla.d/cpuset.conf` with `/etc/scylla.d/cpuset.conf.old` you created in step 1.
   > - If they are exactly the same, rename `/etc/scylla.d/perftune.yaml.old` you created in step 1 back to `/etc/scylla.d/perftune.yaml` and continue to the next node.
   > - If they are different, move on to the next steps.
4. Restart the `scylla-server` service.
   ```console
   nodetool drain
   sudo systemctl restart scylla-server
   ```
5. Wait for the service to become up and running (similarly to how it is done during a [rolling restart](https://opensource.docs.scylladb.com/branch-6.0/operating-scylla/procedures/config-change/rolling-restart.md)). It may take a considerable amount of time before the node is in the UN state due to resharding.
6. Continue to the next node.
