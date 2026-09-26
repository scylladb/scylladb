# What to do if a Node Starts Automatically

If, for any reason, the Scylla service started before you had a chance to update the configuration file, some of the system tables may already reflect an incorrect status, and unfortunately, a simple restart will not fix the issue.
In this case, the safest way is to stop the service, clean all of the data, and start the service again.

## Procedure

1. Stop the Scylla service.

   Supported OS
   ```shell
   sudo systemctl stop scylla-server
   ```

   Docker
   ```shell
   docker exec -it some-scylla supervisorctl stop scylla
   ```

   (without stopping *some-scylla* container)
2. Delete the Data and Commitlog folders.
   ```sh
   sudo rm -rf /var/lib/scylla/data
   sudo find /var/lib/scylla/commitlog -type f -delete
   sudo find /var/lib/scylla/hints -type f -delete
   sudo find /var/lib/scylla/view_hints -type f -delete
   ```
3. Start the Scylla service.

   Supported OS
   ```shell
   sudo systemctl start scylla-server
   ```

   Docker
   ```shell
   docker exec -it some-scylla supervisorctl start scylla
   ```

   (with *some-scylla* container already running)
4. Run ‘nodetool status’ to verify all nodes are up and joined.

## Additional Topics

[Create a Scylla Cluster - Single Data Center (DC)](https://opensource.docs.scylladb.com/branch-6.0/operating-scylla/procedures/cluster-management/create-cluster.md)

[Scylla Procedures](https://opensource.docs.scylladb.com/branch-6.0/operating-scylla/procedures/cluster-management/index.md)
