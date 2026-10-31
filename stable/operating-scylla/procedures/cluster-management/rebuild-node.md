# Rebuild a Node After Losing the Data Volume

When running in ScyllaDB on EC2, it is recommended to use i3 type instances, storing the data on fast, ephemeral SSD drives.
Stopping the node and starting it for whatever reason means the data on the node is lost.
This data loss applies not only to i3 type instances but to the i2 type as well.
The good news is ScyllaDB is a HA database, and replicas of the data are stored on additional nodes.
This is also why it’s highly recommended to use a replication factor of at least three per Data Center (for example, 1 DC, RF = 3, 2 DCs RF = 6).

To recover the data and rebuild the node, follow this procedure:

1. Open the `/etc/scylla/scylla.yaml` file.
2. Add, if not present, else edit, the `replace_node_first_boot` parameter and change it to the
   Host ID of the node before it restarted.
3. Stop ScyllaDB Server

   Supported OS
   ```shell
   sudo systemctl stop scylla-server
   ```

   Docker
   ```shell
   docker exec -it some-scylla supervisorctl stop scylla
   ```

   (without stopping *some-scylla* container)
4. If there are multiple disks, execute a RAID setup for the disks by running the following script: `/opt/scylladb/scylla-machine-image/scylla_create_devices`.
5. Start ScyllaDB Server

   Supported OS
   ```shell
   sudo systemctl start scylla-server
   ```

   Docker
   ```shell
   docker exec -it some-scylla supervisorctl start scylla
   ```

   (with *some-scylla* container already running)
6. Revert the `replace_node_first_boot` setting to what they were before you ran this procedure.
   For ease of use, you can comment out the `replace_node_first_boot` parameter.
