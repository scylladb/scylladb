# Launch ScyllaDB on AWS

This article will guide you through self-managed ScyllaDB deployment on AWS. For a fully-managed deployment of ScyllaDB
as-a-service, see [ScyllaDB Cloud documentation](https://cloud.docs.scylladb.com/).

## Launching Instances from ScyllaDB AMI

1. Choose your region, and click the **Node** link to open the EC2 instance creation wizard.

   The following table shows the latest patch release. See [AWS Images](https://opensource.docs.scylladb.com/branch-6.0/reference/aws-images.md) for earlier releases.
2. Choose the instance type. See [Cloud Instance Recommendations for AWS](https://opensource.docs.scylladb.com/branch-6.0/getting-started/cloud-instance-recommendations.md#system-requirements-aws) for the list of recommended instances.

   Other instance types will work, but with lesser performance. If you choose an instance type other than the recommended ones, make sure to run the [scylla_setup](https://opensource.docs.scylladb.com/branch-6.0/getting-started/system-configuration.md#system-configuration-scripts) script.
3. Configure your instance details.
   * **Number of instances** – If you are launching more than one instance, make sure to correctly set the IP of the first instance with the `seeds` parameter - either in the User Data (see below) or after launch.
   * **Network** – Configure the network settings.
     * Select your VPC.
     * Configure the security group. Ensure that all [ScyllaDB ports](https://opensource.docs.scylladb.com/branch-6.0/operating-scylla/security/security-checklist.md#networking-ports) are open.
   * **Advanced Details> User Data** – Here, you can add ScyllaDB configuration options in the JSON format.
     See [scylla.yaml](https://opensource.docs.scylladb.com/branch-6.0/operating-scylla/admin.md#admin-scylla-yaml) for information about supported options.
     <!-- TODO Replace the link to scylla.yaml to the full list of supported options - when all the options are documented. -->

     The following example shows a configuration using the most popular options.
     * `cluster_name` - The name of the cluster.
     * `seed_provider` - The IP of the first node. New nodes will use the IP of this seed node to connect to the cluster and learn the cluster topology and state. See [ScyllaDB Seed Nodes](https://opensource.docs.scylladb.com/branch-6.0/kb/seed-nodes.md).
     * `post_configuration_script` - A base64 encoded bash script that will be executed after the configuration is completed.
     * `start_scylla_on_first_boot` - Starts ScyllaDB once the configuration is completed.

     Example:
     ```json
     {
          "scylla_yaml": {
              "cluster_name": "test-cluster",
              "seed_provider": [{"class_name": "org.apache.cassandra.locator.SimpleSeedProvider",
                                 "parameters": [{"seeds": "10.0.219.209"}]}],
          },
          "post_configuration_script": "#! /bin/bash\nyum install cloud-init-cfn",
          "start_scylla_on_first_boot": true
     }
     ```

     For full documentation of ScyllaDB AMI user data, see the [ScyllaDB Image documentation](https://github.com/scylladb/scylla-machine-image).
4. Add storage.
   * ScyllaDB AMI requires XFS to work. You **must** attach at least one drive for ScyllaDB to use as XFS for the data directory.
     When attaching more than one drive, the AMI setup will install RAID0 on all of them.
   * The ScyllaDB AMI requires at least two instance store volumes. The ScyllaDB data directory will be formatted with XFS when the instance
     first boots. ScyllaDB will fail to start if only one volume is configured.
5. Tag your instance.
6. Click **Launch Cluster**. You now have a running ScyllaDB cluster on EC2.
7. Connect to the servers using the username `scyllaadm`.
   ```console
   ssh -i your-key-pair.pem scyllaadm@ec2-public-ip
   ```

   The default file paths:
   * The `scylla.yaml` file: `/etc/scylla/scylla.yaml`
   * Data: `/var/lib/scylla/`

   To check that the ScyllaDB server and the JMX component are running, run:
   ```console
   nodetool status
   ```

## Next Steps

* [Configure ScyllaDB](https://opensource.docs.scylladb.com/branch-6.0/getting-started/system-configuration.md)
* Manage your clusters with [ScyllaDB Manager](https://manager.docs.scylladb.com/)
* Monitor your cluster and data with [ScyllaDB Monitoring](https://monitoring.docs.scylladb.com/)
* Get familiar with ScyllaDB’s [command line reference guide](https://opensource.docs.scylladb.com/branch-6.0/operating-scylla/nodetool.md).
* Learn about ScyllaDB at [ScyllaDB University](https://university.scylladb.com/)
