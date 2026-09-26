# Launch ScyllaDB on GCP

This article will guide you through self-managed ScyllaDB deployment on GCP. For a fully-managed deployment of ScyllaDB
as-a-service, see [ScyllaDB Cloud documentation](https://cloud.docs.scylladb.com/).

## Prerequisites

* Active GCP account
* [Google SDK](https://cloud.google.com/sdk/docs/install), which includes the `gcloud` command-line tool
* ScyllaDB Image requires at least 2 vCPU servers.

## Launching ScyllaDB on GCP

1. Choose an instance type. See [Cloud Instance Recommendations for GCP](https://opensource.docs.scylladb.com/branch-6.0/getting-started/cloud-instance-recommendations.md#system-requirements-gcp) for the list of recommended instances.

   Other instance types will work, but with lesser performance. If you choose an instance type other than the recommended ones, make sure to run the [scylla_setup](https://opensource.docs.scylladb.com/branch-6.0/getting-started/system-configuration.md#system-configuration-scripts) script.
2. See the following table to obtain image information for the latest patch release.
   For earlier releases, see [GCP Images](https://opensource.docs.scylladb.com/branch-6.0/reference/gcp-images.md)
   <!-- -*- mode: rst -*- -->

   ### 6.0.4

   | Image Name     |            Image ID |
   |----------------|---------------------|
   | scylladb-6-0-4 | 5215863022418167664 |
3. Launch a ScyllaDB instance on GCP with `gcloud` using the information from the previous step. Use the following syntax:
   ```console
   gcloud compute instances create <name of new instance> --image <ScyllaDB image name> --image-project < ScyllaDB project name> --local-ssd interface=nvme --zone <GCP zone - optional> --machine-type=<machine type>
   ```

   For example:
   ```console
   gcloud compute instances create scylla-node1 --image scylladb-5-2-1 --image-project scylla-images --local-ssd interface=nvme --machine-type=n1-highmem-8
   ```

   To add more storage to the VM, add multiple `--local-ssd interface=nvme` options to the command. For example, the following
   command will launch a VM with 4 SSD, and 1.5TB of data (4 \* [375 GB](https://cloud.google.com/compute/docs/disks/local-ssd)):
   ```console
   gcloud compute instances create scylla-node1 --image scylladb-5-2-1 --image-project scylla-images --local-ssd interface=nvme --local-ssd interface=nvme --local-ssd interface=nvme --local-ssd interface=nvme --machine-type=n1-highmem-8
   ```

   For more information about GCP image create see the [Google Cloud SDK documentation](https://cloud.google.com/sdk/gcloud/reference/compute/images/create).
4. (Optional) Configure firewall rules.

   Ensure that all [ScyllaDB ports](https://opensource.docs.scylladb.com/branch-6.0/operating-scylla/security/security-checklist.md#networking-ports) are open.
5. Connect to the servers:
   > > ```console
   > > gcloud compute ssh <name of the created instance>
   > > ```

   > For example:
   > > ```console
   > > gcloud compute ssh scylla-node1
   > > ```

   To check that the ScyllaDB server and the JMX component are running, run:
   > ```console
   > nodetool status
   > ```

## Next Steps

* [Configure ScyllaDB](https://opensource.docs.scylladb.com/branch-6.0/getting-started/system-configuration.md)
* Manage your clusters with [ScyllaDB Manager](https://manager.docs.scylladb.com/)
* Monitor your cluster and data with [ScyllaDB Monitoring](https://monitoring.docs.scylladb.com/)
* Get familiar with ScyllaDB’s [command line reference guide](https://opensource.docs.scylladb.com/branch-6.0/operating-scylla/nodetool.md).
