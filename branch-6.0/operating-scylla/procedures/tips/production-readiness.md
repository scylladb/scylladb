# Production Readiness Guidelines

The goal of this document is to have a checklist that production customers can use to make sure their
deployment adheres to ScyllaDB’s recommendations.
Before deploying to production you should follow up on each of the main bullets described below to verify they comply with the
recommendations provided. Click the links for more information on each topic.

## Before You Begin

### Pre-Deployment Requirements

* [Scylla System Requirements](https://opensource.docs.scylladb.com/branch-6.0/getting-started/system-requirements.md) - verify your instances, system, OS, etc are supported by Scylla for production machines.
* [Scylla Getting Started](https://opensource.docs.scylladb.com/branch-6.0/getting-started/index.md)

### Choose a Compaction Strategy

Each workload may require a specific strategy. Refer to [Choose a Compaction Strategy](https://opensource.docs.scylladb.com/branch-6.0/architecture/compaction/compaction-strategies.md) for details.

## Resiliency

When rolling out to production it is important to make sure your data is recoverable and your database can function anytime there is a power or equipment failure.

### Replication Factors

Verify the [Replication Factor](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Replication-Factor-RF) is set properly **for each keyspace**.

We recommend using an  of **at least** three.

If you have a multi-datacenter architecture we recommend to have `RF=3` on each DC.

For additional information:

* Read more about [Scylla Fault Tolerance](https://opensource.docs.scylladb.com/branch-6.0/architecture/architecture-fault-tolerance.md)
* Take a course at [Scylla University on RF](https://university.scylladb.com/courses/scylla-essentials-overview/lessons/high-availability/topic/fault-tolerance-replication-factor/).

### Consistency Levels

Verify the [Consistency Level (CL)](https://opensource.docs.scylladb.com/branch-6.0/reference/glossary.md#term-Consistency-Level-CL) is set properly **for each table**.

We recommend using `LOCAL_QUORUM` across **the cluster and DCs**

For additional information:

* Refer to [Scylla Fault Tolerance](https://opensource.docs.scylladb.com/branch-6.0/architecture/architecture-fault-tolerance.md)
* Watch a [Demo](https://opensource.docs.scylladb.com/branch-6.0/architecture/console-CL-full-demo.md)
* Take a course at [Scylla University on CL](https://university.scylladb.com/courses/scylla-essentials-overview/lessons/high-availability/topic/consistency-level/)

### Gossip Configuration

1. Choose the correct Snitch.

   **Always use** `GossipingPropertyFileSnitch` or `Ec2MultiRegionSnitch`
   **Do Not** use SimpleStrategy on any production machine, even if you only have a single DC.

   For additional information:
   * Refer to [Gossip in Scylla](https://opensource.docs.scylladb.com/branch-6.0/kb/gossip.md)
   * Follow the [How to Switch Snitches](https://opensource.docs.scylladb.com/branch-6.0/operating-scylla/procedures/config-change/switch-snitch.md) procedure if required.
   * Take a course at [Scylla University on Gossip](https://university.scylladb.com/courses/scylla-essentials-overview/lessons/architecture/topic/gossip/)
2. Use the correct Data Replication strategy.

   Use `NetworkTopologyStrategy` replication-strategy as it supports multi-DC for your keyspaces.

## Performance

Verify you have run scylla_setup in order to tune ScyllaDB to your hardware.

If you are running on a physical hardware please take a look into the following configuration files:

* [perftune.yaml]()
* [cpuset.conf]()

### perftune.yaml

If you have more than 8 cores or 16 vcpu **always use** `mode: sq_split`

### cpuset.conf

Make sure that the configuration in `/etc/scylla.d/cpuset.conf` corresponds to `sq_split` and that the  hyperthreads of physical core 0 are excluded from the CPU list.

## Compression

<!-- note: Compression trades CPU for networking so this trade-off may be expensive for you and may not be beneficial. -->

### Inter-node Compression

Enable Inter-node Compression by editing the Scylla Configuration file (/etc/scylla.yaml).

`internode_compression: all`

For additional information, see the Admin Guide [Inter-node Compression](https://opensource.docs.scylladb.com/branch-6.0/operating-scylla/admin.md#internode-compression) section.

### Driver Compression

This refers to compressing traffic between the client and Scylla.
Verify your client driver is using compressed traffic when connected to Scylla.
As compression is driver settings dependent, please check your client driver manual or [Scylla Drivers](https://opensource.docs.scylladb.com/branch-6.0/using-scylla/drivers/index.md).

## Connectivity

### Drivers Settings

* Use shard aware drivers wherever possible. [Scylla Drivers](https://opensource.docs.scylladb.com/branch-6.0/using-scylla/drivers/index.md) (not third-party drivers) are shard aware.
* Configure connection pool - open more connections (>3 per shard) and/Or more clients. See [this blog](https://www.scylladb.com/2019/11/20/maximizing-performance-via-concurrency-while-minimizing-timeouts-in-distributed-databases/).

## Management

You must use both Scylla Manager and Scylla Monitor.

### Scylla Manager

Scylla Manager enables centralized cluster administration and database
automation such as repair, backup, and health-check.

#### Repair

Run repairs preferably once a week and run them exclusively from Scylla Manager.
Refer to [Repair a Cluster](https://manager.docs.scylladb.com/branch-2.2/repair/index.html)

#### Backup and Restore

We recommend the following:

* Run a full weekly backup from Scylla Manager
* Run a daily backup from Scylla Manager
* Check the bucket used for restore. This can be done by performing a [restore](https://manager.docs.scylladb.com/branch-2.2/restore/index.html) and making sure the data is valid. This action should be done once a month, or more frequently if needed. Ask our Support team to help you with this.
* Save backup to a bucket supported by Scylla Manager.

For additional information:

* [Backup](https://manager.docs.scylladb.com/branch-2.2/backup/index.html)
* [Restore a Backup](https://manager.docs.scylladb.com/branch-2.2/restore/index.html)

### ScyllaDB Monitoring Stack

ScyllaDB Monitoring Stack helps you monitor everything about your ScyllaDB cluster. ScyllaDB Support team
usually asks for your monitoring metrics when you open a ticket.

See [ScyllaDB Monitoring Stack](https://monitoring.docs.scylladb.com/stable/) for details.

### Configuration Management

Using tools such as Ansible, Chef, Puppet, Salt, or Juju are recommended.

See this [article](https://www.softwaretestinghelp.com/top-5-software-configuration-management-tools/) for more information.

## Security

Use the following guidelines to keep your data and database secure.

* Enable [Authentication](https://opensource.docs.scylladb.com/branch-6.0/operating-scylla/security/authentication.md)
* Create Roles for all users and use [RBAC](https://opensource.docs.scylladb.com/branch-6.0/operating-scylla/security/rbac-usecase.md) with or without LDAP (coming soon).
* Use Encryption in Transit [between nodes](https://opensource.docs.scylladb.com/branch-6.0/operating-scylla/security/node-node-encryption.md) and [client to node](https://opensource.docs.scylladb.com/branch-6.0/operating-scylla/security/client-node-encryption.md).
* Refer to the [Security Checklist](https://opensource.docs.scylladb.com/branch-6.0/operating-scylla/security/security-checklist.md) for more information.

## HA Testing

HA testing in single DC - for example:

1. Shutdown one node from the cluster (Or scylla service if on the cloud) for 30 min.
2. Turn it back on.

HA testing in multi DC - for example:

1. Disconnect one DC from the other by stopping scylla service on all of these DC
   nodes.
2. Reconnect the DC.

## Additional Topics

* [Add a Node](https://opensource.docs.scylladb.com/branch-6.0/operating-scylla/procedures/cluster-management/add-node-to-cluster.md)
* [Repair](https://manager.docs.scylladb.com/branch-2.2/repair/index.html)
* [Cleanup](https://opensource.docs.scylladb.com/branch-6.0/operating-scylla/nodetool-commands/cleanup.md)
* Tech Talk: [How to be successful with Scylla](https://www.scylladb.com/tech-talk/how-to-be-successful-with-scylla/)
