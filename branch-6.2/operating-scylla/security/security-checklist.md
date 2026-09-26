# ScyllaDB Security Checklist

The ScyllaDB Security checklist is a list of security recommendations that should be implemented to protect your ScyllaDB cluster.

## Enable Authentication

[Authentication](https://opensource.docs.scylladb.com/stable/operating-scylla/security/authentication.md) is a security step to verify the identity of a client. When enabled, ScyllaDB requires all clients to authenticate themselves to determine their access to the cluster.

## Enable Authorization

[Authorization](https://opensource.docs.scylladb.com/stable/operating-scylla/security/enable-authorization.md) is a security step to verify the granted permissions of a client. When enabled, ScyllaDB will check all clients for their access permissions to the cluster objects(keyspaces, tables).

## Role Base Access

Role-Based Access Control ([RBAC](https://opensource.docs.scylladb.com/stable/operating-scylla/security/rbac-usecase.md)), a method of reducing lists of authorized users to a few roles assigned to multiple users. RBAC is sometimes referred to as role-based security. It is recommended to:

* Set [roles](https://opensource.docs.scylladb.com/stable/operating-scylla/security/rbac-usecase.md#rbac-usecase-grant-roles-and-permissions) per keyspace/table.
* Use the [principle of least privilege](https://en.wikipedia.org/wiki/Principle_of_least_privilege#Details) per keyspace/table. [Start](https://opensource.docs.scylladb.com/stable/operating-scylla/security/rbac-usecase.md#rbac-usecase-use-case) by granting no permissions to all roles, then grant read access only to roles who need it, write access to roles who need to write, etc. It’s better to have more roles, each with fewer permissions.

## Encryption on Transit, Client to Node and Node to Node

Encryption on Transit protects your communication against a 3rd interception on the network connection.
Configure ScyllaDB to use TLS/SSL for all the connections. Use TLS/SSL to encrypt communication between ScyllaDB nodes and client applications.

* [Encryption Data in Transit Client to Node](https://opensource.docs.scylladb.com/stable/operating-scylla/security/client-node-encryption.md)
* [Encryption Data in Transit Node to Node](https://opensource.docs.scylladb.com/stable/operating-scylla/security/node-node-encryption.md)

## Encryption at Rest ScyllaDB Enterprise

Encryption at Rest is available in [ScyllaDB Enterprise](https://enterprise.docs.scylladb.com/).

Encryption at Rest protects the privacy of your user’s data, reduces the risk of data breaches, and helps meet regulatory requirements.
In particular, it provides an additional level of protection for your data persisted in storage or backup.

See [Encryption at Rest](https://enterprise.docs.scylladb.com/stable/operating-scylla/security/encryption-at-rest.html) for details.

## Reduce the Network Exposure

Ensure that ScyllaDB runs in a trusted network environment.
A best practice is to maintain a list of ports used by ScyllaDB and to monitor them to ensure that only trusted clients access those network interfaces and ports.
The diagram below shows a single datacenter cluster deployment, with the list of ports used for each connection type. You should periodically check to make sure that only these ports are open and that they are open to relevant IPs only.
Most of the ports’ settings are configurable in the scylla.yaml file.
Also, see the list of ports used by ScyllaDB.

![image](operating-scylla/security/Scylla-Ports2.png)

The ScyllaDB ports are detailed in the table below. For ScyllaDB Manager ports, see the [ScyllaDB Manager Documentation](https://manager.docs.scylladb.com).

<a id="networking-ports"></a>

ScyllaDB uses the following ports:

|   Port | Description                              | Protocol   |
|--------|------------------------------------------|------------|
|   9042 | CQL (native_transport_port)              | TCP        |
|   9142 | SSL CQL (secure client to node)          | TCP        |
|   7000 | Inter-node communication (RPC)           | TCP        |
|   7001 | SSL inter-node communication (RPC)       | TCP        |
|  10000 | ScyllaDB REST API                        | TCP        |
|   9180 | Prometheus API                           | TCP        |
|   9100 | node_exporter (Optionally)               | TCP        |
|  19042 | Native shard-aware transport port        | TCP        |
|  19142 | Native shard-aware transport port  (ssl) | TCP        |

#### NOTE
For ScyllaDB Manager ports, see the [ScyllaDB Manager](https://manager.docs.scylladb.com/) documentation.

## Audit System Activity

Auditing is available in [ScyllaDB Enterprise](https://enterprise.docs.scylladb.com/).

Using the [auditing feature](https://enterprise.docs.scylladb.com/stable/operating-scylla/security/auditing.html) allows the administrator to know “who did / looked at / changed what and when.” It also allows logging some or all the activities a user performs on the ScyllaDB cluster.

## General Recommendations

* Update your cluster with the latest ScyllaDB version.
* Make sure to update your Operating System, and libraries are up to date.
