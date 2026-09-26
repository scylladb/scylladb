# Remove a Seed Node from Seed List

This procedure describes how to remove a seed node from the seed list.

#### NOTE
The seed concept in gossip has been removed. Starting with Scylla Open Source 4.3 and Scylla Enterprise 2021.1, a seed node
is only used by a new node during startup to learn about the cluster topology. As a result, you only need to configure one
seed node in a node’s `scylla.yaml` file.

## Prerequisites

Verify that the seed node you want to remove is listed as a seed node in the `scylla.yaml` file by running `cat /etc/scylla/scylla.yaml | grep seeds:`

## Procedure

1. Update the Scylla configuration file, scylla.yaml, which can be found under `/etc/scylla/`. For example:

Seed list before removing the node:

```shell
- seeds: "10.240.0.83,10.240.0.93,10.240.0.103"
```

Seed list after removing the node:

```shell
- seeds: "10.240.0.83,10.240.0.93"
```

1. Scylla will read the updated seed list the next time it starts. You can force Scylla to read the list immediately by restarting Scylla as follows:

Supported OS

```shell
sudo systemctl restart scylla-server
```

Docker

```shell
docker exec -it some-scylla supervisorctl restart scylla
```

(without restarting *some-scylla* container)
