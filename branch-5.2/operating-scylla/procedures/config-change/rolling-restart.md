# Rolling Restart Procedure

This is a general procedure that describes how to perform a rolling restart. You can use this procedure where a restart of each node is required (changing the `scylla.yaml` file, for example).

#### NOTE
Perform this procedure on one node at the time. Move to the next node **only after** validating the current node is up and running.

## Procedure

1. Run [nodetool drain](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/nodetool-commands/drain.md) command (Scylla stops listening to its connections from the client and other nodes).
2. Stop the Scylla node.

Supported OS

```shell
sudo systemctl stop scylla-server
```

Docker

```shell
docker exec -it some-scylla supervisorctl stop scylla
```

(without stopping *some-scylla* container)

1. Update the relevant configuration file, for example, scylla.yaml the file can be found under `/etc/scylla/`.
2. Start the Scylla node.

Supported OS

```shell
sudo systemctl start scylla-server
```

Docker

```shell
docker exec -it some-scylla supervisorctl start scylla
```

(with *some-scylla* container already running)

1. Verify the node is up and has returned to the Scylla cluster using [nodetool status](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/nodetool-commands/status.md).
2. Repeat this procedure for all the relevant nodes in the cluster.
