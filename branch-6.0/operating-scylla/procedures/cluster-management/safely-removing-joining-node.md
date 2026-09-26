# Safely Remove a Joining Node

Sometimes when adding a node to the cluster, it gets stuck in a JOINING state (UJ) and never completes the process to an Up-Normal (UN) state. The only solution is to remove the node. As long as the node did not join the cluster, meaning it never went into UN state, you can stop this node, clean its data, and try again.

1. Run the [nodetool drain](https://opensource.docs.scylladb.com/branch-6.0/operating-scylla/nodetool-commands/drain.md) command (Scylla stops listening to its connections from the client and other nodes).
2. Stop the node

Supported OS

```shell
sudo systemctl stop scylla-server
```

Docker

```shell
docker exec -it some-scylla supervisorctl stop scylla
```

(without stopping *some-scylla* container)

1. Clean the data

```sh
sudo rm -rf /var/lib/scylla/data
sudo find /var/lib/scylla/commitlog -type f -delete
sudo find /var/lib/scylla/hints -type f -delete
sudo find /var/lib/scylla/view_hints -type f -delete
```

1. Start the node

Supported OS

```shell
sudo systemctl start scylla-server
```

Docker

```shell
docker exec -it some-scylla supervisorctl start scylla
```

(with *some-scylla* container already running)
