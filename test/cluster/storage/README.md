Extension of the cluster tests, where all tests require externally mounted volumes
to be used by scylla cluster nodes. The volumes are passed with --workdir.

The volumes can be of any size. A help generator `space_limited_servers` is provided
to easily create a cluster of any given number of nodes, where each node operates in
a separate volume.
