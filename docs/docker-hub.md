# What is ScyllaDB ?

ScyllaDB is a high-performance Cassandra implementation written in C++14. Classified as a NoSQL database, ScyllaDB deliver a high number of transactions per seconds making it one of the fastest database on the planet. ScyllaDB is released under the GNU Affero General Public License version 3 and the Apache License, ScyllaDB is free and open-source software.

> [ScyllaDB](http://www.scylladb.com/)

![logo](http://www.scylladb.com/img/logo.svg)

# How to use this image

## Start a `scylla` server instance

```console
$ docker run --name some-scylla -d scylladb/scylla
```

## Run `nodetool` utility

```
$ docker exec -it some-scylla nodetool status
Datacenter: datacenter1
=======================
Status=Up/Down
|/ State=Normal/Leaving/Joining/Moving
--  Address     Load       Tokens  Owns (effective)  Host ID                               Rack
UN  172.17.0.2  125.51 KB  256     100.0%            c9155121-786d-44f8-8667-a8b915b95665  rack1
```

## Run `cqlsh` utility

```
$ docker exec -it some-scylla cqlsh
Connected to Test Cluster at 172.17.0.2:9042.
[cqlsh 5.0.1 | Cassandra 2.1.8 | CQL spec 3.2.1 | Native protocol v3]
Use HELP for help.
cqlsh>
```

## Make a cluster

```
$ docker run --name some-scylla2 -d scylladb/scylla --seeds="$(docker inspect --format='{{ .NetworkSettings.IPAddress }}' some-scylla)"
```

## Check `scylla` logs

```
$ docker logs some-scylla | tail
INFO  2016-08-04 06:57:40,836 [shard 5] database - Setting compaction strategy of system_traces.events to SizeTieredCompactionStrategy
INFO  2016-08-04 06:57:40,836 [shard 3] database - Setting compaction strategy of system_traces.events to SizeTieredCompactionStrategy
INFO  2016-08-04 06:57:40,836 [shard 1] database - Setting compaction strategy of system_traces.events to SizeTieredCompactionStrategy
INFO  2016-08-04 06:57:40,836 [shard 2] database - Setting compaction strategy of system_traces.events to SizeTieredCompactionStrategy
INFO  2016-08-04 06:57:40,836 [shard 4] database - Setting compaction strategy of system_traces.events to SizeTieredCompactionStrategy
INFO  2016-08-04 06:57:40,836 [shard 7] database - Setting compaction strategy of system_traces.events to SizeTieredCompactionStrategy
INFO  2016-08-04 06:57:40,837 [shard 6] database - Setting compaction strategy of system_traces.events to SizeTieredCompactionStrategy
INFO  2016-08-04 06:57:40,839 [shard 0] database - Schema version changed to fea14d93-9c5a-34f5-9d0e-2e49dcfa747e
INFO  2016-08-04 06:57:40,839 [shard 0] storage_service - Starting listening for CQL clients on 172.17.0.2:9042...
INFO  2016-08-04 06:57:40,840 [shard 0] storage_service - Thrift server listening on 172.17.0.2:9160 ...
```

## Configuring data volume for storage

You can use Docker volumes to improve performance of Scylla.

Create a Scylla data directory ``/var/lib/scylla`` on the host, which is used by Scylla container to store all data:

```console
$ sudo mkdir -p /var/lib/scylla/data /var/lib/scylla/commitlog
```

Launch Scylla using Docker's ``--volume`` command line option to mount the created host directory as a data volume in the container and disable Scylla's developer mode to run I/O tuning before starting up the Scylla node.

```
docker run --name some-scylla --volume /var/lib/scylla:/var/lib/scylla -d scylladb/scylla --developer-mode=0
```

## Restricting container CPUs

Scylla utilizes all CPUs by default. To restrict your Docker container to a subset of the CPUs, use the ``--cpuset`` command line option:

```
docker run --name some-scylla -d scylladb/scylla --cpuset 0-4
```

# User Feedback

## Issues

Don't hesitage to report bugs or issues on http://www.github.com/scylladb/scylla's bug tracker.

## Contributing

Want to scratch your own itch and contribute a patch.
We are eager to review and merge your code.
Please consult the [Contributing on Scylla page](http://www.scylladb.com/kb/contributing/)
