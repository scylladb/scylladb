# What is ScyllaDB ?

ScyllaDB is a high-performance NoSQL database system, fully compatible with Apache Cassandra.
ScyllaDB is released under the GNU Affero General Public License version 3 and the Apache License, ScyllaDB is free and open-source software.

> [ScyllaDB](http://www.scylladb.com/)

![logo](http://www.scylladb.com/wp-content/uploads/mascot_medium.png)

# How to use this image

## Start a `scylla` server instance

```console
$ docker run --name some-scylla --hostname some-scylla -d scylladb/scylla
```

## Run `nodetool` utility

```console
$ docker exec -it some-scylla nodetool status
Datacenter: datacenter1
=======================
Status=Up/Down
|/ State=Normal/Leaving/Joining/Moving
--  Address     Load       Tokens  Owns (effective)  Host ID                               Rack
UN  172.17.0.2  125.51 KB  256     100.0%            c9155121-786d-44f8-8667-a8b915b95665  rack1
```

## Run `cqlsh` utility

```console
$ docker exec -it some-scylla cqlsh
Connected to Test Cluster at 172.17.0.2:9042.
[cqlsh 5.0.1 | Cassandra 2.1.8 | CQL spec 3.2.1 | Native protocol v3]
Use HELP for help.
cqlsh>
```

## Make a cluster

```console
$ docker run --name some-scylla2  --hostname some-scylla2 -d scylladb/scylla --seeds="$(docker inspect --format='{{ .NetworkSettings.IPAddress }}' some-scylla)"
```

## Check `scylla` logs

```console
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
$ sudo mkdir -p /var/lib/scylla/data /var/lib/scylla/commitlog /var/lib/scylla/hints /var/lib/scylla/view_hints
```

Launch Scylla using Docker's ``--volume`` command line option to mount the created host directory as a data volume in the container and disable Scylla's developer mode to run I/O tuning before starting up the Scylla node.

```console
$ docker run --name some-scylla --volume /var/lib/scylla:/var/lib/scylla -d scylladb/scylla --developer-mode=0
```

## Configuring resource limits

The Scylla docker image defaults to running on overprovisioned mode and won't apply any CPU pinning optimizations, which it normally does in non-containerized environments.
For better performance, it is recommended to configure resource limits for your Docker container using the `--smp`, `--memory`, and `--cpuset` command line options, as well as 
disabling the overprovisioned flag as documented in the section "Command-line options".

## Restart Scylla

The Docker image uses supervisord to manage Scylla processes. You can restart Scylla in a Docker container using

```
docker exec -it some-scylla supervisorctl restart scylla
```

## Command-line options

The Scylla image supports many command line options that are passed to the `docker run` command.

### `--seeds SEEDS`

The `-seeds` command line option configures Scylla's seed nodes.
If no `--seeds` option is specified, Scylla uses its own IP address as the seed.

For example, to configure Scylla to run with two seed nodes `192.168.0.100` and `192.168.0.200`.

```console
$ docker run --name some-scylla -d scylladb/scylla --seeds 192.168.0.100,192.168.0.200
```

### `--listen-address ADDR`

The `--listen-address` command line option configures the IP address the Scylla instance listens for client connections.

For example, to configure Scylla to use listen address `10.0.0.5`:

```console
$ docker run --name some-scylla -d scylladb/scylla --listen-address 10.0.0.5
```

**Since: 1.4**

### `--broadcast-address ADDR`

The `--broadcast-address` command line option configures the IP address the Scylla instance tells other Scylla nodes in the cluster to connect to.

For example, to configure Scylla to use broadcast address `10.0.0.5`:

```console
$ docker run --name some-scylla -d scylladb/scylla --broadcast-address 10.0.0.5
```

### `--broadcast-rpc-address ADDR`

The `--broadcast-rpc-address` command line option configures the IP address the Scylla instance tells clients to connect to.

For example, to configure Scylla to use broadcast RPC address `10.0.0.5`:

```console
$ docker run --name some-scylla -d scylladb/scylla --broadcast-rpc-address 10.0.0.5
```

### `--smp COUNT`

The `--smp` command line option restricts Scylla to `COUNT` number of CPUs.
The option does not, however, mandate a specific placement of CPUs.
See the `--cpuset` command line option if you need Scylla to run on specific CPUs.

For example, to restrict Scylla to 2 CPUs:

```console
$ docker run --name some-scylla -d scylladb/scylla --smp 2
```

### `--memory AMOUNT`

The `--memory` command line options restricts Scylla to use up to `AMOUNT` of memory.
The `AMOUNT` value supports both `M` unit for megabytes and `G` unit for gigabytes.

For example, to restrict Scylla to 4 GB of memory:

```console
$ docker run --name some-scylla -d scylladb/scylla --memory 4G
```

### `--overprovisioned ENABLE`

The `--overprovisioned` command line option enables or disables optimizations for running Scylla in an overprovisioned environment.
If no `--overprovisioned` option is specified, Scylla defaults to running with optimizations *enabled*. If `--overprovisioned` is
not specified and is left at its default, specifying `--cpuset` will automatically disable `--overprovisioned`

For example, to enable optimizations for running in an statically partitioned environment:

```console
$ docker run --name some-scylla -d scylladb/scylla --overprovisioned 0
```

### `--cpuset CPUSET`

The `--cpuset` command line option restricts Scylla to run on only on CPUs specified by `CPUSET`.
The `CPUSET` value is either a single CPU (e.g. `--cpuset 1`), a range (e.g. `--cpuset 2-3`), or a list (e.g. `--cpuset 1,2,5`), or a combination of the last two options (e.g. `--cpuset 1-2,5`).

For example, to restrict Scylla to run on physical CPUs 0 to 2 and 4:

```console
$ docker run --name some-scylla -d scylladb/scylla --cpuset 0-2,4
```

### `--developer-mode ENABLE`

The `--developer-mode` command line option enables Scylla's developer mode, which relaxes checks for things like XFS and enables Scylla to run on unsupported configurations (which usually results in suboptimal performance).
If no `--developer-mode` command line option is defined, Scylla defaults to running with developer mode *enabled*.
It is highly recommended to disable developer mode for production deployments to ensure Scylla is able to run with maximum performance.

For example, to disable developer mode:

```console
$ docker run --name some-scylla -d scylladb/scylla --developer-mode 0
```

### `--experimental ENABLE`

The `--experimental` command line option enables Scylla's experimental mode
If no `--experimental` command line option is defined, Scylla defaults to running with experimental mode *disabled*.
It is highly recommended to disable experimental mode for production deployments.

For example, to enable experimental mode:

```console
$ docker run --name some-scylla -d scylladb/scylla --experimental 1
```

**Since: 2.0**

### `--disable-version-check`

The `--disable-version-check` disable the version validation check.

**Since: 2.2**

### `--authenticator AUTHENTICATOR`

The `--authenticator` command lines option allows to provide the authenticator class Scylla will use. By default Scylla uses the `AllowAllAuthenticator` which performs no credentials checks. The second option is using the `PasswordAuthenticator` parameter, which relies on username/password pairs to authenticate users.

**Since: 2.3**

### `--authorizer AUTHORIZER`

The `--authorizer` command lines option allows to provide the authorizer class Scylla will use. By default Scylla uses the `AllowAllAuthorizer` which allows any action to any user. The second option is using the `CassandraAuthorizer` parameter, which stores permissions in `system_auth.permissions` table.

**Since: 2.3**

## Related Links

* [Best practices for running Scylla on docker](http://docs.scylladb.com/procedures/best_practices_scylla_on_docker/)

# User Feedback

## Issues

For bug reports, please use Scylla's [issue tracker](https://github.com/scylladb/scylla/issues) on GitHub.
Please read the [How to report a Scylla problem](http://docs.scylladb.com/operating-scylla/troubleshooting/report_scylla_problem/) page before you report bugs.

For general help, see Scylla's [documentation](http://www.scylladb.com/doc/).
For questions and comments, use Scylla's [mailing lists](http://www.scylladb.com/community/).

## Contributing

Want to scratch your own itch and contribute a patch.
We are eager to review and merge your code.
Please consult the [Contributing on Scylla page](http://www.scylladb.com/kb/contributing/)
