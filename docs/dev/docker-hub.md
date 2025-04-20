# Docker Hub Image

## What is ScyllaDB?

ScyllaDB is a high-performance NoSQL database system, fully compatible with Apache Cassandra.
ScyllaDB is released under the GNU Affero General Public License version 3 and the Apache License, ScyllaDB is free and open-source software.

> [ScyllaDB](http://www.scylladb.com/)

![logo](http://www.scylladb.com/wp-content/uploads/mascot_medium.png)

ScyllaDB Docker supports x86_64 for all versions, and aarch64 starting from ScyllaDB 4.6. Get it now with ScyllaDB [nightly build](https://hub.docker.com/r/scylladb/scylla-nightly)

## Quick start

To startup a ScyllaDB single-node cluster in developer mode, execute:

```console
$ docker run --name some-scylla --hostname some-scylla -d scylladb/scylla --smp 1
```

This command will start a ScyllaDB single-node cluster in developer mode
(see `--developer-mode 1`) limited by a single CPU core (see `--smp`).
Production grade configuration requires tuning a few kernel parameters
such that limiting the number of available cores (with `--smp 1`) is
the simplest way to go.

Using multiple cores requires storing a proper value to `/proc/sys/fs/aio-max-nr`.
While the default value for `aio-max-nr` on many non-production systems is 64K,
this may not be optimal for high-performance workloads. The ideal value depends
on the current value of `/proc/sys/fs/aio-nr` and also on the number of cores to
be used by ScyllaDB:

    Available AIO on the system >= (AIO requests per-cpu) * ncpus

Expanding the definitions on both sides, we get:

    aio_max_nr - aio_nr >= (storage_iocbs + preempt_iocbs + network_iocbs) * ncpus
                                     1024               2           50000

Which yields, for `/proc/sys/fs/aio-max-nr`:

    aio_max_nr >= aio_nr + 51026 * ncpus

## How to use this image

### Start a `scylla` server instance

```console
$ docker run --name some-scylla --hostname some-scylla -d scylladb/scylla
```

If you're on macOS and plan to start a multi-node cluster (3 nodes or more), start ScyllaDB with
`–reactor-backend=epoll` to override the default `linux-aio` reactor backend:

```console
$ docker run --name some-scylla --hostname some-scylla -d scylladb/scylla --reactor-backend=epoll
```

### Run `nodetool` utility

```console
$ docker exec -it some-scylla nodetool status
Datacenter: datacenter1
=======================
Status=Up/Down
|/ State=Normal/Leaving/Joining/Moving
--  Address     Load       Tokens  Owns (effective)  Host ID                               Rack
UN  172.17.0.2  125.51 KB  256     100.0%            c9155121-786d-44f8-8667-a8b915b95665  rack1
```

### Run `cqlsh` utility

```console
$ docker exec -it some-scylla cqlsh
Connected to Test Cluster at 172.17.0.2:9042.
[cqlsh 5.0.1 | Cassandra 2.1.8 | CQL spec 3.2.1 | Native protocol v3]
Use HELP for help.
cqlsh>
```

### Make a cluster

```console
$ docker run --name some-scylla2  --hostname some-scylla2 -d scylladb/scylla --seeds="$(docker inspect --format='{{ .NetworkSettings.IPAddress }}' some-scylla)"
```
If you're on macOS, ensure to add the `–reactor-backend=epoll` option when adding new nodes:

```console
$ docker run --name some-scylla2  --hostname some-scylla2 -d scylladb/scylla --reactor-backend=epoll --seeds="$(docker inspect --format='{{ .NetworkSettings.IPAddress }}' some-scylla)"
```

#### Make a cluster with Docker Compose

First, create a `docker-compose.yml` file with the following contents:

```yaml
version: '3'

services:
  some-scylla:
    image: scylladb/scylla
    container_name: some-scylla

  some-scylla2:
    image: scylladb/scylla
    container_name: some-scylla2
    command: --seeds=some-scylla

  some-scylla3:
    image: scylladb/scylla
    container_name: some-scylla3
    command: --seeds=some-scylla
```

Then, launch the 3-node cluster as follows:

```
docker-compose up -d
```

### Check `scylla` logs

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
```

### Configuring data volume for storage

You can use Docker volumes to improve the performance of ScyllaDB.

Create a Scylla data directory ``/var/lib/scylla`` on the host, which is used by the ScyllaDB container to store all the data:

```console
$ sudo mkdir -p /var/lib/scylla/data /var/lib/scylla/commitlog /var/lib/scylla/hints /var/lib/scylla/view_hints
```

Launch ScyllaDB using Docker's ``--volume`` command line option to mount the created host directory as a data volume in the container and disable ScyllaDB's developer mode to run I/O tuning before starting up the ScyllaDB node.

```console
$ docker run --name some-scylla --volume /var/lib/scylla:/var/lib/scylla -d scylladb/scylla --developer-mode=0
```

### Configuring resource limits

The ScyllaDB docker image defaults to running on overprovisioned mode and won't apply any CPU pinning optimizations, which it normally does in non-containerized environments.
For better performance, it is recommended to configure resource limits for your Docker container using the `--smp`, `--memory`, and `--cpuset` command line options, as well as
disabling the overprovisioned flag as documented in the section "Command-line options".

### Restart ScyllaDB

The Docker image uses supervisord to manage ScyllaDB processes. You can restart ScyllaDB in a Docker container using:

```
docker exec -it some-scylla supervisorctl restart scylla
```

### Command-line options

The ScyllaDB image supports many command line options that are passed to the `docker run` command.

#### `--seeds SEEDS`

The `-seeds` command line option configures ScyllaDB's seed nodes.
If no `--seeds` option is specified, ScyllaDB uses its own IP address as the seed.

For example, to configure ScyllaDB to run with two seed nodes `192.168.0.100` and `192.168.0.200`:

```console
$ docker run --name some-scylla -d scylladb/scylla --seeds 192.168.0.100,192.168.0.200
```

#### `--listen-address ADDR`

The `--listen-address` command line option configures the IP address the ScyllaDB instance listens on for connections from other ScyllaDB nodes.

For example, to configure ScyllaDB to use listen address `10.0.0.5`:

```console
$ docker run --name some-scylla -d scylladb/scylla --listen-address 10.0.0.5
```

**Since: 1.4**

#### `--alternator-address ADDR`

The `--alternator-address` command line option configures the Alternator API listen address. The default value is the same as `--listen-address`.

**Since: 3.2**

#### `--alternator-port PORT`

The `--alternator-port` command line option configures the Alternator API listen port. The Alternator API is disabled by default. You need to specify the port to enable it.

For example, to configure ScyllaDB to listen to Alternator API at port `8000`:

```console
$ docker run --name some-scylla -d scylladb/scylla --alternator-port 8000
```

**Since: 3.2**

#### `--alternator-https-port PORT`

The `--alternator-https-port` option is similar to `--alternator-port`, just enables an encrypted (HTTPS) port. Either the `--alternator-https-port` or `--alternator-http-port`, or both, can be used to enable Alternator.

Note that the `--alternator-https-port` option also requires that files `/etc/scylla/scylla.crt` and `/etc/scylla/scylla.key` be inserted into the image. These files contain an SSL certificate and key, respectively.

**Since: 4.2**

#### `--alternator-write-isolation policy`

The `--alternator-write-isolation` command line option chooses between four allowed write isolation policies described in docs/alternator/alternator.md. This option must be specified if Alternator is enabled - it does not have a default.

**Since: 4.1**

#### `--broadcast-address ADDR`

The `--broadcast-address` command line option configures the IP address the ScyllaDB instance tells other ScyllaDB nodes in the cluster to connect to.

For example, to configure ScyllaDB to use broadcast address `10.0.0.5`:

```console
$ docker run --name some-scylla -d scylladb/scylla --broadcast-address 10.0.0.5
```

#### `--broadcast-rpc-address ADDR`

The `--broadcast-rpc-address` command line option configures the IP address the ScyllaDB instance tells clients to connect to.

For example, to configure ScyllaDB to use broadcast RPC address `10.0.0.5`:

```console
$ docker run --name some-scylla -d scylladb/scylla --broadcast-rpc-address 10.0.0.5
```

#### `--smp COUNT`

The `--smp` command line option restricts ScyllaDB to `COUNT` number of CPUs.
The option does not, however, mandate a specific placement of CPUs.
See the `--cpuset` command line option if you need ScyllaDB to run on specific CPUs.

For example, to restrict ScyllaDB to 2 CPUs:

```console
$ docker run --name some-scylla -d scylladb/scylla --smp 2
```

#### `--memory AMOUNT`

The `--memory` command line option restricts ScyllaDB to use up to `AMOUNT` of memory.
The `AMOUNT` value supports both `M` unit for megabytes and `G` unit for gigabytes.

For example, to restrict ScyllaDB to 4 GB of memory:

```console
$ docker run --name some-scylla -d scylladb/scylla --memory 4G
```

#### `--reserve-memory AMOUNT`

The `--reserve-memory` command line option configures ScyllaDB to reserve the `AMOUNT` of memory to the OS.
The `AMOUNT` value supports both `M` unit for megabytes and `G` unit for gigabytes.

For example, to reserve 4 GB of memory to the OS:

```console
$ docker run --name some-scylla -d scylladb/scylla --reserve-memory 4G
```

#### `--overprovisioned ENABLE`

The `--overprovisioned` command line option enables or disables optimizations for running ScyllaDB in an overprovisioned environment.
If no `--overprovisioned` option is specified, ScyllaDB defaults to running with optimizations *enabled*. If `--overprovisioned` is
not specified and is left at its default, specifying `--cpuset` will automatically disable `--overprovisioned`

For example, to enable optimizations for running in an statically partitioned environment:

```console
$ docker run --name some-scylla -d scylladb/scylla --overprovisioned 0
```

#### `--io-setup ENABLE`

The `--io-setup` command line option specifies if the `scylla_io_setup` script is run when the container is started for the first time.
This is useful if users want to specify I/O settings themselves in environments such as Kubernetes, where running `iotune` is problematic.
The default of `--io-setup` is `1`, which means I/O setup is run.

For example, to skip running I/O setup:

```console
$ docker run --name some-scylla -d scylladb/scylla --io-setup 0
```

**Since: 4.3**

#### `--cpuset CPUSET`

The `--cpuset` command line option restricts ScyllaDB to run on only on CPUs specified by `CPUSET`.
The `CPUSET` value is either a single CPU (e.g. `--cpuset 1`), a range (e.g. `--cpuset 2-3`), or a list (e.g. `--cpuset 1,2,5`), or a combination of the last two options (e.g. `--cpuset 1-2,5`).

For example, to restrict ScyllaDB to run on physical CPUs 0 to 2 and 4:

```console
$ docker run --name some-scylla -d scylladb/scylla --cpuset 0-2,4
```

#### `--developer-mode ENABLE`

The `--developer-mode` command line option enables ScyllaDB's developer mode, which relaxes checks for things like XFS and enables ScyllaDB to run on unsupported configurations (which usually results in suboptimal performance).
If no `--developer-mode` command line option is defined, ScyllaDB defaults to running with developer mode *enabled*.
It is highly recommended to disable developer mode for production deployments to ensure ScyllaDB is able to run with maximum performance.

For example, to disable developer mode:

```console
$ docker run --name some-scylla -d scylladb/scylla --developer-mode 0
```

#### `--experimental-features FEATURE`

The `--experimental-features` command line option enables ScyllaDB's experimental feature individually. If no feature flags are specified, ScyllaDB runs with only *stable* features enabled.

Running experimental features in production environments is not recommended.

For example, to enable the User Defined Functions (UDF) feature:

```console
$ docker run --name some-scylla -d scylladb/scylla --experimental-feature=udf
```

**Since: 2.0**

#### `--disable-version-check`

The `--disable-version-check` disable the version validation check.

**Since: 2.2**

#### `--authenticator AUTHENTICATOR`

The `--authenticator` command lines option allows to provide the authenticator class ScyllaDB will use. By default ScyllaDB uses the `AllowAllAuthenticator` which performs no credentials checks. The second option is using the `PasswordAuthenticator` parameter, which relies on username/password pairs to authenticate users.

**Since: 2.3**

#### `--authorizer AUTHORIZER`

The `--authorizer` command lines option allows to provide the authorizer class ScyllaDB will use. By default ScyllaDB uses the `AllowAllAuthorizer` which allows any action to any user. The second option is using the `CassandraAuthorizer` parameter, which stores permissions in `system.permissions` table.

### Related Links

* [Best practices for running ScyllaDB on docker](http://docs.scylladb.com/procedures/best_practices_scylla_on_docker/)

## User Feedback

### Issues

For bug reports, please use ScyllaDB's [issue tracker](https://github.com/scylladb/scylla/issues) on GitHub.
Please read the [How to report a ScyllaDB problem](http://docs.scylladb.com/operating-scylla/troubleshooting/report_scylla_problem/) page before you report bugs.

For general help, see ScyllaDB's [documentation](https://docs.scylladb.com/stable/).

You can find training material and online courses at [ScyllaDB University](https://university.scylladb.com/).

For questions and comments, use ScyllaDB's [Community Forum](https://forum.scylladb.com/).

### Contributing

Want to scratch your own itch and contribute a patch?
We are eager to review and merge your code.
Please consult the [Contributing to ScyllaDB](https://github.com/scylladb/scylladb/blob/master/CONTRIBUTING.md) page.
