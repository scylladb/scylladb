# Docker Hub Image

## What is ScyllaDB ?

ScyllaDB is a high-performance NoSQL database system, fully compatible with Apache Cassandra.
ScyllaDB is released under the GNU Affero General Public License version 3 and the Apache License, ScyllaDB is free and open-source software.

> [ScyllaDB](http://www.scylladb.com/)

![logo](http://www.scylladb.com/wp-content/uploads/mascot_medium.png)

## Quick start

To startup a Scylla single-node cluster in developer mode execute:

```console
$ docker run --name some-scylla --hostname some-scylla -d scylladb/scylla --smp 1
```

This command will start a Scylla single-node cluster in developer mode
(see `--developer-mode 1`) limited by a single CPU core (see `--smp`).
Production grade configuration requires tuning a few kernel parameters
such that limiting number of available cores (with `--smp 1`) is
the simplest way to go.

Multiple cores requires setting a proper value to the `/proc/sys/fs/aio-max-nr`.
On many non production systems it will be equal to 65K. The formula
to calculate proper value is:

    Available AIO on the system - (request AIO per-cpu * ncpus) =
    aio_max_nr - aio_nr < (reactor::max_aio + detect_aio_poll + reactor_backend_aio::max_polls) * cpu_cores =
    aio_max_nr - aio_nr < (1024 + 2 + 10000) * cpu_cores =
    aio_max_nr - aio_nr < 11026 * cpu_cores

    where

    reactor::max_aio = max_aio_per_queue * max_queues,
    max_aio_per_queue = 128,
    max_queues = 8.

## How to use this image

### Start a `scylla` server instance

```console
$ docker run --name some-scylla --hostname some-scylla -d scylladb/scylla
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
INFO  2016-08-04 06:57:40,840 [shard 0] storage_service - Thrift server listening on 172.17.0.2:9160 ...
```

### Configuring data volume for storage

You can use Docker volumes to improve performance of Scylla.

Create a Scylla data directory ``/var/lib/scylla`` on the host, which is used by Scylla container to store all data:

```console
$ sudo mkdir -p /var/lib/scylla/data /var/lib/scylla/commitlog /var/lib/scylla/hints /var/lib/scylla/view_hints
```

Launch Scylla using Docker's ``--volume`` command line option to mount the created host directory as a data volume in the container and disable Scylla's developer mode to run I/O tuning before starting up the Scylla node.

```console
$ docker run --name some-scylla --volume /var/lib/scylla:/var/lib/scylla -d scylladb/scylla --developer-mode=0
```

### Configuring resource limits

The Scylla docker image defaults to running on overprovisioned mode and won't apply any CPU pinning optimizations, which it normally does in non-containerized environments.
For better performance, it is recommended to configure resource limits for your Docker container using the `--smp`, `--memory`, and `--cpuset` command line options, as well as 
disabling the overprovisioned flag as documented in the section "Command-line options".

### Restart Scylla

The Docker image uses supervisord to manage Scylla processes. You can restart Scylla in a Docker container using

```
docker exec -it some-scylla supervisorctl restart scylla
```

### Command-line options

The Scylla image supports many command line options that are passed to the `docker run` command.

#### `--seeds SEEDS`

The `-seeds` command line option configures Scylla's seed nodes.
If no `--seeds` option is specified, Scylla uses its own IP address as the seed.

For example, to configure Scylla to run with two seed nodes `192.168.0.100` and `192.168.0.200`.

```console
$ docker run --name some-scylla -d scylladb/scylla --seeds 192.168.0.100,192.168.0.200
```

#### `--listen-address ADDR`

The `--listen-address` command line option configures the IP address the Scylla instance listens on for connections from other Scylla nodes.

For example, to configure Scylla to use listen address `10.0.0.5`:

```console
$ docker run --name some-scylla -d scylladb/scylla --listen-address 10.0.0.5
```

**Since: 1.4**

#### `--alternator-address ADDR`

The `--alternator-address` command line option configures the Alternator API listen address. The default value is the same as `--listen-address`.

**Since: 3.2**

#### `--alternator-port PORT`

The `--alternator-port` command line option configures the Alternator API listen port. The Alternator API is disabled by default. You need to specify the port to enable it.

For example, to configure Scylla to listen to Alternator API at port `8000`:

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

The `--broadcast-address` command line option configures the IP address the Scylla instance tells other Scylla nodes in the cluster to connect to.

For example, to configure Scylla to use broadcast address `10.0.0.5`:

```console
$ docker run --name some-scylla -d scylladb/scylla --broadcast-address 10.0.0.5
```

#### `--broadcast-rpc-address ADDR`

The `--broadcast-rpc-address` command line option configures the IP address the Scylla instance tells clients to connect to.

For example, to configure Scylla to use broadcast RPC address `10.0.0.5`:

```console
$ docker run --name some-scylla -d scylladb/scylla --broadcast-rpc-address 10.0.0.5
```

#### `--smp COUNT`

The `--smp` command line option restricts Scylla to `COUNT` number of CPUs.
The option does not, however, mandate a specific placement of CPUs.
See the `--cpuset` command line option if you need Scylla to run on specific CPUs.

For example, to restrict Scylla to 2 CPUs:

```console
$ docker run --name some-scylla -d scylladb/scylla --smp 2
```

#### `--memory AMOUNT`

The `--memory` command line option restricts Scylla to use up to `AMOUNT` of memory.
The `AMOUNT` value supports both `M` unit for megabytes and `G` unit for gigabytes.

For example, to restrict Scylla to 4 GB of memory:

```console
$ docker run --name some-scylla -d scylladb/scylla --memory 4G
```

#### `--reserve-memory AMOUNT`

The `--reserve-memory` command line option configures Scylla to reserve the `AMOUNT` of memory to the OS.
The `AMOUNT` value supports both `M` unit for megabytes and `G` unit for gigabytes.

For example, to reserve 4 GB of memory to the OS:

```console
$ docker run --name some-scylla -d scylladb/scylla --reserve-memory 4G
```

#### `--overprovisioned ENABLE`

The `--overprovisioned` command line option enables or disables optimizations for running Scylla in an overprovisioned environment.
If no `--overprovisioned` option is specified, Scylla defaults to running with optimizations *enabled*. If `--overprovisioned` is
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

The `--cpuset` command line option restricts Scylla to run on only on CPUs specified by `CPUSET`.
The `CPUSET` value is either a single CPU (e.g. `--cpuset 1`), a range (e.g. `--cpuset 2-3`), or a list (e.g. `--cpuset 1,2,5`), or a combination of the last two options (e.g. `--cpuset 1-2,5`).

For example, to restrict Scylla to run on physical CPUs 0 to 2 and 4:

```console
$ docker run --name some-scylla -d scylladb/scylla --cpuset 0-2,4
```

#### `--developer-mode ENABLE`

The `--developer-mode` command line option enables Scylla's developer mode, which relaxes checks for things like XFS and enables Scylla to run on unsupported configurations (which usually results in suboptimal performance).
If no `--developer-mode` command line option is defined, Scylla defaults to running with developer mode *enabled*.
It is highly recommended to disable developer mode for production deployments to ensure Scylla is able to run with maximum performance.

For example, to disable developer mode:

```console
$ docker run --name some-scylla -d scylladb/scylla --developer-mode 0
```

#### `--experimental ENABLE`

The `--experimental` command line option enables Scylla's experimental mode
If no `--experimental` command line option is defined, Scylla defaults to running with experimental mode *disabled*.
It is highly recommended to disable experimental mode for production deployments.

For example, to enable experimental mode:

```console
$ docker run --name some-scylla -d scylladb/scylla --experimental 1
```

**Since: 2.0**

#### `--disable-version-check`

The `--disable-version-check` disable the version validation check.

**Since: 2.2**

#### `--authenticator AUTHENTICATOR`

The `--authenticator` command lines option allows to provide the authenticator class Scylla will use. By default Scylla uses the `AllowAllAuthenticator` which performs no credentials checks. The second option is using the `PasswordAuthenticator` parameter, which relies on username/password pairs to authenticate users.

**Since: 2.3**

#### `--authorizer AUTHORIZER`

The `--authorizer` command lines option allows to provide the authorizer class Scylla will use. By default Scylla uses the `AllowAllAuthorizer` which allows any action to any user. The second option is using the `CassandraAuthorizer` parameter, which stores permissions in `system_auth.permissions` table.

**Since: 2.3**

### JMX parameters

JMX Scylla service is initialized from the `/scylla-jmx-service.sh` on
container startup. By default the script uses `/etc/sysconfig/scylla-jmx`
to read default configuration. It then can be overridden by setting
an environmental parameters.

An example:

    docker run -d -e "SCYLLA_JMX_ADDR=-ja 0.0.0.0" -e SCYLLA_JMX_REMOTE=-r --publish 7199:7199 scylladb/scylla

#### SCYLLA_JMX_PORT

Scylla JMX listening port.

Default value:

    SCYLLA_JMX_PORT="-jp 7199"

#### SCYLLA_API_PORT

Scylla API port for JMX to connect to.

Default value:

    SCYLLA_API_PORT="-p 10000"

#### SCYLLA_API_ADDR

Scylla API address for JMX to connect to.

Default value:

    SCYLLA_API_ADDR="-a localhost"

#### SCYLLA_JMX_ADDR

JMX address to bind on.

Default value:

    SCYLLA_JMX_ADDR="-ja localhost"

For example, it is possible to make JMX available to the outer world
by changing its bind address to `0.0.0.0`:

    docker run -d -e "SCYLLA_JMX_ADDR=-ja 0.0.0.0" -e SCYLLA_JMX_REMOTE=-r --publish 7199:7199 scylladb/scylla

`cassandra-stress` requires direct access to the JMX.

#### SCYLLA_JMX_FILE

A JMX service configuration file path.

Example value:

    SCYLLA_JMX_FILE="-cf /etc/scylla.d/scylla-user.cfg"

#### SCYLLA_JMX_LOCAL

The location of the JMX executable.

Example value:

    SCYLLA_JMX_LOCAL="-l /opt/scylladb/jmx

#### SCYLLA_JMX_REMOTE

Allow JMX to run remotely.

Example value:

    SCYLLA_JMX_REMOTE="-r"

#### SCYLLA_JMX_DEBUG

Enable debugger.

Example value:

    SCYLLA_JMX_DEBUG="-d"

### Related Links

* [Best practices for running Scylla on docker](http://docs.scylladb.com/procedures/best_practices_scylla_on_docker/)

## User Feedback

### Issues

For bug reports, please use Scylla's [issue tracker](https://github.com/scylladb/scylla/issues) on GitHub.
Please read the [How to report a Scylla problem](http://docs.scylladb.com/operating-scylla/troubleshooting/report_scylla_problem/) page before you report bugs.

For general help, see Scylla's [documentation](http://www.scylladb.com/doc/).
For questions and comments, use Scylla's [mailing lists](http://www.scylladb.com/community/).

### Contributing

Want to scratch your own itch and contribute a patch.
We are eager to review and merge your code.
Please consult the [Contributing on Scylla page](http://www.scylladb.com/kb/contributing/)
