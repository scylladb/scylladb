
Best Practices for Running ScyllaDB on Docker
===============================================


This is an article on how to use the ScyllaDB Docker image to start up a ScyllaDB node, access ``nodetool`` and ``cqlsh`` utilities, start a cluster of ScyllaDB nodes, configure data volume for storage, configure resource limits of the Docker container, use additional command line flags and overwrite ``scylla.yaml`` settings. Finally, there is an additional section with some basic usage of ScyllaDB within Docker.

See also the image description on `Docker Hub <https://hub.docker.com/r/scylladb/scylla/>`_ or our `original blog <http://www.scylladb.com/2016/11/09/scylla-on-docker/>`_.

Please note that these instructions assume that you have configured Docker so that you can run it as a regular user. Usually, this is done by adding the user to a Docker group. See your platform-specific Docker installation documentation on how to do that (see, for example, instructions for `Fedora <https://docs.docker.com/install/linux/docker-ce/fedora/>`_ and `Ubuntu <https://docs.docker.com/install/linux/docker-ce/ubuntu/>`_). If you have not configured a Docker group, you need to prefix the Docker commands with sudo to have sufficient permissions to run them.

**NOTE: You should allocate a minimum of 1.5 GB of RAM per container.**

Basic Operations
^^^^^^^^^^^^^^^^

Starting a Single ScyllaDB Node
++++++++++++++++++++++++++++++++++
To start a single ScyllaDB node instance in a Docker container, run:

.. code-block:: console

 docker run --name some-scylla -d scylladb/scylla

The ``docker run`` command starts a new Docker instance in the background named some-scylla that runs the ScyllaDB server:

.. code-block:: console

 docker ps

 CONTAINER ID        IMAGE               COMMAND                  CREATED             STATUS              PORTS                                          NAMES
 616ee646cb9d        scylladb/scylla     "/docker-entrypoint.p"   4 seconds ago       Up 4 seconds        7000-7001/tcp, 9042/tcp, 9160/tcp, 10000/tcp   some-scylla

As seen from the ``docker ps`` output, the image exposes ports **7000-7001** (Inter-node RPC), **9042** (CQL), **9160** (Thrift), and **10000** (REST API).


Viewing ScyllaDB Server Logs
++++++++++++++++++++++++++++++

To access ScyllaDB server logs, you can use the ``docker logs`` command:

.. code-block:: console

 docker logs some-scylla  | tail

 INFO  2016-11-09 10:27:48,191 [shard 6] database - Setting compaction strategy of system_traces.node_slow_log to SizeTieredCompactionStrategy
 INFO  2016-11-09 10:27:48,191 [shard 4] database - Setting compaction strategy of system_traces.node_slow_log to SizeTieredCompactionStrategy
 INFO  2016-11-09 10:27:48,191 [shard 3] database - Setting compaction strategy of system_traces.node_slow_log to SizeTieredCompactionStrategy
 INFO  2016-11-09 10:27:48,191 [shard 1] database - Setting compaction strategy of system_traces.node_slow_log to SizeTieredCompactionStrategy

Checking Server Status with Nodetool
++++++++++++++++++++++++++++++++++++
The Docker image also has ScyllaDB’s utilities installed. :doc:`Nodetool </../../operating-scylla/nodetool/>` is a command line tool for querying and managing a ScyllaDB cluster. The simplest ``nodetool`` command is ``nodetool status``, which displays information about the cluster state:

.. code-block:: console

 docker exec -it some-scylla nodetool status
 Datacenter: datacenter1
 =======================
 Status=Up/Down
 |/ State=Normal/Leaving/Joining/Moving
 --  Address     Load       Tokens  Owns (effective)  Host ID                               Rack
 UN  172.17.0.2  125 KB     256     100.0%            c1906b2b-ce0c-4890-a9d4-8c360f111ad0  rack1

Using cqlsh
+++++++++++
The ``cqlsh`` tool (CQL Shell) is an interactive Cassandra Query Language (CQL) shell for querying and manipulating data in the ScyllaDB cluster.

To start an interactive session, run the following command:

.. code-block:: console

 docker exec -it some-scylla cqlsh
 Connected to Test Cluster at 172.17.0.2:9042.
 [cqlsh 5.0.1 | Cassandra 2.1.8 | CQL spec 3.2.1 | Native protocol v3]
 Use HELP for help.

and then run CQL queries against the cluster:

.. code-block:: cql

 cqlsh> SELECT cluster_name FROM system.local;

 cluster_name
 --------------
 Test Cluster

 (1 rows)

Starting a Cluster
++++++++++++++++++
With a single ``some-scylla`` instance running,  joining new nodes to form a cluster is easy:

.. code-block:: console

 docker run --name some-scylla2 -d scylladb/scylla --seeds="$(docker inspect --format='{{ .NetworkSettings.IPAddress }}' some-scylla)"

To query when the node is up and running (and view the status of the entire cluster) use the ``nodetool status`` command:

.. code-block:: console

 docker exec -it some-scylla nodetool status
 Datacenter: datacenter1
 =======================
 Status=Up/Down
 |/ State=Normal/Leaving/Joining/Moving
 --  Address     Load       Tokens  Owns (effective)  Host ID                               Rack
 UN  172.17.0.3  177.48 KB  256     100.0%            097caff5-892d-412f-af78-11d572795d6f  rack1
 UN  172.17.0.2  125 KB     256     100.0%            c1906b2b-ce0c-4890-a9d4-8c360f111ad0  rack1

Restarting ScyllaDB from within the Running Node 
+++++++++++++++++++++++++++++++++++++++++++++++++++
The Docker image uses ``supervisord`` to manage ScyllaDB processes. You can restart ScyllaDB in a Docker container using:

.. code-block:: console

 docker exec -it some-scylla supervisorctl restart scylla

Configuring a Data Volume for Storage
+++++++++++++++++++++++++++++++++++++
The default filesystem in Docker is inadequate for anything else than just testing out ScyllaDB, but you can use Docker volumes for improving storage performance.

To use data volumes, ensure first that it’s on a ScyllaDB-supported filesystem like XFS, then create a ScyllaDB data directory ``/var/lib/scylla`` on the host. This will be used by ScyllaDB container to store all data:

.. code-block:: console

 sudo mkdir -p /var/lib/scylla/data /var/lib/scylla/commitlog

Then launch ScyllaDB instances using Docker’s ``--volume`` command line option to mount the created host directory as a data volume in the container and disable ScyllaDB’s developer mode to run I/O tuning before starting up the ScyllaDB node.

.. code-block:: console

 docker run --name some-scylla --volume /var/lib/scylla:/var/lib/scylla -d scylladb/scylla --developer-mode=0

Overriding scylla.yaml with a Master File
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Sometimes, it’s not possible to adjust ScyllaDB-specific settings (including non-network properties, like ``cluster_name`` ) directly from the command line when ScyllaDB is running within Docker.

Instead, it may be necessary to incrementally override ``scylla.yaml`` settings by passing an external, master Scylla.yaml file when starting the Docker container for the node.

To do this, you can use the ``--volume (-v)`` command as before to specify the overriding ``.yaml`` file:

**NOTE**:  you can create a ``master_scylla.yaml`` in current host dir: just copy the file from ``https://github.com/scylladb/scylla/blob/master/conf/scylla.yaml``.

1. On the host, create and edit ``master_scylla.yaml``, for example. Uncomment and change the "cluster_name" parameter.
2. Start the ScyllaDB node, with the command to override ``scylla.yaml`` with ``master_scylla.yaml`` :

.. code-block:: console

  docker run --name some-scylla --volume ~/master_scylla.yaml:/etc/scylla/scylla.yaml -d scylladb/scylla

**NOTE**:  You can start a Docker node with any other alternate parameter configured in ``scylla.yaml`` using this technique.

3. Finally, you can check that the setting was changed:

.. code-block:: console

 docker exec -it some-scylla nodetool describecluster

 Cluster Information:
	Name: Doobie Snarf
	Snitch: org.apache.cassandra.locator.SimpleSnitch
	Partitioner: org.apache.cassandra.dht.Murmur3Partitioner
	Schema versions:
		34259144-0f3f-305f-a777-2811e30e17b3: [172.17.0.2]

Getting Performance out of your Docker Container
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
By default, our Docker image defaults to a mode where ScyllaDB’s architectural optimizations are not enabled. With these command-line settings, you can introduce incremental changes that boost your ScyllaDB performance on Docker even more.

Configuring Resource Limits
+++++++++++++++++++++++++++
ScyllaDB uses all CPUs and memory by default.   To configure resource limits for your Docker container, you can use the ``--smp``, ``--memory``, and ``--cpuset`` command line options documented in the section "Network and command-line settings" below.

The recommended way to run multiple ScyllaDB instances on the same physical hardware is by statically partitioning all resources. For example, using the ``--cpuset`` option to assign cores ``0`` and ``1`` to one instance, and ``2`` and ``3`` to another.

In scenarios in which static partitioning is not desired - like mostly-idle cluster without hard latency requirements, the ``--overprovisioned``  command-line option is recommended. This enables certain optimizations for ScyllaDB to run efficiently in an overprovisioned environment.

**NOTE: You should allocate a minimum of 1.5 GB of RAM per container.**

Network and Command-Line Settings
+++++++++++++++++++++++++++++++++
The ScyllaDB image supports many command line options that are passed to the Docker run command.  Keep in mind that these command-line settings override the corresponding settings in your ``scylla.yaml``.

--seeds SEEDS
-------------
The ``--seeds`` command line option configures ScyllaDB's seed nodes. If no ``--seeds`` option is specified, ScyllaDB uses its own IP address as the seed.

For example, to configure ScyllaDB to run with two seed nodes ``192.168.0.100`` and ``192.168.0.200``.

.. code-block:: console

 docker run --name some-scylla -d scylladb/scylla --seeds 192.168.0.100,192.168.0.200

--listen-address ADDR
---------------------
The ``--listen-address`` command line option configures the IP address the ScyllaDB instance listens for client connections.

For example, to configure ScyllaDB to use listen address ``10.0.0.5``:

.. code-block:: console

  docker run --name some-scylla -d scylladb/scylla --listen-address 10.0.0.5

--broadcast-address ADDR
------------------------
The ``--broadcast-address`` command line option configures the IP address the ScyllaDB instance tells other ScyllaDB nodes in the cluster to connect to.

For example, to configure ScyllaDB to use broadcast address ``10.0.0.5``:

.. code-block:: console

 docker run --name some-scylla -d scylladb/scylla --broadcast-address 10.0.0.5

--broadcast-rpc-address ADDR
----------------------------
The ``--broadcast-rpc-address`` command line option configures the IP address the ScyllaDB instance tells clients to connect to.

For example, to configure ScyllaDB to use broadcast RPC address ``10.0.0.5``:

.. code-block:: console

 docker run --name some-scylla -d scylladb/scylla --broadcast-rpc-address 10.0.0.5

--smp COUNT
-----------
The ``--smp`` command line option restricts ScyllaDB to ``COUNT`` number of CPUs. The option does not, however, mandate a specific placement of CPUs. See the ``--cpuset`` command line option if you need ScyllaDB to run on specific CPUs.

For example, to restrict ScyllaDB to 2 CPUs:

.. code-block:: console

 docker run --name some-scylla -d scylladb/scylla --smp 2

--memory AMOUNT
---------------
The ``--memory`` command line option restricts ScyllaDB to use up to ``AMOUNT`` of memory. The ``AMOUNT`` value supports both ``M`` unit for megabytes and ``G`` unit for gigabytes.

For example, to restrict ScyllaDB to 4 GB of memory:

.. code-block:: console

 docker run --name some-scylla -d scylladb/scylla --memory 4G

 **NOTE: You should allocate a minimum of 1.5 GB of RAM per container.**

--overprovisioned ENABLE
------------------------
The ``--overprovisioned`` command line option enables or disables optimizations for running ScyllaDB in an overprovisioned environment. If no ``--overprovisioned`` option is specified, ScyllaDB defaults to running with optimizations enabled.

For example, to enable optimizations for running in an overprovisioned environment:

.. code-block:: console

  docker run --name some-scylla -d scylladb/scylla --overprovisioned 1

--cpuset CPUSET
---------------
The ``--cpuset`` command line option restricts ScyllaDB to run on only on CPUs specified by ``CPUSET``. The ``CPUSET`` value is either a single CPU (e.g. ``--cpuset 1``), a range (e.g. ``--cpuset 2-3``), or a list (e.g. ``--cpuset 1,2,5``), or a combination of the last two options (e.g. ``--cpuset 1-2,5``).

For example, to restrict ScyllaDB to run on physical CPUs 0 to 2 and 4:

.. code-block:: console

  docker run --name some-scylla -d scylladb/scylla --cpuset 0-2,4

--developer-mode ENABLE
-----------------------
The ``--developer-mode`` command line option enables ScyllaDB's developer mode, which relaxes checks for things like XFS and enables ScyllaDB to run on unsupported configurations (which usually results in suboptimal performance). If no ``--developer-mode`` command line option is defined, ScyllaDB defaults to running with developer mode enabled.

**It is highly recommended to disable developer mode for production deployments** to ensure ScyllaDB is able to run with maximum performance.

To disable developer mode:

.. code-block:: console

  docker run --name some-scylla -d scylladb/scylla --developer-mode 0

--experimental ENABLE
---------------------
The ``--experimental`` command line option enables Scylla's experimental mode. If no ``--experimental`` command line option is defined, ScyllaDB defaults to running with experimental mode disabled.

**It is highly recommended to disable experimental mode for production deployments.**

For example, to enable experimental mode:

.. code-block:: console

  docker run --name some-scylla -d scylladb/scylla --experimental 1

Other Useful Tips and Tricks
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Checking the Current Version of ScyllaDB on the Node
++++++++++++++++++++++++++++++++++++++++++++++++++++++

.. code-block:: console

  docker exec -it some-scylla scylla --version

Using a Local CSV File to Import Data into ScyllaDB
+++++++++++++++++++++++++++++++++++++++++++++++++++++
First, download the file locally to the node:

.. code-block:: console

  sudo docker exec -it some-scylla.2.0.1 curl -o file.csv https://<url>.com/<path>/<path>/<file>.csv

Once you have the ``.csv`` downloaded, you can use the CQL ``COPY FROM`` command as explained :doc:`here </cql/cqlsh>` to load the data into ScyllaDB.


Such a copy command might look like this:

.. code-block:: cql

 cqlsh:my_keyspace> COPY <table_name> FROM 'file.csv' WITH HEADER=true;

Searching for a setting in scylla.yaml
++++++++++++++++++++++++++++++++++++++++
``scylla.yaml`` can be found at ``/etc/scylla/scylla.yaml``.  In this case, you can search for a specific entry in the file.   For example, if you wanted to determine if a setup was experimental and were to search for ``experimental`` in the file, you could try:

.. code-block:: console

 docker exec -it some-scylla grep -H 'experimental' /etc/scylla/scylla.yaml

.. include:: /rst_include/apache-copyrights.rst
