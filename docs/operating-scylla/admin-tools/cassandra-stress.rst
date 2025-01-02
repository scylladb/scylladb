Cassandra Stress
================

The cassandra-stress tool is used for benchmarking and load testing both ScyllaDB and Cassandra clusters. The cassandra-stress tool also supports testing arbitrary CQL tables and queries to allow users to benchmark their data model.

This documentation focuses on user mode as this allows the testing of your actual schema.


Installation
============

The latest version of `cassandra-stress` at the time of writing this document is **3.17.0**. Please check the latest version on the `GitHub releases page <https://github.com/scylladb/cassandra-stress/releases>`_

There are multiple ways to install `cassandra-stress` provided by ScyllaDB. Choose the method that best suits your environment.

1. **Use Docker Image (Official Way)**
2. **Download Prebuilt Binaries**
3. **Build from Source**
4. **Build Docker Image from Source**

Use Docker Image (Official Way)
-------------------------------

The recommended and official way to use `cassandra-stress` is through the Docker image provided by ScyllaDB. This ensures a consistent and ready-to-use environment.

You can pull the latest Docker image from Docker Hub:

.. code-block:: shell

   docker pull scylladb/cassandra-stress:latest

The `latest` tag is always paired with the version tag on GitHub, such as **3.17.0**. To check all available tags, visit:

`Docker Hub Cassandra-Stress Tags <https://hub.docker.com/r/scylladb/cassandra-stress/tags>`_

Once downloaded, you can run `cassandra-stress` directly:

.. code-block:: shell

   docker run --rm scylladb/cassandra-stress <commands>


If you need to use a specific version, replace `latest` with the desired version tag:


.. code-block:: shell

   docker pull scylladb/cassandra-stress:3.17.0
   docker run --rm scylladb/cassandra-stress:3.17.0 <commands>


Performance Penatly
-------------------

When running in a Docker container, you might experience a performance penalty due to the containerization overhead. To mitigate this, you can use the **--network=host** option to run the container in the host network namespace. This option allows the container to share the host's network stack, which can improve performance.
**--security-opt seccomp=unconfined** allow unrestricted system calls inside the container.


.. code-block:: shell

   docker run --rm --network=host --security-opt seccomp=unconfined scylladb/cassandra-stress <commands>


Download Prebuilt Binaries
---------------------------

You can download the prebuilt binaries directly from our GitHub releases.

**Prerequisite**: Ensure that Java Runtime Environment (JRE) version 11 or newer is installed on your system.

**Using `curl`:**

.. code-block:: shell

   curl -LO https://github.com/scylladb/cassandra-stress/releases/download/v3.17.0/cassandra-stress-3.17.0-bin.tar.gz
   curl -LO https://github.com/scylladb/cassandra-stress/releases/download/v3.17.0/cassandra-stress-3.17.0-bin.tar.gz.sha256

**Using `wget`:**

.. code-block:: shell

   wget https://github.com/scylladb/cassandra-stress/releases/download/v3.17.0/cassandra-stress-3.17.0-bin.tar.gz
   wget https://github.com/scylladb/cassandra-stress/releases/download/v3.17.0/cassandra-stress-3.17.0-bin.tar.gz.sha256

**Verify the download using `sha256sum`:**

   .. code-block:: shell

      sha256sum -c cassandra-stress-3.17.0-bin.tar.gz.sha256

If the verification is successful, you will see:

   .. code-block:: text

      cassandra-stress-3.17.0-bin.tar.gz: OK

**Running Cassandra-Stress**:

After extracting the binaries, you can run `cassandra-stress` directly:

.. code-block:: shell

   ./tools/bin/cassandra-stress <commands>


Build from Source
-----------------

To build `cassandra-stress` from source, ensure you have the following prerequisites installed:
- **Java JDK**: Version 11 or newer
- **Apache Ant**: Build tool

Follow these steps:

1. Clone the repository:

   .. code-block:: shell

      git clone --depth=1 https://github.com/scylladb/cassandra-stress.git

2. Checkout the specific version (3.17.0):

   .. code-block:: shell

      cd cassandra-stress
      git checkout tags/3.17.0

3. Build the binaries using Apache Ant:

   .. code-block:: shell

      ant -Drelease=true artifacts

4. Running Cassandra-Stress:

   .. code-block:: shell

      ./tools/bin/cassandra-stress <commands>

The compiled binaries will be located in the `build` directory.

Build Docker Image from Source
------------------------------

If you prefer a Dockerized build, follow these steps:

1. Clone the repository:

   .. code-block:: shell

      git clone --depth=1 https://github.com/scylladb/cassandra-stress.git

2. Build the Docker image:

   .. code-block:: shell

      cd cassandra-stress
      docker build -t cassandra-stress --compress .

Once built, you can run the image locally using:

.. code-block:: shell

   docker run --rm cassandra-stress <commands>


Usage
-----

There are several operation types:

* write-only, read-only, and mixed workloads of standard data
* write-only and read-only workloads for counter columns
* user configured workloads, running custom queries on custom schemas
* The syntax is cassandra-stress <command> [options]. If you want more information on a given command or options, just run cassandra-stress help

Commands:

1. **read**: Multiple concurrent reads - the cluster must first be populated by a write test.

2. **write**: Multiple concurrent writes against the cluster.

3. **mixed**: Interleaving of any basic commands, with configurable ratio and distribution - the cluster must first be populated by a write test.

4. **counter_write**: Multiple concurrent updates of counters.

5. **counter_read**: Multiple concurrent reads of counters. The cluster must first be populated by a counterwrite test.

6. **user**: Interleaving of user provided queries, with configurable ratio and distribution.

7. **help**: Print help for a command or option.

8. **print**: Inspect the output of a distribution definition.

9. **legacy**: Legacy support mode.

Primary Options:
----------------

- **-pop**: Population distribution and intra-partition visit order.

- **-insert**: Insert specific options relating to various methods for batching and splitting partition updates.

- **-col**: Column details such as size and count distribution, data generator, names, comparator and if super columns should be used.

- **-rate**: Thread count, rate limit or automatic mode (default is auto).

- **-mode**: CQL with options.

- **-errors**: How to handle errors when encountered during stress.

- **-sample**: Specify the number of samples to collect for measuring latency.

- **-schema**: Replication settings, compression, compaction, etc.

- **-node**: Nodes to connect to.

- **-log**: Where to log progress to, and the interval at which to do it.

- **-transport**: Custom transport factories.

- **-port**: The port to connect to cassandra nodes on.

- **-sendto**: Specify a stress server to send this command to.

- **-graph**: Graph recorded metrics.

- **-tokenrange**: Token range settings.


User mode
---------

User mode allows you to use your stress your own schemas. This can save time in the long run rather than building an application and then realising your schema doesn’t scale.

Profile
.......

User mode requires a profile defined in YAML. Multiple YAML files may be specified in which case operations in the ops argument are referenced as specname.opname.

An identifier for the profile:

.. code-block:: cql

   specname: staff_activities

The keyspace for the test:

.. code-block:: cql

   keyspace: staff

CQL for the keyspace. Optional if the keyspace already exists:

.. code-block:: cql

   keyspace_definition: |
   CREATE KEYSPACE stresscql WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};

The table to be stressed:

.. code-block:: cql

   table: staff_activities

CQL for the table. Optional if the table already exists:

.. code-block:: cql

   table_definition: |
   CREATE TABLE staff_activities (
   name text,
   when timeuuid,
   what text,
   PRIMARY KEY(name, when, what)
   )

Optional meta information on the generated columns in the above table. The min and max only apply to text and blob types. The distribution field represents the total unique population distribution of that column across rows:

.. code-block:: cql

   columnspec:
   - name: name
     size: uniform(5..10) # The names of the staff members are between 5-10 characters
     population: uniform(1..10) # 10 possible staff members to pick from
   - name: when
     cluster: uniform(20..500) # Staff members do between 20 and 500 events
   - name: what
     size: normal(10..100,50)

Supported types are:

An exponential distribution over the range [min..max]:

.. code-block:: cql

   EXP(min..max)

An extreme value (Weibull) distribution over the range [min..max]:

.. code-block:: cql

   EXTREME(min..max,shape)

A gaussian/normal distribution, where mean=(min+max)/2, and stdev is (mean-min)/stdvrng:

.. code-block:: cql

   GAUSSIAN(min..max,stdvrng)

A gaussian/normal distribution, with explicitly defined mean and stdev:

.. code-block:: cql

   GAUSSIAN(min..max,mean,stdev)

A uniform distribution over the range [min, max]:

.. code-block:: cql

   UNIFORM(min..max)

A fixed distribution, always returning the same value:

.. code-block:: cql

   FIXED(val)

If preceded by ~, the distribution is inverted

Defaults for all columns are size: uniform(4..8), population: uniform(1..100B), cluster: fixed(1)

Insert distributions:

.. code-block:: cql

   insert:
   # How many partition to insert per batch
   partitions: fixed(1)
   # How many rows to update per partition
   select: fixed(1)/500
   # UNLOGGED or LOGGED batch for insert
   batchtype: UNLOGGED

Currently all inserts are done inside batches.

Read statements to use during the test:

.. code-block:: cql

   queries:
      events:
         cql: select *  from staff_activities where name = ?
         fields: samerow
      latest_event:
         cql: select * from staff_activities where name = ?  LIMIT 1
         fields: samerow

Running a user mode test:

.. code-block:: cql

   cassandra-stress user profile=./example.yaml duration=1m "ops(insert=1,latest_event=1,events=1)" truncate=once

This will create the schema then run tests for 1 minute with an equal number of inserts, latest_event queries and events queries. Additionally the table will be truncated once before the test.

The full example can be found here yaml

Running a user mode test with multiple yaml files:

.. code-block::

   cassandra-stress user profile=./example.yaml,./example2.yaml duration=1m “ops(ex1.insert=1,ex1.latest_event=1,ex2.insert=2)” truncate=once

This will run operations as specified in both the example.yaml and example2.yaml files. example.yaml and example2.yaml can reference the same table
although care must be taken that the table definition is identical (data generation specs can be different).

.. Lightweight transaction support
.. ...............................

.. cassandra-stress supports lightweight transactions. In this it will first read current data from Cassandra and then uses read value(s) to fulfill lightweight transaction condition(s).

.. Lightweight transaction update query:

.. .. code-block:: cql

..    queries:
..      regularupdate:
..          cql: update blogposts set author = ? where domain = ? and published_date = ?
..          fields: samerow
..      updatewithlwt:
..          cql: update blogposts set author = ? where domain = ? and published_date = ? IF body = ? AND url = ?
..          fields: samerow

Graphing
........

Graphs can be generated for each run of stress.

.. image:: example-stress-graph.png

To create a new graph:

.. code-block:: cql

   cassandra-stress user profile=./stress-example.yaml "ops(insert=1,latest_event=1,events=1)" -graph file=graph.html title="Awesome graph"

To add a new run to an existing graph point to an existing file and add a revision name:

.. code-block:: cql

   cassandra-stress user profile=./stress-example.yaml duration=1m "ops(insert=1,latest_event=1,events=1)" -graph file=graph.html title="Awesome graph" revision="Second run"

FAQ
...

How do you use NetworkTopologyStrategy for the keyspace?

Use the schema option making sure to either escape the parenthesis or enclose in quotes:

.. code-block:: cql

   cassandra-stress write -schema "replication(strategy=NetworkTopologyStrategy,datacenter1=3)"

How do you use SSL?

Use the transport option:

.. code-block:: cql

   cassandra-stress "write n=100k cl=ONE no-warmup" -transport "truststore=$HOME/jks/truststore.jks truststore-password=cassandra"



.. include:: /rst_include/apache-copyrights.rst
