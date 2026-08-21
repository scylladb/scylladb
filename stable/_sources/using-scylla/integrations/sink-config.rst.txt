==================================
Kafka Sink Connector Configuration
==================================

**Topic: Kafka Sink Connector configuration properties**

**Learn: How to configure the ScyllaDB Kafka Sink Connector**

**Audience: ScyllaDB application developers**


Synopsis
--------

This document contains Kafka Sink Connector configuration properties and descriptions. 


Usage
-----

To use this connector, specify the name of the connector class in the connector.class configuration property.

.. code-block:: none

   connector.class=io.connect.scylladb.ScyllaDbSinkConnector

Connection
----------


scylladb.contact.points
^^^^^^^^^^^^^^^^^^^^^^^

Specifies which ScyllaDB hosts to connect to. 
ScyllaDB nodes use this list of hosts to find each other and learn the topology of the ring. 
You must change this if you are running multiple nodes. 
It's essential to put at least two hosts in case of bigger clusters for high availability purposes. 
If you are using a docker image, connect to the host it uses.

* Type: List
* Importance: High
* Default Value: [localhost]

scylladb.port
^^^^^^^^^^^^^

Specifies the port that the ScyllaDB hosts are listening on. 
For example, when using a docker image, connect to the port it uses (use ``docker ps``).

* Type: Int
* Importance: Medium
* Default Value: 9042
* Valid Values: ValidPort{start=1, end=65535}

scylladb.loadbalancing.localdc
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Specifies the  local Data Center name (case-sensitive) that is local to the machine on which the connector is running.

* Type: string
* Default: ""
* Importance: high

scylladb.security.enabled
^^^^^^^^^^^^^^^^^^^^^^^^^

Enables security while loading the sink connector and connecting to ScyllaDB.

* Type: Boolean
* Importance: High
* Default Value: False

scylladb.username
^^^^^^^^^^^^^^^^^

Specifies the username to use to connect to ScyllaDB. Set ``scylladb.security.enable = true`` when using this parameter.

* Type: String
* Importance: High
* Default Value: cassandra

scylladb.password
^^^^^^^^^^^^^^^^^

Specifies the password to use to connect to ScyllaDB. Set ``scylladb.security.enable = true`` when using this parameter.

* Type: Password
* Importance: High
* Default Value: cassandra

scylladb.compression
^^^^^^^^^^^^^^^^^^^^

Specifies the compression algorithm to use when connecting to ScyllaDB.

* Type: string
* Default: NONE
* Valid Values: [NONE, SNAPPY, LZ4]
* Importance: low

scylladb.ssl.enabled
^^^^^^^^^^^^^^^^^^^^

Specifies if SSL should be enabled when connecting to ScyllaDB.

* Type: boolean
* Default: false
* Importance: high


SSL
---

scylladb.ssl.truststore.path 
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Specifies the path to the Java Truststore.

* Type: string
* Default: ""
* Importance: medium

scylladb.ssl.truststore.password 
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Specifies the password to use to access the Java Truststore.

* Type: password
* Default: [hidden]
* Importance: medium

scylladb.ssl.provider
^^^^^^^^^^^^^^^^^^^^^

Specifies the SSL Provider to use when connecting to ScyllaDB.

* Type: string
* Default: JDK
* Valid Values: [JDK, OPENSSL, OPENSSL_REFCNT]
* Importance: low

Keyspace
--------

scylladb.keyspace
^^^^^^^^^^^^^^^^^

Specifies the keyspace to write to. This keyspace is like a database in the ScyllaDB cluster.

* Type: String
* Importance: High

scylladb.keyspace.create.enabled
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Determines if the keyspace should be created if it does not exist. 

.. note:: An error will result if a new keyspace (which already exists) has to be created, and this parameter is set to false.

* Type: Boolean
* Importance: High
* Default Value: true

scylladb.keyspace.replication.factor
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Specifies the replication factor to use if a keyspace is created by the connector. 
The Replication Factor (RF) is equivalent to the number of nodes where data (rows and partitions) are replicated. 
Data is replicated to multiple (RF=N) nodes

* Type: int
* Default: 3
* Valid Values: [1,...]
* Importance: high

Table
-----

scylladb.table.manage.enabled
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Determines if the connector should manage the table.

* Type: Boolean
* Importance: High
* Default Value: true

scylladb.table.create.compression.algorithm
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Specifies the compression algorithm to use when the table is created.

* Type: string
* Default: NONE
* Valid Values: [NONE, SNAPPY, LZ4, DEFLATE]
* Importance: medium

scylladb.offset.storage.table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The table within the ScyllaDB keyspace to store the offsets that have been read from Apache Kafka. 
This is used once to enable delivery to ScyllaDB.

* Type: String
* Importance: Low
* Default: kafka_connect_offsets

Write
-----

scylladb.consistency.level
^^^^^^^^^^^^^^^^^^^^^^^^^^

The requested consistency level to use when writing to ScyllaDB.
The Consistency Level (CL) determines how many replicas in a cluster that must acknowledge read or write operations before it is considered successful.

* Type: String
* Importance: High
* Default Value: LOCAL_QUORUM
* Valid Values: ANY, ONE, TWO, THREE, QUORUM, ALL, LOCAL_QUORUM, EACH_QUORUM, SERIAL, LOCAL_SERIAL, LOCAL_ONE

scylladb.deletes.enabled 
^^^^^^^^^^^^^^^^^^^^^^^^

Determines if the connector should process deletes. 
The Kafka records with a Kafka record value as null will result in the deletion of the ScyllaDB record with the primary key present in the Kafka record key.

* Type: boolean
* Default: true
* Importance: high

scylladb.execute.timeout.ms
^^^^^^^^^^^^^^^^^^^^^^^^^^^

The timeout for executing a ScyllaDB statement.

* Type: Long
* Importance: Low
* Default Value: 30000

scylladb.ttl
^^^^^^^^^^^^

The retention period for the data in ScyllaDB. 
After this interval elapses, ScyllaDB will remove these records. 
If this configuration is not provided, the Sink Connector will perform insert operations in ScyllaDB without the TTL setting.

* Type: Int
* Importance: Medium
* Default Value: null

scylladb.offset.storage.table.enable
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If true, Kafka consumer offsets will be stored in the ScyllaDB table. 
If false, the connector will skip writing offset information into ScyllaDB (this might imply duplicate writes into ScyllaDB when a task restarts).

* Type: Boolean
* Importance: Medium
* Default Value: True

scylladb.max.batch.size.kb 
^^^^^^^^^^^^^^^^^^^^^^^^^^

Maximum size(in kilobytes) of a single batch consisting of ScyllaDB operations. 
Should be equal to ``batch_size_warn_threshold_in_kb`` and 1/10th of the ``batch_size_fail_threshold_in_kb`` configured in ``scylla.yaml``. 
The default value is set to 5kb, any change in this configuration should be accompanied by a change in ``scylla.yaml``.

* Type: int
* Default: 5
* Valid Values: [1,...]
* Importance: high

Confluent Platform configurations
---------------------------------

tasks.max
^^^^^^^^^

Specifies the maximum number of tasks to use for the connector that helps in parallelism.

* Type:int
* Importance: high

topics
^^^^^^

Specifies the name of the topics to consume data from and write to ScyllaDB.

* Type: list
* Importance: high

confluent.topic.bootstrap.servers
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A list of host/port pairs to use for establishing the initial connection to the Kafka cluster used for licensing. 
All servers in the cluster will be discovered from the initial connection. 
This list should be in the form ``host1:port1,host2:port2,…``. 
Since these servers are just used for the initial connection to discover the full cluster membership (which may change dynamically), this list need not contain the full set of servers (you may want more than one, though, in case a server is down).

* Type: list
* Importance: high

Additional References
---------------------

* :doc:`Kafka Sink Connector Quickstart </using-scylla/integrations/kafka-connector>`
