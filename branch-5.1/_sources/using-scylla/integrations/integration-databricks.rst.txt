================================
Integrate Scylla with Databricks
================================

Scylla is Apache Cassandra compatible at the CQL binary protocol level, and any driver which uses CQL will work with Scylla (more :doc:`here </using-scylla/drivers/index>`). Any application which uses a CQL driver will work with Scylla, for example, Databricks Spark cluster. 

Resource list
-------------
Although your requirements may be different, this example uses the following resources:

* Scylla cluster
* Databricks account

Integration instructions
------------------------

**Before you begin** 

Verify that you have installed Scylla and know the Scylla server IP address. 
Make sure you have a connection on port 9042:

.. code-block:: none

   curl <scylla_IP>:9042


**Procedure**

1. Create a new Databricks cluster with the following configuration: 

Databricks runtime version:

.. code-block:: none

   Runtime: 9.1 LTS (Scala 2.12, Spark 3.1.2) 

Spark config:

.. code-block:: none

   spark.sql.catalog.<your_catalog> com.datastax.spark.connector.datasource.CassandraCatalog
   spark.sql.catalog.<your_catalog>.spark.cassandra.connection.host <your_host>
   spark.cassandra.auth.username <your_username>
   spark.cassandra.auth.password <your_password>   

2. Once this set up, install connector library by Maven:
(Path: Libraries --> Install new --> Maven --> Search Packages --> Maven Centrall)

.. code-block:: none

   com.datastax.spark:spark-cassandra-connector-assembly_2.12:3.1.0 

**Test case**

1. Prepare test data [Scylla]: 

.. code-block:: none

   CREATE KEYSPACE databriks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor' : 3};
   CREATE TABLE databriks.demo1 (pk text PRIMARY KEY, ck1 text, ck2 text);
   INSERT INTO databriks.demo1  (pk, ck1, ck2) VALUES ('pk', 'ck1', 'ck2'); 

2. Create and play new notebook [Databricks]: 

.. code-block:: none

   df = spark.read.cassandraFormat.table("<your_catalog>.databriks.demo1")
   display(df)

