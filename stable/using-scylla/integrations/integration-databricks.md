# Integrate ScyllaDB with Databricks

ScyllaDB is Apache Cassandra compatible at the CQL binary protocol level, and any driver which uses CQL will work with ScyllaDB (more [here](https://opensource.docs.scylladb.com/stable/using-scylla/drivers/index.md)). Any application which uses a CQL driver will work with ScyllaDB, for example, Databricks Spark cluster.

## Resource list

Although your requirements may be different, this example uses the following resources:

* ScyllaDB cluster
* Databricks account

## Integration instructions

**Before you begin**

Verify that you have installed ScyllaDB and know the ScyllaDB server IP address.
Make sure you have a connection on port 9042:

```none
curl <scylla_IP>:9042
```

**Procedure**

1. Create a new Databricks cluster with the following configuration:

Databricks runtime version:

```none
Runtime: 9.1 LTS (Scala 2.12, Spark 3.1.2)
```

Spark config:

```none
spark.sql.catalog.<your_catalog> com.datastax.spark.connector.datasource.CassandraCatalog
spark.sql.catalog.<your_catalog>.spark.cassandra.connection.host <your_host>
spark.cassandra.auth.username <your_username>
spark.cassandra.auth.password <your_password>
```

2. Once this set up, install connector library by Maven:
(Path: Libraries –> Install new –> Maven –> Search Packages –> Maven Centrall)

```none
com.datastax.spark:spark-cassandra-connector-assembly_2.12:3.1.0
```

**Test case**

1. Prepare test data [ScyllaDB]:

```none
CREATE KEYSPACE databriks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor' : 3};
CREATE TABLE databriks.demo1 (pk text PRIMARY KEY, ck1 text, ck2 text);
INSERT INTO databriks.demo1  (pk, ck1, ck2) VALUES ('pk', 'ck1', 'ck2');
```

1. Create and play new notebook [Databricks]:

```none
df = spark.read.cassandraFormat.table("<your_catalog>.databriks.demo1")
display(df)
```
