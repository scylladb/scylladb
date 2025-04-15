ScyllaDB and Spark integration
==============================


Simple ScyllaDB-Spark integration example
-----------------------------------------

This is an example of how to create a very simple Spark application that
uses ScyllaDB to store its data. The application is going to read people's
names and ages from one table and write the names of the adults to
another one. It also will show the number of adults and all people in
the database.

Prerequisites
~~~~~~~~~~~~~

-  ScyllaDB
-  sbt

Prepare ScyllaDB
~~~~~~~~~~~~~~~~

Firstly, we need to create keyspace and tables in which data processed
by the example application will be stored.

Launch ScyllaDB and connect to it using cqlsh. The following commands will
create a new keyspace for our tests and make it the current one.

::

    CREATE KEYSPACE spark_example WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3};
    USE spark_example;

Then, tables both for input and output data need to be created:

::

    CREATE TABLE persons (name TEXT PRIMARY KEY, age INT);
    CREATE TABLE adults (name TEXT PRIMARY KEY);

Lastly, the database needs to contain some input data for our
application to process:

::

    INSERT INTO persons (name, age) VALUES ('Anne', 34);
    INSERT INTO persons (name, age) VALUES ('John', 47);
    INSERT INTO persons (name, age) VALUES ('Elisabeth', 89);
    INSERT INTO persons (name, age) VALUES ('George', 52);
    INSERT INTO persons (name, age) VALUES ('Amy', 17);
    INSERT INTO persons (name, age) VALUES ('Jack', 16);
    INSERT INTO persons (name, age) VALUES ('Treebeard', 36421);

Prepare the application
~~~~~~~~~~~~~~~~~~~~~~~

With a database containing all the necessary tables and data, it is now
time to write our example application. Create a directory
``scylla-spark-example``, which will contain all source code and build
configuration.

First, very important file is ``build.sbt``, which should be created in
the project main directory. It contains all the application metadata,
including name, version, and dependencies.

::

    name := "scylla-spark-example-simple"
    version := "1.0"
    scalaVersion := "2.10.5"

    libraryDependencies ++= Seq(
            "com.datastax.spark" %% "spark-cassandra-connector" % "1.5.0-M1",
            "org.apache.spark" %% "spark-catalyst" % "1.5.0" % "provided"
        )

Then, we need to enable ``sbt-assembly`` plugin. Create directory
``project`` and create file ``plugins.sbt`` with the following content:

::

    addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.0")

The steps above should cover all build configuration, what is left is
the actual logic of the application. Create file
``src/main/scala/ScyllaSparkExampleSimple.scala``:

.. code:: scala

    import org.apache.spark.{SparkContext,SparkConf}
    import com.datastax.spark.connector._

    object ScyllaSparkExampleSimple {
        def main(args: Array[String]): Unit = {
            val sc = new SparkContext(new SparkConf())

            val persons = sc.cassandraTable("spark_example", "persons")

            val adults = persons.filter(_.getInt("age") >= 18).map(n => Tuple1(n.getString("name")))
            adults.saveToCassandra("spark_example", "adults")

            val out = s"Adults: %d\nTotal: %d\n".format(adults.count(), persons.count())
            println(out)
        }
    }

Since we don't want to hardcode in our application any information about
ScyllaDB or Spark we will also need an additional configuration file
``spark-scylla.conf``.

::

    spark.master local
    spark.cassandra.connection.host 127.0.0.1

Now it is time to build the application and create a self-containing jar
file that we will be able to send to Spark. To do that, execute the command:

::

    sbt assembly

It will download all necessary dependencies, build our example, and
create an output jar file in
``target/scala-2.10/scylla-spark-example-simple-assembly-1.0.jar``.

Download and run Spark
~~~~~~~~~~~~~~~~~~~~~~

The next step is to get Spark running. Pre-built binaries can be
downloaded from `this <http://spark.apache.org/downloads.html>`__
website. Make sure to choose release 1.5.0. Since we are going to use it
with ScyllaDB Hadoop version doesn't matter.

Once the download has finished, unpack the archive and in its root
directory, execute the following command to start Spark Master:

::

    ./sbin/start-master.sh -h localhost

Spark Web UI should now be available at http://localhost:8080. The Spark
URL used to connect its workers is ``spark://localhost:7077``.

With the master running, the only thing left to have minimal Spark
deployment is to start a worker. This can be done with the following
command:

::

    ./sbin/start-slave.sh spark://localhost:7077

Run application
~~~~~~~~~~~~~~~

The application is built, Spark is up, and ScyllaDB has all the necessary
tables created and contains the input data for our example. This means
that we are ready to run the application. Make sure that ScyllaDB is
running and execute (still in the Spark directory) the following
command):

::

    ./bin/spark-submit --properties-file /path/to/scylla-spark-example/spark-scylla.conf \
        --class ScyllaSparkExampleSimple /path/to/scylla-spark-example/target/scala-2.10/scylla-spark-example-simple-assembly-1.0.jar

``spark-submit`` will output some logs and debug information, but among
them, there should be a message from the application:

::

    Adults: 5
    Total: 7

You can also connect to ScyllaDB with cqlsh, and using the following query,
see the results of our example in the database.

::

    SELECT * FROM spark_example.adults;

Expected output:

::

     name
    -----------
     Treebeard
     Elisabeth
        George
          John
          Anne

Based on
http://www.planetcassandra.org/getting-started-with-apache-spark-and-cassandra/
and http://koeninger.github.io/spark-cassandra-example/#1.

RoadTrip example
----------------

This is a short guide explaining how to run a Spark example application
available `here <https://github.com/jsebrien/spark-tests>`__ with
ScyllaDB.

Prerequisites
~~~~~~~~~~~~~

-  ScyllaDB
-  Maven
-  Git

Get the source code
~~~~~~~~~~~~~~~~~~~

You can get the source code of this example by cloning the following
repository:

::

    https://github.com/jsebrien/spark-tests

Disable Apache Cassandra
~~~~~~~~~~~~~~~~~~~~~~~~

spark-tests are configured to launch Cassandra, which is not what we want
to achieve here. The following patch disables Cassandra. It can be
applied, for example, using ``git apply --ignore-whitespace -``.

.. code:: diff

    diff --git a/src/main/java/blog/hashmade/spark/util/CassandraUtil.java b/src/main/java/blog/hashmade/spark/util/CassandraUtil.java
    index 37bbc2e..bfe5517 100644
    --- a/src/main/java/blog/hashmade/spark/util/CassandraUtil.java
    +++ b/src/main/java/blog/hashmade/spark/util/CassandraUtil.java
    @@ -14,7 +14,7 @@ public final class CassandraUtil {
        }

        static Session startCassandra() throws Exception {
    -       EmbeddedCassandraServerHelper.startEmbeddedCassandra();
    +       //EmbeddedCassandraServerHelper.startEmbeddedCassandra();
            Thread.sleep(EMBEDDED_CASSANDRA_SERVER_WAITING_TIME);
            Cluster cluster = new Cluster.Builder().addContactPoints("localhost")
                    .withPort(9142).build();

Update connector
~~~~~~~~~~~~~~~~

spark-tests use Spark Cassandra Connector in version 1.1.0 which is too
old for our purposes. Before 1.3.0 the connector used to use Thrift as
well CQL and that won't work with ScyllaDB. Updating the example isn't
very complicated and can be accomplished by applying the following
patch:

.. code:: diff

    diff --git a/pom.xml b/pom.xml
    index 673e22b..1245ffc 100644
    --- a/pom.xml
    +++ b/pom.xml
    @@ -142,7 +142,7 @@
            <dependency>
                <groupId>org.apache.spark</groupId>
                <artifactId>spark-core_2.10</artifactId>
    -           <version>1.1.0</version>
    +           <version>1.3.0</version>
                <exclusions>
                    <exclusion>
                        <groupId>com.google.guava</groupId>
    @@ -157,7 +157,7 @@
            <dependency>
                <groupId>org.apache.spark</groupId>
                <artifactId>spark-streaming_2.10</artifactId>
    -           <version>1.1.0</version>
    +           <version>1.3.0</version>
            </dependency>
            <dependency>
                <groupId>org.cassandraunit</groupId>
    @@ -173,18 +173,18 @@
            <dependency>
                <groupId>com.datastax.cassandra</groupId>
                <artifactId>cassandra-driver-core</artifactId>
    -           <version>2.1.2</version>
    +           <version>2.1.7.1</version>
            </dependency>
            <!-- Datastax -->
            <dependency>
                <groupId>com.datastax.spark</groupId>
                <artifactId>spark-cassandra-connector_2.10</artifactId>
    -           <version>1.1.0-beta2</version>
    +           <version>1.3.0</version>
            </dependency>
            <dependency>
                <groupId>com.datastax.spark</groupId>
                <artifactId>spark-cassandra-connector-java_2.10</artifactId>
    -           <version>1.1.0-beta2</version>
    +           <version>1.3.0</version>
            </dependency>
            <dependency>
                <groupId>net.sf.supercsv</groupId>
    diff --git a/src/main/java/blog/hashmade/spark/DatastaxSparkTest.java b/src/main/java/blog/hashmade/spark/DatastaxSparkTest.java
    index 1027e42..190eb3d 100644
    --- a/src/main/java/blog/hashmade/spark/DatastaxSparkTest.java
    +++ b/src/main/java/blog/hashmade/spark/DatastaxSparkTest.java
    @@ -43,8 +43,7 @@ public class DatastaxSparkTest {
                    .setAppName("DatastaxTests")
                    .set("spark.executor.memory", "1g")
                    .set("spark.cassandra.connection.host", "localhost")
    -               .set("spark.cassandra.connection.native.port", "9142")
    -               .set("spark.cassandra.connection.rpc.port", "9171");
    +               .set("spark.cassandra.connection.port", "9142");
            SparkContext ctx = new SparkContext(conf);
            SparkContextJavaFunctions functions = CassandraJavaUtil.javaFunctions(ctx);
            CassandraJavaRDD<CassandraRow> rdd = functions.cassandraTable("roadtrips", "roadtrip");

Build the example
~~~~~~~~~~~~~~~~~

The example can be built with Maven:

::

    mvn compile

Start ScyllaDB
~~~~~~~~~~~~~~

The application we are trying to run will try to connect with ScyllaDB
using custom port 9142. That's why when starting ScyllaDB, an additional
flag is needed to make sure that's the port it listens on
(alternatively, you can change all occurrences of 9142 to 9042 in the
example source code).

::

    scylla --native-transport-port=9142

Run the application
~~~~~~~~~~~~~~~~~~~

With the example compiled and ScyllaDB running all that is left to be done
is to actually run the application:

::

    mvn exec:java

ScyllaDB limitations
--------------------

-  ScyllaDB needs Spark Cassandra Connector 1.3.0 or later.
-  ScyllaDB doesn't populate ``system.size_estimates``, and therefore the
   connector won't be able to perform automatic split sizing optimally.

For more compatibility information check `ScyllaDB status <http://www.scylladb.com/technology/status/>`_

:doc:`Knowledge Base </kb/index>`



.. include:: /rst_include/apache-copyrights.rst
