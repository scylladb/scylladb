# Kafka Sink Connector Quickstart

**Topic: Kafka Connector**

**Learn: how to setup the ScyllaDB Sink Connector against a Dockerized ScyllaDB**

**Audience: Application Developer**

## Synopsis

This quickstart will show how to setup the ScyllaDB Sink Connector against a Dockerized ScyllaDB.

## Preliminary setup

1. Using [Docker](https://hub.docker.com/r/scylladb/scylla/), follow the instructions to launch Scylla.
2. Start the Docker container, replacing the `--name` and `--host name` parameters with your own information. For example:
   ```none
   docker run --name some-scylla --hostname some-scylla -d scylladb/scylla
   ```
3. Run `docker ps` to show the exposed ports. The output should be similar to this example:
   ```none
   docker ps
   CONTAINER ID        IMAGE                     COMMAND                  CREATED             STATUS              PORTS                                              NAMES
   26cc6d47efe3        replace-with-image-name   "/docker-entrypoint.…"   4 hours ago         Up 23 seconds       0.0.0.0:32777->1883/tcp, 0.0.0.0:32776->9001/tcp   anonymous_my_1
   ```
4. Continue with either Confluent or Manual Installation.

### Install using Confluent Platform

If you are new to Confluent, [download Confluent Platform](https://www.confluent.io/download/).

1. In the Self managed software box, click **DOWNLOAD FREE**
2. Fill in your email address.
3. Open the **Select Deployment Type** drop-down and select **ZIP**.
4. Accept the Terms & Conditions and click **DOWNLOAD FREE**.
5. You will receive an email with instructions. Download / move the file to the desired location.
6. Continue with the setup following [this document](https://docs.confluent.io/current/quickstart/ce-quickstart.html#ce-quickstart).

### Install Kafka Connector manually

1. Navigate to the Kafka Connect Scylladb Sink [github page](https://github.com/scylladb/kafka-connect-scylladb) and clone the repository.
2. Using a terminal, open the source code (src) folder.
3. Run the command `mvn clean install`.
4. Run the Integration Tests in an IDE. If tests fail run `mvn clean install -DskipTests`.

   #### NOTE
   To run Integration Tests, there is no need to run Confluent. Use docker-compose.yml file in the github repository and run the following command (it contains images to run Kafka and other services):
   ```none
   docker-compose -f docker-compose.yml up
   ```

   After completion of the above steps, a folder named `components` will be created in the target folder of the source code folder. The Connector jar files are present in `{source-code-folder}/target/components/packages/[jar-files]`
   Create a folder by the name of `ScyllaDB-Sink-Connector` and copy the jar files into it. Navigate to your Confluent Platform installation directory and place this folder in `{confluent-directory}/share/java`.

## Add Sink Connector plugin

The Scylla sink connector is used to publish records from a Kafka topic into Scylla.
Adding a new connector plugin requires restarting Connect.
Use the Confluent CLI to restart Connect.

1. Run the following
   ```none
   confluent local stop && confluent local start
   ```

   The output will be similar to:
   ```none
   confluent local stop && confluent local start
   Starting zookeeper
   zookeeper is [UP]
   Starting Kafka
   Kafka is [UP]
   Starting schema-registry
   schema-registry is [UP]
   Starting kafka-rest
   kafka-rest is [UP]
   Starting connect
   connect is [UP]
   ```
2. Check if the kafka-connect-scylladb connector plugin has been installed correctly and picked up by the plugin loader:
   ```none
   curl -sS localhost:8083/connector-plugins | jq .[].class | grep ScyllaDbSinkConnector
   ```

   Your output should resemble:

   `io.connect.scylladb.ScyllaDbSinkConnector`

## Connector configuration

1. Save the configuration settings in a file named `kafka-connect-scylladb.json` its contents should contain:
   ```json
   {
        "name" : "scylladb-sink-connector",
        "config" : {
          "connector.class" : "io.connect.scylladb.ScyllaDbSinkConnector",
          "tasks.max" : "1",
          "topics" : "topic1,topic2,topic3",
          "scylladb.contact.points" : "scylladb-hosts",
          "scylladb.keyspace" : "test"
   }
   ```
2. Load the connector. Run the following command:
   ```shell
   curl -s -X POST -H 'Content-Type: application/json' --data @kafka-connect-scylladb.json http://localhost:8083/connectors
   ```
3. Update the configuration of the existing connector.
   ```shell
   curl -s -X PUT -H 'Content-Type: application/json' --data @kafka-connect-scylladb.json http://localhost:8083/connectors/scylladb/config
   ```
4. Once the Connector is up and running, use the command `kafka-avro-console-producer` to produce records(in AVRO format) into the Kafka topic.
   ```none
   kafka-avro-console-producer
   --broker-list localhost:9092
   --topic example
   --property parse.key=true
   --property key.schema='{"type":"record",name":"key_schema","fields":[{"name":"id","type":"int"}]}'
   --property "key.separator=$"
   --property value.schema='{"type":"record","name":"value_schema","fields":[{"name":"id","type":"int"},
   {"name":"firstName","type":"string"},{"name":"lastName","type":"string"}]}'
   {"id":1}${"id":1,"firstName":"first","lastName":"last"}
   ```
5. Test Scylla by running a select cql query:
   ```none
   cqlsh>select * from demo.example;
    id | firstname | lastname
   ----+-----------+----------
     1 |     first |     last
   ```

## Scylla modes

There are two modes, Standalone and Distributed.

* Standard - will use the properties based example.
* Distributed - will use the JSON / REST examples.

Use this command to load the connector and connect to ScyllaDB instance without authentication:

```none
curl -s -X POST -H 'Content-Type: application/json' --data @kafka-connect-scylladb.json http://localhost:8083/connectors
```

Select one of the following configuration methods based on how you have deployed `|kconnect-long|`. Distributed Mode will the JSON / REST examples. The standalone mode will use the properties based example.

#### NOTE
Each json record should consist of a schema and payload.

### Distributed Mode JSON example

```none
 {
  "name" : "scylladb-sink-connector",
  "config" : {
    "connector.class" : "io.connect.scylladb.ScyllaDbSinkConnector",
    "tasks.max" : "1",
    "topics" : "topic1,topic2,topic3",
    "scylladb.contact.points" : "scylladb-hosts",
    "scylladb.keyspace" : "test",
    "key.converter" : "org.apache.kafka.connect.json.JsonConverter",
    "value.converter" : "org.apache.kafka.connect.json.JsonConverter"
    "key.converter.schemas.enable" : "true",
    "value.converter.schemas.enable" : "true",

    "transforms" : "createKey",
    "transforms.createKey.fields" : "[field-you-want-as-primary-key-in-scylla]",
    "transforms.createKey.type" : "org.apache.kafka.connect.transforms.ValueToKey"
  }
}
```

### Standalone Mode JSON example

To load the connector in Standalone mode use:

```none
confluent local load scylladb-sink-conector -- -d scylladb-sink-connector.properties
```

Use the following configuratopn settings:

```none
connector.class=io.connect.scylladb.ScyllaDbSinkConnector
tasks.max=1
topics=topic1,topic2,topic3
scylladb.contact.points=cassandra
scylladb.keyspace=test

key.converter=org.apache.kafka.connect.json.JsonConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
key.converter.schemas.enable=true
value.converter.schemas.enable=true

transforms=createKey
transforms.createKey.fields=[field-you-want-as-primary-key-in-scylla]
transforms.createKey.type=org.apache.kafka.connect.transforms.ValueToKey
```

For Example:

```none
kafka-console-producer --broker-list localhost:9092 --topic sample-topic
>{"schema":{"type":"struct","fields":[{"type":"int32","optional":false,"field":"id"},{"type":"string","optional":false,"field":"name"},{"type":"string","optional":true,"field":"department"},"payload":{"id":10,"name":"John Doe10","department":"engineering"}}
```

Run the select cql query to view the data:

```none
Select * from keyspace_name.topic-name;
```

#### NOTE
To publish records in Avro Format use the following properties:

```none
key.converter=io.confluent.connect.avro.AvroConverter
key.converter.schema.registry.url=http://localhost:8081
value.converter=io.confluent.connect.avro.AvroConverter
value.converter.schema.registry.url=http://localhost:8081
key.converter.schemas.enable=true
value.converter.schemas.enable=true
```

## Authentication

This example connects to a Scylla instance with security enabled and username / password authentication.

Select one of the following configuration methods based on how you have deployed `|kconnect-long|`. Distributed Mode will the JSON / REST examples. The standalone mode will use the properties based example.

### Distributed Mode example

```none
{
  "name" : "scylladbSinkConnector",
  "config" : {
    "connector.class" : "io.connect.scylladb.ScyllaDbSinkConnector",
    "tasks.max" : "1",
    "topics" : "topic1,topic2,topic3",
    "scylladb.contact.points" : "cassandra",
    "scylladb.keyspace" : "test",
    "scylladb.security.enabled" : "true",
    "scylladb.username" : "example",
    "scylladb.password" : "password",
    **add other properties same as in the above example**
  }
}
```

### Standalone Mode example

```none
connector.class=io.connect.scylladb.ScyllaDbSinkConnector
tasks.max=1
topics=topic1,topic2,topic3
scylladb.contact.points=cassandra
scylladb.keyspace=test
scylladb.ssl.enabled=true
scylladb.username=example
scylladb.password=password
```

## Logging

To check logs for the Confluent Platform use:

```none
confluent local log <service> -- [<argument>] --path <path-to-confluent>
```

To check logs for Scylla:

```none
docker logs some-scylla | tail
```

## Additional information

* [Kafka Sink Connector Configuration](https://opensource.docs.scylladb.com/branch-6.0/using-scylla/integrations/sink-config.md)
