================================
Kafka Sink Connector Quickstart
================================

**Topic: Kafka Connector**

**Learn: how to setup the ScyllaDB Sink Connector against a Dockerized ScyllaDB**

**Audience: Application Developer**


Synopsis
--------

This quickstart will show how to setup the ScyllaDB Sink Connector against a Dockerized ScyllaDB.

Preliminary setup
-----------------
#. Using `Docker <https://hub.docker.com/r/scylladb/scylla/>`_, follow the instructions to launch Scylla.
#. Start the Docker container, replacing the ``--name`` and ``--host name`` parameters with your own information. For example:

   .. code-block:: none

      docker run --name some-scylla --hostname some-scylla -d scylladb/scylla

#. Run ``docker ps`` to show the exposed ports. The output should be similar to this example:

   .. code-block:: none

      docker ps 
      CONTAINER ID        IMAGE                     COMMAND                  CREATED             STATUS              PORTS                                              NAMES
      26cc6d47efe3        replace-with-image-name   "/docker-entrypoint.…"   4 hours ago         Up 23 seconds       0.0.0.0:32777->1883/tcp, 0.0.0.0:32776->9001/tcp   anonymous_my_1

#. Continue with either Confluent or Manual Installation.

Install using Confluent Platform
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you are new to Confluent, `download Confluent Platform <https://www.confluent.io/download/>`_.

#. In the Self managed software box, click **DOWNLOAD FREE** 

#. Fill in your email address.

#. Open the **Select Deployment Type** drop-down and select **ZIP**. 

#. Accept the Terms & Conditions and click **DOWNLOAD FREE**.

#. You will receive an email with instructions. Download / move the file to the desired location.

#. Continue with the setup following `this document <https://docs.confluent.io/current/quickstart/ce-quickstart.html#ce-quickstart>`_.

Install Kafka Connector manually
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

#. Navigate to the Kafka Connect Scylladb Sink `github page <https://github.com/scylladb/kafka-connect-scylladb>`_ and clone the repository.
#. Using a terminal, open the source code (src) folder.
#. Run the command ``mvn clean install``.
#. Run the Integration Tests in an IDE. If tests fail run ``mvn clean install -DskipTests``.

   .. note:: To run Integration Tests, there is no need to run Confluent. Use docker-compose.yml file in the github repository and run the following command (it contains images to run Kafka and other services):

      .. code-block:: none
   
         docker-compose -f docker-compose.yml up

   After completion of the above steps, a folder named ``components`` will be created in the target folder of the source code folder. The Connector jar files are present in ``{source-code-folder}/target/components/packages/[jar-files]``
   Create a folder by the name of ``ScyllaDB-Sink-Connector`` and copy the jar files into it. Navigate to your Confluent Platform installation directory and place this folder in ``{confluent-directory}/share/java``.

Add Sink Connector plugin
-------------------------

The Scylla sink connector is used to publish records from a Kafka topic into Scylla. 
Adding a new connector plugin requires restarting Connect. 
Use the Confluent CLI to restart Connect.

#. Run the following

   .. code-block:: none
  
      confluent local stop && confluent local start

   The output will be similar to:

   .. code-block:: none

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

#. Check if the kafka-connect-scylladb connector plugin has been installed correctly and picked up by the plugin loader:

   .. code-block:: none

      curl -sS localhost:8083/connector-plugins | jq .[].class | grep ScyllaDbSinkConnector

   Your output should resemble:

   ``io.connect.scylladb.ScyllaDbSinkConnector``

Connector configuration
-----------------------

#. Save the configuration settings in a file named ``kafka-connect-scylladb.json`` its contents should contain:

   .. code-block:: json

      {
           "name" : "scylladb-sink-connector",
           "config" : {
             "connector.class" : "io.connect.scylladb.ScyllaDbSinkConnector",
             "tasks.max" : "1",
             "topics" : "topic1,topic2,topic3",
             "scylladb.contact.points" : "scylladb-hosts",
             "scylladb.keyspace" : "test"
      }

#. Load the connector. Run the following command:

   .. code-block:: shell

      curl -s -X POST -H 'Content-Type: application/json' --data @kafka-connect-scylladb.json http://localhost:8083/connectors

#. Update the configuration of the existing connector.

   .. code-block:: shell

      curl -s -X PUT -H 'Content-Type: application/json' --data @kafka-connect-scylladb.json http://localhost:8083/connectors/scylladb/config

#. Once the Connector is up and running, use the command ``kafka-avro-console-producer`` to produce records(in AVRO format) into the Kafka topic.

   .. code-block:: none

      kafka-avro-console-producer 
      --broker-list localhost:9092 
      --topic example  
      --property parse.key=true 
      --property key.schema='{"type":"record",name":"key_schema","fields":[{"name":"id","type":"int"}]}' 
      --property "key.separator=$" 
      --property value.schema='{"type":"record","name":"value_schema","fields":[{"name":"id","type":"int"},
      {"name":"firstName","type":"string"},{"name":"lastName","type":"string"}]}'
      {"id":1}${"id":1,"firstName":"first","lastName":"last"}

#. Test Scylla by running a select cql query:

   .. code-block:: none

     cqlsh>select * from demo.example;
      id | firstname | lastname
     ----+-----------+----------
       1 |     first |     last

Scylla modes
------------

There are two modes, Standalone and Distributed.

* Standard - will use the properties based example.
* Distributed - will use the JSON / REST examples. 

Use this command to load the connector and connect to ScyllaDB instance without authentication:

.. code-block:: none

   curl -s -X POST -H 'Content-Type: application/json' --data @kafka-connect-scylladb.json http://localhost:8083/connectors

Select one of the following configuration methods based on how you have deployed ``|kconnect-long|``. Distributed Mode will the JSON / REST examples. The standalone mode will use the properties based example.

.. note:: Each json record should consist of a schema and payload.

Distributed Mode JSON example
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: none

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

Standalone Mode JSON example
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To load the connector in Standalone mode use:

.. code-block:: none

   confluent local load scylladb-sink-conector -- -d scylladb-sink-connector.properties

Use the following configuratopn settings:

.. code-block:: none

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

For Example:

.. code-block:: none

   kafka-console-producer --broker-list localhost:9092 --topic sample-topic
   >{"schema":{"type":"struct","fields":[{"type":"int32","optional":false,"field":"id"},{"type":"string","optional":false,"field":"name"},{"type":"string","optional":true,"field":"department"},"payload":{"id":10,"name":"John Doe10","department":"engineering"}}

Run the select cql query to view the data:

.. code-block:: none

   Select * from keyspace_name.topic-name;

.. note:: To publish records in Avro Format use the following properties:

.. code-block:: none

   key.converter=io.confluent.connect.avro.AvroConverter
   key.converter.schema.registry.url=http://localhost:8081
   value.converter=io.confluent.connect.avro.AvroConverter
   value.converter.schema.registry.url=http://localhost:8081
   key.converter.schemas.enable=true
   value.converter.schemas.enable=true

Authentication
--------------

This example connects to a Scylla instance with security enabled and username / password authentication.

Select one of the following configuration methods based on how you have deployed ``|kconnect-long|``. Distributed Mode will the JSON / REST examples. The standalone mode will use the properties based example.

Distributed Mode example
^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: none

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

Standalone Mode example
^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: none

   connector.class=io.connect.scylladb.ScyllaDbSinkConnector
   tasks.max=1
   topics=topic1,topic2,topic3
   scylladb.contact.points=cassandra
   scylladb.keyspace=test
   scylladb.ssl.enabled=true
   scylladb.username=example
   scylladb.password=password

Logging
-------

To check logs for the Confluent Platform use:

.. code-block:: none

   confluent local log <service> -- [<argument>] --path <path-to-confluent>

To check logs for Scylla:

.. code-block:: none

   docker logs some-scylla | tail

Additional information
----------------------

* :doc:`Kafka Sink Connector Configuration </using-scylla/integrations/sink-config>`
