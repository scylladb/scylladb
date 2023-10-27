==============================================
Scylla CDC Source Connector Quickstart
==============================================


Synopsis
--------

This quickstart will show you how to setup the Scylla CDC Source Connector to replicate changes made in 
a Scylla table using :doc:`Scylla CDC <../cdc/cdc-intro>`.

Scylla setup
------------

First, let's setup a Scylla cluster and create a CDC-enabled table.

Scylla installation
^^^^^^^^^^^^^^^^^^^

For the purpose of this quickstart, we will configure a Scylla instance using Docker. You can skip this 
section if you have already installed Scylla. To learn more about installing Scylla in production
environments, please refer to the :doc:`Install Scylla page </getting-started/install-scylla/index>`.

#. Using `Docker <https://hub.docker.com/r/scylladb/scylla/>`_, follow the instructions to launch Scylla.
#. Start the Docker container, replacing the ``--name`` and ``--host name`` parameters with your own information. For example:

   .. code-block:: bash

      docker run --name scylla-cdc-quickstart --hostname scylla-cdc-quickstart -d scylladb/scylla
      docker run --name scylla-cdc-quickstart-2 --hostname scylla-cdc-quickstart-2 -d scylladb/scylla --seeds="$(docker inspect --format='{{ .NetworkSettings.IPAddress }}' scylla-cdc-quickstart)"
      docker run --name scylla-cdc-quickstart-3 --hostname scylla-cdc-quickstart-3 -d scylladb/scylla --seeds="$(docker inspect --format='{{ .NetworkSettings.IPAddress }}' scylla-cdc-quickstart)"

#. Run ``docker ps`` to show the exposed ports. The output should be similar to this example:

   .. code-block:: none

      docker ps
      CONTAINER ID   IMAGE                 COMMAND                  CREATED              STATUS              PORTS                                                            NAMES
      b72f341f53c0   scylladb/scylla       "/docker-entrypoint.…"   12 seconds ago       Up 11 seconds       22/tcp, 7000-7001/tcp, 9042/tcp, 9160/tcp, 9180/tcp, 10000/tcp   scylla-cdc-quickstart-3
      e1ac1ccb4d12   scylladb/scylla       "/docker-entrypoint.…"   16 seconds ago       Up 15 seconds       22/tcp, 7000-7001/tcp, 9042/tcp, 9160/tcp, 9180/tcp, 10000/tcp   scylla-cdc-quickstart-2
      f1668fba1e7b   scylladb/scylla       "/docker-entrypoint.…"   About a minute ago   Up About a minute   22/tcp, 7000-7001/tcp, 9042/tcp, 9160/tcp, 9180/tcp, 10000/tcp   scylla-cdc-quickstart

Creating a CDC-enabled table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Let's connect to your Scylla cluster and create a new CDC-enabled table. We will create an example table by 
issuing the following CQL query and insert some example data:

.. code-block:: cql

   CREATE KEYSPACE quickstart_keyspace WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3};

   CREATE TABLE quickstart_keyspace.orders(
      customer_id int, 
      order_id int, 
      product text, 
      PRIMARY KEY(customer_id, order_id)) WITH cdc = {'enabled': true};

   INSERT INTO quickstart_keyspace.orders(customer_id, order_id, product) VALUES (1, 1, 'pizza'); 
   INSERT INTO quickstart_keyspace.orders(customer_id, order_id, product) VALUES (1, 2, 'cookies');
   INSERT INTO quickstart_keyspace.orders(customer_id, order_id, product) VALUES (1, 3, 'tea');

If you already have a table you wish to use, but it does not have CDC enabled, you can turn it on by using the following CQL query:

.. code-block:: cql

   ALTER TABLE keyspace.table_name with cdc = {'enabled': true};

To learn more about Scylla CDC, visit :doc:`Change Data Capture (CDC) page <../cdc/index>`.

Kafka setup
-----------

Scylla CDC Source Connector works well with both `open-source Kafka <https://kafka.apache.org/>`_ 
and `Confluent Platform <https://www.confluent.io/>`_. In this quickstart we will show how
to install the Confluent Platform and deploy the connector (applicable to both open-source Kafka
and Confluent Platform).

Installing Confluent Platform
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you are new to Confluent, `download Confluent Platform <https://www.confluent.io/download/>`_.

#. In the *Download Confluent Platform* section fill in your email address
#. Open the *Select Deployment Type* drop-down and select *ZIP*
#. Accept the Terms & Conditions and click *DOWNLOAD FREE*
#. You will receive an email with instructions. Download / move the file to the desired location
#. Continue with the setup following `this document <https://docs.confluent.io/current/quickstart/ce-quickstart.html#ce-quickstart>`_

Installing Scylla CDC Source Connector
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

#. Download or build Scylla CDC Source Connector using `the project build instructions <https://github.com/scylladb/scylla-cdc-source-connector#building>`_

#. Deploy the connector:

   #. If you use Confluent Platform, move connector JAR files to the ``share/java`` folder

   #. If you use open-source Kafka, make sure that ``plugin.path`` of Kafka Connect configuration contains the folder with connector JAR files

Connector configuration
-----------------------

After you have successfully configured Scylla and Kafka, the next step is to configure the connector
and start it up.

Configuration using Confluent Control Center
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you use Confluent Platform, the easiest way to configure and start up the Scylla CDC Source Connector
is to use Confluent Control Center web interface.

#. Open the Confluent Control Center. By default, it is started at port ``9021``:

   .. image:: images/scylla-cdc-source-connector-control-center1.png
       :align: left
       :alt: Confluent Control Center main page

#. Click on the cluster you want to start the connector in and open the "Connect" tab:

   .. image:: images/scylla-cdc-source-connector-control-center2.png
       :align: left
       :alt: Confluent Control Center "Connect" tab

#. Click on the Kafka Connect cluster:

   .. image:: images/scylla-cdc-source-connector-control-center3.png
       :align: left
       :alt: Confluent Control Center "connect-default" cluster

#. Click "Add connector":

   .. image:: images/scylla-cdc-source-connector-control-center4.png
       :align: left
       :alt: Confluent Control Center "Add connector"

#. Click "ScyllaConnector (Source Connector)":

   .. image:: images/scylla-cdc-source-connector-control-center5.png
       :align: left
       :alt: Confluent Control Center "ScyllaConnector (Source Connector)"

#. Configure the connector. You need to fill in these required configuration parameters:

   #. Name: the name of this configuration
   #. Key converter class, value converter class: converters that determine the format 
      of produced messages. You can read more about them at `Kafka Connect Deep Dive – Converters and Serialization Explained <https://www.confluent.io/blog/kafka-connect-deep-dive-converters-serialization-explained/>`_
   #. Hosts: contact points of Scylla
   #. Namespace: a unique name that identifies the Scylla cluster and that is used as a prefix for all schemas, topics.
   #. Table names: the names of CDC-enabled tables you want to replicate

   For the quickstart example here are the values we will use:

   #. Name: ``QuickstartConnector``
   #. Key converter class, value converter class: ``org.apache.kafka.connect.json.JsonConverter``
   #. Hosts: ``172.17.0.2:9042`` (Scylla started in Docker)
   #. Namespace: ``QuickstartConnectorNamespace``
   #. Table names: ``quickstart_keyspace.orders``

   .. image:: images/scylla-cdc-source-connector-control-center6.png
       :align: left
       :alt: Confluent Control Center connector configuration

#. Click "Continue" and "Launch"

#. After a short while, a new ``QuickstartConnectorNamespace.quickstart_keyspace.orders`` topic will be automatically created
   and inserted rows will be replicated. You can browse them by going to the "Topics" tab, selecting 
   ``QuickstartConnectorNamespace.quickstart_keyspace.orders`` topic, going to "Message" tab and inputting ``0`` to "Jump to offset"
   field:

    .. image:: images/scylla-cdc-source-connector-control-center7.png
       :align: left
       :alt: Confluent Control Center connector messages

Configuration using open-source Kafka
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

#. Start Kafka Connect standalone using `this guide <https://kafka.apache.org/documentation/#connect_running>`_. You
   will have to create a ``connector.properties`` file with the following contents:
   
   .. code-block:: none

      name = QuickstartConnector
      connector.class = com.scylladb.cdc.debezium.connector.ScyllaConnector
      key.converter = org.apache.kafka.connect.json.JsonConverter
      value.converter = org.apache.kafka.connect.json.JsonConverter
      scylla.cluster.ip.addresses = 172.17.0.2:9042
      scylla.name = QuickstartConnectorNamespace
      scylla.table.names = quickstart_keyspace.orders

#. After starting the connector, you can see the generated messages by using ``kafka-console-consumer`` tool:

   .. code-block:: bash
      
      bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic QuickstartConnectorNamespace.quickstart_keyspace.orders --from-beginning

Additional information
----------------------

* `Scylla CDC Source Connector GitHub project <https://github.com/scylladb/scylla-cdc-source-connector>`_
