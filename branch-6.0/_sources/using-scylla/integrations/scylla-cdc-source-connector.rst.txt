==========================================
Scylla CDC Source Connector
==========================================

.. toctree::
   :hidden:

   scylla-cdc-source-connector-quickstart

Scylla CDC Source Connector is a source connector capturing row-level changes in the tables of a Scylla cluster. It is a Debezium connector, compatible with Kafka Connect (with Kafka 2.6.0+) and built on top of scylla-cdc-java library. The source code of the connector is available at `GitHub <https://github.com/scylladb/scylla-cdc-source-connector>`_.

The connector reads the CDC log for specified tables and produces Kafka messages for each row-level ``INSERT``, ``UPDATE`` or ``DELETE`` operation. The connector is able to split reading the CDC log across multiple processes: the connector can start a separate Kafka Connect task for reading each :doc:`Vnode of Scylla cluster </architecture/ringarchitecture/index>` allowing for high throughput. You can limit the number of started tasks by using ``tasks.max`` property.

Scylla CDC Source Connector seamlessly handles schema changes and topology changes (adding, removing nodes from Scylla cluster). The connector is fault-tolerant, retrying reading data from Scylla in case of failure. It periodically saves the current position in Scylla CDC log using Kafka Connect offset tracking (configurable by ``offset.flush.interval.ms`` parameter). If the connector is stopped, it is able to resume reading from previously saved offset. Scylla CDC Source Connector has at-least-once semantics.

The connector has the following capabilities:

* Kafka Connect connector using Debezium framework
* Replication of row-level changes from Scylla using :doc:`Scylla CDC <../cdc/cdc-intro>`. The connector replicates the following operations: ``INSERT``, ``UPDATE``, ``DELETE`` (single row deletes)
* High scalability - able to split work across multiple Kafka Connect workers
* Fault tolerant - connector periodically saves its progress and can resume from previously saved offset (with at-least-once semantics)
* Support for many standard Kafka Connect converters, such as JSON and Avro
* Compatible with standard Kafka Connect transformations
* Metadata about CDC events - each generated Kafka message contains information about source, such as timestamp and table name
* Seamless handling of schema changes and topology changes (adding, removing nodes from Scylla cluster)

The connector has the following limitations:

* Only Kafka 2.6.0+ is supported
* Only row-level operations are produced (``INSERT``, ``UPDATE``, ``DELETE``) - partition deletes and row range deletes are not replicated
* No support for collection types (``LIST``, ``SET``, ``MAP``) and ``UDT`` - columns with those types are omitted from generated messages
* Preimage and postimage - changes only contain those columns that were modified, not the entire row before/after change

The following documents will help you get started with Scylla CDC Source Connector:

* :doc:`Scylla CDC Source Connector Quickstart <scylla-cdc-source-connector-quickstart>`