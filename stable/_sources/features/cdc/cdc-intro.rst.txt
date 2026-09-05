============
CDC Overview
============


:abbr:`CDC (Change Data Capture)` is a feature that allows you to not only query the current state of a database's table, but also query the history of all changes made to the table.

As an example, suppose you made a sequence of changes to some table in the given order:

.. code-block:: cql

    UPDATE ks.t SET v = 0 WHERE pk = 0 AND ck = 0;
    UPDATE ks.t SET v = 1 WHERE pk = 0 AND ck = 0;
    UPDATE ks.t SET v = 2 WHERE pk = 0 AND ck = 0;
    UPDATE ks.t SET v = 2 WHERE pk = 0 AND ck = 1;
    UPDATE ks.t SET v = 1 WHERE pk = 0 AND ck = 1;
    UPDATE ks.t SET v = 0 WHERE pk = 0 AND ck = 1;

Normally, querying the table would return

.. code-block:: cql

     pk | ck | v
    ----+----+---
      0 |  0 | 2
      0 |  1 | 0

    (2 rows)

but with CDC, you can also learn the history of all changes:

.. code-block:: none


   change at 2020-01-29 14:37:32: UPDATE ks.t SET v = 0 WHERE pk = 0 AND ck = 0;
   change at 2020-01-29 14:37:33: UPDATE ks.t SET v = 1 WHERE pk = 0 AND ck = 0;
   change at 2020-01-29 14:37:35: UPDATE ks.t SET v = 2 WHERE pk = 0 AND ck = 0; <- latest change
   change at 2020-01-29 14:37:38: UPDATE ks.t SET v = 2 WHERE pk = 0 AND ck = 1;
   change at 2020-01-29 14:37:39: UPDATE ks.t SET v = 1 WHERE pk = 0 AND ck = 1;
   change at 2020-01-29 14:37:40: UPDATE ks.t SET v = 0 WHERE pk = 0 AND ck = 1; <- latest change

(not an actual syntax, the above example just presents the general concept).

Use cases for CDC
-----------------

Some examples where CDC may be beneficial:

* Heterogeneous database replication: applying captured changes to another database or table. The other database may use a different schema (or no schema at all), better suited for some specific workloads. An example is replication to ElasticSearch for efficient text searches.
* Implementing a notification system.
* In-flight analytics: looking for patterns in the changes in order to derive useful information, e.g. for fraud detection.

In ScyllaDB CDC is optional and enabled on a per-table basis. The history of changes made to a CDC-enabled table is stored in a separate associated table.

Terminology
-----------

* **Base Table** - this is the original table, where all changes are made.
* **Log Table** - this is the table associated to the base table which is created when CDC is enabled. Read about it in the :doc:`log table document <./cdc-log-table>`.

Enabling CDC
------------

You can enable CDC when creating or altering a table using the ``cdc`` option, for example:

.. code-block:: none

    CREATE TABLE ks.t (pk int, ck int, v int, PRIMARY KEY (pk, ck, v)) WITH cdc = {'enabled':true};

.. note::
   If you enabled CDC and later decide to disable it, you need to **stop all writes** to the base table before issuing the ``ALTER TABLE ... WITH cdc = {'enabled':false};`` command.

.. include:: /features/cdc/_common/cdc-params.rst

Using CDC with Applications
---------------------------

When writing applications, you can now use our language specific libraries to simplify writing applications which will read from ScyllaDB CDC.
The following libraries are available:

* `Go <https://github.com/scylladb/scylla-cdc-go>`_
* `Java <https://github.com/scylladb/scylla-cdc-java>`_
* `Rust <https://github.com/scylladb/scylla-cdc-rust>`_

More information
----------------

`ScyllaDB University: Change Data Capture (CDC) lesson <https://university.scylladb.com/courses/data-modeling/lessons/change-data-capture-cdc/>`_ -  Learn how to use CDC. Some of the topics covered are:

* An overview of Change Data Capture,  what exactly is it, what are some common use cases, what does it do, and an overview of how it works
* How can that data be consumed? Different options for consuming the data changes including normal CQL, a layered approach, and integrators
* How does CDC work under the hood? Covers an example of what happens in the DB on different operations to allow CDC
* A summary of  CDC: It’s easy to integrate and consume, it uses plain CQL tables, it’s robust, it’s replicated in the same way as normal data, it has a reasonable overhead, it does not overflow if the consumer fails to act and data is TTL’ed. The summary also includes a comparison with Cassandra, DynamoDB, and MongoDB.
