.. highlight:: cql

.. _delete_statement:

DELETE
^^^^^^

Deleting rows or parts of rows uses the ``DELETE`` statement:

.. code-block::
   
   delete_statement: DELETE [ `simple_selection` ( ',' `simple_selection` ) ]
                   : FROM `table_name`
                   : [ USING `update_parameter` ( AND `update_parameter` )* ]
                   : WHERE `where_clause`
                   : [ IF ( EXISTS | `condition` ( AND `condition` )* ) ]
                   : [ BYPASS LARGE_DATA_GUARDRAILS ]

For instance:

.. code-block:: cql

   DELETE FROM NerdMovies USING TIMESTAMP 1240003134000000
     WHERE movie = 'Serenity';

   DELETE phone FROM Users
     WHERE userid IN (C73DE1D3-AF08-40F3-B124-3FF3E5109F22, B70DE1D0-9908-4AE3-BE34-5573E5B09F14);

The ``DELETE`` statement deletes columns and rows. If column names are provided directly after the ``DELETE`` keyword,
only those columns are deleted from the row indicated by the ``WHERE`` clause. Otherwise, whole rows are removed.

The ``WHERE`` clause specifies which rows are to be deleted. Multiple rows may be deleted with one statement by using an
``IN`` operator. A range of rows may be deleted using an inequality operator (such as ``>=``).

``DELETE`` supports the ``TIMESTAMP`` option with the same semantics as the TIMESTAMP parameter used in the ``UPDATE`` statement.
The ``DELETE`` statement deletes data written with :ref:`INSERT <insert-statement>` or :ref:`UPDATE <update-statement>` (or :ref:`BATCH <batch_statement>`)
using a timestamp that is less than or equal to the ``DELETE`` timestamp.
For more information on the :token:`update_parameter` refer to the :ref:`UPDATE <update-parameters>` section.

In a ``DELETE`` statement, all deletions within the same partition key are applied atomically,
meaning either all columns mentioned in the statement are deleted or none.
If ``DELETE`` statement has the same timestamp as ``INSERT`` or
``UPDATE`` of the same primary key, delete operation prevails (see :ref:`update ordering <update-ordering>`).

A ``DELETE`` operation can be conditional through the use of an ``IF`` clause, similar to ``UPDATE`` and ``INSERT``
statements. Each such ``DELETE`` gets a globally unique timestamp.
However, as with ``INSERT`` and ``UPDATE`` statements, this will incur a non-negligible performance cost
(internally, Paxos will be used) and so should be used sparingly.

The ``BYPASS LARGE_DATA_GUARDRAILS`` clause is a ScyllaDB CQL extension for
deleting data from tables with :ref:`large data guardrails <guardrails-large-data>`
enabled. It skips large data guardrail checks for this standalone ``DELETE`` so
that oversized data can be removed even when writes to the same partition, row,
or collection would otherwise be rejected.

Please refer to the :ref:`update parameters <update-parameters>` section for more information on the :token:`update_parameter`.


Range deletions
~~~~~~~~~~~~~~~

Range deletions allow you to delete rows from a single partition, given that the clustering key is within the given range. The delete request can be determined on both ends, or it can be open-ended.

.. _open-range-deletions:


Open range deletions
~~~~~~~~~~~~~~~~~~~~

Open range deletions delete rows based on an open-ended request (>, <, >=, =<, etc.)

For example, suppose your data set for events at Madison Square Garden includes:


.. code-block:: none

   CREATE KEYSPACE mykeyspace WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': '3'}  AND durable_writes = true;
   use mykeyspace ;
   CREATE TABLE events ( id text, created_at date, content text, PRIMARY KEY (id, created_at) );
   
   INSERT into events (id, created_at, content) VALUES ('concert', '2019-11-19', 'SuperM');
   INSERT into events (id, created_at, content) VALUES ('concert', '2019-11-15', 'Billy Joel');
   INSERT into events (id, created_at, content) VALUES ('game', '2019-11-03', 'Knicks v Sacramento');
   INSERT into events (id, created_at, content) VALUES ('concert', '2019-10-31', 'Dead & Company');
   INSERT into events (id, created_at, content) VALUES ('game', '2019-10-28', 'Knicks v Chicago');
   INSERT into events (id, created_at, content) VALUES ('concert', '2019-10-25', 'Billy Joel');


   SELECT * from events;

    id      | created_at | content
   ---------+------------+---------------------
       game | 2019-10-28 |    Knicks v Chicago
       game | 2019-11-03 | Knicks v Sacramento
    concert | 2019-10-25 |          Billy Joel
    concert | 2019-10-31 |      Dead & Company
    concert | 2019-11-15 |          Billy Joel
    concert | 2019-11-19 |              SuperM

   (6 rows)

And you wanted to delete all of the concerts from the month of October using an open-ended range delete. You would run:

.. code-block:: none
  
   DELETE FROM events WHERE id='concert' AND created_at <= '2019-10-31';

   SELECT * from events;

    id      | created_at | content
   ---------+------------+---------------------
       game | 2019-10-28 |    Knicks v Chicago
       game | 2019-11-03 | Knicks v Sacramento
    concert | 2019-11-15 |          Billy Joel
    concert | 2019-11-19 |              SuperM

   (4 rows)


.. _delete-all-data-statement:

DELETE ALL DATA
^^^^^^^^^^^^^^^

Deleting all the data of a table uses the ``DELETE ALL DATA`` statement:

.. code-block::

   delete_all_data_statement: DELETE ALL DATA FROM `table_name`
                            : [ USING `delete_all_data_parameter` ( AND `delete_all_data_parameter` )* ]
   delete_all_data_parameter: ( TIMESTAMP `int_value` | TIMEOUT `duration` )

For instance:

.. code-block:: cql

   DELETE ALL DATA FROM NerdMovies;

   DELETE ALL DATA FROM NerdMovies USING TIMESTAMP 1240003134000000 AND TIMEOUT 30s;

The ``DELETE ALL DATA`` statement deletes all the data of the table that was written with a timestamp less than or
equal to the statement's timestamp, like a ``DELETE`` of every partition of the table, and keeps the data written with
a higher timestamp. Writes concurrent with the statement are resolved by their timestamp, like writes concurrent with a
``DELETE``. The data of the table's materialized views and secondary indexes is deleted with it, with the same timestamp.

The timestamp is given by the ``USING TIMESTAMP`` clause, or else by the client-side timestamp, if the driver sends one,
or else by the coordinator. ``USING TIMEOUT`` sets the timeout of the request; the default is the timeout
of ``TRUNCATE`` (``truncate_request_timeout_in_ms``), not that of writes, since the statement waits for the cluster's metadata to be updated. ``USING TTL`` is not supported. The statement requires the ``MODIFY`` permission on
the table.

.. caution:: Most drivers send a client-side timestamp with every request by default. If the clock of the client which
   deletes the data is behind the clock of a client which wrote data shortly before, that data has a higher timestamp
   than the deletion, and is not deleted. Likewise, data written with timestamps in the future is not deleted.
   Use ``USING TIMESTAMP`` when it matters exactly which data is deleted.

``DELETE ALL DATA`` is supported only for tables in keyspaces which use tablets, and not for strongly consistent tables.
For other tables it returns an error; use :ref:`TRUNCATE <truncate-statement>` for them. It is available once all the
nodes of the cluster are upgraded to a version that supports it. It cannot be used on a materialized view; delete the
data of the base table instead. It is not supported for tables which have a vector index.

``DELETE ALL DATA`` differs from a regular ``DELETE`` in the following ways:

* It has no consistency level. The deletion is not written to the table's replicas like other writes: it is recorded in
  the cluster's metadata, which, like a schema change, requires a majority (quorum) of the cluster's nodes to be
  available, and does not require the table's replicas to be alive. A replica which is down observes the deletion
  when it comes back.
* It cannot be part of a ``BATCH``, and it has no ``IF`` clause.
* It does not write a CDC log entry, and it does not delete anything from the table's CDC log table.
* Counters can be updated again after it, and count from zero. A counter deleted by a regular ``DELETE`` cannot be used
  again. The outcome of counter updates concurrent with ``DELETE ALL DATA`` is undefined, so they should be stopped
  while the data of a counter table is deleted.

The deleted data is not removed from disk right away. It is removed as the table's SSTables are compacted, and major
compaction (``nodetool compact``) removes all of it. SSTables which hold only deleted data, typically most of the
table's SSTables, are removed without being rewritten.

The deletion is remembered like a tombstone: while it is, data written or loaded into the table with a timestamp at or
before the deletion timestamp is deleted too. The deletion is forgotten under the table's ``tombstone_gc`` rules
(see :ref:`Tombstones GC options <ddl-tombstones-gc>`), once no replica holds data which it deleted. Whether it was
forgotten can be read from the table's partition in ``system.tablets``, where ``data_deleted_at_timestamp`` is null when
no deletion is remembered:

.. code-block:: cql

   SELECT id FROM system_schema.tables WHERE keyspace_name = 'mykeyspace' AND table_name = 'nerdmovies';
   SELECT data_deleted_at_timestamp FROM system.tablets WHERE table_id = <id> LIMIT 1;

:ref:`TRUNCATE <truncate-statement>` of the table also makes the deletion forgotten.

Unlike :ref:`TRUNCATE <truncate-statement>`, ``DELETE ALL DATA`` may be used while the table is written to, and does not
remove data regardless of its timestamp. To empty a table before restoring a backup into it, use ``TRUNCATE``:
restored data which is older than a ``DELETE ALL DATA`` would stay deleted.


:doc:`Apache Cassandra Query Language (CQL) Reference </cql/index>`

.. include:: /rst_include/apache-copyrights.rst


.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at
..
..     http://www.apache.org/licenses/LICENSE-2.0
..
.. Unless required by applicable law or agreed to in writing, software
.. distributed under the License is distributed on an "AS IS" BASIS,
.. WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
.. See the License for the specific language governing permissions and
.. limitations under the License.
