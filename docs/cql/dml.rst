.. highlight:: cql

Data Manipulation
-----------------

This following pages describes the statements supported by CQL to insert, update, delete, and query data, followed by
sections common to data updating statements.

.. toctree::
   :maxdepth: 1

   dml/select
   dml/insert
   dml/update
   dml/delete
   dml/batch

.. _update-parameters:

Update parameters
~~~~~~~~~~~~~~~~~

The ``UPDATE``, ``INSERT`` (and ``DELETE`` and ``BATCH`` for the ``TIMESTAMP`` and ``TIMEOUT``) statements support the following
parameters:

- ``TIMESTAMP``: sets the timestamp for the operation. If not specified, the coordinator will use the current time, in
  *microseconds* since the `Unix epoch <https://en.wikipedia.org/wiki/Unix_time>`_ (January 1st 1970 at 00:00:00 UTC),
  at the start of statement execution as the timestamp. This is usually a suitable default.
  :ref:`INSERT <insert-statement>`, :ref:`UPDATE <update-statement>`, :ref:`DELETE <delete_statement>`, or :ref:`BATCH <batch_statement>`
  statements ``USING TIMESTAMP`` should provide a unique timestamp value, similar to the one
  implicitly set by the coordinator by default, when the `USING TIMESTAMP` update parameter is absent.
  ScyllaDB ensures that query timestamps created by the same coordinator node are unique (even across different shards
  on the same node). However, timestamps assigned at different nodes are not guaranteed to be globally unique.
  Note that with a steadily high write rate, timestamp collision is not unlikely. If it happens, e.g. two INSERTS
  have the same timestamp, a conflict resolution algorithm determines which of the inserted cells prevails (see :ref:`update ordering <update-ordering>` for more information):
- ``TTL``: specifies an optional Time To Live (in seconds) for the inserted values. If set, the inserted values are
  automatically removed from the database after the specified time. Note that the TTL concerns the inserted values, not
  the columns themselves. This means that any subsequent update of the column will also reset the TTL (to whatever TTL
  is specified in that update). By default, values never expire. A TTL of 0 is equivalent to no TTL. If the table has a
  default_time_to_live, a TTL of 0 will remove the TTL for the inserted or updated values. A TTL of ``null`` is equivalent
  to inserting with a TTL of 0. You can read more about TTL in the :doc:`documentation </cql/time-to-live>` and also in `this ScyllaDB University lesson <https://university.scylladb.com/courses/data-modeling/lessons/advanced-data-modeling/topic/expiring-data-with-ttl-time-to-live/>`_.
- ``TIMEOUT``: specifies a timeout duration for the specific request.
  Please refer to the :ref:`SELECT <using-timeout>` section for more information.

.. _update-ordering:

Update ordering
~~~~~~~~~~~~~~~

:ref:`INSERT <insert-statement>`, :ref:`UPDATE <update-statement>`, and :ref:`DELETE <delete_statement>`
operations are ordered by their ``TIMESTAMP``.

Ordering of such changes is done at the cell level, where each cell carries a write ``TIMESTAMP``,
other attributes related to its expiration when it has a non-zero time-to-live (``TTL``),
and the cell value.

The fundamental rule for ordering cells that insert, update, or delete data in a given row and column
is that the cell with the highest timestamp wins.

However, it is possible that multiple such cells will carry the same ``TIMESTAMP``.
There could be several reasons for ``TIMESTAMP`` collision:

* Benign collision can be caused by "replay" of a mutation, e.g., due to client retry, or due to internal processes.
  In such cases, the cells are equivalent, and any of them can be selected arbitrarily.
* ``TIMESTAMP`` collisions might be normally caused by parallel queries that are served
  by different coordinator nodes. The coordinators might calculate the same write ``TIMESTAMP``
  based on their local time in microseconds.
* Collisions might also happen with user-provided timestamps if the application does not guarantee
  unique timestamps with the ``USING TIMESTAMP`` parameter (see :ref:`Update parameters <update-parameters>` for more information).

As said above, in the replay case, ordering of cells should not matter, as they carry the same value
and same expiration attributes, so picking any of them will reach the same result.
However, other ``TIMESTAMP`` conflicts must be resolved in a consistent way by all nodes.
Otherwise, if nodes would have picked an arbitrary cell in case of a conflict and they would
reach different results, reading from different replicas would detect the inconsistency and trigger
read-repair that will generate yet another cell that would still conflict with the existing cells,
with no guarantee for convergence.

Therefore, ScyllaDB implements an internal, consistent conflict-resolution algorithm
that orders cells with conflicting ``TIMESTAMP`` values based on other properties, like:

* whether the cell is a tombstone or a live cell,
* whether the cell has an expiration time,
* the cell ``TTL``,
* and finally, what value the cell carries.

The conflict-resolution algorithm is documented in `ScyllaDB's internal documentation <https://github.com/scylladb/scylladb/blob/master/docs/dev/timestamp-conflict-resolution.md>`_
and it may be subject to change.

Reliable serialization can be achieved using unique write ``TIMESTAMP``
and by using :doc:`Lightweight Transactions (LWT) </features/lwt>` to ensure atomicity of
:ref:`INSERT <insert-statement>`, :ref:`UPDATE <update-statement>`, and :ref:`DELETE <delete_statement>`.


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
