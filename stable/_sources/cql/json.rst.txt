

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

.. highlight:: cql

.. _cql-json:

JSON Support
------------

ScyllaDB introduces JSON support to :ref:`SELECT <select-statement>` and :ref:`INSERT <insert-statement>`
statements. This support does not fundamentally alter the CQL API (for example, the schema is still enforced). It simply
provides a convenient way to work with JSON documents.

SELECT JSON
^^^^^^^^^^^

With ``SELECT`` statements, the ``JSON`` keyword can be used to return each row as a single ``JSON`` encoded map. The
remainder of the ``SELECT`` statement behavior is the same.

The result map keys are the same as the column names in a normal result set. For example, a statement like ``SELECT JSON
a, ttl(b) FROM ...`` would result in a map with keys ``"a"`` and ``"ttl(b)"``. However, this is one notable exception:
for symmetry with ``INSERT JSON`` behavior, case-sensitive column names with upper-case letters will be surrounded with
double-quotes. For example, ``SELECT JSON myColumn FROM ...`` would result in a map key ``"\"myColumn\""`` (note the
escaped quotes).

The map values will ``JSON``-encoded representations (as described below) of the result set values.

INSERT JSON
^^^^^^^^^^^

With ``INSERT`` statements, the new ``JSON`` keyword can be used to enable inserting a ``JSON`` encoded map as a single
row. The format of the ``JSON`` map should generally match that returned by a ``SELECT JSON`` statement on the same
table. In particular, case-sensitive column names should be surrounded by double-quotes. For example, to insert into a
table with two columns named "myKey" and "value", you would do the following::

    INSERT INTO mytable JSON '{ "\"myKey\"": 0, "value": 0}'

By default (or if ``DEFAULT NULL`` is explicitly used), a column omitted from the ``JSON`` map will be set to ``NULL``,
meaning that any pre-existing value for that column will be removed (resulting in a tombstone being created).
Alternatively, if the ``DEFAULT UNSET`` directive is used after the value, omitted column values will be left unset,
meaning that pre-existing values for those columns will be preserved.


JSON Encoding of ScyllaDB Data Types
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Where possible, ScyllaDB will represent and accept data types in their native ``JSON`` representation. ScyllaDB will
also accept string representations matching the CQL literal format for all single-field types. For example, floats,
ints, UUIDs, and dates can be represented by CQL literal strings. However, compound types, such as collections, tuples,
and user-defined types, must be represented by native ``JSON`` collections (maps and lists) or a JSON-encoded string
representation of the collection.

The following table describes the encodings that ScyllaDB will accept in ``INSERT JSON`` values (and ``fromJson()``
arguments) as well as the format ScyllaDB will use when returning data for ``SELECT JSON`` statements (and
``fromJson()``):

=============== ======================== =============== ==============================================================
 Type            Formats accepted         Return format   Notes
=============== ======================== =============== ==============================================================
 ``ascii``       string                   string          Uses JSON's ``\u`` character escape
 ``bigint``      integer, string          integer         String must be valid 64 bit integer
 ``blob``        string                   string          String should be 0x followed by an even number of hex digits
 ``boolean``     boolean, string          boolean         String must be "true" or "false"
 ``date``        string                   string          Date in format ``YYYY-MM-DD``, timezone UTC
 ``decimal``     integer, float, string   float           May exceed 32 or 64-bit IEEE-754 floating point precision in
                                                          client-side decoder
 ``double``      integer, float, string   float           String must be valid integer or float
 ``float``       integer, float, string   float           String must be valid integer or float
 ``inet``        string                   string          IPv4 or IPv6 address
 ``int``         integer, string          integer         String must be valid 32 bit integer
 ``list``        list, string             list            Uses JSON's native list representation
 ``map``         map, string              map             Uses JSON's native map representation
 ``smallint``    integer, string          integer         String must be valid 16 bit integer
 ``set``         list, string             list            Uses JSON's native list representation
 ``text``        string                   string          Uses JSON's ``\u`` character escape
 ``time``        string                   string          Time of day in format ``HH-MM-SS[.fffffffff]``
 ``timestamp``   integer, string          string          A timestamp. Strings constant allows to input :ref:`timestamps
                                                          as dates <timestamps>`. Datestamps with format ``YYYY-MM-DD
                                                          HH:MM:SS.SSS`` are returned.
 ``timeuuid``    string                   string          Type 1 UUID. See :token:`constant` for the UUID format
 ``tinyint``     integer, string          integer         String must be valid 8 bit integer
 ``tuple``       list, string             list            Uses JSON's native list representation
 ``UDT``         map, string              map             Uses JSON's native map representation with field names as keys
 ``uuid``        string                   string          See :token:`constant` for the UUID format
 ``varchar``     string                   string          Uses JSON's ``\u`` character escape
 ``varint``      integer, string          integer         Variable length; may overflow 32 or 64 bit integers in
                                                          client-side decoder
=============== ======================== =============== ==============================================================

The fromJson() Function
^^^^^^^^^^^^^^^^^^^^^^^

The ``fromJson()`` function may be used similarly to ``INSERT JSON``, but for a single column value. It may only be used
in the ``VALUES`` clause of an ``INSERT`` statement or as one of the column values in an ``UPDATE``, ``DELETE``, or
``SELECT`` statement. For example, it cannot be used in the selection clause of a ``SELECT`` statement.

The toJson() Function
^^^^^^^^^^^^^^^^^^^^^

The ``toJson()`` function may be used similarly to ``SELECT JSON``, but for a single column value. It may only be used
in the selection clause of a ``SELECT`` statement.


* :doc:`Apache Cassandra Query Language (CQL) Reference </cql/index>`

.. include:: /rst_include/apache-copyrights.rst
