

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

.. _cql-functions:

.. Need some intro for UDF and native functions in general and point those to it.
.. _native-functions:

Functions
---------

CQL supports two main categories of functions:

- The :ref:`scalar functions <scalar-functions>`, which simply take a number of values and produce an output with it.
- The :ref:`aggregate functions <aggregate-functions>`, which are used to aggregate multiple rows of results from a
  ``SELECT`` statement.

In both cases, CQL provides a number of native "hard-coded" functions as well as the ability to create new user-defined
functions.

.. note:: Although user-defined functions are sandboxed, protecting the system from a "rogue" function, user-defined functions are disabled by default for extra security.
   See the ``enable_user_defined_functions`` in ``scylla.yaml`` to enable them.

   Additionally, user-defined functions are still experimental and need to be explicitly enabled by adding ``udf`` to the list of
   ``experimental_features`` configuration options in ``scylla.yaml``, or turning on the ``experimental`` flag.
   See :ref:`Enabling Experimental Features <yaml_enabling_experimental_features>` for details.

.. A function is identifier by its name:

.. .. code-block::
   
   function_name: [ `keyspace_name` '.' ] `name`

.. _scalar-functions:

Scalar functions
^^^^^^^^^^^^^^^^

.. _scalar-native-functions:

Native functions
~~~~~~~~~~~~~~~~

Cast
````

Supported starting from ScyllaDB version 2.1 

The ``cast`` function can be used to convert one native datatype to another.

The following table describes the conversions supported by the ``cast`` function. ScyllaDB will silently ignore any cast converting a cast datatype into its own datatype.

=============== =======================================================================================================
 From            To
=============== =======================================================================================================
 ``ascii``       ``text``, ``varchar``
 ``bigint``      ``tinyint``, ``smallint``, ``int``, ``float``, ``double``, ``decimal``, ``varint``, ``text``,
                 ``varchar``
 ``boolean``     ``text``, ``varchar``
 ``counter``     ``tinyint``, ``smallint``, ``int``, ``bigint``, ``float``, ``double``, ``decimal``, ``varint``,
                 ``text``, ``varchar``
 ``date``        ``timestamp``
 ``decimal``     ``tinyint``, ``smallint``, ``int``, ``bigint``, ``float``, ``double``, ``varint``, ``text``,
                 ``varchar``
 ``double``      ``tinyint``, ``smallint``, ``int``, ``bigint``, ``float``, ``decimal``, ``varint``, ``text``,
                 ``varchar``
 ``float``       ``tinyint``, ``smallint``, ``int``, ``bigint``, ``double``, ``decimal``, ``varint``, ``text``,
                 ``varchar``
 ``inet``        ``text``, ``varchar``
 ``int``         ``tinyint``, ``smallint``, ``bigint``, ``float``, ``double``, ``decimal``, ``varint``, ``text``,
                 ``varchar``
 ``smallint``    ``tinyint``, ``int``, ``bigint``, ``float``, ``double``, ``decimal``, ``varint``, ``text``,
                 ``varchar``
 ``time``        ``text``, ``varchar``
 ``timestamp``   ``date``, ``text``, ``varchar``
 ``timeuuid``    ``timestamp``, ``date``, ``text``, ``varchar``
 ``tinyint``     ``tinyint``, ``smallint``, ``int``, ``bigint``, ``float``, ``double``, ``decimal``, ``varint``,
                 ``text``, ``varchar``
 ``uuid``        ``text``, ``varchar``
 ``varint``      ``tinyint``, ``smallint``, ``int``, ``bigint``, ``float``, ``double``, ``decimal``, ``text``,
                 ``varchar``
=============== =======================================================================================================

The conversions rely strictly on Java's semantics. For example, the double value 1 will be converted to the text value
'1.0'. For instance::

    SELECT avg(cast(count as double)) FROM myTable

Token
`````

The ``token`` function computes a token for a given partition key. The exact signature of the token function
depends on the table concerned and on the partitioner used by the cluster.

The arguments of the ``token`` depend on the type of the partition key columns that are used. The return type depends on
the partitioner in use:

- For Murmur3Partitioner, the return type is ``bigint``.

For instance, in a cluster using the default Murmur3Partitioner, if a table is defined by::

    CREATE TABLE users (
        userid text PRIMARY KEY,
        username text,
    )

The ``token`` function accepts single argument of type ``text`` (in that case, the partition key is ``userid``
(there are no clustering columns, so the partition key is the same as the primary key)), and the return type will be
``bigint``.

Uuid
````
The ``uuid`` function takes no parameters and generates a random type 4 uuid suitable for use in ``INSERT`` or
``UPDATE`` statements.

.. _timeuuid-functions:

.. Timeuuid functions
.. ``````````````````

.. ``now``
.. #######

.. The ``now`` function takes no arguments and generates, on the coordinator node, a new unique timeuuid at the 
.. time the function is invoked. Note that this method is useful for insertion but is largely non-sensical in
.. ``WHERE`` clauses. For instance, a query of the form::

..    SELECT * FROM myTable WHERE t = now()

.. will never return any result by design, since the value returned by ``now()`` is guaranteed to be unique.

.. ``currentTimeUUID`` is an alias of ``now``.

.. ``minTimeuuid`` and ``maxTimeuuid``
.. ###################################

.. The ``minTimeuuid`` (resp. ``maxTimeuuid``) function takes a ``timestamp`` value ``t`` (which can be `either a timestamp
.. or a date string <timestamps>`) and return a *fake* ``timeuuid`` corresponding to the *smallest* (resp. *biggest*)
.. possible ``timeuuid`` having for timestamp ``t``. So for instance::

..     SELECT * FROM myTable
..      WHERE t > maxTimeuuid('2013-01-01 00:05+0000')
..        AND t < minTimeuuid('2013-02-02 10:00+0000')

.. will select all rows where the ``timeuuid`` column ``t`` is strictly older than ``'2013-01-01 00:05+0000'`` but strictly
.. younger than ``'2013-02-02 10:00+0000'``. Please note that ``t >= maxTimeuuid('2013-01-01 00:05+0000')`` would still
.. *not* select a ``timeuuid`` generated exactly at '2013-01-01 00:05+0000' and is essentially equivalent to ``t >
.. maxTimeuuid('2013-01-01 00:05+0000')``.

.. note:: We called the values generated by ``minTimeuuid`` and ``maxTimeuuid`` *fake* UUID because they do no respect
   the Time-Based UUID generation process specified by the `RFC 4122 <http://www.ietf.org/rfc/rfc4122.txt>`__. In
   particular, the value returned by these two methods will not be unique. This means you should only use those methods
   for querying (as in the example above). Inserting the result of those methods is almost certainly *a bad idea*.

Datetime functions
``````````````````

Retrieving the current date/time
################################

The following functions can be used to retrieve the date/time at the time where the function is invoked:

===================== ===============
Function name         Output type
===================== ===============
``currentTimestamp``  ``timestamp``
``currentDate``       ``date``
``currentTime``       ``time``
``currentTimeUUID``   ``timeUUID``
===================== ===============

For example, to retrieve data up to today, run the following query::

   SELECT * FROM myTable WHERE date >= currentDate()

Time conversion functions
#########################

A number of functions are provided to “convert” a ``timeuuid``, a ``timestamp``, or a ``date`` into another ``native``
type.

===================== =============== ===================================================================
Function name         Input type      Description
===================== =============== ===================================================================
``toDate``            ``timeuuid``    Converts the ``timeuuid`` argument into a ``date`` type
``toDate``            ``timestamp``   Converts the ``timestamp`` argument into a ``date`` type
``toTimestamp``       ``timeuuid``    Converts the ``timeuuid`` argument into a ``timestamp`` type
``toTimestamp``       ``date``        Converts the ``date`` argument into a ``timestamp`` type
``toUnixTimestamp``   ``timeuuid``    Converts the ``timeuuid`` argument into a ``bigInt`` raw value
``toUnixTimestamp``   ``timestamp``   Converts the ``timestamp`` argument into a ``bigInt`` raw value
``toUnixTimestamp``   ``date``        Converts the ``date`` argument into a ``bigInt`` raw value
``dateOf``            ``timeuuid``    Similar to ``toTimestamp(timeuuid)`` (DEPRECATED)
``unixTimestampOf``   ``timeuuid``    Similar to ``toUnixTimestamp(timeuuid)`` (DEPRECATED)
===================== =============== ===================================================================

.. Blob conversion functions
.. `````````````````````````
.. A number of functions are provided to “convert” the native types into binary data (``blob``). For every
.. ``<native-type>`` ``type`` supported by CQL (a notable exceptions is ``blob``, for obvious reasons), the function
.. ``typeAsBlob`` takes a argument of type ``type`` and return it as a ``blob``. Conversely, the function ``blobAsType``
.. takes a 64-bit ``blob`` argument and convert it to a ``bigint`` value. And so for instance, ``bigintAsBlob(3)`` is
.. ``0x0000000000000003`` and ``blobAsBigint(0x0000000000000003)`` is ``3``.


Blob conversion functions
`````````````````````````
A number of functions are provided to “convert” the native types into binary data (``blob``). For every
``<native-type>`` ``type`` supported by CQL (a notable exception is a ``blob``, for obvious reasons), the function
``typeAsBlob`` takes an argument of type ``type`` and returns it as a ``blob``. Conversely, the function ``blobAsType``
takes a 64-bit ``blob`` argument and converts it to a ``bigint`` value. For example, ``bigintAsBlob(3)`` is
``0x0000000000000003`` and ``blobAsBigint(0x0000000000000003)`` is ``3``.

.. _udfs:

User-defined functions :label-caution:`Experimental`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

User-defined functions (UDFs) execute user-provided code in ScyllaDB. Supported languages are currently Lua and WebAssembly.

UDFs are part of the ScyllaDB schema and are automatically propagated to all nodes in the cluster.
UDFs can be overloaded, so that multiple UDFs with different argument types can have the same function name, for example::

   CREATE FUNCTION sample ( arg int ) ...;
   CREATE FUNCTION sample ( arg text ) ...;

When calling a user-defined function, arguments can be literals or terms. Prepared statement placeholders can be used, too.

CREATE FUNCTION statement
`````````````````````````

Creating a new user-defined function uses the ``CREATE FUNCTION`` statement. For example::

    CREATE OR REPLACE FUNCTION div(dividend double, divisor double)
      RETURNS NULL ON NULL INPUT
      RETURNS double
      LANGUAGE LUA
      AS 'return dividend/divisor;';

``CREATE FUNCTION`` with the optional ``OR REPLACE`` keywords creates either a function
or replaces an existing one with the same signature. A ``CREATE FUNCTION`` without ``OR REPLACE``
fails if a function with the same signature already exists. If the optional ``IF NOT EXISTS``
keywords are used, the function will only be created only if another function with the same
signature does not exist. ``OR REPLACE`` and ``IF NOT EXISTS`` cannot be used together.

Behavior for null input values must be defined for each function:

* ``RETURNS NULL ON NULL INPUT`` declares that the function will always return null (without being executed) if any of the input arguments is null.
* ``CALLED ON NULL INPUT`` declares that the function will always be executed.

The ``LANGUAGE`` clause specifies the language of the function, and the ``AS`` clause defines the function body:

* For Lua functions, the ``LANGUAGE`` is ``lua`` and the body is a string literal containing the Lua script.

* For Wasm functions, the ``LANGUAGE`` is ``wasm`` and the body is a string literal containing a WebAssembly module in WebAssembly text format, which exports a function with the same name as specified in the ``CREATE FUNCTION`` statement. More details on generating the Wasm modules can be found :doc:`here </cql/wasm>`.

If the function code contains single quotes, they must be escaped by doubling them, for example::

   CREATE FUNCTION hworld () RETURNS text LANGUAGE LUA AS 'return ''hello world'';';

Function Signature
``````````````````

Signatures are used to distinguish individual functions. The signature consists of a fully-qualified function name of the <keyspace>.<function_name> and a concatenated list of all the argument types.

Note that keyspace names, function names and argument types are subject to the default naming conventions and case-sensitivity rules.

Functions belong to a keyspace; if no keyspace is specified, the current keyspace is used. User-defined functions are not allowed in the system keyspaces.

DROP FUNCTION statement
```````````````````````

Dropping a function uses the ``DROP FUNCTION`` statement. For example::

   DROP FUNCTION myfunction;
   DROP FUNCTION mykeyspace.afunction;
   DROP FUNCTION afunction ( int );
   DROP FUNCTION afunction ( text );

You must specify the argument types of the function, the arguments_signature, in the drop command if there are multiple overloaded functions with the same name but different signatures.
``DROP FUNCTION`` with the optional ``IF EXISTS`` keywords drops a function if it exists, but does not throw an error if it doesn’t.

.. _aggregate-functions:

Aggregate functions
^^^^^^^^^^^^^^^^^^^

Aggregate functions work on a set of rows. They receive values for each row and return one value for the whole set.

If ``normal`` columns, ``scalar functions``, ``UDT`` fields, ``writetime``, or ``ttl`` are selected together with
aggregate functions, the values returned for them will be the ones of the first row matching the query.

.. note::
    The ``LIMIT`` and ``PER PARTITION LIMIT`` used in the ``SELECT`` query will have no effect if the limit is greater 
    than or equal to 1 because they are applied to the output of the aggregate functions (which return one value for 
    the whole set of rows).


Native aggregates
~~~~~~~~~~~~~~~~~

.. _count-function:

Count
`````

The ``count`` function can be used to count the rows returned by a query. Example::

    SELECT COUNT (*) FROM plays;
    SELECT COUNT (1) FROM plays;

It also can be used to count the non-null value of a given column::

    SELECT COUNT (scores) FROM plays;

.. note::
    Counting all rows in a table may be time-consuming and exceed the default timeout. In such a case, 
    see :doc:`Counting all rows in a table is slow </kb/count-all-rows>` for instructions.

Max and Min
```````````

The ``max`` and ``min`` functions can be used to compute the maximum and the minimum value returned by a query for a
given column. For instance::

    SELECT MIN (players), MAX (players) FROM plays WHERE game = 'quake';

Sum
```

The ``sum`` function can be used, to sum up all the values returned by a query for a given column. For instance::

    SELECT SUM (players) FROM plays;

Avg
```

The ``avg`` function can be used to compute the average of all the values returned by a query for a given column. For
instance::

    SELECT AVG (players) FROM plays;

.. _user-defined-aggregates-functions:

User-defined aggregates (UDAs) :label-caution:`Experimental`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

User-defined aggregates allow the creation of custom aggregate functions. User-defined aggregates can be used in SELECT statement.

Each aggregate requires an initial state of type ``STYPE`` defined with the ``INITCOND`` value (default value: ``null``). The first argument of the state function must have type STYPE. The remaining arguments of the state function must match the types of the user-defined aggregate arguments. The state function is called once for each row, and the value returned by the state function becomes the new state. After all rows are processed, the optional FINALFUNC is executed with the last state value as its argument.

The ``STYPE`` value is mandatory in order to distinguish possibly overloaded versions of the state and/or final function, since the overload can appear after creation of the aggregate.

A complete working example for user-defined aggregates (assuming that a keyspace has been selected using the ``USE`` statement)::

   CREATE FUNCTION accumulate_len(acc tuple<bigint,bigint>, a text)
   	  RETURNS NULL ON NULL INPUT
   	  RETURNS tuple<bigint,bigint>
   	  LANGUAGE lua as 'return {acc[1] + 1, acc[2] + #a}';

   CREATE OR REPLACE FUNCTION present(res tuple<bigint,bigint>)
   	  RETURNS NULL ON NULL INPUT
   	  RETURNS text
   	  LANGUAGE lua as
   	    'return "The average string length is " .. res[2]/res[1] .. "!"';

   CREATE OR REPLACE AGGREGATE avg_length(text)
      SFUNC accumulate_len
      STYPE tuple<bigint,bigint>
      FINALFUNC present
      INITCOND (0,0);

CREATE AGGREGATE statement
``````````````````````````

The ``CREATE AGGREGATE`` command with the optional ``OR REPLACE`` keywords creates either an aggregate or replaces an existing one with the same signature. A ``CREATE AGGREGATE`` without ``OR REPLACE`` fails if an aggregate with the same signature already exists. The ``CREATE AGGREGATE`` command with the optional ``IF NOT EXISTS`` keywords creates an aggregate if it does not already exist. The ``OR REPLACE`` and ``IF NOT EXISTS`` phrases cannot be used together.

The ``STYPE`` value defines the type of the state value and must be specified. The optional ``INITCOND`` defines the initial state value for the aggregate; the default value is null. A non-null ``INITCOND`` must be specified for state functions that are declared with ``RETURNS NULL ON NULL INPUT``.

The ``SFUNC`` value references an existing function to use as the state-modifying function. The first argument of the state function must have type ``STYPE``. The remaining arguments of the state function must match the types of the user-defined aggregate arguments. The state function is called once for each row, and the value returned by the state function becomes the new state. State is not updated for state functions declared with ``RETURNS NULL ON NULL INPUT`` and called with null. After all rows are processed, the optional ``FINALFUNC`` is executed with last state value as its argument. It must take only one argument with type ``STYPE``, but the return type of the ``FINALFUNC`` may be a different type. A final function declared with ``RETURNS NULL ON NULL INPUT`` means that the aggregate’s return value will be null, if the last state is null.

If no ``FINALFUNC`` is defined, the overall return type of the aggregate function is ``STYPE``. If a ``FINALFUNC`` is defined, it is the return type of that function.

DROP AGGREGATE statement
````````````````````````

Dropping an user-defined aggregate function uses the DROP AGGREGATE statement. For example::

   DROP AGGREGATE myAggregate;
   DROP AGGREGATE myKeyspace.anAggregate;
   DROP AGGREGATE someAggregate ( int );
   DROP AGGREGATE someAggregate ( text );

The ``DROP AGGREGATE`` statement removes an aggregate created using ``CREATE AGGREGATE``. You must specify the argument types of the aggregate to drop if there are multiple overloaded aggregates with the same name but a different signature.

The ``DROP AGGREGATE`` command with the optional ``IF EXISTS`` keywords drops an aggregate if it exists, and does nothing if a function with the signature does not exist.

.. include:: /rst_include/apache-cql-return-index.rst 

.. include:: /rst_include/apache-copyrights.rst