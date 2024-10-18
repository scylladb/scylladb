Data Types
----------
.. _data-types:


.. highlight:: cql

.. _UUID: https://en.wikipedia.org/wiki/Universally_unique_identifier


CQL is a typed language and supports a rich set of data types, including :ref:`native types <native-types>` and
:ref:`collection types <collections>`.

.. code-block::
   
   cql_type: `native_type` | `collection_type` | `user_defined_type` | `tuple_type` 



Working with Bytes
^^^^^^^^^^^^^^^^^^

Bytes can be input with:

* a hexadecimal literal.
* one of the ``typeAsBlob()`` cql functions. There is such a function for each native type.

For example:

.. code-block:: cql


   INSERT INTO blobstore (id, data) VALUES (4375645, 0xabf7971528cfae76e00000008bacdf);
   INSERT INTO blobstore (id, data) VALUES (4375645, intAsBlob(33));
   
.. _native-types:

Native Types
^^^^^^^^^^^^

The native types supported by CQL are:

.. code-block:: cql

   native_type: ASCII
              : | BIGINT
              : | BLOB
              : | BOOLEAN
              : | COUNTER
              : | DATE
              : | DECIMAL
              : | DOUBLE
              : | DURATION
              : | FLOAT
              : | INET
              : | INT
              : | SMALLINT
              : | TEXT
              : | TIME
              : | TIMESTAMP
              : | TIMEUUID
              : | TINYINT
              : | UUID
              : | VARCHAR
              : | VARINT

The following table gives additional information on the native data types and which kind of :ref:`constants
<constants>` each type supports:

======================= ===================== ========================================================================================================
 type                    constants supported   description
======================= ===================== ========================================================================================================
 ``ascii``               :token:`string`       ASCII character string
 ``bigint``              :token:`integer`      64-bit signed long
 ``blob``                :token:`blob`         Arbitrary bytes (no validation). See `Working with Bytes`_ for details
 ``boolean``             :token:`boolean`      Either ``true`` or ``false``
 ``counter``             :token:`integer`      Counter column (64-bit signed value). See :ref:`counters` for details
 ``date``                :token:`integer`,     A date (with no corresponding time value). See :ref:`dates` below for details
                         :token:`string`
 ``decimal``             :token:`integer`,     Variable-precision decimal
                         :token:`float`
 ``double``              :token:`integer`      64-bit IEEE-754 floating point
                         :token:`float`
 ``duration``            :token:`duration`,    A duration with nanosecond precision. See :ref:`durations` below for details
  ``float``              :token:`integer`,     32-bit IEEE-754 floating point
                         :token:`float`
 ``inet``                :token:`string`       An IP address, either IPv4 (4 bytes long) or IPv6 (16 bytes long). Note that
                                               there is no ``inet`` constant, IP address should be input as strings
 ``int``                 :token:`integer`      32-bit signed int
 ``smallint``            :token:`integer`      16-bit signed int
 ``text | varchar``      :token:`string`       UTF8 encoded string
 ``time``                :token:`integer`,     A time (with no corresponding date value) with nanosecond precision. See
                         :token:`string`       :ref:`times` below for details
 ``timestamp``           :token:`integer`,     A timestamp (date and time) with millisecond precision. See :ref:`timestamps`
                         :token:`string`       below for details
 ``timeuuid``            :token:`uuid`         Version 1 UUID_, generally used as a “conflict-free” timestamp. See `Working with UUIDs`_ for details
 ``tinyint``             :token:`integer`      8-bit signed int
 ``uuid``                :token:`uuid`         A UUID_ (of any version). See `Working with UUIDs`_ for details
 ``varint``              :token:`integer`      Arbitrary-precision integer
======================= ===================== ========================================================================================================

.. _counters:

Counters
~~~~~~~~

The ``counter`` type is used to define *counter columns*. A counter column is a column with a 64-bit signed
integer value and on which two operations are supported: incrementing and decrementing (see the :ref:`UPDATE statement
<update-statement>` for syntax). Note that the value of a counter cannot be set: a counter does not exist until first
incremented/decremented, and that first increment/decrement is made as if the prior value was 0.

.. _counter-limitations:

Counters have a number of important limitations:

- They cannot be used for columns part of the ``PRIMARY KEY`` of a table.
- A table that contains a counter can only contain counters. In other words, either all the columns of a table outside
  the ``PRIMARY KEY`` have the ``counter`` type, or none of them have it.
- Counters do not support expiring data with :doc:`Time to Live (TTL) </cql/time-to-live>`.
- The deletion of counters is supported but is only guaranteed to work the first time you delete a counter. In other
  words, you should not re-update a counter that you have deleted (if you do, proper behavior is not guaranteed).
- Counter updates are, by nature, **not** `idempotent <https://en.wikipedia.org/wiki/Idempotence>`__. An important
  consequence is that if a counter update fails unexpectedly (timeout or loss of connection to the coordinator node),
  the client has no way to know if the update has been applied or not. In particular, replaying the update may or may
  not lead to an overcount.

.. _timestamps:

Working with timestamps
^^^^^^^^^^^^^^^^^^^^^^^

Values of the ``timestamp`` type are encoded as 64-bit signed integers representing a number of milliseconds since the
standard base time known as `the epoch <https://en.wikipedia.org/wiki/Unix_time>`__: January 1st 1970 at 00:00:00 GMT.

Timestamps can be input in CQL either using their value as an :token:`integer`, or using a :token:`string` that
represents an `ISO 8601 <https://en.wikipedia.org/wiki/ISO_8601>`__ date. For instance, all of the values below are
valid ``timestamp`` values for  Mar 2, 2011, at 04:05:00 AM, GMT:

- ``1299038700000``
- ``'2011-02-03 04:05+0000'``
- ``'2011-02-03 04:05:00+0000'``
- ``'2011-02-03 04:05:00.000+0000'``
- ``'2011-02-03T04:05+0000'``
- ``'2011-02-03T04:05:00+0000'``
- ``'2011-02-03T04:05:00.000+0000'``

The ``+0000`` above is an RFC 822 4-digit time zone specification; ``+0000`` refers to GMT. US Pacific Standard Time is
``-0800``. The time zone may be omitted if desired (``'2011-02-03 04:05:00'``), and if so, the date will be interpreted
as being in the time zone under which the coordinating ScyllaDB node is configured. However, there are difficulties
inherent in relying on the time zone configuration as expected, so it is recommended that the time zone always be
specified for timestamps when feasible.

The time of day may also be omitted (``'2011-02-03'`` or ``'2011-02-03+0000'``), in which case the time of day will
default to 00:00:00 in the specified or default time zone. However, if only the date part is relevant, consider using
the :ref:`date <dates>` type.

.. _dates:

Working with dates
^^^^^^^^^^^^^^^^^^

Values of the ``date`` type are encoded as 32-bit unsigned integers representing a number of days with “the epoch” at
the center of the range (2^31). Epoch is January 1st, 1970.

As for :ref:`timestamp <timestamps>`, a date can be input either as an :token:`integer` or using a date
:token:`string`. In the latter case, the format should be ``yyyy-mm-dd`` (so ``'2011-02-03'``, for instance).

.. _times:

Working with times
^^^^^^^^^^^^^^^^^^

Values of the ``time`` type are encoded as 64-bit signed integers representing the number of nanoseconds since midnight.

As for :ref:`timestamp <timestamps>`, time can be input either as an :token:`integer` or using a :token:`string`
representing the time. In the latter case, the format should be ``hh:mm:ss[.fffffffff]`` (where the sub-second precision
is optional and if provided, can be less than the nanosecond). So, for instance, the following are valid inputs for a
time:

-  ``'08:12:54'``
-  ``'08:12:54.123'``
-  ``'08:12:54.123456'``
-  ``'08:12:54.123456789'``

.. _durations:

Working with durations
^^^^^^^^^^^^^^^^^^^^^^

Values of the ``duration`` type are encoded as three signed integers of variable lengths. The first integer represents the
number of months, the second the number of days, and the third the number of nanoseconds. This is due to the fact that
the number of days in a month can change, and a day can have 23 or 25 hours depending on the daylight saving.
Internally, the number of months and days is decoded as 32 bits integers, whereas the number of nanoseconds is decoded
as a 64 bits integer.

A duration can be input as:

#. ``(quantity unit)+`` like ``12h30m`` where the unit can be:

   * ``y``: years (12 months)
   * ``mo``: months (1 month)
   * ``w``: weeks (7 days)
   * ``d``: days (1 day)
   * ``h``: hours (3,600,000,000,000 nanoseconds)
   * ``m``: minutes (60,000,000,000 nanoseconds)
   * ``s``: seconds (1,000,000,000 nanoseconds)
   * ``ms``: milliseconds (1,000,000 nanoseconds)
   * ``us`` or ``µs`` : microseconds (1000 nanoseconds)
   * ``ns``: nanoseconds (1 nanosecond)
#. ISO 8601 format:  ``P[n]Y[n]M[n]DT[n]H[n]M[n]S or P[n]W``
#. ISO 8601 alternative format: ``P[YYYY]-[MM]-[DD]T[hh]:[mm]:[ss]``

For example::

 INSERT INTO RiderResults (rider, race, result) VALUES ('Christopher Froome', 'Tour de France', 89h4m48s);
 INSERT INTO RiderResults (rider, race, result) VALUES ('BARDET Romain', 'Tour de France', PT89H8M53S);
 INSERT INTO RiderResults (rider, race, result) VALUES ('QUINTANA Nairo', 'Tour de France', P0000-00-00T89:09:09);

.. _duration-limitation:

Duration columns cannot be used in a table's ``PRIMARY KEY``. This limitation is due to the fact that
durations cannot be ordered. It is effectively not possible to know if ``1mo`` is greater than ``29d`` without a date
context.

A ``1d`` duration does not equal to a ``24h`` one as the duration type has been created to be able to support daylight
saving.

Working with UUIDs
^^^^^^^^^^^^^^^^^^

The values of the UUID type are encoded as 32 hex (base 16) digits,
in five groups separated by hyphens (-), in the form 8-4-4-4-12 for a total of 36 characters (32 hexadecimal characters and 4 hyphens).
 
Use an integer literal and not a string. For example ``123e4567-e89b-12d3-a456-426655440000`` and not ``'123e4567-e89b-12d3-a456-426655440000'``.


.. _collections:

Collections
^^^^^^^^^^^

CQL supports 3 kinds of collections: :ref:`maps`, :ref:`sets` and :ref:`lists`. The types of those collections is defined
by:

.. code-block:: cql

   collection_type: MAP '<' `cql_type` ',' `cql_type` '>'
                  : | SET '<' `cql_type` '>'
                  : | LIST '<' `cql_type` '>'

and their values can be input using collection literals:

.. code-block:: cql

   collection_literal: `map_literal` | `set_literal` | `list_literal`

   map_literal: '{' [ `term` ':' `term` (',' `term` : `term`)* ] '}'

   set_literal: '{' [ `term` (',' `term`)* ] '}'

   list_literal: '[' [ `term` (',' `term`)* ] ']'

Note that neither :token:`bind_marker` nor ``NULL`` are supported inside collection literals.

Noteworthy characteristics
~~~~~~~~~~~~~~~~~~~~~~~~~~

Collections are meant for storing/denormalizing a relatively small amount of data. They work well for things like “the
phone numbers of a given user”, “labels applied to an email”, etc. But when items are expected to grow unbounded (“all
messages sent by a user”, “events registered by a sensor”...), then collections are not appropriate, and a specific table
(with clustering columns) should be used. A collection can be **frozen** or **non-frozen**.

A non-frozen collection can be modified, i.e., have an element added or removed. 
A frozen collection is *immutable*, and can only be updated as a whole.
Only frozen collections can be used as primary keys or nested collections.

By default, a collection is non-frozen. 
To declare a frozen collection, use ``FROZEN`` keyword:

.. code-block:: cql

   frozen_collection_type: FROZEN '<' MAP '<' `cql_type` ',' `cql_type` '>' '>'
                          : | FROZEN '<' SET '<' `cql_type` '>' '>'
                          : | FROZEN '<' LIST '<' `cql_type` '>' '>'

Collections, frozen and non-frozen, have the following noteworthy characteristics and limitations:

- Individual collections are not indexed internally. This means that even to access a single element of a collection,
  the whole collection has to be read (and reading one is not paged internally).
- While insertion operations on sets and maps never incur a read-before-write internally, some operations on lists do.
  Further, some list operations are not idempotent by nature (see the section on :ref:`lists <lists>` below for
  details), making their retry in case of timeout problematic. It is thus advised to prefer sets over lists when
  possible.

Please note that while some of those limitations may or may not be removed/improved upon in the future, it is an
anti-pattern to use a (single) collection to store large amounts of data.

.. _maps:

Maps
~~~~

A ``map`` is a (sorted) set of key-value pairs, where keys are unique, and the map is sorted by its keys. You can define a map column with::

    CREATE TABLE users (
        id text PRIMARY KEY,
        name text,
        favs map<text, text> // A map of text keys, and text values
    );

A map column can be assigned new contents with either ``INSERT`` or ``UPDATE``,
as in the following examples. In both cases, the new contents replace the map's
old content, if any::

    INSERT INTO users (id, name, favs)
               VALUES ('jsmith', 'John Smith', { 'fruit' : 'Apple', 'band' : 'Beatles' });

    UPDATE users SET favs = { 'fruit' : 'Banana' } WHERE id = 'jsmith';

Note that ScyllaDB does not distinguish an empty map from a missing value,
thus assigning an empty map (``{}``) to a map is the same as deleting it.

Further, maps support:

- Updating or inserting one or more elements::

    UPDATE users SET favs['author'] = 'Ed Poe' WHERE id = 'jsmith';
    UPDATE users SET favs = favs + { 'movie' : 'Cassablanca', 'band' : 'ZZ Top' } WHERE id = 'jsmith';

- Removing one or more element (if an element doesn't exist, removing it is a no-op but no error is thrown)::

    DELETE favs['author'] FROM users WHERE id = 'jsmith';
    UPDATE users SET favs = favs - { 'movie', 'band'} WHERE id = 'jsmith';

  Note that for removing multiple elements in a ``map``, you remove from it a ``set`` of keys.

Lastly, TTLs are allowed for both ``INSERT`` and ``UPDATE``, but in both cases, the TTL set only applies to the newly
inserted/updated elements. In other words::

    UPDATE users USING TTL 10 SET favs['color'] = 'green' WHERE id = 'jsmith';

will only apply the TTL to the ``{ 'color' : 'green' }`` record, the rest of the map remaining unaffected.

.. _sets:

Sets
~~~~

A ``set`` is a (sorted) collection of unique values. You can define a set column with::

    CREATE TABLE images (
        name text PRIMARY KEY,
        owner text,
        tags set<text> // A set of text values
    );

A set column can be assigned new contents with either ``INSERT`` or ``UPDATE``,
as in the following examples. In both cases, the new contents replace the set's
old content, if any::

    INSERT INTO images (name, owner, tags)
                VALUES ('cat.jpg', 'jsmith', { 'pet', 'cute' });

    UPDATE images SET tags = { 'kitten', 'cat', 'lol' } WHERE name = 'cat.jpg';

Note that ScyllaDB does not distinguish an empty set from a missing value,
thus assigning an empty set (``{}``) to a set is the same as deleting it.

Further, sets support:

- Adding one or multiple elements (as this is a set, inserting an already existing element is a no-op)::

    UPDATE images SET tags = tags + { 'gray', 'cuddly' } WHERE name = 'cat.jpg';

- Removing one or multiple elements (if an element doesn't exist, removing it is a no-op but no error is thrown)::

    UPDATE images SET tags = tags - { 'cat' } WHERE name = 'cat.jpg';

Lastly, as for :ref:`maps <maps>`, TTLs, if used, only apply to the newly inserted values.

.. _lists:

Lists
~~~~~

.. note:: As mentioned above and further discussed at the end of this section, lists have limitations and specific
   performance considerations that you should take into account before using them. In general, if you can use a
   :ref:`set <sets>` instead of a list, always prefer a set.


A ``list`` is an ordered list of values (not necessarily unique). You can define a list column with::

    CREATE TABLE plays (
        id text PRIMARY KEY,
        game text,
        players int,
        scores list<int> // A list of integers
    );

A list column can be assigned new contents with either ``INSERT`` or ``UPDATE``,
as in the following examples. In both cases, the new contents replace the list's
old content, if any::

    INSERT INTO plays (id, game, players, scores)
               VALUES ('123-afde', 'quake', 3, [17, 4, 2]);

    UPDATE plays SET scores = [ 3, 9, 4] WHERE id = '123-afde';

Note that ScyllaDB does not distinguish an empty list from a missing value,
thus assigning an empty list (``[]``) to a list is the same as deleting it.

Further, lists support:

- Appending and prepending values to a list::

    UPDATE plays SET players = 5, scores = scores + [ 14, 21 ] WHERE id = '123-afde';
    UPDATE plays SET players = 6, scores = [ 3 ] + scores WHERE id = '123-afde';

- Setting the value at a particular position in the list. This implies that the list has a pre-existing element for that
  position or an error will be thrown that the list is too small::

    UPDATE plays SET scores[1] = 7 WHERE id = '123-afde';

- Removing an element by its position in the list. This implies that the list has a pre-existing element for that position,
  or an error will be thrown that the list is too small. Further, as the operation removes an element from the list, the
  list size will be diminished by 1, shifting the position of all the elements following the one deleted::

    DELETE scores[1] FROM plays WHERE id = '123-afde';

- Deleting *all* the occurrences of particular values in the list (if a particular element doesn't occur at all in the
  list, it is simply ignored, and no error is thrown)::

    UPDATE plays SET scores = scores - [ 12, 21 ] WHERE id = '123-afde';

.. warning:: The append and prepend operations are not idempotent by nature. So, in particular, if one of these operation
   timeouts, then retrying the operation is not safe, and it may (or may not) lead to appending/prepending the value
   twice.

.. warning:: Setting and removing an element by position and removing occurrences of particular values incur an internal
   *read-before-write*. They will thus run more slowly and take more resources than usual updates (with the exclusion
   of conditional write that have their own cost).

Lastly, as for :ref:`maps <maps>`, TTLs, when used, only apply to the newly inserted values.

.. _udts:

User-Defined Types
^^^^^^^^^^^^^^^^^^

CQL support the definition of user-defined types (UDT for short). Such a type can be created, modified and removed using
the :token:`create_type_statement`, :token:`alter_type_statement` and :token:`drop_type_statement` described below. But
once created, a UDT is simply referred to by its name:

.. code-block::
   
   user_defined_type: `udt_name`
   udt_name: [ `keyspace_name` '.' ] `identifier`


Creating a UDT
~~~~~~~~~~~~~~

Creating a new user-defined type is done using a ``CREATE TYPE`` statement defined by:

.. code-block::
   
   create_type_statement: CREATE TYPE [ IF NOT EXISTS ] `udt_name`
                        :     '(' `field_definition` ( ',' `field_definition` )* ')'
   field_definition: `identifier` `cql_type`

A UDT has a name (``udt_name``), which is used to declare columns of that type and is a set of named and typed fields. The ``udt_name`` can be any
type, including collections or other UDTs. UDTs and collections inside collections must always be frozen (no matter which version of ScyllaDB you are using). 

For example::

  CREATE TYPE full_name (
    first text,
    last text
  );


  CREATE TYPE phone (
    country_code int,
    number text,
  );

  CREATE TYPE address (
    street text,
    city text,
    zip text,
    phones map<text, frozen<phone>>
  );


  CREATE TABLE superheroes (
       name frozen<full_name> PRIMARY KEY,
       home frozen<address>
  );

.. note::

   - Attempting to create an already existing type will result in an error unless the ``IF NOT EXISTS`` option is used. If it is used, the statement will be a no-op if the type already exists.
   - A type is intrinsically bound to the keyspace in which it is created and can only be used in that keyspace. At creation, if the type name is prefixed by a keyspace name, it is created in that keyspace. Otherwise, it is created in the current keyspace.
   - As of ScyllaDB Open Source 3.2, UDTs not inside collections do not have to be frozen, but in all versions prior to ScyllaDB Open Source 3.2, and in all ScyllaDB Enterprise versions, UDTs **must** be frozen. 


A non-frozen UDT example with ScyllaDB Open Source 3.2 and higher::

   CREATE TYPE ut (a int, b int);
   CREATE TABLE cf (a int primary key, b ut);

Same UDT in versions prior::

   CREATE TYPE ut (a int, b int);
   CREATE TABLE cf (a int primary key, b frozen<ut>);

UDT literals
~~~~~~~~~~~~

Once a user-defined type has been created, value can be input using a UDT literal:

.. code-block::
   
   udt_literal: '{' `identifier` ':' `term` ( ',' `identifier` ':' `term` )* '}'

In other words, a UDT literal is like a :ref:`map <maps>` literal but its keys are the names of the fields of the type.
For instance, one could insert into the table defined in the previous section using::

  
  INSERT INTO superheroes (name, home)
       VALUES ({first: 'Buffy', last: 'Summers'},
               {street: '1630 Revello Drive',
                city: 'Sunnydale',
                phones: { 'land-line' : { country_code: 1, number: '1234567890'},
                          'fax' : { country_code: 1, number: '10000000'}}
               }
       );

  INSERT INTO superheroes (name, home)
       VALUES ({first: 'Al', last: 'Bundy'},
               {street: '9764 Jeopardy Lane',
                city: 'Chicago'});

To be valid, a UDT literal should only include fields defined by the type it is a literal of, but it can omit some fields
(in which case those will be ``null``).


You can use UDT in a WHERE clause of a SELECT statement as follow::

  SELECT * from superheroes WHERE name={first: 'Al', last: 'Bundy'};

result::

  name                         | home
  ------------------------------+--------------------------------------------------------------------------
  {first: 'Al', last: 'Bundy'} | {street: '9764 Jeopardy Lane', city: 'Chicago', zip: null, phones: null}



Note that if you provide a subset of the UDT, for example, just the ``first`` name in the example below, **null** will be used for the missing values.
For example::

    SELECT * from superheroes WHERE name={first: 'Al'};
    
result::

   name | home
   ------+------

   (0 rows)


Altering a UDT
~~~~~~~~~~~~~~

An existing user-defined type can be modified using an ``ALTER TYPE`` statement:

.. code-block::
   
   alter_type_statement: ALTER TYPE `udt_name` `alter_type_modification`
   alter_type_modification: ADD `field_definition`
                          : | RENAME `identifier` TO `identifier` ( `identifier` TO `identifier` )*

You can:

- Add a new field to the type (``ALTER TYPE address ADD country text``). That new field will be ``null`` for any values
  of the type created before the addition.
- Rename the fields of the type (``ALTER TYPE address RENAME zip TO zipcode``).

Dropping a UDT
~~~~~~~~~~~~~~

You can drop an existing user-defined type using a ``DROP TYPE`` statement:

.. code-block::
   
   drop_type_statement: DROP TYPE [ IF EXISTS ] `udt_name`

Dropping a type results in the immediate, irreversible removal of that type. However, attempting to drop a type that is
still in use by another type, table or function will result in an error.

If the type dropped does not exist, an error will be returned unless ``IF EXISTS`` is used, in which case the operation
is a no-op.

.. _tuples:

Tuples
^^^^^^

CQL also supports tuples and tuple types (where the elements can be of different types). Functionally, tuples can be
thought as anonymous UDTs with anonymous fields. Tuple types and tuple literals are defined by:

.. code-block::
   
   tuple_type: TUPLE '<' `cql_type` ( ',' `cql_type` )* '>'
   tuple_literal: '(' `term` ( ',' `term` )* ')'

and can be used thusly::

    CREATE TABLE durations (
        event text,
        duration tuple<int, text>,
    )

    INSERT INTO durations (event, duration) VALUES ('ev1', (3, 'hours'));

Unlike other "composed" types (collections and UDT), a tuple is always ``frozen<tuple>`` (without the need of the
`frozen` keyword), and it is not possible to update only some elements of a tuple (without updating the whole tuple).
Also, a tuple literal should always provide values for all the components of the tuple type (some of
those values can be null, but they need to be explicitly declared as so).

.. .. _custom-types:

.. Custom Types
.. ^^^^^^^^^^^^

.. .. note:: Custom types exists mostly for backward compatiliby purposes and their usage is discouraged. Their usage is
..    complex, not user friendly and the other provided types, particularly :ref:`user-defined types <udts>`, should almost
..    always be enough.

.. A custom type is defined by:

.. .. code-block::
   
..    custom_type: `string`

.. A custom type is a :token:`string` that contains the name of Java class that extends the server side ``AbstractType``
.. class and that can be loaded by Apache Cassandra (it should thus be in the ``CLASSPATH`` of every node running Cassandra). That
.. class will define what values are valid for the type and how the time sorts when used for a clustering column. For any
.. other purpose, a value of a custom type is the same than that of a ``blob``, and can in particular be input using the
.. :token:`blob` literal syntax.

.. include:: /rst_include/apache-cql-return-index.rst 

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
