Nodetool getendpoints
=====================
**getendpoints** - :code:`<keyspace>` :code:`<table>` :code:`<key>`- Print the end-points IP or name of the nodes that own the partition key.
**getendpoints** - :code:`<keyspace>` :code:`<table>` :code:`<key-components>` - Print the end-points IP or name of the nodes that own the partition key.

Use this command to know which node holds the specific partition key.

For example:

``nodetool getendpoints nba player_name Russell``

==============  ==============
Parameter       Description
==============  ==============
keyspace        keyspace name
--------------  --------------
table           table name
--------------  --------------
key             partition key
--------------  --------------
key-components  partition key (Alternative way to specify composite partition keys using individual components.)
==============  ==============

In a case of a composite partition key, use ``:`` between columns in the key. All columns in the partition key are required.
Alternatively, you can use the ``--key-components`` option to specify each component of the partition key separately,
instead of combining them into a single colon-separated string.
This approach avoids ambiguity when key values contain colons.
All partition key columns must be specified, and they must be provided in the correct order as defined in the table schema.
You must specify either the ``--key`` or the ``--key-components`` option, but not both.

ScyllaDB does not support *getendpoints* for a partition key with a frozen UDT.

**Example 1: Single partition key**

.. code-block:: cql

   CREATE TABLE superheroes (
      firstname text,
      lastname text,
      age int,
      PRIMARY KEY (firstname)
   );

.. code-block:: bash

   nodetool getendpoints "superheroes" "peter"

This retrieves the nodes responsible for the partition key ``peter`` in the ``superheroes`` table.

**Example 2: Composite partition key (specified as colon-separated string)**

.. code-block:: cql

   CREATE TABLE superheroes (
      firstname text,
      lastname text,
      age int,
      PRIMARY KEY ((firstname, lastname))
   );

.. code-block:: bash

   nodetool getendpoints "mykeyspace" "superheroes" "peter:parker"

When using a composite partition key, separate the components with a colon (`:`) in the order defined in the schema.

**Example 3: Composite partition key (specified by individual components)**

.. code-block:: cql

   CREATE TABLE superheroes (
      firstname text,
      lastname text,
      age int,
      PRIMARY KEY ((firstname, lastname))
   );

.. code-block:: bash

   nodetool getendpoints "mykeyspace" "superheroes" --key-components "peter" --key-components "parker"

**Example 4: Composite partition key with colon**

.. code-block:: cql

   CREATE TABLE superheroes (
      firstname text,
      lastname text,
      age int,
      PRIMARY KEY ((firstname, lastname))
   );

.. code-block:: bash

   nodetool getendpoints "mykeyspace" "superheroes" --key-components "peter:the-great" --key-components "parker"

In this example, the first key component includes a colon, which would cause parsing issues in the string format.
Using ``--key-components`` ensures it's interpreted correctly.

.. include:: nodetool-index.rst
