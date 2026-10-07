ScyllaDB Types
==============

Introduction
-------------
This tool allows you to examine raw values obtained from SStables, logs, coredumps, etc., by performing operations on them,
such as ``deserialize``, ``compare``, or ``validate``. See :ref:`Supported Operations <scylla-types-operations>` for details.

Run ``scylla types --help`` for additional information about the tool and the operations.

Usage
------

The command syntax is as follows:

.. code-block:: console

   scylla types <operation> [options] <hex_value1> [hex_value2]


* Provide the values in the hex form without a leading 0x prefix. The exception is the ``serialize`` operation, which expects values in human-readable form.
* You must specify the type of the provided values. See :ref:`Specifying the Value Type <scylla-types-type>`.
* The number of provided values depends on the operation. See :ref:`Supported Operations <scylla-types-operations>` for details.
* The ``scylla types`` operations come with additional options. See :ref:`Additional Options <scylla-types-options>` for the list of options.

.. _scylla-types-type:

Specifying the Value Type
-------------------------

You must specify the type of the value(s) you want to examine by adding the ``-t [type name]`` option to the operation.
Specify the type by providing either its CQL name (for example, ``int`` or ``map<int, text>``) or its Cassandra class name (the prefix can be omitted,
for example, you can provide ``Int32Type`` instead of ``org.apache.cassandra.db.marshal.Int32Type``). See `CQL3 Type Mapping <https://github.com/scylladb/scylladb/blob/master/docs/dev/cql3-type-mapping.md>`_
for a mapping of CQL types to Cassandra type class names. CQL names are case-insensitive and can be mixed with Cassandra class names, for example,
``ReversedType(timeuuid)``.

Type names and values containing spaces, for example ``MapType(Int32Type, UTF8Type)``, have to be quoted on the command line.

If you provide more than one value, all of the values must share the same type. For example:

.. code-block:: console

   scylla types deserialize -t Int32Type b34b62d4 00783562

.. _scylla-types-compound:

**Compounds**

A compound is a single value that is composed of multiple values of possibly different types. An example of a compound value is a clustering key or a partition key.

You can use the ``--prefix-compound``, ``--full-compound`` or ``--legacy-composite`` options to indicate that the provided value is a compound
(see :ref:`Additional Options <scylla-types-options>`) for details. These options have more human-friendly aliases, which can be used
interchangeably with them: ``--clustering-key`` (alias of ``--prefix-compound``), ``--partition-key`` (alias of ``--full-compound``) and
``--legacy-partition-key`` (alias of ``--legacy-composite``).

Full compounds (partition keys) can be serialized in two formats: ScyllaDB's in-memory format (``--full-compound``), which is the format
partition keys are printed in, in ScyllaDB's logs, and the legacy composite format (``--legacy-composite``), which is the format partition
keys are stored in, in SStables. Note that in the legacy composite format, single-component keys are serialized as the value of the component.

When a value is a compound, you can specify a different type for each value making up the compound, **respectively** (i.e., the order 
of the types on the command line must be the same as the order in the compound). For example:

.. code-block:: console

   scylla types deserialize --prefix-compound -t TimeUUIDType -t Int32Type 0010d00819896f6b11ea00000000001c571b000400000010


.. _scylla-types-operations:

Supported Operations
--------------------
* ``serialize`` - Serializes the value and prints it in a hex encoded form. Required arguments: 1 value in human-readable form, or in the case of
  compounds, 1 value for each component (``--full-compound``), or for some of the components (``--prefix-compound``). To avoid problems around
  special symbols, separate values with ``--`` from the rest of the arguments. Serializing values of collection and vector types (including
  tuples and UDTs, which have fields of such types) is not supported, such values are rejected with an error.
* ``deserialize`` - Deserializes and prints the provided value in a human-readable form. Required arguments: 1 or more serialized values.
* ``compare`` - Compares two values and prints the result. Required arguments: 2 serialized values.
* ``ring-order-compare`` - Compares two partition keys in ring order, the order ScyllaDB orders partitions in, and prints the result, along with the
  tokens of the keys: partition keys are ordered by their token first and only by the keys themselves on token collision. Required arguments: 2 serialized values. Only accepts partition keys
  (``--full-compound``/``--partition-key`` or ``--legacy-composite``/``--legacy-partition-key``).
* ``validate`` - Verifies if the value is valid for the type, according to the requirements of the type. Required arguments: 1 or more serialized values.
* ``tokenof`` - Calculates the token of the partition key (i.e. decorates it). Required arguments: 1 or more serialized values. Only accepts partition keys (``--full-compound``/``--partition-key`` or ``--legacy-composite``/``--legacy-partition-key``).
* ``shardof`` - Calculates the token of the partition key and the shard it belongs to, given the provided shard configuration (``--shards`` and ``--ignore-msb-bits``). In most cases, only ``--shards`` has to be provided unless you have a non-standard configuration. Required arguments: 1 or more serialized values. Only accepts partition keys (``--full-compound``/``--partition-key`` or ``--legacy-composite``/``--legacy-partition-key``).


You can learn more about each operation by invoking its help:

    .. code-block:: console

        scylla types $OPERATION --help

.. _scylla-types-options:

Additional Options
------------------

You can run ``scylla types [operation] --help`` for additional information on a given operation.

* ``-h`` ( or ``--help``) - Prints the help message.
* ``--help-seastar`` - Prints the help message about the Seastar options.
* ``--help-loggers`` - Prints a list of logger names.
* ``-t`` ( or ``--type``) - Specifies the type of the provided value. See :ref:`Specifying the Value Type <scylla-types-type>`.
* ``--prefix-compound`` (or ``--clustering-key``) - Indicates that the value is a prefixable compound (e.g., clustering key) composed of multiple values of possibly different types.
* ``--full-compound`` (or ``--partition-key``) - Indicates that the value is a full compound (e.g., partition key) composed of multiple values of possibly different types.
* ``--legacy-composite`` (or ``--legacy-partition-key``) - Indicates that the value is a full compound (e.g., partition key), serialized in the legacy composite format,
  used in SStables, instead of ScyllaDB's in-memory format.
* ``--shards`` - The number of shards (only relevant for the ``shardof`` operation).
* ``--ignore-msb-bits`` - The number of the most significant bits of the token to ignore, when calculating the shard. Defaults to 12, the default
  value of the ``murmur3_partitioner_ignore_msb_bits`` configuration option (only relevant for the ``shardof`` operation).
* ``--value arg`` - Specifies the value to process (if not provided as a positional argument).

Examples
--------
* Serializing a value of type Int32Type:

    .. code-block:: console

        scylla types serialize -t Int32Type -- -1286905132

    Output:

    .. code-block:: console
       :class: hide-copy-button

        b34b62d4

* Serializing a clustering-key (``--prefix-compound``):

    .. code-block:: console

        scylla types serialize --prefix-compound -t TimeUUIDType -t Int32Type -- d0081989-6f6b-11ea-0000-0000001c571b 16

    Output:

    .. code-block:: console
       :class: hide-copy-button

        0010d00819896f6b11ea00000000001c571b000400000010

* Serializing a clustering-key prefix (``--prefix-compound``), with only some of the components present:

    .. code-block:: console

        scylla types serialize --prefix-compound -t TimeUUIDType -t Int32Type -- d0081989-6f6b-11ea-0000-0000001c571b

    Output:

    .. code-block:: console
       :class: hide-copy-button

        0010d00819896f6b11ea00000000001c571b

* Serializing a partition-key (``--full-compound``):

    .. code-block:: console

        scylla types serialize --full-compound -t Int32Type -t UTF8Type -- 1 abc

    Output:

    .. code-block:: console
       :class: hide-copy-button

        0004000000010003616263

* Serializing a partition-key in the legacy composite format, used in SStables (``--legacy-composite``):

    .. code-block:: console

        scylla types serialize --legacy-composite -t Int32Type -t UTF8Type -- 1 abc

    Output:

    .. code-block:: console
       :class: hide-copy-button

        00040000000100000361626300

* Deserializing and printing a value of type Int32Type:

    .. code-block:: console

       scylla types deserialize -t Int32Type b34b62d4

    Output:

    .. code-block:: console
       :class: hide-copy-button
    
       -1286905132

* Validating a value of type Int32Type:

    .. code-block:: console

       scylla types validate -t Int32Type b34b62d4

    Output:

    .. code-block:: console
       :class: hide-copy-button

       b34b62d4: VALID - -1286905132

* Deserializing a value of a collection type, specified with its CQL name:

    .. code-block:: console

       scylla types deserialize -t 'map<int, text>' 0000000100000004000000010000000161

    Output:

    .. code-block:: console
       :class: hide-copy-button

       {1 : a}

* Comparing two values of ReversedType(TimeUUIDType):

    .. code-block:: console

       scylla types compare -t 'ReversedType(TimeUUIDType)' b34b62d46a8d11ea0000005000237906 d00819896f6b11ea00000000001c571b

    Output:

    .. code-block:: console
       :class: hide-copy-button

       b34b62d4-6a8d-11ea-0000-005000237906 > d0081989-6f6b-11ea-0000-0000001c571b

* Comparing two partition keys in ring order. Note that ``compare`` would return the opposite result, as it compares the components of the keys,
  not their tokens:

    .. code-block:: console

       scylla types ring-order-compare --partition-key -t Int32Type -t UTF8Type 0004000000010003616263 0004000000020003616263

    Output:

    .. code-block:: console
       :class: hide-copy-button

       {token: 8771735466527499816, key: (1, abc)} > {token: -3504390351319460166, key: (2, abc)}

* Deserializing and printing a compound value:

    .. code-block:: console

       scylla types deserialize --prefix-compound -t TimeUUIDType -t Int32Type 0010d00819896f6b11ea00000000001c571b000400000010

    Output:

    .. code-block:: console
       :class: hide-copy-button

       (d0081989-6f6b-11ea-0000-0000001c571b, 16)

* Calculating the token of a partition key:

    .. code-block:: console

        scylla types tokenof --full-compound -t UTF8Type -t SimpleDateType -t UUIDType 000d66696c655f696e7374616e63650004800049190010c61a3321045941c38e5675255feb0196

    Output:

    .. code-block:: console
       :class: hide-copy-button

        (file_instance, 2021-03-27, c61a3321-0459-41c3-8e56-75255feb0196): -5043005771368701888

* Calculating the owner shard of a partition key:

    .. code-block:: console

        scylla types shardof --full-compound -t UTF8Type -t SimpleDateType -t UUIDType --shards=7 000d66696c655f696e7374616e63650004800049190010c61a3321045941c38e5675255feb0196

    Output:

    .. code-block:: console
       :class: hide-copy-button

        (file_instance, 2021-03-27, c61a3321-0459-41c3-8e56-75255feb0196): token: -5043005771368701888, shard: 1
