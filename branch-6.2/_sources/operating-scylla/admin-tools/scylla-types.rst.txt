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


* Provide the values in the hex form without a leading 0x prefix.
* You must specify the type of the provided values. See :ref:`Specifying the Value Type <scylla-types-type>`.
* The number of provided values depends on the operation. See :ref:`Supported Operations <scylla-types-operations>` for details.
* The ``scylla types`` operations come with additional options. See :ref:`Additional Options <scylla-types-options>` for the list of options.

.. _scylla-types-type:

Specifying the Value Type
-------------------------

You must specify the type of the value(s) you want to examine by adding the ``-t [type name]`` option to the operation. 
Specify the type by providing its Cassandra class name (the prefix can be omitted, for example, you can provide ``Int32Type`` 
instead of ``org.apache.cassandra.db.marshal.Int32Type``). See `CQL3 Type Mapping <https://github.com/scylladb/scylladb/blob/master/docs/dev/cql3-type-mapping.md>`_ for a mapping of cql3 types to Cassandra type class names.

If you provide more than one value, all of the values must share the same type. For example:

.. code-block:: console

   scylla types deserialize -t Int32Type b34b62d4 00783562

.. _scylla-types-compound:

**Compounds**

A compound is a single value that is composed of multiple values of possibly different types. An example of a compound value is a clustering key or a partition key.

You can use the ``--prefix-compound`` or ``--full-compound``  options to indicate that the provided value is a compound 
(see :ref:`Additional Options <scylla-types-options>`) for details.

When a value is a compound, you can specify a different type for each value making up the compound, **respectively** (i.e., the order 
of the types on the command line must be the same as the order in the compound). For example:

.. code-block:: console

   scylla types deserialize --prefix-compound -t TimeUUIDType -t Int32Type 0010d00819896f6b11ea00000000001c571b000400000010


.. _scylla-types-operations:

Supported Operations
--------------------
* ``serialize`` - Serialize the value and prints it in a hex encoded form. Required arguments: 1 value in human-readable form. To avoid problems around special symbols, separate values with ``--`` from the rest of the arguments.
* ``deserialize`` - Deserializes and prints the provided value in a human-readable form. Required arguments: 1 or more serialized values.
* ``compare`` - Compares two values and prints the result. Required arguments: 2 serialized values.
* ``validate`` - Verifies if the value is valid for the type, according to the requirements of the type. Required arguments: 1 or more serialized values.
* ``tokenof`` - Calculates the token of the partition key (i.e. decorates it). Required arguments: 1 or more serialized values. Only accepts partition keys (``--full-compound``).
* ``shardof`` - Calculates the token of the partition key and the shard it belongs to, given the provided shard configuration (``--shards`` and ``--ignore-msb-bits``). In most cases, only ``--shards`` has to be provided unless you have a non-standard configuration. Required arguments: 1 or more serialized values. Only accepts partition keys (``--full-compound``).


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
* ``--prefix-compound`` - Indicates that the value is a prefixable compound (e.g., clustering key) composed of multiple values of possibly different types.
* ``--full-compound`` - Indicates that the value is a full compound (e.g., partition key) composed of multiple values of possibly different types.
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

* Serializing a partition-key (``--full-compound``):

    .. code-block:: console

        scylla types serialize --prefix-compound -t TimeUUIDType -t Int32Type -- d0081989-6f6b-11ea-0000-0000001c571b

    Output:

    .. code-block:: console
       :class: hide-copy-button

        0010d00819896f6b11ea00000000001c571b

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

* Comparing two values of ReversedType(TimeUUIDType):

    .. code-block:: console

       scylla types compare -t 'ReversedType(TimeUUIDType)' b34b62d46a8d11ea0000005000237906 d00819896f6b11ea00000000001c571b

    Output:

    .. code-block:: console
       :class: hide-copy-button

       b34b62d4-6a8d-11ea-0000-005000237906 > d0081989-6f6b-11ea-0000-0000001c571b

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
