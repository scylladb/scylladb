Scylla Types
==============

.. versionadded:: 5.0

Introduction
-------------
This tool allows you to examine raw values obtained from SStables, logs, coredumps, etc., by performing operations on them,
such as ``print``, ``compare``, or ``validate``. See :ref:`Supported Operations <scylla-types-operations>` for details.

Run ``scylla types --help`` for additional information about the tool and the operations.

Usage
------

Syntax
^^^^^^

The command syntax is as follows:

.. code-block:: console

   scylla types <operation> [options] <hex_value1> [hex_value2]


* Provide the values in the hex form without a leading 0x prefix.
* You must specify the type of the provided values. See :ref:`Specifying the Value Type <scylla-types-type>`.
* The number of provided values depends on the operation. See :ref:`Supported Operations <scylla-types-operations>` for details.
* The ``scylla types`` operations come with additional options. See :ref:`Additional Options <scylla-types-options>` for the list of options.

.. _scylla-types-type:

Specifying the Value Type
^^^^^^^^^^^^^^^^^^^^^^^^^^

You must specify the type of the value(s) you want to examine by adding the ``-t [type name]`` option to the operation. 
Specify the type by providing its Cassandra class name (the prefix can be omitted, for example, you can provide ``Int32Type`` 
instead of ``org.apache.cassandra.db.marshal.Int32Type``). See https://github.com/scylladb/scylla/blob/master/docs/design-notes/cql3-type-mapping.md for a mapping of cql3 types to Cassandra type class names.

If you provide more than one value, all of the values must share the same type. For example:

.. code-block:: console

   scylla types print -t Int32Type b34b62d4 00783562

.. _scylla-types-compound:

**Compounds**

A compound is a single value that is composed of multiple values of possibly different types. An example of a compound value is a clustering key or a partition key.

You can use the ``--prefix-compound`` or ``--full-compound``  options to indicate that the provided value is a compound 
(see :ref:`Additional Options <scylla-types-options>`) for details.

When a value is a compound, you can specify a different type for each value making up the compound, **respectively** (i.e., the order 
of the types on the command line must be the same as the order in the compound). For example:

.. code-block:: console

   scylla types print --prefix-compound -t TimeUUIDType -t Int32Type 0010d00819896f6b11ea00000000001c571b000400000010


.. _scylla-types-operations:

Supported Operations
^^^^^^^^^^^^^^^^^^^^^^^
* ``print`` - Deserializes and prints the provided value in a human-readable form. Required arguments: 1 or more serialized values.
* ``compare`` - Compares two values and print the result. Required arguments: 2 serialized values.
* ``validate`` - Verifies if the value is valid for the type, according to the requirements of the type. Required arguments: 1 or more serialized values.


.. _scylla-types-options:

Additional Options
^^^^^^^^^^^^^^^^^^^

You can run ``scylla types [operation] --help`` for additional information on a given operation.

* ``-h`` ( or ``--help``) - Prints the help message.
* ``--help-seastar`` - Prints the help message about the Seastar options.
* ``--help-loggers`` - Prints a list of logger names.
* ``-t`` ( or ``--type``) - Specifies the type of the provided value. See :ref:`Specifying the Value Type <scylla-types-type>`.
* ``--prefix-compound`` - Indicates that the value is a prefixable compound (e.g., clustering key) composed of multiple values of possibly different types.
* ``--full-compound`` - Indicates that the value is a full compound (e.g., partition key) composed of multiple values of possibly different types.
* ``--value arg`` - Specifies the value to process (if not provided as a positional argument).

Examples
^^^^^^^^
* Deserializing and printing a value of type Int32Type:

    .. code-block:: console

       scylla types print -t Int32Type b34b62d4

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

       scylla types print --prefix-compound -t TimeUUIDType -t Int32Type 0010d00819896f6b11ea00000000001c571b000400000010

    Output:

    .. code-block:: console
       :class: hide-copy-button

       (d0081989-6f6b-11ea-0000-0000001c571b, 16)

