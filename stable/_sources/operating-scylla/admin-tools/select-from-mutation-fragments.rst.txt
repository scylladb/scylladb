==========================
Reading mutation fragments
==========================

.. warning:: This statement is not final and is subject to change without notice and in backwards-incompatible ways.

.. note:: The target audience of this statement and therefore that of this document is people who are familiar with the internals of ScyllaDB.

The ``SELECT * FROM MUTATION_FRAGMENTS()`` statement allows for reading the raw underlying mutations (data) from a table.
This is intended to be used as a diagnostics tool to debug performance or correctness issues, where inspecting the raw underlying data, as scylla stores it, is desired.
So far this was only possible with sstables, using a tool like :doc:`ScyllaDB SStable</operating-scylla/admin-tools/scylla-sstable>`.
This statement allows inspecting the content of the row-cache, as well as that of individual memtables, in addition to individual sstables.

The statement has to be used on an existing table, by using a regular ``SELECT`` query, which wraps the table name in ``MUTATION_FRAGMENTS()``. For example, to dump all mutations from ``my_keyspace.my_table``:

.. code-block:: cql

    SELECT * FROM MUTATION_FRAGMENTS(my_keyspace.my_table);

Output Schema
-------------

The schema of the statement, and therefore the columns available to select and to restrict, are different from that of the underlying table.
The output schema is computed from the schema of the underlying table, as follows:

* The partition key columns are copied as-is
* The clustering key is computed as follows:

    - ``mutation_source text``
    - ``partition_region byte``
    - The clustering columns of the underlying table
    - ``position_weight byte``

* Regular columns:

    - ``mutation_fragment_kind text``
    - ``metadata text``
    - ``value text``


So for a table with the following definition:

.. code-block:: cql

    CREATE TABLE my_keyspace.my_table (
        pk1 int,
        pk2 text,
        ck1 byte,
        ck2 text,
        col1 text,
        col2 text,
        PRIMARY KEY ((pk1, pk2), ck1, ck2)
    );


The transformed schema would look like this:

.. code-block:: cql

    CREATE TABLE "my_keyspace.my_table_$mutation_fragments"(
        pk1 int,
        pk2 text,
        mutation_source text,
        partition_region byte,
        ck1 byte,
        ck2 text,
        position_weight byte,
        mutation_fragment_kind text,
        metadata text,
        value text,
        PRIMARY KEY ((pk1, pk2), mutation_source, partition_region, ck1, ck2, position_weight)
    );

Note how the partition-key columns are identical, the clustering key columns are derived from that of the underlying table and the regular columns are completely replaced.

Each row in the output represents a mutation-fragment in the underlying representation, and each partition in the output represents a mutation in the underlying representation.

Columns
^^^^^^^

mutation_source
~~~~~~~~~~~~~~~

The mutation source the mutation originates from. It has the following format: ``${mutation_source_kind}[:${mutation_source_id}]``.
Where ``mutation_source_kind`` is one of:

* ``memtable``
* ``row-cache``
* ``sstable``


And the ``mutation_source_id`` is used to distinguish individual mutation sources of the same kind, where applicable:

* ``memtable`` - a numeric id, starting from ``0``
* ``row-cache`` - N/A, there is only a single cache per table
* ``sstable`` - the path of the sstable


partition_region
~~~~~~~~~~~~~~~~

The numeric representation of the ``enum`` with the same name:

.. code-block:: c++

    enum class partition_region : uint8_t {
        partition_start, // 0
        static_row,      // 1
        clustered,       // 2
        partition_end,   // 3
    };

The reason for using the underlying numeric representation, instead of the name, is to sort mutation-fragments in their natural order.

position_weight
~~~~~~~~~~~~~~~

The position-weight of the underlying mutation-fragment, describing its relation to the clustering key in its position. This is either:

* ``-1`` - before
* ``0`` - at
* ``1`` - after


The reason for using the underlying numeric representation, instead of the human-readeable text, is to sort mutation-fragments in their natural order.

mutation_fragment_kind
~~~~~~~~~~~~~~~~~~~~~~

The kind of the mutation fragment, the row represents. One of:

* ``partition start``
* ``static row``
* ``clustering row``
* ``range tombstone change``
* ``partition end``


This is the text representation of the ``enum class mutation_fragment_v2_kind``. Since this is a regular column, the human readable name is used.

metadata
~~~~~~~~

The content of the mutation-fragment represented as JSON, without the values, if applicable.
This is uses the same JSON schema as :ref:`scylla sstable dump-data<scylla-sstable-dump-data-operation>`.
Content of ``metadata`` column for various mutation fragment kinds:

+------------------------+-------------------------------------------------------------+
| mutation fragment kind | Content                                                     |
+========================+=============================================================+
| partition start        | ``{"tombstone": $TOMBSTONE}``                               |
+------------------------+-------------------------------------------------------------+
| static row             | ``$COLUMNS``                                                |
+------------------------+-------------------------------------------------------------+
| clustering row         | ``$CLUSTERING_ROW`` without the ``type`` and ``key`` fields |
+------------------------+-------------------------------------------------------------+
| range tombstone change | ``{"tombstone": $TOMBSTONE}``                               |
+------------------------+-------------------------------------------------------------+
| partition end          | N/A                                                         |
+------------------------+-------------------------------------------------------------+

JSON symbols are represented as ``$SYMBOL_NAME``, the definition of these can be found in :ref:`scylla sstable dump-data<scylla-sstable-dump-data-operation>`.

value
~~~~~

The value of the mutation-fragment, represented as JSON, if applicable.
Only ``static row`` and ``clustering row`` fragments have values.
The JSON schema of both is that of the ``$COLUMNS`` JSON symbol.
See :ref:`scylla sstable dump-data<scylla-sstable-dump-data-operation>` for the definition of these.
Only the ``value`` field is left in cell objects (``$REGULAR_CELL``, ``$COUNTER_SHARDS_CELL``, ``$COUNTER_UPDATE_CELL`` and ``$FROZEN_COLLECTION``) and the ``cells`` field in collection objects (``$COLLECTION``).
The reason for extracting this out into a separate column, is to allow deselecting the potentially large values, de-cluttering the CQL output, and reducing the amount of data that has to transferred.

Limitations and Peculiarities
-----------------------------

Data is read locally, from the node which receives the query, so replica is always the same node as the coordinator.
The query cannot be migrated between nodes. If a query is paged, all its pages have to be served by the same coordinator. This is enforced, and any attempt to migrate the query to another coordinator will result in the query being aborted.
Note that by default, drivers use round robin load balancing policies, and consequently they will attempt to read each page from a different coordinator.


The statement can output rows with a non-full clustering prefix.

Examples
--------

Given a table, with the following definition:

.. code-block:: cql

    CREATE TABLE ks.tbl (
        pk int,
        ck int,
        v int,
        PRIMARY KEY (pk, ck)
    );

And the following content:

.. code-block:: console

    cqlsh> DELETE FROM ks.tbl WHERE pk = 0;
    cqlsh> DELETE FROM ks.tbl WHERE pk = 0 AND ck > 0 AND ck < 2;
    cqlsh> INSERT INTO ks.tbl (pk, ck, v) VALUES (0, 0, 0);
    cqlsh> INSERT INTO ks.tbl (pk, ck, v) VALUES (0, 1, 0);
    cqlsh> INSERT INTO ks.tbl (pk, ck, v) VALUES (0, 2, 0);
    cqlsh> INSERT INTO ks.tbl (pk, ck, v) VALUES (1, 0, 0);

    cqlsh> SELECT * FROM ks.tbl;

     pk | ck | v
    ----+----+---
      1 |  0 | 0
      0 |  0 | 0
      0 |  1 | 0
      0 |  2 | 0

    (4 rows)

Dump the content of the entire table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: console

    cqlsh> SELECT * FROM MUTATION_FRAGMENTS(ks.tbl);

     pk | mutation_source | partition_region | ck | position_weight | metadata                                                                                                                 | mutation_fragment_kind | value
    ----+-----------------+------------------+----+-----------------+--------------------------------------------------------------------------------------------------------------------------+------------------------+-----------
      1 |      memtable:0 |                0 |    |                 |                                                                                                         {"tombstone":{}} |        partition start |      null
      1 |      memtable:0 |                2 |  0 |               0 | {"marker":{"timestamp":1688122873341627},"columns":{"v":{"is_live":true,"type":"regular","timestamp":1688122873341627}}} |         clustering row | {"v":"0"}
      1 |      memtable:0 |                3 |    |                 |                                                                                                                     null |          partition end |      null
      0 |      memtable:0 |                0 |    |                 |                                      {"tombstone":{"timestamp":1688122848686316,"deletion_time":"2023-06-30 11:00:48z"}} |        partition start |      null
      0 |      memtable:0 |                2 |  0 |               0 | {"marker":{"timestamp":1688122860037077},"columns":{"v":{"is_live":true,"type":"regular","timestamp":1688122860037077}}} |         clustering row | {"v":"0"}
      0 |      memtable:0 |                2 |  0 |               1 |                                      {"tombstone":{"timestamp":1688122853571709,"deletion_time":"2023-06-30 11:00:53z"}} | range tombstone change |      null
      0 |      memtable:0 |                2 |  1 |               0 | {"marker":{"timestamp":1688122864641920},"columns":{"v":{"is_live":true,"type":"regular","timestamp":1688122864641920}}} |         clustering row | {"v":"0"}
      0 |      memtable:0 |                2 |  2 |              -1 |                                                                                                         {"tombstone":{}} | range tombstone change |      null
      0 |      memtable:0 |                2 |  2 |               0 | {"marker":{"timestamp":1688122868706989},"columns":{"v":{"is_live":true,"type":"regular","timestamp":1688122868706989}}} |         clustering row | {"v":"0"}
      0 |      memtable:0 |                3 |    |                 |                                                                                                                     null |          partition end |      null

      (10 rows)

Dump the content of a single partition of interest
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: console

    cqlsh> SELECT * FROM MUTATION_FRAGMENTS(ks.tbl) WHERE pk = 1;

     pk | mutation_source | partition_region | ck | position_weight | metadata                                                                                                                 | mutation_fragment_kind | value
    ----+-----------------+------------------+----+-----------------+--------------------------------------------------------------------------------------------------------------------------+------------------------+-----------
      1 |      memtable:0 |                0 |    |                 |                                                                                                         {"tombstone":{}} |        partition start |      null
      1 |      memtable:0 |                2 |  0 |               0 | {"marker":{"timestamp":1688122873341627},"columns":{"v":{"is_live":true,"type":"regular","timestamp":1688122873341627}}} |         clustering row | {"v":"0"}
      1 |      memtable:0 |                3 |    |                 |                                                                                                                     null |          partition end |      null

    (3 rows)

This works just like selecting a partition from the base table.

Mutation sources
^^^^^^^^^^^^^^^^

Note how after insertion, all data is in the memtable (see above). After flushing the memtable, this will look like this:

.. code-block:: console

    cqlsh> SELECT * FROM MUTATION_FRAGMENTS(ks.tbl) WHERE pk = 1;

     pk | mutation_source                                                                                                  | partition_region | ck | position_weight | metadata                                                                                                                 | mutation_fragment_kind | value
    ----+------------------------------------------------------------------------------------------------------------------+------------------+----+-----------------+--------------------------------------------------------------------------------------------------------------------------+------------------------+-----------
      1 | sstable:/var/lib/scylla/data/ks/tbl-259b2520104011ee822ed2e489876007/me-3g79_0ur3_48e402ejkwsvj7viqr-big-Data.db |                0 |    |                 |                                                                                                         {"tombstone":{}} |        partition start |      null
      1 | sstable:/var/lib/scylla/data/ks/tbl-259b2520104011ee822ed2e489876007/me-3g79_0ur3_48e402ejkwsvj7viqr-big-Data.db |                2 |  0 |               0 | {"marker":{"timestamp":1688122873341627},"columns":{"v":{"is_live":true,"type":"regular","timestamp":1688122873341627}}} |         clustering row | {"v":"0"}
      1 | sstable:/var/lib/scylla/data/ks/tbl-259b2520104011ee822ed2e489876007/me-3g79_0ur3_48e402ejkwsvj7viqr-big-Data.db |                3 |    |                 |                                                                                                                     null |          partition end |      null

    (3 rows)

After executing a read on the queried partition of the underlying table, the data will also be included in the row-cache:

.. code-block:: console

    cqlsh> SELECT * FROM MUTATION_FRAGMENTS(ks.tbl) WHERE pk = 1;

     pk | mutation_source                                                                                                  | partition_region | ck | position_weight | metadata                                                                                                                 | mutation_fragment_kind | value
    ----+------------------------------------------------------------------------------------------------------------------+------------------+----+-----------------+--------------------------------------------------------------------------------------------------------------------------+------------------------+-----------
      1 |                                                                                                        row-cache |                0 |    |                 |                                                                                                         {"tombstone":{}} |        partition start |      null
      1 |                                                                                                        row-cache |                2 |  0 |               0 | {"marker":{"timestamp":1688122873341627},"columns":{"v":{"is_live":true,"type":"regular","timestamp":1688122873341627}}} |         clustering row | {"v":"0"}
      1 |                                                                                                        row-cache |                3 |    |                 |                                                                                                                     null |          partition end |      null
      1 | sstable:/var/lib/scylla/data/ks/tbl-259b2520104011ee822ed2e489876007/me-3g79_0ur3_48e402ejkwsvj7viqr-big-Data.db |                0 |    |                 |                                                                                                         {"tombstone":{}} |        partition start |      null
      1 | sstable:/var/lib/scylla/data/ks/tbl-259b2520104011ee822ed2e489876007/me-3g79_0ur3_48e402ejkwsvj7viqr-big-Data.db |                2 |  0 |               0 | {"marker":{"timestamp":1688122873341627},"columns":{"v":{"is_live":true,"type":"regular","timestamp":1688122873341627}}} |         clustering row | {"v":"0"}
      1 | sstable:/var/lib/scylla/data/ks/tbl-259b2520104011ee822ed2e489876007/me-3g79_0ur3_48e402ejkwsvj7viqr-big-Data.db |                3 |    |                 |                                                                                                                     null |          partition end |      null

    (6 rows)

It is possible to restrict the output to a single mutation source, or mutation source kind:

.. code-block:: console

    cqlsh> SELECT * FROM MUTATION_FRAGMENTS(ks.tbl) WHERE pk = 1 AND mutation_source = 'row-cache';

     pk | mutation_source | partition_region | ck | position_weight | metadata                                                                                                                 | mutation_fragment_kind | value
    ----+-----------------+------------------+----+-----------------+--------------------------------------------------------------------------------------------------------------------------+------------------------+-----------
      1 |       row-cache |                0 |    |                 |                                                                                                         {"tombstone":{}} |        partition start |      null
      1 |       row-cache |                2 |  0 |               0 | {"marker":{"timestamp":1688122873341627},"columns":{"v":{"is_live":true,"type":"regular","timestamp":1688122873341627}}} |         clustering row | {"v":"0"}
      1 |       row-cache |                3 |    |                 |                                                                                                                     null |          partition end |      null

    (3 rows)

Filtering and Aggregation
^^^^^^^^^^^^^^^^^^^^^^^^^

Select only clustering elements:

.. code-block:: console

    cqlsh> SELECT * FROM MUTATION_FRAGMENTS(ks.tbl) WHERE pk = 0 AND partition_region = 2 ALLOW FILTERING;

     pk | mutation_source                                                                                                  | partition_region | ck | position_weight | metadata                                                                                                                 | mutation_fragment_kind | value
    ----+------------------------------------------------------------------------------------------------------------------+------------------+----+-----------------+--------------------------------------------------------------------------------------------------------------------------+------------------------+-----------
      0 | sstable:/var/lib/scylla/data/ks/tbl-259b2520104011ee822ed2e489876007/me-3g79_0ur3_48e402ejkwsvj7viqr-big-Data.db |                2 |  0 |               0 | {"marker":{"timestamp":1688122860037077},"columns":{"v":{"is_live":true,"type":"regular","timestamp":1688122860037077}}} |         clustering row | {"v":"0"}
      0 | sstable:/var/lib/scylla/data/ks/tbl-259b2520104011ee822ed2e489876007/me-3g79_0ur3_48e402ejkwsvj7viqr-big-Data.db |                2 |  0 |               1 |                                      {"tombstone":{"timestamp":1688122853571709,"deletion_time":"2023-06-30 11:00:53z"}} | range tombstone change |      null
      0 | sstable:/var/lib/scylla/data/ks/tbl-259b2520104011ee822ed2e489876007/me-3g79_0ur3_48e402ejkwsvj7viqr-big-Data.db |                2 |  1 |               0 | {"marker":{"timestamp":1688122864641920},"columns":{"v":{"is_live":true,"type":"regular","timestamp":1688122864641920}}} |         clustering row | {"v":"0"}
      0 | sstable:/var/lib/scylla/data/ks/tbl-259b2520104011ee822ed2e489876007/me-3g79_0ur3_48e402ejkwsvj7viqr-big-Data.db |                2 |  2 |              -1 |                                                                                                         {"tombstone":{}} | range tombstone change |      null
      0 | sstable:/var/lib/scylla/data/ks/tbl-259b2520104011ee822ed2e489876007/me-3g79_0ur3_48e402ejkwsvj7viqr-big-Data.db |                2 |  2 |               0 | {"marker":{"timestamp":1688122868706989},"columns":{"v":{"is_live":true,"type":"regular","timestamp":1688122868706989}}} |         clustering row | {"v":"0"}

    (5 rows)

Count range tombstone changes:

.. code-block:: console

    cqlsh> SELECT COUNT(*) FROM MUTATION_FRAGMENTS(ks.tbl) WHERE pk = 0 AND mutation_fragment_kind = 'range tombstone change' ALLOW FILTERING;

     count
    -------
         2

    (1 rows)
