.. highlight:: cql

Data Definition
===============

CQL stores data in *tables*, whose schema defines the layout of said data in the table, and those tables are grouped in
*keyspaces*. A keyspace defines a number of options that apply to all the tables it contains, most prominently of
which is the replication strategy used by the keyspace. An application can have only one keyspace. However, it is also possible to 
have multiple keyspaces in case your application has different replication requirements. 

.. note::

    Schema updates require at least a quorum of nodes in a cluster to be available. 
    If the quorum is lost, it must be restored before a schema is updated. 
    See :doc:`Handling Node Failures </troubleshooting/handling-node-failures>` for details. 

This section describes the statements used to create, modify, and remove keyspaces and tables.

:ref:`CREATE KEYSPACE <create-keyspace-statement>`

.. _create-keyspace:

:ref:`USE <use-statement>`

.. _use:

:ref:`ALTER KEYSPACE <alter-keyspace-statement>`

.. _ALTER KEYSPACE:

:ref:`DROP KEYSPACE <drop-keyspace-statement>`

.. _DROP KEYSPACE:

:ref:`CREATE TABLE <create-table-statement>`

.. _CREATE TABLE:

:ref:`ALTER TABLE <alter-table-statement>`

.. _ALTER TABLE:

:ref:`DROP TABLE <drop-table-statement>`

.. _DROP TABLE:

:ref:`TRUNCATE <truncate-statement>`

.. _TRUNCATE:

.. _data-definition:

Common Definitions
^^^^^^^^^^^^^^^^^^

Keyspace and table names are defined by the following grammar:

.. code-block:: cql

   keyspace_name: `name`
   table_name: [ `keyspace_name` '.' ] `name`
   name: `unquoted_name` | `quoted_name`
   unquoted_name: re('[a-zA-Z_0-9]{1, 48}')
   quoted_name: '"' `unquoted_name` '"'

Both keyspace and table names consist of only alphanumeric characters, cannot be empty, and are limited in
size to 48 characters (that limit exists mostly to avoid filenames, which may include the keyspace and table name, to go
over the limits of certain file systems). By default, keyspace and table names are case insensitive (``myTable`` is
equivalent to ``mytable``), but case sensitivity can be forced by using double-quotes (``"myTable"`` is different from
``mytable``).

Further, a table is always part of a keyspace, and a table name can be provided fully-qualified by the keyspace it is
part of. If it is not fully-qualified, the table is assumed to be in the *current* keyspace (see :ref:`USE statement
<use-statement>`).

Further, valid column names are simply defined as:

.. code-block:: cql

   column_name: `identifier`

We also define the notion of statement options for use in the following section:

.. code-block:: cql

   options: `option` ( AND `option` )*
   option: `identifier` '=' ( `identifier` | `constant` | `map_literal` )

.. _create-keyspace-statement:

In all cases, for creating keyspaces and tables, if you are using :doc:`Reserved Keywords </cql/reserved-keywords>`, enclose them in single or double-quotes.

CREATE KEYSPACE
^^^^^^^^^^^^^^^

A keyspace is created using a ``CREATE KEYSPACE`` statement:

.. code-block:: cql

   create_keyspace_statement: CREATE KEYSPACE [ IF NOT EXISTS ] `keyspace_name` WITH `options`

For example:

.. code-block:: cql

   CREATE KEYSPACE Excalibur
   WITH replication = {'class': 'NetworkTopologyStrategy', 'DC1' : 1, 'DC2' : 3}
   AND durable_writes = true;

The supported ``options`` are:

=================== ========== =========== ========= ===================================================================
name                 kind       mandatory   default   description
=================== ========== =========== ========= ===================================================================
``replication``      *map*      yes                   The replication strategy and options to use for the keyspace (see
                                                      details below).
``durable_writes``   *simple*   no          true      Whether to use the commit log for updates on this keyspace
                                                      (disable this option at your own risk!).
``tablets``          *map*      no                    Enables or disables tablets for the keyspace (see :ref:`tablets <tablets>`)
=================== ========== =========== ========= ===================================================================

The ``replication`` property is mandatory and must at least contains the ``'class'`` sub-option, which defines the
replication strategy class to use. The rest of the sub-options depend on what replication
strategy is used. By default, ScyllaDB supports the following ``'class'``:

.. _replication-strategy:

SimpleStrategy
~~~~~~~~~~~~~~~

A simple strategy that defines a replication factor for data to be spread
across the entire cluster. This is generally not a wise choice for production
because it does not respect datacenter layouts and can lead to wildly varying
query latency. For a production ready strategy, see *NetworkTopologyStrategy* . *SimpleStrategy* supports a single mandatory argument:

========================= ====== ======= =============================================
sub-option                 type   since   description
========================= ====== ======= =============================================
``'replication_factor'``   int    all     The number of replicas to store per range.

                                          The replication factor should be equal to
                                          or lower than the number of nodes.
                                          Configuring a higher RF may prevent
                                          creating tables in that keyspace. 
========================= ====== ======= =============================================

.. note:: Using NetworkTopologyStrategy is recommended. Using SimpleStrategy will make it harder to add Data Center in the future.

NetworkTopologyStrategy
~~~~~~~~~~~~~~~~~~~~~~~

A production ready replication strategy that allows to set the replication
factor independently for each data-center. The rest of the sub-options are
key-value pairs where a key is a data-center name and its value is the
associated replication factor. Options:

===================================== ====== =============================================
sub-option                             type  description
===================================== ====== =============================================
``'<datacenter>'``                     int   The number of replicas to store per range in
                                             the provided datacenter.
``'replication_factor'``               int   The number of replicas to use as a default
                                             per datacenter if not specifically provided.
                                             Note that this always defers to existing
                                             definitions or explicit datacenter settings.
                                             For example, to have three replicas per
                                             datacenter, supply this with a value of 3.

                                             The replication factor configured for a DC
                                             should be equal to or lower than the number
                                             of nodes in that DC. Configuring a higher RF 
                                             may prevent creating tables in that keyspace. 
===================================== ====== =============================================

Note that when ``ALTER`` ing keyspaces and supplying ``replication_factor``,
auto-expansion will only *add* new datacenters for safety, it will not alter
existing datacenters or remove any even if they are no longer in the cluster.
If you want to remove datacenters while still supplying ``replication_factor``,
explicitly zero out the datacenter you want to have zero replicas.

An example of auto-expanding datacenters with two datacenters: ``DC1`` and ``DC2``::

    CREATE KEYSPACE excalibur
        WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor' : 3}

    DESCRIBE KEYSPACE excalibur
        CREATE KEYSPACE excalibur WITH replication = {'class': 'NetworkTopologyStrategy', 'DC1': '3', 'DC2': '3'} AND durable_writes = true;


An example of auto-expanding and overriding a datacenter::

    CREATE KEYSPACE excalibur
        WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor' : 3, 'DC2': 2}

    DESCRIBE KEYSPACE excalibur
        CREATE KEYSPACE excalibur WITH replication = {'class': 'NetworkTopologyStrategy', 'DC1': '3', 'DC2': '2'} AND durable_writes = true;

An example that excludes a datacenter while using ``replication_factor``::

    CREATE KEYSPACE excalibur
        WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor' : 3, 'DC2': 0} ;

    DESCRIBE KEYSPACE excalibur
        CREATE KEYSPACE excalibur WITH replication = {'class': 'NetworkTopologyStrategy', 'DC1': '3'} AND durable_writes = true;



.. only:: opensource
  
  Keyspace storage options :label-caution:`Experimental`
  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  By default, SStables of a keyspace are stored locally.
  As an alternative, you can configure your keyspace to be stored
  on Amazon S3 or another S3-compatible object store.
  See :ref:`Keyspace storage options <admin-keyspace-storage-options>` for details.

.. _tablets:

The ``tablets`` property
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``tablets`` property enables or disables tablets-based distribution
for a keyspace. 

Options:

===================================== ====== =============================================
sub-option                             type  description
===================================== ====== =============================================
``'enabled'``                          bool  Whether or not to enable tablets for a keyspace
``'initial'``                          int   The number of tablets to start with
===================================== ====== =============================================

.. scylladb_include_flag:: tablets-default.rst

A good rule of thumb to calculate initial tablets is to divide the expected total storage used
by tables in this keyspace by (``replication_factor`` * 5GB). For example, if you expect a 30TB
table and have a replication factor of 3, divide 30TB by (3*5GB) for a result of 2000. Since the
value must be a power of two, round up to 2048.
The calculation applies to every table in the keyspace.

An example that creates a keyspace with 2048 tablets per table::

    CREATE KEYSPACE excalibur
    WITH replication = {
        'class': 'NetworkTopologyStrategy',
        'replication_factor': 3,
    } AND tablets = {
        'initial': 2048
    };


See :doc:`Data Distribution with Tablets </architecture/tablets>` for more information about tablets.

.. _use-statement:        
        
USE
^^^

The ``USE`` statement allows you to change the *current* keyspace (for the *connection* on which it is executed). Some objects in CQL are bound to a keyspace (tables, user-defined types, functions, ...), and the current keyspace is the
default keyspace used when those objects are referred without a fully-qualified name (that is, without being prefixed a
keyspace name). A ``USE`` statement simply takes the specified keyspace and uses the name as an argument for all future actions until this name is changed.

.. code-block:: cql

   use_statement: USE `keyspace_name`

.. _alter-keyspace-statement:

ALTER KEYSPACE
^^^^^^^^^^^^^^

An ``ALTER KEYSPACE`` statement lets you modify the options of a keyspace:

.. code-block:: cql

   alter_keyspace_statement: ALTER KEYSPACE `keyspace_name` WITH `options`

For instance::

  ALTER KEYSPACE Excelsior 
   WITH replication = { 'class' : 'NetworkTopologyStrategy', 'dc1' : 3, 'dc2' : 0};


The supported options are the same as :ref:`creating a keyspace <create-keyspace-statement>`.

ALTER KEYSPACE with Tablets :label-caution:`Experimental`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Modifying a keyspace with tablets enabled is possible and doesn't require any special CQL syntax. However, there are some limitations:

- The replication factor (RF) can be increased or decreased by at most 1 at a time. To reach the desired RF value, modify the RF repeatedly.
- The ``ALTER`` statement rejects the ``replication_factor`` tag. List the DCs explicitly when altering a keyspace. See :ref:`NetworkTopologyStrategy <replication-strategy>`.
- If there's any other ongoing global topology operation, executing the ``ALTER`` statement will fail (with an explicit and specific error) and needs to be repeated.
- The ``ALTER`` statement may take longer than the regular query timeout, and even if it times out, it will continue to execute in the background.
- The replication strategy cannot be modified, as keyspaces with tablets only support ``NetworkTopologyStrategy``.

.. _drop-keyspace-statement:

DROP KEYSPACE
^^^^^^^^^^^^^

Dropping a keyspace can be done using the ``DROP KEYSPACE`` statement:

.. code-block:: cql

   drop_keyspace_statement: DROP KEYSPACE [ IF EXISTS ] `keyspace_name`

For instance::

    DROP KEYSPACE Excelsior;

Dropping a keyspace results in the immediate removal of that keyspace, including all the tables, UTD and
functions in it, and all the data contained in those tables.

.. include:: /getting-started/_common/note-reclaim-space.rst

If the keyspace does not exist, the statement will return an error unless ``IF EXISTS`` is used, in which case the
operation is a no-op.

.. _create-table-statement:

CREATE TABLE
^^^^^^^^^^^^

Creating a new table uses the ``CREATE TABLE`` statement:

.. code-block:: cql

   create_table_statement: CREATE TABLE [ IF NOT EXISTS ] `table_name`
                         : '('
                         :     `column_definition`
                         :     ( ',' `column_definition` )*
                         :     [ ',' PRIMARY KEY '(' `primary_key` ')' ]
                         : ')' [ WITH `table_options` ]
   
   column_definition: `column_name` `cql_type` [ STATIC ] [ PRIMARY KEY]
   
   primary_key: `partition_key` [ ',' `clustering_columns` ]
   
   partition_key: `column_name`
                : | '(' `column_name` ( ',' `column_name` )* ')'
   
   clustering_columns: `column_name` ( ',' `column_name` )*
   
   table_options: COMPACT STORAGE [ AND `table_options` ]
                   : | CLUSTERING ORDER BY '(' `clustering_order` ')' [ AND `table_options` ]
                   : | scylla_encryption_options: '=' '{'[`cipher_algorithm` : <hash>]','[`secret_key_strength` : <len>]','[`key_provider`: <provider>]'}'
                   : | caching  '=' ' {'caching_options'}'
                   : | `options`
   
   clustering_order: `column_name` (ASC | DESC) ( ',' `column_name` (ASC | DESC) )*

For instance::

    CREATE TABLE monkeySpecies (
        species text PRIMARY KEY,
        common_name text,
        population varint,
        average_size int
    ) WITH comment='Important biological records';

    CREATE TABLE timeline (
        userid uuid,
        posted_month int,
        posted_time uuid,
        body text,
        posted_by text,
        PRIMARY KEY (userid, posted_month, posted_time)
    ) WITH compaction = { 'class' : 'LeveledCompactionStrategy' };

    CREATE TABLE loads (
        machine inet,
        cpu int,
        mtime timeuuid,
        load float,
        PRIMARY KEY ((machine, cpu), mtime)
    ) WITH CLUSTERING ORDER BY (mtime DESC);

    CREATE TABLE users_picture (
        userid uuid,
        pictureid uuid,
        body text,
        posted_by text,
        PRIMARY KEY (userid, pictureid, posted_by)
    ) WITH compression = {'sstable_compression': 'LZ4Compressor'};


    CREATE TABLE data_atrest (
        pk text PRIMARY KEY, 
        c0 int
    ) WITH scylla_encryption_options = {  
       'cipher_algorithm' : 'AES/ECB/PKCS5Padding', 
       'secret_key_strength' : 128,  
       'key_provider': 'LocalFileSystemKeyProviderFactory',  
       'secret_key_file': '/etc/scylla/data_encryption_keys/secret_key'};

   CREATE TABLE caching (
                    k int PRIMARY KEY,
                    v1 int,
                    v2 int,
                ) WITH caching = {'enabled': 'true'};


A CQL table has a name and is composed of a set of *rows*. Creating a table amounts to defining which :ref:`columns
<column-definition>` the rows will be composed, which of those columns compose the :ref:`primary key <primary-key>`, as
well as optional :ref:`options <create-table-options>` for the table.

Attempting to create an already existing table will return an error unless the ``IF NOT EXISTS`` directive is used. If
it is used, the statement will be a no-op if the table already exists.


.. _column-definition:

Column definitions
~~~~~~~~~~~~~~~~~~

Every row in a CQL table has a set of predefined columns defined at the time of the table creation (or added later
using an :ref:`alter statement<alter-table-statement>`).

A :token:`column_definition` is primarily comprised of the name of the column defined and its :ref:`type <data-types>`,
which restricts which values are accepted for that column. Additionally, a column definition can have the following
modifiers:

``STATIC``
    declares the column as being a :ref:`static column <static-columns>`.

``PRIMARY KEY``
    declares the column as being the sole component of the :ref:`primary key <primary-key>` of the table.

.. _static-columns:

Static columns
``````````````
Some columns can be declared as ``STATIC`` in a table definition. A column that is static will be “shared” by all the
rows belonging to the same partition (having the same :ref:`partition key <partition-key>`). For instance::

    CREATE TABLE t (
        pk int,
        t int,
        v text,
        s text static,
        PRIMARY KEY (pk, t)
    );

    INSERT INTO t (pk, t, v, s) VALUES (0, 0, 'val0', 'static0');
    INSERT INTO t (pk, t, v, s) VALUES (0, 1, 'val1', 'static1');

    SELECT * FROM t;
       pk | t | v      | s
      ----+---+--------+-----------
       0  | 0 | 'val0' | 'static1'
       0  | 1 | 'val1' | 'static1'

As can be seen, the ``s`` value is the same (``static1``) for both of the rows in the partition (the partition key in
that example being ``pk``, both rows are in that same partition): the 2nd insertion has overridden the value for ``s``.

Static columns have the following restrictions:

- tables with the ``COMPACT STORAGE`` option (see below) cannot use them.
- a table without clustering columns cannot have static columns (in a table without clustering columns, every partition
  has only one row, and so every column is inherently static).
- only non ``PRIMARY KEY`` columns can be static.

.. _primary-key:

The Primary key
~~~~~~~~~~~~~~~

Within a table, a row is uniquely identified by its ``PRIMARY KEY``, and hence all tables **must** define a PRIMARY KEY
(and only one). A ``PRIMARY KEY`` definition is composed of one or more of the columns defined in the table.
Syntactically, the primary key is defined by the keywords ``PRIMARY KEY``, followed by a comma-separated list of the column
names composing it within parenthesis. However, if the primary key has only one column, one can alternatively follow that
column definition by the ``PRIMARY KEY`` keywords. The order of the columns in the primary key definition matter.

A CQL primary key is composed of 2 parts:

- the :ref:`partition key <partition-key>` part. It is the first component of the primary key definition. It can be a
  single column or, using additional parenthesis, can be multiple columns. A table always has at least a partition key,
  the smallest possible table definition is::

      CREATE TABLE t (k text PRIMARY KEY);

- the :ref:`clustering columns <clustering-columns>`. Those are the columns after the first component of the primary key
  definition, and the order of those columns define the *clustering order*.

Some examples of primary key definition are:

- ``PRIMARY KEY (a)``: ``a`` is the partition key, and there are no clustering columns.
- ``PRIMARY KEY (a, b, c)``: ``a`` is the partition key, and ``b`` and ``c`` are the clustering columns.
- ``PRIMARY KEY ((a, b), c)``: ``a`` and ``b`` compose the partition key (this is often called a *composite* partition
  key), and ``c`` is the clustering column.


.. note:: A *null* value is not allowed as any partition-key or clustering-key column. A Null value is *not* the same as an empty string.

.. _partition-key:

The partition key
`````````````````

Within a table, CQL defines the notion of a *partition*. A partition is simply the set of rows that share the same value
for their partition key. Note that if the partition key is composed of multiple columns, then rows belong to the same
partition only when they have the same values for all those partition key columns. So, for instance, given the following table
definition and content::

    CREATE TABLE t (
        a int,
        b int,
        c int,
        d int,
        PRIMARY KEY ((a, b), c, d)
    );

    SELECT * FROM t;
       a | b | c | d
      ---+---+---+---
       0 | 0 | 0 | 0    // row 1
       0 | 0 | 1 | 1    // row 2
       0 | 1 | 2 | 2    // row 3
       0 | 1 | 3 | 3    // row 4
       1 | 1 | 4 | 4    // row 5

``row 1`` and ``row 2`` are in the same partition, ``row 3`` and ``row 4`` are also in the same partition (but a
different one) and ``row 5`` is in yet another partition.

Note that a table always has a partition key and that if the table has no :ref:`clustering columns
<clustering-columns>`, then every partition of that table is only comprised of a single row (since the primary key
uniquely identifies rows and the primary key is equal to the partition key if there are no clustering columns).

The most important property of partition is that all the rows belonging to the same partition are guarantee to be stored
on the same set of replica nodes. In other words, the partition key of a table defines which of the rows will be
localized together in the cluster, and it is thus important to choose your partition key wisely so that rows that need
to be fetched together are in the same partition (so that querying those rows together require contacting a minimum of
nodes).

However, please note that there is a flip-side to this guarantee: as all rows sharing a partition key are guaranteed to
be stored on the same set of replica nodes, a partition key that groups too much data can create a hotspot.

Another useful property of a partition is that when writing data, all the updates belonging to a single partition are
done *atomically* and in *isolation*, which is not the case across partitions.

The proper choice of the partition key and clustering columns for a table is probably one of the most important aspects
of data modeling in ScyllaDB. It largely impacts which queries can be performed and how efficient they are.

.. note:: An empty string is *not* allowed as a partition key value. In a compound partition key (multiple partition-key columns), any or all of them may be empty strings. Empty string is *not* a Null value.


.. _clustering-columns:

The clustering columns
``````````````````````

The clustering columns of a table define the clustering order for the partition of that table. For a given
:ref:`partition <partition-key>`, all the rows are physically ordered inside ScyllaDB by that clustering order. For
instance, given::

    CREATE TABLE t (
        a int,
        b int,
        c int,
        PRIMARY KEY (a, b, c)
    );

    SELECT * FROM t;
       a | b | c
      ---+---+---
       0 | 0 | 4     // row 1
       0 | 1 | 9     // row 2
       0 | 2 | 2     // row 3
       0 | 3 | 3     // row 4

then the rows (which all belong to the same partition) are all stored internally in the order of the values of their
``b`` column (the order they are displayed above). So, where the partition keys of the table let you group rows on the
same replica set, the clustering columns control how those rows are stored on the replica. That sorting allows the
retrieval of a range of rows within a partition (for instance, in the example above, ``SELECT * FROM t WHERE a = 0 AND b
> 1 and b <= 3``) is very efficient.

.. note:: An empty string is allowed as a clustering key value. Empty string is *not* a Null value.


.. _create-table-options:

Table options
~~~~~~~~~~~~~

A CQL table has a number of options that can be set at creation (and, for most of them, :ref:`altered
<alter-table-statement>` later). These options are specified after the ``WITH`` keyword.

Amongst those options, two important ones cannot be changed after creation and influence which queries can be done
against the table: the ``COMPACT STORAGE`` option and the ``CLUSTERING ORDER`` option. Those, as well as the other
options of a table are described in the following sections.

.. _compact-tables:

Compact tables
``````````````

..
   .. caution:: Since Apache Cassandra 3.0, compact tables have the exact same layout internally than non compact ones (for the
      same schema obviously), and declaring a table compact **only** creates artificial limitations on the table definition
      and usage. It only exists for historical reason and is preserved for backward compatibility And as ``COMPACT
      STORAGE`` cannot, as of Apache Cassandra |version|, be removed, it is strongly discouraged to create new table with the
      ``COMPACT STORAGE`` option.

A *compact* table is one defined with the ``COMPACT STORAGE`` option. This option is only maintained for backward
compatibility for definitions created before CQL version 3 and shouldn't be used for new tables. Declaring a
table with this option creates limitations for the table, which are largely arbitrary (and exists for historical
reasons). Amongst these limitations:

- a compact table cannot use collections nor static columns.
- if a compact table has at least one clustering column, then it must have *exactly* one column outside of the primary
  key ones. This implies that you cannot add or remove columns in particular after creation.
- a compact table is limited as to the indexes it can create, and no materialized view can be created on it.

.. _clustering-order:

Reversing the clustering order
``````````````````````````````

The clustering order of a table is defined by the :ref:`clustering columns <clustering-columns>` of that table. By
default, that ordering is based on the natural order of the clustering order, but the ``CLUSTERING ORDER`` lets you
change that clustering order to use the *reverse* natural order for some (potentially all) of the columns.

The ``CLUSTERING ORDER`` option takes the comma-separated list of the clustering column, each with an ``ASC`` (for
*ascendant*, e.g. the natural order) or ``DESC`` (for *descendant*, e.g. the reverse natural order). Note in particular
that the default (if the ``CLUSTERING ORDER`` option is not used) is strictly equivalent to using the option with all
clustering columns using the ``ASC`` modifier.

Note that this option is basically a hint for the storage engine to change the order in which it stores the row, but it
has three visible consequences:

- it limits which ``ORDER BY`` clause is allowed for :ref:`selects <select-statement>` on that table. You can only
  order results by the clustering order or the reverse clustering order. Meaning that if a table has two clustering columns
  ``a`` and ``b``,  and you define ``WITH CLUSTERING ORDER (a DESC, b ASC)``, then in queries, you will be allowed to use
  ``ORDER BY (a DESC, b ASC)`` and (reverse clustering order) ``ORDER BY (a ASC, b DESC)`` but **not** ``ORDER BY (a
  ASC, b ASC)`` (nor ``ORDER BY (a DESC, b DESC)``).
- it also changes the default order of results when queried (if no ``ORDER BY`` is provided). Results are always returned
  in clustering order (within a partition).
- it has a small performance impact on some queries as queries in reverse clustering order are slower than the one in
  forward clustering order. In practice, this means that if you plan on querying mostly in the reverse natural order of
  your columns (which is common with time series, for instance, where you often want data from the newest to the oldest),
  it is an optimization to declare a descending clustering order.

.. _create-table-general-options:

Other table options
```````````````````

.. _todo: review (misses cdc if nothing else) and link to proper categories when appropriate (compaction, for instance)

A table supports the following options:

.. list-table::
   :widths: 20 10 10 60
   :header-rows: 1

   * - Option
     - Kind
     - Default
     - Description
   * - ``comment``
     - simple
     - none
     - A free-form, human-readable comment.
   * - ``speculative_retry``
     - simple
     - 99PERCENTILE
     - :ref:`Speculative retry options <speculative-retry-options>`
   * - ``gc_grace_seconds``
     - simple
     - 864000
     - Time to wait before garbage collecting tombstones (deletion markers). See :ref:`Tombstones GC options <ddl-tombstones-gc>`.
   * - ``tombstone_gc``
     - mode
     - see below
     - The mode of garbage collecting tombstones. See :ref:`Tombstones GC options <ddl-tombstones-gc>`.
   * - ``bloom_filter_fp_chance``
     - simple
     - 0.01
     - The target probability of false-positive of the sstable bloom filters. Sstable bloom filters will be sized to provide the provided probability (thus lowering this value impact the size of bloom filters in-memory and on-disk).
   * - ``default_time_to_live``
     - simple
     - 0
     - The default expiration time (“TTL”) in seconds for a table.
   * - ``memtable_flush_period_in_ms``
     - simple
     - 0
     - Flush the memtables associated with this table every ``memtable_flush_period_in_ms`` milliseconds. When set to ``0``, periodic flush is disabled. Cannot set to values lower than ``60000`` (1 minute). Default: ``0``.
   * - ``min_index_interval``
     - simple
     - 128
     - Minimum gap between summary entries: ScyllaDB will create summary entries with at least this amount of partitions between them. Controls the maximums density of the summary.
   * - ``max_index_interval``
     - simple
     - 2048
     - Not implemented (option value is ignored).
   * - ``compaction``
     - map
     - see below
     - :ref:`Compaction options <cql-compaction-options>`
   * - ``compression``
     - map
     - see below
     - :ref:`Compression options <cql-compression-options>`
   * - ``caching``
     - map
     - see below
     - :ref:`Caching Options <cql-caching-options>`
   * - ``cdc``
     - map
     - see below
     - :ref:`CDC Options <cdc-options>`


.. _speculative-retry-options:

Speculative retry options
#########################

By default, ScyllaDB read coordinators only query as many replicas as necessary to satisfy
consistency levels: one for consistency level ``ONE``, a quorum for ``QUORUM``, and so on.
``speculative_retry`` determines when coordinators may query additional replicas, which is useful
when replicas are slow or unresponsive.  The following are legal values (case-insensitive):

========================= ================ =============================================================================
 Format                    Example          Description
========================= ================ =============================================================================
 ``XPERCENTILE``           90.5PERCENTILE   Coordinators record average per-table response times for all replicas.
                                            If a replica takes longer than ``X`` percent of this table's average
                                            response time, the coordinator queries an additional replica.
                                            ``X`` must be between 0 and 100.
 ``XP``                    90.5P            Synonym for ``XPERCENTILE``
 ``Yms``                   25ms             If a replica takes more than ``Y`` milliseconds to respond,
                                            the coordinator queries an additional replica.
 ``ALWAYS``                                 Coordinators always query all replicas.
 ``NONE``                                   Coordinators never query additional replicas.
========================= ================ =============================================================================

This setting does not affect reads with consistency level ``ALL`` because they already query all replicas.

Note that frequently reading from additional replicas can hurt cluster performance.
When in doubt, keep the default ``99PERCENTILE``.


.. _cql-compaction-options:

Compaction options
##################

The ``compaction`` options must at least define the ``'class'`` sub-option, which defines the compaction strategy class
to use. The default supported class are ``'SizeTieredCompactionStrategy'``,
``'LeveledCompactionStrategy'``, and ``'IncrementalCompactionStrategy'``.
Custom strategy can be provided by specifying the full class name as a :ref:`string constant
<constants>`.

All default strategies support a number of common options, as well as options specific to
the strategy chosen (see the section corresponding to your strategy for details: :ref:`STCS <stcs-options>`, :ref:`LCS <lcs-options>`, and :ref:`TWCS <twcs-options>`).

.. _cql-compression-options:

Compression options
###################

The ``compression`` options define if and how the sstables of the table are compressed. The following sub-options are
available:

========================= =============== =============================================================================
 Option                    Default         Description
========================= =============== =============================================================================
 ``sstable_compression``   LZ4Compressor   The compression algorithm to use. Available compressors are
                                           LZ4Compressor, SnappyCompressor, DeflateCompressor, and ZstdCompressor.
 ``chunk_length_in_kb``    4               On disk SSTables are compressed by block (to allow random reads). This
                                           defines the size (in KB) of the block. Bigger values may improve the
                                           compression rate, but increases the minimum size of data to be read from disk
                                           for a read. Allowed values are powers of two between 1 and 128.
 ``crc_check_chance``      1.0             Not implemented (option value is ignored).
========================= =============== =============================================================================

.. crc_check_chance was promoted to a top-level table option since Cassandra 3.0, but we didn't do this.

For example, to enable compression:

.. code-block:: console

   
   CREATE TABLE id (id int PRIMARY KEY) WITH compression = {'sstable_compression': 'LZ4Compressor'};


For example, to disable compression:

.. code-block:: console

   CREATE TABLE id (id int PRIMARY KEY) WITH compression = {};


.. _cdc-options:

CDC options
###########

The following options can be used with Change Data Capture.

+---------------------------+-----------------+------------------------------------------------------------------------------------------------------------------------+
| option                    |  default        | description                                                                                                            |
+===========================+=================+========================================================================================================================+
| ``enabled``               | ``false``       | When set to ``true``, another table --- the CDC log table --- is created and associated with the table you are         |
|                           |                 | creating/altering (for example, customer_data). All writes made to this table (customer_data) are reflected in         |
|                           |                 | the corresponding CDC log table.                                                                                       |
+---------------------------+-----------------+------------------------------------------------------------------------------------------------------------------------+
| ``preimage``              | ``false``       | When set to ``true``, it saves the result of what a client performing a write would display if it has queried this     |
|                           |                 | table before making the write into the corresponding CDC log table.                                                    |
+---------------------------+-----------------+------------------------------------------------------------------------------------------------------------------------+
| ``ttl``                   | 86400 seconds   | Time after which data stored in CDC will be removed and won’t be accessible to the client anymore.                     |
|                           | 24 hours        |                                                                                                                        |
+---------------------------+-----------------+------------------------------------------------------------------------------------------------------------------------+

For example:

.. code-block:: cql

    CREATE TABLE customer_data (
        cust_id uuid,
        cust_first-name text,
        cust_last-name text,
        cust_phone text,
        cust_get-sms text,
        PRIMARY KEY (customer_id)
    ) WITH cdc = { 'enabled' : 'true', 'preimage' : 'true' };

.. _cql-caching-options:

Caching options
###############

Caching optimizes cache memory usage of a table. The cached data is weighed by size and access frequency.

+---------------------------+-----------------+------------------------------------------------------------------------------------------------------------------------+
| option                    |  default        | description                                                                                                            |
+===========================+=================+========================================================================================================================+
| ``enabled``               | ``TRUE``        | When set to TRUE enables caching on the specified table. Valid options are TRUE and FALSE.                             |
+---------------------------+-----------------+------------------------------------------------------------------------------------------------------------------------+


For example,

.. code-block:: cql

   CREATE TABLE caching (
                    k int PRIMARY KEY,
                    v1 int,
                    v2 int,
                ) WITH caching = {'enabled': 'true'};

.. _ddl-tombstones-gc:
.. _gc_grace_seconds:

Tombstones GC options
#######################

ScyllaDB inherited the ``gc_grace_seconds`` option from Apache Cassandra. The option allows you to specify the wait time 
(in seconds) before data marked with a deletion :term:`tombstone` is removed via compaction.
This option assumes that you run :term:`repair` during the specified time. Failing to run repair during the wait 
time may result in the resurrection of deleted data.

The ``tombstone_gc`` option allows you to prevent data resurrection. With the ``repair`` mode configured, :term:`tombstone` 
are only removed after :term:`repair` is performed. Unlike  ``gc_grace_seconds``, ``tombstone_gc`` has no time constraints - when 
the ``repair`` mode is on, tombstones garbage collection will wait until repair is run. For tables which use tablets ``repair``
mode is set by default.

You can enable the after-repair tombstone GC by setting the ``repair`` mode using 
``ALTER TABLE`` or ``CREATE TABLE``. For example:

.. code-block:: cql

    CREATE TABLE ks.cf (key blob PRIMARY KEY,  val blob) WITH tombstone_gc = {'mode':'repair'};

.. code-block:: cql

    ALTER TABLE ks.cf WITH tombstone_gc = {'mode':'repair'} ;

The following modes are available:

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Mode
     - Description
   * - ``timeout``
     - Tombstone GC is performed after the wait time specified with ``gc_grace_seconds`` (default).
   * - ``repair``
     - Tombstone GC is performed after repair is run.
   * - ``disabled``
     - Tombstone GC is never performed. This mode may be useful when loading data to the database, to avoid tombstone GC when part of the data is not yet available.
   * - ``immediate``
     - Tombstone GC is immediately performed. There is no wait time or repair requirement. This mode is useful for a table that uses the TWCS compaction strategy with no user deletes. After data is expired after TTL, ScyllaDB can perform compaction to drop the expired data immediately.

Other considerations:
#####################

- Adding new columns (see ``ALTER TABLE`` below) is a constant time operation. There is thus no need to try to
  anticipate future usage when creating a table.

.. _ddl-per-parition-rate-limit:

Limiting the rate of requests per partition
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can limit the read rates and writes rates into a partition by applying 
a ScyllaDB CQL extension to the CREATE TABLE or ALTER TABLE statements. 
See `Per-partition rate limit <https://docs.scylladb.com/stable/cql/cql-extensions.html#per-partition-rate-limit>`_ 
for details.

 .. REMOVE IN FUTURE VERSIONS - Remove the URL above (temporary solution) and replace it with a relative link (once the solution is applied).

.. _alter-table-statement:

ALTER TABLE
^^^^^^^^^^^

Altering an existing table uses the ``ALTER TABLE`` statement:

.. code-block:: cql

   alter_table_statement: ALTER TABLE `table_name` `alter_table_instruction`
   alter_table_instruction: ADD `column_name` `cql_type` ( ',' `column_name` `cql_type` )*
                          : | DROP `column_name` [ USING TIMESTAMP `timestamp` ]
                          : | DROP '(' `column_name` ( ',' `column_name` )* ')' [ USING TIMESTAMP `timestamp` ]
                          : | ALTER `column_name` TYPE `cql_type`
                          : | WITH `options`
                          : | scylla_encryption_options: '=' '{'[`cipher_algorithm` : <hash>]','[`secret_key_strength` : <len>]','[`key_provider`: <provider>]'}'

For instance:

.. code-block:: cql

    ALTER TABLE addamsFamily ADD gravesite varchar;

    ALTER TABLE addamsFamily
           WITH comment = 'A most excellent and useful table';


    ALTER TABLE data_atrest (
        pk text PRIMARY KEY, 
        c0 int
    ) WITH scylla_encryption_options = {  
       'cipher_algorithm' : 'AES/ECB/PKCS5Padding', 
       'secret_key_strength' : 128,  
       'key_provider': 'LocalFileSystemKeyProviderFactory',  
       'secret_key_file': '/etc/scylla/data_encryption_keys/secret_key'};

    ALTER TABLE customer_data 
       WITH cdc = { 'enabled' : 'true', 'preimage' : 'true' };

The ``ALTER TABLE`` statement can:

- Add new column(s) to the table (through the ``ADD`` instruction). Note that the primary key of a table cannot be
  changed, and thus newly added column will, by extension, never be part of the primary key. Also, note that :ref:`compact
  tables <compact-tables>` have restrictions regarding column addition. Note that this is constant (in the amount of
  data the cluster contains) time operation.
- Remove column(s) from the table. This drops both the column and all its content, but note that while the column
  becomes immediately unavailable, its content is only removed lazily during compaction. Please also note the warnings
  below. Due to lazy removal, the altering itself is a constant (in the amount of data removed or contained in the
  cluster) time operation.
- Change data type of the column to a compatible type.
- Change some of the table options (through the ``WITH`` instruction). The :ref:`supported options
  <create-table-options>` are the same that when creating a table (outside of ``COMPACT STORAGE`` and ``CLUSTERING
  ORDER`` that cannot be changed after creation). Note that setting any ``compaction`` sub-options has the effect of
  erasing all previous ``compaction`` options, so you need to re-specify all the sub-options if you want to keep them.
  The same note applies to the set of ``compression`` sub-options.
- Change or add any of the ``Encryption options`` above.
- Change or add any of the :ref:`CDC options <cdc-options>` above.
- Change or add per-partition rate limits. See :ref:`Limiting the rate of requests per partition <ddl-per-parition-rate-limit>`.

.. warning:: Dropping a column assumes that the timestamps used for the value of this column are "real" timestamp in
   microseconds. Using "real" timestamps in microseconds is the default is and is **strongly** recommended, but as
   ScyllaDB allows the client to provide any timestamp on any table, it is theoretically possible to use another
   convention. Please be aware that if you do so, dropping a column will not work correctly.

.. warning:: Once a column is dropped, it is allowed to re-add a column with the same name as the dropped one
   **unless** the type of the dropped column was a (non-frozen) column (due to an internal technical limitation).

It is also possible to drop a column with specified timestamp ``ALTER TABLE ... DROP ... USING TIMESTAMP ...``.
The purpose of this statement is to be able to safely restore schema (see :doc:`Backup and Restore Procedures </operating-scylla/procedures/backup-restore/index>`) in the case a column was dropped and re-added later.
The timestamp should be obtained by describing schema with internals ``DESC SCHEMA WITH INTERNALS`` (or other descriptions like ``DESC TABLE ks.cf WITH INTERNALS``)

For example: 
Let's say you have a table with some data. Then you drop one of the column and re-add it later.
In the future, when you wish to restore the schema, you **have to** also drop the column with specified timestamp (the same timestamp as the original drop)
and re-add it again.
Otherwise, you can resurrect your data (if you skip ``ALTER ... DROP/ADD ...`` entirely) 
or you can lose data inserted after column re-addition (if you drop the column without the timestamp).

.. warning:: Dropping a column with specified timestamp should only be used to restore schema from description (``DESCRIBE SCHEMA WITH INTERNALS``).

.. _drop-table-statement:

DROP TABLE
^^^^^^^^^^

Dropping a table uses the ``DROP TABLE`` statement:

.. code-block::
   
   drop_table_statement: DROP TABLE [ IF EXISTS ] `table_name`

Dropping a table results in the immediate removal of the table, including all data it contains and any associated secondary indexes.

.. include:: /getting-started/_common/note-reclaim-space.rst

If the table does not exist, the statement will return an error unless ``IF EXISTS`` is used, in which case the
operation is a no-op.

.. note:: Dropping a table that has materialized views is disallowed and will return an error. To do so, the materialized views that depend on the table must first be explicitly dropped. Refer to :doc:`Materialized Views </cql/mv>` for details.

.. _truncate-statement:

TRUNCATE
^^^^^^^^

A table can be truncated using the ``TRUNCATE`` statement:

.. code-block::
   
   truncate_statement: TRUNCATE [ TABLE ] `table_name`
                     : [ USING TIMEOUT `timeout` ]
   timeout: `duration`

Note that ``TRUNCATE TABLE foo`` is allowed for consistency with other DDL statements, but tables are the only object
that can be truncated currently and so the ``TABLE`` keyword can be omitted.

Truncating a table permanently removes all existing data from the table, but without removing the table itself.

The ``USING TIMEOUT`` clause allows specifying a timeout for a specific request.

For example::

  TRUNCATE TABLE users USING TIMEOUT 5m;

.. caution:: Do not run any operation on a table that is being truncated. Truncate operation is an administrative operation, and running any other operation on the same table in parallel may cause the truncating table's data to end up in an undefined state.

* :doc:`Apache Cassandra Query Language (CQL) Reference </cql/index>`

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
