=====================
Advanced column types
=====================

Prerequisites: we assume that the reader is familiar with the :doc:`CDC log table <./cdc-log-table>` and :doc:`Basic operations in CDC <./cdc-basic-operations>` documents.

As explained in :doc:`CDC log table <./cdc-log-table>`, each *atomic* non-primary-key column of the base table gets two corresponding columns in the log table:

* one with the same name and type; we call this the `value column`,
* one with the original name prefixed with ``cdc$deleted_`` and of ``boolean`` type; we call this the `deletion column`.

`Atomic` means any column whose type is not a non-frozen collection, non-frozen UDT, or a counter. Examples of atomic column types are: ``int``, ``timeuuid``, ``text``, ``frozen<map<int, int>>``, ``frozen<my_type>`` (where ``my_type`` was created using a ``CREATE TYPE`` statement). Examples of non-atomic types are: ``map<int, int>``, ``set<int>``, ``my_type``.

These columns are filled in CDC log delta rows created for ``UPDATE``, ``INSERT``, and column ``DELETE`` statements, as explained in the :doc:`basic operations <./cdc-basic-operations>` document. If the statement writes a non-null value to the column, its corresponding value column in the delta row gets filled with that exact value. If the statement writes a ``null``, its corresponding deletion column in the delta row gets filled with ``True``.

For example:

.. code-block:: cql

   CREATE TABLE ks.t (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH cdc = {'enabled': true};
   UPDATE ks.t SET v = 0 WHERE pk = 0 AND ck = 0;
   UPDATE ks.t SET v = null WHERE pk = 0 AND ck = 0;
   SELECT "cdc$time", pk, ck, v, "cdc$deleted_v" from ks.t_scylla_cdc_log;

gives:

.. code-block:: none

     cdc$time                             | pk | ck | v    | cdc$deleted_v
    --------------------------------------+----+----+------+---------------
     9d764f58-2f32-11eb-ac58-84f095630d16 |  0 |  0 |    0 |          null
     9d768b58-2f32-11eb-5cf2-f8131733ca0b |  0 |  0 | null |          True

The situation is more complicated for `non-atomic` columns. Each different case has its own section below.

.. _cdc_maps:

Maps
----

**Note**: everything in this section applies only to `non-frozen` maps. Frozen maps are atomic and behave in the same way as other atomic types (such as `int`). The same comment applies to sets and lists in sections below.

Let's start with an example.

.. code-block:: cql

   CREATE TABLE ks.t (pk int, ck int, v map<int, text>, PRIMARY KEY (pk, ck)) WITH cdc = {'enabled': true};
   DESCRIBE TABLE ks.t_scylla_cdc_log;

gives:

.. code-block:: none

   CREATE TABLE ks.t_scylla_cdc_log (
       ...
       "cdc$deleted_elements_v" frozen<set<int>>,
       "cdc$deleted_v" boolean,
       ...
       v frozen<map<int, text>>,
       ...

We replaced the irrelevant lines with ``...``. The important information for us is the set of columns created for the ``v map<int, text>`` column; there are three:

* ``v``: it has the same name as the corresponding base column and its type is a ``frozen`` version of the base column's type; the base type was ``map<int, text>``, so this column's type is ``frozen<map<int, text>>``,
* ``cdc$deleted_v``: its name is the base column's name prefixed with ``cdc$deleted_``, and its type is ``boolean``,
* ``cdc$deleted_elements_v``: its name is the base column's name prefixed with ``cdc$deleted_elements_``, and its type is a frozen set whose element type is the base column's key type. Since the base column's type was ``map<int, text>``, meaning that its key type is ``int``, we obtained ``frozen<set<int>>``.

The general rule is: for each column ``X map<K, V>`` in the base table, where ``X`` is a column name and ``K``, ``V`` are types, the CDC log table has 3 corresponding columns:

* ``X frozen<map<K, V>>``, which we call the `added elements column`,
* ``cdc$deleted_X boolean``, which we call the `collection deletion column`,
* ``cdc$deleted_elements_X frozen<set<K>>``, which we call the `deleted elements column`.

Adding elements
+++++++++++++++

The added elements column is filled when new elements are added to the map. Continuing our example:

.. code-block:: cql

   UPDATE ks.t SET v = v + {1: 'v1', 2: 'v2'} WHERE pk = 0 AND ck = 0;
   SELECT "cdc$time", pk, ck, v, "cdc$deleted_v", "cdc$deleted_elements_v" FROM ks.t_scylla_cdc_log;

result:

.. code-block:: none

     cdc$time                             | pk | ck | v                  | cdc$deleted_v | cdc$deleted_elements_v
    --------------------------------------+----+----+--------------------+---------------+------------------------
     e3ef7952-2f3f-11eb-dc8e-210f916d0a5b |  0 |  0 | {1: 'v1', 2: 'v2'} |          null |                   null

Deleting elements
+++++++++++++++++

The deleted elements column is filled when elements are removed from the map:

.. code-block:: cql

   UPDATE ks.t SET v = v - {1, 2, 3} WHERE pk = 0 AND ck = 0;
   SELECT "cdc$time", pk, ck, v, "cdc$deleted_v", "cdc$deleted_elements_v" FROM ks.t_scylla_cdc_log;

result:

.. code-block:: none

     cdc$time                             | pk | ck | v    | cdc$deleted_v | cdc$deleted_elements_v
    --------------------------------------+----+----+------+---------------+------------------------
     5bb26094-2f40-11eb-63e9-a0d4519f9c1b |  0 |  0 | null |          null |              {1, 2, 3}

Note that the elements don't need to exist to be removed. Removing an element is expressed by adding a special ``tombstone`` value (as usual in ScyllaDB) under the given key. Thus, we can understand the ``cdc$deleted_elements_X`` column as showing the set of keys which were assigned tombstones in the corresponding statement. Recall that a tombstone removes a value if its timestamp is greater than or equal to the value's timestamp.

Deleting values for specific keys in CQL as above can only be done using an ``UPDATE`` statement with the ``- {...}`` notation.

Deleting the entire map
+++++++++++++++++++++++

The collection deletion column is set to ``True`` when a `collection-wide` tombstone is created; the tombstone removes `all` elements with timestamps lower than or equal to the tombstone's timestamp. To delete an entire collection, either set the entire collection value to ``null``, or to ``{}``; both methods have the same effect:

.. code-block:: cql

   UPDATE ks.t SET v = null WHERE pk = 0 AND ck = 0;
   UPDATE ks.t SET v = {} WHERE pk = 0 AND ck = 0;
   SELECT "cdc$time", pk, ck, v, "cdc$deleted_v", "cdc$deleted_elements_v" FROM ks.t_scylla_cdc_log;

result:

.. code-block:: none

     cdc$time                             | pk | ck | v    | cdc$deleted_v | cdc$deleted_elements_v
    --------------------------------------+----+----+------+---------------+------------------------
     2fcc5e48-2f41-11eb-eb46-b59d059991a9 |  0 |  0 | null |          True |                   null
     2fcca81c-2f41-11eb-2332-1c30f8a2f717 |  0 |  0 | null |          True |                   null

Overwriting maps
++++++++++++++++

A single ``UPDATE`` statement can be used to `overwrite` a collection using the ``= {...}`` notation. Overwriting is the same as creating a collection-wide tombstone and adding new elements at the same time. In other words, the following statement:

.. code-block:: cql

   UPDATE ks.t SET v = {1: 'v1', 2: 'v2'} WHERE pk = 0 AND ck = 0;

is equivalent to the following:

.. code-block:: cql

   BEGIN UNLOGGED BATCH;
       UPDATE ks.t SET v = {} WHERE pk = 0 AND ck = 0;
       UPDATE ks.t SET v = v + {1: 'v1', 2: 'v2'} WHERE pk = 0 AND ck = 0;
   APPLY BATCH;

CDC understands this equivalence. Overwriting a map column ``X`` is expressed in CDC by setting both the ``X`` and ``cdc$deleted_X`` columns. For example:

.. code-block:: cql

   BEGIN UNLOGGED BATCH
       UPDATE ks.t SET v = {} WHERE pk = 0 AND ck = 0;
       UPDATE ks.t SET v = v + {1: 'v1', 2: 'v2'} WHERE pk = 0 AND ck = 0;
   APPLY BATCH;
   SELECT "cdc$time", pk, ck, v, "cdc$deleted_v", "cdc$deleted_elements_v" FROM ks.t_scylla_cdc_log;

result:

.. code-block:: none

     cdc$time                             | pk | ck | v                  | cdc$deleted_v | cdc$deleted_elements_v
    --------------------------------------+----+----+--------------------+---------------+------------------------
     555a4034-2f42-11eb-3e76-a6bbe7fa31b2 |  0 |  0 | {1: 'v1', 2: 'v2'} |          True |                   null

The same result would be obtained if we used a single ``SET v = {1: 'v1', 2: 'v2'}`` statement.

Deleting a map can be understood as a special case of overwriting with 0 elements.

``INSERT`` statements can only overwrite (or delete) maps; with ``INSERT``, there is no way to add elements to a map without overwriting it, since there is no notation for ``INSERT`` that would correspond to the ``UPDATE``-specific ``SET X = X + {...}`` notation. There is also no way to remove specific keys with an ``INSERT``.

Example of using an ``INSERT``, compared with an ``UPDATE``:

.. code-block:: cql

   INSERT INTO ks.t (pk, ck, v) VALUES (0, 0, {1: 'v1', 2: 'v2'});
   UPDATE ks.t SET v = {1: 'v1', 2: 'v2'} WHERE pk = 0 AND ck = 0;
   SELECT "cdc$time", pk, ck, v, "cdc$deleted_v", "cdc$deleted_elements_v", "cdc$operation" FROM ks.t_scylla_cdc_log;

result:

.. code-block:: none

     cdc$time                             | pk | ck | v                  | cdc$deleted_v | cdc$deleted_elements_v | cdc$operation
    --------------------------------------+----+----+--------------------+---------------+------------------------+---------------
     90dd349e-2f43-11eb-fce8-46a663386323 |  0 |  0 | {1: 'v1', 2: 'v2'} |          True |                   null |             2
     90dd91b4-2f43-11eb-7ee7-e37ab84122d4 |  0 |  0 | {1: 'v1', 2: 'v2'} |          True |                   null |             1

As we can see, the effect on the ``v``-related columns for ``INSERT`` is the same as for ``UPDATE``. Which statement was used can be determined by the ``cdc$operation`` column. The exact difference between the two types of statements was explained in the :doc:`basic operations <./cdc-basic-operations>` document.

.. _cdc_collection_tombstones:

Collection-wide tombstones and timestamps
-----------------------------------------

The examples in this section use maps, but the discussion applies to **all** non-frozen collections: maps, sets, and lists, and even to non-frozen UDTs.

Executing the following statements:

.. code-block:: cql

   BEGIN UNLOGGED BATCH
       UPDATE ks.t SET v = v + {1: 'v1', 2: 'v2'} WHERE pk = 0 AND ck = 0;
       UPDATE ks.t SET v = {} WHERE pk = 0 AND ck = 0;
   APPLY BATCH;
   SELECT * from ks.t;

is equivalent to:

.. code-block:: cql

   UPDATE ks.t SET v = {1: 'v1', 2: 'v2'} WHERE pk = 0 AND ck = 0;
   SELECT * from ks.t;

and gives:

.. code-block:: none

     pk | ck | v
    ----+----+--------------------
      0 |  0 | {1: 'v1', 2: 'v2'}

We've explained that the ``SET v = {...}`` notation creates a `collection-wide tombstone`, and tombstones delete all values that have timestamps lower than or equal to the tombstone's timestamp. How is it then possible to both delete a collection and add elements to it in the same statement?

Each CQL statement that arrives to ScyllaDB comes with a timestamp (or multiple timestamps, in case of specially constructed batches, but that's rare); generally, it is the timestamp that's assigned to the written data. However, collection-wide tombstones written by ``UPDATE ... SET X = {...}`` statements or ``INSERT`` statements are an exception.

The rule is as follows:

    Given an ``UPDATE`` or an ``INSERT`` statement, if the timestamp of this statement is ``T``, then the timestamp of collection-wide tombstones written by this statement is ``T - 1``.

This is what makes the above behavior possible. Suppose that the statement

.. code-block:: cql

   UPDATE ks.t SET v = {1: 'v1', 2: 'v2'} WHERE pk = 0 AND ck = 0;

has timestamp ``T``. It is translated by ScyllaDB into 3 pieces of information: an element ``(1, 'v1')`` with timestamp ``T``, an element ``(2, 'v2')`` with timestamp ``T``, and a collection-wide tombstone with timestamp ``T-1``. The tombstone will therefore remove all elements that have timestamps lower than or equal to ``T - 1``, but will not remove the elements ``(1, 'v1'), (2, 'v2')``, since their timestamps are greater.

**Warning**: this rule **does not** apply when deleting collections using a column ``DELETE``. In that case, the original timestamp is used. The following example illustrates that:

.. code-block:: cql

   BEGIN UNLOGGED BATCH
       DELETE v FROM ks.t WHERE pk = 0 AND ck = 0;
       UPDATE ks.t SET v = v + {1: 'v1', 2: 'v2'} WHERE pk = 0 AND ck = 0;
   APPLY BATCH;
   SELECT * from ks.t;

gives:

.. code-block:: none

     pk | ck | v
    ----+----+---

    (0 rows)

In this example, the elements ``(1, 'v1'), (2, 'v2')`` were given the same timestamp as the tombstone coming from the ``DELETE`` statement (by default, all statements in a ``BATCH`` are given the same timestamp), hence they were deleted by that tombstone.

CDC takes this rule into account when calculating the value of the ``cdc$time`` column. Recall from the :doc:`CDC log table <./cdc-log-table>` document that the ``cdc$time`` column is a `timeuuid` that contains a timestamp. Usually, when we write a cell to the base table, a corresponding row in the CDC log table appears whose ``cdc$time`` is a timeuuid whose timestamp is equal to the timestamp of the written cell. However, collection-wide tombstones are an exception; because of the above rule, the CDC log row which informs about the tombstone (i.e. one which has ``cdc$deleted_X`` column set to ``True``) has ``cdc$time`` such that its timestamp is the timestamp of the collection-wide tombstone `increased by 1`.

We already saw this phenomenon before. Consider the following example, where we pick the timestamp manually:

.. code-block:: cql

   UPDATE ks.t USING TIMESTAMP 1606390225588947 SET v = {1: 'v1', 2: 'v2'} WHERE pk = 0 AND ck = 0;
   SELECT "cdc$time", pk, ck, v, "cdc$deleted_v" FROM ks.t_scylla_cdc_log;

result:

.. code-block:: none

     cdc$time                             | pk | ck | v                  | cdc$deleted_v
    --------------------------------------+----+----+--------------------+---------------
     c72c7c3e-2fda-11eb-ba1d-e2d8e9bb0299 |  0 |  0 | {1: 'v1', 2: 'v2'} |          True

There is a `single row` that informs both about the collection-wide tombstone and the elements ``(1, 'v1'), (2, 'v2')`` written by the ``UPDATE``, even though the tombstone and the elements have different timestamps, as previously explained. The timestamp of this timeuuid in microseconds (which we can obtain using the Python snippet from the aforementioned document) is ``1606390225588947``, i.e. the one we picked. By our previous discussion, this means that the timestamp of the elements is ``1606390225588947`` and the timestamp of the collection-wide tombstone is ``1606390225588946``.

This is how CDC compensates for the collection-wide tombstone rule. If it didn't, i.e. if it instead always enforced that ``cdc$time``'s timestamp is the same as the timestamp of all elements and tombstones of the base write, the above example would have to produce two rows with different ``cdc$time`` values, one for the collection-wide tombstone and one for the elements.

This compensation may introduce confusion when using column deletes, which are an exception to the rule. When we delete a collection column using a ``DELETE`` statement, the original timestamp of the write is used, but `CDC still applies the compensation`, i.e. the ``cdc$time``'s timestamp is then equal to the timestamp of the ``DELETE`` increased by 1:

.. code-block:: cql

   DELETE v FROM ks.t USING TIMESTAMP 1606390225588947 WHERE pk = 0 AND ck = 0;
   SELECT "cdc$time", pk, ck, v, "cdc$deleted_v" FROM ks.t_scylla_cdc_log;

result:

.. code-block:: none

     cdc$time                             | pk | ck | v    | cdc$deleted_v
    --------------------------------------+----+----+------+---------------
     c72c7c48-2fda-11eb-1510-6f4271ad8200 |  0 |  0 | null |          True

the timestamp of this timeuuid is ``1606390225588948``, i.e. it is the timestamp used in the delete statement increased by 1.

Another example:

.. code-block:: cql

   BEGIN UNLOGGED BATCH
       DELETE v FROM ks.t USING TIMESTAMP 1606390225588946 WHERE pk = 0 AND ck = 0;
       UPDATE ks.t USING TIMESTAMP 1606390225588947 SET v = v + {1: 'v1', 2: 'v2'} WHERE pk = 0 AND ck = 0;
   APPLY BATCH;
   SELECT "cdc$time", pk, ck, v, "cdc$deleted_v" FROM ks.t_scylla_cdc_log;

result:

.. code-block:: none

     cdc$time                             | pk | ck | v                  | cdc$deleted_v
    --------------------------------------+----+----+--------------------+---------------
     c72c7c3e-2fda-11eb-b78e-840de9f2b0dd |  0 |  0 | {1: 'v1', 2: 'v2'} |          True

Sets
----

Non-frozen sets behave similarly to non-frozen maps. In fact, we can think of them as special cases of maps, where the type of the value is a `unit` type, i.e. a type with exactly one possible value. The only difference in CDC is the type of the of the added elements column in the CDC log table; instead of a frozen map, we're using a frozen set.

For example:

.. code-block:: cql

   CREATE TABLE ks.t (pk int, ck int, v set<int>, PRIMARY KEY (pk, ck)) WITH cdc = {'enabled': true};
   DESCRIBE TABLE ks.t_scylla_cdc_log;

gives:

.. code-block:: none

    CREATE TABLE ks.t_scylla_cdc_log (
        ...
        "cdc$deleted_elements_v" frozen<set<int>>,
        "cdc$deleted_v" boolean,
        ...
        v frozen<set<int>>,
        ...

For each column ``X set<K>`` in the base table, where ``X`` is a column name and ``K`` is a type, the CDC log table has 3 corresponding columns:

* ``X frozen<set<K>>``, the `added elements column`,
* ``cdc$deleted_X boolean``, the `collection deletion column`,
* ``cdc$deleted_elements_X frozen<set<K>>``, the `deleted elements column`.

Adding elements
+++++++++++++++

The `added elements` column describes new elements added to the set:

.. code-block:: cql

   UPDATE ks.t SET v = v + {1, 2} WHERE pk = 0 AND ck = 0;
   SELECT "cdc$time", pk, ck, v, "cdc$deleted_v", "cdc$deleted_elements_v" FROM ks.t_scylla_cdc_log;

result:

.. code-block:: none

     cdc$time                             | pk | ck | v      | cdc$deleted_v | cdc$deleted_elements_v
    --------------------------------------+----+----+--------+---------------+------------------------
     d67d19b2-2fe1-11eb-d2a2-1f69144504ce |  0 |  0 | {1, 2} |          null |                   null


Deleting elements
+++++++++++++++++

The `deleted elements` column describes elements that are removed from the set:

.. code-block:: cql

   UPDATE ks.t SET v = v - {1, 2, 3} WHERE pk = 0 AND ck = 0;
   SELECT "cdc$time", pk, ck, v, "cdc$deleted_v", "cdc$deleted_elements_v" FROM ks.t_scylla_cdc_log;

result:

.. code-block:: none

     cdc$time                             | pk | ck | v    | cdc$deleted_v | cdc$deleted_elements_v
    --------------------------------------+----+----+------+---------------+------------------------
     eef36438-2fe1-11eb-b7b0-c0a03f64d719 |  0 |  0 | null |          null |              {1, 2, 3}

Deleting and overwriting sets
+++++++++++++++++++++++++++++

The collection deletion column is set to ``True`` when a collection-wide tombstone is created. Furthermore, a single ``UPDATE`` or ``INSERT`` statement can be used to `overwrite` a set using the ``= {...}`` notation, which is the same as creating a collection-wide tombstone and adding new elements at the same time. The behavior is the same as for maps. Example for deleting follows:

.. code-block:: cql

   UPDATE ks.t SET v = null WHERE pk = 0 AND ck = 0;
   UPDATE ks.t SET v = {} WHERE pk = 0 AND ck = 0;
   SELECT "cdc$time", pk, ck, v, "cdc$deleted_v", "cdc$deleted_elements_v" FROM ks.t_scylla_cdc_log;

result:

.. code-block:: none

     cdc$time                             | pk | ck | v    | cdc$deleted_v | cdc$deleted_elements_v
    --------------------------------------+----+----+------+---------------+------------------------
     1c89ecc8-2fe2-11eb-f3d0-c8f925d280df |  0 |  0 | null |          True |                   null
     1c8a2850-2fe2-11eb-ad18-c31ba093947b |  0 |  0 | null |          True |                   null

Another example for overwriting:

.. code-block:: cql

   UPDATE ks.t SET v = {1, 2} WHERE pk = 0 AND ck = 0;
   SELECT "cdc$time", pk, ck, v, "cdc$deleted_v", "cdc$deleted_elements_v" FROM ks.t_scylla_cdc_log;

result:

.. code-block:: none

     cdc$time                             | pk | ck | v      | cdc$deleted_v | cdc$deleted_elements_v
    --------------------------------------+----+----+--------+---------------+------------------------
     78a546ec-2fe2-11eb-3dda-ce195f6906c6 |  0 |  0 | {1, 2} |          True |                   null

The rules for timestamps described in the :ref:`cdc_collection_tombstones` section apply.

Lists
-----

Non-frozen lists are possibly the weirdest types you can find in ScyllaDB (and Cassandra). Perhaps it's surprising when we say that non-frozen lists are also special cases of non-frozen maps; when querying tables that use lists, however, the `key` is hidden and only the values are shown. The type of the key used in the internal map representation of a list is ``timeuuid``.

Although you can't see list keys when using CQL read queries, you can `update` the value under any given key. For example:

.. code-block:: cql

   CREATE TABLE ks.t (pk int, ck int, v list<int>, PRIMARY KEY (pk, ck));
   UPDATE ks.t SET v[SCYLLA_TIMEUUID_LIST_INDEX(839e7120-2fe4-11eb-af55-000000000001)] = 0 WHERE pk = 0 AND ck = 0;

Thus, the syntax is:

.. code-block:: cql

   UPDATE ... SET X[SCYLLA_TIMEUUID_LIST_INDEX(k)] = v WHERE ...;

where ``X`` is the list column name, ``k`` is a timeuuid, and ``v`` is a value of the list's value type.

The keys define the order of elements in the list. When using the standard list update syntax (e.g. ``SET v = v + [1, 2]``), the timeuuids are automatically generated by ScyllaDB using the current time. This method allows fast, conflict-free concurrent updates to the list (such as appending or prepending elements). This list representation is a simple example of a `CRDT <https://en.wikipedia.org/wiki/Conflict-free_replicated_data_type>`_.

In the CDC log table, the key is revealed to the user. The design goals of CDC enforce this: the user should have the possibility of replaying `the exact same sequence of changes` to another ScyllaDB cluster in order to obtain the same result. In the case of lists, one can't allow the other cluster to generate the timeuuids since the resulting order of elements in the list may end up different; the user must specify the keys on their own, thus they must be able to learn what the keys are in the first place.

Let's start with an example:

.. code-block:: cql

   CREATE TABLE ks.t (pk int, ck int, v list<int>, PRIMARY KEY (pk, ck)) WITH cdc = {'enabled': true};
   DESCRIBE TABLE ks.t_scylla_cdc_log;

result:

.. code-block:: none

    CREATE TABLE ks.t_scylla_cdc_log (
        ...
        "cdc$deleted_elements_v" frozen<set<timeuuid>>,
        "cdc$deleted_v" boolean,
        ...
        v frozen<map<timeuuid, int>>,
        ...

After reading the :ref:`cdc_maps` section and the above discussion one shouldn't be surprised by the resulting types. If one remembers that ``list<V>`` is syntactic sugar for ``map<timeuuid, V>`` everything else should become obvious.

For each column ``X list<V>`` in the base table, where ``X`` is a column name and ``V`` is a type, the CDC log table has 3 corresponding columns:

* ``X frozen<map<timeuuid, V>>``, the `added elements column`,
* ``cdc$deleted_X boolean``, the `collection deletion column`,
* ``cdc$deleted_elements_X frozen<set<timeuuid>>``, the `deleted elements column`.

Adding elements
+++++++++++++++

The added elements column is filled when new elements are added to the list:

.. code-block:: cql

   UPDATE ks.t SET v = v + [1, 2] WHERE pk = 0 AND ck = 0;
   SELECT "cdc$time", pk, ck, v, "cdc$deleted_v", "cdc$deleted_elements_v" FROM ks.t_scylla_cdc_log;

result:

.. code-block:: none

     cdc$time                             | pk | ck | v                                                                                  | cdc$deleted_v | cdc$deleted_elements_v
    --------------------------------------+----+----+------------------------------------------------------------------------------------+---------------+------------------------
     db320398-2fe9-11eb-6e4b-cb8fd582dbb5 |  0 |  0 | {db3214f0-2fe9-11eb-af55-000000000001: 1, db3214f1-2fe9-11eb-af55-000000000001: 2} |          null |                   null

example of using the ``SCYLLA_TIMEUUID_LIST_INDEX`` syntax:

.. code-block:: cql

   UPDATE ks.t SET v[SCYLLA_TIMEUUID_LIST_INDEX(0dd381f0-2fea-11eb-af55-000000000001)] = 0 WHERE pk = 0 AND ck = 0;
   SELECT "cdc$time", pk, ck, v, "cdc$deleted_v", "cdc$deleted_elements_v" FROM ks.t_scylla_cdc_log;

result:

.. code-block:: none

     cdc$time                             | pk | ck | v                                         | cdc$deleted_v | cdc$deleted_elements_v
    --------------------------------------+----+----+-------------------------------------------+---------------+------------------------
     21c6a3fe-2fea-11eb-15ca-5fb980489b57 |  0 |  0 | {0dd381f0-2fea-11eb-af55-000000000001: 0} |          null |                   null

as we can see, the added element's key is equal to the timeuuid we've used in the ``UPDATE`` statement.

Deleting elements
+++++++++++++++++

The `deleted elements` column describes elements that are removed from the list. ScyllaDB offers the ``v = v - [...]`` syntax for removing all elements whose values appear in the provided list of values. To do this, ScyllaDB first performs a read, obtaining the set of all keys that contain the values from the provided list, and then writes a tombstone for each obtained key. For example:

.. code-block:: cql

   UPDATE ks.t SET v = v + [1, 2, 1, 3] WHERE pk = 0 AND ck = 0;
   UPDATE ks.t SET v = v - [1] WHERE pk = 0 AND ck = 0;
   SELECT "cdc$time", pk, ck, v, "cdc$deleted_v", "cdc$deleted_elements_v" FROM ks.t_scylla_cdc_log;

result:

.. code-block:: none

     cdc$time                             | pk | ck | v                                                                                                                                                                    | cdc$deleted_v | cdc$deleted_elements_v
    --------------------------------------+----+----+----------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------------+------------------------------------------------------------------------------
     cc5bb172-2fec-11eb-2396-140c0c098ef0 |  0 |  0 | {cc5baec0-2fec-11eb-af55-000000000001: 1, cc5baec1-2fec-11eb-af55-000000000001: 2, cc5baec2-2fec-11eb-af55-000000000001: 1, cc5baec3-2fec-11eb-af55-000000000001: 3} |          null |                                                                         null
     cc5bfc40-2fec-11eb-24e1-0da297c23c66 |  0 |  0 |                                                                                                                                                                 null |          null | {cc5baec0-2fec-11eb-af55-000000000001, cc5baec2-2fec-11eb-af55-000000000001}

We can see how the deleted keys in the second row are the same as the keys that were assigned to the list elements with value ``1`` in the first row. The resulting list is ``[2, 3]``, represented by the map ``{cc5baec1-2fec-11eb-af55-000000000001: 2, cc5baec3-2fec-11eb-af55-000000000001: 3}`` underneath.

You can also delete the value under a specific key:

.. code-block:: cql

   UPDATE ks.t SET v[SCYLLA_TIMEUUID_LIST_INDEX(cc5baec0-2fec-11eb-af55-000000000001)] = null WHERE pk = 0 AND ck = 0;
   SELECT "cdc$time", pk, ck, v, "cdc$deleted_v", "cdc$deleted_elements_v" FROM ks.t_scylla_cdc_log;

result:

.. code-block:: none

     cdc$time                             | pk | ck | v    | cdc$deleted_v | cdc$deleted_elements_v
    --------------------------------------+----+----+------+---------------+----------------------------------------
     23158f88-2fed-11eb-0968-538fd19b1920 |  0 |  0 | null |          null | {cc5baec0-2fec-11eb-af55-000000000001}

Deleting and overwriting lists
++++++++++++++++++++++++++++++

The collection deletion column is set to ``True`` when a collection-wide tombstone is created. Furthermore, a single ``UPDATE`` or ``INSERT`` statement can be used to `overwrite` a list using the ``= [...]`` notation, which is the same as creating a collection-wide tombstone and adding new elements at the same time. The behavior is the same as for maps. Example for deleting:

.. code-block:: cql

   UPDATE ks.t SET v = null WHERE pk = 0 AND ck = 0;
   UPDATE ks.t SET v = [] WHERE pk = 0 AND ck = 0;
   SELECT "cdc$time", pk, ck, v, "cdc$deleted_v", "cdc$deleted_elements_v" FROM ks.t_scylla_cdc_log;

.. code-block:: none

     cdc$time                             | pk | ck | v    | cdc$deleted_v | cdc$deleted_elements_v
    --------------------------------------+----+----+------+---------------+------------------------
     fe29e712-2fee-11eb-ae33-9223dde04e80 |  0 |  0 | null |          True |                   null
     fe2a31e0-2fee-11eb-6a46-0da9bf86f6e9 |  0 |  0 | null |          True |                   null

Another example for overwriting:

.. code-block:: cql

   UPDATE ks.t SET v = [1, 2] WHERE pk = 0 AND ck = 0;
   SELECT "cdc$time", pk, ck, v, "cdc$deleted_v", "cdc$deleted_elements_v" FROM ks.t_scylla_cdc_log;

.. code-block:: none

     cdc$time                             | pk | ck | v                                                                                  | cdc$deleted_v | cdc$deleted_elements_v
    --------------------------------------+----+----+------------------------------------------------------------------------------------+---------------+------------------------
     276b9724-2fef-11eb-d5b0-dbccd2164c64 |  0 |  0 | {276b8860-2fef-11eb-af55-000000000001: 1, 276b8861-2fef-11eb-af55-000000000001: 2} |          True |                   null

The rules for timestamps described in the :ref:`cdc_collection_tombstones` section apply.

User Defined Types
------------------

Unsurprisingly, non-frozen UDTs are also special cases of non-frozen maps. This time the key type is ``smallint`` and the keys stand for field indices. When using UDTs one refers to the field names and ScyllaDB translates them to keys (field indices) using schema internal definitions.

Understanding the correspondence between field names and field indices is important when using CDC with non-frozen UDT columns. Everything then works the same as for maps.

Let's start with an example.

.. code-block:: cql

   CREATE TYPE ks.ut (a int, b int, c int);
   CREATE TABLE ks.t (pk int, ck int, v ut, PRIMARY KEY (pk, ck)) WITH cdc = {'enabled': true};
   DESCRIBE TABLE ks.t_scylla_cdc_log;

result:

.. code-block:: none

    CREATE TABLE ks.t_scylla_cdc_log (
        ...
        "cdc$deleted_elements_v" frozen<set<smallint>>,
        "cdc$deleted_v" boolean,
        ...
        v frozen<ut>,
        ...

For each column ``X T`` in the base table, where ``X`` is a column name and ``T`` is a user-defined-type, the CDC log table has 3 corresponding columns:

* ``X frozen<T>``, the `added elements column`,
* ``cdc$deleted_X boolean``, the `collection deletion column`,
* ``cdc$deleted_elements_X frozen<set<smallint>>``, the `deleted elements column`.

Adding elements
+++++++++++++++

The added elements column is filled when non-null values are assigned to the fields of the user type:

.. code-block:: cql

   UPDATE ks.t SET v.a = 0, v.b = 1 WHERE pk = 0 AND ck = 0;
   SELECT "cdc$time", pk, ck, v, "cdc$deleted_v", "cdc$deleted_elements_v" FROM ks.t_scylla_cdc_log;

result:

.. code-block:: none

     cdc$time                             | pk | ck | v                     | cdc$deleted_v | cdc$deleted_elements_v
    --------------------------------------+----+----+-----------------------+---------------+------------------------
     6380f682-2ff4-11eb-c312-0e51af7972af |  0 |  0 | {a: 0, b: 1, c: null} |          null |                   null

All fields that were not assigned a non-null value are shown as ``null`` in the added elements column (in this case, ``c: null``). Note that this is only an artifact of the CQL syntax and **does not mean that the fields were deleted**: field deletions are shown in the ``cdc$deleted_elements_X`` column, as we will see in the next subsection.

Deleting elements
+++++++++++++++++

The deleted elements column is filled when fields are set to ``null``:

.. code-block:: cql

   UPDATE ks.t SET v.a = null, v.b = null WHERE pk = 0 AND ck = 0;
   SELECT "cdc$time", pk, ck, v, "cdc$deleted_v", "cdc$deleted_elements_v" FROM ks.t_scylla_cdc_log;

result:

.. code-block:: none

     cdc$time                             | pk | ck | v                           | cdc$deleted_v | cdc$deleted_elements_v
    --------------------------------------+----+----+-----------------------------+---------------+------------------------
     d5da679a-2ff4-11eb-6db1-fb8aacbc8bca |  0 |  0 | {a: null, b: null, c: null} |          null |                 {0, 1}

as we can see, two field indices were shown in the ``cdc$deleted_elements_v`` column: the index ``0`` corresponds to field ``a``, and the index ``1`` corresponds to field ``b``. As explained in the `Adding elements` subsection above, since no field was assigned a non-null value, the ``v`` column shows each field as ``null`` (for no reason other than a CQL syntax quirk).

The indices are assigned to fields consecutively, starting from ``0``, in the order that the fields are listed in the result of ``DESCRIBE TYPE`` statement. For example, suppose that the following statement:

.. code-block:: cql

   DESCRIBE TYPE ks.ut;

gives the following result:

.. code-block:: none

    CREATE TYPE ks.ut (
        a int,
        b int,
        c int
    );

The fields ``a``, ``b``, ``c`` are assigned indices ``0``, ``1``, ``2`` correspondingly. Renaming a field does not change its placement in the order, therefore it does not change the index that it's assigned to. You can't remove a field. You can only add a field, in which case the new field is assigned the next free index. Continuing the above example, suppose you execute the statement

.. code-block:: cql

   ALTER TYPE ks.ut ADD d int;

The field ``d`` will be assigned index ``3`` (assuming there was no concurrent ``ALTER TYPE`` performed which executed before the above statement; use ``DESCRIBE TYPE`` to learn what the end result is).

The following example illustrates how the same statement can both set a non-null value to one of the fields and remove another:

.. code-block:: cql

   UPDATE ks.t SET v.a = 42, v.c = null WHERE pk = 0 AND ck = 0;
   SELECT "cdc$time", pk, ck, v, "cdc$deleted_v", "cdc$deleted_elements_v" FROM ks.t_scylla_cdc_log;

result:

.. code-block:: none

     cdc$time                             | pk | ck | v                         | cdc$deleted_v | cdc$deleted_elements_v
    --------------------------------------+----+----+---------------------------+---------------+------------------------
     e0debf6e-2ff5-11eb-0509-e0877d00c0c2 |  0 |  0 | {a: 42, b: null, c: null} |          null |                    {2}

The index ``2`` corresponds to field ``c``, so the deleted elements set contains ``2``, as expected.

Unfortunately, ScyllaDB offers no syntax to operate directly on field indices; you can only perform writes using field names. Thus, to replay a CDC log entry which contains user type field deletions, you must manually translate the field indices to field names using the rule explained above.

Deleting and overwriting UDTs
+++++++++++++++++++++++++++++

The collection deletion column is set to ``True`` when a collection-wide tombstone is created. Furthermore, a single ``UPDATE`` or ``INSERT`` statement can be used to `overwrite` a set using the ``= {...}`` notation, which is the same as creating a collection-wide tombstone and adding new elements at the same time. The behavior is the same as for maps. Example for deleting follows:

.. code-block:: cql

   UPDATE ks.t SET v = null WHERE pk = 0 AND ck = 0;
   SELECT "cdc$time", pk, ck, v, "cdc$deleted_v", "cdc$deleted_elements_v" FROM ks.t_scylla_cdc_log;

result:

.. code-block:: none

     cdc$time                             | pk | ck | v                           | cdc$deleted_v | cdc$deleted_elements_v
    --------------------------------------+----+----+-----------------------------+---------------+------------------------
     0176e250-2ff7-11eb-fcff-e94971544fd7 |  0 |  0 | {a: null, b: null, c: null} |          True |                   null

Another example for overwriting:

.. code-block:: cql

   UPDATE ks.t SET v = {a: 1, b: 2} WHERE pk = 0 AND ck = 0;
   SELECT "cdc$time", pk, ck, v, "cdc$deleted_v", "cdc$deleted_elements_v" FROM ks.t_scylla_cdc_log;

result:

.. code-block:: none

     cdc$time                             | pk | ck | v                     | cdc$deleted_v | cdc$deleted_elements_v
    --------------------------------------+----+----+-----------------------+---------------+------------------------
     17a530b8-2ff7-11eb-ce58-8914310ad1e2 |  0 |  0 | {a: 1, b: 2, c: null} |          True |                   null

The rules for timestamps described in the :ref:`cdc_collection_tombstones` section apply.

Counters
--------

Counters are currently not supported in CDC. If you try to enable CDC on a table with counters, a message like the following will be generated:

.. code-block:: none

    Cannot create CDC log for table ks.t. Counter support not implemented.

You can track the counter support issue here: https://github.com/scylladb/scylla/issues/6013.
