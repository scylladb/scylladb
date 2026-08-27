

.. highlight:: cql

.. _time-to-live:

Expiring Data with Time to Live (TTL)
--------------------------------------

ScyllaDB (as well as Apache Cassandra) provides the functionality to automatically delete expired data according to the Time to Live (or TTL) value.
TTL is measured in seconds. If the field is not updated within the TTL it is deleted.
The TTL can be set when defining a Table (CREATE), or when using the INSERT and UPDATE queries.
The expiration works at the individual column level, which provides a lot of flexibility.
By default, the TTL value is null, which means that the data will not expire.

ScyllaDB supports two TTL mechanisms:

**Per-write TTL** (also supported by Apache Cassandra) attaches an expiration
duration to individual column writes. Because each write carries its own TTL,
different columns in the same row can expire at different times.

**Per-row TTL** is a ScyllaDB extension (not available in Apache Cassandra)
where a single designated column holds the absolute expiration timestamp for
the entire row. When no per-write TTLs are set on the row's columns, the whole
row expires together, regardless of when its individual columns were written,
and the expiration time can be changed by updating that one column. Another
benefit of per-row TTL is that it generates a CDC event when a row expires —
in contrast to per-write TTL where expiration events do not show up in CDC.

See `Choosing Between Per-Write and Per-Row TTL`_ for a side-by-side
comparison and guidance on which to use.

.. note::

   For per-write TTL, the expiration time is always calculated as
   *now() on the Coordinator + TTL*, where *now()* is the wall clock
   during the corresponding write operation. In particular, a value given
   via ``USING TIMESTAMP`` is **not** taken into account for expiration.

Per-Write TTL
^^^^^^^^^^^^^

In per-write TTL, an expiration duration is attached to each column write.
TTL can be set on individual statements, or as a table-wide default.

TTL using UPDATE and INSERT
...........................

To set the TTL value using the UPDATE query use the following command:

.. code-block:: cql

        UPDATE heartrate USING TTL 600 SET heart_rate =
        110 WHERE pet_chip_id = 123e4567-e89b-12d3-a456-426655440b23;

In this case, the TTL for the heart_rate column is set 10 minutes (600 seconds).

To check the TTL, use the ``TTL()`` function:

.. code-block:: cql
        
        SELECT name, heart_rate, TTL(heart_rate)
        FROM heartrate WHERE pet_chip_id = 123e4567-e89b-12d3-a456-426655440b23;

The TTL has a value that is lower than 600 as a few seconds passed between setting the TTL and the SELECT query. 
If you wait 10 minutes and run this command again, you will get a null value for the heart_rate. 

It's also possible to set the TTL when performing an INSERT. To do this use: 

.. code-block:: cql

        INSERT INTO heartrate(pet_chip_id, name, heart_rate) VALUES (c63e71f0-936e-11ea-bb37-0242ac130002, 'Rocky', 87) USING TTL 30;

In this case, a TTL of 30 seconds is set. 


TTL for a Table
...............

Use the CREATE TABLE or ALTER TABLE commands and set the default_time_to_live value: 

.. code-block:: cql

        CREATE TABLE heartrate_ttl (
            pet_chip_id  uuid,
            name text,
            heart_rate int,
            PRIMARY KEY (pet_chip_id))
        WITH default_time_to_live = 600;

Here a TTL of  10 minutes is applied to all rows, however, keep in mind that TTL is stored on a per column level for non-primary key columns.

It's also possible to change the default_time_to_live on  an existing table using the ALTER command:

.. code-block:: cql

        ALTER TABLE heartrate_ttl WITH default_time_to_live = 3600;

TTL with LWT
````````````

.. include:: /rst_include/note-ttl-lwt.rst

Refer to :doc:`LWT </features/lwt/>` for more information.

The ``gc_grace_seconds`` parameter is defined :ref:`here <create-table-general-options>`.


TTL for a Collection
....................

You can set the TTL on a per element basis for collections.

See for example the :ref:`Maps <maps>` CQL Reference for map collections.
For a non-frozen map or set column, each element is stored independently and can have its own TTL. You can query the
remaining TTL for a specific map element using ``TTL(map_column[key])`` or for a specific set element using
``TTL(set_column[element])``:

.. code-block:: cql

    CREATE TABLE t (pk int PRIMARY KEY, m map<int, int>, s set<int>);
    INSERT INTO t (pk, m, s) VALUES (1, {1: 10}, {100}) USING TTL 3600;
    UPDATE t USING TTL 7200 SET m = m + {2: 20}, s = s + {200} WHERE pk = 1;

    -- Returns the remaining TTL for each map element independently
    SELECT TTL(m[1]), TTL(m[2]) FROM t WHERE pk = 1;

    -- Returns the remaining TTL for each set element independently
    SELECT TTL(s[100]), TTL(s[200]) FROM t WHERE pk = 1;

Similarly, you can retrieve the write timestamp of a specific map element using ``WRITETIME(map_column[key])``
or of a specific set element using ``WRITETIME(set_column[element])``.

For a non-frozen user-defined type (UDT) column, each field is also stored independently and can have its own TTL.
You can query the remaining TTL or write timestamp of a specific field using dot notation:
``TTL(udt_column.field)`` and ``WRITETIME(udt_column.field)``.

See the :ref:`WRITETIME and TTL function <select-writetime-ttl>` section for details.

Per-Write TTL Notes
...................

* Notice that setting the TTL on a column using UPDATE or INSERT overrides the default_time_to_live set at the Table level. 
* The TTL is determined by the coordinator node. When using TTL, make sure that all the nodes in the cluster have synchronized clocks. 
* When using TTL for a table, consider using the TWCS compaction strategy. 
* ScyllaDB defines TTL on a per column basis, for non-primary key columns. It's impossible to set the TTL for the entire row after an initial insert; instead, you can reinsert the row (which is actually an upsert). 
* TTL can not be defined for counter columns. 
* To remove the TTL, set it to 0.

.. _per-row-ttl:

Per-Row TTL
^^^^^^^^^^^

Per-row TTL is a ScyllaDB-specific extension that is **not available in Apache
Cassandra**. Instead of attaching a duration to each column write, one column
is designated as the *expiration-time* column and its value determines when
the entire row expires. This makes it straightforward to update a row's
expiration time without rewriting all of its data, and to update data without
changing its expiration time.

CQL's traditional per-write TTL attaches an expiration time to each cell —
i.e., each value in each column. For example, the statement:

.. code-block:: cql

    UPDATE tbl USING TTL 60 SET x = 1 WHERE p = 2

sets a new value for the column ``x`` in row ``p = 2``, and asks for this
value to expire in 60 seconds. When a row is updated incrementally, with
different columns set at different times, this can result in different pieces
of the row expiring at different times. Applications rarely want
partially-expired rows, so they often need to re-write an entire row each time
the row needs updating. In particular, it is not possible to change the
expiration time of an existing row without re-writing it.

The expiration-time column of a table can be designated at creation time by
adding the keyword ``TTL`` to the column definition:

.. code-block:: cql

    CREATE TABLE tab (
        id int PRIMARY KEY,
        t text,
        expiration timestamp TTL
    );

The TTL column's name (``expiration`` in this example) can be anything.

Per-row TTL can also be enabled on an existing table by designating one of its
columns:

.. code-block:: cql

    ALTER TABLE tab TTL colname

Or the per-row TTL designation can be removed, so that the designated column
no longer controls row expiration:

.. code-block:: cql

    ALTER TABLE tab TTL NULL

It is not possible to enable per-row TTL if it is already enabled, or disable
it when already disabled. To move the TTL designation from one column to
another, first disable TTL and then re-enable it on the second column.

The designated TTL column must have the type ``timestamp`` or ``bigint`` and
specifies the *absolute* time when the row should expire (``bigint`` is
interpreted as seconds since the UNIX epoch). It must be a regular column (not
a primary key column or a static column), and there can only be one such column
per table.

The 32-bit type ``int`` (seconds since the UNIX epoch) is also accepted, but
not recommended because it will overflow in 2038. Unless you have pre-existing
expiration data stored as ``int``, prefer ``timestamp`` or ``bigint``.

Unlike per-write TTL, where a value becomes unreadable at the precise specified
second, per-row TTL expiration is *eventual* — the row will be deleted some
time *after* its requested expiration time. The delay is controlled by the
``alternator_ttl_period_in_seconds`` configuration option. Until the row is
actually deleted it can still be read and written.

CDC integration
...............

When CDC is enabled on a table, per-row TTL generates a deletion event in the
CDC log each time a row expires — something that does **not** happen with
per-write TTL. The deletion event is distinguishable from user-initiated
deletes: user deletes carry ``cdc_operation`` 3 (``row_delete``) or 4
(``partition_delete``), while expiration-triggered deletes carry
``cdc_operation`` -3 (``service_row_delete``) or -4
(``service_partition_delete``). The CDC event appears immediately after the
row is finally deleted.

.. _choosing-between-per-write-and-per-row-ttl:

Choosing Between Per-Write and Per-Row TTL
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The two TTL mechanisms have different trade-offs. The table below summarises
the key differences:

.. list-table::
   :header-rows: 1
   :widths: 40 30 30

   * -
     - Per-write TTL
     - Per-row TTL
   * - **Cassandra compatibility**
     - Yes
     - No (ScyllaDB only)
   * - **Expiry granularity**
     - Per cell (column value)
     - Entire row
   * - **Expiration time specified as**
     - Relative duration in seconds (``USING TTL 60``)
     - Absolute timestamp stored in a column
   * - **Expiration precision**
     - Second accuracy
     - Eventual (configurable delay)
   * - **Change expiry without rewriting data**
     - No
     - Yes (update the expiration column)
   * - **Partial-row expiry**
     - Yes (columns written at different times can expire separately)
     - No (whole row expires together, provided no per-write TTLs are also set)
   * - **CDC / Streams / VS deletion event on expiry**
     - No
     - Yes

**Use per-write TTL when:**

* You need Apache Cassandra compatibility.
* You want second-accurate expiration.
* Fine-grained, per-column expiry is intentional (e.g. short-lived sensor
  readings where each column has an independent TTL).

**Use per-row TTL when:**

* You want to update a row's expiration time without rewriting all its data.
* You need CDC, Change Streams, or Vector Search to receive a delete event
  when a row expires.
* You prefer DynamoDB-style row-level expiration semantics.
* Whole-row expiration is important and partial expiry is undesirable (and
  no per-write TTLs are set on the same row).

It is important to note that the per-cell TTL and per-row TTL features are
separate and distinct, use a different CQL syntax, have a different
implementation and provide different guarantees. It is possible to use
both features in the same table, or even the same row.

Additional Information
^^^^^^^^^^^^^^^^^^^^^^

To learn more about TTL, and see a hands-on example, check out `this lesson <https://university.scylladb.com/courses/data-modeling/lessons/advanced-data-modeling/topic/expiring-data-with-ttl-time-to-live/>`_ on ScyllaDB University.

* `Video: Managing data expiration with Time-To-Live <https://www.youtube.com/watch?v=SXkbu7mFHeA>`_
* :doc:`Apache Cassandra Query Language (CQL) Reference </cql/index>`
* :doc:`KB Article:How to Change gc_grace_seconds for a Table </kb/gc-grace-seconds/>`
* :doc:`KB Article:Time to Live (TTL) and Compaction </kb/ttl-facts/>`
* :ref:`CQL Reference: Table Options <create-table-general-options>`
