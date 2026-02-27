

.. highlight:: cql

.. _time-to-live:

Expiring Data with Time to Live (TTL)
-------------------------------------

ScyllaDB (as well as Apache Cassandra) provides the functionality to automatically delete expired data according to the Time to Live (or TTL) value.
TTL is measured in seconds. If the field is not updated within the TTL it is deleted.
The TTL can be set when defining a Table (CREATE), or when using the INSERT and UPDATE  queries.
The expiration works at the individual column level, which provides a lot of flexibility.
By default, the TTL value is null, which means that the data will not expire.

.. note::

   The expiration time is always calculated as *now() on the Coordinator + TTL* where,  *now()* is the wall clock during the corresponding write operation.
   In particular, it means that a value given via USING TIMESTAMP is **not** taken into an account for an expiration calculation.

TTL using UPDATE and INSERT
^^^^^^^^^^^^^^^^^^^^^^^^^^^

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

It’s also possible to set the TTL when performing an INSERT. To do this use: 

.. code-block:: cql

        INSERT INTO heartrate(pet_chip_id, name, heart_rate) VALUES (c63e71f0-936e-11ea-bb37-0242ac130002, 'Rocky', 87) USING TTL 30;

In this case, a TTL of 30 seconds is set. 


TTL for a Table
^^^^^^^^^^^^^^^
Use the CREATE TABLE or ALTER TABLE commands and set the default_time_to_live value: 

.. code-block:: cql

        CREATE TABLE heartrate_ttl (
            pet_chip_id  uuid,
            name text,
            heart_rate int,
            PRIMARY KEY (pet_chip_id))
        WITH default_time_to_live = 600;

Here a TTL of  10 minutes is applied to all rows, however, keep in mind that TTL is stored on a per column level for non-primary key columns.

It’s also possible to change the default_time_to_live on  an existing table using the ALTER command:

.. code-block:: cql

        ALTER TABLE heartrate_ttl WITH default_time_to_live = 3600;

TTL with  LWT
.............

.. include:: /rst_include/note-ttl-lwt.rst

Refer to :doc:`LWT </features/lwt/>` for more information.

The ``gc_grace_seconds`` parameter is defined :ref:`here <create-table-general-options>`.



TTL for a Collection
^^^^^^^^^^^^^^^^^^^^

You can set the TTL on a per element basis for collections. An example of how to do this, is shown in the :ref:`Maps <maps>` CQL Reference.

Notes
^^^^^
* Notice that setting the TTL on a column using UPDATE or INSERT overrides the default_time_to_live set at the Table level. 
* The TTL is determined by the coordinator node. When using TTL, make sure that all the nodes in the cluster have synchronized clocks. 
* When using TTL for a table, consider using the TWCS compaction strategy. 
* ScyllaDB defines TTL on a per column basis, for non-primary key columns. It’s impossible to set the TTL for the entire row after an initial insert; instead, you can reinsert the row (which is actually an upsert). 
* TTL can not be defined for counter columns. 
* To remove the TTL, set it to 0.



Additional Information
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To learn more about TTL, and see a hands-on example, check out `this lesson <https://university.scylladb.com/courses/data-modeling/lessons/advanced-data-modeling/topic/expiring-data-with-ttl-time-to-live/>`_ on ScyllaDB University.

* :doc:`Apache Cassandra Query Language (CQL) Reference </cql/index>`
* :doc:`KB Article:How to Change gc_grace_seconds for a Table </kb/gc-grace-seconds/>`
* :doc:`KB Article:Time to Live (TTL) and Compaction </kb/ttl-facts/>`
* :ref:`CQL Reference: Table Options <create-table-general-options>`
