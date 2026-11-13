
Time Range Queries Do Not Return Some or All of the Data
========================================================

This troubleshooting article describes what to do when time range queries do not return some of or all of the expected data.

Problem
^^^^^^^
In many cases, the client that performs an ``INSERT``, is not the same client that runs a ``SELECT``. As such, the clients may be in different time zones (TZ) or may be using client/server timestamps with different TZs.

Solution
^^^^^^^^

It is highly recommended when submitting a ``SELECT`` query to include the client’s local TZ within the :ref:`timestamp <timestamps>`  (for example:  ``2019-02-18 06:00:00+0000``). Otherwise, the query will parse differently as a result of different TZs and may not return some / all the dataset you're trying to fetch.

.. note:: The ``+0000`` (above) is an RFC 822 4-digit time zone specification where ``+0000`` refers to GMT. US Pacific Standard Time, for example is ``-0800``. A timestamp using US Pacific Standard Time would be ``2019-02-18 06:00:00-0800``.


Example
^^^^^^^
This example creates a table, inserts data with a time zone timestamp, and then shows two queries: one with and one without a time zone timestamp

1. Create the table

.. code-block:: cql 

   CREATE KEYSPACE IF NOT EXISTS mykeyspace 
      WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 3 };
   USE mykeyspace;
   CREATE TABLE heartrate (
      pet_chip_id uuid,
      time timestamp,
      heart_rate int,
      PRIMARY KEY (pet_chip_id, time));

2. Insert data into the table where the timezone is Pacific Standard Time (-0800)

.. code-block:: cql 

   INSERT INTO heartrate(pet_chip_id, time, heart_rate)
      VALUES (123e4567-e89b-12d3-a456-426655440b23, '2019-03-04 07:01:00-0800', 100);
   INSERT INTO heartrate(pet_chip_id, time, heart_rate) 
      VALUES (123e4567-e89b-12d3-a456-426655440b23, '2019-03-04 07:02:00-0800', 103);
   INSERT INTO heartrate(pet_chip_id, time, heart_rate) 
      VALUES (123e4567-e89b-12d3-a456-426655440b23, '2019-03-04 07:03:00-0800', 130);

3. Query for data within a time range without using a timezone. There are no results.

.. code-block:: cql 

   SELECT * from heartrate 
      WHERE pet_chip_id = 123e4567-e89b-12d3-a456-426655440b23 
      AND time>='2019-03-04 07:00:00' 
      AND time <= '2019-03-04 08:00:00';

   pet_chip_id  | time | heart_rate
   -------------+------+------------

   (0 rows)

4. Now query for data within a time range with a timezone. Notice how the time corrects itself to GMT timezone. 

.. code-block:: cql 

   SELECT * from heartrate 
      WHERE pet_chip_id = 123e4567-e89b-12d3-a456-426655440b23 
      AND time>='2019-03-04 07:00:00-0800' 
      AND time <= '2019-03-04 08:00:00-0800';

   pet_chip_id                           | time                            | heart_rate
   --------------------------------------+---------------------------------+------------
   123e4567-e89b-12d3-a456-426655440b23  | 2019-03-04 15:01:00.000000+0000 | 100
   123e4567-e89b-12d3-a456-426655440b23  | 2019-03-04 15:02:00.000000+0000 | 103
   123e4567-e89b-12d3-a456-426655440b23  | 2019-03-04 15:03:00.000000+0000 | 130

   (3 rows)

