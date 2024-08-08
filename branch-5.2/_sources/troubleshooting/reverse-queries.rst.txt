
Reverse Queries fail when the partition is larger than the maximum safe size
=============================================================================

This troubleshooting article describes what to do when queries that return results in a different order than the data model start failing.

It is highly recommended that when using the CQL **ORDER BY** clause, the order in the query is the same as the data model.

For example, the following table uses *time* as a clustering key, using the default ASC order.

.. code:: cql

   CREATE TABLE heartrate (
   pet_chip_id  uuid,
   time timestamp,
   heart_rate int,
   PRIMARY KEY (pet_chip_id, time));


For good performance, it's best to use the same order in queries:


.. code:: cql

   SELECT * from heartrate WHERE pet_chip_id = 123e4567-e89b-12d3-a456-426655440b23;

A reverse query will use a reverse order to the schema:

.. code:: cql

   SELECT * from heartrate WHERE pet_chip_id = 123e4567-e89b-12d3-a456-426655440b23 ORDER BY time DESC;

However, when the partition is large, Scylla needs to read the WHOLE partition and reverses it in memory. In the best case, this creates stress for resources on Scylla nodes, and in the worst case, it can  trigger a crash.

See issue `#5804  <https://github.com/scylladb/scylla/issues/5804>`_ for more information.

.. note::

   Scylla monitoring dashboards detect reverse queries and display them on the CQL Optimization dashboard.
   See `Reversed CQL Reads <https://monitoring.docs.scylladb.com/stable/use-monitoring/cql_optimization.html#reversed-cql-reads>`_ for details.

.. note::
   To figure out WHICH query is causing the failure, you might try the procedure described in :ref:`probabilistic tracing <tracing-probabilistic-tracing>`.

Problem
^^^^^^^

After upgrading to Scylla Enterprise version 2019.1.8, I noticed the Scylla node logs contained the following error for my reverse queries:

.. code-block:: console

   Exception when communicating with 192.168.1.120: std::runtime_error (Aborting reverse partition read because partition 1134022 is larger than the maximum safe size of 1048576 for reversible partitions.)

The error is a result of a new configuration parameter *max_memory_for_unlimited_query* limiting the amount of memory Scylla allocates for reverse queries. The parameter is part of the Scylla configuration settings and can be changed in the scylla.yaml file.

Solution
^^^^^^^^

If possible, try to avoid reverse order queries by choosing the right order when creating the table or updating the queries.

If you want to reverse order queries on large partitions, you can override the 1MB limit by increasing the configuration parameter in the ``scylla.yaml`` file.

Update ``/etc/scylla/scylla.yaml`` on all nodes as shown in the example.
The example increases the limit to 10MB - you should adjust the limit to your setup and needs. Keep in mind that the higher the value, the bigger the risk of out of memory issues.

.. code-block:: console

   max_memory_for_unlimited_query: 10485760

You need to perform a :doc:`rolling restart </operating-scylla/procedures/config-change/rolling-restart>` of all nodes above to apply the new settings.
