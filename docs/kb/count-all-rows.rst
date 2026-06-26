====================================
Counting all rows in a table is slow
====================================

**Audience: ScyllaDB users**

Trying to count all rows in a table using

.. code-block:: cql

   SELECT COUNT(1) FROM ks.table;

may fail with the **ReadTimeout** error.

COUNT() runs a full-scan query on all nodes, which might take a long time to finish. As a result, the count time may be greater than the ScyllaDB query timeout. 
One way to prevent that issue is to increase the timeout for the query using the :ref:`USING TIMEOUT <using-timeout>` directive, for example:


.. code-block:: cql

   SELECT COUNT(1) FROM ks.table USING TIMEOUT 120s;

You can also get an *estimation* of the number **of partitions** (not rows) with :doc:`nodetool tablestats </operating-scylla/nodetool-commands/tablestats>`.
