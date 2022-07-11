====================================
Counting all rows in a table is slow
====================================

**Audience: Scylla users**

Trying to count all rows in a table using

.. code-block:: cql

   SELECT COUNT(1) FROM ks.table;

often fails with **ReadTimeout** error.

COUNT() is running a full-scan query on all nodes, which might take a long time to finish. Often the time is greater than Scylla query timeout. 
One way to bypass this in Scylla 4.4 or later is increasing the timeout for this query using the :ref:`USING TIMEOUT <using-timeout>` directive, for example:


.. code-block:: cql

   SELECT COUNT(1) FROM ks.table USING TIMEOUT 120s;

You can also get an *estimation* of the number **of partitions** (not rows) with :doc:`nodetool tablestats </operating-scylla/nodetool-commands/tablestats>`
