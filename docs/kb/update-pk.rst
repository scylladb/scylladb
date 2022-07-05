==============================
Update a Primary Key
==============================

**Topic: Can you Update a Primary Key in Scylla?**

**Audience: Scylla administrators**

In Scylla, you cannot update a primary key. It is impossible to do so.

However, you can migrate the data from the old table with the old primary key to a new table with a new primary key.
There are two ways to handle the migration:

* Fork-lifting the historical data with the :doc:`Spark Migrator </using-scylla/mig-tool-review/>` tool.
* Double writing the new data.

If you are running Scylla Enterprise it is advantageous to control the migration resources using :doc:`Workload Prioritization </using-scylla/workload-prioritization/>`.
