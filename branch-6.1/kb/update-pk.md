# Update a Primary Key

**Topic: Can you Update a Primary Key in ScyllaDB?**

**Audience: ScyllaDB administrators**

In ScyllaDB, you cannot update a primary key. It is impossible to do so.

However, you can migrate the data from the old table with the old primary key to a new table with a new primary key.
There are two ways to handle the migration:

* Fork-lifting the historical data with the [Spark Migrator](https://opensource.docs.scylladb.com/branch-6.1/using-scylla/mig-tool-review.md) tool.
* Double writing the new data.
