====================================================
How does ScyllaDB LWT Differ from Apache Cassandra ?
====================================================

ScyllaDB is making an effort to be compatible with Cassandra, down to the level of limitations of the implementation. 
How is it different?

* ScyllaDB most commonly uses fewer rounds than Cassandra to complete a lightweight transaction. While Cassandra issues a separate read query to fetch the old record, scylla piggybacks the read result on the response to the prepare round.
* ScyllaDB will automatically use synchronous commit log write mode for all lightweight transaction writes. Before a lightweight transaction completes, scylla will ensure that the data in it has hit the device. This is done in all commitlog_sync modes.
* Conditional statements return a result set, and unlike Cassandra, ScyllaDB result set metadata doesn’t change from execution to execution: ScyllaDB always returns  the old version of the  row, regardless of whether the condition is true or not. This ensures conditional statements work well with prepared statements.
* For batch statement, the returned result set contains an old row for every conditional statement in the batch, in statement order. Cassandra returns results in clustering key order.
* For batch statement, ScyllaDB allows mixing `IF EXISTS`, `IF NOT EXISTS`, and other conditions for the same row.
* Unlike Cassandra, ScyllaDB  uses per-core data partitioning, so the RPC  that is done to perform a transaction talks directly to the right core on a peer replica, avoiding the concurrency overhead. This is,  of course, true, if ScyllaDB’s own shard-aware driver is used - otherwise we  add an extra hop to the right core at the coordinator node.
* ScyllaDB does not store  hints for lightweight transaction writes, since this is redundant as all such writes are already present in system.paxos table.


More on :doc:`Lightweight Transactions (LWT) </features/lwt>`

Additional Notes
================

Mixing LWT IF clauses in BATCH statements
-----------------------------------------

`Conditional batches` are `BATCH` statements that contain one or more conditional statements. A batch is executed only if all conditions in all statements are true, and all conditions are evaluated against the initial database state. ScyllaDB allows using different conditions such as `IF EXISTS`, `IF NOT EXISTS`, and other `IF` expressions within the same batch, even if the statements affect the same row.

For example, the following batch statement is valid in ScyllaDB and will be applied successfully:

.. code-block:: cql

   BEGIN BATCH
       UPDATE movies.nowshowing SET main_actor = NULL WHERE movie = 'Invisible Man' IF director = 'Leigh Whannell'
       UPDATE movies.nowshowing SET released = NULL WHERE movie = 'Invisible Man' IF EXISTS
   APPLY BATCH;

    [applied] | movie         | location | run_day | run_time | director       | main_actor     | released   | theater
   -----------+---------------+----------+---------+----------+----------------+----------------+------------+---------
         True | Invisible Man |     null |    null |     null | Leigh Whannell | Elisabeth Moss | 2022-04-06 |    null
         True | Invisible Man |     null |    null |     null | Leigh Whannell | Elisabeth Moss | 2022-04-06 |    null

By contrast, Cassandra does not allow mixing `IF EXISTS`, `IF NOT EXISTS`, and other conditions for the same row:

.. code-block:: cql

   BEGIN BATCH
       UPDATE movies.nowshowing SET main_actor = NULL WHERE movie = 'Invisible Man' IF director = 'Leigh Whannell'
       UPDATE movies.nowshowing SET released = NULL WHERE movie = 'Invisible Man' IF EXISTS
   APPLY BATCH;

   InvalidRequest: Error from server: code=2200 [Invalid query] message="Cannot mix IF conditions and IF EXISTS for the same row"

Moreover, ScyllaDB does not return an error even if the conditions are impossible to satisfly, i.e. a batch contains `IF EXISTS` and `IF NOT EXISTS` clauses for the same row. The following query will be executed, though not applied, as the conditions are impossible to satisfy:

.. code-block:: cql

    BEGIN BATCH
        INSERT INTO movies.nowshowing (movie, location, theater, run_day, run_time) VALUES ('Invisible Man', 'Times Square', 'AMC Empire 25', 'Saturday', '23:00:00') IF NOT EXISTS
        UPDATE movies.nowshowing SET theater = NULL WHERE movie = 'Invisible Man' AND location = 'Times Square' AND run_day = 'Saturday' AND run_time = '23:00:00' IF EXISTS
    APPLY BATCH;

    [applied] | movie | location | run_day | run_time | director | main_actor | released | theater
    -----------+-------+----------+---------+----------+----------+------------+----------+---------
        False |  null |     null |    null |     null |     null |       null |     null |    null
        False |  null |     null |    null |     null |     null |       null |     null |    null

In comparison, Cassandra returns an error in this case:

.. code-block:: cql

    BEGIN BATCH
        INSERT INTO movies.nowshowing (movie, location, theater, run_day, run_time) VALUES ('Invisible Man', 'Times Square', 'AMC Empire 25', 'Saturday', '23:00:00') IF NOT EXISTS
        UPDATE movies.nowshowing SET theater = NULL WHERE movie = 'Invisible Man' AND location = 'Times Square' AND run_day = 'Saturday' AND run_time = '23:00:00' IF EXISTS
    APPLY BATCH;

    InvalidRequest: Error from server: code=2200 [Invalid query] message="Cannot mix IF EXISTS and IF NOT EXISTS conditions for the same row"
