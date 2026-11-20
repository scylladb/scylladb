# Basic operations in CDC

The CDC log table reflects operations that are performed on the base table. Different types of operations give different corresponding entries in the CDC log. These operations are:

* inserts,
* updates,
* single row deletions,
* row range deletions,
* partition deletions.

The following sections describe how each of these operations are handled by the CDC log.

<a id="updates"></a>

## Updates

Updates are the most basic statements that can be performed. The following is an example of an update with CDC enabled.

1. Start with a basic table and perform some UPDATEs:
   ```cql
   CREATE TABLE ks.t (pk int, ck int, v1 int, v2 int, PRIMARY KEY (pk, ck)) WITH cdc = {'enabled':'true'};
   UPDATE ks.t SET v1 = 0 WHERE pk = 0 AND ck = 0;
   UPDATE ks.t SET v2 = null WHERE pk = 0 AND ck = 0;
   ```
2. Confirm that the update was performed by displaying the contents of the table:
   ```cql
   SELECT * FROM ks.t;
   ```

   returns:
   ```none
    pk | ck | v1 | v2
   ----+----+----+------
     0 |  0 |  0 | null

   (1 rows)
   ```
3. Display the contents of the CDC log table:
   ```cql
   SELECT "cdc$batch_seq_no", pk, ck, v1, "cdc$deleted_v1", v2, "cdc$deleted_v2", "cdc$operation" FROM ks.t_scylla_cdc_log;
   ```

   returns:
   ```none
    cdc$batch_seq_no | pk | ck | v1   | cdc$deleted_v1 | v2   | cdc$deleted_v2 | cdc$operation
   ------------------+----+----+------+----------------+------+----------------+---------------
                   0 |  0 |  0 |    0 |           null | null |           null |             1
                   0 |  0 |  0 | null |           null | null |           True |             1

   (2 rows)
   ```

Delta rows corresponding to updates are indicated by `cdc$operation = 1`. All columns corresponding to the primary key in the log are set to the same values as in the base table, in this case `pk = 0` and `ck = 0`.

Each non-key column, such as `v1` above, has a corresponding column in the log with the same name, but there’s an additional column with the `cdc$deleted_` prefix (e. g. `cdc$deleted_v1`) used to indicate whether the column was set to `null` in the update.

If the update sets a column `X` to a non-null value, the `X` column in the log will have that value; however, if the update sets `X` to a null value, the `X` column in the log will be `null`, and we’ll use the `cdc$deleted_X` column to indicate the update by setting it to `True`. In the example above, this is what happened with the `v2` column.

Column deletions are a special case of an update statement. That is, the following two statements are equivalent:

```cql
UPDATE <table_name> SET X = null WHERE <condition>;
DELETE X FROM <table_name> WHERE <condition>;
```

Thus, in the example above, instead of

```cql
UPDATE ks.t SET v2 = null WHERE pk = 0 AND ck = 0;
```

we could have used

```cql
DELETE v2 FROM ks.t WHERE pk = 0 AND ck = 0;
```

and we would’ve obtained the same result.

Note that column deletions, (which are equivalent to updates that set a column to `null`) *are different than row deletions*, i.e. `DELETE` statements that specify a clustering row but don’t specify any particular column, like the following:

```cql
DELETE FROM ks.t WHERE pk = 0 AND ck = 0;
```

You can read about row deletions in the [corresponding section](#row-deletions).

### Digression: static rows in ScyllaDB

If a table in ScyllaDB has static columns, then every partition in this table contains a *static row*, which is global for the partition. This static row is different from the clustered rows: it contains values for partition key columns and static columns, while clustered rows contain values for partition key, clustering key, and regular columns. The following example illustrates how the static row can be used:

```cql
CREATE TABLE ks.t (pk int, ck int, s int static, c int, PRIMARY KEY (pk, ck));
UPDATE ks.t SET s = 0 WHERE pk = 0;
SELECT * from ks.t WHERE pk = 0;
```

returns:

```none
 pk | ck   | s | c
----+------+---+------
  0 | null | 0 | null

(1 rows)
```

Even though no regular columns were set, the above query returned a row; the static row.

We can still update clustered rows, of course:

```cql
UPDATE ks.t SET c = 0 WHERE pk = 1 AND ck = 0;
SELECT * from ks.t WHERE pk = 1;
```

returns:

```none
 pk | ck | s    | c
----+----+------+---
  1 |  0 | null | 0

(1 rows)
```

Somewhat confusingly, CQL mixes the static row with the clustered rows if both appear within the same partition. Example:

```cql
UPDATE ks.t SET c = 0 WHERE pk = 2 AND ck = 0;
UPDATE ks.t SET c = 1 WHERE pk = 2 AND ck = 1;
UPDATE ks.t SET s = 2 WHERE pk = 2;
SELECT * from ks.t WHERE pk = 2;
```

returns:

```none
 pk | ck | s | c
----+----+---+---
  2 |  0 | 2 | 0
  2 |  1 | 2 | 1

(2 rows)
```

From the above query result it seems as if the static column was a part of every clustered row, and setting the static column to a value within one partition is setting it “for every clustered row”. **That is not the case**, it is simply CQL’s way of showing the static row when both the static row and clustered rows are present: it “mixes” the static row into each clustered row. But to understand the static row, think of it as a separate row in the partition, with the property that there can be at most one static row in a partition; the static row exists if and only if at least one of the static columns are non-null.

### Static rows in CDC

CDC separates static row updates from clustered row updates, showing them as different entries, even if you update both within one statement. Example:

```cql
CREATE TABLE ks.t (pk int, ck int, s int static, c int, PRIMARY KEY (pk, ck)) WITH cdc = {'enabled': true};
UPDATE ks.t SET s = 0, c = 0 WHERE pk = 0 AND ck = 0;
SELECT "cdc$batch_seq_no", pk, ck, s, c, "cdc$operation" FROM ks.t_scylla_cdc_log;
```

returns:

```none
 cdc$batch_seq_no | pk | ck   | s    | c    | cdc$operation
------------------+----+------+------+------+---------------
                0 |  0 | null |    0 | null |             1
                1 |  0 |    0 | null |    0 |             1

(2 rows)
```

CDC recognizes that logically two updates happened: one to the static row and one to the clustered row. In other words, CDC interprets the following:

```cql
UPDATE ks.t SET s = 0, c = 0 WHERE pk = 0 AND ck = 0;
```

as follows:

```cql
BEGIN UNLOGGED BATCH
    UPDATE ks.t SET s = 0 WHERE pk = 0;
    UPDATE ks.t SET c = 0 WHERE pk = 0 AND ck = 0;
APPLY BATCH;
```

However, since they happened in a single statement and had a single timestamp, it grouped them using `cdc$batch_seq_no`. The static row update can be distinguished from the clustered row update by looking at clustering key columns, in case `ck`: they are `null` for static row updates and non-null for clustered row updates.

## Inserts

### Digression: the difference between inserts and updates

Inserts are not the same as updates, contrary to a popular belief in Cassandra/ScyllaDB communities. The following example illustrates the difference:

```cql
CREATE TABLE ks.t (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH cdc = {'enabled':'true'};
UPDATE ks.t SET v = null WHERE pk = 0 AND ck = 0;
SELECT * FROM ks.t WHERE pk = 0 AND ck = 0;
```

returns:

```cql
 pk | ck | v
----+----+---

(0 rows)
```

However:

```cql
INSERT INTO ks.t (pk,ck,v) VALUES (0, 0, null);
SELECT * FROM ks.t WHERE pk = 0 AND ck = 0;
```

returns:

```none
 pk | ck | v
----+----+------
  0 |  0 | null

(1 rows)
```

<a id="row-marker"></a>

Each table has an additional invisible column called the *row marker*. It doesn’t hold a value; it only holds *liveness information* (timestamp and time-to-live). If the row marker is alive, the row shows up when you query it, even if all its non-key columns are null. The difference between inserts and updates is that **updates don’t affect the row marker**, while **inserts create an alive row marker**.

Here’s another example:

```cql
CREATE TABLE ks.t (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH cdc = {'enabled':'true'};
UPDATE ks.t SET v = 0 WHERE pk = 0 AND ck = 0;
SELECT * FROM ks.t;
```

returns:

```cql
 pk | ck | v
----+----+---
  0 |  0 | 0

(1 rows)
```

The value in the `v` column keeps the `(pk = 0, ck = 0)` row alive, therefore it shows up in the query. After we delete it, the row will be gone:

```cql
UPDATE ks.t SET v = null WHERE pk = 0 AND ck = 0;
SELECT * FROM ks.t;
```

returns:

```none
 pk | ck | v
----+----+---

(0 rows)
```

However, if we had used an `INSERT` instead of an `UPDATE` in the first place, the row would still show up even after deleting `v`:

```cql
INSERT INTO ks.t (pk, ck, v) VALUES (0, 0, 0);
UPDATE ks.t set v = null where pk = 0 and ck = 0;
SELECT * from ks.t;
```

returns:

```none
 pk | ck | v
----+----+------
  0 |  0 | null

(1 rows)
```

The row marker introduced by `INSERT` keeps the row alive, even if there are no other non-key columns that are not `null`. Therefore the row shows up in the query.
We can create just the row marker, without updating any columns, like this:

```cql
INSERT INTO ks.t (pk, ck) VALUES (0, 0);
```

When specifying both key and non-key columns in an `INSERT` statement, we’re saying “create a row marker, *and* set cells for this row”. We can explicitly divide these two operations; the following:

```cql
INSERT INTO ks.t (pk, ck, v) VALUES (0, 0, 0);
```

is equivalent to:

```cql
BEGIN UNLOGGED BATCH
    INSERT INTO ks.t (pk, ck) VALUES (0, 0);
    UPDATE ks.t SET v = 0 WHERE pk = 0 AND ck = 0;
APPLY BATCH;
```

The `INSERT` creates a row marker, the `UPDATE` sets the cell in the `(pk, ck) = (0, 0)` row and `v` column.

### Inserts in CDC

Inserts affect the CDC log very similarly to updates; if no collections or static columns are involved, the difference lies only in the `cdc$operation` column:

1. Start with a basic table and perform some insert:
   ```cql
   CREATE TABLE ks.t (pk int, ck int, v1 int, v2 int, PRIMARY KEY (pk, ck)) WITH cdc = {'enabled':'true'};
   INSERT INTO ks.t (pk, ck, v1) VALUES (0, 0, 0);
   INSERT INTO ks.t (pk, ck, v2) VALUES (0, 0, NULL);
   ```
2. Confirm that the insert was performed by displaying the contents of the table:
   ```cql
   SELECT * FROM ks.t;
   ```

   returns:
   ```none
    pk | ck | v1 | v2
   ----+----+----+------
     0 |  0 |  0 | null

   (1 rows)
   ```
3. Display the contents of the CDC log table:
   ```cql
   SELECT "cdc$batch_seq_no", pk, ck, v1, "cdc$deleted_v1", v2, "cdc$deleted_v2", "cdc$operation" FROM ks.t_scylla_cdc_log;
   ```

   returns:
   ```none
    cdc$batch_seq_no | pk | ck | v1   | cdc$deleted_v1 | v2   | cdc$deleted_v2 | cdc$operation
   ------------------+----+----+------+----------------+------+----------------+---------------
                   0 |  0 |  0 |    0 |           null | null |           null |             2
                   0 |  0 |  0 | null |           null | null |           True |             2

   (2 rows)
   ```

Delta rows corresponding to inserts are indicated by `cdc$operation = 2`.

If a static row update is performed within an `INSERT`, it is separated from the `INSERT`, in the same way a clustered row update is separated from a static row update. Example:

```cql
CREATE TABLE ks.t (pk int, ck int, s int static, c int, PRIMARY KEY (pk, ck)) WITH cdc = {'enabled': true};
INSERT INTO ks.t (pk, ck, s, c) VALUES (0, 0, 0, 0);
SELECT "cdc$batch_seq_no", pk, ck, s, c, "cdc$operation" FROM ks.t_scylla_cdc_log;
```

returns:

```none
 cdc$batch_seq_no | pk | ck   | s    | c    | cdc$operation
------------------+----+------+------+------+---------------
                0 |  0 | null |    0 | null |             1
                1 |  0 |    0 | null |    0 |             2

(2 rows)
```

There is no such thing as a “static row insert”. Indeed, static rows don’t have a row marker; the only way to make a static row show up is to set a static column to a non-null value. Therefore, the following statement (using the table from above):

```cql
INSERT INTO ks.t (pk, s) VALUES (0, 0);
```

is equivalent to:

```cql
UPDATE ks.t SET s = 0 WHERE pk = 0;
```

This is the reason why `cdc$operation` is `1`, not `2`, in the example above for the static row update.

<a id="row-deletions"></a>

## Row deletions

Row deletions are operations where you delete an entire clustering row by using a `DELETE` statement, specifying the values of all clustering key columns, but **not** specifying any particular regular column.
The following is an example of a row deletion with CDC enabled.

1. Start with a basic table and insert some data into it.
   ```cql
   CREATE TABLE ks.t (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH cdc = {'enabled': 'true'};
   INSERT INTO ks.t (pk, ck, v) VALUES (0,0,0);
   ```
2. Display the contents of the table
   ```cql
   SELECT * from ks.t;
   ```

   returns:
   ```none
    pk | ck | v
   ----+----+---
     0 |  0 | 0

   (1 rows)
   ```
3. Remove the row where the primary key 0 and the clustering key is 0.
   ```cql
   DELETE FROM ks.t WHERE pk = 0 AND ck = 0;
   ```
4. Check that after performing a row deletion, querying the deleted row returns 0 results:
   ```cql
   SELECT * from ks.t WHERE pk = 0 AND ck = 0;
   ```

   returns:
   ```none
    pk | ck | v
   ----+----+------

   (0 rows)
   ```

Note that row deletions *are different than column deletions*, which are `DELETE` statements with some particular non-key column specified, like the following:

```cql
DELETE v FROM ks.t WHERE pk = 0 AND ck = 0;
```

Column deletions are equivalent to UPDATEs with the column set to null, e.g.:

```cql
UPDATE ks.t SET v = null WHERE pk = 0 AND ck = 0;
```

You can read about UPDATEs in the [corresponding section](#updates).

## Row range deletions

Range deletions are statements in which you delete a set of rows specified by a continuous range of clustering keys.
They work within one partition, so you must provide a single partition key when performing a range deletion.

The following is an example of a row range deletion with CDC enabled.

1. Start with a basic table and insert some data into it.
   ```cql
   CREATE TABLE ks.t (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH cdc = {'enabled':'true'};
   INSERT INTO ks.t (pk,ck,v) VALUES (0,0,0);
   INSERT INTO ks.t (pk,ck,v) VALUES (0,1,1);
   INSERT INTO ks.t (pk,ck,v) VALUES (0,2,2);
   INSERT INTO ks.t (pk,ck,v) VALUES (0,3,3);
   ```
2. Display the contents of the table
   ```cql
   SELECT * FROM ks.t;
   ```

   returns:
   ```none
    pk | ck | v
   ----+----+---
     0 |  0 | 0
     0 |  1 | 1
     0 |  2 | 2
     0 |  3 | 3

   (4 rows)
   ```
3. Remove all rows where the primary key is 0 and the clustering key is in the `(0, 2]` range (left-opened, right-closed).
   ```cql
   DELETE FROM ks.t WHERE pk = 0 AND ck <= 2 and ck > 0;
   ```
4. Display the contents of the table after the delete:
   ```cql
   SELECT * FROM ks.t;
   ```

   returns:
   ```none
    pk | ck | v
   ----+----+---
     0 |  0 | 0
     0 |  3 | 3

   (2 rows)
   ```
5. Display the contents of the CDC log table:
   ```cql
   SELECT "cdc$batch_seq_no", pk, ck, v, "cdc$operation" FROM ks.t_scylla_cdc_log;
   ```

   returns:
   ```none
    cdc$batch_seq_no | pk | ck | v    | cdc$operation
   ------------------+----+----+------+---------------
                   0 |  0 |  0 |    0 |             2
                   0 |  0 |  1 |    1 |             2
                   0 |  0 |  2 |    2 |             2
                   0 |  0 |  3 |    3 |             2
                   0 |  0 |  0 | null |             6
                   1 |  0 |  2 | null |             7

   (6 rows)
   ```

A range deletion appears in CDC as a batch of up to two entries into the log table: an entry corresponding to range’s left bound (if specified), and an entry corresponding to the range’s right bound (if specified). The appropriate values of `cdc$operation` are as follows:

* `5` denotes an inclusive left bound,
* `6` denotes an exclusive left bound,
* `7` denotes an inclusive right bound,
* `8` denotes an exclusive right bound.

In the example above we’ve used the range `(0, 2]`, with an exclusive left bound and inclusive right bound. This is presented in the CDC log as two log rows: one with `cdc$operation = 6` and the other with `cdc$operation = 7`.

The values for non-key base columns are `null` in these entries.

If you specify a one-sided range:

```cql
DELETE FROM ks.t WHERE pk = 0 AND ck < 3;
```

then two entries will appear in the CDC log: one with `cdc$operation = 5` and `null` clustering key, indicating the range is not left-bounded and the other with `cdc$operation = 8`, representing the right bound of the range:

```none
 cdc$batch_seq_no | pk | ck   | v    | cdc$operation
------------------+----+------+------+---------------
                0 |  0 | null | null |             5
                1 |  0 |    3 | null |             8
```

### Range deletions with multi-column clustering keys

In the case of multi-column clustering keys, range deletions allow to specify an equality relation for some prefix of the clustering key, followed by an ordering relation on the column that follows the prefix.

1. Start with a basic table:
   ```cql
   CREATE TABLE ks.t (pk int, ck1 int, ck2 int, ck3 int, v int, primary key (pk, ck1, ck2, ck3)) WITH cdc = {'enabled':'true'};
   ```
2. Remove the rows where the primary key is 0, a prefix of the clustering key (`ck1`) is `0`, and the following column of the clustering key (`ck2`) is in the range `(0, 3)`:
   ```cql
   DELETE FROM ks.t WHERE pk = 0 and ck1 = 0 AND ck2 > 0 AND ck2 < 3;
   ```
3. Display the contents of the table:
   ```cql
   SELECT "cdc$batch_seq_no", pk, ck1, ck2, ck3, v, "cdc$operation" FROM ks.t_scylla_cdc_log;
   ```

   returns:
   ```none
    cdc$batch_seq_no | pk | ck1 | ck2 | ck3  | v    | cdc$operation
   ------------------+----+-----+-----+------+------+---------------
                   0 |  0 |   0 |   0 | null | null |             6
                   1 |  0 |   0 |   3 | null | null |             8

   (2 rows)
   ```

## Partition deletion

An example for deleting a partition is as follows:

1. Start with a basic table and insert some data into it.
   ```cql
   CREATE TABLE ks.t (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH cdc = {'enabled':'true'};
   INSERT INTO ks.t (pk,ck,v) VALUES (0,0,0);
   INSERT INTO ks.t (pk,ck,v) VALUES (0,1,1);
   ```
2. Display the contents of the base table
   ```cql
   SELECT * FROM ks.t;
   ```

   returns:
   ```none
    pk | ck | v
   ----+----+---
     0 |  0 | 0
     0 |  1 | 1

   (2 rows)
   ```
3. Remove all rows where the primary key is equal to 0.
   ```cql
   DELETE FROM ks.t WHERE pk = 0;
   ```
4. Display the contents of the corresponding CDC log table:
   ```cql
   SELECT "cdc$batch_seq_no", pk, ck, v, "cdc$operation" FROM ks.t_scylla_cdc_log;
   ```

   returns:
   ```none
    cdc$batch_seq_no | pk | ck   | v    | cdc$operation
   ------------------+----+------+------+---------------
                   0 |  0 |    0 |    0 |             2
                   0 |  0 |    1 |    1 |             2
                   0 |  0 | null | null |             4

   (3 rows)
   ```

In the CDC log, partition deletion is identified by `cdc$operation = 4`. Columns in the log that correspond to clustering key and non-key columns in the base table are set to `null`.
