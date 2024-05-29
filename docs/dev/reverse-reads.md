# Reverse reads

A read is called reverse when it reads with reverse clustering order
(compared to that of the schema). Example:

    CREATE TABLE mytable (
        pk int,
        ck int,
        s int STATIC,
        v int,
        PRIMARY KEY (pk, ck)
    ) WITH
        CLUSTERING ORDER BY (ck ASC);

    # Forward read (using table's native order)
    SELECT * FROM mytable WHERE pk = 1;
    # Explicit forward order
    SELECT * FROM mytable WHERE pk = 1 ORDER BY ck ASC;

    # Reverse read
    SELECT * FROM mytable WHERE pk = 1 ORDER BY ck DESC;

If the table's native clustering order is DESC, then a read with ASC
order is considered reverse.

## Native format

The native format uses ordering equivalent to that of a table with
reverse clustering format. Using `mytable` as an example, the native
reverse format would be an identical table `my_reverse_table`, which
uses `CLUSTERING ORDER BY (ck DESC);`. This allows middle layers in a
read pipeline to just use a schema with reversed clustering order and
process the reverse stream as normal.

### Request

The `query::partition_slice::options::reversed` flag is set.
Clustering ranges in both `query::partition_slice::_row_ranges` and
`query::specific_ranges::_ranges`
(`query::partition_slice::_specific_ranges`)
are reversed: they are ordered in reverse. Example:

For the clustering keys (ASC order): `ck1`, `ck2`, `ck3`, `ck4`, `ck5`,
`ck6`.
A `_row_ranges` field of a slice might contain this:

    [ck1, ck2], [ck4, ck5]

The reversed version would look like this:

    [ck5, ck4], [ck2, ck1]

In addition to this, the schema is reversed on the replica, at the start
of the read, so all the reverse-capable and intermediate readers in the
stack get a reversed schema to work with.

### Result

Results are ordered with the reversed clustering order with
the bounds of range-tombstones swapped. For example given the following
partition:

    ps{pk1}, sr{}, cr{ck1}, rt{[ck2, ck4)}, cr{ck2}, cr{ck3}, cr{ck4}, ck{ck5}, pe{}

The native reverse version would look like this:

    ps{pk1}, sr{}, cr{ck5}, cr{ck4}, rt{(ck4, ck2]}, cr{ck3}, cr{ck2}, ck{ck1}, pe{}


Legend:
* ps = partitions-tart
* sr = static-row
* cr = clustering-row
* rt = range-tombstone
* pe = partition-end

