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

## Legacy format

The legacy format is how scylla handled reverse queries internally. We
are in the process of migrating to the native reverse format, but for
now coordinator-side code still uses the legacy format.

### Request

The `query::partition_slice::options::reversed` flag is set.
Clustering ranges in both `query::partition_slice::_row_ranges` and
`query::specific_ranges::_ranges`
(`query::partition_slice::_specific_ranges`) are half-reversed: they
are ordered in reverse, but when they are compared to other
mutation-fragments, their end bound is used as position, instead of the
start bound as usual. When compared to other clustering ranges the end
bound is used as the start bound and vice-versa.
Example:

For the clustering keys (ASC order): `ck1`, `ck2`, `ck3`, `ck4`, `ck5`,
`ck6`.
A `_row_ranges` field of a slice might contain this:

    [ck1, ck2], [ck4, ck5]

The legacy reversed version would look like this:

    [ck4, ck5], [ck1, ck2]

Note how the ranges themselves are the same (bounds not reversed), it is
just the range vector itself that is reversed.

### Result

Results are ordered with the reversed clustering order with the caveat
that range-tombstones are ordered by their end bound, using the native
schema's comparators. For example given the following partition:

    ps{pk1}, sr{}, cr{ck1}, rt{[ck2, ck4)}, cr{ck2}, cr{ck3}, cr{ck4}, ck{ck5}, pe{}

The legacy reverse format equivalent of this looks like the following:

    ps{pk1}, sr{}, cr{ck5}, rt{[ck2, ck4)}, cr{ck4}, cr{ck3}, cr{ck2}, ck{ck1}, pe{}

Note:
* Only clustering elements change;
* Range tombstone's bounds are not reversed;
* Range tombstones can be ordered off-by-one due to native schema
  comparators used: `rt{[ck2, ck4)}` should be ordered *after*
  `cr{ck4}`.

Legend:
* ps = partitions-tart
* sr = static-row
* cr = clustering-row
* rt = range-tombstone
* pe = partition-end

## Native format

The native format uses ordering equivalent to that of a table with
reverse clustering format. Using `mytable` as an example, the native
reverse format would be an identical table `my_reverse_table`, which
uses `CLUSTERING ORDER BY (ck DESC);`. This allows middle layers in a
read pipeline to just use a schema with reversed clustering order and
process the reverse stream as normal.

### Request

The `query::partition_slice::options::reversed` flag is set as in the
legacy format. Clustering ranges in both
`query::partition_slice::_row_ranges` and
`query::specific_ranges::_ranges`
(`query::partition_slice::_specific_ranges`) are fully-reversed: they
are ordered in reverse, their bound being swapped as well.
Example:

For the clustering keys (ASC order): `ck1`, `ck2`, `ck3`, `ck4`, `ck5`,
`ck6`.
A `_row_ranges` field of a slice might contain this:

    [ck1, ck2], [ck4, ck5]

The native reversed version would look like this:

    [ck5, ck4], [ck2, ck1]

In addition to this, the schema is reversed on the replica, at the start
of the read, so all the reverse-capable and intermediate readers in the
stack get a reversed schema to work with.

### Result

Results are ordered with the reversed clustering order with
the bounds of range-tombstones swapped. For example, given the same
partition that was used in the legacy format example, the native reverse
version would look like this:

    ps{pk1}, sr{}, cr{ck5}, cr{ck4}, rt{(ck4, ck2]}, cr{ck3}, cr{ck2}, ck{ck1}, pe{}
