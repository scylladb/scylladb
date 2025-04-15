# Scylla CQL extensions

Scylla extends the CQL language to provide a few extra features. This document
lists those extensions.

## SCYLLA_TIMEUUID_LIST_INDEX function

The `SCYLLA_TIMEUUID_LIST_INDEX` function allows using the raw (sstable) collection
key for lists, which is a timeuuid, as "index" when performing a modification.

The function is used to wrap the index part of a list assignment:

    UPDATE ... SET list_column[SCYLLA_TIMEUUID_LIST_INDEX(:v1)] = :v2 ... 

It is only used by sstableloader.

## SCYLLA_COUNTER_SHARD_LIST function

The `SCYLLA_COUNTER_SHARD_LIST` function allows setting a counter column value by 
its internal representation.

The function is used to wrap the value part of a counter assignment:

    UPDATE ... SET counter_column = SCYLLA_COUNTER_SHARD_LIST(:v1) ... 

It is only used by sstableloader.

## SCYLLA_CLUSTERING_BOUND function

The `SCYLLA_CLUSTERING_BOUND` function marks a query bound as being 
a CQL clustering boundary, not a value range bound as in normal CQL.

This function can be used on the right-hand side of a `WHERE` statement multicolumn 
comparison, excluding the IN and LIKE operators. Its parameters are values corresponding 
to the clustering prefix specified on the left-hand side of that comparison.

    UPDATE ... WHERE (c1, c2) < SCYLLA_CLUSTERING_BOUND(:v1, :v2) AND (c1, c2) > SCYLLA_CLUSTERING_BOUND(:v3, :v4) ...

It is only used by sstableloader.
