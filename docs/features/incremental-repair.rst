.. _incremental-repair:

Incremental Repair
==================

ScyllaDB's standard repair process scans and processes all the data on a node, regardless of whether it has changed since the last repair. This operation can be resource-intensive and time-consuming. The Incremental Repair feature provides a much more efficient and lightweight alternative for maintaining data consistency.

The core idea of incremental repair is to repair only the data that has been written or changed since the last repair was run. It intelligently skips data that has already been verified, dramatically reducing the time, I/O, and CPU resources required for the repair operation.

How It Works
------------

ScyllaDB keeps track of the repair status of its data files (SSTables). When new data is written or existing data is modified, it is considered "unrepaired." When you run an incremental repair, the process works as follows:

1.  ScyllaDB identifies and selects only the SSTables containing unrepaired data.
2.  It then synchronizes this data across the replica nodes.
3.  Once the data is successfully synchronized, the corresponding SSTables are marked as "repaired."

Subsequent incremental repairs will skip these marked SSTables, focusing only on new data that has arrived since. To ensure data integrity, ScyllaDB's compaction process handles repaired and unrepaired SSTables separately.

This approach is highly efficient because it allows entire SSTables to be skipped, avoiding the overhead of reading and processing unchanged data.

Prerequisites
-------------

Incremental Repair is only supported for tables that use the tablets architecture. It is not available for legacy vnode-based tables.

Incremental Repair Modes
------------------------

Incremental is currently disabled by default. You can control its behavior for a given repair operation using the ``incremental_mode`` parameter.
This is useful for enabling incremental repair, or in situations where you might need to force a full data validation.

The available modes are:

*   ``incremental``: Performs a standard incremental repair. It processes only unrepaired data and skips SSTables that are already marked as repaired. The repair status is updated after the operation.
*   ``full``: Forces the repair to process **all** SSTables, including those that have been previously repaired. This is useful when a complete data validation is required. The repair status is updated upon completion.
*   ``disabled``: Completely disables the incremental repair logic for the current operation. The repair behaves like a classic, non-incremental repair, and it does not read or update any incremental repair status markers.


The incremental_mode parameter can be specified using nodetool cluster repair, e.g., nodetool cluster repair --incremental-mode incremental. It can also be specified with the REST API, e.g., curl -X POST "http://127.0.0.1:10000/storage_service/tablets/repair?ks=ks1&table=tb1&tokens=all&incremental_mode=incremental"

Benefits of Incremental Repair
------------------------------

*   **Faster Repairs:** By targeting only new or changed data, repair operations complete in a fraction of the time.
*   **Reduced Resource Usage:** Consumes significantly less CPU, I/O, and network bandwidth compared to a full repair.
*   **More Frequent Repairs:** The efficiency of incremental repair allows you to run it more frequently, ensuring a higher level of data consistency across your cluster at all times.

Notes
-----

With the incremental repair feature, the repaired and unrepaired SSTables are compacted separately. After incremental repair, unrepaired SSTables become repaired SSTables, allowing them to be compacted together. A shorter repair interval is therefore recommended to mitigate potential space amplification resulting from these separate compactions.
