# Reader concurrency semaphore

The role of the reader concurrency semaphore is to keep resource consumption of reads under a given limit.
The semaphore manages two kinds of resources: memory and "count". The former is a kind of "don't go crazy" limit on the maximum number of concurrent reads.
This memory limit is expressed as a certain percentage of the shard's memory and it is defined in the code, not user configurable.
There is a separate reader concurrency semaphore for each scheduling group:
* `statement` (user reads) - 100 count and 2% of shard memory (queue size: 2% memory / 1KB)
* `system` (internal reads) - 10 count and 2% of shard memory (no queue limit)
* `streaming` (maintenance operations) - 10 count and 2% of shard memory (no queue limit)

On enterprise releases, the `statement` scheduling group is broken up into a per workload prioritization group semaphore. Each such semaphore has 100 count resources and a share of the memory limit proportional to its shares.

## Admission

Reads interact with the semaphore via a permit object. The permit is created when the read starts on the replica. Creating the permit involves waiting for the conditions to be appropriate for allowing the read to start, this is called admission.
When the permit object is returned to the read, it is said to be admitted. The read can start at that point.

For a permit to be admitted, the following conditions have to be true:
* There are enough resources to admit the permit. Currently, each permit takes 1 count resource and 128K memory resource on admission.
* There are no reads which currently only need CPU to make further progress. Permits can opt-in to participate in this criteria (block other permits from being admitted, while they need more CPU) by being marked as `need_cpu`.

API wise, there are 3 main ways to create permits:
* `obtain_permit()` - this is the most generic way to obtain a permit. The method creates a permit, waits for admission (this might be immediate if the conditions are right) and then returns the permit to be used.
* `with_permit()` - the permit is created and then waits for admission as with `obtain_permit()`. But instead of returning the admitted permit, this method runs the functor passed in as its func parameter once the permit is admitted. This facilitates batch-running cache reads. If a permit is already available (saved paged read resuming), `with_ready_permit()` can be used to benefit of the batching.
* `make_tracking_only_permit()` - make a permit that bypasses admission and is only used to keep track of the memory consumption of a read. Used in places that don't want to wait for admission.

For more details on the reader concurrency semaphore's API, check [reader_concurrency_semaphore.hh](../../reader_concurrency_semaphore.hh).

## Inactive Reads

Permits can be registered as "inactive". This means that the reader object associated with the permit will be kept around only as long as resource consumption is below the semaphore's limit. Otherwise, the reader object will be evicted (destroyed) to free up resources. Evicted permits have to be re-admitted to continue the read.

This is used in multiple places, but in general it is used to cache readers between different pages of a query.
Making reads inactive is also used to prevent deadlocks, where a single process has to obtain permits on multiple shards. To avoid deadlocks, the process marks all shards it is not currently using, as inactive, to allow a concurrent process to be able to obtain permits on those shards. Repair and multi-shard reads mark unused shard readers as inactive for this purpose.

Inactive reads are only evicted when their eviction can potentially allow for permits currently waiting on admission to be admitted. So for example if admission is blocked by lack of memory, inactive reads will be evicted. If admission is blocked by some permit being marked as `need_cpu`, inactive readers will not be evicted.

## Anti-OOM Protection

The semaphore has anti-OOM protection measures. This is governed by two limits:
* `serialize_limit_multiplier`
* `kill_limit_multiplier`

Both limits are multipliers and the final limit is calculated by multiplying them with the semaphore's memory limit. So e.g. a `serialize_limit_multiplier` limit of `2` means that the protection menchanism is activated when the memory consumption of the current reads reaches the semaphore limit times two.

After reaching the serialize limit, requests for more memory are queued for all reads except one, which is called the blessed read. The hope is that if only one read is allowed to progress at a time, the memory consumption will not balloon any more. When the memory consumption goes back below the serialize limit, reads are again allowed to progress in parallel.
Note that participation in this is opt-in for reads, in that there is a separate accounting method for registering memory consumption, which participates in this system. Currently only memory request on behalf of I/O use this API.

When reaching the kill limit, the semaphore will start throwing `std::bad_alloc` from all memory consumption registering API calls. This is a drastic measure which will result in reads being killed. This is meant to provide a hard upper limit on the memory consumption of all reads.

## Permit States

Permits are in one of the following states:
* `waiting_for_admission` - the permit is waiting for admission;
* `waiting_for_memory` - the permit is waiting for memory to become available to continue (see serialize limit);
* `waiting_for_execution` - the permit was admitted and is waiting to be executed in the execution stage (see admission via `with_permit()` and `with_ready_permit()`);
* `active` - the permit was admitted and the read is in progress;
* `active/need_cpu` - the permit was admitted and it participates in CPU based admission, i.e. it blocks other permits from admission while in this state;
* `active/await` - a previously `active/need_cpu` permit, which needs something other than CPU to proceed, it is waiting on I/O or a remote shards, other permits can be admitted while the permit is in this state, pending resource availability;
* `inactive` - the permit was marked inactive, it can be evicted to make room for admitting more permits if needed;
* `evicted` - a former inactive permit which was evicted, the permit has to undergo admission again for the read to resume;

Note that some older releases will have different names for some of these states or lack some of the states altogether:

Changes in 5.3:
* `active/unused` -> `active`;
* `active/used` -> `active/need_cpu`;
* `active/blocked` -> `active/await`;

Changes in 5.2:
* Changed: `waiting` -> `waiting_for_admission`;
* Added: `waiting_for_memory` and `waiting_for_execution`;

Reader concurrency semaphore diagnostic dumps
---------------------------------------------

When a read waiting to obtain a permit times out, or if the wait queue of the reader concurrency semaphore overflows, the reader concurrency semaphore will dump diagnostics to the logs, with the aim of helping users to diagnose the problem.
Example diagnostics dump:

    [shard 1] reader_concurrency_semaphore - Semaphore _read_concurrency_sem with 35/100 count and 14858525/209715200 memory resources: timed out, dumping permit diagnostics:
    permits count   memory  table/operation/state
    34  34  14M ks1.table1_mv_0/data-query/active/await
    1   1   16K ks1.table1_mv_0/data-query/active/need_cpu
    7   0   0B  ks1.table1/data-query/waiting
    1251    0   0B  ks1.table1_mv_0/data-query/waiting

    1293    35  14M total

    Total: 1293 permits with 35 count and 14M memory resources

Note that the diagnostics dump logging is rate limited to 1 in 30 seconds (as timeouts usually come in bursts). You might also see a message to this effect.

The dump contains the following information:
* The semaphore's name: `_read_concurrency_sem`;
* Currently used count resources: 35;
* Limit of count resources: 100;
* Currently used memory resources: 14858525;
* Limit of memory resources: 209715200;
* Dump of the permit states;

Permits are grouped by table, operation (see below), and state, while groups are sorted by memory consumption.
The first group in this example contains 34 permits, all for reads against table `ks1.table1_mv_0`, all data-query reads and in state `active/await`.

The dump can reveal what the bottleneck holding up the reads is:
* CPU - there will be one `active/need_cpu` permit (there might be `active/await` and active permits too), both count and memory resources are available (not maxed out);
* Disk - count resource is maxed out by active/await permits using up all count resources;
* Memory - memory resource is maxed out (usually even above the limit);

There might be inactive reads if CPU is a bottleneck; otherwise, there shouldn't be any (they should be evicted to free up resources).

### Operations

Table of all permit operations possibly contained in diagnostic dumps, from the user or system semaphore:

| Operation name                                | Description                                                                                                                  |
| --------------------------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| `counter-read-before-write`                   | Read-before-write done on counter writes.                                                                                    |
| `data-query`                                  | Regular single-partition query.                                                                                              |
| `multishard-mutation-query`                   | Part of a range-scan, which runs on the coordinator-shard of the scan.                                                       |
| `mutation-query`                              | Single-partition read done on behalf of a read-repair.                                                                       |
| `push-view-updates-1`                         | Reader which reads the applied base-table mutation, when it needs view update generation (no read-before-write needed).      |
| `push-view-updates-2`                         | Reader which reads the applied base-table mutation, when it needs view update generation (read-before-write is also needed). |
| `shard-reader`                                | Part of a range-scan, which runs on replica-shards.                                                                          |


Table of all permit operations possibly contained in diagnostic dumps, from the streaming semaphore:

| Operation name                                | Description                                                                                                                  |
| --------------------------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| `repair-meta`                                 | Repair reader.                                                                                                               |
| `sstables_loader::load_and_stream()`          | Sstable reader, reading sstables on behalf of load-and-stream.                                                               |
| `stream-session`                              | Permit created for streaming (receiver side).                                                                                |
| `stream-transfer-task`                        | Permit created for streaming (sender side).                                                                                  |
| `view_builder`                                | Permit created for the view-builder service.                                                                                 |
| `view_update_generator`                       | Reader which reads the staging sstables, for which view updates have to be generated.                                        |
