Reader concurrency semaphore
----------------------------

The role of the reader concurrency semaphore is to keep resource consumption of reads under a certain limit.
Each read has to obtain a permit before it is started. Permits are only issued when there are available resources to start a new read.
For more details on its API, check [reader_concurrency_semaphore.hh](../../reader_concurrency_semaphore.hh).

There is a separate reader concurrency for each scheduling group:
* statement (user reads) - 100 count and 2% of shard memory (queue size: 2% memory / 1KB)
* system (internal reads) - 10 count and 2% of shard memory (no queue limit)
* streaming (maintenance operations) - 10 count and 2% of shard memory (no queue limit)

There are 3 main ways to create permits:
* obtain_permit() - this is the most generic way to obtain a permit. The method creates a permit, waits for admission (if necessary) and then returns the permit to be used.
* with_permit() - the permit is created and then waits for admission as with obtain_permit(). But instead of returning the admitted permit, this method runs the functor passed in as its func parameter once the permit is admitted. This facilitates batch-running cache reads. If a permit is already available (saved paged read resuming), `with_ready_permit()` can be used to benefit of the batching.
* make_tracking_only_permit() - make a permit that bypasses admission and is only used to keep track of the memory consumption of a read. Used in places that don't want to wait for admission.

A permit is admitted if the following conditions are met:
* There are enough resources to admit the read. Currently, each permit takes 1 count resource and 128K memory resource on admission.
* There are no reads which currently only need CPU to make further progress. Permits can opt-in to participate in this criteria (block other permits from being admitted, while they need more CPU) by being marked as "need_cpu".

Reader concurrency semaphore diagnostic dumps
=============================================

When a read waiting to obtain a permit times out, or if the wait queue of the reader concurrency semaphore overflows, the reader concurrency semaphore will dump diagnostics to the logs, with the aim of helping users to diagnose the problem.
Example diagnostics dump:

    [shard 1] reader_concurrency_semaphore - Semaphore _read_concurrency_sem with 35/100 count and 14858525/209715200 memory resources: timed out, dumping permit diagnostics:
    permits count   memory  table/description/state
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

Permits are grouped by table, description, and state, while groups are sorted by memory consumption.
The first group in this example contains 34 permits, all for reads against table `ks1.table1_mv_0`, all data-query reads and in state `active/await`.

Permits have the following states:
* waiting - the permit is waiting for admission;
* active - the permit was admitted;
* active/need_cpu - the permit was admitted and it participates in CPU based admission;
* active/await - a previously active/need_cpu permit, which needs something other than CPU to proceed, it is waiting on I/O or a remote shards;
* inactive - the read was marked inactive, it can be evicted to make room for admitting more permits if needed;
* evicted - the read was inactive and then evicted;

The dump can reveal what the bottleneck holding up the reads is:
* CPU - there will be one active/need_cpu permit (there might be active/await and active permits too), both count and memory resources are available (not maxed out);
* Disk - count resource is maxed out by active/await permits using up all count resources;
* Memory - memory resource is maxed out (usually even above the limit);

There might be inactive reads if CPU is a bottleneck; otherwise, there shouldn't be any (they should be evicted to free up resources).
