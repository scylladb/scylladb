# Paged queries

Queries can return any amount of data. The amount of data is only found
out when the query is actually executed. This creates all sorts of
resource management problems for the client as well as for the database.
To avoid these problems query results are transmitted in chunks of
limited size, one chunk at a time. After transmitting each chunk the
database stops and waits for the client to request the next one. This is
repeated until the entire result set is transitted. This is called
paging.

The size of pages can be limited by the client by the number of rows
they can contain. There is also a built-in (non-optional) size limit of
1MB. If a page reaches the size limit before it reaches the
client-provided row limit it's called a short page or short read.

To be able to continue the query on the next page the database has to
remember where it stopped. This is done by recording the position where
the query was interrupted when the page was filled in an opaque (to the
client) cookie called the *paging state*. This cookie is transmitted
with every page to the client and the client has to retransmit it to the
database on every page request. Since the paging state is completely
opaque (just a binary blob) to the client it can be used to store other
query-related state besides just the page end position.

## How did they work?

### Single partition queries

The coordinator selects a list of replicas for each partition to send
read requests to (IN queries are considered single partition queries
also).
The set of replicas is selected such that it satisfies required CL. An
additional replica may be selected for a speculative read. All read
requests are sent concurrently.

The replica executes the read request via `database::query()` and when
the page is filled it sends the results back to the coordinator.

At the end of each page, if there is more data expected (either the row
or the memory limits of the page were reached), the coordinator saves
the last partition key and the last clustering key in the paging state.
In case of an IN query, if the data returned from the replicas exceeds
the page size, any excess is discarded. There cannot be excess results
when a single partition is queried, the coordinator requests just
one page worth of data from the replicas.

At the beginning of each page the partition list is adjusted:
* Finished partitions are dropped.
* The partition slice of the currently read partition is adjusted, a
  special clustering range is added so that the read continues after the
  last clustering key.
When a single partition is queried the list contains a single entry.

### Range scans

The coordinator splits the partition range into sub-ranges that are
localized to a single vnode. It then dispatches read requests for these
sub-ranges to enough replicas to satisfy CL requirements. The reads
start with a concurrency of 1, that is a single vnode is read at a time,
exponentially increasing it if the results didn’t fill the page.

On the replica the range is further split into sub-ranges that are
localized to a single shard using
`dht::ring_position_exponential_vector_sharder`. The sharder will start
reading a single sub-range exponentially increasing concurrency (reading
more and more shard-local sub ranges concurrently) until the page is
filled. Each read is executed with `database::query_mutations()`. The
results from these individual reads are then merged and sent back to the
coordinator. Care is taken to only send to the coordinator the exact
amount of data it requested. If the last round of read from the shards
yielded so much data that the page is overflown any extra data is
discarded.

The coordinator merges results from all read requests. If there are too
many results excess rows and/or partitions are discarded.

At the beginning of each page, similarly to single partition queries, the
partition range is adjusted:
* The lower bound of the range is set to the last partition of the last
  page.
* The partition slice of the currently read partition is adjusted, a
  special clustering range is added so that the read continues after the
  last clustering key.

## Stateful queries

Before, for paged queries we threw away all readers and any associated
state accumulated during filling the page, and on the next page we
created them from scratch again. Thus on each page we threw away a
considerable amount of work, only to redo it again on the next page.
This significantly increased latency and reduced throughput as from the
point of view of a replica each page is as much work as a fresh query.

The solution is to make queries stateful: instead of throwing away all
state related to a query after filling the page on each replica, save
this state in a cache and on the next page reuse it to continue the
query where it was left off.

### Single partition queries

#### The querier

The essence of making queries stateful is saving the readers and any
associated state on the replicas. To make this easy the reader and all
associated objects that are necessary to serve a read on a shard are
wrapped in a querier object which was designed to be suspendable and
resumable, while offering a simple interface to client code.

#### The querier cache

Queriers are saved in a special-purpose cache. Queriers are not reusable
across queries even for those reading from the same table. Different
queries can have different restrictions, order, query time, etc.
Validating all this to test whether a querier can be used for an
arbitrary read request would be high-impossible and error-prone. To
avoid all this each query has a unique identifier (the `query_uuid`).
This identifier is used as the key to the cache under which the querier
is saved.
There is a querier cache object for each shard and it is stored in the
database object of the respective shard.

#### Choosing the same replicas

In order for caching to work each page of a query has to be consistently
read from the same replicas for the entire duration of the query.
Otherwise the read might miss the querier cache and won't be able to
reuse the queriers from the previous page.
To faciliate this the list of replicas used for each page is saved in
the paging state and on the next page the same replicas will be
preferred over other replicas.

#### Putting it all together

##### Coordinator

On the first page of the query the coordinator will generate a unique
identifier for the query. This identifier will be transmitted to the
replicas as part of the read request. The replicas will use this key to
lookup saved queriers from the previous page and save them after filling
the page. On the first page of the query no replicas will have any
cached queriers. To avoid a pointless lookup but even more importantly
to avoid introducing noise into the [diagnostic counters](#diagnostics)
a flag (`is_first_page`) is added to the read request. When this flag is
set replicas will not attempt to lookup queriers from the previous page.

At the end of each page, in addition to what was already saved, the
coordinator saves in the paging state:
* The `query_uuid`.
* The list of replicas used for the page (`last_replicas`).
* The [read repair decision](#probabilistic-read-repair)

##### Replica

At the start of each page, if `query_uuid` is set and `is_first_page` is
`false` a lookup of the querier from the last page will be attempted. If
this succeeds the querier will be removed from the cache and reused for
continuing the read. If it fails a new one will be created and used for
the remainder of the query.

At the end of each page, if there is still data left (at least one of
the page limits were reached) the querier is saved again in the cache.
Note that since there is no way to know whether there is more data to be
read without actually reading it the only way to determine whether the
query is done is to look at whether the page is full. If the page is not
full it means there wasn't enough data to fill it and thus the query is
done. On the other hand if the page is full there might be more data to
read. This might result in an empty last page if there was just enough
data to fill the previous page but not more.

##### Read repair

If the coordinator gets different results from the replicas (e.g.
because one of the replicas missed a write for some reason) it
reconciles them. This will result in some replicas having queriers with
the wrong position on the next page. For example replicas that sent
rows that are now dead (missed some deletes) will get a new page start
position that is ahead of their saved querier's while replicas that
excluded some rows (missed some writes) will get a new page start
position that is behind their saved querier's.

Since readers cannot be rewound to an earlier position the saved querier
has to be discarded and a new one created on these replicas. To identify
these cases on each cache lookup the position of the found querier is
validated to match *exactly* the new page's read start position. When a
mismatch is detected the saved querier is dropped and a new one is
created instead. Note that altough readers can technically be
fast-forwarded to a later position all position mismatches are treated
the same (querier is dropped) even if the reader could theoretically be
fast-forwarded to the page start position. The reason for this is that
using readers that could do that would results in significantly more
complicated code and also reduced performance.

##### Discarded results

As already mentioned, in the case of IN queries a page may be
over-filled as all partitions are read concurrently. In this case the
coordinator will discard any extra rows to fit the results into the page
limits. This poses a problem for cached queriers as those queriers,
whose results were partly or fully discarded will receive a read request
on the next page, with a start position that they already passed. The
position validation introduced in [read repair](#read-repair) will also
catch these position mismatches and the saved querier will be dropped.

##### Schema upgrades

The schema of the read table can change between two pages. Dealing with
this properly would be complicated and would not be worth the effort. So
on lookup the schema versions are also checked and in case the cached
querier's schema version differs from that of the new page's schema's it
is dropped and a new querier is created instead.

##### Concurrent reads against the same shard of the same replica

In the case of an IN query two listed partitions might be colocated on
the same shard of the same replica. This will result in two concurrent
read requests (reading different partitions) executing on said shard,
both attempting to save and/or lookup queriers using the same
`query_uuid`. This can result in the lookup finding a querier
which is reading another partition. To avoid this, on lookup, the
partition each found querier is reading is matched with that of the read
request. In case when no matching querier is found a new querier is
created as if the lookup missed.

##### Probabilistic read repair

On each page of a query there is a chance (user-changable property of
the table) that a read-repair will be attempted. This hurts stateful
queries as each page has a chance of using additional replicas in the
query and on the next page not use some of them. This will result in
cache misses when new replicas are involved and querier drops when these
abandoned replicas will be attempted to be used again (the read position
of the saved queriers that were neglected for some pages will not match
the current one). To solve this problem we make the read repair decision
apply to an entire query instead of a single page. Make it on the first
page and stick to it for the entire duration of the query. The read
repair decision is generated on the first page and saved in the paging
state to be remembered for the duration of the query.

##### Cache eviction

###### Time based

Reads may be abandoned by the client or the coordinator may chose to use
a different replica for the remainder of the query. To avoid abandoned
queriers accumulating in the cache each cached querier has a TTL. After
this expires it is evicted from the cache.

###### Resource based

The concurrency of reads executing on a given shard is limited to avoid
unbounded resource usage. For this reason each reader needs to obtain a
permit before it can start reading and holds on to this permit until it
is destroyed. Suspended readers (those that are part of a cached querier
object) also hold on to their permit and thus may prevent new readers
from being admitted to read. Since new, active readers should be
preferred over suspended ones, when there is a shortage of permits,
queriers are evicted from the cache until enough permits are recovered
to admit all new readers, or until the cache is empty. Queriers are
evicted in LRU order.

#### Diagnostics

To observe the effectiveness of the caching, as well as aid in finding
any problems a number of counters are added:
1. `querier_cache_lookups` counts the total number of querier cache
  lookups. Not all page-fetches will result in a querier lookup. For
  example the first page of a query will not do a lookup as there was no
  previous page to reuse the querier from. The second, and all
  subsequent pages however should attempt to reuse the querier from the
  previous page.
2. `querier_cache_misses` counts the subset of (1) where the read have
  missed the querier cache (failed to find a saved querier with a
  read-range matching that of the page).
3. `querier_cache_drops` counts the subset of (1) where a saved querier
  was found with a matching read range but it cannot be used to continue
  the read for other reasons so it was dropped. This can happen for
  example if the querier was at the wrong position.
4. `querier_cache_time_based_evictions` counts the cached entries that
  were evicted due to their TTL expiring.
5. `querier_cache_resource_based_evictions` counts the cached entries
  that were evicted due to reader-resource (those limited by
  reader-concurrency limits) shortage.
6. `querier_cache_querier_population` is the current number of querier
  entries in the cache.

Note:
* The count of cache hits can be derived from these counters as
(1) - (2).
* A cache drop (3) also implies a cache hit (see above). This means that
the number of actually reused queriers is: (1) - (2) - (3)

Counters (2) to (6) are soft badness counters. They might be non-zero in
a healthy cluster but high values or sudden spikes can indicate
problems.

### Range scans

Stateful range scans are built on top of the infrastructure introduced
for stateful single partition queries. That is, reads on replicas are
done using a querier objects that are wrapping a reader which executes
the actual read. This querier is then saved in a cache (`querier_cache`)
at the end of the page and is reused on the next page. The major
difference is that as opposed to single partition reads range scans read
from all shards on a replica.

#### multishard_combining_reader

Using the querier mandates using a `flat_mutation_reader`. Range scans
used an open-coded algorithm on the replica for the read. As already
explained in [the introduction](#range-scans) this algorithm
uses several calls to `database::query_muations()` to the remote shards
then merging the produced `reconcilable_result`. This algoritm did not
lend itself for being wrapped in a `flat_mutation_reader` so a new,
suitable one was written from scratch. This is
`multishard_combining_reader`. In addition to implementing a
multishard-reading algorithm that is suspendable, an effort was made to
solve some of the weak points of the previous open-coded implementation,
mainly cold start and result merging.

#### query_mutations_on_all_shards()

Implementing a stateful range scan using `multishard_combining_reader`,
`querier` and `querier_cache` still has a lot of involved details to
it. To make this as accessible and resuable as possible a function was
added that takes care of all this, offering a simple interface to
clients. This is `query_mutations_on_all_shards()`, which takes care of
all details related to replica local range scans. It supports both
stateful and stateless queries transparently.

#### Suspending and resuming the multishard_combining_reader

Saving the `multishard_combining_reader` in a querier would be the
natural choice for saving the query state. This however would create
some serious problems:
* It is not enough to just save the `multishard_combining_reader` reader
  in a querier. All the shard readers have to be saved on their home
  shard and made individually evictable as well but this has to be
  transparent to the multishard reader which, being a plain
  `flat_mutation_reader`, has no way to be told that the query is
  "suspended" and later "resumed" and thus could not do the
  save/lookup/recreate itself.
* It mandates the consistent usage of the same shard throughout all the
  pages of the query. This is problematic for load balancing.
* The querier wrapping the `multishard_combining_reader` would be a
  single point of failure for the entire query. If evicted the entire
  saved state (all shard readers) would have to be dropped as well.

While some of these issues could be worked around and others could be
lived with, overall they make this option unfeasable.

An alternative but less natural option is to "dismantle" the multishard
reader, that is remove and take ownership of all its shard readers and
move any fragments that were popped from a shard reader but not
consumed (included in the results) back to their originating shard
reader, so that only the shard readers need to be saved and resumed. In
other words move all state required to suspend/resume the query into the
shard readers.

This option addresses all the problems the "natural" option has:
* All shard readers are independently evictable.
* No coordinator shard is needed, each page request can be executed on
  any shard.
* Evicting a shard reader has no effect on the remaining ones.

Of course it also has it own problems:
* On each page a certain amount of work is undone by moving some
  fragments back to their original readers.
* The concurrency state of the multishard reader is lost and it has to
  start from 1 on the next page.

But these problems are much more manageable and some (for example the
last) can be worked around if found to be a real problem.

#### Putting it all together

##### Coordinator

In principle the same as that for [single partition queries](#coordinator).

##### Replica

The storage proxy can now simply call
[query_mutations_on_all_shards()](#query_mutations_on_all_shards) with
the appropriate parameters which takes care of executing the read,
including saving and reusing the shard readers.

#### Diagnostics

Additional counters are added to detect possible problems with stateful
range scans:
1. `multishard_query_unpopped_fragments`
2. `multishard_query_unpopped_bytes`
3. `multishard_query_failed_reader_stops`
4. `multishard_query_failed_reader_saves`

(1) and (2) track the amount of data pushed back to shard readers while
[dismantling](#suspending-and-resuming-the-multishard_combining_reader)
the multishard reader. These are soft badness counters, they will not be
zero in a normally operating cluster, however sudden spikes in their
values can indicate problems.
(3) tracks the number of times stopping any of the shard readers failed.
Shard readers are said to be stopped when the page is filled, that is
any pending read-ahead is waited upon. Since saving a reader will not
fail the read itself these failures will normally go undetected. To
avoid hiding any bug or problem due to this, track these background
failures using this counters. This counter is a hard badness counter,
that is it should *always* be zero. Any other value indicates problems
in the respective shard/node.
(4) tracks the number of times saving the reader failed. This only
includes preparing the querier object and inserting it into the querier
cache. Like (3) this is a hard badness counter.

## Future work

Since the protocol specifications allows for over or underfilling a
page in the future we might get rid of discarding results on the
coordinator to free ourselves from all the [problems](#discarded-results)
it causes.

The present state optimizes for range scans on huge tables, where the
page is filled from a single shard of a single vnode. Further
optimizations are possible for scans on smaller tables, that have to
cross shards or even vnodes to fill the page. One obvious candidate is
saving and restoring the current concurrency on the coordinator (how
many vnodes have to be read concurrently) and on the replica (how many
shards we should read-ahead on).

## Further reading

* [querier.hh](https://github.com/scylladb/scylla/blob/master/querier.hh) `querier` and `querier_cache`.
* [multishard_mutation_query.hh](https://github.com/scylladb/scylla/blob/master/multishard_mutation_query.hh)
  `query_mutations_on_all_shards()`.
* [mutation_reader.hh](https://github.com/scylladb/scylla/blob/master/mutation_reader.hh)
  `multishard_combining_reader`.
