# The Log-Structured Allocator (LSA)

ScyllaDB keeps most of its in-memory data, the row cache and the memtables,
in memory managed by its own allocator, the *log-structured allocator* (LSA,
implemented in `utils/logalloc.{hh,cc}`). This document has two parts:

* [Part 1](#part-1-introduction) is an introduction for developers who have
  never heard of LSA: what problem it solves, the main idea, and what it costs.
* Parts [2](#part-2-how-lsa-works) to [6](#part-6-where-lsa-lives-in-the-source-code)
  are a reference for developers who write or review code that uses LSA:
  how it works, how to use it correctly and efficiently, the pitfalls, how to
  debug it, and where its code is.

If you only want the rules, read the [checklist](#part-4-pitfalls-checklist).

# Part 1: Introduction

## Why ScyllaDB needs its own allocator

A Seastar application owns its memory. At startup each shard gets a fixed
part of the machine's memory, and Seastar's allocator (`seastar::memory`)
manages it. There is no swap to fall back on, and memory is not returned to
the operating system.

ScyllaDB uses *all* of this memory, and much of it is the **cache**: whatever
memory the rest of the system does not need at the moment holds cached data
(the row cache, and also sstable index caches). So, by design, memory is
always nearly full of cache, which leads to three problems:

1. **The cache must give memory back on demand.** When some other component
   (a query, a compaction, an RPC buffer) needs memory, the cache must shrink
   to make room for it. Usually it does so ahead of time: LSA keeps some
   memory free and refills it in the background (see
   [Reclamation](#reclamation)). But when an allocation finds no free memory,
   or no free range as large as it needs, the cache must shrink *right now*,
   synchronously, inside that allocation. Seastar lets an application
   register a *reclaimer* for this.

2. **Freeing scattered small objects does not produce usable memory.** Suppose
   the cache is made of millions of 100-byte objects allocated with `malloc()`,
   and someone needs 128 KiB of contiguous memory (an I/O buffer, say). Any
   128 KiB range of memory holds about 1,300 cache objects, hot and cold ones
   mixed together, and the range becomes usable only when *all* of them are
   freed. The cache can try two ways to get there, and neither works:

   * **Evict in LRU order.** This frees 100-byte holes at essentially random
     addresses. A range becomes completely empty only when its last object is
     evicted, and since every range contains some recently used objects, by the
     time any range is empty nearly the whole cache has been evicted.
   * **Evict a chosen range.** This would need to know which objects live
     between two addresses and who points to them, so they can be removed from
     the cache's data structures, and `malloc()` gives the cache no way to find
     either. Even if it could, it would throw away hot objects just because of
     where they happen to live.

   This is *memory fragmentation*: plenty of memory is free, but not in the
   shape that is needed. In a long-running database whose objects have
   different sizes and lifetimes it is a real problem, not a theoretical one.
   LSA solves it by knowing which objects are in each part of memory and
   *moving* them out of the way instead of evicting them, as explained below.

3. **We need to know how much memory a data structure uses.** Memtables are
   flushed according to how much memory they use, and the cache reports its
   size. With `malloc()`, a data structure can only add up the sizes of its own
   objects, by asking the allocator for each object's real size
   (`malloc_usable_size()`) when it is allocated and freed. That costs work on
   every allocation, and it still misses fragmentation: the free holes left
   between the data structure's objects, which nobody else can use either.

## The idea: allocate like a log, and compact

LSA follows the idea of the paper [Log-structured Memory for DRAM-based Storage](https://www.usenix.org/conference/fast14/technical-sessions/presentation/rumble)
(Rumble, Kejriwal and Ousterhout, FAST 2014), which the RAMCloud key-value
store uses:

* Memory is divided into fixed-size **segments** (128 KiB in ScyllaDB).
* New objects are **appended** to the current segment: allocation is just a
  pointer bump.
* Freeing an object only marks it dead, which leaves a **hole** in its segment.
* When memory is needed, LSA picks the sparsest segments and **compacts**
  them: it copies their live objects into a new segment, and the old segment
  becomes entirely free. A free segment is a 128 KiB contiguous block that
  can go back to Seastar's allocator.
* An LSA user can also make data **evictable**. When compaction alone is not
  enough, LSA asks the user (the cache) to evict some data, which leaves more
  holes for compaction to remove.

So LSA can turn "N bytes of evictable data" into "N bytes of contiguous free
memory" without evicting more than necessary. It also gives cheap and exact
memory accounting per **region** (a group of segments that belongs to one
data structure: one memtable, or the cache), and it can **merge** one region
into another without copying anything, which is how a flushed memtable's data
moves into the cache.

## The catch: objects move

Compaction moves objects to new addresses. Any pointer or reference to an
LSA-managed object can become invalid whenever LSA decides to compact, and
LSA may decide to compact during *any memory allocation, in any allocator*,
because Seastar calls the LSA reclaimer synchronously when it runs out of
free memory. This has consequences:

* **Every object stored in LSA must know how to move itself.** LSA calls its
  move constructor (which must be `noexcept`), and any pointers *to* the
  object must be updated by that move constructor. That is why LSA data
  structures use back-pointers, intrusive containers and special "managed"
  types (`managed_bytes`, `managed_ref`, ...) instead of `std::vector` and
  `std::unique_ptr`. They are listed in
  [LSA-aware containers and helpers](#lsa-aware-containers-and-helpers).
* **References are valid only as long as reclamation is disabled.** Code that
  needs a stable pointer must lock the region against reclamation (with a
  [`reclaim_lock`](#keeping-references-valid-reclaim_lock) or an
  [allocating section](#allocating-sections)), or must re-find the object
  when it continues, for example after a deferring point (see
  [Keeping a position across deferring points](#keeping-a-position-across-deferring-points)).
* **Allocating while reclamation is disabled can fail even when there is
  plenty of reclaimable memory**, because the memory could only have been
  reclaimed from the locked region. The
  [allocating section](#allocating-sections) mechanism deals with this: it
  reserves memory up front, learns from failures how much to reserve, and
  retries the operation.
* **No single object can be larger than about 13 KiB** (10% of a segment).
  Larger values must be split into fragments, which is why `managed_bytes`
  is fragmented (see [Allocation and freeing](#allocation-and-freeing)).

Most of the rest of this document explains how to live with these
consequences.

## The other catch: large allocations can stall

LSA's constraints are not limited to LSA's own data. Because the cache fills
all the memory that nothing else is using, *any* allocation, anywhere in
ScyllaDB and in any allocator, may find no free memory. Usually it does not,
because LSA keeps some memory free in advance. But when that free memory runs
out, or has no free range as large as the allocation needs, LSA must reclaim
memory synchronously, inside the allocation: it compacts segments, copying
their live objects elsewhere, and evicts cache data. This work cannot be
preempted, and the larger the allocation, the more of it there is. An
allocation larger than a segment also needs several freed segments that
together form one contiguous block, which can take even more reclamation,
or fail (see [Why larger allocations are hard](#why-larger-allocations-are-hard)).

So ScyllaDB code avoids large allocations. Seastar logs "oversized
allocation", with a backtrace, for any allocation larger than 128 KiB (the
default of `large_memory_allocation_warning_threshold`). Containers that
keep large data in standard memory split it into chunks of at most 128 KiB.
For example, when a `std::vector` grows, it allocates one block of the new
size, which may stall for as long as it takes to reclaim all of it.
`utils::chunked_vector` allocates one chunk at a time instead, so each
allocation stalls for less time, and code that fills it can preempt between
chunks. The total work is not smaller, only split into shorter stalls.
Other chunked types are `bytes_ostream` and `fragmented_temporary_buffer` for
byte buffers, and `managed_bytes` for values. Inside LSA the limit is much
lower, about 13 KiB, as explained above.

## A short history

LSA was written by Tomasz Grabiec. His 2014 design document for a
log-structured allocator for Seastar's memcached already followed the RAMCloud
paper. The first version of `utils/logalloc.cc` was committed in July 2015
(commit `5a9e296803`), and most of the important pieces were added soon
after: the emergency segment reserve, synchronous reclamation, allocating
sections (September 2015), and *region groups*, which track the total memory
of all memtables in order to limit it. Region groups are no longer part of
LSA: in 2022 they moved into the dirty memory manager (see
[Region listeners and dirty memory accounting](#region-listeners-and-dirty-memory-accounting)).
Later milestones:

* 2015-2017: *zones* of contiguous segments (Paweł Dziepak), replaced in
  2017 by Avi Kivity's simpler scheme of moving one segment at a time and
  placing LSA data at high addresses. Segments were reduced from 256 KiB to
  128 KiB.
* 2017: object descriptors were compressed to typically 1 byte, using a
  ULEB64-like encoding and a migrator *index* instead of a pointer.
* 2018: a debug-mode LSA sanitizer (Paweł Dziepak).
* 2020-2021: reserve decay, background (preemptible) reclaim, `lsa_buffer`,
  `lsa::weak_ptr`, and stall diagnostics for reclamation (`lsa-timing`).
* 2024-2025: `hold_reserve`; allocations above the size limit became an
  internal error instead of silently falling back to the standard allocator.
* 2025: named allocating sections and reclaim/evict/compact time metrics.

## Where LSA is used today

The cache is LSA's biggest user, but not its only one. All LSA memory belongs
to one of two kinds of regions: **memtables** and **the cache region**. Here
is everything that uses them:

| User | Region | Evictable? | Code |
|---|---|---|---|
| **Memtables** of all tables, user and system, holding the writes since the last flush | One region per memtable (`replica::memtable` *is* a `logalloc::region`); its size is reported to the `dirty_memory_manager`, which decides when to flush and when to throttle writes | No: memory is freed by flushing | `replica/memtable.{hh,cc}`, `replica/dirty_memory_manager.{hh,cc}` |
| **Row cache** of all tables | The cache region: one per shard, owned by `cache_tracker` | Yes, through a single LRU shared by all its users | `db/cache_tracker.hh`, `db/row_cache.{hh,cc}`, `db/cache_mutation_reader.hh` |
| **SStable partition index cache**: parsed pages of the partition index. Only sstables with the older index format (`Index.db`) use it; sstables with the newer trie-based index (`Partitions.db` and `Rows.db`) do not | The cache region | Yes | `sstables/partition_index_cache.hh`, `sstables/index_reader.hh` |
| **Cached sstable index file pages** (raw 4 KiB pages of `Index.db`, `Partitions.db` and `Rows.db`) | The cache region, as `lsa_buffer`s | Yes | `utils/cached_file.hh` |
| **Logstor cache**: rows of key-value tables stored in logstor | The cache region | Yes | `replica/logstor/cache.{hh,cc}` |
| **Memtables used as sorting buffers**, not attached to any table: rows of virtual tables (`memtable_filling_virtual_table`), partitions that scrub in segregate mode found out of order (`partition_based_splitting_writer`), and the `scylla sstable write` operation | One temporary region per memtable, not accounted as dirty memory | No: freed when the buffer is consumed | `db/virtual_table.{hh,cc}`, `mutation_writer/partition_based_splitting_writer.cc`, `tools/scylla-sstable/scylla-sstable.cc` |

The two kinds of regions are connected: when a memtable is flushed, its region
is **merged** into the cache region (`row_cache::do_update()` in
`db/row_cache.cc`), and the memtable's data is moved into the cache entry by
entry without copying any bytes.

The *types* used for this data are LSA-aware everywhere, not only inside these
regions. `mutation_partition`, `rows_entry`, `managed_bytes` and the trees
that hold them are the same types that the rest of ScyllaDB uses for
mutations in ordinary (standard allocator) memory, for example in the write
path, in query results and in streaming. That is why `managed_bytes` is
fragmented even when it is not in LSA, and why code far from the cache has to
care about the [current allocator](#allocation-strategies-and-with_allocator).

Not everything near these users is in LSA. For example, the logstor primary
index is a B+ tree in standard memory, and only the cached rows it points to
are in the cache region.

## When to use LSA, and when not to

Use LSA for data that:

* is **long-lived** and can grow to fill a large part of memory, like caches and
  in-memory write buffers;
* consists of **many small objects**, each well under 13 KiB, or of larger
  values that can be split into fragments;
* can be **evicted** when memory is needed, or whose memory is limited and
  accounted in some other way (memtables are flushed);
* is accessed by code that can work under the constraints described here.

Do not use LSA for:

* **short-lived, temporary allocations**, such as data built during a single
  request. The standard allocator is simpler and fast enough. The default
  allocation strategy is the standard allocator, and code should switch to
  LSA only around operations on LSA data structures;
* objects that must have **stable addresses** referenced from many places
  that you cannot update when the object moves;
* objects that are **referenced across deferring points** through raw pointers
  (see [cursors](#keeping-a-position-across-deferring-points) for the
  alternatives);
* data shared between **shards**. A region and its objects belong to one shard.

If you are adding a new cache, consider whether its data can live in the
existing cache region and its LRU, as the sstable index caches and the
logstor cache do. They then share eviction with the row cache and are not
competing caches with separate memory limits.

# Part 2: How LSA works

## Segments and the segment pool

Memory is managed in 128 KiB segments (`logalloc::segment_size`,
`segment_size_shift = 17`). This size is a compromise between two needs:

* Smaller segments are better for compaction. Each segment is faster to
  compact, and with more, smaller segments there is a better chance that some
  of them are nearly empty and therefore cheap to compact.
* Segments should not be smaller than the largest common allocations that
  the rest of ScyllaDB makes with the standard allocator: I/O buffers and
  thread stacks, which are 128 KiB each. When such an allocation needs memory,
  LSA can satisfy it by freeing a single segment. If segments were smaller,
  LSA would have to free several segments that together form one contiguous
  block, which is much harder.

Conversely, no single freed segment can satisfy an allocation larger than
128 KiB. See [Why larger allocations are hard](#why-larger-allocations-are-hard).

The **segment pool** (`segment_pool` in `logalloc.cc`) owns the segments of
the shard. Each segment is either in use by a region, free but owned by LSA,
or returned to the standard allocator.

LSA keeps some bookkeeping for every segment, in a small struct called the
**segment descriptor** (`segment_descriptor` in `logalloc.cc`). It records
how much of the segment is free, which region owns the segment, and whether
it holds ordinary objects or [`lsa_buffer`s](#lsa_buffer). It also contains
the hook that links the segment into the structure in which its region keeps
its closed segments ordered by free space. The descriptors are not stored inside the segments, so
all 128 KiB of a segment are available for data. Instead they are in a
separate array with one entry for every 128 KiB of the shard's memory. When
an object is freed, LSA finds its segment by rounding the object's address
down to a multiple of 128 KiB, and then finds the segment's descriptor in
the array by the segment's position in memory. (Do not confuse segment
descriptors with the per-object [object descriptors](#object-layout-descriptors-and-migrators)
described below.)

Where segments are placed in memory matters. LSA and the standard allocator
share the shard's memory and pass it back and forth one segment at a time. If
LSA segments and standard allocations were interleaved, both sides would
suffer from fragmentation:

* A segment that LSA returns would be an isolated 128 KiB hole between
  standard allocations. The standard allocator could not use it for anything
  larger than 128 KiB.
* A long-lived standard allocation in the middle of LSA's memory would keep
  its whole 128 KiB-aligned block from ever becoming an LSA segment again,
  even after everything else in that block is freed.

So LSA keeps the two kinds of memory apart. At startup `prime_segment_pool()`
(called from `main.cc`) allocates *all* of the shard's memory as LSA
segments and then gives back the lowest-addressed segments: enough for
Seastar's `min_free_memory()` plus a gap of `available_memory / 16`, clamped
between 1 MiB and 32 MiB. From then on LSA prefers to use high-address
segments and to return low-address ones. LSA memory therefore stays at the
top of the shard's memory and standard memory at the bottom, and the boundary
between them moves as memory passes from one side to the other.

The pool keeps an **emergency reserve** of free segments. Its purpose is to
make sure that compaction itself, which needs a free segment to copy live
objects into, never fails. The reserve size is controlled by two numbers:
the *max* (by default 30 segments, temporarily raised by allocating sections)
and the *current goal* (by default 1, lowered to 0 during compaction so
compaction can use the reserve, and raised by `hold_reserve`).

### Where segment memory comes from

Where LSA gets its segments depends on which allocator Seastar uses:

* **Normal ScyllaDB (release and dev builds)** uses Seastar's own allocator,
  which gives each shard a fixed, contiguous memory arena. Segments are
  carved out of that arena with `aligned_alloc()` and given back with
  `free()`. This is how memory passes between LSA and the standard allocator
  one segment at a time, as described above. LSA can grow to almost all of
  the shard's memory. It stops taking segments from Seastar when Seastar's
  free memory would drop below the reserve set at startup, and compacts and
  evicts instead. Because the arena is contiguous, finding a segment's
  descriptor is cheap: its index is the segment's offset in the arena
  divided by 128 KiB.
* **Debug and sanitize builds** make Seastar use the system allocator
  (`malloc()`), so that AddressSanitizer can check every allocation. There is
  no per-shard arena, and nothing tells LSA when the rest of the program is
  short of memory. So LSA pretends the shard has 1 GiB of memory: it
  allocates up to 8192 segments, each with its own `aligned_alloc()`, and then
  compacts and evicts as if memory were full. This means that in these builds
  LSA reaches memory pressure at 1 GiB per shard, whatever the machine has.
  The number is arbitrary. It is large enough for the small data sets that
  tests use, and small enough that a test machine running many ScyllaDB
  nodes does not run out of memory (and that AddressSanitizer's limit on the
  total number of allocations is not reached, commit `2c670f6161`). Segments
  are allocated only when needed: startup priming is skipped
  (commit `e1ea8a996b`).
* **Tools** (`scylla sstable`, `scylla nodetool`, ...) also use the system
  allocator. They may run next to a production server, so they take memory as
  they need it instead of reserving most of the machine's memory at startup.
  LSA gets a fixed area reserved with `mmap()` when the tool starts
  (`use_standard_allocator_segment_pool_backend()`). Each tool chooses the
  size (`lsa_segment_pool_backend_size_mb` in `tools/utils.hh`): 100 MB for
  `scylla sstable` and 1 MB for `scylla nodetool`. LSA never gives this area
  back. Freed segments just return to its free list.

### Why larger allocations are hard

Seastar's allocator is a *buddy allocator*: an allocation larger than a page
needs a free block whose size is a power of two (the requested size rounded
up) and whose address is a multiple of that size. Every segment that LSA
frees is such a block of 128 KiB, so any one freed segment, wherever it is,
satisfies an allocation of up to 128 KiB. A larger allocation needs several
freed segments that together form one larger block.

Because LSA memory is at the top and standard memory at the bottom, LSA
always gives back its *lowest* segments, the ones right next to standard
memory. `segment_pool::reclaim_segments()` walks LSA's segments from the
lowest address up. A segment that is already free can be returned
immediately. A segment that is in use must first be compacted, however full
it is: its live objects are moved into other segments, which LSA prefers to
take from high addresses, and then it is returned too. So the segments that
LSA returns one after another are adjacent to each other, and they extend
the standard memory below them.

So LSA can provide a block larger than a segment, by returning several of
its lowest segments. But this is slow and unreliable:

* Unless the background reclaimer did it in advance, it all happens
  synchronously, inside the allocation that needs the memory. Every segment
  that is in use costs a compaction, so the stall grows with the size of the
  allocation.
* The freed segments must also form an aligned block. For example, if the
  lowest LSA segment is the upper half of a 256 KiB block whose lower half is
  still in use, freeing it does not help a 256 KiB allocation. Seastar then
  asks LSA for more memory and tries again, so LSA may reclaim more than the
  allocation's size.
* A segment whose region has reclamation disabled (it is inside a
  `reclaim_lock` or an allocating section) cannot be moved.
  `reclaim_segments()` skips it, so the memory it frees is split in two
  around that segment, and the large allocation can still fail. After 10
  such failures it gives up rather than keep fragmenting memory. If the
  allocation was made inside an [allocating section](#allocating-sections),
  the section then releases its lock, raises its reserve and retries, and
  this time every region can be compacted.

An allocation of at most 128 KiB avoids the last two problems, because any
one freed segment is enough for it. It does not avoid the first: it too
can wait for synchronous reclamation. See
[The other catch](#the-other-catch-large-allocations-can-stall) for how
ScyllaDB code keeps its allocations small.

This applies to normal ScyllaDB (release and dev builds). In debug and
sanitize builds, and in tools, LSA's segments are separate from the standard
allocator's memory, so the layout described here does not exist.

## The tracker

The **tracker** (`logalloc::tracker`) is the object at the top of LSA. There
is one per shard, a thread-local object returned by `logalloc::shard_tracker()`.
While each [region](#regions) manages its own segments, the tracker manages
memory *between* regions, and between LSA and the rest of the shard:

* It owns the shard's segment pool.
* It knows all regions: every region registers itself with the tracker when
  constructed and unregisters when destroyed.
* It registers LSA's reclaimer with Seastar's allocator. When Seastar runs out
  of free memory, it calls `tracker::reclaim()`, which decides which regions
  to compact and evict from (see [Reclamation](#reclamation)).
* It runs the background reclaimer and idle compaction.
* It exports the `lsa_*` [metrics](#metrics).
* It holds the configuration that `main.cc` sets with `tracker::configure()`
  (see [Configuration options](#configuration-options)), and `main.cc` stops
  its background work at shutdown with `tracker::stop()`.

Most code never uses the tracker directly. It works with regions, and the
tracker acts behind the scenes. The tracker's public methods are mostly for
statistics (`occupancy()`, `region_occupancy()`, `non_lsa_used_space()`,
`statistics()`) and for tests (`reclaim()`, `full_compaction()`).

Do not confuse the LSA tracker with `cache_tracker` (`db/cache_tracker.hh`).
The latter belongs to the row cache: it owns the cache region and its LRU, and
it is just one of the LSA tracker's users.

## Regions

A `logalloc::region` is a set of segments that belong to one large,
long-lived data structure, or to several data structures that share one
memory policy. Regions are big: today there is one region for each
memtable, holding all of its partitions and rows, and one cache region per
shard, holding the row cache of all tables together with the index caches
(see [Where LSA is used today](#where-lsa-is-used-today)). A region is the
unit of memory accounting (its size is the number of segments it holds) and
of locking against reclamation. Eviction is a property of the whole region:
a region is either evictable, with one eviction function, or not. What gets
evicted, though, is individual objects inside its segments, a few at a
time, never the region as a whole. Objects with different policies, such
as memtable data, which can only be freed by flushing, and cache data, which
can be evicted, must therefore be in different regions. Do not create a
region for something small, like a single vector or a single partition: even
a nearly empty region holds at least one 128 KiB segment, and the tracker
visits every region when it reclaims memory.

A region is an `allocation_strategy` (see [Allocation strategies](#allocation-strategies-and-with_allocator)).
Each region has:

* an *active segment* into which new objects are appended;
* a set of *closed segments*, kept in a histogram ordered by free space
  (`log_heap`) so that the sparsest one can be found quickly;
* separate active and closed segments for `lsa_buffer`s, which are kept apart
  from ordinary objects;
* an optional *eviction function* (`make_evictable()`);
* a *reclaiming enabled* flag (`reclaim_lock` and allocating sections clear it);
* an *invalidate counter* (`reclaim_counter()`), increased every time objects
  in the region may have moved or been evicted;
* an optional `region_listener`, informed when the region grows or shrinks.

Objects belong to the region that allocated them and must be freed through
that same region. A region must be empty when it is destroyed. The tracker
aborts at shutdown if any region is still registered.

## Object layout: descriptors and migrators

Objects are laid out one after another in a segment and never cross a segment
boundary. Each object is preceded by a small *object descriptor*, encoded in
a variable-length format (`utils/vle.hh`) that can be decoded both forward
(when walking a segment during compaction) and backward (when freeing an
object, given only its address). A live object's descriptor holds the
**index of its migrator**, and is usually one byte. A dead object's descriptor
holds its size, so compaction can skip it.

A **migrator** (`migrate_fn_type` in `utils/allocation_strategy.hh`) is a
per-type object that knows how to move an object of that type and how to
compute its size. For almost every type it is
`standard_migrator<T>`, which move-constructs the object at the new address
and destroys the old one, and gets the size from `sizeof(T)` or, for
variable-size types (the `DynamicObject` concept, types ending in a flexible
array), from `obj.storage_size()`. Compaction runs in the middle of some
unrelated allocation and has no way to recover if moving an object fails, so
`standard_migrator<T>` requires `T` to be nothrow move constructible and
nothrow destructible.

Each migrator registers itself in a per-shard table when it is created, and
the index it gets in that table is what the object descriptor stores. That
is how a one-byte descriptor can identify the object's type.

Each object is placed at the alignment its type requires: `alignof(T)`, which
`construct<T>()` and `alloc<T>()` pass to the allocator. The descriptor is
written right after the previous object, and if the new object needs padding
to be aligned, the descriptor is encoded with extra bytes that fill the gap.
Some of the most common LSA objects, such as the storage behind
`managed_bytes`, are packed structs with an alignment of 1, so they need no
padding at all. In debug and sanitize builds, every object additionally starts
and ends on an 8-byte boundary, so that AddressSanitizer can poison the free
space between objects.

## Allocation and freeing

`region_impl::alloc()` appends the object to the active segment. When the
rest of the active segment is too small for the new object, the segment is
*closed* (moved into the histogram of closed segments) and a new segment is
taken from the pool. The unused space at the end of the closed segment is
marked as a dead object. LSA never goes back to fill it with a later, smaller
object, but it is counted as free space, so compaction can recover it like
any other hole. Because no object is larger than 10% of a segment, this
waste is less than 10% of the segment.

Taking a new segment is the only part of allocation that can be expensive.
LSA tries these, in order (`segment_pool::allocate_segment()`):

1. Use a free segment that LSA already owns, if there is one above the
   emergency reserve.
2. Take a new segment from Seastar, but only if Seastar's free memory is
   large enough: at least the reserve set at startup (Seastar's
   `min_free_memory()` plus a gap of 1 MiB to 32 MiB) plus the new segment.
   LSA does not wait until Seastar runs out of memory, because then every
   standard allocation would have to reclaim memory from LSA, and the two
   allocators would fight over every last segment. The headroom lets standard
   allocations proceed without reclaiming most of the time. This step can
   also fail if Seastar has enough free memory but no free 128 KiB-aligned
   block.
3. Otherwise, free segments by compacting and evicting LSA's own memory (see
   [Reclamation](#reclamation)), and try again.

If step 3 cannot free anything either, allocation throws `std::bad_alloc`.
More precisely it throws `logalloc::bad_alloc`, a subclass whose message
says what LSA was trying to do when it ran out of memory: allocate a segment
for an object, or fill the memory reserves of an
[allocating section](#allocating-sections). See [Logs](#logs) for the exact
messages.

Allocations larger than `max_managed_object_size` (10% of a segment, 13107
bytes) are not supported. They used to be passed silently to the standard
allocator. Since 2025 (commit `cb02d47b10`, Michał Chojnowski) they are an
internal error (`Contiguous allocation of size ... exceeds logalloc's limit`),
because the fallback caused hard-to-find accounting bugs.

So containers that may hold large data split it into pieces. Each
[allocator](#allocation-strategies-and-with_allocator) reports its limit
through `preferred_max_contiguous_allocation()` (13107 bytes for LSA, 128 KiB
for the standard allocator), and `managed_bytes` splits its data into
fragments of at most that size. `lsa::chunked_managed_vector` uses LSA's
limit directly. `managed_vector` does not split: its users must keep it small.

Freeing writes a dead descriptor over the object, updates the segment's
free-space count and moves it in the histogram. A closed segment that becomes
completely empty is returned to the pool immediately. Freeing never moves
anything, and it does not invalidate references to other objects.

## Compaction

Compacting a segment means copying each of its live objects (using their
migrators) into the region's active segment and then freeing the old segment.
A segment is a compaction candidate only if at least 15% of it is free
(`max_used_space_ratio_for_compaction = 0.85`). A region is considered
*compactible* only if reclamation is enabled for it, its closed segments have
at least 4 segments' worth of free space in total, and it has at least one
candidate segment. These limits guarantee that compaction always makes
progress: each compaction of a segment with utilization `U` copies `U` of a
segment and gains `1 - U`. The cost per byte gained, `U / (1 - U)`, grows
quickly with `U`: it is 1.5 at `U = 60%`, 5.7 at `U = 85%`, and 9 at
`U = 90%`. That is why LSA does not compact
dense segments, and why it prefers to *evict* from an evictable region whose
average occupancy is above 85%.

Compaction never moves objects from one region to another. Every segment
holds objects of only one region, and compacting it copies them into the
active segment of that same region. So the holes in one region's segments
are never filled with another region's objects. Their memory becomes
available to other regions only when compaction empties a whole segment and
returns it to the segment pool. The only way objects change regions is when
a whole region is [merged](#region-merging) into another, and that moves
segments, not objects.

To choose what to compact, LSA works at two levels. Inside each region, the
histogram of closed segments finds the region's sparsest segment. Across
regions, each time the tracker reclaims memory it arranges its list of all
regions into a heap (with `std::make_heap()`), ordered by the free space of
each region's sparsest segment, with regions that are not compactible last.
It then compacts the sparsest segment of the region at the top of the heap,
updates the heap, and repeats until enough memory is freed.

## Eviction

A region becomes evictable with `region::make_evictable(eviction_fn)`. The
function frees "some" memory from the region and returns
`memory::reclaiming_result::reclaimed_something`, or `reclaimed_nothing` when
there is nothing left to evict. LSA calls it repeatedly. The cache's eviction
function (in the `cache_tracker` constructor, `db/row_cache.cc`) first frees
garbage partition versions, then evicts one entry from the shared LRU
(`utils/lru.hh`). Rows (`rows_entry`), sstable index entries and cached file
pages are all `evictable` objects linked in that LRU. An index entry is
evicted first if the index caches use more than `index_cache_fraction` of the
cache memory.

When the tracker needs to free memory from an evictable region, it calls
`reclaim_from_evictable()` with a *target*: how much memory LSA's segments
should hold when it is done. The target is the memory in use now, minus the
amount this reclamation was asked to free (plus anything missing from the
emergency reserve). The difference between the two is the *deficit*.
Evicting objects only leaves holes in segments; memory in use drops only
when whole segments become empty. So `reclaim_from_evictable()` first evicts
objects totaling the deficit plus one segment, without compacting, because in
workloads where eviction order matches allocation order this empties whole
segments and no compaction is needed at all. It then alternates eviction and
compaction until the memory in use is down to the target. When called
synchronously, this loop cannot yield, so a large deficit means a long
stall; see [Avoiding reclamation stalls](#avoiding-reclamation-stalls).

Eviction runs inside the allocation that triggered it, so the eviction function
must be `noexcept`, must not allocate (or must tolerate allocation failure),
and must not touch any data structure the interrupted code might be in the
middle of changing. See [Making a region evictable](#making-a-region-evictable).

## Reclamation

There are four ways memory gets reclaimed:

1. **Synchronous reclaim from Seastar.** The tracker registers a
   `seastar::memory::reclaimer`. When a *standard* allocation finds no free
   memory, Seastar calls `tracker::reclaim()`, which returns free LSA segments
   to Seastar and, if needed, compacts and evicts to free more segments
   (`tracker::impl::reclaim_locked()`). It reclaims at least
   `lsa_reclamation_step` segments (default 1) at a time.
2. **Segment allocation inside LSA.** When a region needs a segment and none
   is available, `segment_pool::allocate_segment()` calls
   `compact_and_evict()` directly.
3. **Background reclaim.** A coroutine in the `background_reclaim`
   scheduling group tries to keep at least 60 MB of Seastar memory free
   (`background_reclaim_free_memory_threshold`), so that foreground
   allocations rarely have to reclaim synchronously. Its shares rise as free
   memory falls. It reclaims preemptibly, in contrast to synchronous reclaim,
   which cannot stop until it has reached its goal.
4. **Idle compaction**, only if `defragment_memory_on_idle` is set (it is not
   set by default): compacts segments while the CPU is idle.

The first three are built from three functions in `logalloc.cc`, which call
each other in this order:

* `tracker::impl::reclaim()` gives memory **back to Seastar**. It first
  returns the free segments LSA already has. If they are not enough, it calls
  `compact_and_evict()` to free more segments, and returns those too. Seastar's
  reclaimer (1) and background reclaim (3) use it, and so do allocating
  sections that need free standard memory.
* `tracker::impl::compact_and_evict()` frees segments but **keeps them in
  LSA**, for LSA's own use. LSA calls it when it needs segments itself (2),
  including when an allocating section fills its reserve of segments. It works
  across all regions, using the heap of regions described in
  [Compaction](#compaction): for the region at the top of the heap it either
  compacts one segment or, if the region is evictable and more than 85% full,
  calls `reclaim_from_evictable()`. If that is still not enough, it calls
  `reclaim_from_evictable()` on each evictable region in turn.
* `reclaim_from_evictable()` works on **one evictable region**, alternating
  eviction and compaction as described in [Eviction](#eviction).

Reclamation skips regions whose reclamation is disabled. When returning
segments to Seastar, it walks LSA's segments from the lowest address, and
compacts each used one (moving its objects to another segment) so it can be
returned. It gives up after 10 segments that it could not compact because
they belong to locked regions. Then the surrounding allocating section unlocks
the region, raises its reserves and retries
(commit `ff6325ee7e`).

Reclamation that takes longer than the reactor stall threshold is reported by
the `lsa-timing` logger (see [Logs](#logs)). How to avoid such stalls is
explained in [Avoiding reclamation stalls](#avoiding-reclamation-stalls).

## Reference invalidation and the reclaim counter

Code often needs to remember a position in an LSA data structure across
operations that may reclaim memory. For example, a reader of the cache
returns some rows, yields, and later continues from where it stopped. Keeping
a pointer or iterator is not safe, because compaction may have moved the
objects meanwhile, or eviction may have freed them. But looking the position
up again from scratch (say, by key) every time is wasteful, because most of
the time nothing moved.

The **reclaim counter** lets such code tell the two cases apart cheaply.
Every region has a counter, which LSA increases whenever objects in the
region may have moved or been freed: whenever it compacts the region or
evicts from it. Code that keeps a position saves the counter's value
(`region::reclaim_counter()`, or the equivalent
`allocator().invalidate_counter()`) together with its pointer. When it
continues, it compares the counter again. If the value is the same, LSA has
not moved or freed anything, and the pointer can be used directly. If it
changed, the code must find its position again, for example by looking up
the last key it saw. [Keeping a position across deferring points](#keeping-a-position-across-deferring-points)
shows how real readers do this.

The counter covers only what LSA does. Code that frees an object itself, or
changes a data structure in a way that makes saved positions wrong, must
increase the counter too, with `allocator().invalidate_references()`. For
example, `cache_tracker` does so when it inserts or removes a partition,
because cursors such as `partition_range_cursor` rely on the counter to
notice such changes. Alternatively, code can pair the counter with a change
counter of its own: `partition_snapshot::change_mark` adds the number of
partition versions, because merging versions frees some of them.

Merging two regions makes the counter of the result the maximum of both, so
cursors on either one notice future increments.

An [allocating section](#allocating-sections) (explained later) runs a
function with reclamation disabled, and if the function runs out of memory,
the section frees memory and runs the function again from the start. Freeing
that memory may compact or evict the region, so before running the function
again the section always increases the region's counter (commit
`8d69d217af`). Code that saved pointers during the failed run can therefore
rely on the counter to know that they are no longer valid.

## `lsa_buffer`

`lsa_buffer` is a second way to allocate in LSA, made for storing raw byte
buffers, such as cached 4 KiB pages of sstable index files (its only user
today is `cached_file`). A buffer can have any size up to a whole segment
(128 KiB), but it always starts at a 4 KiB boundary and takes up a whole
number of 4 KiB units, so sizes that are multiples of 4 KiB waste no space.
`cached_file`'s 4 KiB pages are the simplest case. For storing raw byte
buffers, `lsa_buffer` has three advantages over ordinary LSA allocations:

* **Packing.** Buffer segments contain no object descriptors, so buffers are
  packed back to back: a segment holds exactly 32 buffers of 4 KiB. Ordinary
  allocations put a descriptor in front of each object, so only 31 such
  buffers would fit, and keeping each of them aligned to 4 KiB would cost
  almost 4 KiB of padding per buffer.
* **Back-pointers.** An ordinary object must fix up the pointers to it when it
  moves, which a plain array of bytes cannot do. Instead, the `lsa_buffer`
  that owns a buffer is updated automatically when compaction moves the
  buffer, so its owner can keep it anywhere, even in standard memory, as
  `cached_file` does.
* **Size.** A buffer can be as large as a whole segment (128 KiB), while an
  ordinary LSA object is limited to 10% of a segment.

Storing the pages in LSA rather than in standard memory makes them part of
the cache: they are counted in the cache region, evicted through the same LRU
as cached rows, and compacted instead of fragmenting standard memory.

The API is `region::alloc_buf(size)`, which allocates a buffer and returns an
owning smart pointer to it, `logalloc::lsa_buffer`. Buffers
live in separate segments that contain only buffers. The segment descriptor
keeps an `entangled` link to each buffer's `lsa_buffer`, which is how
compaction updates the pointer. The pointer returned by `lsa_buffer::get()` is
valid only until the next reclamation, but the `lsa_buffer` always holds the
buffer's current address, and it may be destroyed in any allocator context.
(If the `lsa_buffer` itself lives in LSA memory, compaction may move it like
any other object, and its move constructor updates the link.) It must
not outlive its region.

## Region merging

`region::merge(other)` moves all segments of `other` into `this`, without
copying any objects, so it does not invalidate references.

After the merge, `other` is not empty but becomes a second name for the
merged region. A `logalloc::region` object is only a handle: it holds a
shared pointer to the real region (`region_impl`), and `merge()` makes
`other`'s handle point to the same `region_impl` as `this`. So everything
done through `other` afterwards (allocating, freeing, locking) acts on the
merged region. This matters because a memtable is itself a
`logalloc::region`, and the memtable object stays alive after its data is
merged into the cache region, as long as something still references it.
Code that still uses the memtable, such as a reader that locks
`memtable::region()` or the memtable's destructor freeing whatever entries
are left, then acts on the cache region, where the data now is.

Normally a region outlives its data: a region must be empty when it is
destroyed. A merge breaks this pairing. The data now lives in the cache
region, but the original `logalloc::region` object (the memtable) may be
destroyed while that data is still in use. Any object that keeps a raw
pointer to the original `logalloc::region` and uses it to work on the data
would then use a dangling pointer. Such objects must be switched to the
merged region. For example, a `partition_snapshot` keeps a pointer to its
region, and snapshots that are still waiting to be cleaned up when a
memtable is merged into the cache are switched to the cache region with
`partition_snapshot::migrate()` (in `mutation_cleaner_impl::merge()`).

## Region listeners and dirty memory accounting

`logalloc::region_listener` (in `utils/logalloc.hh`) is an abstract class that
lets code outside LSA follow how big a region is. A region can have one
listener, attached with `region::listen()` and detached with
`region::unlisten()`. LSA calls the listener's virtual methods when the region
is attached or detached (`add()`, `del()`), when the `logalloc::region` handle
is C++-moved to another object with its move constructor or move assignment
(`moved()`, needed because the listener keeps a pointer to the handle; this
has nothing to do with compaction or merging), and whenever the region gains
or loses a segment (`increase_usage()`, `decrease_usage()`). The callbacks
run in the middle of LSA's own operations, so they must be cheap. In
particular, `decrease_usage()` is called when the region gives up a segment,
which happens inside the region's `free()` (when freeing an object empties
its segment) and during compaction. Both are `noexcept`, so
`decrease_usage()` must not throw.

Its one user today is the memtable, which is its own listener (`memtable`
derives from both `logalloc::region` and `region_listener`). It forwards the
size changes to the `dirty_memory_manager` (`replica/dirty_memory_manager.hh`),
whose `region_group` (historically part of LSA, moved out in 2022) adds up the
size of all memtables and decides when to flush memtables and when to block
writes. To choose which memtable to flush next, the `region_group` keeps
memtables ordered by their "evictable occupancy" (a historical name;
memtables are not evictable). When a memtable is sealed for flushing,
`region::ground_evictable_occupancy()` sets that number to zero, and LSA
tells the listener through `decrease_evictable_usage()`, so that the flusher
does not keep picking a memtable that is already being flushed.

# Part 3: Using LSA

## Allocation strategies and `with_allocator`

The same types are used for data in LSA and in standard memory. A
`mutation_partition`, with its tree of rows and its `managed_bytes` cells,
may live in a memtable or in the cache (in LSA), or may be a temporary in
standard memory, for example a mutation that was just received from the
network. The code of these types allocates and frees memory all the time
(a new row, a new fragment of a value), and it has to know *where*: an
object inside an LSA region must get its parts from the same region, and an
object in standard memory must get them from the standard allocator.

One way to do this would be to store a pointer to the allocator in every
object, but with millions of small objects that costs too much memory.
Another would be to pass the allocator to every function that might
allocate, which would mean changing a great many functions. Instead,
ScyllaDB keeps the *current allocator* in a thread-local variable: every
allocation and free in these types goes through `current_allocator()`,
which returns the `allocation_strategy&` stored there. It is the standard
allocator (`standard_allocator()`) by default, and the code that owns a data
structure, which knows where that data structure lives, sets it with
`with_allocator()` for the duration of a function:

```c++
with_allocator(region.allocator(), [&] {
    auto* e = current_allocator().construct<my_entry>(args...);
    // ...
    current_allocator().destroy(e);
});
```

`with_allocator()` does nothing else: it saves the previous current
allocator, sets the new one, calls the function, and restores the previous
one when the function returns. It can be nested, with the innermost one
winning. It does not make the code inside it atomic in any sense: it does
not lock anything, and it does not stop LSA from compacting or evicting, so
references into the region can still become invalid inside the block. That
protection comes from [`reclaim_lock`](#keeping-references-valid-reclaim_lock)
and [allocating sections](#allocating-sections), which are separate and
are often combined with `with_allocator()`. Several calls inside one
`with_allocator()` block behave exactly like the same calls in separate
blocks; using one block is just shorter and slightly cheaper.

Because the current allocator is a thread-local variable, and many tasks
share a shard's thread, the function passed to `with_allocator()` must
not defer. If it yields, for example inside a `seastar::thread`, other tasks
run with its allocator as their current allocator. And if it returns a
future, the previous allocator is restored as soon as the function returns,
before the continuations that the future runs later.

If the current allocator is wrong, things break:

* **Freeing in the wrong allocator corrupts memory.** Freeing a standard
  allocation through an LSA region, or an LSA object through the standard
  allocator, hands an address to an allocator that never allocated it. At
  best this aborts on an assertion; at worst it corrupts the allocator's
  data or its memory accounting.
* **Allocating in the wrong allocator breaks LSA's guarantees.** If code that
  adds a row to the cache runs with the standard allocator current, the new
  row is allocated in standard memory but linked into the cache's data
  structures. It is not counted in the cache region, it cannot be compacted,
  and it adds to the fragmentation LSA exists to avoid. And because it is
  later freed through the cache region, it leads to the first problem too.

Objects do not remember which allocator created them. **An object must be freed
in the same allocator context in which it was allocated.** LSA-aware types
such as `managed_bytes` and `managed_ref` also call `current_allocator()` in
their constructors, copy operations and destructors. So copying a
`managed_bytes` into LSA memory requires the LSA allocator to be current, and
destroying a `managed_bytes` whose storage was allocated in standard memory
(typically one that lives in a standard container) requires the standard
allocator to be current, even if the code is running inside some
`with_allocator(region.allocator(), ...)`. What matters is the allocator that
allocated the storage, not the container that holds the object.

The opposite mistake is just as common: an LSA operation running under
`with_allocator(region.allocator(), ...)` builds a temporary that is meant to
live in standard memory, for example a key copied into a `std::unordered_map`
or an object that is kept after the section ends. Wrap such code in
`with_allocator(standard_allocator(), ...)`. Real bugs of both kinds:
`cached_file` pages freed in the LSA context during eviction (#10056: freed
standard objects were counted against LSA, and memory reclamation stopped
working), a coroutine object destroyed in the wrong context after an
exception (#12068), `row_cache` cleared in the wrong allocator when its
constructor failed (#15632), and the large-data guardrail caches, which now
insert under `with_allocator(standard_allocator(), ...)`
(`db/large_data_cache_tracker.hh`).

`alloc_strategy_unique_ptr<T>` is a `std::unique_ptr` that frees through
`current_allocator()`. It may be used for LSA objects only while references
are stable, that is, within one allocating section.

## Making a type storable in LSA

A type stored in LSA must:

* be **nothrow move constructible**: LSA moves it during compaction, from
  inside an allocation, and cannot handle an exception;
* **fix up every pointer that points to it** in its move constructor. The usual
  ways are intrusive container hooks that relink themselves when moved
  (Boost.Intrusive hooks, `utils/intrusive_btree.hh`), an explicit
  back-pointer (as in `managed_ref`, `managed_bytes` and `managed_vector`), or
  an `entangled` link (`utils/entangled.hh`), a pair of pointers that keep
  pointing at each other when either one moves;
* allocate everything it owns through `current_allocator()`, in pieces no
  larger than `current_allocator().preferred_max_contiguous_allocation()`;
* have a `noexcept` destructor that frees through the allocator it was created
  with;
* for variable-size objects, satisfy `DynamicObject`
  (`size_t storage_size() const noexcept`) and be allocated with
  `current_allocator().alloc<T>(size)` followed by placement new.

Iterators into an intrusive container are pointers too. They become invalid
like any other reference.

## LSA-aware containers and helpers

| Type | Header | What it is |
|---|---|---|
| `managed_bytes`, `managed_bytes_view` | `utils/managed_bytes.hh` | Byte string. Stored inline when small (up to 15 bytes), otherwise in one external chunk or a chain of fragments, each within the allocator's preferred contiguous size. Views iterate over fragments. Use instead of `bytes` in LSA. |
| `managed_ref<T>` | `utils/managed_ref.hh` | Like `std::unique_ptr<T>`, with a back-pointer so it follows `T` when `T` moves. Created with `make_managed<T>()`. |
| `managed_vector<T>` | `utils/managed_vector.hh` | A small vector with a back-pointer to its external storage. |
| `lsa::chunked_managed_vector<T>` | `utils/lsa/chunked_managed_vector.hh` | A vector of any size split into chunks within the LSA size limit. |
| `bplus::tree`, `intrusive_b::tree`, `compact_radix_tree::tree` | `utils/bptree.hh`, `utils/intrusive_btree.hh`, `utils/compact-radix-tree.hh` | Trees whose nodes are allocated through `current_allocator()` and can move. The cache and memtables use them for partitions and rows. |
| `entangled` | `utils/entangled.hh` | A pair of pointers to each other, kept consistent when either side moves. |
| `lsa::weak_ptr<T>`, `lsa::weakly_referencable<T>` | `utils/lsa/weak_ptr.hh` | A non-owning pointer, itself in standard memory, that stays valid when the LSA object moves and becomes null when it is destroyed. Can be held across deferring points and used without locking the region (but the `T*` it returns is only valid until the next reclamation). |
| `logalloc::lsa_buffer` | `utils/logalloc.hh` | Owning pointer to a page-aligned buffer in LSA (see [above](#lsa_buffer)). |

Do not put `std::vector`, `std::string`, `std::unique_ptr`, `std::map`,
`bytes` or `lw_shared_ptr` inside an LSA object unless they point to *standard*
memory and you are sure they are created and destroyed in the standard
allocator context. Prefer the managed types.

## Keeping references valid: `reclaim_lock`

`logalloc::reclaim_lock` disables compaction and eviction of one region for as
long as it exists (it can be nested). While it is held, pointers and
references into the region stay valid.

This is needed even in code that never defers. LSA compacts and evicts
*synchronously*, inside an allocation, and that can be any allocation, even a
standard one made by innocent-looking code: building a string, growing a
`std::vector`, throwing an exception. So synchronous code that holds a
pointer into a region and then calls anything that might allocate can find
the pointer invalid when the call returns. Only code that certainly does not
allocate between getting a pointer and its last use can do without a
`reclaim_lock`.

Conversely, a `reclaim_lock` is not meant to be held across deferring points.
Other code needing memory would find the region unavailable for reclamation
for an unbounded time. Code that defers must instead re-find its position
afterwards, as described in [Keeping a position across deferring
points](#keeping-a-position-across-deferring-points).

Use `reclaim_lock` for short code that reads LSA data and allocates little or
nothing. Two examples: the formatter for `replica::memtable` holds one while
it walks the memtable's partitions to print them, and `partition_snapshot_reader`
holds one while it reads the tombstone of the partition it is reading (from
its `partition_snapshot`, the reader's view of one partition, described in
[mvcc.md](mvcc.md)).

If the code inside allocates more than a little, from any allocator, use an
allocating section instead. While a `reclaim_lock` is held, an allocation
that needs memory freed cannot get it from the locked region; LSA can only
compact or evict *other* regions. When the locked region is the cache, which
usually holds most of the shard's memory, what remains (LSA's free segments,
and the memtables, which can only be compacted, not evicted) may have little
to give, and the allocation can fail even though the cache has plenty of
memory it could have evicted.

## Allocating sections

### The problem

Most operations on LSA data structures, such as inserting a row into the
cache or applying a mutation to a memtable, both hold references into the
region and allocate. They need reclamation disabled (for the references) and
they need allocation to succeed (but the memory may only be available by
reclaiming the very region that is locked).

### The solution

`logalloc::allocating_section` (in `utils/logalloc.hh`) is an ordinary class,
not meant to be subclassed. You create an `allocating_section` object, usually
as a long-lived member of the object that owns the data structure, and call
it like a function, passing the region and the code to run:

```c++
logalloc::allocating_section _section;    // a member
...
auto result = _section(region, [&] {
    // Code that uses references into the region and may allocate.
    return ...;
});
```

The call returns whatever the function returns. A typical use, from
`replica::memtable::apply()`:

```c++
with_allocator(allocator(), [this, &m] {
    _table_shared_data.allocating_section(*this, [&, this] {
        auto& p = find_or_create_partition(m.decorated_key());
        p.apply(region(), cleaner(), *_schema, m.partition(), ...);
    });
});
```

The `allocating_section` object holds two numbers, its **reserves**: how many
free LSA segments, and how many bytes of free standard memory, the function
is expected to need (the private members `_lsa_reserve` and `_std_reserve`).
Each call runs in three steps:

1. **Reserve**: with the region still reclaimable, make sure that at least
   that many LSA segments are free (by raising the emergency reserve, which
   may compact and evict) and at least that many bytes of standard memory are
   free.
2. **Run**: lock the region against reclamation and run the function. LSA
   allocations inside can use the reserved segments.
3. **Retry**: if the function throws `std::bad_alloc`, the section works out
   which allocator failed, doubles that reserve, unlocks the region,
   reserves again, and **runs the function again from the start**. If
   reserving fails, the `std::bad_alloc` is passed to the caller.

Because the `allocating_section` object remembers the reserves it learned,
later calls start with reserves that have worked before. The reserves *decay*:
`_lsa_reserve` is halved after every `100000 / _lsa_reserve` calls and
`_std_reserve` after every `10 GB / _std_reserve` calls (never below 1 segment
and 1 KiB). A single large operation therefore does not make every later
operation pay for a big reserve for long, and big reserves, which are the
expensive ones to maintain, decay fastest.

The first two steps are also available separately, as the methods
`with_reserve(region, func)` and `with_reclaiming_disabled(region, func)`, for
code that wants to reserve once and then run several short locked steps (see
the rules below). Calling the `allocating_section` object as a function is the
same as `with_reserve()` around `with_reclaiming_disabled()`. The
`allocating_section` constructor optionally takes a name, used in log
messages, and the methods `set_lsa_reserve()` and `set_std_reserve()` set the
initial reserves.

One kind of `std::bad_alloc` is not retried: `utils::memory_limit_reached`.
The reader concurrency semaphore (`reader_concurrency_semaphore.hh`) tracks
how much memory each read uses, and when the reads on a shard use far more
than their limit, it kills reads by throwing this exception, a subclass of
`std::bad_alloc`, when a read asks to account for more memory (which can
happen inside an allocating section, for example in a cache read). This does
not mean memory is short, so freeing memory and retrying would not help: the
read would just be killed again. The allocating section rethrows it to the
caller at once (commit `c0da6bcfb8`).

### Rules for code inside an allocating section

* **The function must be safe to run again.** If it throws `bad_alloc`
  halfway, it is called again with the region possibly compacted, so it must
  not keep pointers from the failed attempt, and the state it left must be
  valid and still lead to the right result when run again. Data structures are
  designed for this: `mutation_partition_v2::apply_monotonically()` has
  "monotonic" exception guarantees (after an exception, the sum of source and
  target still represents the same writes). Be careful with inputs that the
  function consumes: `memtable::apply(const frozen_mutation&, ...)` rebuilds
  the partition it moves from inside the lambda for this reason. Parsers that
  run inside a section must reset their state on retry (fixed in
  `sstables/mx/bsearch_clustered_cursor.hh`, commit `8aca93b3ec`). Side effects
  such as statistics or LRU updates must also be correct when repeated, or be
  done outside the section.
* **Do not let references escape.** Everything obtained inside is valid only
  until the section ends. Return values, not pointers, unless the caller
  re-validates (for example, using the reclaim counter).
* **Do not defer inside.** The function is synchronous, but in a
  `seastar::thread` it is possible to yield. Do not, because the region would
  stay locked for everyone else.
* **Do not nest allocating sections on the same region.**
  `with_reclaiming_disabled()` asserts that the region is reclaimable on entry.
  Sections on different regions may nest.
* **Choose how much work to do in each call.** There are costs in both
  directions:
  * Each call has a fixed cost: the reserve step checks the reserves, and may
    compact and evict to refill them. Calling the section separately for each
    of many tiny operations wastes this. For example, the sstable index reader
    inserts a whole page of index entries in one call instead of one call per
    key (commit `e9c98274b5`).
  * Everything done in one call must fit in the reserves, keeps the region
    locked, and is redone from the start on a retry. So a large operation
    should be split into bounded steps (see
    [Avoiding reclamation stalls](#avoiding-reclamation-stalls)).

  The separate `with_reserve()` and `with_reclaiming_disabled()` methods help
  with both: `partition_snapshot_reader` calls `with_reserve()` once per
  `fill_buffer()`, and inside it `with_reclaiming_disabled()` once per
  fragment. It pays for the reserve step once, but keeps each locked step,
  and each retry, short.
* **Use separate section objects for operations with different memory
  needs.** The learned reserve is per object. If one rare, large operation
  shares a section with a frequent, small one, the small one pays for the
  large reserve until it decays. The row cache has separate `_read_section`,
  `_update_section` and `_populate_section`. Memtables share theirs per table
  (`memtable_table_shared_data`), so a new memtable, or a tablet that just
  migrated in, starts with reserves learned by the table's previous memtables.
* **Name your sections** (`allocating_section(abstract_formatter name)`), so
  the "increasing reserve" log message can tell which table or component
  caused it (commit `082342ecad`).
* **Anything that can be evicted from must be modified under the section,
  even if it lives in standard memory.** The eviction function can run during
  any allocation that is not inside a locked section. For example
  `cached_file` keeps its page index in a standard-memory B+ tree from which
  eviction erases entries. It must call `_cache.emplace()` inside an
  allocating section, because an allocation in the middle of `emplace()`
  could otherwise trigger an eviction that changes the tree (#9915).
* **Memory that must be accessed outside the section belongs in standard
  memory.** The partition index cache once allocated a `shared_promise` future
  inside a section, so a `noexcept` function could throw `bad_alloc` (#9666).

### Reserving memory for critical LSA allocations: `hold_reserve`

Sometimes code must allocate in a context that cannot fail, for example to
restore a data structure's invariants in a destructor while reclamation is
disabled. `hold_reserve` (in `utils/allocation_strategy.hh`) is meant to make
*LSA* allocations (not standard ones) in such code succeed. When constructed,
it raises the goal of LSA's [emergency
reserve](#segments-and-the-segment-pool) of free segments by the requested
amount (rounded up to whole segments), freeing segments if needed, which may
throw. While it exists, ordinary LSA allocations cannot use those segments.
When it is destroyed, it lowers the goal again, so the segments it set aside
become available to the LSA allocations that run after it. Both its
constructor and its destructor use `current_allocator()`, so an LSA region
must be the current allocator both when it is created and when it is
destroyed. Under the standard allocator, the constructor does nothing, and
the destructor does not lower the goal, so the segments stay set aside for
good. See its use
in `mutation_partition_v2::apply_monotonically()`, which keeps a segment in
reserve so it can insert sentinel rows during cleanup without failing.

**Warning:** despite its purpose, `hold_reserve` does not make allocations
infallible in general. It only guarantees memory for a small, known set of
LSA allocations that run right after the `hold_reserve` is destroyed, and
only if nothing else allocates in between:

* **Standard allocations are not covered.** A standard allocation in the
  critical code can fail as it always could. In a destructor or other
  `noexcept` context, that means `std::terminate()`.
* **A standard allocation can take the reserved segments.** After the
  `hold_reserve` is destroyed, the set-aside segments are ordinary free LSA
  segments again. If a standard allocation then finds Seastar short of free
  memory, Seastar synchronously calls LSA's reclaimer, which first returns
  LSA's free segments to Seastar, down to the (now lowered) emergency reserve
  goal (`segment_pool::reclaim_segments()`). This can give away exactly the
  segments the critical LSA allocations were counting on. This happens even
  while the region is locked by an allocating section, because a
  `reclaim_lock` only protects the region's objects, not LSA's free segments.
* **Other LSA allocations can take them too**, so the reserved amount must
  cover every LSA allocation made by the critical code, and no other LSA
  allocations may run before them.

So the critical code should make only the LSA allocations that the reserve
was sized for, and no standard allocations at all (the sentinel insertion in
`apply_monotonically()` inserts B-tree nodes in LSA and links them into the
LRU, which does not allocate, apart from trace-level log messages). The test
`test_hold_reserve` in `test/boost/logalloc_test.cc` likewise avoids
standard allocations, because they "could make the test imprecise".

## Keeping a position across deferring points

Readers of the cache and memtables may yield between fragments (for example
when their buffer is full, or when preempted), and the region may be
compacted or evicted while they wait. So they cannot keep a raw pointer or an
iterator across such points. The common patterns:

* **Remember a key and the reclaim counter; look up again when the counter
  changes.** `row_cache`'s `partition_range_cursor` and the memtable's
  `iterator_reader` (`update_iterators()`) work this way. Note that the
  memtable reader also compares the partition count, because inserting into a
  memtable does not invalidate references, while the cache explicitly bumps
  the counter on insert and erase.
* **Use a change mark.** `partition_snapshot::get_change_mark()` combines the
  region's reclaim counter with the snapshot's version count, and
  `partition_snapshot_row_cursor` re-positions itself when it changes.
* **Use `lsa::weak_ptr`** (`utils/lsa/weak_ptr.hh`), a smart pointer to one
  LSA object that itself lives in standard memory, so it can be kept anywhere,
  across deferring points, without locking the region. Dereferencing it
  always gives the object's current address, even if compaction has moved the
  object since, and the `weak_ptr` becomes null if the object is destroyed
  (it does not keep the object alive). The object's class must derive from
  `lsa::weakly_referencable<T>`, and a `weak_ptr` to it is made with
  `weak_from_this()`. The cost is one pointer in each object, plus a small
  standard allocation while any `weak_ptr` to it exists. As always, the
  address obtained by dereferencing is only valid until the next reclamation.
  The sstable partition index cache uses it for the index pages that readers
  hold; it also removes a held page from the LRU, so that it is not evicted
  while in use.
* **Keep the referenced thing out of LSA.** If a small piece of data has to
  be referenced a lot from outside, store it in standard memory and point to
  it from the LSA object (or vice versa through an `entangled` pair), as the
  logstor cache does: its index lives in a standard-memory B+ tree and the
  cached entry in LSA, linked by an `entangled` pair.

## Making a region evictable

To make data evictable, either link it into the shared cache LRU (derive from
`evictable` or `index_evictable` in `utils/lru.hh`, allocate it in the
cache region, and implement `on_evicted()`), or, for a new region, call
`region::make_evictable()` with your own function. Either way:

* The eviction code runs **synchronously inside an allocation** made by
  unrelated code. It must be `noexcept`, should not allocate, and must not
  assume that any of your data structures is in a consistent state unless you
  protect their modification with an allocating section or `reclaim_lock`.
* LSA itself **does not set the current allocator** before calling the
  eviction function. The cache tracker's evictor wraps everything in
  `with_allocator(_region.allocator(), ...)`, so `on_evicted()` runs with the
  cache region as the current allocator. A function you pass to
  `region::make_evictable()` gets no such wrapper and must call
  `with_allocator()` itself. If `on_evicted()` destroys objects in standard
  memory, wrap that in `with_allocator(standard_allocator(), ...)` (#10056).
* Each call should free a bounded amount of memory, for latency. The cache
  evicts one LRU item per call.
* Do not use it from another shard.

## Freeing large structures gently

Destroying a large LSA structure, like a big partition, all at once can stall
the reactor. The row cache and memtables hand partition data they no longer
need to a `mutation_cleaner` (`mutation/mutation_cleaner.hh`), which has two
jobs:

* **Freeing garbage partition versions** a little at a time
  (`clear_gently()` frees part of a version and returns, to be called again
  later). The cache does this from its eviction function, so garbage is freed
  before live data is evicted. This only frees memory, so it needs no
  allocating section.
* **Merging partition versions.** When the last reader of an old MVCC
  snapshot is done, the snapshot's versions of the partition still have to be
  merged into one (see [mvcc.md](mvcc.md)). The cleaner does this in the
  background, a bit at a time (`merge_some()`). Merging moves rows from one
  version to another, which allocates (for example, B-tree nodes), so this
  part runs in an allocating section of its own.

Note the comment in `mutation_cleaner.hh`: objects
waiting in the cleaner may outlive their schema, so their destructors and
migrators must not depend on the schema, and all migrators they use must stay
registered. In particular, a `mutation_cleaner` must not be a thread-local
object.

## Avoiding reclamation stalls

Reclamation that the background reclaimer did not do in advance is
*synchronous*: it runs inside some allocation and cannot yield until it has
freed the memory it was asked for. While it compacts and evicts, the reactor
is stalled. How long that takes depends mostly on how much memory is
requested at once, and on how dense the segments are (see the cost of
compaction in [Compaction](#compaction)).

LSA itself works to keep these stalls short:

* The background reclaimer keeps about 60 MB of Seastar memory free, in
  small preemptible steps, so that ordinary allocations rarely need to
  reclaim synchronously at all.
* When Seastar does need memory, LSA reclaims only `lsa_reclamation_step`
  segments (by default one) at a time. The default used to be 16 segments
  (4 MiB, as segments were then 256 KiB), and reclamation then routinely took
  6 to 9 ms (#1274). Changing it to one segment brought even the worst case
  measured below 1 ms (commit `cd9955d2ce`, 2016).
* For dense evictable regions, it evicts instead of compacting, and it evicts
  only a little more than needed before compacting.
* Allocating section reserves decay, so a reserve that grew once does not
  stay large.

The biggest requests, and therefore the worst stalls, usually come from
**allocating sections with large reserves**. Before running its function, an
allocating section makes sure its whole learned reserve is free, without
preemption. If an operation once needed 128 segments (16 MiB) with
reclamation disabled, every later entry to that section, until the reserve
decays, may first compact and evict up to 16 MiB in one stall. The sections
that update the row cache when a memtable is flushed have been a frequent
source of such stalls (commit `082342ecad`). To avoid them:

* **Do little work in each allocating section.** Large operations should
  proceed in small steps, each in its own entry to the section, with
  preemption checks between the steps. This is how the cache is updated from
  a flushed memtable: `row_cache::do_update()` moves one partition (or part
  of a large partition) per entry to the section, and yields between them.
  `mutation_partition_v2::apply_monotonically()` supports this by being
  preemptible and resumable.
* **Do preparation work outside the section**, in standard memory, where it can
  yield. For example, a large frozen mutation is first deserialized with
  `unfreeze_gently()`, which yields, and only then applied to the memtable
  (commit `a9f157b648`), instead of being deserialized inside the section.
* **Do not mix rare large operations with frequent small ones in the same
  allocating section object**, so the frequent ones do not inherit a large
  reserve.
* **Free large structures gradually** (see the previous section), and make
  eviction functions free a bounded amount of memory per call.

To find the cause of a stall, look for `lsa-timing` warnings: they say how
much memory the reclamation tried to free and what the reserve was, and they
include a backtrace for non-preemptible reclamations. Then look for
`increasing reserve in section ...` messages from the `lsa` logger, which
name the section whose reserve grew and include a backtrace of the operation
that needed it. The `reclaim_time_ms`, `compact_time_ms` and `evict_time_ms`
metrics show the total time spent. See [Logs](#logs) and [Metrics](#metrics).

# Part 4: Pitfalls checklist

When writing or reviewing code that touches LSA, check that:

1. **Allocator contexts match.** Everything allocated under
   `with_allocator(X)` is freed under `with_allocator(X)`, including temporaries,
   objects in standard containers, coroutine frames' contents, error paths and
   destructors that run during stack unwinding. LSA does not set the
   allocator for eviction functions: a function passed to
   `region::make_evictable()` must set it itself, as the cache tracker's does.
2. **No reference into a region survives a possible reclamation**: any
   allocation (standard or LSA) outside a locked region, any deferring point,
   and any retry of an allocating section. This includes iterators and
   `bytes_view`s/`managed_bytes_view`s into LSA data.
3. **Code in an allocating section can be retried**: no consumed inputs, no
   side effects that become wrong when repeated, parsers are reset.
4. **No allocating section nests inside another on the same region**, and
   nothing yields while a region is locked or inside `with_allocator()`.
5. **No contiguous allocation through LSA exceeds
   `preferred_max_contiguous_allocation()`** (13107 bytes). Use fragmented
   types. Be careful when copying a value that came from standard memory, where
   the limit is 128 KiB (`managed_bytes`' copy constructor once got this
   wrong, commit `4e2f62143b`).
6. **Types stored in LSA are nothrow move constructible and fix up pointers to
   themselves** when moved.
7. **Data structures that eviction modifies are only changed under a lock**,
   even if the structures are in standard memory.
8. **Reclamation is not left disabled for long**, and reserves are not
   unnecessarily large: a big reserve evicts a lot of cache on every
   section entry.
9. **Regions are empty when destroyed**, `lsa_buffer`s do not outlive their
   region, and nothing in LSA is accessed from another shard.
10. **Tests exercise allocation failures**: run the code under Seastar's
    allocation failure injection (`seastar::memory::with_allocation_failures()`,
    used by many tests in `test/boost/`) to make sure the retry paths work.

# Part 5: Debugging and observability

## Metrics

LSA metrics are in the `lsa` group (so in Prometheus `scylla_lsa_*`). Each
has one value per shard, which is a total over all the regions of that shard.
They are not broken down by region, but the related metrics listed after the
table give the cache's and the memtables' shares:

| Metric | Meaning |
|---|---|
| `total_space_bytes` | Memory in segments owned by all regions (used or free space inside them). |
| `used_space_bytes` | Bytes of live objects in all regions. |
| `occupancy` | `used_space_bytes / total_space_bytes`, in percent. Low occupancy means a lot of compaction work is possible (and maybe needed). |
| `free_space` | Free segments held by LSA, beyond the emergency reserve. |
| `non_lsa_used_space_bytes` | Memory used by the standard allocator, that is all allocated memory minus LSA memory. |
| `memory_allocated`, `memory_freed` | Bytes allocated and freed in LSA (counters). |
| `memory_compacted`, `segments_compacted` | Bytes copied and segments freed by compaction. Compare to `memory_allocated` to estimate the compaction overhead. |
| `memory_evicted` | Bytes freed by eviction functions. |
| `reclaim_time_ms` | Time spent returning memory to the standard allocator (synchronous and background reclaim, reserve refill). |
| `compact_time_ms`, `evict_time_ms` | Time spent compacting and evicting while building LSA reserves (entering allocating sections, LSA allocations needing a segment), not already counted in `reclaim_time_ms`. |

Related metrics: `scylla_cache_bytes_used` and `scylla_cache_bytes_total` (the
cache region's occupancy), `scylla_memory_{regular,system}_dirty_bytes` and
`..._unspooled_dirty_bytes` (memtable memory, from the dirty memory manager),
and Seastar's `scylla_memory_free_memory` and `scylla_memory_allocated_memory`.
The `perf-simple-query` tool and other tools based on `test/perf/perf.hh`
report `logallocs/op`, the number of LSA allocations per operation.

## Logs

* **`lsa-timing`**: a warning when a reclamation took longer than the reactor
  stall threshold (`blocked-reactor-notify-ms`), followed by details at info
  level:

  ```
  lsa-timing - reclaim took 25012 us, trying to release 0.125 MiB non-preemptibly, reserve: {goal: 1, max: 30}, at <backtrace>
  lsa-timing - - reclamation rate = ... MiB/s
  lsa-timing - - evicted memory: ... MiB
  ```

  The name is `reclaim`, `reclaim_segments`, `compact` or `evict`. A large
  `max` reserve points to an allocating section with a big learned reserve.
  The backtrace (for non-preemptible reclaims) shows which allocation triggered
  it. Set `--logger-log-level lsa-timing=debug` to see every reclamation.
* **`lsa`** at info level:
  `LSA allocation failure, increasing reserve in section 0x... (<name>) to N segments; trace: ...`
  and `Standard allocator failure, increasing head-room in section ...`.
  These are normal occasionally. Frequent ones, or a large `N`, mean an
  operation needs a lot of memory with reclamation disabled, and the
  backtrace shows which one.
* **`logalloc::bad_alloc`** messages say where LSA gave up:
  `failed to refill emergency reserve of 30 (have 17 free segments)`,
  `failed to allocate segment (_current_emergency_reserve_goal=...)`, or
  `failed to reclaim N bytes of memory, while attempting to ensure an std reserve of M`.
  They mean that LSA could not find memory even after compacting and
  evicting everything it could: a real out-of-memory condition in which
  memory is held by non-evictable regions (usually memtables) or by the
  standard allocator.
* `lsa` at debug and trace levels logs the reclamation algorithm in detail.

## Configuration options

| Option | Default | Effect |
|---|---|---|
| `abort_on_lsa_bad_alloc` (`--abort-on-lsa-bad-alloc`) | false | Abort, producing a core dump, when an allocating section cannot reserve memory. Normally only the operation fails, which makes the cause hard to find. `test.py` enables it. |
| `lsa_reclamation_step` | 1 | Minimum number of segments to reclaim each time Seastar asks. It was lowered from 16 to 1 in 2016 because of reclamation stalls of several milliseconds (see [Avoiding reclamation stalls](#avoiding-reclamation-stalls)). |
| `defragment_memory_on_idle` | false | Compact segments when the CPU is idle. |
| `sanitizer_report_backtrace` | false | In debug builds, record a backtrace for every LSA allocation so sanitizer reports show where an object came from. Slow. |

## Debug builds

Debug and sanitize builds define `DEBUG_LSA_SANITIZER`. The *LSA sanitizer*
(`region_sanitizer` in `logalloc.cc`, logger `lsa-sanitizer`) tracks every
allocation and aborts with a report on: leaked objects when a region is
destroyed, freeing an address that was not allocated, a size mismatch between
allocation and free, a migrator that reports a different size, and
overlapping objects after a region merge. With `sanitizer_report_backtrace`
the report includes the allocation backtrace. A separate check (logger
`lsa-migrator-sanitizer`) catches use of migrators that were unregistered,
for example by a type whose objects outlived it.

Free space in segments is also poisoned for AddressSanitizer, so use-after-free
and out-of-bounds accesses inside LSA segments are detected like ordinary
heap errors.

## GDB

`scylla-gdb.py` (see [debugging.md](debugging.md)) has these LSA commands:

* `scylla memory`: overall memory use, including LSA allocated/used/free, the
  cache and memtables.
* `scylla lsa`: LSA memory in use, the emergency reserve, and each region with
  its occupancy and whether it is reclaimable.
* `scylla segment-descs`: every segment with its free space and owner region.
* `scylla lsa-segment <address>`: the objects in one segment, with their
  sizes and migrators (that is, their types).
* `scylla lsa-check`: consistency checks of the region and segment
  bookkeeping.

To find out what types use LSA memory in a running test,
`region::collect_stats()` returns live bytes per migrator name
(`test/perf/memory_footprint_test.cc` uses it).

## REST API

`POST /lsa/compact` calls `tracker::reclaim()` on every shard with an
unlimited target. Despite its name it does not just defragment: it releases as
much memory as LSA can, which **evicts the whole row cache**. Use it only in
tests.

# Part 6: Where LSA lives in the source code

| File | Contents |
|---|---|
| `utils/allocation_strategy.hh` | `allocation_strategy`, `current_allocator()`, `with_allocator()`, `standard_migrator<T>`, `DynamicObject`, `hold_reserve`, `alloc_strategy_unique_ptr`. |
| `utils/logalloc.hh` | Public API: `region`, `tracker`, `shard_tracker()`, `reclaim_lock`, `allocating_section`, `lsa_buffer`, `region_listener`, `occupancy_stats`, size constants. |
| `utils/logalloc.cc` | Everything else: segments, segment pool and store backends, `region_impl` (allocation, freeing, compaction, merging), tracker (reclamation, eviction, idle compaction, metrics), background reclaimer, `reclaim_timer` (stall reports), sanitizer. The comment above `class region_impl` describes the segment layout. |
| `utils/lsa/weak_ptr.hh`, `utils/lsa/chunked_managed_vector.hh` | LSA-specific helpers. |
| `utils/managed_bytes.hh`, `utils/managed_ref.hh`, `utils/managed_vector.hh`, `utils/entangled.hh` | LSA-aware types. |
| `utils/log_heap.hh`, `utils/vle.hh` | The histogram of segments, and the object descriptor encoding. |
| `utils/lru.hh` | `evictable`, `index_evictable`, `lru`: the cache's eviction order. |
| `db/cache_tracker.hh`, `db/row_cache.{hh,cc}`, `db/cache_mutation_reader.hh` | The cache region, its eviction function, and the most complex LSA user. |
| `replica/memtable.{hh,cc}`, `replica/dirty_memory_manager.{hh,cc}` | Memtable regions and dirty memory accounting. |
| `mutation/partition_version.{hh,cc}`, `mutation/mutation_partition_v2.{hh,cc}`, `mutation/mutation_cleaner.hh` | MVCC data structures in LSA. See also [mvcc.md](mvcc.md) and [row_cache.md](row_cache.md). |
| `replica/partition_snapshot_reader.hh` | A reader that keeps its position in LSA across deferring points. |
| `utils/cached_file.hh`, `sstables/partition_index_cache.hh`, `sstables/index_reader.hh` | Index caches in the cache region. |
| `replica/logstor/cache.hh` | The logstor cache, in the cache region. |
| `main.cc` | `prime_segment_pool()`, tracker configuration, the `background_reclaim` scheduling group. |
| `api/lsa.cc` | The `/lsa/compact` REST API. |
| `scylla-gdb.py` | GDB commands. |

## Tests

* `test/boost/logalloc_test.cc`: the allocator itself (compaction, eviction,
  reserves, `lsa_buffer`, `hold_reserve`, ...).
* `test/boost/logalloc_standard_allocator_segment_pool_backend_test.cc`,
  `test/boost/managed_bytes_test.cc`, `test/boost/managed_vector_test.cc`,
  `test/boost/chunked_managed_vector_test.cc`, `test/boost/cached_file_test.cc`.
* `test/perf/logalloc.cc` (microbenchmark) and
  `test/perf/memory_footprint_test.cc` (memory used per mutation in cache and
  memtables).
* Row cache and MVCC tests (`test/boost/row_cache_test.cc`,
  `test/boost/mvcc_test.cc`) exercise LSA under allocation failures and
  memory pressure.

## Further reading

* S. M. Rumble, A. Kejriwal, J. Ousterhout, [Log-structured Memory for DRAM-based Storage](https://www.usenix.org/conference/fast14/technical-sessions/presentation/rumble),
  FAST 2014: the idea behind LSA, including the analysis of compaction cost
  versus memory utilization.
* The commit messages in `git log -- utils/logalloc.cc` explain the reasons
  for most design decisions and tuning constants. Many of them are cited
  above.
