# Live minidumps

A minidump is an ELF core file of a running Scylla process. gdb opens it like
a kernel core dump and `scylla-gdb.py` works on it. It leaves out the cache
and memtable contents and other bulk data (see "Known limits" for the user
data it can still hold). It is taken without stopping the node: each shard
stops for about 0.1 ms, and then waits in write faults for a few to a few tens
of milliseconds in total while its memory is copied.

```
curl -X POST 'http://localhost:10000/system/minidump'
nodetool minidump [--path FILE] [--include SELECTORS] [--exclude SELECTORS] [--dry-run]
gdb -ex 'source scylla-gdb.py' build/release/scylla /var/lib/scylla/minidumps/scylla-20261013T120000.core
```

The call returns a JSON report with:

- the file size and the bytes of every memory category, with whether each was
  included;
- per-shard stop and fault-wait times, and fault percentiles;
- timings of each phase.

A failed or aborted dump returns HTTP 500 with the report as the message, so
nodetool fails.

The same report, without the timing of the copy, is stored in the dump as an
ELF note named `SCYLLA`.

## What is in the dump

Memory is classified into categories, and rules decide which are dumped.

| Category | What | Default |
|---|---|---|
| `small`, `large` | seastar allocator memory outside LSA: database, tables, schemas, sstable objects, reactor, tasks, queues, the allocator's page array | included |
| `free` | free pages of the seastar allocator | excluded |
| `lsa:<owner>[:<name>]` | LSA segments by owning region: `lsa:cache` (row cache and index caches), `lsa:memtable:<ks>.<table>`, `lsa:free`, `lsa:unknown` | excluded |
| `tag:<name>` | ranges registered with `minidump::registered_ranges`: `tag:bloom_filter`, `tag:sstable_summary`, `tag:commitlog_buffer` | excluded |
| `map:<path>` | memory outside the seastar allocator: `.data`/`.bss`/RELRO of the binary and libraries, the vDSO, other anonymous mappings (`map:[anon]`) | included (anonymous mappings over 256MB are skipped) |

Rules are globs. They are matched in this order:

1. user `--include` selectors;
2. user `--exclude` selectors;
3. the defaults: `exclude lsa:*`, `exclude tag:*`, `exclude free`, `include *`.

For example, `--include lsa:memtable:ks.t` adds the memtables of one table.

Within included small-object pages, free objects and the unused tails of
spans are zeroed, because `free()` does not clear memory. The link word of
each free object is kept, so free lists stay walkable. Registered ranges which
don't cover whole pages are zeroed too.

Reactor threads are dumped with their registers, their live stack and their
TLS. The stale part of the stack, below the stack pointer, is left out. Other
threads are listed by gdb (through libthread_db) but have no registers.

### Known limits

- Live allocations can still carry stale bytes. Seastar does not clear memory
  on allocation, so the unused part of a live object (for example the tail of
  a 512-byte pool slot holding a smaller object) keeps whatever the previous
  occupant left there. That can include user data.
- In-flight requests and results are dumped: CQL and RPC buffers, and reader
  buffers.
- The sstable first and last keys in sstable objects are dumped.
- Prepared statement query text is dumped.
- Secrets in ordinary heap memory are dumped: TLS private keys, cached
  credentials and encryption-at-rest keys, including the key schedules
  OpenSSL and GnuTLS keep internally. Handle a minidump like the node's key
  material.

## How it works

1. **Collect shard information.** On shard 0, collect every shard's memory
   range, stack and TLS location, and register each shard's memory with
   `userfaultfd` in write-protect mode. Fault-handler threads start.
2. **Park the shards, one at a time.** Each shard gets a task
   (`alien::run_on`) which, at a task boundary:
   - saves the registers;
   - copies the live stack and TLS;
   - exports LSA segment ownership, the registered ranges and the free-list
     heads;
   - write-protects the shard's memory with one `UFFDIO_WRITEPROTECT` call.

   That is the snapshot instant of the shard. Shards are snapshotted a few ms
   apart, with no global barrier.
3. **Copy on write.** A write to protected memory blocks the writer while a
   fault handler copies the 64KB block to a pre-touched area and lifts the
   protection.
4. **Classify and copy, shard by shard as soon as it is parked.** A worker:
   1. lifts protection from excluded LSA segments;
   2. reads the page map as of the snapshot and lifts protection from free
      pages and registered ranges;
   3. applies the rules;
   4. copies everything included into memory, hottest first (the page map,
      then small-object spans), lifting protection as it goes;
   5. walks the free lists to find what to zero.
5. **Write the ELF file:** the notes, then shard memory from the copies, then
   the memory outside the allocator.

Helper threads run on CPUs without shards when there are any, else beside the
shards. Pass `helper_cpus` to choose others: CPUs the process may not use, or
CPUs that shards run on, are refused with an error. There are up to 16 fault
handlers (capped at the number of helper CPUs) at normal priority, and up to 8
workers at the lowest priority; the report lists the CPUs and both counts.

## Safety caps

The dump is aborted, and all protection lifted, when any of these is exceeded:

| Cap | Limit |
|---|---|
| Snapshot stop of one shard | 5 ms |
| A single fault wait | 5 ms |
| One shard's total fault wait | 30 ms (`max_stall_us`) |
| Copy buffer (fault area and pre-copy area) | full |
| The copy | 60 s |
| Dump size | 0.15 of the shards' memory (`max_size_ratio`) |
| Free disk left after the dump | under 10% (`min_free_disk_ratio`) |

Only one dump runs at a time.

## Requirements

x86_64 only for now, and only in build modes with the seastar allocator
(release and dev); elsewhere the call fails.

The process must be allowed to create a userfaultfd that handles kernel faults,
such as writes by `read()` or AIO into protected buffers. Any of these works:

- `vm.unprivileged_userfaultfd=1`;
- `CAP_SYS_PTRACE`;
- read-write access to `/dev/userfaultfd`.

## Reading a minidump

```
gdb -q build/release/scylla scylla.core
(gdb) source scylla-gdb.py
(gdb) info threads
(gdb) scylla memory
(gdb) scylla lsa
(gdb) scylla tables
(gdb) scylla task_histogram
```

Memory which was left out reads as "Cannot access memory at address ...".
`scylla cache` reports the cache contents as not available.
