# Live minidumps

A minidump is an ELF core file of a running Scylla process. gdb opens it like
a kernel core dump and `scylla-gdb.py` works on it. It leaves out the cache
and memtable contents and other bulk data (see "Known limits" for the user
data it can still hold). It is taken without stopping the node: each shard
stops for about 0.1 ms, and then waits in write faults for a few to a few tens
of milliseconds in total while its memory is copied.

```
curl -X POST 'http://localhost:10000/system/minidump'
nodetool minidump [--path FILE] [--include SELECTORS] [--exclude SELECTORS] [--dry-run] [--param NAME=VALUE ...]
gdb -ex 'source scylla-gdb.py' build/release/scylla /var/lib/scylla/minidumps/scylla-20261013T120000.core
```

The call returns a JSON report with:

- the file size and the bytes of every memory category, with whether each was
  included;
- per-shard stop and fault-wait times, and fault percentiles;
- the memory the dump could use and used;
- timings of each phase.

A failed or aborted dump returns HTTP 500 with the report as the message, so
nodetool fails. When a cap stopped the dump, the reason names the cap and how
to rerun with a different value (see "Safety caps").

The same report, without the timing of the copy, is stored in the dump as an
ELF note named `SCYLLA`.

A dry run (`--dry-run`, `dry_run=true`) returns the report with the sizes of
every category and what would be included, without writing a file. It still
stops each shard once, like a real dump, but doesn't write-protect memory, so
it needs no userfaultfd access and causes no write faults. Memory is read
while the shards run, so the sizes are approximate, and the caps on stalls,
copy memory, dump size and disk space are not checked.

## What is in the dump

Memory is classified into categories, and rules decide which are dumped.

| Category | What | Default |
|---|---|---|
| `small`, `large` | seastar allocator memory outside LSA: database, tables, schemas, sstable objects, reactor, tasks, queues, the allocator's page array | included |
| `free` | free pages of the seastar allocator | excluded |
| `lsa:<owner>` | LSA segments by owning region: `lsa:cache` (row cache and index caches), `lsa:memtable`, `lsa:free`, `lsa:unknown` | excluded |
| `tag:<name>` | ranges registered with `minidump::registered_ranges`: `tag:bloom_filter`, `tag:commitlog_buffer` | excluded |
| `map:<path>` | memory outside the seastar allocator: `.data`/`.bss`/RELRO of the binary and libraries, the vDSO, other anonymous mappings (`map:[anon]`) | included (anonymous mappings over 256MB are skipped) |

Rules are globs. They are matched in this order:

1. user `--include` selectors;
2. user `--exclude` selectors;
3. the defaults: `exclude lsa:*`, `exclude tag:*`, `exclude free`, `include *`.

For example, `--include lsa:memtable` adds the memtables.

Registered ranges which don't cover whole pages are zeroed.

Reactor threads are dumped with their registers, their live stack and their
TLS. The stale part of the stack, below the stack pointer, is left out. Other
threads are listed by gdb (through libthread_db) but have no registers. A
shard which doesn't reach a task boundary within 1s isn't parked: its heap,
stack and registers are left out, and only its TLS is dumped, as it is while
the shard runs.

### Known limits

- Freed objects in dumped pages keep their old contents, since `free()` does
  not clear memory; so does the unused part of a live object (for example the
  tail of a 512-byte pool slot holding a smaller object). That can include
  user data. Only whole free pages are left out.
- In-flight requests and results are dumped: CQL and RPC buffers, and reader
  buffers.
- The sstable first and last keys in sstable objects, and the partition keys
  sampled in sstable summaries, are dumped.
- Prepared statement query text is dumped.
- Secrets in ordinary heap memory are dumped: TLS private keys, cached
  credentials and encryption-at-rest keys, including the key schedules
  OpenSSL and GnuTLS keep internally. Handle a minidump like the node's key
  material.
- Pages a shard has never touched are not write-protected, since protecting
  them would make its snapshot stop take milliseconds. If such a page is
  written after the snapshot and before it is copied, the dump shows the
  newer content instead of zeros.

## How it works

1. **Collect shard information.** On shard 0, collect every shard's memory
   range, stack and TLS location, and register each shard's memory with
   `userfaultfd` in write-protect mode. Fault-handler threads start.
2. **Park the shards, one at a time.** Each shard gets a task
   (`alien::run_on`) which, at a task boundary:
   - saves the registers;
   - copies the live stack and TLS;
   - exports LSA segment ownership and the registered ranges;
   - write-protects the shard's memory with one `UFFDIO_WRITEPROTECT` call.

   That is the snapshot instant of the shard. There is no global barrier,
   and a shard is parked only when a worker (step 4) is free to take it:
   with a worker per shard the snapshots are a few ms apart; with fewer,
   each waits for a worker to finish a shard, so they can spread over
   seconds.
3. **Copy on write.** A write to protected memory blocks the writer while a
   fault handler copies the 16KB block to the copy buffer and lifts the
   protection.
4. **Classify, copy and write, shard by shard as soon as it is parked.** A
   worker:
   1. has its helper thread lift protection from excluded LSA segments,
      meanwhile;
   2. reads the page map as of the snapshot and lifts protection from free
      pages and other excluded memory;
   3. applies the rules;
   4. copies everything included into the copy buffer, hottest first (the
      page map, then small-object spans), lifting protection in runs as it
      goes; that ends the shard's protected time;
   5. writes the shard's memory to the file, from the copies, and gives their
      room in the copy buffer back.
5. **Finish the ELF file:** the memory outside the allocator, then the program
   headers and notes, then the ELF header at the start of the file, which
   points at them.

The copy buffer is shared by the fault handlers and the background copy, and
populated a little ahead of its use; since each shard is written as soon as it
is copied, it holds about one shard's dump per worker, whatever the size of the
node. What doesn't fit in it stays protected until its shard is written.
Nothing the dumper allocates is mapped while a shard is protected: mapping
memory takes the lock the shards' snapshots and faults need.

Helper threads run on CPUs without shards when there are any, else beside the
shards. Pass `helper_cpus` to choose others: CPUs the process may not use, or
CPUs that shards are pinned to, are refused with an error (shards which aren't
pinned, as with `--overprovisioned`, may run on any CPU). There are up to 16 fault
handlers (capped at the number of helper CPUs) at normal priority, and up to 8
workers at the lowest priority; the report lists the CPUs and both counts.

## Memory

The dumper's memory comes from outside Scylla's own, from what the system (or
Scylla's memory cgroup) keeps free. It counts free memory as `MemAvailable`,
or the cgroup's headroom with its page cache as reclaimable when that is less,
and keeps at least `min_free_memory_mb` (default: 512 MB or 2% of RAM,
whichever is more) free:

- a dump is refused if it would leave less than 64 MB for copies after the
  workers' scratch memory;
- the copy buffer can take half of the rest, up to 8 GB (`max_memory_mb` sets
  it), but takes only what it uses;
- a watchdog aborts the dump if free memory falls under the floor while it
  runs;
- each shard's part of the file is written out and dropped from the page cache
  once written.

## Safety caps

The dump is aborted, and all protection lifted, when any of these is exceeded.
Each is a REST parameter of `/system/minidump`, which nodetool passes with
`--param NAME=VALUE`; the abort reason gives the command to rerun with.

| Cap | Default | Parameter |
|---|---|---|
| Stopping one shard for its snapshot | 50 ms | `max_park_us` |
| A single write fault | 50 ms | `max_fault_us` |
| One shard's stop and fault waits together | 250 ms | `max_stall_us` |
| The whole dump | 60 s | `max_copy_seconds` |
| Dump size | 0.15 of the shards' memory | `max_size_ratio` |
| Free disk left | 10% | `min_free_disk_ratio` |
| Free memory left | 512 MB or 2% of RAM | `min_free_memory_mb` |
| Copy buffer | full | `max_memory_mb` |

A write fault's wait is counted from when a fault handler reads it from the
userfaultfd, so the time it waits for a free handler before that is not
counted.

Only one dump runs at a time.

## Requirements

x86_64 or aarch64 with 4 KB pages, and only in build modes with the seastar
allocator (release and dev); elsewhere the call fails. On aarch64 the kernel
must support userfaultfd write protection there (`CONFIG_HAVE_ARCH_USERFAULTFD_WP`).

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
