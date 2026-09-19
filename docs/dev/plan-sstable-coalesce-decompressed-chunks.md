# Plan: coalesce decompressed chunks in the compressed sstable data source

Scope: `compressed_file_data_source_impl` (sstables/compress.cc) and the
`READING_BYTES` fragment vector in `primitive_consumer_impl`
(sstables/consumer.hh). Sizing target throughout: a 1 MB cell, the default
`compaction_large_cell_warning_threshold_mb`.

## Problem

`compressed_file_data_source_impl::get()` returns exactly one decompressed
compression chunk per call. With the default `chunk_length_in_kb = 4` a 1 MB
value reaches the parser as 256 fragments. Each fragment pays: a heap
allocation for the output buffer, a `reader_permit::request_memory()` round
trip and a tracked deleter, one `input_stream::consume()` iteration with a
`mark_blocked()/mark_unblocked()` pair, and one parser-coroutine resume.

The underlying `file_input_stream` already reads 128 KB
(`sstable_buffer_size`, sstables/sstables.cc:3304) with `read_ahead = 4`, so
the compressed bytes for ~30 consecutive chunks are in memory when `get()`
runs. We hand them out one at a time.

### Measured (dev build, one shard, data in page cache)

`scylla perf-sstable --mode sequential_read --partitions 300 --num_columns 1
--column_size 1048576`, LZ4, random (incompressible) data:

| | |
|---|---|
| throughput | 4040 rows/s ≈ 4 GB/s ≈ 1 µs per 4 KB chunk |
| intrinsic CPU | 50 %: memmove 35 (LZ4 literal copy + copy into LSA), crc32 13, LZ4 3 |
| per-fragment CPU | 40 %: input_stream/future plumbing 16, malloc/free 10, permit accounting 6, `get()` frame + offsets lookup 6, parser state machine 2.5 |
| other | 10 %: I/O submission, mutation build |

Coalescing removes 31 of every 32 fragments, so the ceiling is ≈ 38 % less
CPU per MB on this shape. Real, compressible data spends more in
decompression and lowers the per-fragment share; zstd and deflate lower it
further. Honest range: 20–38 %.

This is a CPU change only. Disk I/O (128 KB reads, read-ahead 4) is
unchanged by construction.

## Design

### Principle: one underlying read per `get()`, never two

Today `get()` calls `_input_stream->read_exactly(chunk_len)`. Internally
that either shares a slice of the stream's buffered 128 KB (`_buf`) or, when
`_buf` is empty, awaits `_fd.get()`.

New `get()` takes the whole buffered block instead: `input_stream::read()`
moves out the stream's current `_buf` if non-empty, otherwise awaits exactly
one `_fd.get()`. We keep that block in a member `_buf` and carve compressed
chunks from it, decompressing each into one output buffer, until:

1. `_buf` has fewer bytes than the next chunk needs (partial chunk stays
   in `_buf` for the next call), or
2. the output buffer is full (cap below), or
3. `_pos == _end_pos` (range end), or
4. `need_preempt()` is set (only checked after the first chunk).

Then request permit memory for the exact output size and return.

The only `co_await` on the input stream happens when nothing has been
produced yet in this call — the same condition under which today's `get()`
waits. There is no readiness probing and no pending future: the amount of
coalescing is simply "whatever one underlying buffer holds". That gives the
same 32× fragment reduction for 4 KB chunks in 128 KB reads, because one
compressed 128 KB block decompresses to ≥ 128 KB.

### Pseudocode

```
get():
  if _pos >= _end_pos: return {}
  open stream if needed
  ucl = uncompressed_chunk_length()
  cap_chunks = max(1, _max_out / ucl)                     // _max_out = options.buffer_size
  remaining_chunks = ceil((_end_pos - (_pos - _pos % ucl)) / ucl)
  out = temporary_buffer(min(cap_chunks, remaining_chunks) * ucl)   // one allocation
  produced = 0; first_offset = -1
  loop:
    addr = locate(_pos)
    if _pos != _beg_pos and addr.offset != 0: throw not-aligned     // unchanged check
    if addr.chunk_len == 0: throw malformed                          // unchanged
    if _buf.size() < addr.chunk_len:
        if produced > 0: break                                       // never wait mid-call
        chunk = co_await read_chunk(addr.chunk_len)                  // waits like today
    else:
        chunk = _buf.share(0, addr.chunk_len); _buf.trim_front(addr.chunk_len)
    verify checksum; feed digest                                     // unchanged, per chunk
    n = uncompress(chunk → out + produced)
    if first_offset < 0: first_offset = addr.offset
    produced += n; _pos += n - (first chunk ? addr.offset : 0); _underlying_pos += addr.chunk_len
    if _pos >= _end_pos or produced + ucl > out.size() or need_preempt(): break
  out.trim(produced); out.trim_front(first_offset)
  digest end-of-file check                                           // unchanged
  units = co_await _permit.request_memory(out.size())
  return make_tracked_temporary_buffer(out, units)

read_chunk(len):                       // only when produced == 0
  if _buf.size() >= len: share/trim as above
  chunk = temporary_buffer(len); copy _buf into it; _buf = {}
  while filled < len:
      b = co_await _input_stream->read()
      if b.empty(): throw premature end-of-file                      // same message as today
      copy min(needed, b.size()); keep the rest in _buf
  return chunk
```

`skip(n)`: unchanged arithmetic. Then, if `underlying_n <= _buf.size()`,
trim `_buf`; else drop `_buf` and forward the remainder to
`_input_stream->skip()`. `close()` is unchanged (no pending reads exist).

### Rules that keep latency flat

1. **One await, same as today.** The stream is awaited only when the call
   has produced nothing yet. A point read whose bytes are in chunk k gets
   chunk k after the same wait as today, plus bounded CPU for chunks
   k+1.. that are already resident.
2. **Cap the output.** `options.buffer_size` (128 KB in production) worth
   of decompressed bytes per call, and never past `_end_pos`. Any number of
   whole chunks is a valid return; the parser does not assume chunk-sized
   buffers.
3. **Yield to the reactor.** `need_preempt()` between chunks. Returning
   early is the yield; no `maybe_yield()`. `need_preempt()` is two relaxed
   loads (seastar preempt.hh:50-62), ≈ 1 ns per 4 KB chunk against ≈ 1 µs of
   work. Not a cost.

Bounded extra CPU per call on resident data, per 128 KB of output
(*hypothesis from vendor throughput*): LZ4 ≈ 40 µs, zstd ≈ 130 µs, deflate
≈ 430 µs. All under the 1 ms stall threshold; rule 3 covers deflate under
contention.

### Read-ahead interaction

`file_data_source_impl::get()` pops one read-ahead buffer and re-issues so
that `read_ahead` reads stay in flight. We call it exactly once per
underlying block, as today (`read_exactly` on an empty `_buf` also calls it
once). `file_input_stream_history` heuristics see the same pattern.

### Memory

Transient permit charge per `get()` rises from 4 KB to ≤ 128 KB. That equals
the tracked read buffer the permit already carries for the same stream
(`make_tracked_file`, sstables.cc:3316). Steady-state memory is unchanged:
the parser pins the same value bytes in fewer, larger fragments. We request
the exact produced size once, after the loop, so under memory pressure a
call that produced one chunk asks for one chunk, as today.

Ordering change: the output buffer is allocated before the permit grant
rather than after. The bytes exist for at most the duration of the
`request_memory` wait; acceptable, and simpler than requesting the cap up
front and shrinking (no shrink API on `resource_units`).

### Correctness hot spots

- `addr.offset` applies only to the first chunk after construction or
  `skip()` (`_pos == _beg_pos`). Recorded on the first iteration and
  applied as one `trim_front` at the end. `_pos` advances by
  `n - offset` for that chunk and by `n` afterwards.
- `_pos` / `_underlying_pos` advance per chunk so `locate()` and `skip()`
  arithmetic stay exact.
- A short decompressed chunk that is not the last chunk of the file leaves
  `_pos` misaligned; the existing alignment check at the top of the next
  iteration throws, as it does today across calls.
- Digest: per-chunk feeding verbatim; end-of-file mismatch check after the
  loop keyed on `_pos == uncompressed_file_length()`, as today.
- Exception on chunk k+3 now surfaces on the `get()` that would also have
  returned k..k+2. Consumers treat any `get()` exception as fatal for the
  stream, so only the count of valid bytes seen before the error changes.
  No test asserts that count (grep `failed checksum` in test/).
- Both formats: the class is a template over checksum type and digest mode;
  k/l (`adler32`, chunks-only digest) and mc+ (`crc32`, checksum-all)
  share the loop.
- Reversed reads and index skips reach this class only through
  `skip()`/`get()`; no new entry points.

### Interaction with item 1 (skip unrequested cell values)

Independent code. Item 1 makes mid-row `skip()` routine; that exercises the
"`_buf` partially consumed, then skip" path here. The wasted decompression
before a skip grows from ≤ 4 KB to ≤ 128 KB per skipped value, out of ≥ 1 MB.
Item 2 first, then item 1 with tests that skip into a coalesced buffer.

## Fold-in: reuse the `READING_BYTES` fragment vector

`primitive_consumer_impl::read_bytes()` (consumer.hh:246) slow path collects
fragments in `utils::small_vector<Buffer, 1> _read_bytes` and on completion
(consumer.hh:446) builds a new `std::vector` from it: one malloc + one free
per straddling value.

Change: make `_read_bytes` a `std::vector<Buffer>`; on slow-path entry seed
it with `std::move(where).release()` (keeps the destination's capacity),
on completion move it back into the destination. Also `clear()` it in
`reset()` so an index skip landing mid-value releases pinned buffers.

Small cells are unaffected: the fast path (value inside the current buffer,
consumer.hh:247-252) does not touch `_read_bytes`. A cell straddles a 128 KB
buffer with probability ≈ size/128 KB: 0.08 % at 100 B, 0.8 % at 1 KB,
7.6 % at 10 KB. After coalescing, buffers are 128 KB for compressed tables
too, so straddling becomes rare for everything but large values. Six lines;
rides along because coalescing is what makes the vector small and stable.

## Rejected alternatives

- **Probe `read_exactly()` futures for readiness and continue while
  available.** Coalesces across underlying blocks, but a non-ready future
  has already called `_fd.get()` and will later write into the stream's
  `_buf`; it must be stored and drained before any `skip()`/`close()`
  (`file_data_source_impl` asserts no reads in flight on destruction,
  fstream.cc:218). Extra state, extra failure mode, same 32× for the
  default configuration. Not worth it.
- **Loop over several buffers inside `primitive_consumer_impl::consume()`.**
  `input_stream::consume()` hands one buffer per call; changing that touches
  every sstable component reader.
- **Bigger `chunk_length_in_kb`.** User-visible, hurts point-read
  amplification, does nothing for existing sstables.
- **Per-LSA-chunk copy in `set_value`.** LSA chunks are ≈ 13 KB
  (`max_managed_object_size`), so the destination is ≈ 80 pieces per MB
  regardless; `write_fragmented` already does the minimal N + M − 1
  memcpys. Coalescing shrinks N from 256 to 8; M is intrinsic. Dropped.

## Tests

New, in test/boost/sstable_test.cc next to
`test_skipping_in_compressed_stream`, using the same file-level harness
(`make_compressed_file_m_format_output_stream` / `_input_stream` with a
plain `file_data_source`):

- `test_compressed_stream_coalesces_chunks`: write ≈ 1.2 MB (300 chunks of
  4 KB, compressible pattern) with `buffer_size = 128 KB`, `read_ahead = 4`.
  Read with `input_stream::read()` until EOF. Assert byte equality with the
  written data and that the number of non-empty buffers is
  ≤ ceil(size / 128 KB) + 2.
- Same file: `skip(k)` for k inside a chunk and inside a coalesced block,
  then read to EOF; assert equality with the tail. Repeat with a skip after
  a partial read (exercises trimming `_buf`).
- Corrupt one byte in chunk 5 of the file; assert
  `malformed_sstable_exception` on read.
- Existing: `sstable_test`, `sstable_3_x_test` (zstd `multiple_chunks`,
  `test_sstable_write_large_cell_f`), `sstable_datafile_test`,
  `sstable_compaction_test`, `broken_sstable_test`, run in a dev build;
  the sstable suites also with `--blocked-reactor-notify-ms 1`.

## Measurement

Same data set and command as the baseline (`perf-sstable`, 300 × 1 MB, LZ4,
one shard) with the old binary and the new one, several runs each; report
rows/s and the profile split. Expect the per-fragment 40 % bucket to shrink
to a few percent and rows/s to rise accordingly. Also confirm no
`--blocked-reactor-notify-ms 1` reports in the boost sstable suites.

## Review

Findings from a hostile pass over the first draft, and what changed:

1. *The pending-future design has an unsafe `skip()`.* A stored
   `read_exactly` future, when it resolves, assigns the stream's `_buf` and
   recurses; a `skip()` issued meanwhile moves the stream, and the late
   assignment then serves stale bytes. Fixed by dropping readiness probing
   altogether; we own the block, the stream has no hidden state.
2. *`options.buffer_size` defaults to 8 KB in `file_input_stream_options`.*
   Tests and callers that do not set it would coalesce at most two 4 KB
   chunks. Cap is `max(options.buffer_size, one chunk)`; production sets
   128 KB. Documented; not a correctness issue.
3. *A short middle chunk.* `uncompress()` may legitimately return fewer
   bytes only for the file's last chunk. Mid-loop, the alignment check on
   the next iteration catches a short non-final chunk exactly as today's
   cross-call check does. Kept the check inside the loop rather than only
   at entry.
4. *`remaining_chunks` must be computed from the chunk base, not `_pos`.*
   Otherwise the first call after a mid-chunk start under-allocates by one
   chunk and the loop breaks early. Fixed in pseudocode.
5. *Permit request after allocation.* Called out as an ordering change and
   accepted; the alternative needs a shrink API that does not exist.
6. *Exception timing shift.* Enumerated; no test depends on it.
7. *`need_preempt()` before the first chunk would return an empty buffer,
   which means EOF to `input_stream`.* Checked only after producing at
   least one chunk.
8. *k/l format has `checksum_chunks_only` digest mode.* Same loop, only the
   `if constexpr` differs; covered by `sstable_test` k/l cases.
9. *Ordering claim "no added I/O waits" needs to be exact.* Restated as:
   the stream is awaited only when the current call has produced nothing,
   which is precisely when today's `get()` awaits.

## Measured result

`scylla perf-sstable --mode sequential_read --partitions 300 --num_columns 1
--column_size 1048576 --smp 1`, dev build, LZ4, one shard, against a fixed
316 MB compressed sstable data set (data resident in page cache), 3 runs
each:

| Build | partitions/s |
|---|---|
| origin/master (8066859d35) | 3006, 3023, 3006 (avg 3012) |
| this branch | 3197, 3183, 3186 (avg 3189) |

**≈ 6% higher throughput.** This is well below the 20–38% ceiling estimated
from the CPU-share profile in "Measured" above. The profile numbers came
from a separate microbenchmark tool with a different code path and
`smp`/parallelism setup than `perf-sstable`; the CPU-share breakdown is
directionally useful (it explains *why* coalescing helps) but the
percentage does not transfer directly to end-to-end throughput on this
harness — other costs on the `perf-sstable` critical path (partition/row
building, LSA, mutation assembly) that are outside `get()` dilute the
per-fragment share this change removes. Treat the 20–38 % figure as a
ceiling on the read-path CPU only, and 6 % as the honest, measured,
end-to-end number for this workload and harness.

All sstable boost suites (`sstable_test`, `sstable_3_x_test`,
`sstable_datafile_test`, `broken_sstable_test`, `sstable_compaction_test`,
495 cases total) pass, including with `--blocked-reactor-notify-ms 1`
(225 of them, the file-backed subset), with no stall reports.

## Self-review

Findings from a hostile read of the diff after implementation, and what
changed:

1. *Unnecessary copy on cold start.* The first draft of `read_chunk()`
   always allocated a fresh `temporary_buffer` and copied into it, even
   when a single `_input_stream->read()` returned enough bytes to satisfy
   the whole chunk in one shot — a case `read_exactly()` handles today by
   sharing without copying. Fixed: when nothing has been collected yet and
   a freshly fetched block already covers the requested length, share and
   trim instead of copying. This only affects the first chunk of each
   coalesced `get()` call (the one call per `get()` that may need to wait),
   so it does not change the no-stall argument, only removes a redundant
   memcpy.
2. *`_pos += len - addr.offset` sign/scope check.* `addr.offset` is only
   nonzero when `_pos == _beg_pos`, which is only possible on the very
   first iteration of the very first (or first-after-`skip()`) call,
   because after that iteration `_pos != _beg_pos` and the alignment check
   at the top of the loop throws if `locate()` ever returns a nonzero
   offset again. Confirmed correct by inspection and by the offset test
   case (`skip()` into the middle of a chunk, then read to EOF).
3. *`close()` and `_buf`.* No pending future exists in this design (that
   was the rejected alternative), so `close()` needs no change: `_buf`
   holds plain bytes, not a read in flight; no seastar-side assertion is
   at risk.
4. *`skip()` re-checked against `_buf`.* Confirmed `underlying_n` is
   computed in the compressed byte space and compared against `_buf.size()`
   in the same space (both are counts of compressed bytes), so trimming
   `_buf` by `underlying_n` when it fits is the correct amount, matching
   the bytes `_stream_creator`'s underlying stream would otherwise skip.
5. *Memory ordering claim re-verified.* `resource_units` has no partial
   release API (grepped `reader_permit.hh`), confirming the "request after
   the fact" design is the only option without a larger change; documented
   as accepted trade-off, not fixed.
6. *k/l format.* No separate review needed beyond compilation and the
   existing `sstable_test` k/l-format cases, since the change is entirely
   inside the shared template; both formats compiled and passed.
7. *Reversed reads / index skip.* `compressed_file_data_source_impl` has no
   reverse-read logic (that lives above it, in the mx/kl readers); no new
   code path is introduced for reversed iteration. Not applicable.

No further issues found. The exception-timing shift and the digest,
`need_preempt()`, and `addr.offset` handling documented in "Risk &
complexity" / "Review" above were re-checked against the final code and
match.
