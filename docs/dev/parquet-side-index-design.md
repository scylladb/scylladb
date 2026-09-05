# Row-group side index for `pq` sstables — design

Status: **measured, and decided against — 2026-08-20. Not implemented, and not planned.**
The measurement and the reasoning are in `parquet-storage-format.md` §10.27; the summary is below.
This file is kept because the design is sound and the decision is contingent on numbers that could
change, not because the work is queued.

## Decision

**The cost is real and the design would remove it. It is not worth an on-disk structure.**

At the shipping defaults (`rows_per_row_group` 5 000, ~400 row groups and 568 kB of footer per
sstable), a genuine *first* read of an sstable costs **3 680 µs** where a subsequent read costs
**1 149 µs**, and the **2 530 µs difference is the footer** — 69 % of the first read. Three
independent estimators agree on it to within 3 %: defeating the footer cache with the reclaim
threshold, the reader's own per-miss instrumentation, and the literal first reads after six restarts.
Split roughly 45 % fetch / 55 % Thrift walk, both scaling with row groups.

**What decides it is the frequency, not the share.** That 2.5 ms is paid once per sstable per
restart — the footer cache holds it thereafter, and a shard's footer working set fits the reclaim
budget at any sane row-group size — so a 200-sstable shard is exposed to about half a second of extra
latency, once, per restart. Steady state does not move at all: every cold ratio published for `pq` is
`min` over 400 probes and therefore a footer-cache *hit*, so the index would not change a single
number in the document. Against that, the index has to be written on **both** write paths (the
divergence behind two previous bugs), read at every sstable open, and kept working through compaction
and `scylla-sstable`, with a fallback path to test for as long as the format exists.

**Two hesitations recorded below were wrong when measured**, and both are corrected in place:
residency is ~8 kB per sstable rather than ~40 kB, and an encrypted file would keep roughly half the
benefit rather than losing most of it.

**What would reopen it:** an SLO that covers the minutes after a restart, or a `rows_per_row_group`
low enough that the footer working set stops fitting the reclaim budget (a thousand 1 601-group
sstables want ~2.4 GB against 1.6 GB on an 8 G shard). Both are recognisable in advance.

The rest of this file is the design as it stood, with the two measured corrections marked.

## The problem, restated from measurement

A point read needs one row group's metadata — about 1 420 bytes — and reads the entire footer:
0.14 MB at 100 row groups per file, 2.84 MB at 2 000 (§10.22). Cold, that cost the measured
`2.1 ms + 3.1 ms per MB` (§10.21), and it is the reason the pre-cache slope was 4.42 µs per row
group per file.

It cannot be avoided by reading less of the footer. `FileMetaData` is one Thrift compact struct
whose `row_groups` list is delta-encoded and variable-length, so entry *N* cannot be located
without walking 0…N−1, and Parquet publishes no offset index for the footer itself. The reader
already takes the only shortcut the format allows — `metadata_mode::lazy` records each group's byte
extent and decodes exactly one — so **decode is already O(1) and the read and walk are O(all
groups)**.

The footer cache (§10.24) removes this for the *second* read of a file. It does nothing for the
first, and after a restart every read is a first read. That is the gap this closes.

## What the index has to contain

Enough that a point read touches the footer **once, at a known offset, for one row group**:

| field | width | why |
|---|---:|---|
| `schema_extent` | 8 + 4 | byte range of the footer prefix holding `version` + `schema`; needed to build the mapped schema, and its length is not knowable a priori |
| per row group: `columns_offset` | 8 | byte offset, within the footer, of that group's column list — exactly what `parse_row_group_light()` already computes |
| per row group: `columns_length` | 4 | its length |
| per row group: `num_rows` | 8 | so ordinal → row group needs no footer read at all |

20 bytes per row group plus a small header. At 2 000 groups that is **40 kB**, against a 2.84 MB
footer — a 71× reduction in bytes touched, and fixed-width records make each record access O(1)
rather than a walk.

The record stores each group's **cumulative first-row offset**, not its own `num_rows`: a per-group
count is not monotonic, so it is not a search key. The cold path becomes: read the side index,
binary-search the first-row offsets for the ordinal — **O(log N) in row groups** — then one O(1)
fixed-width record access, one ~1 420-byte slab read plus the schema prefix, decode.

## Where it lives, and why not a new component

The obvious shape is a new `component_type`. **Rejected**: `component_type` is a fixed enum whose
`Unknown` sentinel sizes arrays, adding a member changes TOC contents, and an older node meeting an
unfamiliar component in a TOC is a downgrade question this project has already had to answer once
for sstable *versions* (§10.9, §10.20). Introducing a second such question to save a few kilobytes
is a poor trade.

**Use the `Scylla` component's existing attribute map instead.** It is already an extensible
key → blob store for Scylla-private per-sstable data, and encryption at rest already uses it exactly
this way — `ent/encryption` stores its serialised options and key id there (`encryption_attribute_ds`,
`key_id_attribute_ds`). So the precedent, the plumbing and the compatibility story all exist:

- no new component, no TOC change, no downgrade hazard;
- an older node ignores an attribute it does not know;
- an sstable written before this change simply lacks the attribute, and the reader falls back to
  the current whole-footer path. **The fallback is the compatibility story** — there is no migration.

## The one thing to measure before building — measured, and it is a non-issue

`Scylla.db` is read when an sstable is opened, not lazily per read. So the index becomes resident
for **every** open sstable, not just ones being read: 40 kB × sstables. A thousand 2 000-group
sstables is ~40 MB — cheap next to the footer cache's 1 522 B/row group (~3 MB for one such
sstable), but it is memory spent whether or not anyone reads the file, which is the opposite of the
footer cache's profile.

That suggests it should be **reclaimable on the same terms as the footer cache** — registered with
`sstables_manager`'s `_total_reclaimable_memory` against `components_memory_reclaim_threshold`, the
machinery bloom filters and the footer cache already share — with re-read from `Scylla.db` on miss.
Whether that is worth the complexity depends on the real number, so **measure resident size across a
realistic sstable count first**. If 40 MB is noise, pin it and keep the code simple.

> **Measured (§10.27): pin it, and none of the above is needed.** The 40 kB figure quotes the
> 2 000-row-group arm, which this project argues against on both size and latency grounds. At the
> *shipping* defaults an sstable holds ~400 row groups, so the index is **20 B × 400 ≈ 8 kB** and a
> thousand sstables cost **8 MB**. The reclaim question never arises, and the paragraph above was
> the wrong thing to have called "the one thing to measure before building" — the thing that
> actually decided it was how often a first read happens.

## Interaction with the footer cache

They compose rather than overlap, and the distinction is worth stating because §10.4l originally
conflated them:

- **side index** — makes the *first* read cheap, by not reading the whole footer;
- **footer cache** — makes *subsequent* reads free, by not re-parsing what was read.

With both, a cold read costs one small index lookup plus one 1 420-byte slab, and warm reads cost
nothing. The cache's entry shape may need to express "schema plus one materialised group" rather
than "whole footer parsed", since with the index the reader never holds the whole footer — worth
checking against `cached_footer` before implementing.

## Open questions

- Does anything else depend on the whole footer being resident? Compaction and `scylla-sstable`
  tooling read footers; both should keep working through the fallback path, but that needs checking
  rather than assuming.
- The index must be written on **both** write paths — `cut_row_group()` and the single-row-group
  `write_rows()` — which is the divergence that produced two separate bugs already (§8.2b, §10.15).
  Whatever writes it should be asserted on both in one test.
- Encrypted files: the extents are offsets into the *plaintext* footer, so for `PARE` files the
  index must describe post-decryption offsets, and the whole encrypted footer must still be fetched
  and decrypted before the slab can be cut out of it. **That removes most of the benefit for
  encrypted tables**, and is the one case where this design does not obviously pay. Needs thought
  before implementation, not after.

  > **Measured (§10.27): about half the benefit is kept, not lost.** The fetch that an encrypted file
  > cannot avoid is only ~45 % of the footer cost; the Thrift walk is ~55 %, and the index removes it
  > for a `PARE` file exactly as for a plaintext one. Decryption is not the obstacle either — AES-GCM
  > over a footer-sized buffer runs at ~5 GB/s, about 110 µs for 568 kB, against ~1 150 µs to fetch
  > it. So this was the wrong reason to hesitate; the decision rests on frequency instead.
