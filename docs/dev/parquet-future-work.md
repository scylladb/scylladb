# Parquet storage format — deferred work

Companion to `parquet-storage-format.md`, which is the design and the evidence. This file is
only the backlog: what is deliberately not done, why, and what a person picking it up needs to
know that is not obvious from the code.

Every item names the trap that would catch a reasonable first attempt, because in this project
that has been the expensive part rather than the implementation.

Last reviewed 2026-08-20. Corpus figures live in `parquet-storage-format.md` §10.1f-prod; the
decks are generated from `~/pq-lab/deck_data.py`.

---

## Read-path performance

Point-read p50 went **1 915 µs → 581 µs (3.3×)** across four changes (§10.4c, 10.4f, 10.4i, and
dictionary paging), for about 14 % of size. Point-read optimisation stays **paused** — items 2–4
below are recorded, not scheduled. The scan path was reopened on 2026-08-20 when storage-format
§10.26 found the format's headline advantage was never real; **both items it named are now done**
(0 and 0a below), and what is left of projection is not a pq change at all.

### 0. ~~A bounded partition range takes the point-read path~~ — fixed 2026-08-20
`pq_reader::next_window()` picked its unit of work from `bounded = _pr->start() || _pr->end()`, and
since the coordinator splits every range scan at tablet boundaries, every scan a client could issue
took the point-read path: 512-row windows, re-fetching each window's containing page for every leaf.
A whole-table aggregate over 8 M rows was **2.29×** the row format, 460 MB through a 23 MB file, in
~497 000 read extents.

Fixed, and **not** by testing the width of `[_row_lo, _row_hi)` as this item proposed. Width needs a
threshold and the threshold has no defensible value; the comparison the cost model actually turns on
does not. The ordinal window is contiguous, so a row group is either wanted whole — in which case one
sequential read is unbeatable and no page index is even needed — or wanted in part, which only the
two groups at the ends of a range can be, in which case the OffsetIndex the paged path must read
anyway says exactly what paging would fetch: page it unless the re-fetching costs the group's own
extent. Measured after: **1.03×** of the row format through CQL, 1 601 read extents, 21.3 MB
(storage-format §10.26, "Fixed, and re-measured"). Point reads improved too, because at the shipping
defaults `page_rows ≥ rows_per_row_group` means the "page" is the chunk, so paging was fetching the
whole group in 2 × leaves operations where streaming does it in one.

`perf_pq_vs_default`'s `bscan` column stays: it is now the regression test for the fork (bounded
within 0.98–1.01× of unbounded across nine arms), and it is the cheapest place to catch a
reintroduction.

### 0a. Column projection — the safe half is done, the rest is not a pq change
§10.26 asked for projection pushdown and measured that the prize on a shredded schema is read
*operations*, not bytes: the two dozen `__ttl_*`, `__ldt_*` and tombstone leaves are all-null, RLE to
~45 kB each, so they are 5 % of the bytes and 23/28 of the per-leaf work.

**That half is done, and without consulting the query at all.** A leaf whose chunk statistics say
`null_count == num_values` is null for every row of its row group, so the reader neither fetches nor
decodes it and tells the reassembler *absent* (`column_data::skipped`). Being a property of the file,
it applies to `SELECT *`, compaction and repair as well. On the paging arm of the §10.26 harness that
took a whole-table aggregate from 99 902 read extents to 33 038. Excluded, deliberately: repeated
leaves and leaves inside a collection group, because `num_values` counts *slots* and a repeated slot
that is "present but empty" counts as null there — which is not the same as absent, and
`read_collection()` distinguishes the two.

**Query projection itself is blocked, and not at this layer.** A CQL row is live if its marker is
live *or* any of its cells is, and the compacting reader decides that from the fragment it is handed.
Drop the cells of an unprojected column and a marker-less row — i.e. any row written by `UPDATE` —
loses its liveness: `SELECT b` over rows that only ever had `a` written must return rows with
`b = null` and would return nothing instead. "The marker is live in this sstable" does not save it
either, because a row tombstone in another sstable can shadow the marker while a cell of ours written
with a later `USING TIMESTAMP` survives. Cassandra solves this by distinguishing *fetched* from
*queried* columns — it reads every regular column from storage and projects above it — and mx
sidesteps it by never consulting the slice. So a working projection needs either that same split
above the sstable layer, or a way for `mutation_fragment_v2` to assert row liveness without carrying
the cell. **Whoever picks this up: the first attempt that "just honours `slice.regular_columns`"
passes every round-trip test in the tree and loses rows in production.**
`test_pq_restricted_slice_still_returns_every_cell` is there to fail it.

What it is now worth, since the numbers moved: a whole-table aggregate already reads 21.3 MB of a
23.4 MB file (read amplification 0.91×), so projection can no longer be a headline. It is worth
having on wide tables, where the unnamed columns are most of the bytes.

### 1. ~~Footer metadata cache~~ — built 2026-08-20
Built, measured and evicted on a running node: storage-format §10.4l for the design, §10.24 for the
numbers. Cold point reads improved 1.5× to 12.6× depending on row-group count, with the biggest gain
at the worst configuration, and the row-group latency slope inverted from +4.42 µs per row group per
file to −0.449.

Two of the three obstacles recorded here turned out to be avoidable rather than solved:

- the `shareable_components` cross-shard hazard is sidestepped by holding the entry on the
  shard-local `sstable` object, where the reclaim machinery already operates;
- the format-layer refactor ("the bulk of the work") was not needed: a reader materialises the one
  row group it reads into a *one-group* `file_metadata` and passes index 0, so `read_row_range()`
  needs no new parameter and the shared entry is never mutated.

**The other half is measured and closed, by declining it.** A cache helps the *second* read of a
file. The first read after a restart still fetches and walks the whole footer, and on a node that has
just restarted every read is a first read — the control run in §10.24 is exactly that cost. The
persisted side index of §10.23 ("row group → footer offset, length" beside `Data.db`, ~20 bytes per
row group) is what would make the first read O(1).

**§10.27 measured it at 2.5 ms per sstable — 69 % of a 3 680 µs first read — and decided against
building it**, because that cost is paid once per sstable per restart (about half a second for a
200-sstable shard) and nothing in steady state moves: every published *steady-state* ratio is `min`
over 400 probes and so a footer-cache hit. (Relabelled 2026-08-21, item 15: §10.24's `cached` column,
§10.25 and §10.26 are those hits; §10.21 predates the cache and is the uncached cost, which makes it a
fourth estimate of the 3 680 µs rather than an exception to the argument.) The design in
`parquet-side-index-design.md` is kept, with the conditions that would reopen it, rather than queued.

**Also not done:** the recovered `mapped_schema` is still rebuilt per reader. It is O(leaves), not
O(row groups), so it does not scale with sstable size; caching it would mean keying by query schema,
which is a different and worse problem.

**Known trade, accepted:** a compaction reading N sstables publishes N entries it will never read
twice, converting footer bytes into resident memory for the duration. The reclaimer bounds it — the
entries are the largest reclaimable objects on the shard and the first thing dropped — but "populate
only for bounded reads" is the obvious refinement if it ever shows up as pressure.

### 2. `decode_cpu` — 279 µs, largest remaining phase at test scale
Needs sub-splitting into page-header parse versus value decode before anything is attempted, on
the §10.4h precedent: phase boundaries chosen for convenience hid the real split once already.

### 3. Re-measure the whole of §10.4 at production sstable size
**This is the most important item in this section.** Every latency figure was measured on a
100 000-row, 1.27 MB file — two to three orders of magnitude smaller than a bottom-tier sstable.
§10.4j showed that footer parse alone reorders the whole ranking at realistic sizes. The fixes are
real; the *priorities* derived from them are not trustworthy at scale.

### 4. I/O queue depth from batched reads — watch, do not pre-empt
§10.4i replaced 35 serial reads with one concurrent batch. That multiplies queue depth by leaf
count under concurrent point reads. Nothing here measures a loaded node. If it surfaces as
queueing, cap the batch; do not return to serial.

---

## Size accounting: `pq` sstables report a different unit — **closed 2026-08-19**

`sstable::data_size()` returns the *uncompressed* data length when a `CompressionInfo` component
exists, and the file size otherwise. A `pq` sstable has no `CompressionInfo` — Parquet compresses
internally — so for it `data_size() == ondisk_data_size()`, while a native compressed sstable
reports several times more than it occupies. `get_compression_ratio()` likewise returned
`NO_COMPRESSION_RATIO` for `pq`, and now falls back to the statistics value.

**C1's bottom-tier rule** compared `data_size()` across candidates, which in a hybrid table —
where both formats are present by definition — compared mixed units. The same data reports several
times smaller once converted, so a `pq` sstable would never be judged "the largest" and C1 would
read false for a bucket that is genuinely the bottom tier. Uses `ondisk_data_size()`. The gain
estimator and `make_tiering_inputs()` already did.

**Size-tiered bucketing**, the part that was open: `size_tiered_compaction_strategy` and
`incremental_compaction_strategy` bucket on `data_size()`, so a converted sstable's reported size
drops by the compression factor and it buckets several tiers below its true peer — scheduling
repeated rewrites of the format that is most expensive to rewrite. This is in the path for both
in-scope strategies, ICS directly and TWCS through its per-window size-tiered fallback.

Resolved by making the unit a property of the *candidate set* rather than of the format:

- An all-native set keeps using `data_size()`. Every entry there uses the same convention, so the
  ratios bucketing is built on are self-consistent and the inconsistency is invisible. **No
  existing cluster's bucketing changes** — which is what made option 2 below unacceptable as a
  blanket change.
- A mixed set uses `ondisk_data_size()`, the one measure that means the same thing for both.

`mixed_formats()` + `bucketing_size()` in each of the two strategy files;
`sstables::sstable_run::ondisk_data_size()` added for the run-level case. The rejected
alternatives, for the record: (1) synthesise an uncompressed length for `pq` so `data_size()`
means one thing — computable, but it fabricates a `CompressionInfo`-like value for a file with no
Scylla compression; (2) move all bucketing to `ondisk_data_size()` — correct in principle, a
behaviour change for every existing cluster, and not a Parquet decision to make; (3) document that
hybrid tables should avoid size-bucketing strategies — which rules out both supported strategies.

Untested at scale: the runs in §10.6 convert everything in one major compaction, so no measurement
yet exercises a genuinely mixed candidate set through bucketing. That is the natural next scale
test. (An earlier version of this note claimed a TWCS active window is permanently mixed, because
flushes were native. Since §10.7 they are not: a TWCS table is Parquet end to end. The mixed case is
ICS under `'hybrid'`, and any table mid-`ALTER` — real, but transient rather than steady-state.)

---

## Verified, not open — recorded so nobody re-investigates

The operational surface works on `pq` sstables and was checked against a live node, not inferred:
`dump-data` (full row count), `dump-statistics`, `dump-index`, `dump-summary`, `validate`
(0 errors), `validate-checksums` (digest and CRC verified), `nodetool upgradesstables` (converts
`me` → `pq`), and `nodetool scrub` in validate mode (passes, rows still readable). The design
doc's claim that `upgradesstables` does not force convergence was stale — it does, because the
sstable creator is shared by the rewrite paths.

**Snapshot and restore round-trip works, and finding out exposed a real bug.** Snapshot captures
`pq` sstables; truncate-and-refresh returns every row. But `refresh` in load-and-stream mode
re-streams the partitions rather than adopting the files, and the streaming creators in
`replica/table.cc` did not consult `storage_format` — so repair, bootstrap, decommission and
refresh all wrote **native** sstables into a table declared `'parquet'`. Fixed and re-verified by
the same round trip. `'hybrid'` still streams native by design, since streamed data is not
bottom-tier.

---

## Format gaps

### 5. ~~`DELTA_BYTE_ARRAY` is unimplemented~~ — closed 2026-08-19
Implemented with `DELTA_LENGTH_BYTE_ARRAY`, on both the write and read paths, and hinted for text and
blob **clustering** keys only — where sortedness is structural, not a guess about the type (§10.3f).
35 % of PLAIN on sorted keys, 70 % when nothing is shared, so the downside is bounded. Interop 18/18
with `DELTA_BYTE_ARRAY` visibly present in the two new fixtures and read by both pyarrow and DuckDB.

Two things worth keeping from doing it. It **found a latent bug in the older encoding**: the
DELTA_BINARY_PACKED decoder's `total == 0` path never consumed the header's first-value field, which
was invisible until two delta blocks were concatenated. And the interop suite **could not have caught
any of it** — every one of its eight shapes used `ck int`, so no fixture had a text clustering key.
The suite now reports the encodings each file uses, because a fixture that passes without exercising
the new path looks like coverage and is not.

### 5a. Superseded: the original note

The encoding that would help exactly where Parquet does worst: HackerNews at 82.5 % and Wikipedia
pageviews at 90.0 % are both dominated by near-unique text (§10.1f-prod). Prefix-sharing between
adjacent sorted strings is the one mechanism that attacks that, and it is unavailable.

### 6. ~~Counter shard leaves are not typed~~ — closed 2026-08-19
`value` and `clock` are now two `INT64` leaves of the counter's group, so an external reader sees a
count and a logical clock rather than a packed blob. The nested-group work this was deferred for was
never needed: the `key_value` group already carries five children rather than a strict MAP's two, so
a sixth typed sibling costs nothing structurally. The metadata declaration added earlier stays as
documentation of the shard-id packing, which is still 16 bytes.

### 7. ~~Statistics metadata is thinly fed~~ — closed 2026-08-19
Investigated and largely a false alarm. `update_live_row_marker_timestamp` *is* fed (an earlier
grep here only matched the `_collector.` prefix, and the writer calls it through `_c_stats`), so
`pq` never hits the `on_internal_error_noexcept` path in `get_max_purgeable_timestamp`. The one
real gap was `add_compression_ratio`, now fixed: the writer records it from the Parquet footer and
`sstable::get_compression_ratio()` falls back to the statistics value when there is no
`CompressionInfo` component. Measured 0.0732 through the REST API, corroborated to within 0.2
points by the independent raw measurement in §10.1f-raw.

### 7a. ~~The deletion channel was never folded~~ — closed 2026-08-21

Not previously an item here, which is part of why it survived so long: the cost showed up as a
Backblaze *measurement* anomaly and was filed as measurement methodology (§10.18), so nothing pointed
at the format.

L1 folded every cell's write time into one `__ts` per row from the beginning. A dead cell's deletion
time never got the same treatment — it went into its own column's `__ldt_<col>` leaf, 195 leaves and
60.7 MB on Backblaze's 197 columns against 1.9 MB for the same rows' write times. That ~32× gap is
the whole reason `pq` paid ~63.7 MB for retained tombstones where the row format paid ~10.7 MB.

Now four leaves independent of table width: `__ldt`, `__dmask`, `__ldtx_mask`, `__ldtx_vals`.
Measured on real `pq` sstables, tombstones retained, identical pipeline both sides: **87 532 117 ->
28 246 463 B**. §10.28 has the design, the losslessness argument, the compatibility story and what is
deliberately excluded (collections, for §10.26's reason).

**Still open, and small:** `__ldt` is emitted PLAIN. Deletion times are a narrow, heavily repeating
range of int32, which is what dictionary and `DELTA_BINARY_PACKED` are for, and the leaf is now
406 102 B of a 28 MB file. Worth a look, but note the warning attached to `__ts` in
`build_mapped_schema()`: asking for delta on a folded metadata leaf produced a real correctness
failure there and was reverted for ~4 KB. Measure before changing.

---

## Measurement-method debt

Not defects — the numbers below are all real. They answer a different question from the one their
label implies, which is easier to overlook than a wrong number and worse for a reader who trusts the
label. Each item is a relabelling-and-scoping job, not a re-engineering one.

### 15. ~~Every "cold" figure in §10.21, §10.24, §10.25 and §10.26 is a warm reading~~ — DONE 2026-08-21

**Relabelled, and one of the four was not what this item said it was.** §10.24's `cached` column,
§10.25 and §10.26 were `min`-of-400-after-a-restart with the footer cache in the binary, so they are
cache hits; each now carries a table or paragraph saying which question it answers, and the word
"cold" is gone from all three in favour of `steady`. **§10.21 is the exception**: it was measured
*before the footer cache existed*, so all 400 of its probes paid the footer and its column is the
uncached cost — it is renamed `uncached min`, not relabelled as a hit. It then turns out to be a
fourth independent estimate of the very quantity §10.27 measured: same sstable shape, **3 958 µs
against 3 680 µs, 7.0 % apart**. §10.27's heading ("every cold figure … is a footer-cache hit") is
over-general and now says so inline. Both numbers are stated at every site, because a latency budget
needs first contact *and* steady state: **3 680 µs and 1 149 µs at the shipping default, 3.2× apart.**

The original item, for the record:

Those figures are `min` over 400 probes after a single restart. The first probe pays whatever
per-sstable one-time cost exists, so the *minimum* can only be a footer-cache hit — by construction it
is the one reading that cannot be cold.

The two numbers, both real, both measured:

| | at shipping defaults |
|---|---:|
| labelled "cold" (min of 400 after a restart) | 1 149 µs |
| genuine first read of an sstable | **3 680 µs** |

They answer different questions — steady-state-after-warmup versus first contact — and both are worth
having. §10.27 established this and drew the right conclusion from it (it is why the side index was
decided against: 2.5 ms paid once per sstable per restart does not move steady state). What it did not
do is go back and relabel the four sections whose tables still say "cold".

**The job:** relabel them, and state at each which question the figure answers. A GA reader sizing a
latency budget will read "cold" as cold. Where a section's argument depends on the distinction, say
which number the argument uses.

### 16. ~~`harness.py`'s "Parquet" figures are pyarrow re-encodings, not `pq` sstables~~ — DONE 2026-08-21

**Audited in full, and the significant case re-measured.** The inventory is §10.3j; each of the eight
tables now carries its verdict inline — one of which the candidate list had missed. Headlines:

* **§10.3's folding table is re-measured through the real write path** (`~/pq-lab/fold_levels.sh`,
  the shape of §10.1f-prod). **L0/L1 on Backblaze is 6.63×, not 26.8×** — the model's L0 column was
  4× too big because it writes a dense per-column `__ts_<col>` for every row, including the 73 % of
  Backblaze cells that do not exist; its L0 figure is 97.7 % of `195 × 300 000 × 8 B`, i.e. the size
  of an array the format never writes. The stated mechanism is also corrected: L0's cost is
  per-column duplication × *distinct write times* × cell density, and on this corpus partition-key
  cardinality dominates column count. The decision the table supports is unchanged and not close.
* **L2 is unreachable in the `realistic` regime** — all three `uniform` requests came back
  `folding_effective: L1`, byte-identical to `row`. The old L2 column described a file the format
  cannot produce. Real L2 saving, where it applies: 1.4 % (§10.1m).
* **The model's L1 column is good to ~2 % on narrow/dense schemas and 18 % optimistic on wide sparse
  ones** — and it is right for the wrong reason even where it is right: 106 modelled leaves against
  330 real ones on ClickBench, the difference being all-null channels worth 0.8 % of the file. A model
  that agrees on bytes is not thereby validated on structure.
* **One false positive:** §10.1f's export corpus is our own writer, not pyarrow. Superseded for
  absolute sizes, but for an unrelated reason.
* **One conclusion that rested on a mechanism we do not have:** §10.1g's `isdfloat` result. Our
  writer's file grows **+19.6 %** on `int32` → `double`, not +0.1 %, because `numeric_dictionary` is
  off by default. The conclusion survives (Parquet's saving 50.8 % → 52.8 %) but the effect is 2
  points rather than 12.4.
* **Un-measurable, permanently:** §10.1a's token-order penalty. Both arms must be models, because an
  sstable is token-ordered and has no other order to be written in. The ratio is the trustworthy part.

The original item, for the record:

`harness.py`'s "Parquet" figures are pyarrow re-encodings, not `pq` sstables

`harness.py` reads rows back over CQL and re-encodes them with `pyarrow.parquet.write_table`,
synthesising the metadata leaves itself. Every probe line in a harness run shows `pq=0` — it never
writes a `pq` sstable. It is a *model* of the mapping, and a useful one, but it is not a measurement
of Scylla's Parquet writer, and two of its limits matter:

* It cannot see cell metadata a `SELECT` does not return. It emits `__ldt_<col>` as an all-null
  column, so the 60.7 MB the per-column deletion channel actually cost never appeared in any harness
  figure at all (§10.28) — the defect was invisible to the instrument most often pointed at it.
* It models the leaf *set*, not the writer's encoding choices, page layout, statistics or footer.

**Partly established, 2026-08-21.** Two boundaries are now known and neither should be re-litigated:

* **§10.1f-prod — the eight-dataset corpus table, "the table to quote" — is NOT affected.** It comes
  from `measure_native_vs_pq.sh`, which performs the real `ALTER TABLE ... WITH storage_format =
  'parquet'` and sums actual `pq` sstables. Verified by reading the script.
* **`bb_ldt_fold.py` and `bb_investigate.py` are the real-write-path tools.** Prefer them for any
  format claim.

**What is not established, and is the job.** Which *other* published tables are harness output cited
as format measurements. The candidates, by line in `parquet-storage-format.md`, are the ones whose
column headers say "Parquet L1" or "Parquet L1/zstd-3": the codec matrix (~2041), the token-order
comparison (~2065), the superseded §10.1f export table (~2615, already marked superseded), the
variant tables (~2740, ~2751), the row-count sweep (~2972), and the **folding-level comparison
(~2989)**.

**Flagging the last one specifically, because it is the significant case.** The L0/L1/L2 table — the
source of §10.3's headline folding claim — is pyarrow model output. The model does represent the leaf
*counts* the mapping produces, which is the mechanism the claim is about, so the claim is probably
sound; but a structural claim about our writer currently rests on a re-encoding that is not our
writer, and §10.28 is a concrete instance of that model being blind to a real 60 MB cost. This
deserves a re-measurement through the real write path rather than a relabelling.

---

## Hybrid tiering — trimmed to three criteria, further items open

The decision function is **C1 (bottom tier), C5 (width in columns), C6 (measured gain)**. C2, C3,
C4 and C7 were removed. The pattern: three of the four were thresholds nobody had measured, and
the fourth could not be evaluated at all. **Any proposal to add a criterion should come with a
measurement, not a rationale.**

### Convergence is a write-path property, not a compaction property — closed 2026-08-19
Recorded because the reasoning that got it wrong was plausible.

A TWCS table left entirely to automatic compaction settled at **21 native sstables against 5
Parquet** and stayed there (§10.7). A closed TWCS window holding one sstable is terminal — the
per-window major needs two and the size-tiered fallback needs `min_compaction_threshold` — so it
keeps the format it was flushed in, permanently. Every earlier conversion test used an
operator-triggered rewrite and so could not see this.

Fixed by making `table::make_sstable(state)` consult `writes_parquet_unconditionally()`, so flushes
write Parquet on a table that writes Parquet unconditionally. Parquet flushes are also ~10x smaller
than the native flush of the same rows (0.8 MB against 7.8 MB on the test corpus), so this removes
write volume as well as a rewrite.

**The lesson worth keeping:** "which format does compaction choose" and "what format is the table
in" are different questions, and only the second one matters to an operator. Any future format
decision needs a test that calls no compaction endpoint at all.

### Maintenance tooling and object storage — both closed 2026-08-19
§10.12. The last two Phase 5 surfaces, and one of them had been dismissed for the wrong reason.

**Tooling: 10/10 on a `pq` table.** scrub in all four modes, cleanup, and snapshot → truncate →
refresh with all 200 000 rows restored. `scrub VALIDATE` is the load-bearing one: it reads every
sstable and reports errors without rewriting, which makes it a whole-file integrity check of the
Parquet reader.

**Object storage: 7/7 on minio, and it was never actually blocked.** It was recorded as needing Docker
because the repo's GCS tests cannot pull their image here. But `minio` is installed as a plain binary
(`/usr/local/bin/minio`) and `podman` works even though `docker` does not, so no registry was ever
required. Configuration follows the repo's own `test/pylib/minio_server.py`. The decisive check was
made against the bucket rather than through Scylla: the `Data.db` object carries **`PAR1` at head and
tail**.

**Newly open, and blocking for anyone combining the two features:** on object storage the key is
`sstables/<uuid>/Data.db` — **no version prefix**. The downgrade safety verified in §10.9 is a
*filename* property: an older node aborts because it cannot parse an unknown version in a filename.
With no filename, that abort may not happen, and the failure mode would be an older node
mis-reading an object rather than refusing to start. **§10.9 is verified for local storage only.**

**~~Encryption at rest is not a gap in this tree~~ — that answer was wrong twice over.** First it
conflated Scylla's own encryption at rest with Parquet's. Then the correction repeated a factual
error: Scylla's EaR **is** in this tree, in **`ent/encryption/`** (key providers for local files,
replicated keys, KMIP, AWS KMS, Azure, GCP). The original claim of absence came from grepping for
`ee/` and not checking the directory listing. Those providers are what a **BYOK** deployment uses, and
`key_provider::key()` already returns a key plus an opaque id to store with the data — the same role
Parquet's `key_metadata` plays, so the models line up. But **Parquet Modular Encryption** is a
standard part of parquet-format 2.7+, and it is the right layer for this format: encrypting the Data
component from outside would make the file opaque to every external reader and forfeit the
interoperability that is the whole argument for Parquet. Built and verified end to end on 2026-08-20
— see storage-format §10.17. AES_GCM_V1 and AES_GCM_CTR_V1, encrypted footer, **keys from
`ent/encryption`'s providers so BYOK works**, DDL validation, and pyarrow reading the encrypted
`Data.db` with the provider's key.

Still open within it:

- ~~**Per-column keys are written but not interoperable.**~~ **Interoperable as of 2026-08-20.**
  The cause was `RowGroup.ordinal`, which our writer never emitted. parquet-cpp takes the AAD's
  row-group ordinal from that Thrift field and substitutes **-1** when it is absent, so its AAD
  for the encrypted `ColumnMetaData` carried `0xFFFF` where ours carried `0x0000` — a tag
  mismatch reported as "Failed decryption finalization". Uniform mode never noticed because it is
  the only module parquet-cpp keys off that field; page and footer modules get their ordinals from
  position. The writer now emits it, and `test_encrypt_interop.py` asserts the both-keys case.
  What remains open is the **CQL surface**: per-column keys are still not exposed as a table
  property, because that needs a syntax for "which columns, which keys" and a decision about how
  the ids reach a reader — a separate design question from interop.
- ~~**Alignment with `ent/encryption`.**~~ **Done, 2026-08-20.** The bespoke
  `parquet_encryption_key_file` and its `key_registry` are deleted; the key now comes from
  `encryption_context::get_provider()` with `scylla_encryption_options`' own option vocabulary
  (`key_provider`, `cipher_algorithm`, `secret_key_strength`, `kmip_host`, `secret_key_file`, ...)
  carried in the `parquet` map, so every provider — and therefore BYOK — works. The *selector* stays
  in `parquet = {...}` and a table asking for both it and `scylla_encryption_options` is refused,
  because that property encrypts the whole Data component and would double-encrypt while handing out
  a file no external reader can open. `cipher_algorithm` is defaulted to `AES/GCM/NoPadding` when
  absent and refused when it explicitly names another mode.
- `key_metadata` defaults to the provider's key id verbatim, with parquet-java's key-material JSON as
  an explicit `encryption_key_metadata: 'parquet_kms'` opt-in. Emitting that JSON unconditionally, as
  the first version did, assumed one key-management model for everyone.
- **Key rotation is supported by construction but unverified.** The provider id round-trips through
  `key_metadata`, which is the mechanism; but no rotation has been exercised, and the one provider
  this lab can drive issues no ids, so it cannot demonstrate one.
- **Provider options are read from the schema, not from the file.** Reconfiguring a table's key
  provider makes its existing encrypted sstables unreadable, where the whole-component path — which
  stores the options per sstable — would still open them.
- **Node-global `user_info_encryption` is not covered.** It applies whole-component encryption without
  appearing in a table's schema extensions, so the mutual-exclusion check cannot see it, and such a
  node would encrypt a format-encrypted `pq` table twice. Closing it needs `encryption_context` to
  expose the global extension, which is implementation-private today.
- Plaintext-footer mode.

### Downgrade procedure — written 2026-08-19, was "not started"
§10.9. The load-bearing fact is observed, not reasoned: an older node meeting an sstable version it
does not recognise **aborts during the directory scan** rather than skipping the file. Verified by
planting a file with an unknown version prefix in a live table directory and restarting the node,
which exited with `malformed sstable error (aborting): invalid version`.

Downgrade is therefore safe but strictly manual: `ALTER` back to `'sstable'`, `nodetool
upgradesstables -a` on **every** node, verify per node through
`GET /column_family/storage_format/{ks:table}` until each reports `converged: native`, then downgrade.
This procedure is verified for local storage only — on object storage the rewritten objects have no
version to pin, so downgrade safety there is unverified (see the object-storage note above).
Two things a plan must not miss: TWCS tables on `'hybrid'` are Parquet even though the property does
not say `'parquet'`, and **snapshots taken while the table was Parquet still contain `pq` files**, so
the backup retention window is part of the downgrade window.

Still open in this area: nothing blocks the procedure, but it is undertested — there is no automated
test that drives a full cluster-wide un-conversion and verifies every node. The per-table direction is
covered by `cql_ddl_test/test_storage_format_converts_on_compaction`.

### Two test-harness faults from converting the loaders — fixed 2026-08-19
Both were found by re-running the converted tests rather than trusting the conversion, and both
produced confident wrong verdicts rather than errors.

- **A fixture outlived its bookkeeping.** `cluster_parquet_check.py` had a warm-up row (one CL=ALL
  insert, to let the cluster settle before the Python loader fired 20 000 concurrent writes). latte
  made it unnecessary — it has its own retries and the load takes seconds — so the row went, and
  `EXPECTED = ROWS + 1` stayed. Four checks then reported `300 000 vs 300 001` against a table that
  was perfectly correct. **Delete a fixture and its arithmetic in the same edit.**
- **The multi-node check only worked once.** Its last step bootstraps a fourth node, and that node
  *stays* joined. A second run therefore saw four nodes, failed "cluster formed" (which expected two
  peers), and passed the bootstrap check trivially against sstables n4 already had — a PASS that
  meant nothing. It now decommissions n4 and wipes its data at the start, which both makes the run
  repeatable and exercises decommission (legal from four nodes to three, unlike three to two — §10.8).

**A test that only works the first time is worse than no test**, because the second run's green is
indistinguishable from a real pass.

### Multi-node: first coverage, 2026-08-19
`~/pq-lab/cluster_setup.sh` brings up three nodes (distinct loopback addresses, own data dirs, own
API ports, `GossipingPropertyFileSnitch` in one rack) and `~/pq-lab/cluster_parquet_check.py` runs
the replication and streaming checks that single-node work cannot reach. **9/9 pass** — see §10.8 for
the numbers and for the three test-design errors that each produced a confident wrong answer
(decommission is impossible at RF=3 on three nodes; a single tablet makes the bootstrap test
vacuous; joining and receiving data are minutes apart, not seconds).

Two environment notes for anyone re-running it: `GossipingPropertyFileSnitch` opens
`conf/cassandra-rackdc.properties` relative to the **process working directory**, not the options
file, so the unit needs `WorkingDirectory` set or the node exits at startup; and `repair_async`
returns 403 on this cluster's keyspaces, where `/storage_service/tablets/repair` is the endpoint that
works.

### Strategy scope: ICS and TWCS only — settled 2026-08-19
Not a to-do; recorded so it does not get re-litigated. **STCS is out of scope for this project**
and no measurement, test or benchmark may use it. The supported set is:

- **TWCS** for any schema with a time-based clustering key — the recommended default for time
  series, and the shape Parquet wins hardest on (NOAA ISD-Lite hourly observations, 46.8 % of the
  SSTable at 300 k rows and 43.2 % at 16 M). **A TWCS table is entirely Parquet**: `'hybrid'` and
  `'parquet'` are the same setting there, and the criteria are not evaluated at all.
- **ICS** for everything else. This is the only strategy hybrid tiering applies to.

The criteria exist to keep Parquet out of levels that get rewritten, and TWCS has none — a window is
compacted once and then closed. `writes_parquet_unconditionally()` is the single home for the rule,
consulted by compaction, flush and streaming alike so they cannot disagree.

**What this gives up:** C6 is what catches a schema Parquet stores worse than the row format, and
under TWCS it no longer runs. The 197-column sparse-telemetry dataset measures at 208 % of its
SSTable; on TWCS + `'hybrid'` it will now convert and roughly double its disk. `storage_format =
'sstable'` is the only protection. Deliberate: TWCS tables are overwhelmingly time series, the shape
that wins most, and the decision is now free rather than costing a data sample per compaction.

An earlier design evaluated C1 under TWCS via `newest_bucket()`'s "is this window still the last
active one" — exact, where ICS's size rule is a proxy. It was implemented, tested, and then removed
when the rule above made it unread. Recorded so it is not re-added as a missing feature.

STCS still compiles with the size-tier rule, because ICS shares that code. That is not support:
writing Parquet under any strategy other than ICS or TWCS logs a rate-limited warning from
`compaction_manager` naming the strategy. It does not refuse, because the output is valid — the
point is that an operator should not have to read a design document to find out they are outside
what was tested.

### 8. C2's replacement should gate on rows, not bytes
C2 was removed as subsumed by C6, but if a size floor is ever wanted again it must be a row count:
four row groups is 126 kB at 6.3 B/row and 1.4 MB at 69 B/row, so no byte threshold suits both.
`rows >= 4 × rows_per_row_group`, fed from sstable stats.

### 9. `row_group_buffer_bytes` per shape
The effective row-group size is `min(rows_per_row_group, what 64 MiB of shredder memory allows)`, and
which term binds depends on row *density*, not column count (§10.1f-rg). On dense wide tables the
byte budget binds and the row count is inert. Harder than it looks: this budget exists to stop a
shard OOMing (R-13), so it cannot simply be raised for size.

To be clear about what this item is and is not: `row_group_buffer_bytes` is **not implemented** —
today `rows_per_row_group` alone bounds a group, and this item is the proposal to add the guard.
It would be a **guard rail**, not a
tuning dial -- `rows_per_row_group` is the dial (design doc §8.2). The question here is whether the
guard should be *shape-aware*, because a single 64 MiB figure means very different row counts across
the corpus and on a dense wide table it silently becomes the thing that cuts. That is a correctness-
of-the-guard question. It is not a proposal to trade the budget against file size or latency.

### 10. ~~C7, the read-pattern gate~~ — dropped 2026-08-19, not deferred

The criterion: refuse converting a table to Parquet when it is mostly point-read, however well it
would compress. Removed from the policy 2026-08-18 because nothing could answer "is this table
point-read dominated"; **now removed as a requirement too**, and the order of events is the point.

The input problem was solved first. Per-table `single_partition_reads` and `range_scan_reads` counters
are landed and verified across five query shapes (§10.14), so "there is no data source" stopped being
true. Also worth recording: the old claim that *no* counter separated point reads from scans was too
strong — `scylla_sstables_single_partition_reads`, `scylla_sstables_range_partition_reads` and
`scylla_cql_select_partition_range_scan` all already did, per shard. The real gap was per-table
attribution, and it is closed.

And the criterion was dropped anyway, because having the input is not evidence that it decides better
than what is already there. **C5's column ceiling is the answer rather than a stand-in**: past 128
columns a table is too slow to point-read as Parquet whatever it saves, derived from the ~90 µs per
leaf measurement. Its known error is a false *negative* — declining a wide table that is only ever
scanned, the case Parquet is fastest at. C7 would fix that and add a false positive of its own, since
read mix is measured over a window and a table's mix changes, so it would convert on last week's
traffic. A conservative error that leaves data in the row format beats an optimistic one that rewrites
a table into the wrong format for its present workload.

**What an operator does instead:** `storage_format = 'parquet'` is taken at face value and bypasses the
criteria, so a wide scan-only table is one `ALTER` away from conversion by someone who knows what the
policy cannot. The new counters exist to inform that judgement. A person saying "this table is only
scanned" is better evidence than a window of counter samples.

The counters stay regardless of C7: per-table read shape is useful telemetry on its own, and it is what
makes the manual decision above an informed one.

---

## Accepted limitations — not to-do items

- **Large partitions overshoot the row-group budget.** Cuts happen only at partition boundaries,
  so one oversized partition stays whole. Splitting would put `(row group, ordinal)` in every
  index entry and make point reads span row groups: complexity on every read to bound a rare case.
  The partition stays in one file, by decision.
- **Wide tables are refused conversion even when only scanned.** C5 bounds columns, which is
  cruder than C7's read-pattern gate; a scan is where Parquet is *fastest* (0.82× native). This
  false negative is the price of not instrumenting the read path.

---

## Housekeeping

### The dictionary baseline was never controlled — cause found 2026-08-19, re-measurement open
Full account in §10.11. Short version: the `sstable_dict_autotrainer` ticks in the background and
publishes a new dictionary whenever its validation sample says it is better. Production default is
900 s; the lab node was set to **10 s**. An 858 MB table's dictionary phase runs 2–4 minutes, so
12–24 ticks fire *inside one measurement* and whichever dictionary is live at the final major
compaction sets the bytes. Two runs of the same 16.2 M rows measured 281 861 328 and 316 320 823 —
**12.2 % apart** — while uncompressed size differed 0.20 %.

The 316 M value reproduces to 0.1 % across three independent re-measurements; the 281 M value has not
reproduced once. **The published figure is the outlier.**

The near-miss worth remembering: sample variance in training was measured first and came out at
**0.65 %** over five retrains, which looked like it exonerated the dictionary. It did not — that test
ran on a 67 MB table where each round took ~14 s, so hardly any autotrainer ticks intervened. A
stability test has to run at the size of the thing it is meant to characterise.

**Fixed for measurement:** autotrainer tick and retrain period are now 86 400 s in
`~/pq-lab/node/conf/scylla.yaml` (old values in `scylla.yaml.bak`), so only explicit `retrain_dict`
calls move the dictionary.

**Resolved for ISD, and the ratio survived.** Re-measured with the autotrainer off: dict 272 528 828
(−3.3 % vs published), parquet 116 495 610 (−4.2 %), both stable across repeats — Parquet byte-identical
across two rewrites. `parquet / dict` = **0.427** against the 0.432 on record, a 1.2 % move that is
inside the noise. Both sides of the ratio were inflated by similar factors, so the headline claim
holds even though both absolute figures moved several percent.

**The absolute figures are what moved, and both of them.** Parquet's size drifts between runs too
(121.7 M → 126.9 M → 116.5 M), tracking the internal layout of whatever inputs the compaction had.
Quote absolute bytes only from a deterministic-config run.

**Still open:** re-measure the §10.1 corpus the same way. The ISD result suggests the *ratios* there
are probably sound, but the two rows near parity — Backblaze 95.8 %, Wikipedia pageviews 89.6 % — are
close enough that a few percent on either side decides whether Parquet wins at all, and they have
never been measured under a controlled dictionary.

**And a process note that cost a wrong published claim for ten minutes:** the first recomputation
paired the new deterministic dictionary figure with the *old* Parquet figure and reported a 3.4-point
regression (0.466) that did not exist. Both sides of a ratio must come from the same configuration.

### Load generator: latte, and the three ways a loader swap lies — 2026-08-19
§10.10. The scale tests were client-bound: the Python driver did 11 376 rows/s while the node used
54 % of one core. latte (Rust, Rune workloads) does 58 000–75 000 rows/s at `-t 4` and moves the
bottleneck to Scylla; the 6 M-row run went from 10.6 min to 3.7 min, and a 40 M-row production-scale
run becomes ~18 min instead of ~75.

**The rule this established: a loader swap is not done until it reproduces the figures it replaces.**
It reproduces them now — uncompressed byte-identical, Parquet within 593 bytes of 29 MB — and it took
three corrections to get there, none of which announced itself:

1. payload length (short note vs a 130-character sentence): 3.7x size error;
2. modulo instead of a PRNG for a numeric column: over-compressible;
3. **the mutation timestamp** (monotonic counter vs derived from data): 40 % error in the Parquet
   figure and only 2.1 % in the uncompressed one, because L1 folding stores a per-row `__ts` and
   fixed-width timestamps hide the difference until something compresses them.

Each was caught by comparing against an already-published number. A new workload that has no figure
to reproduce should be treated as unvalidated.

**Do not use latte where rows must contain NULLs.** Rune `None` binds `CqlValue::Empty`, a live
zero-length cell, not a deletion — verified through `writetime()`. Nor can a workload hold a corpus:
`fs::read_lines` is rejected in const context and `Context` has no settable field. The NOAA ISD load
is therefore a six-way sharded Python loader, and `harness.py` stays as it is.

### 11. Decks — one combined deck at v2.8 (2026-08-19)

**The two decks are now one.** `make_combined_deck.py` (HTML) and `make_combined_pptx.py` (PPTX)
emit a single artifact: status slides, a GA-readiness section, then the dataset deep-dive as an
appendix. Build with `~/pq-lab/build_decks.sh`. There were four artifacts for something that was
always sent as one thing, with two title slides, two version stamps and two independent page-number
sequences.

The four original generators are kept and still build standalone, because the combined builders
import them for their slide content — so a slide added to `make_pptx.py` appears in the combined
deck on the next build, and nothing had to be copied.

Two mechanics worth knowing before editing them:

- **PPTX**: both builders draw into one shared `Presentation` (`deck_prs.py`), because python-pptx
  cannot reliably copy finished slides between files. Slide order is therefore *import order*. Page
  numbers are rewritten at the end against the real total — each builder had numbered its own slides
  against its own count.
- **HTML**: the two stylesheets share fourteen selectors (`.note`, `.sub`, `.good`, `.bar`, `body`,
  `h2`, `code`, …), so concatenating them silently restyles both halves. The combined builder renders
  the dataset deck through its own `render()` and then **scopes** the result — every selector
  prefixed with `.ds-appendix`, its `:root` variable block rehung on that class. Operating on
  rendered output means the dataset CSS is never hand-edited and the dataset deck stays
  independently buildable.

**GA content is data, not prose in a generator** (`GA_DONE` / `GA_GAPS` in `deck_data.py`), for the
same reason `HEADLINE_FACTS` is: the two decks once shipped 93 % and 92 % for the same figure.



**v2.8** adds the "Does it hold at scale?" slide (§10.6: the three-way raw/dict/parquet ratios at GB
scale under TWCS) to both the HTML and the PPTX, from a shared `SCALE` block in `deck_data.py` so
they cannot disagree. Two divergences between the generators were found and fixed while doing it,
both the same class of bug this entry is about:

- **The PPTX numbered 14 of its 16 slides.** Two slides had been added after the page numbering was
  written and never got a `pagenum()` call, so the deck said "of 14" while holding 16. Numbering is
  now derived from the slide count rather than written per slide.
- **The two decks ordered the same content differently.** The "layout vs compressor" slide sat at
  position 3 in the HTML and 11 in the PPTX, so the size story appeared before the performance
  section in one and after it in the other. The PPTX now follows the HTML: show the ratio, decompose
  it, then show it holding at scale.



**Prose drifts even when the numbers are regenerated, and that is the recurring failure here.** The
generators recompute every table from `deck_data.py`, so figures are always current — but the
sentences *around* them are literals and do not move. Four claims have now been found stale this way
and each was written correctly against an earlier corpus:

- "half the disk is a property of **wide tables**" — falsified by the three-column variant
- "the three that win have 20–197 columns" — same
- "disk usage on **five** real datasets" — there are eight
- "on three of **five** realistic schemas … at **scan parity**" — eight, and scan is 0.82×, faster

**When adding a dataset or changing a default, re-read the prose, not just the tables.** A grep for
stale figures does not catch "five datasets" or "wide tables", because those are words.

The same sweep over the design doc found three more, one of them substantive: its worked example of
C6 declining a conversion said "D10 and D11 would both be measured at 7–8 % and correctly left as
SSTables". On production figures D10 saves 17.5 % against a 15 % floor, so it is **converted** — the
example had come to illustrate the opposite of its point. Also corrected there: the conclusion still
answered §1 with "on wide tables, roughly half", and "write and scan parity" understated a scan that
is 0.82× and therefore faster than the row format.
Three-criteria policy, eight datasets at the shipped defaults, the three-column ISD variant, a
slide deriving delta encoding of timestamps from the encoder, a slide decomposing the win against
the **uncompressed** baseline, and the corrected L2 story — which is the one a reader is most likely to push back on,
since it shows Scylla's own compressor doing the larger share on six of seven datasets. Version
appears in the filename as well as the title slide.

**Backblaze: resolved, and the lesson is about measurement, not the format.** Its ratio appeared
to swing between 95.4 % and 263.9 %, and it was withheld from v2.4 on that basis. The cause was
**directory selection**, through three successive wrong answers:

1. `list(glob(...))[0]` — arbitrary order.
2. Newest by **mtime** — worse, and the one that produced the wild readings. *Removing files from
   a directory updates that directory's mtime*, so a table being dropped gets a fresh timestamp
   and beats the newly-created one. The pipeline measured the dying directory.
3. **Resolved from the table's id** in `system_schema.tables` — exact, since Scylla names the
   directory `<table>-<id without dashes>`. Now in `live_table_dir.py`, shared by every
   measurement script.

Under the id lookup, two consecutive runs give lz4 **byte-identical** at 59 351 983 and pq
**byte-identical** at 20 803 872, ratio 95.8 % / 95.9 %. Backblaze is restored at 95.8 %.

**Leaf-set hypothesis disproven, 2026-08-19.** The suspicion was that a row-group cut flips the
writer from the *derived* leaf set to the *conservative* one — 199 leaves against 394 on this
table — and that this caused the 4x swing. Swept `rows_per_row_group` on one loaded table under the
id-based lookup:

| `rows_per_row_group` | `pq` bytes |
|---:|---:|
| no cut (10⁸ rows, 1 GiB buffer) | 18 037 425 |
| 20 000 | 19 908 060 |
| **5 000 — the default** | **20 803 872** |
| 2 000 | 26 862 193 |

Two things follow. The leaf set is worth **+10.4 %**, not 4x — real, and the cost of the
conservative set is modest even on the corpus's widest table. And the default reproduces
**20 803 872 exactly**, the same value every controlled run gives, with size growing smoothly as
row groups shrink. **No row-group setting produces 84.5 MB**, so the mechanism is not this.

**Where that leaves it — closed 2026-08-21, and the remaining "anomaly" was never one.** Backblaze
at the shipped defaults is 20 803 872 bytes against a ~21.7 MB native, i.e. 95.8 %, established by
repeated isolated runs and by a controlled sweep that lands on the same number. The 84.5 MB and
32.5 MB readings are a *different quantity*, not a fault: the dataset is loaded by binding every
column with NULL for a missing reading, which writes one cell tombstone per null — 42 448 175 of them
— and those readings are the runs in which the tombstones had not been purged yet.

§10.18 of the design doc settles it. A table created without a `tombstone_gc` property gets mode
`repair`, not `timeout` (`tombstone_gc.cc:325`), and under RF=1 `repair` short-circuits `gc_before` to
*now* (`tombstone_gc.cc:188`) — `gc_grace_seconds` is never read. So on this single-node lab every
cell tombstone is purgeable the moment it is written, and whether a given compaction has got to it is
a commitlog-release race. Measured: 4/4 purged at the default mode, 0/4 under `timeout`, 0/4 under
`disabled`; and on a four-node cluster with the default mode, RF=1 purges while RF=3 retains every
one. Which inverts the framing — **the ~84 MB reading is what a replicated cluster stores, and
20 803 872 is what RF=1 purges down to.** The "batch-only" claim was also wrong; it was never about
batch context, only about timing.

Nothing here was a footer or a format problem, so the footer diff this section proposed as the next
diagnostic would have found nothing. `harness.py` now accepts `PQ_TOMBSTONE_GC` (unset by default) to
pin the mode, which is what makes a bind-NULL dataset reproducible either way.

**Two conclusions I published and then had to withdraw**, worth recording because both were
confidently argued from a broken selector: that the stable 59 351 983 was a fossil (it was the
correct value), and that the format produced 4× swings on this table (it never did). A figure
that repeats is not thereby trustworthy, and neither is a figure that varies — both need the
selector checked first.

**Unverified as a result:** the row-group-cut leaf-set experiment (+22.7 % for a cut, 69.1 → 84.7
MB) ran through its own mtime-based lookup and should be redone before it is cited.

### CI hygiene, done 2026-08-19
Both fixed; recorded because both were invisible failures rather than loud ones.

- **`sstable_parquet_perf_test` ran its full measurement workload in CI.** It is enrolled in the
  standard boost suite (`configure.py`), so every CI invocation was paying for 20 000 partitions
  and 10 000 point reads to print numbers nobody reads. Defaults are now 2 000 / 1 000; the
  measurement sizes come from `PQ_PERF_PARTITIONS` / `PQ_PERF_POINTS`, which anything quoting a
  §10.4 figure must set. The small default is a smoke test, not a measurement.
- **R-13 was measured and thrown away.** `perf_pq_scan_memory_scaling` computed peak scan memory
  at 4 000 and 32 000 partitions, printed the ratio, and asserted nothing — so a reader that
  started materialising the whole sstable would have printed `8.00x` and passed. Now
  `BOOST_REQUIRE(growth < 3.0)`. The bound is loose on purpose: a bounded reader is ~1x, but peak
  allocation at two file sizes is noisy, and the only failure mode worth catching is the ~8x one.

### 12. Run `interop_shapes.py` after any change to the schema mapping
It builds a table per shape the format emits — flat scalars, all three non-frozen collection kinds,
frozen collections, statics, TTL, counters, and a collection at each folding level — converts each,
and reads every resulting `pq` sstable with **both pyarrow and DuckDB**. 11/11 today.

Two readers on purpose: pyarrow *is* parquet-cpp, and the MAP arity check that rejected every
collection file was parquet-cpp's, so a gate built on pyarrow alone tests one implementation's
opinion of the spec. DuckDB has its own reader. (`pip install --user duckdb` was needed here; it is
not a Scylla dependency, only a test one.)

**Gap closed 2026-08-19, and it turned into a finding.** L2 fell back for three successive fixtures
because its precondition forbids a row marker and **every CQL `INSERT` writes one**. Only
`UPDATE`-written data with a single timestamp, no TTL and no deletions can reach L2. With that
fixture L2 applies and shows 4 leaves against L1's 11, verified by both readers. 16/16 shapes now.

**Checked, and it was two separate problems.** The export tool printed the *requested* folding
level rather than the effective one, so every L2 measurement it produced was mislabelled; it now
prints both and the fallback is visible. And §10.1f's L2 savings are the *harness's* Python folding,
not this writer's — which is why they differ from L1 at all, since a fallen-back writer produces
byte-identical output. They answer "is folding worth it", not "what does this implementation
produce". For INSERT-written data the answer to the second is L1 and the saving is zero.

**Measured 2026-08-19 and closed: L2 is worth 1.4 %, not 35 %** (§10.1m). L2 applies only when every
cell shares one timestamp, and a `__ts` column of identical values compresses to nearly nothing —
294 bytes from 240 111 in one measured file. So the column L2 removes is already free precisely when
L2 is allowed to remove it. The 35-point figure came from folding away a *varied* timestamp column,
which is the case L2 cannot legally apply to. Nothing to fix; the size argument for folding rests on
L1, and L2 should not be quoted as a disk lever.

This exists because a passing interop suite of *flat* fixtures let a broken MAP annotation make
every collection and counter file unreadable by parquet-cpp for as long as collections have
existed (§10.3i). The seven original fixtures are still worth keeping, but they only ever proved
that flat schemas interoperate. **Any change to the tree builder or the leaf layout should re-run
this**, because the failure mode is a file no other tool can open, and nothing inside Scylla
notices.

### 13. Measure through `live_table_dir.py`, never by glob or mtime
Every measurement script resolves the table's data directory from its id in
`system_schema.tables`. Do not reintroduce a glob or an mtime heuristic: deleting files from a
directory updates that directory's mtime, so a dropped table outranks the live one and the
pipeline silently measures a corpse. This cost two published conclusions before it was found
(item 11).

### 14. Environment traps worth knowing
- A rebuilt binary does not replace a running node. `~/pq-lab/ensure_fresh_node.sh` is a
  precondition on all measurement scripts for this reason; it cost real debugging time three times
  before it existed.
- The lab keyspace needs `NetworkTopologyStrategy`; `SimpleStrategy` is rejected on this node.
- `sstable_compaction_test` segfaults after ~68 cases in this environment. Verified identical on a
  stashed baseline, so pre-existing.
- **`keyspace_upgrade_sstables` is GET-only**, where `keyspace_flush`, `keyspace_compaction` and
  `retrain_dict` are POST. A POST to it returns 404. The lab helper used to swallow the status, so
  the call was a silent no-op and the script continued as if the sstables had been rewritten — it
  produced a false FAIL on the first run of `twcs_hybrid_check.py`. `api()` now raises on any
  status ≥ 400 in every lab script: a measurement that continues past a failed step is worse than
  one that crashes. (The §10.6 ISD figures are unaffected — the two major compactions after
  `retrain_dict` rewrite everything anyway, which the 860 MB → 282 MB drop confirms.)
- **An object-storage keyspace makes node startup depend on AWS credentials, and it stalls rather
  than failing.** After `objstore_check.py` ran, the `pqs3` keyspace persisted in the schema with
  `storage=s3://pqbucket`, so every later node start had to reach S3 to populate that table. Started
  without `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` the node **hung indefinitely before opening
  CQL** — last log line `s3 - Update creds in the background`, REST port answering, 9042 refused, no
  error and no timeout. Restarted with the credentials present, CQL came up in 6 s. Two properties
  make this a footgun beyond the lab: it presents as "node is up" to any REST health check, and the
  dependency is invisible in `scylla.yaml` because it is the *schema* that carries the S3 reference —
  removing `object_storage_endpoints` does not help while such a keyspace exists. Recovery is to start
  with credentials, drop the keyspace, then restore the config.
- **`wait_up()` polled REST, which is ready long before CQL.** Every script that restarts the node
  through `ensure_fresh_node.sh` and then connects with the driver inherited the race, and it cost two
  runs that died on `NoHostAvailable` against a node starting perfectly normally. Fixed centrally
  rather than per caller: REST first (a node that never answers REST is a louder failure), then a TCP
  connect to 9042.
- **Lab tables inherit `rows_per_partition: ALL`, so a read benchmark measures the row cache.** The
  first version of `~/pq-lab/scale_pointread.py` warmed 50 keys and then timed 2 000 point reads over
  a 9.3 MB table on an 8 GB node: every probe was a cache hit, p50 came out at 286 us, and it looked
  like it had refuted the footer-parse prediction of §10.4j. With
  `caching = {'keys':'NONE','rows_per_partition':'NONE'}` the same table reads at **973 us** — the
  cache was hiding 3.4x of the cost. Any read measurement that means to reach an sstable must disable
  caching explicitly, and should carry a native control in the same run so a rising number cannot be
  mistaken for machine drift.
- **The rewrite and compaction REST calls are asynchronous.** A "wait until the file set stops
  changing" loop polled twice before the work started, saw no change, and reported success. Any such
  wait must require an observed change first — `settle(expect_change_from=...)` — so that a no-op
  reads as a timeout rather than as a result.
