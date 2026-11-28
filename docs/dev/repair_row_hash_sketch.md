# Row hash sketches in row-level repair

In step B of a [row-level repair](row_level_repair.md) round, the master
learns which rows of its working buffer each follower has. Without sketches
a follower whose combined hash differs streams the hash of every row in its
working buffer, 8 bytes per row for up to 32 MiB of rows, even when a few
rows differ. With sketches the cost is proportional to the difference.

## The sketch

`repair_hash_sketch` (repair/hash_sketch.hh) is an invertible Bloom lookup
table over 64-bit row hashes: 4 hash functions, each selecting a cell in its
own sub-table, and 12-byte cells (16-bit count, XOR of the hashes, XOR of a
16-bit check value of each hash; a cell is taken to hold one hash only when
that hash also maps to the cell). Subtracting two sketches with the same
number of cells cancels the hashes both sets hold; peeling the result lists
the symmetric difference while the sketch has about 2 cells per differing
hash (the k=4 decode fails under 0.2% of the time at 1.5 cells per hash from
256 differences up, and always below 1.3).

A sketch that does not peel still measures the difference it holds: the
centered second moment of its counts over k(1 - k/cells) is an unbiased
estimate of the symmetric difference, with a relative deviation of about
sqrt(2/cells) (9% at 256 cells), for any mix of hashes from the two sides.
Counts are 16 bits so that this estimate survives one-sided differences of
up to 2 million hashes at 256 cells.

Sketches nest: a sketch with s times the cells folds into the smaller one
by adding each run of s cells, so a receiver that holds the smaller sketch
can rebuild the larger one from all but one cell of each run. Growing a
sketch that did not decode therefore costs only the new cells.

## Protocol

Gated on the REPAIR_ROW_HASH_SKETCH cluster feature and the
enable_repair_row_hash_sketch option (live-updatable, default true). Used
by vnode and tablet repair alike.

1. The master computes its own combined hash first, then sends
   REPAIR_GET_COMBINED_ROW_HASH_AND_SKETCH to every follower in parallel,
   with its hash and the number of cells it wants. A follower replies with
   its combined hash and, unless its hash matches the master's or the sketch
   would cost more than its row hash list (cells x 12 >= rows x 8), a sketch
   of its working rows.
2. If all hashes match, the round ends, as before.
3. Before pulling any row, the master takes a snapshot of its working row
   hashes and decodes every sketch against it. A decode is accepted only if
   the sketch is well formed, peels to empty, the hashes it lists as
   removed are in the reference set and those it lists as added are not,
   there are no duplicates, and the rebuilt set reproduces the follower's
   combined hash. Repair correctness never depends on the sketch.
4. A follower whose sketch did not peel is asked, through
   REPAIR_GET_ROW_HASH_SKETCH_EXTENSION, to grow it to a multiple of its
   size covering 1.7 cells per differing hash of its estimate, unless the
   new cells would cost more than the row hash list. All extension requests
   of a round go out together. A grown sketch that still does not peel,
   which happens when the estimate was low, is grown once more from its
   own, more accurate estimate.
5. A follower with no decoded sketch streams its row hash list, as before.
6. The rest of the round is unchanged: the master pulls the rows only the
   follower has, then pushes the rows the follower lacks.

### References

Followers are correlated. When the master is the stale replica, the
followers agree with each other, and sketching each one against the
master's rows would cost the whole difference twice. The master decodes a
follower's sketch against the closest reference it knows: its own rows at
compare time, or the rows already recovered for an earlier follower in the
same round (a local sketch of the reference set is built for the
subtraction). On a cold round where neither follower's sketch peels, the
two sketches are subtracted directly (the larger folded to the smaller's
size), which peels when the followers agree, and the peeled difference is
applied once the first follower is recovered.

### Sizing

The first sketch is sized from a history kept in repair_service for the
process lifetime, keyed by table and unordered host pair: the last four
differences measured between the two hosts in rounds where the follower
differed from the master, zeros included (a zero against another follower
records that the two agree). The request asks for 1.5 cells per hash of the
largest recorded difference, at least 256, taking the smallest over the
references the master will have. An empty history asks for 256 cells. The
history carries across rounds, tablets and repairs of the table; a wrong or
stale history costs one 3 KB sketch and one round trip per repair before
the second request corrects it.

## Cost

| case | bytes per differing hash | extra round trips |
|---|---|---|
| follower matches the master, or an earlier follower | 0, or one 3 KB sketch | 0 |
| history right | 18 | 0 |
| history cold or wrong | 21 to 25 | 1 per round, shared by all followers |
| difference over about 30 percent of the rows | the list, 8 per row | 1 |

CPU: 4 cell updates per row per sketch, about 6 ns per row, on the
follower for each sketch it sends and on the master for each reference it
tries; one pass over the cells for the estimate. Memory on the master: one
extra copy of its working row hashes per dirty round.

## Alternatives considered

| scheme | outcome |
|---|---|
| Rateless IBLT (Yang et al., SIGCOMM 2024) | not used: needs no sizing, but at the 95th percentile it uses 1.4 to 1.7 symbols per difference, no fewer than this sketch's 1.5 to 2 cells; encoding costs 100 to 200 ns per row against 6; continuing a stream 1.5x at a time fails 2 to 12% of the time at 300 to 1000 differences |
| PinSketch (BCH codes, minisketch) | not used: 8 bytes per difference, the minimum, but building it costs about 5 ns per row per unit of capacity, 0.4 s per round for 300k rows at capacity 256, on both sides |
| PBS (Gong et al., VLDB 2021), range-based reconciliation (Meyer 2023), Merkle trees | not used: several or log n round trips per round, which lose to a one-round sketch across datacenters |
| XYZ-Sketch (Xu et al. 2026) | not used: near-minimal bytes at IBLT cost, but a preprint with no production implementation |
| Self-sizing IBLT (Wu et al. 2026) | adopted: estimating the difference from a sketch that did not peel; here the sketch is extended instead of resent |

With sizing right, hashes are 10 to 15% of a dirty round's traffic with
100-byte rows and under 3% with 1 KB rows, so a denser encoding saves
little except for very small rows.

## Metrics

| metric | meaning |
|---|---|
| scylla_repair_rx_sketch_cells_nr | sketch cells received by masters on this shard |
| scylla_repair_sketch_decoded_nr | sketches decoded, each replacing a row hash list |
| scylla_repair_sketch_fallback_nr | sketches that did not decode, each followed by a list |
| scylla_repair_sketch_extended_nr | extensions received, each growing a sketch that did not decode |
| scylla_repair_sketch_reference_hit_nr | sketches decoded against another follower's rows |
| scylla_repair_sketch_in_sync_nr | followers that matched the master's hash and sent no sketch |
| scylla_repair_sketch_declined_nr | followers that differed but sent no sketch because the list was smaller |

The error injection repair_row_hash_sketch_decode_fail makes every decode
fail on the master, exercising the list fallback.
