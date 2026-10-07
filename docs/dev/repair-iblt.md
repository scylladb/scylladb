# Row hash sketch in tablet repair

This document describes how tablet repair uses a row hash sketch. The
sketch is an invertible Bloom lookup table (IBLT). For the full row level
repair algorithm, refer to [row_level_repair.md](row_level_repair.md).

## Terms

- **Master**: the node that runs the repair of a range.
- **Follower**: a peer node that the master repairs with.
- **Round**: one step of row level repair. In a round, each node puts up to
  32 MiB of rows in its working row buffer.
- **Row hash**: a 64-bit hash of one row.
- **Combined hash**: one hash of all the row hashes in a working row buffer.
- **Sketch**: an IBLT of row hashes. The type is `repair_hash_sketch`, in
  `repair/hash_sketch.hh`.
- **Cell**: one entry of a sketch.
- **Difference**: the row hashes that are in one set and not in the other
  set (the symmetric difference).

## Problem

Each round, the master compares its combined hash with the combined hash of
each follower. If one row is different, the combined hashes are different.

Without a sketch, the master then gets the full list of row hashes from the
follower. The follower sends 8 bytes for each row in its working row buffer.
A round can have tens of thousands of rows. Thus, a small difference causes
a large transfer.

## Solution

The follower sends a sketch together with its combined hash. The size of the
sketch is related to the size of the difference, not to the number of rows.
The master decodes the sketch and gets the full set of follower row hashes.
The follower does not send the full list.

The sketch does not add a round trip. It uses the same request as the
combined hash.

## Sketch structure

A sketch has a number of cells. The number of cells is a multiple of 4.
Each cell has three fields:

- `counts`: a 16-bit signed count of the row hashes in the cell.
- `key_sums`: the 64-bit XOR of the row hashes in the cell.
- `check_sums`: the 32-bit XOR of a check hash of each row hash in the cell.
  `check(h)` is the upper 32 bits of a mix of `h`.

Each cell uses 14 bytes (`cell_bytes`).

`counts` wraps around at 2^16. This is correct. The follower sketch can have
more than 32767 row hashes in one cell. Only the count after the
subtraction must be correct, and the wrap-around does not change it.

The sketch uses 4 hash functions (`hash_count`). The cells are divided into 4
equal sub-tables. Hash function `j` selects one cell in sub-table `j`. Thus,
the 4 cells of a row hash are always different cells.

### Add a row hash

To add a row hash `h`, the sketch does these steps for each of the 4 cells
of `h`:

1. Add 1 to `counts`.
2. XOR `h` into `key_sums`.
3. XOR `check(h)` into `check_sums`.

### Subtract a sketch

Two sketches must have the same number of cells. To subtract sketch B from
sketch A, the code subtracts the `counts` of each cell. It also XORs the
`key_sums` and the `check_sums` of each cell. Row hashes that are in both
sets cancel. The result contains only the difference.

### Decode (peel)

A cell is pure if all these conditions are true:

- `counts` is 1 or -1.
- `check_sums` is equal to `check(key_sums)`.
- The cell is one of the 4 cells of the row hash `key_sums`.

A pure cell contains exactly one row hash. That row hash is `key_sums`. If
`counts` is 1, the row hash is in sketch A only. If `counts` is -1, the row
hash is in sketch B only.

The decoder does these steps:

1. Find all pure cells.
2. Take one pure cell and record its row hash.
3. Remove that row hash from its 4 cells.
4. If this makes other cells pure, add them to the list.
5. Do steps 2 to 4 again until no pure cells remain.
6. Make sure that all cells are now empty. If a cell is not empty, decoding
   failed.

The decoder also stops if it records more row hashes than there are cells.
A valid sketch cannot do this. This limit stops a damaged sketch from making
the decoder loop.

If the difference is too large for the number of cells, peeling stops before
all cells are empty. The code then uses the fallback (see below).

## Protocol

The sketch is used only when all these conditions are true:

- The repair is a tablet repair.
- The repair uses RPC streams.
- The `REPAIR_ROW_HASH_SKETCH` cluster feature is enabled.

Thus, vnode repair and mixed-version clusters do not change.

### Request

The master sends the `REPAIR_GET_COMBINED_ROW_HASH_WITH_SKETCH` verb to each
follower. The request has the same arguments as
`REPAIR_GET_COMBINED_ROW_HASH`, and also `sketch_cells` and `master_hash`.
`sketch_cells` is the number of cells that the master requests. `master_hash`
is the combined hash of the master rows. The master gets its own combined hash
first, and then sends the request to all followers in parallel.

### Response

The follower calculates its combined hash. Then it makes a sketch only if
its combined hash is not equal to `master_hash`, and the sketch is smaller than
the full list of row hashes:

    sketch_cells * 14 < rows * 8

A follower that is in sync with the master does not send a sketch, because the
master already has its rows.

The response (`get_combined_row_hash_with_sketch_response`) has:

- `hash`: the combined hash.
- `sketch`: the sketch, or no value.

Thus, a sketch never costs more bytes than the full list that it replaces.

## Master procedure

The master does these steps in each round:

1. Get the master combined hash. Then get the combined hash and sketch from
   all followers in parallel.
2. If all combined hashes are equal, go to the next round. The master does
   not decode the sketches.
3. Make a copy of the master row hashes. This copy is the compare-time set.
4. Do steps 5 to 9 for each follower, one follower at a time.
5. If the follower combined hash is equal to the current master combined
   hash, use the master row hashes. Go to the next follower.
6. If the master has no rows, get all rows from the follower. Go to the next
   follower.
7. If the follower sent a sketch, decode it against the compare-time set
   (`decode_peer_row_hashes()`). If the follower combined hash is equal to the
   master combined hash of step 1, use the compare-time set.
8. If decoding succeeded, use the decoded row hashes.
9. If decoding failed, or the follower did not send a sketch, get the full
   list of row hashes from the follower.

After these steps, the master knows the row hashes of each follower. The
other steps of the round do not change.

### Why the master uses a compare-time set

The follower makes its sketch against its rows at the time of the request.
The master must decode the sketch against its own rows at the same time.

In step 4, the master gets rows from followers, one at a time. Thus, the
master rows change during the loop. For this reason, the master decodes each
sketch against the compare-time set and not against its current rows.

This also gives a benefit. Sometimes the master gets old row versions from a
follower that is not synchronized. Then the master combined hash is no longer
equal to the combined hash of a follower that is synchronized. Without a
sketch, that follower sends its full list. With sketches, the master uses the
compare-time set for that follower, and no hashes are sent.

## Validation of a decoded sketch

`decode_peer_row_hashes()` accepts a decoded sketch only if all these
conditions are true:

1. The sketch is well formed. The number of cells is not zero, it is a
   multiple of 4, and the three fields have the same length.
2. Peeling empties all cells.
3. Each row hash that the follower does not have is in the compare-time set.
   It is in the decoded list only one time.
4. Each row hash that only the follower has is not in the compare-time set.
5. The combined hash of the rebuilt follower set is equal to the combined
   hash that the follower sent.

If one condition is false, the function returns no value. The master then
gets the full list of row hashes. Thus, the correctness of repair does not
depend on the sketch.

If all conditions are true, the function returns:

- `hashes`: the compare-time set, without the row hashes that the follower
  does not have, and with the row hashes that only the follower has.
- `diff_size`: the size of the difference.

## Sketch size

The master keeps a size history for each follower in `repair_node_state`.
The history has the difference sizes of the last 4 rounds (`recent_diffs`).

The master requests this number of cells from the follower:

    max(256, 2 * largest difference in recent_diffs)

The master records a difference size after each round:

| Case | Recorded size |
|---|---|
| All combined hashes are equal | 0 |
| Follower combined hash is equal to the master combined hash | 0 |
| The master has no rows | Number of follower rows |
| Sketch decoded | `diff_size` |
| Full list received | Difference between the full list and the compare-time set |

When the master has no rows, the recorded size is large. Thus, the follower
does not send a sketch in the next rounds. This is correct, because the
master does not use that sketch.

A sketch needs approximately 2 cells for each row hash in the difference.
Thus, a sketch uses approximately 28 bytes for each different row hash. The
full list uses 8 bytes for each row.

## Cost

- Follower: one pass over the working row buffer to make the sketch. The
  sketch is not larger than the full list of row hashes.
- Master: one copy of its row hashes in each round that is not synchronized.
  One pass over that copy to make its own sketch.
- Master: the decoding work is related to the number of cells. A follower
  sends fewer cells than 0.57 times its number of rows. Thus, the decoder
  does not yield.
- Rounds in which all combined hashes are equal do not decode sketches.
  Rounds that end before the combined hash request do not request a sketch.

## Metrics

| Metric | Description |
|---|---|
| `scylla_repair_rx_sketch_cells_nr` | Number of sketch cells that the master received |
| `scylla_repair_sketch_decoded_nr` | Number of sketches that decoded. Each one replaces a full list transfer |
| `scylla_repair_sketch_fallback_nr` | Number of sketches that did not decode. Each one causes a full list transfer |

At debug log level, the `repair` logger writes one line for each sketch.
The line shows the number of cells and if the sketch decoded.

## Limitations

- The size history starts empty for each tablet repair. Thus, the first
  round that is not synchronized uses 256 cells. If the difference is
  larger, the sketch does not decode, and the master gets the full list.
  SCYLLADB-5139 tracks this.
- The sketch reduces network transfer. It does not reduce the local work to
  read and hash each row.

## Code and tests

| Item | Location |
|---|---|
| Sketch | `repair/hash_sketch.hh` |
| Decoding and validation | `decode_peer_row_hashes()` in `repair/row_level.cc` |
| Master procedure | `row_level_repair`, in `repair/row_level.cc` |
| Follower handler | `repair_meta::get_combined_row_hash_with_sketch_handler()` |
| Wire format | `idl/repair.idl.hh` |
| Unit test | `test_decode_peer_row_hashes` in `test/boost/repair_test.cc` |
| Cluster test | `test_tablet_repair_row_hash_sketch` in `test/cluster/test_repair.py` |

The error injection `repair_row_hash_sketch_decode_fail` makes decoding fail
on the master. Use it to test the fallback.
