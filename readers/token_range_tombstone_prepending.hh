/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "readers/mutation_reader_fwd.hh"
#include "mutation/token_range_tombstone.hh"

// Returns a reader which emits the given token range tombstones ahead of
// everything the underlying reader emits, as the fragment stream requires.
//
// The whole list is emitted, not only the part covering the read's partition
// range: the list is tiny, and emitting all of it means a later
// fast_forward_to() needs no further tombstones, which it could no longer emit
// anyway once the prologue is behind us.
//
// Returns the underlying reader unchanged if the list is empty.
mutation_reader make_token_range_tombstone_prepending_reader(mutation_reader underlying, token_range_tombstone_list);

// Returns a reader which takes the token range tombstones out of the
// underlying reader's stream and merges them into `into`, emitting none of them
// itself. `into` must outlive the reader.
//
// This is for a consumer which keeps the tombstones itself and emits them on
// its own terms, as the row cache does: the cache serves partitions without
// consulting the source they came from, so it has to hold the tombstones rather
// than let them pass through into the cached data.
mutation_reader make_token_range_tombstone_absorbing_reader(mutation_reader underlying, token_range_tombstone_list& into);
