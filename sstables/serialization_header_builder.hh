/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "sstables/types.hh"

class schema;
struct encoding_stats;

namespace sstables {

// Builds the serialization header that a writer stores in Statistics.db.
//
// The header holds the minimum timestamp, TTL, and local deletion time
// from `enc_stats`, the types of the key columns, and the names and types
// of the static and regular columns. The static and regular columns are in
// the order of `s.v3().all_columns()`.
serialization_header make_serialization_header(const schema& s, const encoding_stats& enc_stats);

} // namespace sstables
