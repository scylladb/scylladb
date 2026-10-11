/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <seastar/util/bool_class.hh>

#include "readers/mutation_reader_fwd.hh"
#include "mutation/tombstone.hh"
#include "seastarx.hh"

using drop_empty_partitions = bool_class<struct drop_empty_partitions_tag>;

/// Creates a reader which strips the data covered by a table's truncation tombstone,
/// set by DELETE ALL DATA (see docs/dev/delete-all-data.md),
/// from the stream of \p source.
///
/// Every atom written at or before `truncation_tombstone.timestamp` is dropped: cells
/// (and collection elements), row markers, and row, range and partition tombstones.
/// The truncation tombstone itself is never emitted. Rows that end up empty and range
/// tombstone changes that end up redundant are dropped too.
///
/// Stripping is sound per source as well as after merging sources: everything dropped
/// is at or below the truncation tombstone and everything kept is above it, so nothing
/// dropped could shadow anything kept. It must be applied to each source before merging
/// them though, since merging counter cells, by logical clock, can keep a covered shard.
///
/// A partition which ends up with nothing is still emitted, as a partition start and a
/// partition end, unless \p drop_empty is set, which a consumer that keeps empty
/// partitions needs, such as one writing the stream to an sstable. Dropping empty
/// partitions is supported only without intra-partition forwarding.
///
/// Supports all forwarding modes the source supports.
mutation_reader make_truncation_stripping_reader(mutation_reader source, tombstone truncation_tombstone,
        drop_empty_partitions drop_empty = drop_empty_partitions::no);
