/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "readers/mutation_reader_fwd.hh"
#include "mutation/tombstone.hh"

/// Creates a reader which strips the data covered by a table's truncation tombstone,
/// set by the eventually-consistent TRUNCATE (see docs/dev/eventually-consistent-truncate.md),
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
/// Supports all forwarding modes the source supports.
mutation_reader make_truncation_stripping_reader(mutation_reader source, tombstone truncation_tombstone);
