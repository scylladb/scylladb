/*
 * Modified by ScyllaDB
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#pragma once

// Scylla includes.
#include "db/commitlog/commitlog.hh"
#include "db/commitlog/commitlog_entry.hh"
#include "db/hints/internal/common.hh"
#include "utils/loading_shared_values.hh"

// STD.
#include <filesystem>

/// This file is supposed to gather meta information about data structures
/// and types related to storing hints.
///
/// Under the hood, commitlog is used for managing, storing, and reading
/// hints from disk.

namespace db::hints {
namespace internal {

using node_to_hint_store_factory_type = utils::loading_shared_values<endpoint_id, db::commitlog>;
using hints_store_ptr = node_to_hint_store_factory_type::entry_ptr;
using hint_entry_reader = commitlog_entry_reader;

/// \brief Rebalance hints segments among all present shards.
///
/// The difference between the number of segments on every two shard will be not greater than 1 after the
/// rebalancing.
///
/// Removes the sub-directories of \ref hints_directory that correspond to shards that are not relevant any more
/// (re-sharding to a lower shards number case).
///
/// Complexity: O(N+K), where N is a total number of present hints' segments and
///                           K = <number of shards during the previous boot> * <number of end points for which hints where ever created>
///
/// \param hints_directory A hints directory to rebalance
/// \return A future that resolves when the operation is complete.
future<> rebalance(std::filesystem::path hints_directory);

} // namespace internal
} // namespace db::hints
