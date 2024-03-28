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
/// The difference between the number of segments on every two shard will not be
/// greater than 1 after the rebalancing.
///
/// Removes the subdirectories of \ref hint_directory that correspond to shards that
/// are not relevant anymore (in the case of re-sharding to a lower shard number).
///
/// Complexity: O(N+K), where N is a total number of present hint segments and
///                           K = <number of shards during the previous boot> * <number of endpoints
///                                 for which hints where ever created>
///
/// \param hint_directory A hint directory to rebalance
/// \return A future that resolves when the operation is complete.
future<> rebalance_hints(std::filesystem::path hint_directory);

class hint_directory_manager {
private:
    std::map<locator::host_id, gms::inet_address> _mappings;

public:
    // Inserts a new mapping and returns it.
    // If either the host ID or the IP is already in the map, the function inserts nothings
    // and returns the existing mapping instead.
    std::pair<locator::host_id, gms::inet_address> insert_mapping(const locator::host_id& host_id,
            const gms::inet_address& ip);

    // Returns the corresponding IP for a given host ID if a mapping is present in the directory manager.
    // Otherwise, an empty optional is returned.
    [[nodiscard]] std::optional<gms::inet_address> get_mapping(const locator::host_id& host_id) const noexcept;

    // Returns the corresponding host ID for a given IP if a mapping is present in the directory manager.
    // Otherwise, an empty optional is returned.
    [[nodiscard]] std::optional<locator::host_id> get_mapping(const gms::inet_address& ip) const noexcept;

    // Returns a mapping corresponding to either the passed host ID, or the passed IP if the mapping exists.
    // Otherwise, an empty optional is returned.
    [[nodiscard]] std::optional<std::pair<locator::host_id, gms::inet_address>> get_mapping(
            const locator::host_id& host_id, const gms::inet_address& ip) const noexcept;

    // Removes a mapping corresponding to the passed host ID if the mapping exists.
    void remove_mapping(const locator::host_id& host_id) noexcept;
    // Removes a mapping corresponding to the passed IP if the mapping exists.
    void remove_mapping(const gms::inet_address& ip) noexcept;

    bool has_mapping(const locator::host_id& host_id) const noexcept;
    bool has_mapping(const gms::inet_address& ip) const noexcept;

    // Removes all of the mappings.
    void clear() noexcept;
};

} // namespace internal
} // namespace db::hints
