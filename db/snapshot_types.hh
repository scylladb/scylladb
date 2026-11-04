/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <string_view>

#include <fmt/format.h>

#include "schema/schema_fwd.hh"
#include "dht/token.hh"
#include "locator/host_id.hh"
#include "sstables/types.hh"
#include "utils/UUID.hh"

namespace utils {
template <typename, size_t>
class chunked_vector;
}

namespace db {

using is_downloaded = bool_class<class is_downloaded_tag>;

enum class snapshot_state : uint8_t {
    unknown,
    local,
    being_backed_up,
    remote_and_local,
    remote,
};

enum class snapshot_table_type : uint32_t {
    cql_table,
    cql_view,
    alternator_table,
};

struct snapshot_sstable_entry {
    sstables::sstable_id sstable_id;
    dht::token first_token;
    dht::token last_token;
    sstring toc_name;
    sstring prefix;
    is_downloaded downloaded{is_downloaded::no};
    locator::host_id node;
    size_t tablet_id;
    snapshot_state state;
    int64_t repaired_at;
    int64_t data_size;
    int64_t index_size;
};

// One snapshot_sstables row, as read by the collection of a failed
// snapshot attempt's residue.
struct snapshot_sstable_cleanup_entry {
    sstables::sstable_id sstable_id;
    dht::token first_token;
    sstring toc_name;
    locator::host_id node;
    int64_t write_timestamp;
};

struct snapshot_sstables_progress {
    size_t nr_sstables;
    size_t nr_downloaded_sstables;
};

// A single cluster level snapshot
struct snapshot_entry {
    std::string name;

    db_clock::time_point created_at;
    // Note: if non-expiring, this is db_clock::time_point{} (i.e. zero)
    db_clock::time_point expires_at;

    std::string namespace_version;
    std::string manifest_version;

    constexpr bool operator==(const snapshot_entry& o) const noexcept = default;
};

using snapshot_attempt_committed = bool_class<class snapshot_attempt_committed_tag>;

// One attempt of a cluster snapshot. The coordinator writes the row before
// the attempt's RPCs go out and sets `committed` when the attempt seals the
// tag. Uncommitted attempts are residue, collected lazily.
struct snapshot_attempt_entry {
    utils::UUID attempt;
    bool committed;

    constexpr bool operator==(const snapshot_attempt_entry& o) const noexcept = default;
};

// A cluster snapshot attempt keys its catalog rows and bucket references by
// "{tag}/{attempt}", so two attempts of one tag never share a name. The user
// tag is the first path segment, which is why a user tag cannot contain '/'.
constexpr char snapshot_attempt_separator = '/';

inline std::string_view snapshot_user_tag(std::string_view name) {
    return name.substr(0, name.find(snapshot_attempt_separator));
}

inline bool is_valid_user_snapshot_tag(std::string_view tag) {
    return !tag.empty() && tag.find(snapshot_attempt_separator) == std::string_view::npos;
}

inline sstring make_snapshot_attempt_name(std::string_view tag, utils::UUID attempt) {
    return sstring(fmt::format("{}{}{}", tag, snapshot_attempt_separator, attempt));
}

// Backup location for a snapshot for a given dc
struct snapshot_remote_location_entry {
    std::string snapshot_name;
    std::string datacenter;

    // reference to named endpoint in scylla conf.
    // note: need to be consistent across creation
    // and usage of this info. 
    std::string endpoint;
    std::string bucket;
    std::string prefix;

    snapshot_state state;

    constexpr bool operator==(const snapshot_remote_location_entry& o) const noexcept = default;
};

// Keyspace as of the time of snapshot
struct snapshot_keyspace_entry {
    std::string snapshot_name;
    std::string keyspace_name;
    std::string keyspace_schema;

    constexpr bool operator==(const snapshot_keyspace_entry& o) const noexcept = default;
};

// Table as of the time of snapshot
struct snapshot_table_entry {
    std::string snapshot_name;
    std::string keyspace_name;
    std::string table_name;

    ::table_id table_id;
    snapshot_table_type type;

    ::table_id base_table_id;

    std::string table_schema;

    std::string tablet_layout;

    constexpr bool operator==(const snapshot_table_entry& o) const noexcept = default;
};

struct snapshot_tablet_cleanup_entry {
    dht::token first_token;
    int64_t write_timestamp;
};

// Tablet as of the time of snapshot
struct snapshot_tablet_entry {
    size_t tablet_id;

    // Token range of tablet, equivalent
    // to tablet_manager::get_first_token/get_last_token
    dht::token first_token;
    dht::token last_token;
    db_clock::time_point repair_time;
    int64_t repaired_at;

    constexpr bool operator==(const snapshot_tablet_entry& o) const noexcept = default;
};

// Node as of the time of snapshot
struct snapshot_node_entry {
    std::string datacenter;
    std::string rack;
    locator::host_id node;

    constexpr bool operator==(const snapshot_node_entry& o) const noexcept = default;
};

struct snapshot_entries {
    utils::chunked_vector<snapshot_sstable_entry> sstables;
    std::vector<snapshot_tablet_entry> tablets;
};

struct snapshot_dc_location {
    std::string endpoint;
    std::string bucket;
    std::string prefix;

    constexpr bool operator==(const snapshot_dc_location& o) const noexcept = default;
};

} // namespace db
