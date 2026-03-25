/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "schema/schema_fwd.hh"
#include "dht/token.hh"
#include "locator/host_id.hh"
#include "sstables/types.hh"

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

// A single cluster level snapshot
struct snapshot_entry {
    std::string name;

    db_clock::time_point created_at;
    db_clock::time_point expires_at;

    std::string namespace_version;
    std::string manifest_version;

    constexpr bool operator==(const snapshot_entry& o) const noexcept = default;
};

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

}
