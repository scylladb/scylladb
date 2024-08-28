/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <cstdint>
#include <unordered_map>
#include <vector>
#include <seastar/core/sstring.hh>
#include "gms/inet_address.hh"
#include "db/commitlog/replay_position.hh"
#include "locator/host_id.hh"

namespace db {
namespace hints {

// A sync point is a collection of positions in hint queues which can be waited on.
// The sync point encompasses one type of hints manager only.
struct sync_point {
    using host_id_or_addr = std::variant<locator::host_id, gms::inet_address>;
    using shard_rps = std::unordered_map<host_id_or_addr, db::replay_position>;
    // ID of the host which created this sync point
    locator::host_id host_id;
    std::vector<shard_rps> regular_per_shard_rps;
    std::vector<shard_rps> mv_per_shard_rps;

    /// \brief Decodes a sync point from an encoded, textual form (a hexadecimal string).
    static sync_point decode(sstring_view s);

    /// \brief Encodes the sync point in a textual form (a hexadecimal string)
    sstring encode() const;

    bool operator==(const sync_point& other) const = default;
};

// IDL type
// Contains per-endpoint and per-shard information about replay positions
// for a particular type of hint queues (regular mutation hints or MV update hints)
struct per_manager_sync_point_v1_or_v2 {
    std::vector<gms::inet_address> endpoints;
    std::vector<db::replay_position> flattened_rps;
};

// IDL type
struct sync_point_v1_or_v2 {
    locator::host_id host_id;
    uint16_t shard_count;

    // Sync point information for regular mutation hints
    db::hints::per_manager_sync_point_v1_or_v2 regular_sp;

    // Sync point information for materialized view hints
    db::hints::per_manager_sync_point_v1_or_v2 mv_sp;
};

// IDL type
// same as per_manager_sync_point_v1_or_v2 except that it stores the
// endpoints as host_id instead of address
struct per_manager_sync_point_v3 {
    std::vector<locator::host_id> endpoints;
    std::vector<db::replay_position> flattened_rps;
};

// IDL type
struct sync_point_v3 {
    locator::host_id host_id;
    uint16_t shard_count;

    // Sync point information for regular mutation hints
    db::hints::per_manager_sync_point_v3 regular_sp;

    // Sync point information for materialized view hints
    db::hints::per_manager_sync_point_v3 mv_sp;
};

}
}
