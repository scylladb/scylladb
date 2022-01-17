/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <cstdint>
#include <string_view>
#include <unordered_map>
#include <vector>
#include <seastar/core/sstring.hh>
#include "gms/inet_address.hh"
#include "db/commitlog/replay_position.hh"

namespace db {
namespace hints {

// A sync point is a collection of positions in hint queues which can be waited on.
// The sync point encompasses one type of hints manager only.
struct sync_point {
    using shard_rps = std::unordered_map<gms::inet_address, db::replay_position>;
    // ID of the host which created this sync point
    utils::UUID host_id;
    std::vector<shard_rps> regular_per_shard_rps;
    std::vector<shard_rps> mv_per_shard_rps;

    /// \brief Decodes a sync point from an encoded, textual form (a hexadecimal string).
    static sync_point decode(sstring_view s);

    /// \brief Encodes the sync point in a textual form (a hexadecimal string)
    sstring encode() const;

    bool operator==(const sync_point& other) const = default;
};

std::ostream& operator<<(std::ostream& out, const sync_point& sp);

}
}
