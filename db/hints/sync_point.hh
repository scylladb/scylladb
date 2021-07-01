/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
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
