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

#include <exception>
#include <unordered_set>

#include <seastar/core/simple-stream.hh>

#include "db/hints/sync_point.hh"
#include "gms/inet_address_serializer.hh"
#include "idl/uuid.dist.hh"
#include "idl/uuid.dist.impl.hh"
#include "idl/replay_position.dist.hh"
#include "idl/replay_position.dist.impl.hh"
#include "idl/hinted_handoff.idl.hh"
#include "idl/hinted_handoff.dist.hh"
#include "idl/hinted_handoff.dist.impl.hh"
#include "serializer.hh"
#include "serializer_impl.hh"
#include "utils/base64.hh"

namespace db {
namespace hints {

// Format V1 (encoded in base64):
//   uint8_t 0x01 - version of format
//   sync_point_v1 - encoded using IMR
//
// sync_point_v1:
//   UUID host_id - ID of the host which created the sync point
//   uint16_t shard_count - the number of shards in this sync point
//   per_manager_sync_point_v1 regular_sp - replay positions for regular mutation hint queues
//   per_manager_sync_point_v1 mv_sp - replay positions for materialized view hint queues
//
// per_manager_sync_point_v1:
//   std::vector<gms::inet_address> addresses - adresses for which this sync point defines replay positions
//   std::vector<db::replay_position> flattened_rps:
//       A flattened collection of replay positions for all addresses and shards.
//       Replay positions are grouped by address, in the same order as in
//       the `addresses` field, and there is one replay position for each of
//       the shards (shard count is defined by the `shard_count`) field.
//       Flattened representation was chosen in order to save space on
//       vector lengths etc.

static std::vector<sync_point::shard_rps> decode_one_type_v1(uint16_t shard_count, const per_manager_sync_point_v1& v1) {
    std::vector<sync_point::shard_rps> ret;

    if (size_t(shard_count) * v1.addresses.size() != v1.flattened_rps.size()) {
        throw std::runtime_error(format("Could not decode the sync point - there should be {} rps in flattened_rps, but there are only {}",
                size_t(shard_count) * v1.addresses.size(), v1.flattened_rps.size()));
    }

    ret.resize(std::max(unsigned(shard_count), smp::count));

    auto rps_it = v1.flattened_rps.begin();
    for (const auto addr : v1.addresses) {
        uint16_t shard;
        for (shard = 0; shard < shard_count; shard++) {
            ret[shard].emplace(addr, *rps_it++);
        }
        // Fill missing shards with zero replay positions so that segments
        // which were moved across shards will be correctly waited on
        for (; shard < smp::count; shard++) {
            ret[shard].emplace(addr, db::replay_position());
        }
    }

    return ret;
}

sync_point sync_point::decode(sstring_view s) {
    bytes raw = base64_decode(s);
    if (raw.empty()) {
        throw std::runtime_error("Could not decode the sync point - not a valid hex string");
    }
    if (raw[0] != 1) {
        throw std::runtime_error(format("Unsupported sync point format version: {}", int(raw[0])));
    }

    seastar::simple_memory_input_stream in{reinterpret_cast<const char*>(raw.data()) + 1, raw.size() - 1};
    sync_point_v1 v1 = ser::serializer<sync_point_v1>::read(in);

    return sync_point{
        v1.host_id,
        decode_one_type_v1(v1.shard_count, v1.regular_sp),
        decode_one_type_v1(v1.shard_count, v1.mv_sp),
    };
}

static per_manager_sync_point_v1 encode_one_type_v1(unsigned shards, const std::vector<sync_point::shard_rps>& rps) {
    per_manager_sync_point_v1 ret;

    // Gather all addresses, from all shards
    std::unordered_set<gms::inet_address> all_addrs;
    for (const auto& shard_rps : rps) {
        for (const auto& p : shard_rps) {
            all_addrs.insert(p.first);
        }
    }

    ret.flattened_rps.reserve(size_t(shards) * all_addrs.size());

    // Encode into v1 struct
    // For each address, we encode a replay position for all shards.
    // If there is no replay position for a shard, we use a zero replay position.
    for (const auto addr : all_addrs) {
        ret.addresses.push_back(addr);
        for (const auto& shard_rps : rps) {
            auto it = shard_rps.find(addr);
            if (it != shard_rps.end()) {
                ret.flattened_rps.push_back(it->second);
            } else {
                ret.flattened_rps.push_back(db::replay_position());
            }
        }
        // Fill with zeros for remaining shards
        for (unsigned i = rps.size(); i < shards; i++) {
            ret.flattened_rps.push_back(db::replay_position());
        }
    }

    return ret;
}

sstring sync_point::encode() const {
    // Encode as v1 structure
    sync_point_v1 v1;
    v1.host_id = this->host_id;
    v1.shard_count = std::max(this->regular_per_shard_rps.size(), this->mv_per_shard_rps.size());
    v1.regular_sp = encode_one_type_v1(v1.shard_count, this->regular_per_shard_rps);
    v1.mv_sp = encode_one_type_v1(v1.shard_count, this->mv_per_shard_rps);

    // Measure how much space we need
    seastar::measuring_output_stream measure;
    ser::serializer<sync_point_v1>::write(measure, v1);

    // Reserve 1 byte for the version
    bytes serialized{bytes::initialized_later{}, 1 + measure.size()};
    serialized[0] = 1;
    seastar::simple_memory_output_stream out{reinterpret_cast<char*>(serialized.data()), measure.size(), 1};
    ser::serializer<sync_point_v1>::write(out, v1);

    return base64_encode(serialized);
}

std::ostream& operator<<(std::ostream& out, const sync_point& sp) {
    out << "{regular_per_shard_rps: " << sp.regular_per_shard_rps
        << ", mv_per_shard_rps: " << sp.mv_per_shard_rps
        << "}";
    return out;
}

}
}
