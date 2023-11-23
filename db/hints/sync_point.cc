/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <exception>
#include <unordered_set>

#include <seastar/core/simple-stream.hh>
#include <seastar/core/smp.hh>

#include "db/hints/sync_point.hh"
#include "sync_point.hh"
#include "idl/hinted_handoff.dist.hh"
#include "idl/hinted_handoff.dist.impl.hh"
#include "utils/base64.hh"
#include "utils/xx_hasher.hh"

namespace db {
namespace hints {
// Sync points can be encoded in two formats: V1 and V2. V2 extends V1 by adding
// a checksum. Currently, we use the V2 format, but sync points encoded in the V1
// format still can be safely decoded.
//
// Format V1 (encoded in base64):
//   uint8_t 0x01 - version of format
//   sync_point_v1 - encoded using IDL
//
// Format V2 (encoded in base64):
//   uint8_t 0x02 - version of format
//   sync_point_v1 - encoded using IDL
//   uint64_t - checksum computed using the xxHash algorithm
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

static constexpr size_t version_size = sizeof(uint8_t);
static constexpr size_t checksum_size = sizeof(uint64_t);

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

static uint64_t calculate_checksum(const sstring_view s) {
    xx_hasher h;
    h.update(s.data(), s.size());
    return h.finalize_uint64();
}

sync_point sync_point::decode(sstring_view s) {
    bytes raw = base64_decode(s);
    if (raw.empty()) {
        throw std::runtime_error("Could not decode the sync point - not a valid hex string");
    }

    sstring_view raw_s(reinterpret_cast<const char*>(raw.data()), raw.size());
    seastar::simple_memory_input_stream in{raw_s.data(), raw_s.size()};

    uint8_t version = ser::serializer<uint8_t>::read(in);
    if (version == 2) {
        if (raw_s.size() < version_size + checksum_size) {
            throw std::runtime_error("Could not decode the sync point encoded in the V2 format - serialized blob is too short");
        }

        seastar::simple_memory_input_stream in_checksum{raw_s.end() - checksum_size, checksum_size};
        uint64_t checksum = ser::serializer<uint64_t>::read(in_checksum);
        if (checksum != calculate_checksum(raw_s.substr(0, raw_s.size() - checksum_size))) {
            throw std::runtime_error("Could not decode the sync point encoded in the V2 format - wrong checksum");
        }
    }
    else if (version != 1) {
        throw std::runtime_error(format("Unsupported sync point format version: {}", int(version)));
    }

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

    // Reserve version_size bytes for the version and checksum_size bytes for the checksum
    bytes serialized{bytes::initialized_later{}, version_size + measure.size() + checksum_size};

    // Encode using V2 format
    seastar::simple_memory_output_stream out{reinterpret_cast<char*>(serialized.data()), serialized.size()};
    ser::serializer<uint8_t>::write(out, 2);
    ser::serializer<sync_point_v1>::write(out, v1);
    sstring_view serialized_s(reinterpret_cast<const char*>(serialized.data()), version_size + measure.size());
    uint64_t checksum = calculate_checksum(serialized_s);
    ser::serializer<uint64_t>::write(out, checksum);

    return base64_encode(serialized);
}

}
}

