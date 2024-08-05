/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

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
//   sync_point_v1_or_v2 - encoded using IDL
//
// Format V2 (encoded in base64):
//   uint8_t 0x02 - version of format
//   sync_point_v1_or_v2 - encoded using IDL
//   uint64_t - checksum computed using the xxHash algorithm
//
// Format V3 (encoded in base64):
//   uint8_t 0x03 - version of format
//   sync_point_v3 - encoded using IDL
//   uint64_t - checksum computed using the xxHash algorithm
//
// sync_point_v1_or_v2:
//   UUID host_id - ID of the host which created the sync point
//   uint16_t shard_count - the number of shards in this sync point
//   per_manager_sync_point_v1_or_v2 regular_sp - replay positions for regular mutation hint queues
//   per_manager_sync_point_v1_or_v2 mv_sp - replay positions for materialized view hint queues
//
// per_manager_sync_point_v1_or_v2:
//   std::vector<gms::inet_address> endpoints - addresses for which this sync point defines replay positions
//   std::vector<db::replay_position> flattened_rps:
//       A flattened collection of replay positions for all endpoints and shards.
//       Replay positions are grouped by address, in the same order as in
//       the `endpoints` field, and there is one replay position for each of
//       the shards (shard count is defined by the `shard_count`) field.
//       Flattened representation was chosen in order to save space on
//       vector lengths etc.
//
// sync_point_v3:
//   similar to sync_point_v1_or_v2 except it uses per_manager_sync_point_v3 instead
//   of per_manager_sync_point_v1_or_v2, which has locator::host_id instead of
//   gms::inet_address.

static constexpr size_t version_size = sizeof(uint8_t);
static constexpr size_t checksum_size = sizeof(uint64_t);

template <typename PerManagerType>
static std::vector<sync_point::shard_rps> decode_one_type(uint16_t shard_count, const PerManagerType& v) {
    std::vector<sync_point::shard_rps> ret;

    if (size_t(shard_count) * v.endpoints.size() != v.flattened_rps.size()) {
        throw std::runtime_error(format("Could not decode the sync point - there should be {} rps in flattened_rps, but there are only {}",
                size_t(shard_count) * v.endpoints.size(), v.flattened_rps.size()));
    }

    ret.resize(std::max(unsigned(shard_count), smp::count));

    auto rps_it = v.flattened_rps.begin();
    for (const auto ep : v.endpoints) {
        uint16_t shard;
        for (shard = 0; shard < shard_count; shard++) {
            ret[shard].emplace(ep, *rps_it++);
        }
        // Fill missing shards with zero replay positions so that segments
        // which were moved across shards will be correctly waited on
        for (; shard < smp::count; shard++) {
            ret[shard].emplace(ep, db::replay_position());
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
    if (version == 2 || version == 3) {
        if (raw_s.size() < version_size + checksum_size) {
            throw std::runtime_error("Could not decode the sync point encoded in the V2/V3 format - serialized blob is too short");
        }

        seastar::simple_memory_input_stream in_checksum{raw_s.end() - checksum_size, checksum_size};
        uint64_t checksum = ser::serializer<uint64_t>::read(in_checksum);
        if (checksum != calculate_checksum(raw_s.substr(0, raw_s.size() - checksum_size))) {
            throw std::runtime_error("Could not decode the sync point encoded in the V2/V3 format - wrong checksum");
        }
    }
    else if (version != 1) {
        throw std::runtime_error(format("Unsupported sync point format version: {}", int(version)));
    }

    if (version == 1 || version == 2) {
        sync_point_v1_or_v2 v = ser::serializer<sync_point_v1_or_v2>::read(in);

        return sync_point{
            v.host_id,
            decode_one_type(v.shard_count, v.regular_sp),
            decode_one_type(v.shard_count, v.mv_sp),
        };
    }

    // version == 3
    sync_point_v3 v3 = ser::serializer<sync_point_v3>::read(in);

    return sync_point{
        v3.host_id,
        decode_one_type(v3.shard_count, v3.regular_sp),
        decode_one_type(v3.shard_count, v3.mv_sp),
    };
}

static per_manager_sync_point_v3 encode_one_type_v3(unsigned shards, const std::vector<sync_point::shard_rps>& rps) {
    per_manager_sync_point_v3 ret;

    // Gather all endpoints, from all shards
    std::unordered_set<locator::host_id> all_eps;
    for (const auto& shard_rps : rps) {
        for (const auto& p : shard_rps) {
            // New sync points are created with host_id only
            all_eps.insert(std::get<locator::host_id>(p.first));
        }
    }

    ret.flattened_rps.reserve(size_t(shards) * all_eps.size());

    // Encode into v3 struct
    // For each endpoint, we encode a replay position for all shards.
    // If there is no replay position for a shard, we use a zero replay position.
    for (const auto ep : all_eps) {
        ret.endpoints.push_back(ep);
        for (const auto& shard_rps : rps) {
            auto it = shard_rps.find(ep);
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
    // Encode as v3 structure
    sync_point_v3 v3;
    v3.host_id = this->host_id;
    v3.shard_count = std::max(this->regular_per_shard_rps.size(), this->mv_per_shard_rps.size());
    v3.regular_sp = encode_one_type_v3(v3.shard_count, this->regular_per_shard_rps);
    v3.mv_sp = encode_one_type_v3(v3.shard_count, this->mv_per_shard_rps);

    // Measure how much space we need
    seastar::measuring_output_stream measure;
    ser::serializer<sync_point_v3>::write(measure, v3);

    // Reserve version_size bytes for the version and checksum_size bytes for the checksum
    bytes serialized{bytes::initialized_later{}, version_size + measure.size() + checksum_size};

    // Encode using V3 format
    seastar::simple_memory_output_stream out{reinterpret_cast<char*>(serialized.data()), serialized.size()};
    ser::serializer<uint8_t>::write(out, 3);
    ser::serializer<sync_point_v3>::write(out, v3);
    sstring_view serialized_s(reinterpret_cast<const char*>(serialized.data()), version_size + measure.size());
    uint64_t checksum = calculate_checksum(serialized_s);
    ser::serializer<uint64_t>::write(out, checksum);

    return base64_encode(serialized);
}

}
}

