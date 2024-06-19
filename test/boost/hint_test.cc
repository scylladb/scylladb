/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <boost/test/unit_test.hpp>
#include "test/lib/scylla_test_case.hh"
#include <seastar/core/smp.hh>
#include <fmt/ranges.h>
#include <unordered_set>
#include <seastar/core/simple-stream.hh>
#include "utils/base64.hh"
#include "utils/xx_hasher.hh"
#include "idl/hinted_handoff.dist.hh"
#include "idl/hinted_handoff.dist.impl.hh"

#include "db/hints/sync_point.hh"

enum class encode_version {
    v1,
    v2,
};

namespace db {
std::ostream& operator<<(std::ostream& out, const replay_position& p) {
    fmt::print(out, "{}", p);
    return out;
}
}

template <>
struct fmt::formatter<db::hints::sync_point::host_id_or_addr> {
    constexpr static auto parse(format_parse_context& ctx) {
        return ctx.begin();
    }
    constexpr static auto format(const db::hints::sync_point::host_id_or_addr& value, fmt::format_context& ctx) {
        return std::visit([&ctx](const auto& v) {
            return fmt::format_to(ctx.out(), "{}", v);
        }, value);
    }
};

namespace db::hints {
std::ostream& operator<<(std::ostream& out, const sync_point& sp) {
    fmt::print(out, "{{regular_per_shard_rps: {}, mv_per_shard_rps: {}}}",
               sp.regular_per_shard_rps, sp.mv_per_shard_rps);
    return out;
}

// the code for v1 and v2 encoding is here for testing of decode only and it is
// based on the encoding code of sync_point, except that in v2 we have
// gms::inet_address instead of locator::host_id, and for v1 additionally we
// don't encode a checksum

static constexpr size_t version_size = sizeof(uint8_t);
static constexpr size_t checksum_size = sizeof(uint64_t);

static uint64_t calculate_checksum(const sstring_view s) {
    xx_hasher h;
    h.update(s.data(), s.size());
    return h.finalize_uint64();
}

static per_manager_sync_point_v1_or_v2 encode_one_type_v2(unsigned shards, const std::vector<sync_point::shard_rps>& rps) {
    per_manager_sync_point_v1_or_v2 ret;

    // Gather all addresses, from all shards
    std::unordered_set<gms::inet_address> all_eps;
    for (const auto& shard_rps : rps) {
        for (const auto& p : shard_rps) {
            all_eps.insert(std::get<gms::inet_address>(p.first));
        }
    }

    ret.flattened_rps.reserve(size_t(shards) * all_eps.size());

    // Encode into v3 struct
    // For each address, we encode a replay position for all shards.
    // If there is no replay position for a shard, we use a zero replay position.
    for (const auto addr : all_eps) {
        ret.endpoints.push_back(addr);
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

sstring encode_v1_or_v2(const sync_point& sp, encode_version v) {
    // Encode as v2 structure
    sync_point_v1_or_v2 v2;
    v2.host_id = sp.host_id;
    v2.shard_count = std::max(sp.regular_per_shard_rps.size(), sp.mv_per_shard_rps.size());
    v2.regular_sp = encode_one_type_v2(v2.shard_count, sp.regular_per_shard_rps);
    v2.mv_sp = encode_one_type_v2(v2.shard_count, sp.mv_per_shard_rps);

    // Measure how much space we need
    seastar::measuring_output_stream measure;
    ser::serializer<sync_point_v1_or_v2>::write(measure, v2);

    // Reserve version_size bytes for the version and checksum_size bytes for the checksum
    bytes serialized{bytes::initialized_later{}, version_size + measure.size()
        + (v == encode_version::v2 ? checksum_size : 0)};

    // Encode using V2 format
    seastar::simple_memory_output_stream out{reinterpret_cast<char*>(serialized.data()), serialized.size()};
    ser::serializer<uint8_t>::write(out, v == encode_version::v1 ? 1 : 2);
    ser::serializer<sync_point_v1_or_v2>::write(out, v2);

    if (v == encode_version::v2) {
        sstring_view serialized_s(reinterpret_cast<const char*>(serialized.data()), version_size + measure.size());
        uint64_t checksum = calculate_checksum(serialized_s);
        ser::serializer<uint64_t>::write(out, checksum);
    }

    return base64_encode(serialized);
}

}

SEASTAR_TEST_CASE(test_hint_sync_point_faithful_reserialization) {
    const unsigned encoded_shard_count = 2;

    const locator::host_id addr1 {utils::UUID(0, 1)};
    const locator::host_id addr2 {utils::UUID(0, 2)};

    const db::replay_position s0_rp1{0, 10, 100};
    const db::replay_position s0_rp2{0, 20, 200};
    const db::replay_position s1_rp1{1, 10, 100};
    const db::replay_position s1_rp2{1, 20, 200};

    db::hints::sync_point spoint;

    spoint.regular_per_shard_rps.resize(encoded_shard_count);
    spoint.regular_per_shard_rps[0][addr1] = s0_rp1;
    spoint.regular_per_shard_rps[0][addr2] = s0_rp2;
    spoint.regular_per_shard_rps[1][addr1] = s1_rp1;

    spoint.mv_per_shard_rps.resize(encoded_shard_count);
    spoint.mv_per_shard_rps[0][addr1] = s0_rp1;
    spoint.mv_per_shard_rps[1][addr1] = s1_rp1;
    spoint.mv_per_shard_rps[1][addr2] = s1_rp2;

    const sstring encoded = spoint.encode();
    const db::hints::sync_point decoded_spoint = db::hints::sync_point::decode(encoded);

    // If some shard is missing a replay position for a given address
    // then it will have a 0 position written there. Fill missing positions
    // with zeros in the original sync point.
    spoint.regular_per_shard_rps[1][addr2] = db::replay_position();
    spoint.mv_per_shard_rps[0][addr2] = db::replay_position();

    // If the sync point contains information about less shards than smp::count,
    // the missing shards are filled with zero. Do it here manually so that
    // we can compare spoint with decoded_spoint.
    const unsigned adjusted_count = std::max(encoded_shard_count, smp::count);
    spoint.regular_per_shard_rps.resize(adjusted_count);
    spoint.mv_per_shard_rps.resize(adjusted_count);
    for (unsigned s = encoded_shard_count; s < smp::count; s++) {
        spoint.regular_per_shard_rps[s][addr1] = db::replay_position();
        spoint.regular_per_shard_rps[s][addr2] = db::replay_position();
        spoint.mv_per_shard_rps[s][addr1] = db::replay_position();
        spoint.mv_per_shard_rps[s][addr2] = db::replay_position();
    }

    std::cout << "spoint:  " << spoint << std::endl;
    std::cout << "encoded: " << encoded << std::endl;
    std::cout << "decoded: " << decoded_spoint << std::endl;

    BOOST_REQUIRE_EQUAL(spoint, decoded_spoint);

    return make_ready_future<>();
}

static future<> test_decode_v1_or_v2(encode_version v)
{
    const unsigned encoded_shard_count = 2;

    const gms::inet_address addr1{"172.16.0.1"};
    const gms::inet_address addr2{"172.16.0.2"};

    const db::replay_position s0_rp1{0, 10, 100};
    const db::replay_position s0_rp2{0, 20, 200};
    const db::replay_position s1_rp1{1, 10, 100};
    const db::replay_position s1_rp2{1, 20, 200};

    db::hints::sync_point spoint;

    spoint.regular_per_shard_rps.resize(encoded_shard_count);
    spoint.regular_per_shard_rps[0][addr1] = s0_rp1;
    spoint.regular_per_shard_rps[0][addr2] = s0_rp2;
    spoint.regular_per_shard_rps[1][addr1] = s1_rp1;

    spoint.mv_per_shard_rps.resize(encoded_shard_count);
    spoint.mv_per_shard_rps[0][addr1] = s0_rp1;
    spoint.mv_per_shard_rps[1][addr1] = s1_rp1;
    spoint.mv_per_shard_rps[1][addr2] = s1_rp2;

    const sstring encoded = encode_v1_or_v2(spoint, v);
    const db::hints::sync_point decoded_spoint = db::hints::sync_point::decode(encoded);

    // If some shard is missing a replay position for a given address
    // then it will have a 0 position written there. Fill missing positions
    // with zeros in the original sync point.
    spoint.regular_per_shard_rps[1][addr2] = db::replay_position();
    spoint.mv_per_shard_rps[0][addr2] = db::replay_position();

    // If the sync point contains information about less shards than smp::count,
    // the missing shards are filled with zero. Do it here manually so that
    // we can compare spoint with decoded_spoint.
    const unsigned adjusted_count = std::max(encoded_shard_count, smp::count);
    spoint.regular_per_shard_rps.resize(adjusted_count);
    spoint.mv_per_shard_rps.resize(adjusted_count);
    for (unsigned s = encoded_shard_count; s < smp::count; s++) {
        spoint.regular_per_shard_rps[s][addr1] = db::replay_position();
        spoint.regular_per_shard_rps[s][addr2] = db::replay_position();
        spoint.mv_per_shard_rps[s][addr1] = db::replay_position();
        spoint.mv_per_shard_rps[s][addr2] = db::replay_position();
    }

    std::cout << "spoint:  " << spoint << std::endl;
    std::cout << "encoded: " << encoded << std::endl;
    std::cout << "decoded: " << decoded_spoint << std::endl;

    BOOST_REQUIRE_EQUAL(spoint, decoded_spoint);

    return make_ready_future<>();
}

SEASTAR_TEST_CASE(test_hint_sync_point_faithful_reserialization_v2) {
    return test_decode_v1_or_v2(encode_version::v2);
};

SEASTAR_TEST_CASE(test_hint_sync_point_faithful_reserialization_v1) {
    return test_decode_v1_or_v2(encode_version::v1);
};
