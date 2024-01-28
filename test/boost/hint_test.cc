/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <boost/test/unit_test.hpp>
#include "test/lib/scylla_test_case.hh"
#include <seastar/core/smp.hh>

#include "db/hints/sync_point.hh"

namespace db {
std::ostream& operator<<(std::ostream& out, const replay_position& p) {
    fmt::print(out, "{}", p);
    return out;
}
}

namespace db::hints {
std::ostream& operator<<(std::ostream& out, const sync_point& sp) {
    out << "{regular_per_shard_rps: " << sp.regular_per_shard_rps
        << ", mv_per_shard_rps: " << sp.mv_per_shard_rps
        << "}";
    return out;
}
}

SEASTAR_TEST_CASE(test_hint_sync_point_faithful_reserialization) {
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
