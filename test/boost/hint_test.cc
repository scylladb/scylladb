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

#include <boost/test/unit_test.hpp>
#include <seastar/testing/test_case.hh>

#include "db/hints/sync_point.hh"

SEASTAR_TEST_CASE(test_hint_sync_point_faithful_reserialization) {
    const gms::inet_address addr1{"172.16.0.1"};
    const gms::inet_address addr2{"172.16.0.2"};

    const db::replay_position s0_rp1{0, 10, 100};
    const db::replay_position s0_rp2{0, 20, 200};
    const db::replay_position s1_rp1{1, 10, 100};
    const db::replay_position s1_rp2{1, 20, 200};

    db::hints::sync_point spoint;

    spoint.regular_per_shard_rps.resize(2);
    spoint.regular_per_shard_rps[0][addr1] = s0_rp1;
    spoint.regular_per_shard_rps[0][addr2] = s0_rp2;
    spoint.regular_per_shard_rps[1][addr1] = s1_rp1;

    spoint.mv_per_shard_rps.resize(2);
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

    std::cout << "spoint:  " << spoint << std::endl;
    std::cout << "encoded: " << encoded << std::endl;
    std::cout << "decoded: " << decoded_spoint << std::endl;

    BOOST_REQUIRE_EQUAL(spoint, decoded_spoint);

    return make_ready_future<>();
}
