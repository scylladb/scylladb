/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "seastar/core/shard_id.hh"
#include <boost/test/unit_test.hpp>
#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>

#include "test/lib/cql_test_env.hh"
#include "test/lib/eventually.hh"
#include "test/lib/log.hh"

#include "db/config.hh"

BOOST_AUTO_TEST_SUITE(streaming_controller_test)

SEASTAR_THREAD_TEST_CASE(test_streaming_controller_shares) {
    auto shared_config = make_shared<db::config>();
    float initial_shares = 300;
    float adjusted_shares = 500;
    shared_config->streaming_min_shares.set(float(initial_shares));
    do_with_cql_env_thread([&] (auto& e) {
        auto sg = get_scheduling_groups().get().streaming_scheduling_group;

        std::function<float()> get_shares = [&] () -> float {
            std::vector<float> shares;
            shares.resize(smp::count);
            smp::invoke_on_all([&] () {
                shares[this_shard_id()] = sg.get_shares();
            }).get();
            for (size_t i = 1; i < smp::count; ++i) {
                if (shares[i] != shares[0]) {
                    testlog.debug("shard[{}] shares={} is not equal to shard[0] shares={}", i, shares[i], shares[0]);
                    return -1;
                }
            }
            return shares[0];
        };

        // Test that streaming shares are configure based
        // on the streaming_min_shares in db::config
        BOOST_REQUIRE_EQUAL(get_shares(), initial_shares);

        // Test that modifying streaming_min_shares in db::config
        // propagates and applied by the streaming controller.
        shared_config->streaming_min_shares.set(float(adjusted_shares));
        REQUIRE_EVENTUALLY_EQUAL(get_shares, adjusted_shares);
    }, cql_test_config(shared_config)).get();
}

BOOST_AUTO_TEST_SUITE_END()
