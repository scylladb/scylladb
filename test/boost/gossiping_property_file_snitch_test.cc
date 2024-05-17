/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */


#include <boost/test/unit_test.hpp>
#include <fmt/ranges.h>
#include "test/lib/scylla_test_case.hh"
#include "test/lib/test_utils.hh"
#include <seastar/util/std-compat.hh>
#include <seastar/core/reactor.hh>
#include <string>
#include "locator/snitch_base.hh"
#include "gms/inet_address.hh"
#include "seastarx.hh"
#include "locator/production_snitch_base.hh"

static std::filesystem::path test_files_subdir("test/resource/snitch_property_files");

future<> one_test(const std::string& property_fname, bool exp_result) {
    using namespace locator;

    printf("Testing %s property file: %s\n",
           (exp_result ? "well-formed" : "ill-formed"),
           property_fname.c_str());

    engine().set_strict_dma(false);

    sharded<snitch_ptr> snitch;
    const auto my_address = gms::inet_address("localhost");
    co_await snitch.start(snitch_config {
        .name = "org.apache.cassandra.locator.GossipingPropertyFileSnitch",
        .properties_file_name = (std::filesystem::path(test_files_subdir) / property_fname).string(),
        .listen_address = my_address,
        .broadcast_address = my_address
    });

    auto start = [&]() {
        return snitch.invoke_on_all(&snitch_ptr::start);
    };
    if (exp_result) {
        BOOST_CHECK_NO_THROW(co_await start());

        std::vector<std::pair<sstring, sstring>> dc_racks(smp::count);
        co_await snitch.invoke_on_all([&] (snitch_ptr& inst) {
            dc_racks[this_shard_id()] = {inst->get_datacenter(), inst->get_rack()};
        });
        for (unsigned i = 1; i < smp::count; ++i) {
            BOOST_REQUIRE_EQUAL(dc_racks[i], dc_racks[0]);
        }
    } else {
        BOOST_CHECK_THROW(co_await start(), bad_property_file_error);
    }
    co_await snitch.stop();
}

#define GOSSIPING_TEST_CASE(tag, exp_res) \
SEASTAR_TEST_CASE(tag) { \
    return one_test(#tag".property", exp_res); \
}

////////////////////////////////////////////////////////////////////////////////
GOSSIPING_TEST_CASE(bad_double_dc,             false);
GOSSIPING_TEST_CASE(bad_double_rack,           false);
GOSSIPING_TEST_CASE(bad_double_prefer_local,   false);
GOSSIPING_TEST_CASE(bad_missing_dc,            false);
GOSSIPING_TEST_CASE(bad_missing_rack,          false);
GOSSIPING_TEST_CASE(good_missing_prefer_local, true);
GOSSIPING_TEST_CASE(bad_format_1,              false);
GOSSIPING_TEST_CASE(bad_format_2,              false);
GOSSIPING_TEST_CASE(bad_format_3,              false);
GOSSIPING_TEST_CASE(bad_format_4,              false);
GOSSIPING_TEST_CASE(bad_format_5,              false);
GOSSIPING_TEST_CASE(bad_format_6,              false);
GOSSIPING_TEST_CASE(good_1,                    true);
GOSSIPING_TEST_CASE(good_2,                    true);
