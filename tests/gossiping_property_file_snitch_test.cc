/*
 * Copyright (C) 2015 ScyllaDB
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
#include "locator/gossiping_property_file_snitch.hh"
#include "tests/test-utils.hh"
#include <boost/filesystem.hpp>
#include <vector>
#include <string>
#include <tuple>

static boost::filesystem::path test_files_subdir("tests/snitch_property_files");

future<> one_test(const std::string& property_fname, bool exp_result) {
    using namespace locator;
    using namespace boost::filesystem;

    printf("Testing %s property file: %s\n",
           (exp_result ? "well-formed" : "ill-formed"),
           property_fname.c_str());

    path fname(test_files_subdir);
    fname /= path(property_fname);

    utils::fb_utilities::set_broadcast_address(gms::inet_address("localhost"));
    utils::fb_utilities::set_broadcast_rpc_address(gms::inet_address("localhost"));

    engine().set_strict_dma(false);

    return i_endpoint_snitch::create_snitch<const sstring&>(
        "org.apache.cassandra.locator.GossipingPropertyFileSnitch",
        sstring(fname.string()))
        .then_wrapped([exp_result] (auto&& f) {
            try {
                f.get();
                if (!exp_result) {
                    BOOST_ERROR("Failed to catch an error in a malformed "
                                "configuration file");
                    return i_endpoint_snitch::stop_snitch();
                }
                auto cpu0_dc = make_lw_shared<sstring>();
                auto cpu0_rack = make_lw_shared<sstring>();
                auto res = make_lw_shared<bool>(true);
                auto my_address = utils::fb_utilities::get_broadcast_address();

                return i_endpoint_snitch::snitch_instance().invoke_on(0,
                        [cpu0_dc, cpu0_rack,
                         res, my_address] (snitch_ptr& inst) {
                    *cpu0_dc =inst->get_datacenter(my_address);
                    *cpu0_rack = inst->get_rack(my_address);
                }).then([cpu0_dc, cpu0_rack, res, my_address] {
                    return i_endpoint_snitch::snitch_instance().invoke_on_all(
                            [cpu0_dc, cpu0_rack,
                             res, my_address] (snitch_ptr& inst) {
                        if (*cpu0_dc != inst->get_datacenter(my_address) ||
                            *cpu0_rack != inst->get_rack(my_address)) {
                            *res = false;
                        }
                    }).then([res] {
                        if (!*res) {
                            BOOST_ERROR("Data center or Rack do not match on "
                                        "different shards");
                        } else {
                            BOOST_CHECK(true);
                        }
                        return i_endpoint_snitch::stop_snitch();
                    });
                });
            } catch (std::exception& e) {
                BOOST_CHECK(!exp_result);
                return make_ready_future<>();
            }
        });
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
