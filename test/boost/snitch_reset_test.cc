/*
 * Copyright (C) 2015-present ScyllaDB
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
#include "utils/fb_utilities.hh"
#include <seastar/testing/test_case.hh>
#include <seastar/util/std-compat.hh>
#include <vector>
#include <string>
#include <tuple>

namespace fs = std::filesystem;

static fs::path test_files_subdir("test/resource/snitch_property_files");

future<> one_test(const std::string& property_fname1,
                  const std::string& property_fname2,
                  bool exp_result) {
    using namespace locator;
    using namespace std::filesystem;

    utils::fb_utilities::set_broadcast_address(gms::inet_address("localhost"));
    utils::fb_utilities::set_broadcast_rpc_address(gms::inet_address("localhost"));

    printf("Testing %s and %s property files. Expected result is %s\n",
           property_fname1.c_str(), property_fname2.c_str(),
           (exp_result ? "success" : "failure"));

    return seastar::async([&property_fname1, &property_fname2, exp_result] {
        auto cpu0_dc = make_lw_shared<sstring>();
        auto cpu0_rack = make_lw_shared<sstring>();
        auto cpu0_dc_new = make_lw_shared<sstring>();
        auto cpu0_rack_new = make_lw_shared<sstring>();
        auto my_address = utils::fb_utilities::get_broadcast_address();

        try {
            path fname1(test_files_subdir);
            fname1 /= path(property_fname1);

            path fname2(test_files_subdir);
            fname2 /= path(property_fname2);

            try {
                i_endpoint_snitch::create_snitch<const sstring&>(
                    "org.apache.cassandra.locator.GossipingPropertyFileSnitch",
                    sstring(fname1.string())
                ).get();
            } catch (std::exception& e) {
                printf("%s\n", e.what());
                BOOST_ERROR("Failed to create an initial snitch");
                return;
            }

            i_endpoint_snitch::snitch_instance().invoke_on(0,
                    [cpu0_dc, cpu0_rack, my_address] (snitch_ptr& inst) {
                *cpu0_dc =inst->get_datacenter(my_address);
                *cpu0_rack = inst->get_rack(my_address);
            }).get();

            i_endpoint_snitch::reset_snitch<const sstring&>(
                "org.apache.cassandra.locator.GossipingPropertyFileSnitch",
                sstring(fname2.string())
            ).get();

            if (!exp_result) {
                BOOST_ERROR("Failed to catch an error in a malformed "
                            "configuration file");
                i_endpoint_snitch::stop_snitch().get();
                return;
            }

            auto res = make_lw_shared<bool>(true);

            // Check that the returned DC and Rack values are different now
            i_endpoint_snitch::snitch_instance().invoke_on(0,
                    [cpu0_dc_new, cpu0_rack_new, my_address] (snitch_ptr& inst) {
                *cpu0_dc_new =inst->get_datacenter(my_address);
                *cpu0_rack_new = inst->get_rack(my_address);
            }).get();

            if (*cpu0_dc == *cpu0_dc_new || *cpu0_rack == *cpu0_rack_new) {
                BOOST_ERROR("Data center or Rack haven't been updated");
                i_endpoint_snitch::stop_snitch().get();
                return;
            }

            // Check that the new DC and Rack values have been propagated to all CPUs
            i_endpoint_snitch::snitch_instance().invoke_on_all(
                    [cpu0_dc_new, cpu0_rack_new, res, my_address] (snitch_ptr& inst) {
                if (*cpu0_dc_new != inst->get_datacenter(my_address) ||
                    *cpu0_rack_new != inst->get_rack(my_address)) {
                    *res = false;
                }
            }).get();

            if (!*res) {
                BOOST_ERROR("Data center or Rack do not match on "
                            "different shards");
            } else {
                BOOST_CHECK(true);
            }

            i_endpoint_snitch::stop_snitch().get();
        } catch (std::exception& e) {
            if (!exp_result) {
                //
                // Verify that the returned DC and Rack values remained the same
                // despite the insuccessful reset_snitch() call.
                //
                i_endpoint_snitch::snitch_instance().invoke_on(0,
                        [cpu0_dc_new, cpu0_rack_new, my_address] (snitch_ptr& inst) {
                    *cpu0_dc_new =inst->get_datacenter(my_address);
                    *cpu0_rack_new = inst->get_rack(my_address);
                }).get();

                if (*cpu0_dc != *cpu0_dc_new || *cpu0_rack != *cpu0_rack_new) {
                    BOOST_ERROR("Data center or Rack have been updated while they shouldn't have");
                    i_endpoint_snitch::stop_snitch().get();
                    return;
                }
            }

            BOOST_CHECK(!exp_result);
            i_endpoint_snitch::stop_snitch().get();
        }
    });
}

#define RESET_SNITCH_TEST_CASE(tag1, tag2, exp_res) \
SEASTAR_TEST_CASE(tag2) { \
    return one_test(#tag1".property", #tag2".property", exp_res); \
}

////////////////////////////////////////////////////////////////////////////////
RESET_SNITCH_TEST_CASE(good_2, bad_format_1,   false);
RESET_SNITCH_TEST_CASE(good_1, good_2,         true);

