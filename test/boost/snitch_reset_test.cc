/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */


#include <filesystem>
#include <string>
#include <boost/test/unit_test.hpp>
#include "locator/snitch_base.hh"
#include "gms/inet_address.hh"
#include "test/lib/scylla_test_case.hh"
#include "seastarx.hh"

namespace fs = std::filesystem;

static fs::path test_files_subdir("test/resource/snitch_property_files");

future<> one_test(const std::string& property_fname1,
                  const std::string& property_fname2,
                  bool exp_result) {
    using namespace locator;
    using namespace std::filesystem;

    printf("Testing %s and %s property files. Expected result is %s\n",
           property_fname1.c_str(), property_fname2.c_str(),
           (exp_result ? "success" : "failure"));

    return seastar::async([&property_fname1, &property_fname2, exp_result] {
        auto cpu0_dc = make_lw_shared<sstring>();
        auto cpu0_rack = make_lw_shared<sstring>();
        auto cpu0_dc_new = make_lw_shared<sstring>();
        auto cpu0_rack_new = make_lw_shared<sstring>();
        sharded<snitch_ptr> snitch;
        auto my_address = gms::inet_address("localhost");

        try {
            path fname1(test_files_subdir);
            fname1 /= path(property_fname1);

            path fname2(test_files_subdir);
            fname2 /= path(property_fname2);

            try {
                snitch_config cfg;
                cfg.name = "org.apache.cassandra.locator.GossipingPropertyFileSnitch";
                cfg.properties_file_name = fname1.string();
                cfg.listen_address = my_address;
                cfg.broadcast_address = my_address;
                snitch.start(cfg).get();
                snitch.invoke_on_all(&snitch_ptr::start).get();
            } catch (std::exception& e) {
                printf("%s\n", e.what());
                BOOST_ERROR("Failed to create an initial snitch");
                return;
            }

            snitch.invoke_on(0,
                    [cpu0_dc, cpu0_rack] (snitch_ptr& inst) {
                *cpu0_dc =inst->get_datacenter();
                *cpu0_rack = inst->get_rack();
            }).get();

            snitch_config cfg;
            cfg.name = "org.apache.cassandra.locator.GossipingPropertyFileSnitch";
            cfg.properties_file_name = fname2.string();
            i_endpoint_snitch::reset_snitch(snitch, cfg).get();

            if (!exp_result) {
                BOOST_ERROR("Failed to catch an error in a malformed "
                            "configuration file");
                snitch.stop().get();
                return;
            }

            auto res = make_lw_shared<bool>(true);

            // Check that the returned DC and Rack values are different now
            snitch.invoke_on(0,
                    [cpu0_dc_new, cpu0_rack_new] (snitch_ptr& inst) {
                *cpu0_dc_new =inst->get_datacenter();
                *cpu0_rack_new = inst->get_rack();
            }).get();

            if (*cpu0_dc == *cpu0_dc_new || *cpu0_rack == *cpu0_rack_new) {
                BOOST_ERROR("Data center or Rack haven't been updated");
                snitch.stop().get();
                return;
            }

            // Check that the new DC and Rack values have been propagated to all CPUs
            snitch.invoke_on_all(
                    [cpu0_dc_new, cpu0_rack_new, res] (snitch_ptr& inst) {
                if (*cpu0_dc_new != inst->get_datacenter() ||
                    *cpu0_rack_new != inst->get_rack()) {
                    *res = false;
                }
            }).get();

            if (!*res) {
                BOOST_ERROR("Data center or Rack do not match on "
                            "different shards");
            } else {
                BOOST_CHECK(true);
            }

            snitch.stop().get();
        } catch (std::exception& e) {
            if (!exp_result) {
                //
                // Verify that the returned DC and Rack values remained the same
                // despite the insuccessful reset_snitch() call.
                //
                snitch.invoke_on(0,
                        [cpu0_dc_new, cpu0_rack_new] (snitch_ptr& inst) {
                    *cpu0_dc_new =inst->get_datacenter();
                    *cpu0_rack_new = inst->get_rack();
                }).get();

                if (*cpu0_dc != *cpu0_dc_new || *cpu0_rack != *cpu0_rack_new) {
                    BOOST_ERROR("Data center or Rack have been updated while they shouldn't have");
                    snitch.stop().get();
                    return;
                }
            }

            BOOST_CHECK(!exp_result);
            snitch.stop().get();
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

