/*
 * Copyright 2015 Cloudius Systems
 */

#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>
#include "locator/gossiping_property_file_snitch.hh"
#include "tests/test-utils.hh"
#include <boost/filesystem.hpp>
#include <vector>
#include <string>
#include <tuple>

static boost::filesystem::path test_files_subdir("tests/urchin/snitch_property_files");

future<> one_test(const std::string& property_fname, bool exp_result) {
    using namespace locator;
    using namespace boost::filesystem;

    printf("Testing %s property file: %s\n",
           (exp_result ? "well-formed" : "ill-formed"),
           property_fname.c_str());

    path fname(test_files_subdir);
    fname /= path(property_fname);

    return make_snitch<gossiping_property_file_snitch>(sstring(fname.string()))
        .then([exp_result] (snitch_ptr sptr) {
            BOOST_CHECK(exp_result);
            return sptr->stop();
        }).then_wrapped([exp_result] (auto&& f) {
            try {
                f.get();
                BOOST_CHECK(exp_result);
                return make_ready_future<>();
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
