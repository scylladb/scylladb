/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>
#include <stdexcept>

#include <seastar/core/on_internal_error.hh>
#include <seastar/core/shared_ptr.hh>
#include "utils/pluggable.hh"

BOOST_AUTO_TEST_SUITE(pluggable_test)

class test_service {
    int& _counter;
public:
    test_service(int& counter) : _counter(counter) { counter = 0; }

    void called() {
        ++_counter;
    }
};

SEASTAR_TEST_CASE(test_pluggable) {
    int counter;
    auto service = make_shared<test_service>(counter);
    utils::pluggable<test_service> plugin("test");

    auto check_unplugged = [&] {
        BOOST_REQUIRE(!plugin);
        BOOST_REQUIRE(!plugin.plugged());
  
        auto permit = plugin.get_permit();
        BOOST_REQUIRE(!permit);
        BOOST_REQUIRE_EQUAL(permit.get(), nullptr);
    };

    auto check_plugged = [&] {
        BOOST_REQUIRE(plugin);
        BOOST_REQUIRE(plugin.plugged());
  
        auto permit = plugin.get_permit();
        BOOST_REQUIRE(permit);
        BOOST_REQUIRE_EQUAL(permit.get(), service.get());
        auto prev = counter;
        permit->called();
        BOOST_REQUIRE_EQUAL(counter, prev + 1);
    };

    check_unplugged();
    co_await plugin.unplug();
    check_unplugged();

    plugin.plug(service);
    check_plugged();

    co_await plugin.unplug();
    check_unplugged();

    plugin.plug(service);
    check_plugged();

    co_await plugin.close();
    check_unplugged();

    set_abort_on_internal_error(false);
    BOOST_REQUIRE_THROW(plugin.plug(service), std::runtime_error);
}

BOOST_AUTO_TEST_SUITE_END()
