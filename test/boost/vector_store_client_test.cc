/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "service/vector_store_client.hh"
#include "db/config.hh"
#include "exceptions/exceptions.hh"
#include <seastar/core/shared_ptr.hh>
#include <seastar/net/api.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/short_streams.hh>


namespace {

using namespace seastar;

using vector_store_client = service::vector_store_client;
using vector_store_client_tester = service::vector_store_client_tester;
using config = vector_store_client::config;
using configuration_exception = exceptions::configuration_exception;
using inet_address = seastar::net::inet_address;
using milliseconds = std::chrono::milliseconds;
using port_number = vector_store_client::port_number;

auto repeat_until(milliseconds timeout, std::function<future<bool>()> func) -> future<bool> {
    auto begin = lowres_clock::now();
    while (!co_await func()) {
        if (lowres_clock::now() - begin > timeout) {
            co_return false;
        }
    }
    co_return true;
}

auto print_addr(const inet_address& addr) -> sstring {
    return format("{}", addr);
}

} // namespace

BOOST_AUTO_TEST_CASE(vector_store_client_test_ctor) {
    {
        auto cfg = config();
        auto vs = vector_store_client{cfg};
        BOOST_CHECK(vs.is_disabled());
        BOOST_CHECK(!vs.host());
        BOOST_CHECK(!vs.port());
    }
    {
        auto cfg = config();
        cfg.vector_store_uri.set("http://good.authority.com:6080");
        auto vs = vector_store_client{cfg};
        BOOST_CHECK(!vs.is_disabled());
        BOOST_CHECK_EQUAL(*vs.host(), "good.authority.com");
        BOOST_CHECK_EQUAL(*vs.port(), 6080);
    }
    {
        auto cfg = config();
        cfg.vector_store_uri.set("http://bad,authority.com:6080");
        BOOST_CHECK_THROW(vector_store_client{cfg}, configuration_exception);
        cfg.vector_store_uri.set("bad-schema://authority.com:6080");
        BOOST_CHECK_THROW(vector_store_client{cfg}, configuration_exception);
        cfg.vector_store_uri.set("http://bad.port.com:a6080");
        BOOST_CHECK_THROW(vector_store_client{cfg}, configuration_exception);
        cfg.vector_store_uri.set("http://bad.port.com:60806080");
        BOOST_CHECK_THROW(vector_store_client{cfg}, configuration_exception);
        cfg.vector_store_uri.set("http://bad.format.com:60:80");
        BOOST_CHECK_THROW(vector_store_client{cfg}, configuration_exception);
        cfg.vector_store_uri.set("http://authority.com:6080/bad/path");
        BOOST_CHECK_THROW(vector_store_client{cfg}, configuration_exception);
    }
}

/// Resolving of the hostname is started in start_background_tasks()
SEASTAR_TEST_CASE(vector_store_client_test_dns_started) {
    auto cfg = config();
    cfg.vector_store_uri.set("http://good.authority.here:6080");

    auto vs = vector_store_client{cfg};
    BOOST_CHECK(!vs.is_disabled());

    vector_store_client_tester::set_dns_refresh_interval(vs, std::chrono::milliseconds(2000));
    vector_store_client_tester::set_wait_for_client_timeout(vs, std::chrono::milliseconds(100));
    vector_store_client_tester::set_dns_resolver(vs, [](auto const& host) -> future<std::optional<inet_address>> {
        BOOST_CHECK_EQUAL(host, "good.authority.here");
        co_return inet_address("127.0.0.1");
    });

    vs.start_background_tasks();

    auto as = abort_source();
    auto addr = co_await vector_store_client_tester::resolve_hostname(vs, as);
    BOOST_REQUIRE(addr);
    BOOST_CHECK_EQUAL(print_addr(*addr), "127.0.0.1");

    co_await vs.stop();
}

/// Unable to resolve the hostname
SEASTAR_TEST_CASE(vector_store_client_test_dns_resolve_failure) {
    auto cfg = config();
    cfg.vector_store_uri.set("http://good.authority.here:6080");


    auto vs = vector_store_client{cfg};
    BOOST_CHECK(!vs.is_disabled());

    vector_store_client_tester::set_dns_refresh_interval(vs, std::chrono::milliseconds(2000));
    vector_store_client_tester::set_wait_for_client_timeout(vs, std::chrono::milliseconds(100));
    vector_store_client_tester::set_dns_resolver(vs, [](auto const& host) -> future<std::optional<inet_address>> {
        BOOST_CHECK_EQUAL(host, "good.authority.here");
        co_return std::nullopt;
    });

    vs.start_background_tasks();

    auto as = abort_source();
    BOOST_CHECK(!co_await vector_store_client_tester::resolve_hostname(vs, as));

    co_await vs.stop();
}

/// Resolving of the hostname is repeated after errors
SEASTAR_TEST_CASE(vector_store_client_test_dns_resolving_repeated) {
    auto cfg = config();
    cfg.vector_store_uri.set("http://good.authority.here:6080");
    auto vs = vector_store_client{cfg};
    BOOST_CHECK(!vs.is_disabled());

    vector_store_client_tester::set_dns_refresh_interval(vs, std::chrono::milliseconds(10));
    vector_store_client_tester::set_wait_for_client_timeout(vs, std::chrono::milliseconds(20));
    auto count = 0;
    vector_store_client_tester::set_dns_resolver(vs, [&count](auto const& host) -> future<std::optional<inet_address>> {
        BOOST_CHECK_EQUAL(host, "good.authority.here");
        count++;
        if (count % 3 != 0) {
            co_return std::nullopt;
        }
        co_return inet_address(format("127.0.0.{}", count));
    });

    vs.start_background_tasks();

    auto as = abort_source();
    BOOST_CHECK(co_await repeat_until(std::chrono::milliseconds(1000), [&vs, &as]() -> future<bool> {
        co_return co_await vector_store_client_tester::resolve_hostname(vs, as);
    }));
    BOOST_CHECK_EQUAL(count, 3);
    auto addr = co_await vector_store_client_tester::resolve_hostname(vs, as);
    BOOST_REQUIRE(addr);
    BOOST_CHECK_EQUAL(print_addr(*addr), "127.0.0.3");

    vector_store_client_tester::trigger_dns_resolver(vs);

    BOOST_CHECK(co_await repeat_until(std::chrono::milliseconds(1000), [&vs, &as]() -> future<bool> {
        co_return !co_await vector_store_client_tester::resolve_hostname(vs, as);
    }));

    BOOST_CHECK(co_await repeat_until(std::chrono::milliseconds(1000), [&vs, &as]() -> future<bool> {
        co_return co_await vector_store_client_tester::resolve_hostname(vs, as);
    }));
    BOOST_CHECK_EQUAL(count, 6);
    addr = co_await vector_store_client_tester::resolve_hostname(vs, as);
    BOOST_REQUIRE(addr);
    BOOST_CHECK_EQUAL(print_addr(*addr), "127.0.0.6");

    co_await vs.stop();
}

/// Minimal interval between DNS refreshes is respected
SEASTAR_TEST_CASE(vector_store_client_test_dns_refresh_respects_interval) {
    auto cfg = config();
    cfg.vector_store_uri.set("http://good.authority.here:6080");
    auto vs = vector_store_client{cfg};
    BOOST_CHECK(!vs.is_disabled());

    vector_store_client_tester::set_dns_refresh_interval(vs, std::chrono::milliseconds(10));
    vector_store_client_tester::set_wait_for_client_timeout(vs, std::chrono::milliseconds(100));
    auto count = 0;
    vector_store_client_tester::set_dns_resolver(vs, [&count](auto const& host) -> future<std::optional<inet_address>> {
        BOOST_CHECK_EQUAL(host, "good.authority.here");
        count++;
        co_return inet_address("127.0.0.1");
    });

    vs.start_background_tasks();
    co_await sleep(std::chrono::milliseconds(20)); // wait for the first DNS refresh

    auto as = abort_source();
    auto addr = co_await vector_store_client_tester::resolve_hostname(vs, as);
    BOOST_REQUIRE(addr);
    BOOST_CHECK_EQUAL(print_addr(*addr), "127.0.0.1");
    BOOST_CHECK_EQUAL(count, 1);
    count = 0;
    vector_store_client_tester::trigger_dns_resolver(vs);
    vector_store_client_tester::trigger_dns_resolver(vs);
    vector_store_client_tester::trigger_dns_resolver(vs);
    vector_store_client_tester::trigger_dns_resolver(vs);
    vector_store_client_tester::trigger_dns_resolver(vs);
    co_await sleep(std::chrono::milliseconds(100)); // wait for the next DNS refresh

    addr = co_await vector_store_client_tester::resolve_hostname(vs, as);
    BOOST_REQUIRE(addr);
    BOOST_CHECK_EQUAL(print_addr(*addr), "127.0.0.1");
    BOOST_CHECK_GE(count, 1);
    BOOST_CHECK_LE(count, 2);

    co_await vs.stop();
}

/// DNS refresh could be aborted
SEASTAR_TEST_CASE(vector_store_client_test_dns_refresh_aborted) {
    auto cfg = config();
    cfg.vector_store_uri.set("http://good.authority.here:6080");
    auto vs = vector_store_client{cfg};
    BOOST_CHECK(!vs.is_disabled());

    vector_store_client_tester::set_dns_refresh_interval(vs, std::chrono::milliseconds(10));
    vector_store_client_tester::set_wait_for_client_timeout(vs, std::chrono::milliseconds(100));
    vector_store_client_tester::set_dns_resolver(vs, [](auto const& host) -> future<std::optional<inet_address>> {
        BOOST_CHECK_EQUAL(host, "good.authority.here");
        co_await sleep(std::chrono::milliseconds(100));
        co_return inet_address("127.0.0.1");
    });

    vs.start_background_tasks();

    auto as = abort_source();
    auto timeout = timer([&as]() {
        as.request_abort();
    });
    timeout.arm(std::chrono::milliseconds(10));
    auto addr = co_await vector_store_client_tester::resolve_hostname(vs, as);
    BOOST_CHECK(!addr);

    co_await vs.stop();
}

