/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once
#include "vector_search/vector_store_client.hh"
#include <seastar/core/future.hh>
#include <seastar/net/inet_address.hh>
#include <optional>
#include <chrono>
#include <vector>

namespace test::vector_search {

class configure {
    std::reference_wrapper<::vector_search::vector_store_client> vs_ref;

public:
    explicit configure(::vector_search::vector_store_client& vs)
        : vs_ref(vs) {
        with_dns_refresh_interval(std::chrono::seconds(2));
        with_wait_for_client_timeout(std::chrono::milliseconds(100));
        with_dns_resolver([](auto const& host) -> seastar::future<std::optional<seastar::net::inet_address>> {
            co_return seastar::net::inet_address("127.0.0.1");
        });
    }

    configure& with_dns_refresh_interval(std::chrono::milliseconds interval) {
        ::vector_search::vector_store_client_tester::set_dns_refresh_interval(vs_ref.get(), interval);
        return *this;
    }

    configure& with_wait_for_client_timeout(std::chrono::milliseconds timeout) {
        ::vector_search::vector_store_client_tester::set_wait_for_client_timeout(vs_ref.get(), timeout);
        return *this;
    }

    configure& with_dns(std::map<std::string, std::optional<std::string>> dns_) {
        ::vector_search::vector_store_client_tester::set_dns_resolver(
                vs_ref.get(), [dns = std::move(dns_)](auto const& host) -> seastar::future<std::vector<seastar::net::inet_address>> {
                    auto value = dns.at(host);
                    if (value) {
                        co_return std::vector<seastar::net::inet_address>{seastar::net::inet_address(*value)};
                    }
                    co_return std::vector<seastar::net::inet_address>{};
                });
        return *this;
    }

    configure& with_dns(std::map<std::string, std::vector<std::string>> dns) {
        ::vector_search::vector_store_client_tester::set_dns_resolver(
                vs_ref.get(), [dns = std::move(dns)](auto const& host) -> seastar::future<std::vector<seastar::net::inet_address>> {
                    std::vector<seastar::net::inet_address> ret;
                    for (auto const& ip : dns.at(host)) {
                        ret.push_back(seastar::net::inet_address(ip));
                    }
                    co_return ret;
                });
        return *this;
    }

    configure& with_dns_resolver(std::function<seastar::future<std::optional<seastar::net::inet_address>>(sstring const&)> resolver) {
        ::vector_search::vector_store_client_tester::set_dns_resolver(
                vs_ref.get(), [r = std::move(resolver)](auto host) -> seastar::future<std::vector<seastar::net::inet_address>> {
                    auto addr = co_await r(host);
                    if (addr) {
                        co_return std::vector<seastar::net::inet_address>{*addr};
                    }
                    co_return std::vector<seastar::net::inet_address>{};
                });
        return *this;
    }
};

} // namespace test::vector_search
