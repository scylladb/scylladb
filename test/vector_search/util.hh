/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/abort_source.hh>
#include <seastar/core/timer.hh>
#include <functional>
#include <chrono>

namespace test::vector_search {

constexpr auto STANDARD_WAIT = std::chrono::seconds(10);

class abort_source_timeout {
    seastar::abort_source as;
    seastar::timer<> t;

public:
    explicit abort_source_timeout(std::chrono::milliseconds timeout = STANDARD_WAIT)
        : t(seastar::timer([&]() {
            as.request_abort();
        })) {
        t.arm(timeout);
    }

    seastar::abort_source& reset(std::chrono::milliseconds timeout = STANDARD_WAIT) {
        t.cancel();
        as = seastar::abort_source();
        t.arm(timeout);
        return as;
    }
};

inline seastar::future<> try_on_loopback_address(std::function<seastar::future<>(seastar::sstring)> func) {
    constexpr size_t MAX_LOCALHOST_ADDR_TO_TRY = 127;
    for (size_t i = 1; i < MAX_LOCALHOST_ADDR_TO_TRY; i++) {
        auto host = fmt::format("127.0.0.{}", i);
        try {
            co_await func(std::move(host));
            co_return;
        } catch (...) {
        }
    }
    throw std::runtime_error("unable to perform action on any 127.0.0.x address");
}


} // namespace test::vector_search
