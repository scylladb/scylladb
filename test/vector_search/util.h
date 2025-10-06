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
#include <functional>

namespace test::vector_search {

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
