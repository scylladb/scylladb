/*
 * Copyright (C) 2020-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "timeout_config.hh"
#include "db/config.hh"
#include <chrono>
#include <seastar/core/future.hh>

using namespace std::chrono_literals;

updateable_timeout_config::updateable_timeout_config(const db::config& cfg)
    : read_timeout_in_ms(cfg.read_request_timeout_in_ms)
    , write_timeout_in_ms(cfg.write_request_timeout_in_ms)
    , range_read_timeout_in_ms(cfg.range_request_timeout_in_ms)
    , counter_write_timeout_in_ms(cfg.counter_write_request_timeout_in_ms)
    , truncate_timeout_in_ms(cfg.truncate_request_timeout_in_ms)
    , cas_timeout_in_ms(cfg.cas_contention_timeout_in_ms)
    , other_timeout_in_ms(cfg.request_timeout_in_ms)
{}

timeout_config updateable_timeout_config::current_values() const {
    return {
        std::chrono::milliseconds(read_timeout_in_ms),
        std::chrono::milliseconds(write_timeout_in_ms),
        std::chrono::milliseconds(range_read_timeout_in_ms),
        std::chrono::milliseconds(counter_write_timeout_in_ms),
        std::chrono::milliseconds(truncate_timeout_in_ms),
        std::chrono::milliseconds(cas_timeout_in_ms),
        std::chrono::milliseconds(other_timeout_in_ms),
    };
}
