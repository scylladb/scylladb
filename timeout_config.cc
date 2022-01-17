/*
 * Copyright (C) 2020-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "timeout_config.hh"
#include "db/config.hh"

using namespace std::chrono_literals;

timeout_config make_timeout_config(const db::config& cfg) {
    timeout_config tc;
    tc.read_timeout = cfg.read_request_timeout_in_ms() * 1ms;
    tc.write_timeout = cfg.write_request_timeout_in_ms() * 1ms;
    tc.range_read_timeout = cfg.range_request_timeout_in_ms() * 1ms;
    tc.counter_write_timeout = cfg.counter_write_request_timeout_in_ms() * 1ms;
    tc.truncate_timeout = cfg.truncate_request_timeout_in_ms() * 1ms;
    tc.cas_timeout = cfg.cas_contention_timeout_in_ms() * 1ms;
    tc.other_timeout = cfg.request_timeout_in_ms() * 1ms;
    return tc;
}
