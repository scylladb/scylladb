/*
 * Copyright (C) 2020-present ScyllaDB
 *
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
