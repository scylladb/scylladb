/*
 * Copyright (C) 2021-present ScyllaDB
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

#pragma once

#include <seastar/core/lowres_clock.hh>

namespace netw {

struct last_seen_info {
    using clock_type = seastar::lowres_clock;
    static constexpr clock_type::time_point unknown = clock_type::time_point::min();
    clock_type::time_point last_sent_without_response = unknown;
    clock_type::time_point last_attempt = unknown;

    void mark_sent(clock_type::time_point tp);
    void mark_got_response();
};

}