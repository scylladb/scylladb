/*
 * Copyright (C) 2015-present ScyllaDB
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

#include <seastar/core/print.hh>

#include "db_clock.hh"
#include "timestamp.hh"

#include "clocks-impl.hh"

std::atomic<int64_t> clocks_offset;

std::ostream& operator<<(std::ostream& os, db_clock::time_point tp) {
    auto t = db_clock::to_time_t(tp);
    ::tm t_buf;
    return os << std::put_time(::gmtime_r(&t, &t_buf), "%Y/%m/%d %T");
}

std::string format_timestamp(api::timestamp_type ts) {
    auto t = std::time_t(std::chrono::duration_cast<std::chrono::seconds>(api::timestamp_clock::duration(ts)).count());
    ::tm t_buf;
    return format("{}", std::put_time(::gmtime_r(&t, &t_buf), "%Y/%m/%d %T"));
}
