/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
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
