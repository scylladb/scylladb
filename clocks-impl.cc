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

std::string format_timestamp(api::timestamp_type ts) {
    auto t = std::time_t(std::chrono::duration_cast<std::chrono::seconds>(api::timestamp_clock::duration(ts)).count());
    ::tm t_buf;
    ::gmtime_r(&t, &t_buf);
    return format("{%Y/%m/%d %T}", t_buf);
}
