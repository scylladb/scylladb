/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <fmt/chrono.h>

#include "timestamp.hh"

#include "clocks-impl.hh"

std::atomic<int64_t> clocks_offset;

std::string format_timestamp(api::timestamp_type ts) {
    std::chrono::system_clock::time_point when{api::timestamp_clock::duration(ts)};
    return fmt::format("{:%Y/%m/%d %T}", when);
}
