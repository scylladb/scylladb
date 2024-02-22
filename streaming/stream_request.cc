/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "streaming/stream_request.hh"

auto fmt::formatter<streaming::stream_request>::format(const streaming::stream_request& sr, fmt::format_context& ctx) const
        -> decltype(ctx.out()) {
    return fmt::format_to(ctx.out(), "[ ks = {} cf = {} ]", sr.keyspace, fmt::join(sr.column_families, " "));
}
