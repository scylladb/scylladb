/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "streaming/stream_summary.hh"

auto fmt::formatter<streaming::stream_summary>::format(const streaming::stream_summary& x, fmt::format_context& ctx) const
        -> decltype(ctx.out()) {
    return fmt::format_to(ctx.out(), "[ cf_id={} ]", x.cf_id);

}
