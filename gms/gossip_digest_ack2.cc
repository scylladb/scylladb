/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "gms/gossip_digest_ack2.hh"

auto fmt::formatter<gms::gossip_digest_ack2>::format(const gms::gossip_digest_ack2& ack2, fmt::format_context& ctx) const
    -> decltype(ctx.out()) {
    auto out = ctx.out();
    out = fmt::format_to(out, "endpoint_state:{{");
    for (auto& [addr, state] : ack2.get_endpoint_state_map()) {
        out = fmt::format_to(out, "[{}->{}]", addr, state);
    }
    return fmt::format_to(out, "}}");
}
