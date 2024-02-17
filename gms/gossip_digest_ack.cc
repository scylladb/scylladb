/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "gms/gossip_digest_ack.hh"

auto fmt::formatter<gms::gossip_digest_ack>::format(const gms::gossip_digest_ack& ack, fmt::format_context& ctx) const
        -> decltype(ctx.out()) {
    auto out = ctx.out();
    out = fmt::format_to(out, "digests:{{");
    for (auto& d : ack._digests) {
        out = fmt::format_to(out, "{} ", d);
    }
    out = fmt::format_to(out, "}} ");
    out = fmt::format_to(out, "endpoint_state:{{");
    for (auto& d : ack._map) {
        out = fmt::format_to(out, "[{}->{}]", d.first, d.second);
    }
    return fmt::format_to(out, "}}");
}
