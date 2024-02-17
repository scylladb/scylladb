/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "gms/gossip_digest_syn.hh"

auto fmt::formatter<gms::gossip_digest_syn>::format(const gms::gossip_digest_syn& syn, fmt::format_context& ctx) const
        -> decltype(ctx.out()) {
    auto out = ctx.out();
    // out = fmt::format_to(out, "cluster_id:{},partioner:{},group0_id{},"
    //                      syn._cluster_id, syn._partioner, syn._group0_id);
    out = fmt::format_to(out, "digests:{{");
    for (auto& d : syn._digests) {
        out = fmt::format_to(out, "{} ", d);
    }
    return fmt::format_to(out, "}}");
}
