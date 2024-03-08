/*
 * Copyright (C) 2019-present ScyllaDB
 *
 * Modified by ScyllaDB
 */
/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "prepare_response.hh"

auto fmt::formatter<service::paxos::promise>::format(const service::paxos::promise& promise,
                                                     fmt::format_context& ctx) const -> decltype(ctx.out()) {
    auto out = fmt::format_to(ctx.out(), "prepare_promise(");
    if (promise.most_recent_commit) {
        out = fmt::format_to(out, "{}", *promise.most_recent_commit);
    } else {
        out = fmt::format_to(out, "empty");
    }
    out = fmt::format_to(out, ", ");
    if (promise.accepted_proposal) {
        out = fmt::format_to(out, "{}", *promise.accepted_proposal);
    } else {
        out = fmt::format_to(out, "empty");
    }
    return fmt::format_to(out, ")");
}
