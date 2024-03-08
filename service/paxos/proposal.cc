/*
 * Copyright (C) 2019-present ScyllaDB
 *
 * Modified by ScyllaDB
 */
/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "proposal.hh"

auto fmt::formatter<service::paxos::proposal>::format(const service::paxos::proposal& proposal,
                                                      fmt::format_context& ctx) const -> decltype(ctx.out()) {
    return fmt::format_to(ctx.out(), "proposal({})", proposal.ballot);
}
