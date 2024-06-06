/*
 * Copyright (C) 2019-present ScyllaDB
 *
 * Modified by ScyllaDB
 */
/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */
#pragma once

#include "mutation/frozen_mutation.hh"
#include <fmt/core.h>

namespace service {

namespace paxos {

// Proposal represents replica's value associated with a given ballot. The origin uses the term
// "commit" for this object, however, Scylla follows the terminology as set by Paxos Made Simple
// paper.
// Each replica persists the proposals it receives in the system.paxos table. A proposal may be
// new, accepted by a replica, or accepted by a majority. When a proposal is accepted by majority it
// is considered "chosen" by Paxos, and we call such a proposal "decision". A decision is
// saved in the paxos table in an own column and applied to the base table during "learn" phase of
// the protocol. After a decision is applied it is considered "committed".
class proposal {
public:
    // The ballot for the update.
    utils::UUID ballot;
    // The mutation representing the update that is being applied.
    frozen_mutation update;

    proposal(utils::UUID ballot_arg, frozen_mutation update_arg)
        : ballot(ballot_arg)
        , update(std::move(update_arg)) {}
};

// Proposals are ordered by their ballot's timestamp.
// A proposer uses it to find the newest proposal accepted
// by some replica among the responses to its own one.
inline bool operator<(const proposal& lhs, const proposal& rhs) {
    return lhs.ballot.timestamp() < rhs.ballot.timestamp();
}

inline bool operator>(const proposal& lhs, const proposal& rhs) {
    return lhs.ballot.timestamp() > rhs.ballot.timestamp();
}

} // end of namespace "paxos"
} // end of namespace "service"

// Used for logging and debugging.
template <> struct fmt::formatter<service::paxos::proposal> : fmt::formatter<string_view> {
    auto format(const service::paxos::proposal&, fmt::format_context& ctx) const -> decltype(ctx.out());
};
