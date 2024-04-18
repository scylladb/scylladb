/*
 * Copyright (C) 2019-present ScyllaDB
 *
 * Modified by ScyllaDB
 */
/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once
#include <variant>
#include <fmt/core.h>
#include "query-result.hh"
#include "service/paxos/proposal.hh"

namespace service {

namespace paxos {

// A response returned by replica in reply to a promised Paxos prepare request.
// In case a promise is rejected, UUID of a more recent commit is used instead.
struct promise {
    // The latest accepted proposal (if any) known to the replica when it received the prepare
    // request. Can be empty if this is the first Paxos round for this key or the previous round
    // data has expired.
    std::optional<proposal> accepted_proposal;
    // The latest chosen proposal (if any) the replica learned about (i.e. persisted in the paxos
    // table) by the time of this prepare request. Empty on first round or if previous round data
    // has expired.
    std::optional<proposal> most_recent_commit;
    // Local query result with digest or just digest, depending on the caller's choice, or none
    // if the query failed. It is used to skip a separate query after the prepare phase provided
    // results from all PAXOS participants match.
    std::optional<std::variant<foreign_ptr<lw_shared_ptr<query::result>>, query::result_digest>> data_or_digest;

    std::optional<std::variant<std::reference_wrapper<query::result>, query::result_digest>> get_data_or_digest() const {
        if (!data_or_digest) {
            return std::optional<std::variant<std::reference_wrapper<query::result>, query::result_digest>>();
        } else if (std::holds_alternative<foreign_ptr<lw_shared_ptr<query::result>>>(*data_or_digest)) {
            return *std::get<foreign_ptr<lw_shared_ptr<query::result>>>(*data_or_digest);
        } else {
            return std::get<query::result_digest>(*data_or_digest);
        }
    }

    promise(std::optional<proposal> accepted_arg, std::optional<proposal> chosen_arg,
            std::optional<std::variant<foreign_ptr<lw_shared_ptr<query::result>>, query::result_digest>> data_or_digest_arg)
        : accepted_proposal(std::move(accepted_arg))
        , most_recent_commit(std::move(chosen_arg))
        , data_or_digest(std::move(data_or_digest_arg)) {}

    promise(std::optional<proposal> accepted_arg, std::optional<proposal> chosen_arg,
            std::optional<std::variant<query::result, query::result_digest>> data_or_digest_arg)
        : accepted_proposal(std::move(accepted_arg))
        , most_recent_commit(std::move(chosen_arg)) {
        if (data_or_digest_arg) {
            if (std::holds_alternative<query::result>(*data_or_digest_arg)) {
                data_or_digest = make_foreign(make_lw_shared<query::result>(std::move(std::get<query::result>(*data_or_digest_arg))));
            } else {
                data_or_digest = std::move(std::get<query::result_digest>(*data_or_digest_arg));
            }
        }
    }
};

// If the prepare request is rejected we return timeUUID of the most recently promised ballot, so
// that the proposer can adjust its own ballot accordingly. Otherwise we return the previous
// accepted proposal and/or committed decision, so that the proposer can either resume its own Paxos
// round or repair the previous one.
using prepare_response = std::variant<utils::UUID, promise>;

} // end of namespace "paxos"

} // end of namespace "service"

template <> struct fmt::formatter<service::paxos::promise> : fmt::formatter<string_view> {
    auto format(const service::paxos::promise&, fmt::format_context& ctx) const -> decltype(ctx.out());
};
