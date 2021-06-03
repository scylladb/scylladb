/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
/*
 * Copyright (C) 2019-present ScyllaDB
 *
 * Modified by ScyllaDB
 */
/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once
#include <variant>
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

std::ostream& operator<<(std::ostream& os, const promise& promise);

} // end of namespace "paxos"

} // end of namespace "service"
