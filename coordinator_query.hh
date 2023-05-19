/*
 * Copyright (C) 2023-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "query-result.hh"
#include "locator/host_id.hh"
#include "db/read_repair_decision.hh"
#include "service_permit.hh"
#include "tracing/trace_state.hh"

namespace service {
class client_state;
};

using replicas_per_token_range = std::unordered_map<dht::token_range, std::vector<locator::host_id>>;

class coordinator_query_options {
    lowres_clock::time_point _timeout;

public:
    service_permit permit;
    service::client_state& cstate;
    tracing::trace_state_ptr trace_state = nullptr;
    replicas_per_token_range preferred_replicas;
    std::optional<db::read_repair_decision> read_repair_decision;

    coordinator_query_options(lowres_clock::time_point timeout,
            service_permit permit_,
            service::client_state& client_state_,
            tracing::trace_state_ptr trace_state = nullptr,
            replicas_per_token_range preferred_replicas = { },
            std::optional<db::read_repair_decision> read_repair_decision = { })
        : _timeout(timeout)
        , permit(std::move(permit_))
        , cstate(client_state_)
        , trace_state(std::move(trace_state))
        , preferred_replicas(std::move(preferred_replicas))
        , read_repair_decision(read_repair_decision) {
    }

    lowres_clock::time_point timeout() const {
        return _timeout;
    }
};

struct coordinator_query_result {
    foreign_ptr<lw_shared_ptr<query::result>> query_result;
    replicas_per_token_range last_replicas;
    db::read_repair_decision read_repair_decision;

    coordinator_query_result(foreign_ptr<lw_shared_ptr<query::result>> query_result,
            replicas_per_token_range last_replicas = {},
            db::read_repair_decision read_repair_decision = db::read_repair_decision::NONE)
        : query_result(std::move(query_result))
        , last_replicas(std::move(last_replicas))
        , read_repair_decision(std::move(read_repair_decision)) {
    }
};
