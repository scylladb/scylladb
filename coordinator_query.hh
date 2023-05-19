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

using replicas_per_token_range = std::unordered_map<dht::token_range, std::vector<locator::host_id>>;

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
