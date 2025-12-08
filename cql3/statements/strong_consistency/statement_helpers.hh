/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "cql3/cql_statement.hh"
#include "locator/tablets.hh"
#include "locator/host_id.hh"

namespace cql3::statements::strong_consistency {

// Exception thrown when a strongly consistent statement is attempted to be executed on a non-leader replica
// This is used to signal that the request should be retried on another node
class not_a_leader_exception : public std::exception {
    locator::host_id _target_host;
    unsigned _target_shard;
    sstring _message;
public:
    not_a_leader_exception(locator::host_id target_host, unsigned target_shard)
        : _target_host(target_host)
        , _target_shard(target_shard)
        , _message(format("Not the leader, should forward to host {} shard {}", target_host, target_shard))
    {}

    const char* what() const noexcept override { return _message.c_str(); }
    locator::host_id target_host() const { return _target_host; }
    unsigned target_shard() const { return _target_shard; }
};

    future<::shared_ptr<cql_transport::messages::result_message>> redirect_statement(
        query_processor& qp,
        const query_options& options,
        const locator::tablet_replica& target);

    bool is_strongly_consistent(data_dictionary::database db, std::string_view ks_name);
}