/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "cql3/cql_statement.hh"
#include "db/consistency_level_type.hh"
#include "locator/tablets.hh"

namespace service::strong_consistency { struct stats; }

namespace cql3::statements::strong_consistency {

future<::shared_ptr<cql_transport::messages::result_message>> redirect_statement(
    query_processor& qp,
    const query_options& options,
    const locator::tablet_replica& target,
    db::timeout_clock::time_point timeout,
    bool is_write,
    service::strong_consistency::stats& stats,
    locator::host_id_or_exception_callback on_forwarding_finished = {});

bool is_strongly_consistent(data_dictionary::database db, std::string_view ks_name);

void validate_write_consistency_level(const db::consistency_level& cl);

// The tablet version block a request carried, if its response may answer it with
// routing information: only over a connection that negotiated TABLETS_ROUTING_V2, and
// only for EXECUTE requests, which are the ones that carry a block. A QUERY request
// targeting a single partition is served all the same, just without routing information.
std::optional<locator::tablet_version_block> tablet_version_block_for(const service::query_state& qs,
        const query_options& options);

}
