/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "cql3/cql_statement.hh"
#include "locator/tablets.hh"

namespace service::strong_consistency { struct stats; }

namespace cql3::statements { class modification_statement; }

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

// Rejects CQL constructs that the strongly consistent write path cannot honour.
// Called at prepare time, so that unsupported statements fail early rather than
// being silently mis-executed.
void validate_modification_support(const cql3::statements::modification_statement& stmt);

}