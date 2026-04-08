/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "cql3/cql_statement.hh"
#include "locator/tablets.hh"
#include <seastar/util/noncopyable_function.hh>

namespace service::strong_consistency { struct stats; }

namespace cql3::statements::strong_consistency {

future<::shared_ptr<cql_transport::messages::result_message>> redirect_statement(
    query_processor& qp,
    const query_options& options,
    const locator::tablet_replica& target,
    db::timeout_clock::time_point timeout,
    bool is_write,
    service::strong_consistency::stats& stats,
    noncopyable_function<void(locator::host_id)> on_node_resolved = {});

bool is_strongly_consistent(data_dictionary::database db, std::string_view ks_name);

}