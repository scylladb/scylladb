/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "select_statement.hh"

namespace cql3::statements::strong_consistency {

using result_message = cql_transport::messages::result_message;

future<::shared_ptr<result_message>> select_statement::do_execute(query_processor& qp,
        service::query_state& state, 
        const query_options& options) const
{
    throw exceptions::invalid_request_exception("not implemented");
}

}