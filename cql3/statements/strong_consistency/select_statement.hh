/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "cql3/cql_statement.hh"
#include "cql3/statements/select_statement.hh"

namespace cql3::statements::strong_consistency {

class select_statement : public cql3::statements::select_statement {
    using result_message = cql_transport::messages::result_message;

public:
    using cql3::statements::select_statement::select_statement;

    future<::shared_ptr<cql_transport::messages::result_message>> do_execute(query_processor& qp,
        service::query_state& state, const query_options& options) const override;
};

}