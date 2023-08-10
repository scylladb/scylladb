/*
 * Copyright 2016-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "permission_altering_statement.hh"

namespace cql3 {

class query_processor;

namespace statements {

class grant_statement : public permission_altering_statement {
public:
    using permission_altering_statement::permission_altering_statement;

    std::unique_ptr<prepared_statement> prepare(data_dictionary::database db, cql_stats& stats) override;

    future<::shared_ptr<cql_transport::messages::result_message>> execute(query_processor&
                    , service::query_state&
                    , const query_options&
                    , std::optional<service::group0_guard> guard) const override;
};

}

}
