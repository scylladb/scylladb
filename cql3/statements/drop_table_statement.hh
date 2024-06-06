/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "cql3/statements/schema_altering_statement.hh"

namespace cql3 {

class query_processor;
class cf_name;

namespace statements {

class drop_table_statement : public schema_altering_statement {
    bool _if_exists;
public:
    drop_table_statement(cf_name cf_name, bool if_exists);

    virtual future<> check_access(query_processor& qp, const service::client_state& state) const override;

    future<std::tuple<::shared_ptr<cql_transport::event::schema_change>, cql3::cql_warnings_vec>> prepare_schema_mutations(query_processor& qp, service::query_state& state, const query_options& options, service::group0_batch& mc) const override;

    virtual std::unique_ptr<prepared_statement> prepare(data_dictionary::database db, cql_stats& stats) override;
};

}

}
