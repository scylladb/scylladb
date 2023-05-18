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

class query_backend;
class cf_name;

namespace statements {

class drop_table_statement : public schema_altering_statement {
    bool _if_exists;
public:
    drop_table_statement(cf_name cf_name, bool if_exists);

    virtual future<> check_access(query_backend& qb, const service::client_state& state) const override;

    virtual void validate(query_backend&, const service::client_state& state) const override;

    future<std::pair<::shared_ptr<cql_transport::event::schema_change>, std::vector<mutation>>> prepare_schema_mutations(query_backend& qb, api::timestamp_type) const override;


    virtual std::unique_ptr<prepared_statement> prepare(data_dictionary::database db, cql_stats& stats) override;
};

}

}
