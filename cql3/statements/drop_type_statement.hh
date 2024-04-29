/*
 * Copyright 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "cql3/statements/schema_altering_statement.hh"
#include "cql3/ut_name.hh"

namespace cql3 {

class query_processor;

namespace statements {

class drop_type_statement : public schema_altering_statement {
    ut_name _name;
    bool _if_exists;
public:
    drop_type_statement(const ut_name& name, bool if_exists);

    virtual void prepare_keyspace(const service::client_state& state) override;

    virtual future<> check_access(query_processor& qp, const service::client_state& state) const override;

    virtual const sstring& keyspace() const override;

    future<std::tuple<::shared_ptr<cql_transport::event::schema_change>, std::vector<mutation>, cql3::cql_warnings_vec>> prepare_schema_mutations(query_processor& qp, const query_options& options, api::timestamp_type) const override;


    virtual std::unique_ptr<prepared_statement> prepare(data_dictionary::database db, cql_stats& stats) override;
private:
    bool validate_while_executing(query_processor&) const;
};

}

}
