/*
 * Copyright 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "cql3/statements/schema_altering_statement.hh"
#include "cql3/cql3_type.hh"
#include "cql3/ut_name.hh"
#include "data_dictionary/data_dictionary.hh"

namespace cql3 {

class query_backend;

namespace statements {

class create_type_statement : public schema_altering_statement {
    ut_name _name;
    std::vector<::shared_ptr<column_identifier>> _column_names;
    std::vector<::shared_ptr<cql3_type::raw>> _column_types;
    bool _if_not_exists;
public:
    create_type_statement(const ut_name& name, bool if_not_exists);

    virtual void prepare_keyspace(const service::client_state& state) override;

    void add_definition(::shared_ptr<column_identifier> name, ::shared_ptr<cql3_type::raw> type);

    virtual future<> check_access(query_backend& qb, const service::client_state& state) const override;

    virtual void validate(query_backend&, const service::client_state& state) const override;

    virtual const sstring& keyspace() const override;

    future<std::pair<::shared_ptr<cql_transport::event::schema_change>, std::vector<mutation>>> prepare_schema_mutations(query_backend& qb, api::timestamp_type) const override;

    virtual std::unique_ptr<prepared_statement> prepare(data_dictionary::database db, cql_stats& stats) override;

    static void check_for_duplicate_names(user_type type);

private:
    bool type_exists_in(data_dictionary::keyspace ks) const;
    std::optional<user_type> make_type(query_backend& qb) const;

public:
    user_type create_type(data_dictionary::database db) const;
};

}

}
