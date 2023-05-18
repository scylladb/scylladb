/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <memory>

#include "cql3/statements/schema_altering_statement.hh"

namespace cql3 {

class query_backend;

namespace statements {

class ks_prop_defs;

class alter_keyspace_statement : public schema_altering_statement {
    sstring _name;
    ::shared_ptr<ks_prop_defs> _attrs;

public:
    alter_keyspace_statement(sstring name, ::shared_ptr<ks_prop_defs> attrs);

    const sstring& keyspace() const override;

    future<> check_access(query_backend& qb, const service::client_state& state) const override;
    void validate(query_backend& qb, const service::client_state& state) const override;
    future<std::pair<::shared_ptr<cql_transport::event::schema_change>, std::vector<mutation>>> prepare_schema_mutations(query_backend& qb, api::timestamp_type) const override;
    virtual std::unique_ptr<prepared_statement> prepare(data_dictionary::database db, cql_stats& stats) override;
    virtual future<::shared_ptr<messages::result_message>> execute(query_backend& qb, service::query_state& state, const query_options& options) const override;
};

}
}
