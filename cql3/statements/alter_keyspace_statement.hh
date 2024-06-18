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

class query_processor;

namespace statements {

class ks_prop_defs;

class alter_keyspace_statement : public schema_altering_statement {
    sstring _name;
    ::shared_ptr<ks_prop_defs> _attrs;

public:
    alter_keyspace_statement(sstring name, ::shared_ptr<ks_prop_defs> attrs);

    bool has_keyspace() const override {
        return true;
    }
    const sstring& keyspace() const override;

    future<> check_access(query_processor& qp, const service::client_state& state) const override;
    void validate(query_processor& qp, const service::client_state& state) const override;
    virtual future<std::tuple<::shared_ptr<event_t>, cql3::cql_warnings_vec>> prepare_schema_mutations(query_processor& qp, service::query_state& state, const query_options& options, service::group0_batch& mc) const override;
    virtual std::unique_ptr<prepared_statement> prepare(data_dictionary::database db, cql_stats& stats) override;
    virtual future<::shared_ptr<messages::result_message>> execute(query_processor& qp, service::query_state& state, const query_options& options, std::optional<service::group0_guard> guard) const override;
    bool changes_tablets(query_processor& qp) const;
};

}
}
