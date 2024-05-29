/*
 * Copyright (C) 2016-present ScyllaDB
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "cql3/statements/schema_altering_statement.hh"
#include "cql3/statements/cf_properties.hh"
#include "cql3/cf_name.hh"
#include "cql3/expr/expression.hh"

#include <seastar/core/shared_ptr.hh>

#include <utility>
#include <vector>

namespace cql3 {

class query_processor;
class relation;

namespace selection {
    class raw_selector;
} // namespace selection

namespace statements {

/** A <code>CREATE MATERIALIZED VIEW</code> parsed from a CQL query statement. */
class create_view_statement : public schema_altering_statement {
private:
    mutable cf_name _base_name;
    std::vector<::shared_ptr<selection::raw_selector>> _select_clause;
    expr::expression _where_clause;
    std::vector<::shared_ptr<cql3::column_identifier::raw>> _partition_keys;
    std::vector<::shared_ptr<cql3::column_identifier::raw>> _clustering_keys;
    cf_properties _properties;
    bool _if_not_exists;

public:
    create_view_statement(
            cf_name view_name,
            cf_name base_name,
            std::vector<::shared_ptr<selection::raw_selector>> select_clause,
            expr::expression where_clause,
            std::vector<::shared_ptr<cql3::column_identifier::raw>> partition_keys,
            std::vector<::shared_ptr<cql3::column_identifier::raw>> clustering_keys,
            bool if_not_exists);

    std::pair<view_ptr, cql3::cql_warnings_vec> prepare_view(data_dictionary::database db) const;

    auto& properties() {
        return _properties;
    }

    // Functions we need to override to subclass schema_altering_statement
    virtual future<> check_access(query_processor& qp, const service::client_state& state) const override;
    future<std::tuple<::shared_ptr<cql_transport::event::schema_change>, std::vector<mutation>, cql3::cql_warnings_vec>> prepare_schema_mutations(query_processor& qp, const query_options& options, api::timestamp_type) const override;

    virtual std::unique_ptr<prepared_statement> prepare(data_dictionary::database db, cql_stats& stats) override;

    // FIXME: continue here. See create_table_statement.hh and CreateViewStatement.java
private:
    ::shared_ptr<event_t> created_event() const;
};

}
}
