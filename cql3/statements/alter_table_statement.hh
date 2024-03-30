/*
 * Copyright 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "cql3/attributes.hh"
#include "cql3/statements/schema_altering_statement.hh"
#include "cql3/statements/cf_prop_defs.hh"
#include "cql3/cql3_type.hh"
#include "cql3/column_identifier.hh"
#include "data_dictionary/data_dictionary.hh"
#include "timestamp.hh"

namespace cql3 {

class query_processor;

namespace statements {

class alter_table_statement : public schema_altering_statement {
public:
    enum class type {
        add,
        alter,
        drop,
        opts,
        rename,
    };
    using renames_type = std::vector<std::pair<shared_ptr<column_identifier::raw>,
                                               shared_ptr<column_identifier::raw>>>;
    struct column_change {
        shared_ptr<column_identifier::raw> name;
        shared_ptr<cql3_type::raw> validator = nullptr;
        bool is_static = false;
    };

    class raw_statement;
private:
    const uint32_t _bound_terms;

    const type _type;
    const std::vector<column_change> _column_changes;
    const std::optional<cf_prop_defs> _properties;
    const renames_type _renames;
    const std::unique_ptr<attributes> _attrs;
public:
    alter_table_statement(uint32_t bound_terms,
                          cf_name name,
                          type t,
                          std::vector<column_change> column_changes,
                          std::optional<cf_prop_defs> properties,
                          renames_type renames,
                          std::unique_ptr<attributes> attrs);

    virtual uint32_t get_bound_terms() const override;
    virtual future<> check_access(query_processor& qp, const service::client_state& state) const override;
    virtual std::unique_ptr<prepared_statement> prepare(data_dictionary::database db, cql_stats& stats) override;
    virtual future<::shared_ptr<messages::result_message>> execute(query_processor& qp, service::query_state& state, const query_options& options, std::optional<service::group0_guard> guard) const override;

    future<std::tuple<::shared_ptr<cql_transport::event::schema_change>, std::vector<mutation>, cql3::cql_warnings_vec>> prepare_schema_mutations(query_processor& qp, const query_options& options, api::timestamp_type) const override;
private:
    void add_column(const query_options& options, const schema& schema, data_dictionary::table cf, schema_builder& cfm, std::vector<view_ptr>& view_updates, const column_identifier& column_name, const cql3_type validator, const column_definition* def, bool is_static) const;
    void alter_column(const query_options& options, const schema& schema, data_dictionary::table cf, schema_builder& cfm, std::vector<view_ptr>& view_updates, const column_identifier& column_name, const cql3_type validator, const column_definition* def, bool is_static) const;
    void drop_column(const query_options& options, const schema& schema, data_dictionary::table cf, schema_builder& cfm, std::vector<view_ptr>& view_updates, const column_identifier& column_name, const cql3_type validator, const column_definition* def, bool is_static) const;
    std::pair<schema_builder, std::vector<view_ptr>> prepare_schema_update(data_dictionary::database db, const query_options& options) const;
};

class alter_table_statement::raw_statement : public raw::cf_statement {
    const alter_table_statement::type _type;
    const std::vector<column_change> _column_changes;
    const std::optional<cf_prop_defs> _properties;
    const alter_table_statement::renames_type _renames;
    const std::unique_ptr<attributes::raw> _attrs;

public:
    raw_statement(cf_name name,
                  type t,
                  std::vector<column_change> column_changes,
                  std::optional<cf_prop_defs> properties,
                  renames_type renames,
                  std::unique_ptr<attributes::raw> attrs);
    
    virtual std::unique_ptr<prepared_statement> prepare(data_dictionary::database db, cql_stats& stats) override;
};

}

}
