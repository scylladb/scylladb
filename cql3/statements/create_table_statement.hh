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
#include "cql3/statements/cf_properties.hh"
#include "cql3/statements/raw/cf_statement.hh"
#include "cql3/cql3_type.hh"

#include "schema/schema_fwd.hh"

#include <seastar/core/shared_ptr.hh>

#include <seastar/util/indirect.hh>
#include <unordered_map>
#include <vector>
#include <set>
#include <optional>

namespace cql3 {

class query_processor;
class cf_prop_defs;

namespace statements {

/** A <code>CREATE TABLE</code> parsed from a CQL query statement. */
class create_table_statement : public schema_altering_statement {
#if 0
    private AbstractType<?> defaultValidator;
#endif
    std::vector<data_type> _partition_key_types;
    std::vector<data_type> _clustering_key_types;
    std::vector<bytes> _key_aliases;
    std::vector<bytes> _column_aliases;
#if 0
    private ByteBuffer valueAlias;
#endif
    bool _use_compact_storage;

    using column_map_type =
        std::unordered_map<::shared_ptr<column_identifier>,
                           data_type,
                           shared_ptr_value_hash<column_identifier>,
                           shared_ptr_equal_by_value<column_identifier>>;
    using column_set_type =
        std::unordered_set<::shared_ptr<column_identifier>,
                           shared_ptr_value_hash<column_identifier>,
                           shared_ptr_equal_by_value<column_identifier>>;
    column_map_type _columns;
    column_set_type _static_columns;
    const ::shared_ptr<cf_prop_defs> _properties;
    const bool _if_not_exists;
    std::optional<table_id> _id;
public:
    create_table_statement(cf_name name,
                           ::shared_ptr<cf_prop_defs> properties,
                           bool if_not_exists,
                           column_set_type static_columns,
                           const std::optional<table_id>& id);

    virtual future<> check_access(query_processor& qp, const service::client_state& state) const override;

    future<std::tuple<::shared_ptr<cql_transport::event::schema_change>, std::vector<mutation>, cql3::cql_warnings_vec>> prepare_schema_mutations(query_processor& qp, const query_options& options, api::timestamp_type) const override;

    virtual std::unique_ptr<prepared_statement> prepare(data_dictionary::database db, cql_stats& stats) override;

    virtual future<> grant_permissions_to_creator(const service::client_state&, service::group0_batch&) const override;

    schema_ptr get_cf_meta_data(const data_dictionary::database) const;

    class raw_statement;

    friend raw_statement;
private:
    std::vector<column_definition> get_columns() const;

    void apply_properties_to(schema_builder& builder, const data_dictionary::database) const;

    void add_column_metadata_from_aliases(schema_builder& builder, std::vector<bytes> aliases, const std::vector<data_type>& types, column_kind kind) const;

    ::shared_ptr<event_t> created_event() const;
};

class create_table_statement::raw_statement : public raw::cf_statement {
private:
    using defs_type = std::unordered_map<::shared_ptr<column_identifier>,
                                         ::shared_ptr<cql3_type::raw>,
                                         shared_ptr_value_hash<column_identifier>,
                                         shared_ptr_equal_by_value<column_identifier>>;
    defs_type _definitions;
    std::vector<std::vector<::shared_ptr<column_identifier>>> _key_aliases;
    std::vector<::shared_ptr<column_identifier>> _column_aliases;
    create_table_statement::column_set_type _static_columns;

    std::multiset<::shared_ptr<column_identifier>,
            indirect_less<::shared_ptr<column_identifier>, column_identifier::text_comparator>> _defined_names;
    bool _if_not_exists;
    cf_properties _properties;
public:
    raw_statement(cf_name name, bool if_not_exists);

    virtual std::unique_ptr<prepared_statement> prepare(data_dictionary::database db, cql_stats& stats) override;

    cf_properties& properties() {
        return _properties;
    }

    data_type get_type_and_remove(column_map_type& columns, ::shared_ptr<column_identifier> t);

    void add_definition(::shared_ptr<column_identifier> def, ::shared_ptr<cql3_type::raw> type, bool is_static);

    void add_key_aliases(const std::vector<::shared_ptr<column_identifier>> aliases);

    void add_column_alias(::shared_ptr<column_identifier> alias);
};

std::optional<sstring> check_restricted_table_properties(
    data_dictionary::database db,
    std::optional<schema_ptr> schema,
    const sstring& keyspace, const sstring& table,
    const cf_prop_defs& cfprops);

}

}
