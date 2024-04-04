/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "schema_altering_statement.hh"
#include "index_target.hh"

#include "schema/schema_fwd.hh"

#include <seastar/core/shared_ptr.hh>

#include <vector>


namespace cql3 {

class query_processor;
class index_name;

namespace statements {

class index_prop_defs;

/** A <code>CREATE INDEX</code> statement parsed from a CQL query. */
class create_index_statement : public schema_altering_statement {
    const sstring _index_name;
    const std::vector<::shared_ptr<index_target::raw>> _raw_targets;
    const ::shared_ptr<index_prop_defs> _properties;
    const bool _if_not_exists;
    cql_stats* _cql_stats = nullptr;

public:
    create_index_statement(cf_name name, ::shared_ptr<index_name> index_name,
            std::vector<::shared_ptr<index_target::raw>> raw_targets,
            ::shared_ptr<index_prop_defs> properties, bool if_not_exists);

    future<> check_access(query_processor& qp, const service::client_state& state) const override;
    void validate(query_processor&, const service::client_state& state) const override;
    future<std::tuple<::shared_ptr<cql_transport::event::schema_change>, std::vector<mutation>, cql3::cql_warnings_vec>> prepare_schema_mutations(query_processor& qp, const query_options& options, api::timestamp_type) const override;


    virtual std::unique_ptr<prepared_statement> prepare(data_dictionary::database db, cql_stats& stats) override;

    struct base_schema_with_new_index {
        schema_ptr schema;
        index_metadata index;
    };
    std::optional<base_schema_with_new_index> build_index_schema(data_dictionary::database db) const;
private:
    void validate_for_local_index(const schema& schema) const;
    void validate_for_frozen_collection(const index_target& target) const;
    void validate_not_full_index(const index_target& target) const;
    void validate_for_collection(const index_target& target, const column_definition&) const;
    void rewrite_target_for_collection(index_target& target, const column_definition&) const;
    void validate_is_values_index_if_target_column_not_collection(const column_definition* cd,
                                                                  const index_target& target) const;
    void validate_target_column_is_map_if_index_involves_keys(bool is_map, const index_target& target) const;
    void validate_targets_for_multi_column_index(std::vector<::shared_ptr<index_target>> targets) const;
    static index_metadata make_index_metadata(const std::vector<::shared_ptr<index_target>>& targets,
                                              const sstring& name,
                                              index_metadata_kind kind,
                                              const index_options_map& options);
    std::vector<::shared_ptr<index_target>> validate_while_executing(data_dictionary::database db) const;
};

}
}
