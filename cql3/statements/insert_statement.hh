/*
 * Copyright (C) 2026-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)
 */

#pragma once

#include "cql3/statements/update_statement.hh"

namespace cql3 {

namespace statements {

/**
 * An <code>INSERT</code> statement parsed from a CQL query statement.
 *
 * An INSERT has no WHERE clause: it names the row it creates by naming its
 * primary key columns, and computes both keys from their values.
 */
class insert_statement : public modification_statement {
    // The primary key columns the statement names, with their prepared values.
    std::vector<std::pair<const column_definition*, expr::expression>> _key_values;
public:
    insert_statement(
            audit::audit_info_ptr&& audit_info,
            uint32_t bound_terms,
            schema_ptr s,
            std::unique_ptr<attributes> attrs,
            cql_stats& stats);

    void add_key_value(const column_definition& def, expr::expression value);

    /// True if the statement names the given primary key column.
    bool names(const column_definition& def) const;

    /// Checks that the statement names the whole primary key, and classifies an
    /// IF [NOT] EXISTS condition.  Call once the operations and conditions are in.
    void validate_addressed_row();

    virtual dht::partition_range_vector build_partition_keys(const query_options& options, const json_cache_opt& json_cache) const override;

    virtual query::clustering_row_ranges create_clustering_ranges(const query_options& options, const json_cache_opt& json_cache) const override;

    virtual void validate_primary_key(const query_options& options) const override;

    virtual utils::chunked_vector<mutation> apply_updates(
            const std::vector<dht::partition_range>& keys,
            const std::vector<query::clustering_range>& ranges,
            const update_parameters& params,
            const json_cache_opt& json_cache) const override;

protected:
    /// The value the statement gives the column, or nullptr if it does not name it.
    const expr::expression* value_for(const column_definition& def) const;

    virtual void execute_operations_for_key(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params, const json_cache_opt& json_cache) const;
};

/*
 * Insert statement specification that has specifically one bound name - a JSON string.
 * Overridden execute_operations_for_key uses this parsed JSON to look up values for columns.
 */
class insert_prepared_json_statement : public insert_statement {
    expr::expression _value;
    bool _default_unset;
public:
    insert_prepared_json_statement(
            audit::audit_info_ptr&& audit_info,
            uint32_t bound_terms,
            schema_ptr s,
            std::unique_ptr<attributes> attrs,
            cql_stats& stats,
            expr::expression v, bool default_unset)
        : insert_statement(std::move(audit_info), bound_terms, s, std::move(attrs), stats)
        , _value(std::move(v))
        , _default_unset(default_unset) {
    }
private:
    virtual void execute_operations_for_key(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params, const json_cache_opt& json_cache) const override;

    virtual dht::partition_range_vector build_partition_keys(const query_options& options, const json_cache_opt& json_cache) const override;

    virtual query::clustering_row_ranges create_clustering_ranges(const query_options& options, const json_cache_opt& json_cache) const override;

    json_cache_opt maybe_prepare_json_cache(const query_options& options) const override;

    void execute_set_value(mutation& m, const clustering_key_prefix& prefix, const update_parameters&
        params, const column_definition& column, const bytes_opt& value) const;
};

}

}
