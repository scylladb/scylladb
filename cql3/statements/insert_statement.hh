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

/*
 * Insert statement specification that has specifically one bound name - a JSON string.
 * Overridden execute_operations_for_key uses this parsed JSON to look up values for columns.
 */
class insert_prepared_json_statement : public update_statement {
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
        : update_statement(std::move(audit_info), statement_type::INSERT, bound_terms, s, std::move(attrs), stats)
        , _value(std::move(v))
        , _default_unset(default_unset) {
        _restrictions = cql3::restrictions::make_empty_insert_restrictions(s);
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
