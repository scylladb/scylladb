/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "cql3/statements/modification_statement.hh"
#include "cql3/attributes.hh"

#include "data_dictionary/data_dictionary.hh"

namespace cql3 {

namespace statements {

/**
 * An <code>UPDATE</code> statement parsed from a CQL query statement.
 */
class update_statement : public modification_statement {
public:
#if 0
    private static final Constants.Value EMPTY = new Constants.Value(ByteBufferUtil.EMPTY_BYTE_BUFFER);
#endif

    update_statement(
            statement_type type,
            uint32_t bound_terms,
            schema_ptr s,
            std::unique_ptr<attributes> attrs,
            cql_stats& stats);
private:
    virtual bool require_full_clustering_key() const override;

    virtual bool allow_clustering_key_slices() const override;

    virtual void add_update_for_key(mutation& m, const query::clustering_range& range, const update_parameters& params, const json_cache_opt& json_cache) const override;

    virtual void execute_operations_for_key(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params, const json_cache_opt& json_cache) const;

public:
    virtual ::shared_ptr<strongly_consistent_modification_statement> prepare_for_broadcast_tables() const override;
};

/*
 * Update statement specification that has specifically one bound name - a JSON string.
 * Overridden add_update_for_key uses this parsed JSON to look up values for columns.
 */
class insert_prepared_json_statement : public update_statement {
    expr::expression _value;
    bool _default_unset;
public:
    insert_prepared_json_statement(
            uint32_t bound_terms,
            schema_ptr s,
            std::unique_ptr<attributes> attrs,
            cql_stats& stats,
            expr::expression v, bool default_unset)
        : update_statement(statement_type::INSERT, bound_terms, s, std::move(attrs), stats)
        , _value(std::move(v))
        , _default_unset(default_unset) {
        _restrictions = restrictions::statement_restrictions(s, false);
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
