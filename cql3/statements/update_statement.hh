/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)
 */

#pragma once

#include "cql3/statements/modification_statement.hh"
#include "cql3/restrictions/statement_restrictions.hh"
#include "cql3/attributes.hh"

#include "data_dictionary/data_dictionary.hh"

namespace cql3 {

namespace statements {

// Writing a row: the part of applying a mutation that an UPDATE and an INSERT
// do identically, given the row's clustering key.

/// The clustering key a range names.  A range with no start bound names the
/// static row, whose key is the empty prefix.
clustering_key_prefix row_key(const query::clustering_range& range);

/// Prepares the row for the column operations that follow: validates what a
/// COMPACT STORAGE table requires, and writes the row marker an INSERT into a
/// CQL3 table leaves behind even when it sets no regular column.
void open_row(const schema& s, statement_type type, bool has_column_operations,
        mutation& m, const clustering_key_prefix& prefix, const update_parameters& params);

/// Applies the statement's column operations to the row.
void apply_column_operations(const std::vector<std::unique_ptr<operation>>& ops,
        mutation& m, const clustering_key_prefix& prefix, const update_parameters& params);

/**
 * An <code>UPDATE</code> statement parsed from a CQL query statement.
 */
class update_statement : public modification_statement {
    shared_ptr<const restrictions::update_restrictions> _restrictions;
public:
#if 0
    private static final Constants.Value EMPTY = new Constants.Value(ByteBufferUtil.EMPTY_BYTE_BUFFER);
#endif

    update_statement(
            audit::audit_info_ptr&& audit_info,
            statement_type type,
            uint32_t bound_terms,
            schema_ptr s,
            std::unique_ptr<attributes> attrs,
            cql_stats& stats);

public:
    /// Reads an UPDATE's WHERE clause, and checks that what it says agrees with what
    /// the statement writes.
    void process_where_clause(data_dictionary::database db, expr::expression where_clause, prepare_context& ctx);

    virtual dht::partition_range_vector build_partition_keys(const query_options& options, const json_cache_opt& json_cache) const override;

    virtual query::clustering_row_ranges create_clustering_ranges(const query_options& options, const json_cache_opt& json_cache) const override;

    virtual void validate_primary_key(const query_options& options) const override;

    const restrictions::update_restrictions& restrictions() const {
        return *_restrictions;
    }

    virtual utils::chunked_vector<mutation> apply_updates(
            const std::vector<dht::partition_range>& keys,
            const std::vector<query::clustering_range>& ranges,
            const update_parameters& params,
            const json_cache_opt& json_cache) const override;

private:
    void validate_where_clause_for_conditions() const;

    virtual void execute_operations_for_key(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params, const json_cache_opt& json_cache) const;
};

}

}
