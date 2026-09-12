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
#include "data_dictionary/data_dictionary.hh"

namespace cql3 {

class attributes;

namespace statements {

/**
* A <code>DELETE</code> parsed from a CQL query statement.
*/
class delete_statement : public modification_statement {
    shared_ptr<const restrictions::modification_restrictions> _restrictions;
public:
    delete_statement(audit::audit_info_ptr&& audit_info, statement_type type, uint32_t bound_terms, schema_ptr s, std::unique_ptr<attributes> attrs, cql_stats& stats);

public:
    /// Reads a DELETE's WHERE clause, and checks that what it says agrees with what
    /// the statement writes.
    void process_where_clause(data_dictionary::database db, expr::expression where_clause, prepare_context& ctx);

    virtual dht::partition_range_vector build_partition_keys(const query_options& options, const json_cache_opt& json_cache) const override;

    virtual query::clustering_row_ranges create_clustering_ranges(const query_options& options, const json_cache_opt& json_cache) const override;

    virtual void validate_primary_key(const query_options& options) const override;

    const restrictions::modification_restrictions& restrictions() const {
        return *_restrictions;
    }

    virtual utils::chunked_vector<mutation> apply_updates(
            const std::vector<dht::partition_range>& keys,
            const std::vector<query::clustering_range>& ranges,
            const update_parameters& params,
            const json_cache_opt& json_cache) const override;

private:
    void validate_where_clause_for_conditions() const;

    void delete_row_range(mutation& m, const query::clustering_range& range, const update_parameters& params) const;
};

}

}
