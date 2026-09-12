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
#include "data_dictionary/data_dictionary.hh"

namespace cql3 {

class attributes;

namespace statements {

/**
* A <code>DELETE</code> parsed from a CQL query statement.
*/
class delete_statement : public modification_statement {
public:
    delete_statement(audit::audit_info_ptr&& audit_info, statement_type type, uint32_t bound_terms, schema_ptr s, std::unique_ptr<attributes> attrs, cql_stats& stats);

    virtual utils::chunked_vector<mutation> apply_updates(
            const std::vector<dht::partition_range>& keys,
            const std::vector<query::clustering_range>& ranges,
            const update_parameters& params,
            const json_cache_opt& json_cache) const override;

private:
    void delete_row_range(mutation& m, const query::clustering_range& range, const update_parameters& params) const;
};

}

}
