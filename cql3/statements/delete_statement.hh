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
#include "data_dictionary/data_dictionary.hh"

namespace cql3 {

class attributes;

namespace statements {

/**
* A <code>DELETE</code> parsed from a CQL query statement.
*/
class delete_statement : public modification_statement {
public:
    delete_statement(statement_type type, uint32_t bound_terms, schema_ptr s, std::unique_ptr<attributes> attrs, cql_stats& stats);

    virtual bool require_full_clustering_key() const override;

    virtual bool allow_clustering_key_slices() const override;

    virtual void add_update_for_key(mutation& m, const query::clustering_range& range, const update_parameters& params, const json_cache_opt& json_cache) const override;
};

}

}
