/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "cql3/expr/expression.hh"
#include "data_dictionary/data_dictionary.hh"
#include "index/secondary_index.hh"
#include "schema/schema_fwd.hh"
#include "utils/rjson.hh"
#include "vector_search/vector_store_client.hh"

#include <seastar/core/future.hh>

class column_definition;

/// The parts of running a vector search that are specific to it: how the query value is read, how
/// many candidates the index is asked for, how it is asked, and how a rescoring index's similarity
/// is recomputed. The statement running the search decides when to ask and what to do with the rows.
namespace cql3::statements::ann_search {

/// The query vector an evaluated query value holds.
std::vector<float> query_vector(const column_definition& column, const cql3::raw_value& value);

/// How many candidates the index is asked for to return `wanted` rows. A quantized index is asked
/// for more than is wanted (its oversampling option), and the recomputed scores decide which are
/// kept.
uint64_t candidates_wanted(const secondary_index::index& index, uint64_t wanted);

/// Asks the index for the rows nearest the query vector. A failed request fails the query.
seastar::future<vector_search::vector_store_client::primary_keys> ask(vector_search::vector_store_client& client,
        const sstring& keyspace, const sstring& index_name, schema_ptr schema, std::vector<float> query_vector, uint64_t wanted,
        const rjson::value& filter, seastar::abort_source& as);

/// The similarity the coordinator recomputes for each row when the index rescores: the index scored
/// a quantized vector, so the similarity is recomputed from the stored one.
expr::expression similarity_expression(const secondary_index::index& index, const column_definition* column,
        const expr::expression& query_vector, data_dictionary::database db, const schema_ptr& schema);

} // namespace cql3::statements::ann_search
