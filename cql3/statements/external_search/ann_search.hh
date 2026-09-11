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

class column_definition;

/// The parts of running a vector search that are specific to it: how the query value is read, how
/// many candidates the index is asked for, and how a rescoring index's similarity is recomputed.
/// The statement running the search decides when to ask and what to do with the rows.
namespace cql3::statements::ann_search {

/// The query vector an evaluated query value holds.
std::vector<float> query_vector(const column_definition& column, const cql3::raw_value& value);

/// How many candidates the index is asked for to return `wanted` rows: more for a quantized index, by
/// its oversampling option, since the recomputed scores decide which are kept.
uint64_t candidates_wanted(const secondary_index::index& index, uint64_t wanted);

/// The similarity the coordinator recomputes for each row when the index rescores: the index scored
/// a quantized vector, so the similarity is recomputed from the stored one.
expr::expression similarity_expression(const secondary_index::index& index, const column_definition* column,
        const expr::expression& query_vector, data_dictionary::database db, const schema_ptr& schema);

} // namespace cql3::statements::ann_search
