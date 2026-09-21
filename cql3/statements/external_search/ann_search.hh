/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "cql3/expr/expression.hh"
#include "cql3/expr/temporary_allocator.hh"
#include "cql3/statements/external_search/external_function.hh"
#include "cql3/statements/external_search/external_index_select_statement.hh"
#include "cql3/statements/external_search/filter.hh"
#include "data_dictionary/data_dictionary.hh"
#include "index/secondary_index.hh"
#include "schema/schema_fwd.hh"
#include "utils/rjson.hh"
#include "vector_search/vector_store_client.hh"

#include <seastar/core/future.hh>

#include <optional>

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

namespace cql3::statements {

/// A query vector written in a SELECT call that prepare could not prove equal to the ORDER BY
/// vector, because a bind marker is involved. Execution compares the bound values;
/// `function_name` is the function the call was written with, for the error message.
struct deferred_select_vector {
    expr::expression vector;
    sstring function_name;
};

/// ANN ordering metadata resolved during prepare.
struct ann_ordering_info {
    secondary_index::index index;
    raw::select_statement::prepared_ann_ordering_type prepared_ann_ordering;
    bool is_rescoring_enabled;
    /// Temporaries holding the Vector Store's score and rank; see
    /// external_search::search_temporaries. ANN() is replaced with a tuple of the two, so it has no
    /// temporary of its own. A rescoring index allocates neither: it recomputes the score locally
    /// and reports no rank.
    external_search::search_temporaries temporaries;
    /// The SELECT occurrences' query vectors that only execution can compare, a bind marker
    /// standing where at least one of the two values will be.
    std::vector<deferred_select_vector> deferred_select_vectors;
};

class vector_indexed_table_select_statement : public external_index_select_statement {
    ann_ordering_info _ann_ordering_info;
    external_search::prepared_filter _prepared_filter;

public:
    static constexpr size_t max_ann_query_limit = 1000;

    static ::shared_ptr<cql3::statements::select_statement> prepare(data_dictionary::database db, schema_ptr schema, uint32_t bound_terms,
            lw_shared_ptr<const parameters> parameters, ::shared_ptr<selection::selection> selection,
            ::shared_ptr<const restrictions::select_restrictions> restrictions, ::shared_ptr<std::vector<size_t>> group_by_cell_indices, bool is_reversed,
            ordering_comparator_type ordering_comparator, std::optional<expr::expression> limit,
            std::optional<expr::expression> per_partition_limit, cql_stats& stats, ann_ordering_info ordering_info, std::unique_ptr<cql3::attributes> attrs);

    vector_indexed_table_select_statement(schema_ptr schema, uint32_t bound_terms, lw_shared_ptr<const parameters> parameters,
            ::shared_ptr<selection::selection> selection, ::shared_ptr<const restrictions::select_restrictions> restrictions,
            ::shared_ptr<std::vector<size_t>> group_by_cell_indices, bool is_reversed, ordering_comparator_type ordering_comparator,
            std::optional<expr::expression> limit, std::optional<expr::expression> per_partition_limit,
            cql_stats& stats, ann_ordering_info ordering_info, external_search::prepared_filter prepared_filter, std::unique_ptr<cql3::attributes> attrs);

private:
    std::string_view index_search_type_name() const override {
        return "Vector Search";
    }

    future<::shared_ptr<cql_transport::messages::result_message>> execute_search(
            query_processor& qp, service::query_state& state, const query_options& options, uint64_t limit) const override;
};

} // namespace cql3::statements
