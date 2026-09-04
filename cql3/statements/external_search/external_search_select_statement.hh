/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "cql3/statements/external_search/external_index_select_statement.hh"
#include "cql3/statements/external_search/external_search_plan.hh"
#include "cql3/statements/external_search/filter.hh"

#include <vector>

namespace cql3::statements {

/// A SELECT served by one or more external searches (ANN, BM25).
///
/// Every such query runs the same way whatever it searches: ask each index for ranked keys, read
/// the rows any of them returned from the base table, fill in the score and rank each search gave
/// each row, and emit the rows in order. What differs by kind (how the query value is read, how the
/// index is asked, whether excerpts can be fetched) lives in ann_search and bm25_search.
class external_search_select_statement : public external_index_select_statement {
    std::vector<search_source> _sources;
    /// The restrictions a vector search prefilters by, serialized for the index. Empty for a
    /// full-text search, which takes no filter.
    external_search::prepared_filter _prepared_filter;

public:
    /// The most candidates an index is asked for, and so the most rows the base-table read fetches.
    static constexpr uint64_t max_query_limit = 1000;

    static ::shared_ptr<select_statement> prepare(
            data_dictionary::database db, std::vector<search_source> sources, external_statement_args args);

    external_search_select_statement(std::vector<search_source> sources, external_search::prepared_filter prepared_filter,
            external_statement_args args);

private:
    future<::shared_ptr<cql_transport::messages::result_message>> execute_search(
            query_processor& qp, service::query_state& state, const query_options& options, uint64_t limit) const override;

    std::string_view index_search_type_name() const override;
};

} // namespace cql3::statements
