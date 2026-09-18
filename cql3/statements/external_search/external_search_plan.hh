/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "cql3/statements/external_search/external_index_select_statement.hh"

namespace cql3::statements {

/// Resolves BM25 ordering metadata from the query's prepared ORDER BY call.
/// Returns std::nullopt if the call is not a native bm25() call, i.e. this is not an FTS query.
std::optional<bm25_ordering_info> get_bm25_ordering_info(
        data_dictionary::database db,
        schema_ptr schema,
        const expr::function_call& fc);

/// Replaces every BM25(), BM25_SCORE() and BM25_RANK() call in the SELECT clause, nested
/// occurrences included, with a read of the temporary holding that value, allocating the temporary
/// on the first occurrence of each. Rejects an occurrence with no BM25 ordering and WHERE clause to
/// agree with, or one that disagrees with them on the column or the search term; a disagreement
/// only execution can settle is recorded in ordering_info for it to check.
void prepare_bm25_selectors(std::vector<selection::prepared_selector>& prepared_selectors, std::optional<bm25_ordering_info>& ordering_info,
        expr::temporary_allocator& temporaries_allocator, prepare_context& ctx);

/// Resolves ANN ordering metadata from the query's prepared ORDER BY call.
/// Returns std::nullopt if the call is not a native ann() call, i.e. this is not an ANN query.
std::optional<ann_ordering_info> get_ann_ordering_info(
        data_dictionary::database db,
        schema_ptr schema,
        const expr::function_call& fc);

/// Replaces every ANN(), ANN_SCORE() and ANN_RANK() call in the SELECT clause, nested occurrences
/// included, with a read of the temporary holding the Vector Store's score or rank. When the index
/// rescores, the score is instead the similarity the coordinator recomputes, and there is no rank:
/// ANN_RANK() and ANN() are rejected. Also rejects an occurrence with no ANN ordering to agree
/// with, or one that disagrees with it on the column or the query vector; a disagreement only
/// execution can settle is recorded in ordering_info for it to check.
void prepare_ann_selectors(std::vector<selection::prepared_selector>& prepared_selectors,
        std::optional<ann_ordering_info>& ordering_info, expr::temporary_allocator& temporaries_allocator,
        data_dictionary::database db, const schema_ptr& schema, prepare_context& ctx);

/// The order the rows come back in when the index rescores: the Vector Store ordered them by the
/// score it reported, which is not the requested order then. Sorting reads a column of the result
/// row, so this appends a trailing selector holding the recomputed similarity - the column the
/// returned comparator sorts by, and the one the caller has to hide from the client.
select_statement::ordering_comparator_type rescored_similarity_ordering(
        std::vector<selection::prepared_selector>& prepared_selectors,
        const ann_ordering_info& ann_ordering_info,
        data_dictionary::database db,
        schema_ptr schema);

/// The external searches one statement runs, and the replacement of the calls that refer to them.
///
/// A call to a search function (ANN(), BM25(), ...) cannot be evaluated from its arguments: only
/// the index can. Preparation finds every such call, decides which search it refers to, and replaces
/// it with a read of the temporary that search's result is delivered in.
///
/// ORDER BY introduces the searches; a call in SELECT or WHERE must refer to one of them. The
/// clauses are handled by separate methods because they become available at different points of
/// select_statement::prepare(); resolve_ordering() comes first.
class external_search_plan {
    data_dictionary::database _db;
    schema_ptr _schema;
    prepare_context& _ctx;
    expr::temporary_allocator& _temporaries_allocator;
    // One per family for now; a query naming several searches needs a list instead.
    std::optional<ann_ordering_info> _ann;
    std::optional<bm25_ordering_info> _bm25;

public:
    external_search_plan(data_dictionary::database db, schema_ptr schema, prepare_context& ctx,
            expr::temporary_allocator& temporaries_allocator)
        : _db(db)
        , _schema(std::move(schema))
        , _ctx(ctx)
        , _temporaries_allocator(temporaries_allocator) {
    }

    /// Resolves the search the ORDER BY call names, if it names one.
    void resolve_ordering(const expr::function_call& fc);

    /// Replaces every call in the SELECT clause that refers to a search bound above.
    void replace_selectors(std::vector<selection::prepared_selector>& prepared_selectors);

    bool has_ann() const {
        return _ann.has_value();
    }

    bool has_bm25() const {
        return _bm25.has_value();
    }

    std::optional<ann_ordering_info>& ann() {
        return _ann;
    }

    std::optional<bm25_ordering_info>& bm25() {
        return _bm25;
    }
};


} // namespace cql3::statements
