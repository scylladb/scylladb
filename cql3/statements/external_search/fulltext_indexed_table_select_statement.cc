/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "cql3/statements/external_search/fulltext_indexed_table_select_statement.hh"
#include "cql3/statements/external_search/external_function.hh"
#include "cql3/statements/external_search/external_search_provider.hh"
#include "cql3/statements/raw/select_statement.hh"
#include "cql3/expr/evaluate.hh"
#include "cql3/expr/expression.hh"
#include "cql3/expr/expr-utils.hh"
#include "cql3/functions/scoring_fcts.hh"
#include "cql3/query_processor.hh"
#include "cql3/restrictions/statement_restrictions.hh"
#include "index/secondary_index_manager.hh"
#include "data_dictionary/data_dictionary.hh"
#include "db/consistency_level_validations.hh"
#include "exceptions/exceptions.hh"
#include "types/types.hh"
#include "utils/assert.hh"

#include <seastar/core/future.hh>
#include <seastar/coroutine/exception.hh>

namespace cql3::statements {

namespace {

/// The column the index is built on: the one the rows are ranked by and a fragment is generated from.
const column_definition& ranked_column(const schema& schema, const secondary_index::index& index) {
    const auto* cdef = schema.get_column_definition(to_bytes(index.target_column()));
    throwing_assert(cdef);
    return *cdef;
}

std::optional<expr::expression> validate_bm25_where_restriction(const expr::binary_operator& binop,
        const bm25_ordering_info& ordering_info) {
    const auto& fc = expr::as<expr::function_call>(binop.lhs);
    if (expr::is_native_function_call(fc, functions::BM25_HIGHLIGHT_FUNCTION_NAME)) {
        // A fragment is generated from a row the search has already selected, so there is nothing
        // here to restrict by.
        throw exceptions::invalid_request_exception("BM25_HIGHLIGHT() is only supported in the SELECT clause");
    }
    auto [col, where_term] = external_search::extract_call_arguments(fc, "BM25");
    if (col->name_as_text() != ordering_info.index.target_column()) {
        throw exceptions::invalid_request_exception("Full-text search queries must reference the same column in both WHERE and ORDER BY clauses");
    }

    if (binop.op != expr::oper_t::GT) {
        throw exceptions::invalid_request_exception(
                seastar::format("Unsupported \"{}\" relation for BM25 function restriction, only \">\" is supported", binop.op));
    }
    const auto* rhs_const = expr::as_if<expr::constant>(&binop.rhs);
    if (!rhs_const || rhs_const->is_null() || rhs_const->view().deserialize<float>(*float_type) != 0.0f) {
        throw exceptions::invalid_request_exception("BM25 function comparison value must be the literal 0");
    }

    const auto terms_equal = external_search::unevaluated_equality(where_term, ordering_info.search_term);
    if (terms_equal != external_search::equality::always) {
        if (terms_equal == external_search::equality::never) {
            throw exceptions::invalid_request_exception(
                    "Full-text search queries must use the same search term in both WHERE and ORDER BY clauses");
        }
        return std::move(where_term);
    }
    return std::nullopt;
}

/// Asks the full-text index for a highlighted fragment of every row's text, and returns the
/// fragments as the values of the highlight temporary: one per row in `rows`, in the same order.
///
/// The text of each row is `row.columns[text_column]`. All the texts are sent in one request, and
/// the reply is an array of the same length: reply[i] is the fragment of rows[i]. A row with no text
/// is sent as an empty string, so that the positions still line up. A row the index found no
/// fragment in gets a null value and is not dropped. If the request fails, the query fails.
future<std::vector<cql3::raw_value>> highlights_of(vector_search::vector_store_client& client, const schema& schema,
        const secondary_index::index& index, const sstring& search_term, std::span<const external_search::joined_row> rows, size_t text_column,
        abort_source& as) {
    const auto& type = *ranked_column(schema, index).type;
    auto documents = std::vector<sstring>{};
    documents.reserve(rows.size());
    for (const auto& row : rows) {
        const auto& text = row.columns.at(text_column);
        documents.push_back(text ? value_cast<sstring>(type.deserialize(managed_bytes_view(*text))) : sstring());
    }

    if (documents.empty()) {
        co_return std::vector<cql3::raw_value>{};
    }

    auto fragments = co_await client.highlight(schema.ks_name(), index.metadata().name(), search_term, std::move(documents), as);
    if (!fragments.has_value()) {
        co_await coroutine::return_exception(
                exceptions::invalid_request_exception(std::visit(vector_search::vector_store_client::fts_error_visitor{}, fragments.error())));
    }

    auto values = std::vector<cql3::raw_value>{};
    values.reserve(fragments->size());
    for (const auto& fragment : *fragments) {
        values.push_back(fragment ? cql3::raw_value::make_value(utf8_type->decompose(*fragment)) : cql3::raw_value::make_null());
    }
    co_return values;
}

} // anonymous namespace

void prepare_bm25_selectors(std::vector<selection::prepared_selector>& prepared_selectors, std::optional<bm25_ordering_info>& ordering_info,
        expr::temporary_allocator& temporaries_allocator, prepare_context& ctx) {
    for (auto& ps : prepared_selectors) {
        ps.expr = expr::search_and_replace(ps.expr, [&](const expr::expression& candidate) -> std::optional<expr::expression> {
            const auto* fc = expr::as_if<expr::function_call>(&candidate);
            if (!fc) {
                return std::nullopt;
            }
            const bool is_score = expr::is_native_function_call(*fc, functions::BM25_FUNCTION_NAME);
            const bool is_highlight = expr::is_native_function_call(*fc, functions::BM25_HIGHLIGHT_FUNCTION_NAME);
            if (!is_score && !is_highlight) {
                return std::nullopt;
            }

            const std::string_view function_name = is_score ? "BM25" : "BM25_HIGHLIGHT";
            if (!ordering_info) {
                throw exceptions::invalid_request_exception(seastar::format(
                        "{}() is not supported in the SELECT clause without matching ORDER BY and WHERE clauses", function_name));
            }
            auto& info = *ordering_info;

            // Every call describes the one search the rows are ranked by, so it has to name the
            // column and the search term the other two clauses do.
            auto [col, sel_term] = external_search::extract_call_arguments(*fc, function_name);
            if (col->name_as_text() != info.index.target_column()) {
                throw exceptions::invalid_request_exception(
                        seastar::format("{}() in SELECT must reference the same column as BM25() in WHERE and ORDER BY", function_name));
            }

            const auto terms_equal = external_search::unevaluated_equality(sel_term, info.search_term);
            if (terms_equal != external_search::equality::always) {
                if (terms_equal == external_search::equality::never) {
                    throw exceptions::invalid_request_exception(
                            seastar::format("{}() in SELECT must use the same search term as BM25() in WHERE and ORDER BY", function_name));
                }
                // Lifted out of the selector tree, so nothing else registers a bind marker in this term.
                expr::fill_prepare_context(sel_term, ctx);
                info.deferred_select_terms.push_back({std::move(sel_term), function_name});
            }

            // Every occurrence of one value reports the same thing, so one temporary serves them all.
            auto& temporary_index = is_score ? info.score_temporary_index : info.highlight_temporary_index;
            if (!temporary_index) {
                temporary_index = temporaries_allocator.allocate();
            }

            return expr::expression(expr::temporary{
                    .index = *temporary_index,
                    .type = is_score ? float_type : utf8_type,
                    .replaced_expr = candidate,
            });
        });
    }
}

std::optional<bm25_ordering_info> get_bm25_ordering_info(
        data_dictionary::database db,
        schema_ptr schema,
        const expr::function_call& fc) {

    if (!expr::is_native_function_call(fc, functions::BM25_FUNCTION_NAME)) {
        return std::nullopt;
    }
    auto [column, search_term] = external_search::extract_call_arguments(fc, "BM25");

    auto cf = db.find_column_family(schema);
    auto& sim = cf.get_index_manager();

    for (const auto& idx : sim.list_indexes()) {
        if (idx.supports_bm25_expression(*column)) {
            return bm25_ordering_info{idx, std::move(search_term)};
        }
    }

    throw exceptions::invalid_request_exception("No fulltext index found for full-text search query");
}

::shared_ptr<cql3::statements::select_statement> fulltext_indexed_table_select_statement::prepare(data_dictionary::database db,
        schema_ptr schema, uint32_t bound_terms, lw_shared_ptr<const parameters> parameters,
        ::shared_ptr<selection::selection> selection, ::shared_ptr<const restrictions::statement_restrictions> restrictions,
        ::shared_ptr<std::vector<size_t>> group_by_cell_indices, bool is_reversed,
        ordering_comparator_type ordering_comparator, std::optional<expr::expression> limit,
        std::optional<expr::expression> per_partition_limit, cql_stats& stats,
        std::optional<bm25_ordering_info> ordering_info,
        std::unique_ptr<attributes> attrs) {

    if (!limit.has_value()) {
        throw exceptions::invalid_request_exception("Full-text search queries require a LIMIT");
    }

    if (per_partition_limit.has_value()) {
        throw exceptions::invalid_request_exception("Full-text search queries do not support per-partition limits");
    }

    if (selection->is_aggregate() || !group_by_cell_indices->empty()) {
        throw exceptions::invalid_request_exception("Full-text search queries cannot be run with aggregation");
    }

    if (!ordering_info) {
        throw exceptions::invalid_request_exception("Full-text search queries require an ORDER BY BM25() clause");
    }

    const auto& scoring_restrictions = restrictions->get_scoring_function_restrictions();
    if (scoring_restrictions.empty()) {
        throw exceptions::invalid_request_exception("Full-text search queries require a WHERE BM25() > 0 clause");
    }
    if (scoring_restrictions.size() > 1) {
        throw exceptions::invalid_request_exception("Full-text search queries support only one WHERE BM25() restriction");
    }

    ordering_info->deferred_where_term = validate_bm25_where_restriction(scoring_restrictions.front(), *ordering_info);

    // Reject any WHERE restrictions beyond the single BM25 clause.
    // BM25 restrictions are excluded from `restrictions`.
    if (!restrictions->partition_key_restrictions_is_empty()
            || !restrictions::is_empty_restriction(restrictions->get_clustering_columns_restrictions())
            || !restrictions::is_empty_restriction(restrictions->get_nonprimary_key_restrictions())) {
        throw exceptions::invalid_request_exception(
                "Full-text search queries do not support additional WHERE restrictions");
    }

    // The score is matched to a row by primary key.
    if (ordering_info->score_temporary_index) {
        external_search::fetch_primary_key_columns(*selection, *schema);
    }

    // The index stores none of the text a fragment is generated from, so it has to be read from
    // every row even when the query does not select the column.
    if (ordering_info->highlight_temporary_index) {
        selection->add_column_for_post_processing(ranked_column(*schema, ordering_info->index));
    }

    return ::make_shared<cql3::statements::fulltext_indexed_table_select_statement>(
            schema,
            bound_terms,
            parameters,
            std::move(selection),
            std::move(restrictions),
            std::move(group_by_cell_indices),
            is_reversed,
            std::move(ordering_comparator),
            std::move(limit),
            std::move(per_partition_limit),
            stats,
            std::move(*ordering_info),
            std::move(attrs));
}

fulltext_indexed_table_select_statement::fulltext_indexed_table_select_statement(schema_ptr schema, uint32_t bound_terms,
        lw_shared_ptr<const parameters> parameters, ::shared_ptr<selection::selection> selection,
        ::shared_ptr<const restrictions::statement_restrictions> restrictions,
        ::shared_ptr<std::vector<size_t>> group_by_cell_indices, bool is_reversed,
        ordering_comparator_type ordering_comparator, std::optional<expr::expression> limit,
        std::optional<expr::expression> per_partition_limit, cql_stats& stats,
        bm25_ordering_info ordering_info, std::unique_ptr<attributes> attrs)
    : external_index_select_statement{schema, bound_terms, parameters, selection, restrictions,
              group_by_cell_indices, is_reversed, ordering_comparator, limit, per_partition_limit,
              stats, ordering_info.index, std::move(attrs)}
    , _bm25_ordering_info{std::move(ordering_info)} {
}

future<shared_ptr<cql_transport::messages::result_message>> fulltext_indexed_table_select_statement::execute_search(
        query_processor& qp, service::query_state& state, const query_options& options, uint64_t limit) const {

    if (limit > max_fts_query_limit) {
        co_await coroutine::return_exception(exceptions::invalid_request_exception(
                fmt::format("Full-text search queries require a LIMIT that is not greater than {}. LIMIT was {}", max_fts_query_limit, limit)));
    }

    auto timeout = db::timeout_clock::now() + get_timeout(state.get_client_state(), options);
    auto aoe = abort_on_expiry(timeout);

    auto search_term_val = expr::evaluate(_bm25_ordering_info.search_term, options);
    if (search_term_val.is_null()) {
        co_await coroutine::return_exception(exceptions::invalid_request_exception("Full-text search query term must not be null"));
    }

    if (_bm25_ordering_info.deferred_where_term
            && expr::evaluate(*_bm25_ordering_info.deferred_where_term, options) != search_term_val) {
        co_await coroutine::return_exception(exceptions::invalid_request_exception(
                "Full-text search queries must use the same search term in both WHERE and ORDER BY clauses"));
    }

    for (const auto& sel_term : _bm25_ordering_info.deferred_select_terms) {
        if (expr::evaluate(sel_term.term, options) != search_term_val) {
            co_await coroutine::return_exception(exceptions::invalid_request_exception(seastar::format(
                    "{}() in SELECT must use the same search term as BM25() in WHERE and ORDER BY", sel_term.function_name)));
        }
    }

    auto search_term_bytes = std::move(search_term_val).to_bytes();
    sstring search_term_text = value_cast<sstring>(utf8_type->deserialize(search_term_bytes));

    auto pkeys = co_await qp.vector_store_client().bm25(_schema->ks_name(), _index.metadata().name(), _schema, search_term_text, limit, aoe.abort_source());
    if (!pkeys.has_value()) {
        co_await coroutine::return_exception(
                exceptions::invalid_request_exception(std::visit(vector_search::vector_store_client::fts_error_visitor{}, pkeys.error())));
    }

    throwing_assert(pkeys->size() <= limit);

    auto table_results = co_await query_base_table(qp, state, options, timeout, pkeys.value());

    const auto score_temporary_index = _bm25_ordering_info.score_temporary_index;
    const auto fragment_temporary_index = _bm25_ordering_info.highlight_temporary_index;

    auto provider = std::optional<external_search::external_search_provider>{};
    if (table_results && (score_temporary_index || fragment_temporary_index)) {
        // A fragment does not exist until the index has been sent the rows' text.
        auto columns = std::vector<const column_definition*>{};
        auto text_column = std::optional<size_t>{};
        if (fragment_temporary_index) {
            text_column = columns.size();
            columns.push_back(&ranked_column(*_schema, _bm25_ordering_info.index));
        }
        // Only the score is matched to a row by key; a fragment is matched by position, and the key
        // columns are read only when the score is selected.
        const auto& read = table_results.value();
        auto rows = external_search::join_table_results(*read.rows, read.command->slice, *_schema, *_selection,
                score_temporary_index ? &pkeys.value() : nullptr, columns);

        auto filled = std::vector<external_search::external_values>{};
        if (score_temporary_index) {
            external_search::drop_unscored_rows(rows, pkeys.value());
            filled.push_back(external_search::external_values{.temporary_index = *score_temporary_index,
                    .values = external_search::similarities_of(rows, pkeys.value())});
        }
        if (fragment_temporary_index) {
            auto fragments = co_await highlights_of(qp.vector_store_client(), *_schema, _index, search_term_text, rows, *text_column, aoe.abort_source());
            filled.push_back(external_search::external_values{.temporary_index = *fragment_temporary_index, .values = std::move(fragments)});
        }
        provider.emplace(std::move(filled), rows);
    }
    co_return co_await emit_result_set(std::move(table_results), options, provider ? &*provider : nullptr);
}

} // namespace cql3::statements
