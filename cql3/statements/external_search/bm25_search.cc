/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "bm25_search.hh"

#include "cql3/expr/expr-utils.hh"
#include "cql3/functions/scoring_fcts.hh"
#include "cql3/statements/external_search/external_function.hh"
#include "index/secondary_index_manager.hh"
#include "schema/schema.hh"
#include "utils/assert.hh"
#include "exceptions/exceptions.hh"
#include "types/types.hh"

#include <seastar/coroutine/exception.hh>

namespace cql3::statements::bm25_search {

sstring query_term(const cql3::raw_value& value) {
    return value_cast<sstring>(utf8_type->deserialize(cql3::raw_value(value).to_bytes()));
}

seastar::future<vector_search::vector_store_client::primary_keys> ask(vector_search::vector_store_client& client,
        const sstring& keyspace, const sstring& index_name, schema_ptr schema, const sstring& term, uint64_t wanted,
        seastar::abort_source& as) {
    auto answer = co_await client.bm25(keyspace, index_name, schema, term, wanted, as);
    if (!answer.has_value()) {
        co_await coroutine::return_exception(exceptions::invalid_request_exception(
                std::visit(vector_search::vector_store_client::fts_error_visitor{}, answer.error())));
    }
    co_return std::move(answer.value());
}

std::optional<expr::expression> validate_restriction(const expr::binary_operator& binop, const secondary_index::index& index,
        const expr::expression& search_term) {
    const auto& fc = expr::as<expr::function_call>(binop.lhs);
    // "WHERE BM25(c, t) > 0" was rewritten to BM25_SCORE() when the relation was prepared,
    // BM25_RANK() and BM25_HIGHLIGHT() were rejected there, and external_search_plan only hands a
    // BM25 search the relations that refer to it.
    throwing_assert(expr::is_native_function_call(fc, functions::BM25_SCORE_FUNCTION_NAME));
    auto [col, where_term] = external_search::extract_call_arguments(fc, "BM25");
    if (col->name_as_text() != index.target_column()) {
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

    const auto terms_equal = external_search::unevaluated_equality(where_term, search_term);
    if (terms_equal != external_search::equality::always) {
        if (terms_equal == external_search::equality::never) {
            throw exceptions::invalid_request_exception(
                    "Full-text search queries must use the same search term in both WHERE and ORDER BY clauses");
        }
        return std::move(where_term);
    }
    return std::nullopt;
}

seastar::future<std::vector<cql3::raw_value>> highlights_of(vector_search::vector_store_client& client, const schema& schema,
        const secondary_index::index& index, const sstring& search_term, std::span<const external_search::joined_row> rows, size_t column,
        seastar::abort_source& as) {
    const auto* cdef = schema.get_column_definition(to_bytes(index.target_column()));
    throwing_assert(cdef);
    const auto& type = *cdef->type;

    auto documents = std::vector<sstring>{};
    documents.reserve(rows.size());
    for (const auto& row : rows) {
        const auto& text = row.columns.at(column);
        documents.push_back(text ? value_cast<sstring>(type.deserialize(managed_bytes_view(*text))) : sstring());
    }

    if (documents.empty()) {
        // Nothing matched, so there is nothing to mark up and no reason to ask.
        co_return std::vector<cql3::raw_value>{};
    }

    auto fragments = co_await client.highlight(schema.ks_name(), index.metadata().name(), search_term, std::move(documents), as);
    if (!fragments.has_value()) {
        co_await coroutine::return_exception(exceptions::invalid_request_exception(
                std::visit(vector_search::vector_store_client::fts_error_visitor{}, fragments.error())));
    }

    auto values = std::vector<cql3::raw_value>{};
    values.reserve(fragments->size());
    for (const auto& fragment : *fragments) {
        values.push_back(fragment ? cql3::raw_value::make_value(utf8_type->decompose(*fragment)) : cql3::raw_value::make_null());
    }
    co_return values;
}

} // namespace cql3::statements::bm25_search
