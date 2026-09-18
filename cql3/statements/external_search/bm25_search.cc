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

secondary_index::index index_for(data_dictionary::database db, const schema_ptr& schema, const column_definition& column) {
    for (const auto& index : db.find_column_family(schema).get_index_manager().list_indexes()) {
        if (index.supports_bm25_expression(column)) {
            return index;
        }
    }
    throw exceptions::invalid_request_exception("No fulltext index found for full-text search query");
}

const column_definition& indexed_column(const schema& schema, const secondary_index::index& index) {
    const auto* cdef = schema.get_column_definition(to_bytes(index.target_column()));
    throwing_assert(cdef);
    return *cdef;
}

sstring query_term(const cql3::raw_value& value) {
    return value_cast<sstring>(utf8_type->deserialize(cql3::raw_value(value).to_bytes()));
}

std::optional<expr::expression> validate_restriction(const expr::binary_operator& binop, const expr::expression& search_term) {
    const auto& fc = expr::as<expr::function_call>(binop.lhs);
    // "WHERE BM25(c, t) > 0" was rewritten to BM25_SCORE() when the relation was prepared, and
    // BM25_RANK() and BM25_HIGHLIGHT() were rejected there.
    throwing_assert(expr::is_native_function_call(fc, functions::BM25_SCORE_FUNCTION_NAME));
    auto where_term = external_search::extract_call_arguments(fc, "BM25").second;

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
    const auto& type = *indexed_column(schema, index).type;
    auto values = std::vector<cql3::raw_value>(rows.size(), cql3::raw_value::make_null());
    auto documents = std::vector<sstring>{};
    documents.reserve(rows.size());
    auto sent_rows = std::vector<size_t>{};
    sent_rows.reserve(rows.size());
    for (size_t row = 0; row < rows.size(); ++row) {
        const auto& text = rows[row].columns.at(column);
        if (rows[row].dropped || !text) {
            continue;
        }
        documents.push_back(value_cast<sstring>(type.deserialize(managed_bytes_view(*text))));
        sent_rows.push_back(row);
    }

    if (documents.empty()) {
        co_return values;
    }

    auto fragments = co_await client.highlight(schema.ks_name(), index.metadata().name(), search_term, std::move(documents), as);
    if (!fragments.has_value()) {
        co_await coroutine::return_exception(
                exceptions::invalid_request_exception(std::visit(vector_search::vector_store_client::fts_error_visitor{}, fragments.error())));
    }

    // The reply has one entry per document sent, in the order they were sent.
    throwing_assert(fragments->size() == sent_rows.size());
    for (size_t i = 0; i < sent_rows.size(); ++i) {
        if (const auto& fragment = (*fragments)[i]) {
            values[sent_rows[i]] = cql3::raw_value::make_value(utf8_type->decompose(*fragment));
        }
    }
    co_return values;
}

} // namespace cql3::statements::bm25_search
