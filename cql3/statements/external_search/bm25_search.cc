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

std::optional<expr::expression> validate_restriction(const expr::binary_operator& binop, const expr::expression& search_term) {
    const auto& fc = expr::as<expr::function_call>(binop.lhs);
    // "WHERE BM25(c, t) > 0" was rewritten to BM25_SCORE() when the relation was prepared, and
    // BM25_RANK() was rejected there.
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

} // namespace cql3::statements::bm25_search
