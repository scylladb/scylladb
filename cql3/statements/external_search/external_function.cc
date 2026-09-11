/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "cql3/statements/external_search/external_function.hh"

#include "cql3/column_identifier.hh"
#include "cql3/expr/expr-utils.hh"
#include "cql3/functions/scoring_fcts.hh"
#include "cql3/selection/selection.hh"
#include "exceptions/exceptions.hh"
#include "schema/schema.hh"
#include "types/types.hh"
#include "utils/assert.hh"

#include <utility>

namespace cql3::statements::external_search {

std::pair<const column_definition*, expr::expression> extract_call_arguments(const expr::function_call& fc,
        std::string_view function_name) {
    // Resolution rejects a call of the wrong arity, so a shorter one is an internal error.
    throwing_assert(fc.args.size() >= 2);

    const auto* col_val = expr::as_if<expr::column_value>(&fc.args[0]);
    if (!col_val) {
        throw exceptions::invalid_request_exception(
                seastar::format("First argument to {}() must be a column reference", function_name));
    }

    const expr::expression& query_value = fc.args[1];
    if (expr::find_in_expression<expr::column_value>(query_value, [] (const expr::column_value&) {
            return true;
        })) {
        throw exceptions::invalid_request_exception(
                seastar::format("Second argument to {}() must not be a column reference", function_name));
    }

    return {col_val->col, query_value};
}

equality unevaluated_equality(const expr::expression& a, const expr::expression& b) {
    if (const auto* a_const = expr::as_if<expr::constant>(&a)) {
        const auto* b_const = expr::as_if<expr::constant>(&b);
        if (!b_const) {
            // e.g. 'dog' against ?, settled once the marker is bound.
            return equality::unknown;
        }
        return *a_const == *b_const ? equality::always : equality::never;
    }

    if (const auto* a_bind = expr::as_if<expr::bind_variable>(&a)) {
        const auto* b_bind = expr::as_if<expr::bind_variable>(&b);
        // :x against :x - equal
        // anything else - :x or ? against 'dog', :y or ? - settled once the markers are bound.
        return b_bind && a_bind->bind_index == b_bind->bind_index ? equality::always : equality::unknown;
    }

    // Anything else is left to execution, even where trying harder here could settle it.
    return equality::unknown;
}

void fetch_primary_key_columns(selection::selection& selection, const schema& schema) {
    for (const auto& cdef : schema.primary_key_columns()) {
        selection.add_column_for_post_processing(cdef);
    }
}

expr::expression replace_search_call(functions::search_value value, const expr::expression& call, search_temporaries& temporaries,
        expr::temporary_allocator& allocator) {
    auto read = [&] (std::optional<size_t>& index, data_type type, std::optional<expr::expression> replaced) {
        if (!index) {
            index = allocator.allocate();
        }
        return expr::expression(expr::temporary{.index = *index, .type = std::move(type), .replaced_expr = std::move(replaced)});
    };

    switch (value) {
    case functions::search_value::score:
        return read(temporaries.score, float_type, call);
    case functions::search_value::rank:
        return read(temporaries.rank, int32_type, call);
    case functions::search_value::score_and_rank:
        return expr::expression(expr::tuple_constructor{
                .elements = {read(temporaries.score, float_type, std::nullopt), read(temporaries.rank, int32_type, std::nullopt)},
                .type = functions::score_and_rank_type(),
        });
    }
    std::unreachable();
}

void name_selector_as_written(selection::prepared_selector& selector, const expr::expression& written) {
    if (selector.alias) {
        return;
    }
    auto name = fmt::format("{:result_set_metadata}", written);
    if (fmt::format("{:result_set_metadata}", selector.expr) != name) {
        selector.alias = ::make_shared<column_identifier>(std::move(name), true);
    }
}

} // namespace cql3::statements::external_search
