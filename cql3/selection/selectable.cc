/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "cql3/query_options.hh"
#include "cql3/functions/functions.hh"
#include "cql3/functions/castas_fcts.hh"
#include "cql3/functions/aggregate_fcts.hh"
#include "cql3/expr/expression.hh"
#include "cql3/expr/expr-utils.hh"

namespace cql3 {

namespace selection {

seastar::logger slogger("cql3_selection");


expr::expression
make_count_rows_function_expression() {
    return expr::function_call{
            cql3::functions::function_name::native_function(cql3::functions::aggregate_fcts::COUNT_ROWS_FUNCTION_NAME),
                    std::vector<cql3::expr::expression>()};
}


bool
selectable_processes_selection(const expr::expression& selectable) {
    return expr::visit(overloaded_functor{
        [&] (const expr::constant&) -> bool {
            on_internal_error(slogger, "no way to express SELECT constant in the grammar yet");
        },
        [&] (const expr::conjunction& conj) -> bool {
            on_internal_error(slogger, "no way to express 'SELECT a AND b' in the grammar yet");
        },
        [&] (const expr::binary_operator& conj) -> bool {
            on_internal_error(slogger, "no way to express 'SELECT a binop b' in the grammar yet");
        },
        [] (const expr::subscript&) -> bool {
            on_internal_error(slogger, "no way to express 'SELECT a[b]' in the grammar yet");
        },
        [&] (const expr::column_value& column) -> bool {
            return false;
        },
        [&] (const expr::unresolved_identifier& ui) -> bool {
            on_internal_error(slogger, "selectable_processes_selection saw an unprepared column_identifier");
        },
        [&] (const expr::column_mutation_attribute& cma) -> bool {
            return true;
        },
        [&] (const expr::function_call& fc) -> bool {
            return true;
        },
        [&] (const expr::cast& c) -> bool {
            return true;
        },
        [&] (const expr::field_selection& fs) -> bool {
            return true;
        },
        [&] (const expr::bind_variable&) -> bool {
            on_internal_error(slogger, "bind_variable found its way to selector context");
        },
        [&] (const expr::untyped_constant&) -> bool {
            on_internal_error(slogger, "untyped_constant found its way to selector context");
        },
        [&] (const expr::tuple_constructor&) -> bool {
            on_internal_error(slogger, "tuple_constructor found its way to selector context");
        },
        [&] (const expr::collection_constructor&) -> bool {
            on_internal_error(slogger, "collection_constructor found its way to selector context");
        },
        [&] (const expr::usertype_constructor&) -> bool {
            on_internal_error(slogger, "collection_constructor found its way to selector context");
        },
        [&] (const expr::temporary& t) -> bool {
            // Well it doesn't process the selection, but it's not bypasses the selection completely
            // so we can't use the fast path. In any case it won't be seen.
            return true;
        },
    }, selectable);
};

}

}
