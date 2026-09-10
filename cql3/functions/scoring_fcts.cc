/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "scoring_fcts.hh"
#include "cql3/expr/expr-utils.hh"
#include "utils/log.hh"
#include <seastar/core/on_internal_error.hh>

#include <algorithm>
#include <cctype>

namespace cql3 {
namespace functions {

extern logging::logger log;

external_search_function::external_search_function(
        sstring name, data_type return_type, std::vector<data_type> arg_types, search_family family)
    : native_scalar_function(std::move(name), std::move(return_type), std::move(arg_types))
    , _family(family)
    , _display_name(this->name().name) {
    std::ranges::transform(_display_name, _display_name.begin(), [] (unsigned char c) { return std::toupper(c); });
}

bytes_opt external_search_function::execute(std::span<const bytes_opt>) {
    on_internal_error(log, format("{}() reached scalar evaluation; prepare-time handling should have prevented this", name()));
}

const external_search_function* as_external_search_function(const expr::function_call& fc) {
    if (!expr::is_external_function_call(fc)) {
        return nullptr;
    }
    // function is a virtual base, so the downcast has to be a dynamic_cast.
    return dynamic_cast<const external_search_function*>(std::get<shared_ptr<function>>(fc.func).get());
}

shared_ptr<function> make_bm25_function() {
    // BM25 fulltext scoring function: bm25(column, query) -> float
    // Registered with utf8_type args; ascii is implicitly coerced to utf8 by the type system.
    //
    // BM25 scores depend on document statistics, so the result is not determined by the visible arguments alone.
    return ::make_shared<external_search_function>(
            BM25_FUNCTION_NAME.name, float_type, std::vector<data_type>{utf8_type, utf8_type}, search_family::bm25);
}

shared_ptr<function> make_ann_function(const std::vector<data_type>& arg_types) {
    // ANN vector ordering function: ann(column, query_vector) -> float
    return ::make_shared<external_search_function>(ANN_FUNCTION_NAME.name, float_type, arg_types, search_family::ann);
}

} // namespace functions
} // namespace cql3
