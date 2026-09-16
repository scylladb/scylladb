/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "scoring_fcts.hh"
#include "cql3/expr/expr-utils.hh"
#include "cql3/functions/functions.hh"
#include "data_dictionary/data_dictionary.hh"
#include "exceptions/exceptions.hh"
#include "schema/schema.hh"
#include "types/tuple.hh"
#include "types/types.hh"
#include "utils/log.hh"
#include <seastar/core/on_internal_error.hh>

#include <algorithm>
#include <cctype>

namespace cql3 {
namespace functions {

extern logging::logger log;

external_search_function::external_search_function(
        sstring name, data_type return_type, std::vector<data_type> arg_types, search_family family, search_value value)
    : native_scalar_function(std::move(name), std::move(return_type), std::move(arg_types))
    , _family(family)
    , _value(value)
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

expr::expression prepare_external_search_relation_lhs(expr::expression lhs, data_dictionary::database db, const schema& table_schema) {
    const auto* fc = expr::as_if<expr::function_call>(&lhs);
    const auto* fun = fc ? as_external_search_function(*fc) : nullptr;
    if (!fun) {
        return lhs;
    }
    switch (fun->value()) {
    case search_value::score:
        return lhs;
    case search_value::rank:
        throw exceptions::invalid_request_exception(format("{}() is not supported in the WHERE clause", fun->display_name()));
    case search_value::score_and_rank:
        break;
    }

    // Resolved through the ordinary overload lookup with the already prepared arguments, so this
    // works for functions created per call site (the ANN family) as well as for declared ones.
    std::vector<shared_ptr<assignment_testable>> provided_args;
    provided_args.reserve(fc->args.size());
    for (const auto& arg : fc->args) {
        provided_args.push_back(expr::as_assignment_testable(arg, expr::type_of(arg)));
    }
    const auto& score_name = fun->family() == search_family::bm25 ? BM25_SCORE_FUNCTION_NAME : ANN_SCORE_FUNCTION_NAME;
    return expr::function_call{
        .func = instance().get(db, table_schema.ks_name(), score_name, provided_args, table_schema.ks_name(), table_schema.cf_name(), nullptr),
        .args = fc->args,
        .lwt_cache_id = fc->lwt_cache_id,
    };
}

namespace {

/// The return type of the ANN-family function `name`.
data_type ann_return_type(const function_name& name) {
    if (name == ANN_SCORE_FUNCTION_NAME) {
        return float_type;
    }
    if (name == ANN_RANK_FUNCTION_NAME) {
        return int32_type;
    }
    return score_and_rank_type();
}

search_value ann_value(const function_name& name) {
    if (name == ANN_SCORE_FUNCTION_NAME) {
        return search_value::score;
    }
    return name == ANN_RANK_FUNCTION_NAME ? search_value::rank : search_value::score_and_rank;
}

} // anonymous namespace

data_type score_and_rank_type() {
    return tuple_type_impl::get_instance({float_type, int32_type});
}

bool is_ann_function_name(const function_name& name) {
    return name == ANN_FUNCTION_NAME || name == ANN_SCORE_FUNCTION_NAME || name == ANN_RANK_FUNCTION_NAME;
}

shared_ptr<function> make_bm25_function() {
    // bm25(column, query) -> (score, rank)
    // Registered with utf8_type args; ascii is implicitly coerced to utf8 by the type system.
    //
    // BM25 scores depend on document statistics, so the result is not determined by the visible arguments alone.
    return ::make_shared<external_search_function>(BM25_FUNCTION_NAME.name, score_and_rank_type(),
            std::vector<data_type>{utf8_type, utf8_type}, search_family::bm25, search_value::score_and_rank);
}

shared_ptr<function> make_bm25_score_function() {
    // bm25_score(column, query) -> float, the score element of what bm25() returns.
    return ::make_shared<external_search_function>(
            BM25_SCORE_FUNCTION_NAME.name, float_type, std::vector<data_type>{utf8_type, utf8_type}, search_family::bm25,
            search_value::score);
}

shared_ptr<function> make_bm25_rank_function() {
    // bm25_rank(column, query) -> int, the position of the row in the search's result, counted
    // from 1. A rank cannot be used in a relation: "WHERE BM25_RANK(c, t) < 3" would be a LIMIT,
    // not a filter.
    return ::make_shared<external_search_function>(
            BM25_RANK_FUNCTION_NAME.name, int32_type, std::vector<data_type>{utf8_type, utf8_type}, search_family::bm25,
            search_value::rank);
}

shared_ptr<function> make_ann_function(const function_name& name, const std::vector<data_type>& arg_types) {
    // ann(column, query_vector) -> (score, rank), ann_score() -> float and ann_rank() -> int.
    return ::make_shared<external_search_function>(name.name, ann_return_type(name), arg_types, search_family::ann, ann_value(name));
}

} // namespace functions
} // namespace cql3
