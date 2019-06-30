/*
 * Copyright 2019 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "expressions.hh"
#include "alternator/expressionsLexer.hpp"
#include "alternator/expressionsParser.hpp"

#include <seastarx.hh>

#include <seastar/core/print.hh>
#include <seastar/util/log.hh>

#include <functional>

namespace alternator {

template <typename Func, typename Result = std::result_of_t<Func(expressionsParser&)>>
Result do_with_parser(std::string input, Func&& f) {
    expressionsLexer::InputStreamType input_stream{
        reinterpret_cast<const ANTLR_UINT8*>(input.data()),
        ANTLR_ENC_UTF8,
        static_cast<ANTLR_UINT32>(input.size()),
        nullptr };
    expressionsLexer lexer(&input_stream);
    expressionsParser::TokenStreamType tstream(ANTLR_SIZE_HINT, lexer.get_tokSource());
    expressionsParser parser(&tstream);

    auto result = f(parser);
    return result;
}

parsed::update_expression
parse_update_expression(std::string query) {
    try {
        return do_with_parser(query,  std::mem_fn(&expressionsParser::update_expression));
    } catch (...) {
        throw expressions_syntax_error(format("Failed parsing UpdateExpression '{}': {}", query, std::current_exception()));
    }
}

std::vector<parsed::path>
parse_projection_expression(std::string query) {
    try {
        return do_with_parser(query,  std::mem_fn(&expressionsParser::projection_expression));
    } catch (...) {
        throw expressions_syntax_error(format("Failed parsing ProjectionExpression '{}': {}", query, std::current_exception()));
    }
}


namespace parsed {

void update_expression::add(update_expression::action a) {
    _actions.push_back(std::move(a));
    if (a.is_set()) {
        seen_set = true;
    } else if (a.is_remove()) {
        seen_remove = true;
    } else if (a.is_add()) {
        seen_add = true;
    } else if (a.is_del()) {
        seen_del = true;
    }
}

void update_expression::append(update_expression other) {
    if ((seen_set && other.seen_set) ||
        (seen_remove && other.seen_remove) ||
        (seen_add && other.seen_add) ||
        (seen_del && other.seen_del)) {
        throw expressions_syntax_error("Each of SET, REMOVE, ADD, DELETE may only appear once in UpdateExpression");
    }
    std::move(other._actions.begin(), other._actions.end(), std::back_inserter(_actions));
    seen_set |= other.seen_set;
    seen_remove |= other.seen_remove;
    seen_add |= other.seen_add;
    seen_del |= other.seen_del;
}

} // namespace parsed
} // namespace alternator
