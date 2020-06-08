/*
 * Copyright 2019 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "expressions.hh"
#include "alternator/expressionsLexer.hpp"
#include "alternator/expressionsParser.hpp"
#include "utils/overloaded_functor.hh"

#include "seastarx.hh"

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

parsed::condition_expression
parse_condition_expression(std::string query) {
    try {
        return do_with_parser(query,  std::mem_fn(&expressionsParser::condition_expression));
    } catch (...) {
        throw expressions_syntax_error(format("Failed parsing ConditionExpression '{}': {}", query, std::current_exception()));
    }
}

namespace parsed {

void update_expression::add(update_expression::action a) {
    std::visit(overloaded_functor {
        [&] (action::set&)    { seen_set = true; },
        [&] (action::remove&) { seen_remove = true; },
        [&] (action::add&)    { seen_add = true; },
        [&] (action::del&)    { seen_del = true; }
    }, a._action);
    _actions.push_back(std::move(a));
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

void condition_expression::append(condition_expression&& a, char op) {
    std::visit(overloaded_functor {
        [&] (condition_list& x) {
            // If 'a' has a single condition, we could, instead of inserting
            // it insert its single condition (possibly negated if a._negated)
            // But considering it we don't evaluate these expressions many
            // times, this optimization is not worth extra code complexity.
            if (!x.conditions.empty() && x.op != op) {
                // Shouldn't happen unless we have a bug in the parser
                throw std::logic_error("condition_expression::append called with mixed operators");
            }
            x.conditions.push_back(std::move(a));
            x.op = op;
        },
        [&] (primitive_condition& x) {
            // Shouldn't happen unless we have a bug in the parser
            throw std::logic_error("condition_expression::append called on primitive_condition");
        }
    }, _expression);
}


} // namespace parsed
} // namespace alternator
