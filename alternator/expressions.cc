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
#include "error.hh"

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

// The following resolve_*() functions resolve references in parsed
// expressions of different types. Resolving a parsed expression means
// replacing:
//  1. In parsed::path objects, replace references like "#name" with the
//     attribute name from ExpressionAttributeNames,
//  2. In parsed::constant objects, replace references like ":value" with
//     the value from ExpressionAttributeValues.
// These function also track which name and value references were used, to
// allow complaining if some remain unused.
// Note that the resolve_*() functions modify the expressions in-place,
// so if we ever intend to cache parsed expression, we need to pass a copy
// into this function.
//
// Doing the "resolving" stage before the evaluation stage has two benefits.
// First, it allows us to be compatible with DynamoDB in catching unused
// names and values (see issue #6572). Second, in the FilterExpression case,
// we need to resolve the expression just once but then use it many times
// (once for each item to be filtered).

static void resolve_path(parsed::path& p,
        const rjson::value* expression_attribute_names,
        std::unordered_set<std::string>& used_attribute_names) {
    const std::string& column_name = p.root();
    if (column_name.size() > 0 && column_name.front() == '#') {
        if (!expression_attribute_names) {
            throw api_error("ValidationException",
                    format("ExpressionAttributeNames missing, entry '{}' required by expression", column_name));
        }
        const rjson::value* value = rjson::find(*expression_attribute_names, column_name);
        if (!value || !value->IsString()) {
            throw api_error("ValidationException",
                    format("ExpressionAttributeNames missing entry '{}' required by expression", column_name));
        }
        used_attribute_names.emplace(column_name);
        p.set_root(std::string(rjson::to_string_view(*value)));
    }
}

static void resolve_constant(parsed::constant& c,
        const rjson::value* expression_attribute_values,
        std::unordered_set<std::string>& used_attribute_values) {
    std::visit(overloaded_functor {
        [&] (const std::string& valref) {
            if (!expression_attribute_values) {
                throw api_error("ValidationException",
                        format("ExpressionAttributeValues missing, entry '{}' required by expression", valref));
            }
            const rjson::value* value = rjson::find(*expression_attribute_values, valref);
            if (!value) {
                throw api_error("ValidationException",
                        format("ExpressionAttributeValues missing entry '{}' required by expression", valref));
            }
            if (value->IsNull()) {
                throw api_error("ValidationException",
                        format("ExpressionAttributeValues null value for entry '{}' required by expression", valref));
            }
            validate_value(*value, "ExpressionAttributeValues");
            used_attribute_values.emplace(valref);
            c.set(*value);
        },
        [&] (const parsed::constant::literal& lit) {
            // Nothing to do, already resolved
        }
    }, c._value);

}

void resolve_value(parsed::value& rhs,
        const rjson::value* expression_attribute_names,
        const rjson::value* expression_attribute_values,
        std::unordered_set<std::string>& used_attribute_names,
        std::unordered_set<std::string>& used_attribute_values) {
    std::visit(overloaded_functor {
        [&] (parsed::constant& c) {
            resolve_constant(c, expression_attribute_values, used_attribute_values);
        },
        [&] (parsed::value::function_call& f) {
            for (parsed::value& value : f._parameters) {
                resolve_value(value, expression_attribute_names, expression_attribute_values,
                        used_attribute_names, used_attribute_values);
            }
        },
        [&] (parsed::path& p) {
            resolve_path(p, expression_attribute_names, used_attribute_names);
        }
    }, rhs._value);
}

void resolve_set_rhs(parsed::set_rhs& rhs,
        const rjson::value* expression_attribute_names,
        const rjson::value* expression_attribute_values,
        std::unordered_set<std::string>& used_attribute_names,
        std::unordered_set<std::string>& used_attribute_values) {
    resolve_value(rhs._v1, expression_attribute_names, expression_attribute_values,
            used_attribute_names, used_attribute_values);
    if (rhs._op != 'v') {
        resolve_value(rhs._v2, expression_attribute_names, expression_attribute_values,
                used_attribute_names, used_attribute_values);
    }
}

void resolve_update_expression(parsed::update_expression& ue,
        const rjson::value* expression_attribute_names,
        const rjson::value* expression_attribute_values,
        std::unordered_set<std::string>& used_attribute_names,
        std::unordered_set<std::string>& used_attribute_values) {
    for (parsed::update_expression::action& action : ue.actions()) {
        resolve_path(action._path, expression_attribute_names, used_attribute_names);
        std::visit(overloaded_functor {
            [&] (parsed::update_expression::action::set& a) {
                resolve_set_rhs(a._rhs, expression_attribute_names, expression_attribute_values,
                        used_attribute_names, used_attribute_values);
            },
            [&] (parsed::update_expression::action::remove& a) {
                // nothing to do
            },
            [&] (parsed::update_expression::action::add& a) {
                resolve_constant(a._valref, expression_attribute_values, used_attribute_values);
            },
            [&] (parsed::update_expression::action::del& a) {
                resolve_constant(a._valref, expression_attribute_values, used_attribute_values);
            }
        }, action._action);
    }
}

static void resolve_primitive_condition(parsed::primitive_condition& pc,
        const rjson::value* expression_attribute_names,
        const rjson::value* expression_attribute_values,
        std::unordered_set<std::string>& used_attribute_names,
        std::unordered_set<std::string>& used_attribute_values) {
    for (parsed::value& value : pc._values) {
        resolve_value(value,
                expression_attribute_names, expression_attribute_values,
                used_attribute_names, used_attribute_values);
    }
}

void resolve_condition_expression(parsed::condition_expression& ce,
        const rjson::value* expression_attribute_names,
        const rjson::value* expression_attribute_values,
        std::unordered_set<std::string>& used_attribute_names,
        std::unordered_set<std::string>& used_attribute_values) {
    std::visit(overloaded_functor {
        [&] (parsed::primitive_condition& cond) {
            resolve_primitive_condition(cond,
                    expression_attribute_names, expression_attribute_values,
                    used_attribute_names, used_attribute_values);
        },
        [&] (parsed::condition_expression::condition_list& list) {
            for (parsed::condition_expression& cond : list.conditions) {
                resolve_condition_expression(cond,
                        expression_attribute_names, expression_attribute_values,
                            used_attribute_names, used_attribute_values);

            }
        }
    }, ce._expression);
}

void resolve_projection_expression(std::vector<parsed::path>& pe,
        const rjson::value* expression_attribute_names,
        std::unordered_set<std::string>& used_attribute_names) {
    for (parsed::path& p : pe) {
        resolve_path(p, expression_attribute_names, used_attribute_names);
    }
}

} // namespace alternator
