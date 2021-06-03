/*
 * Copyright 2019-present ScyllaDB
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
#include "serialization.hh"
#include "base64.hh"
#include "conditions.hh"
#include "alternator/expressionsLexer.hpp"
#include "alternator/expressionsParser.hpp"
#include "utils/overloaded_functor.hh"
#include "error.hh"

#include "seastarx.hh"

#include <seastar/core/print.hh>
#include <seastar/util/log.hh>

#include <boost/algorithm/cxx11/any_of.hpp>
#include <boost/algorithm/cxx11/all_of.hpp>

#include <functional>
#include <unordered_map>

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

void path::check_depth_limit() {
    if (1 + _operators.size() > depth_limit) {
        throw expressions_syntax_error(format("Document path exceeded {} nesting levels", depth_limit));
    }
}

std::ostream& operator<<(std::ostream& os, const path& p) {
    os << p.root();
    for (const auto& op : p.operators()) {
        std::visit(overloaded_functor {
            [&] (const std::string& member) {
                os << '.' << member;
            },
            [&] (unsigned index) {
                os << '[' << index << ']';
            }
        }, op);
    }
    return os;
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

static std::optional<std::string> resolve_path_component(const std::string& column_name,
        const rjson::value* expression_attribute_names,
        std::unordered_set<std::string>& used_attribute_names) {
    if (column_name.size() > 0 && column_name.front() == '#') {
        if (!expression_attribute_names) {
            throw api_error::validation(
                    format("ExpressionAttributeNames missing, entry '{}' required by expression", column_name));
        }
        const rjson::value* value = rjson::find(*expression_attribute_names, column_name);
        if (!value || !value->IsString()) {
            throw api_error::validation(
                    format("ExpressionAttributeNames missing entry '{}' required by expression", column_name));
        }
        used_attribute_names.emplace(column_name);
        return std::string(rjson::to_string_view(*value));
    }
    return std::nullopt;
}

static void resolve_path(parsed::path& p,
        const rjson::value* expression_attribute_names,
        std::unordered_set<std::string>& used_attribute_names) {
    std::optional<std::string> r = resolve_path_component(p.root(), expression_attribute_names, used_attribute_names);
    if (r) {
        p.set_root(std::move(*r));
    }
    for (auto& op : p.operators()) {
        std::visit(overloaded_functor {
            [&] (std::string& s) {
                r = resolve_path_component(s, expression_attribute_names, used_attribute_names);
                if (r) {
                    s = std::move(*r);
                }
            },
            [&] (unsigned index) {
                // nothing to resolve
            }
        }, op);
    }
}

static void resolve_constant(parsed::constant& c,
        const rjson::value* expression_attribute_values,
        std::unordered_set<std::string>& used_attribute_values) {
    std::visit(overloaded_functor {
        [&] (const std::string& valref) {
            if (!expression_attribute_values) {
                throw api_error::validation(
                        format("ExpressionAttributeValues missing, entry '{}' required by expression", valref));
            }
            const rjson::value* value = rjson::find(*expression_attribute_values, valref);
            if (!value) {
                throw api_error::validation(
                        format("ExpressionAttributeValues missing entry '{}' required by expression", valref));
            }
            if (value->IsNull()) {
                throw api_error::validation(
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

// condition_expression_on() checks whether a condition_expression places any
// condition on the given attribute. It can be useful, for example, for
// checking whether the condition tries to restrict a key column.

static bool value_on(const parsed::value& v, std::string_view attribute) {
    return std::visit(overloaded_functor {
        [&] (const parsed::constant& c) {
            return false;
        },
        [&] (const parsed::value::function_call& f) {
            for (const parsed::value& value : f._parameters) {
                if (value_on(value, attribute)) {
                    return true;
                }
            }
            return false;
        },
        [&] (const parsed::path& p) {
            return p.root() == attribute;
        }
    }, v._value);
}

static bool primitive_condition_on(const parsed::primitive_condition& pc, std::string_view attribute) {
    for (const parsed::value& value : pc._values) {
        if (value_on(value, attribute)) {
            return true;
        }
    }
    return false;
}

bool condition_expression_on(const parsed::condition_expression& ce, std::string_view attribute) {
    return std::visit(overloaded_functor {
        [&] (const parsed::primitive_condition& cond) {
            return primitive_condition_on(cond, attribute);
        },
        [&] (const parsed::condition_expression::condition_list& list) {
            for (const parsed::condition_expression& cond : list.conditions) {
                if (condition_expression_on(cond, attribute)) {
                    return true;
                }
            }
            return false;
        }
    }, ce._expression);
}

// for_condition_expression_on() runs a given function over all the attributes
// mentioned in the expression. If the same attribute is mentioned more than
// once, the function will be called more than once for the same attribute.

static void for_value_on(const parsed::value& v, const noncopyable_function<void(std::string_view)>& func) {
    std::visit(overloaded_functor {
        [&] (const parsed::constant& c) { },
        [&] (const parsed::value::function_call& f) {
            for (const parsed::value& value : f._parameters) {
                for_value_on(value, func);
            }
        },
        [&] (const parsed::path& p) {
            func(p.root());
        }
    }, v._value);
}

void for_condition_expression_on(const parsed::condition_expression& ce, const noncopyable_function<void(std::string_view)>& func) {
    std::visit(overloaded_functor {
        [&] (const parsed::primitive_condition& cond) {
            for (const parsed::value& value : cond._values) {
                for_value_on(value, func);
            }
        },
        [&] (const parsed::condition_expression::condition_list& list) {
            for (const parsed::condition_expression& cond : list.conditions) {
                for_condition_expression_on(cond, func);
            }
        }
    }, ce._expression);
}

// The following calculate_value() functions calculate, or evaluate, a parsed
// expression. The parsed expression is assumed to have been "resolved", with
// the matching resolve_* function.

// Take two JSON-encoded list values (remember that a list value is
// {"L": [...the actual list]}) and return the concatenation, again as
// a list value.
static rjson::value list_concatenate(const rjson::value& v1, const rjson::value& v2) {
    const rjson::value* list1 = unwrap_list(v1);
    const rjson::value* list2 = unwrap_list(v2);
    if (!list1 || !list2) {
        throw api_error::validation("UpdateExpression: list_append() given a non-list");
    }
    rjson::value cat = rjson::copy(*list1);
    for (const auto& a : list2->GetArray()) {
        rjson::push_back(cat, rjson::copy(a));
    }
    rjson::value ret = rjson::empty_object();
    rjson::set(ret, "L", std::move(cat));
    return ret;
}

// calculate_size() is ConditionExpression's size() function, i.e., it takes
// a JSON-encoded value and returns its "size" as defined differently for the
// different types - also as a JSON-encoded number.
// It return a JSON-encoded "null" value if this value's type has no size
// defined. Comparisons against this non-numeric value will later fail.
static rjson::value calculate_size(const rjson::value& v) {
    // NOTE: If v is improperly formatted for our JSON value encoding, it
    // must come from the request itself, not from the database, so it makes
    // sense to throw a ValidationException if we see such a problem.
    if (!v.IsObject() || v.MemberCount() != 1) {
        throw api_error::validation(format("invalid object: {}", v));
    }
    auto it = v.MemberBegin();
    int ret;
    if (it->name == "S") {
        if (!it->value.IsString()) {
            throw api_error::validation(format("invalid string: {}", v));
        }
        ret = it->value.GetStringLength();
    } else if (it->name == "NS" || it->name == "SS" || it->name == "BS" || it->name == "L") {
        if (!it->value.IsArray()) {
            throw api_error::validation(format("invalid set: {}", v));
        }
        ret = it->value.Size();
    } else if (it->name == "M") {
        if (!it->value.IsObject()) {
            throw api_error::validation(format("invalid map: {}", v));
        }
        ret = it->value.MemberCount();
    } else if (it->name == "B") {
        if (!it->value.IsString()) {
            throw api_error::validation(format("invalid byte string: {}", v));
        }
        ret = base64_decoded_len(rjson::to_string_view(it->value));
    } else {
        rjson::value json_ret = rjson::empty_object();
        rjson::set(json_ret, "null", rjson::value(true));
        return json_ret;
    }
    rjson::value json_ret = rjson::empty_object();
    rjson::set(json_ret, "N", rjson::from_string(std::to_string(ret)));
    return json_ret;
}

static const rjson::value& calculate_value(const parsed::constant& c) {
    return std::visit(overloaded_functor {
        [&] (const parsed::constant::literal& v) -> const rjson::value& {
            return *v;
        },
        [&] (const std::string& valref) -> const rjson::value& {
            // Shouldn't happen, we should have called resolve_value() earlier
            // and replaced the value reference by the literal constant.
            throw std::logic_error("calculate_value() called before resolve_value()");
        }
    }, c._value);
}

static rjson::value to_bool_json(bool b) {
    rjson::value json_ret = rjson::empty_object();
    rjson::set(json_ret, "BOOL", rjson::value(b));
    return json_ret;
}

static bool known_type(std::string_view type) {
    static thread_local const std::unordered_set<std::string_view> types = {
            "N", "S", "B", "NS", "SS", "BS", "L", "M", "NULL", "BOOL"
    };
    return types.contains(type);
}

using function_handler_type = rjson::value(calculate_value_caller, const rjson::value*, const parsed::value::function_call&);
static const
std::unordered_map<std::string_view, function_handler_type*> function_handlers {
    {"list_append", [] (calculate_value_caller caller, const rjson::value* previous_item, const parsed::value::function_call& f) {
            if (caller != calculate_value_caller::UpdateExpression) {
                throw api_error::validation(
                        format("{}: list_append() not allowed here", caller));
            }
            if (f._parameters.size() != 2) {
                throw api_error::validation(
                        format("{}: list_append() accepts 2 parameters, got {}", caller, f._parameters.size()));
            }
            rjson::value v1 = calculate_value(f._parameters[0], caller, previous_item);
            rjson::value v2 = calculate_value(f._parameters[1], caller, previous_item);
            return list_concatenate(v1, v2);
        }
    },
    {"if_not_exists", [] (calculate_value_caller caller, const rjson::value* previous_item, const parsed::value::function_call& f) {
            if (caller != calculate_value_caller::UpdateExpression) {
                throw api_error::validation(
                        format("{}: if_not_exists() not allowed here", caller));
            }
            if (f._parameters.size() != 2) {
                throw api_error::validation(
                        format("{}: if_not_exists() accepts 2 parameters, got {}", caller, f._parameters.size()));
            }
            if (!std::holds_alternative<parsed::path>(f._parameters[0]._value)) {
                throw api_error::validation(
                        format("{}: if_not_exists() must include path as its first argument", caller));
            }
            rjson::value v1 = calculate_value(f._parameters[0], caller, previous_item);
            rjson::value v2 = calculate_value(f._parameters[1], caller, previous_item);
            return v1.IsNull() ? std::move(v2) : std::move(v1);
        }
    },
    {"size", [] (calculate_value_caller caller, const rjson::value* previous_item, const parsed::value::function_call& f) {
            if (caller != calculate_value_caller::ConditionExpression) {
                throw api_error::validation(
                        format("{}: size() not allowed here", caller));
            }
            if (f._parameters.size() != 1) {
                throw api_error::validation(
                        format("{}: size() accepts 1 parameter, got {}", caller, f._parameters.size()));
            }
            rjson::value v = calculate_value(f._parameters[0], caller, previous_item);
            return calculate_size(v);
        }
    },
    {"attribute_exists", [] (calculate_value_caller caller, const rjson::value* previous_item, const parsed::value::function_call& f) {
            if (caller != calculate_value_caller::ConditionExpressionAlone) {
                throw api_error::validation(
                        format("{}: attribute_exists() not allowed here", caller));
            }
            if (f._parameters.size() != 1) {
                throw api_error::validation(
                        format("{}: attribute_exists() accepts 1 parameter, got {}", caller, f._parameters.size()));
            }
            if (!std::holds_alternative<parsed::path>(f._parameters[0]._value)) {
                throw api_error::validation(
                        format("{}: attribute_exists()'s parameter must be a path", caller));
            }
            rjson::value v = calculate_value(f._parameters[0], caller, previous_item);
            return to_bool_json(!v.IsNull());
        }
    },
    {"attribute_not_exists", [] (calculate_value_caller caller, const rjson::value* previous_item, const parsed::value::function_call& f) {
            if (caller != calculate_value_caller::ConditionExpressionAlone) {
                throw api_error::validation(
                        format("{}: attribute_not_exists() not allowed here", caller));
            }
            if (f._parameters.size() != 1) {
                throw api_error::validation(
                        format("{}: attribute_not_exists() accepts 1 parameter, got {}", caller, f._parameters.size()));
            }
            if (!std::holds_alternative<parsed::path>(f._parameters[0]._value)) {
                throw api_error::validation(
                        format("{}: attribute_not_exists()'s parameter must be a path", caller));
            }
            rjson::value v = calculate_value(f._parameters[0], caller, previous_item);
            return to_bool_json(v.IsNull());
        }
    },
    {"attribute_type", [] (calculate_value_caller caller, const rjson::value* previous_item, const parsed::value::function_call& f) {
            if (caller != calculate_value_caller::ConditionExpressionAlone) {
                throw api_error::validation(
                        format("{}: attribute_type() not allowed here", caller));
            }
            if (f._parameters.size() != 2) {
                throw api_error::validation(
                        format("{}: attribute_type() accepts 2 parameters, got {}", caller, f._parameters.size()));
            }
            // There is no real reason for the following check (not
            // allowing the type to come from a document attribute), but
            // DynamoDB does this check, so we do too...
            if (!f._parameters[1].is_constant()) {
                throw api_error::validation(
                        format("{}: attribute_types()'s first parameter must be an expression attribute", caller));
            }
            rjson::value v0 = calculate_value(f._parameters[0], caller, previous_item);
            rjson::value v1 = calculate_value(f._parameters[1], caller, previous_item);
            if (v1.IsObject() && v1.MemberCount() == 1 && v1.MemberBegin()->name == "S") {
                // If the type parameter is not one of the legal types
                // we should generate an error, not a failed condition:
                if (!known_type(rjson::to_string_view(v1.MemberBegin()->value))) {
                    throw api_error::validation(
                            format("{}: attribute_types()'s second parameter, {}, is not a known type",
                                    caller, v1.MemberBegin()->value));
                }
                if (v0.IsObject() && v0.MemberCount() == 1) {
                    return to_bool_json(v1.MemberBegin()->value == v0.MemberBegin()->name);
                } else {
                    return to_bool_json(false);
                }
            } else {
                throw api_error::validation(
                        format("{}: attribute_type() second parameter must refer to a string, got {}", caller, v1));
            }
        }
    },
    {"begins_with", [] (calculate_value_caller caller, const rjson::value* previous_item, const parsed::value::function_call& f) {
            if (caller != calculate_value_caller::ConditionExpressionAlone) {
                throw api_error::validation(
                        format("{}: begins_with() not allowed here", caller));
            }
            if (f._parameters.size() != 2) {
                throw api_error::validation(
                        format("{}: begins_with() accepts 2 parameters, got {}", caller, f._parameters.size()));
            }
            rjson::value v1 = calculate_value(f._parameters[0], caller, previous_item);
            rjson::value v2 = calculate_value(f._parameters[1], caller, previous_item);
            return to_bool_json(check_BEGINS_WITH(v1.IsNull() ? nullptr : &v1,  v2,
                                    f._parameters[0].is_constant(), f._parameters[1].is_constant()));
        }
    },
    {"contains", [] (calculate_value_caller caller, const rjson::value* previous_item, const parsed::value::function_call& f) {
            if (caller != calculate_value_caller::ConditionExpressionAlone) {
                throw api_error::validation(
                        format("{}: contains() not allowed here", caller));
            }
            if (f._parameters.size() != 2) {
                throw api_error::validation(
                        format("{}: contains() accepts 2 parameters, got {}", caller, f._parameters.size()));
            }
            rjson::value v1 = calculate_value(f._parameters[0], caller, previous_item);
            rjson::value v2 = calculate_value(f._parameters[1], caller, previous_item);
            return to_bool_json(check_CONTAINS(v1.IsNull() ? nullptr : &v1,  v2));
        }
    },
};

// Given a parsed::path and an item read from the table, extract the value
// of a certain attribute path, such as "a" or "a.b.c[3]". Returns a null
// value if the item or the requested attribute does not exist.
// Note that the item is assumed to be encoded in JSON using DynamoDB
// conventions - each level of a nested document is a map with one key -
// a type (e.g., "M" for map) - and its value is the representation of
// that value.
static rjson::value extract_path(const rjson::value* item,
        const parsed::path& p, calculate_value_caller caller) {
    if (!item) {
        return rjson::null_value();
    }
    const rjson::value* v = rjson::find(*item, p.root());
    if (!v) {
        return rjson::null_value();
    }
    for (const auto& op : p.operators()) {
        if (!v->IsObject() || v->MemberCount() != 1) {
            // This shouldn't happen. We shouldn't have stored malformed
            // objects. But today Alternator does not validate the structure
            // of nested documents before storing them, so this can happen on
            // read.
            throw api_error::validation(format("{}: malformed item read: {}", *item));
        }
        const char* type = v->MemberBegin()->name.GetString();
        v = &(v->MemberBegin()->value);
        std::visit(overloaded_functor {
            [&] (const std::string& member) {
                if (type[0] == 'M' && v->IsObject()) {
                    v = rjson::find(*v, member);
                } else {
                    v = nullptr;
                }
            },
            [&] (unsigned index) {
                if (type[0] == 'L' && v->IsArray() && index < v->Size()) {
                    v = &(v->GetArray()[index]);
                } else {
                    v = nullptr;
                }
            }
        }, op);
        if (!v) {
            return rjson::null_value();
        }
    }
    return rjson::copy(*v);
}

// Given a parsed::value, which can refer either to a constant value from
// ExpressionAttributeValues, to the value of some attribute, or to a function
// of other values, this function calculates the resulting value.
// "caller" determines which expression - ConditionExpression or
// UpdateExpression - is asking for this value. We need to know this because
// DynamoDB allows a different choice of functions for different expressions.
rjson::value calculate_value(const parsed::value& v,
        calculate_value_caller caller,
        const rjson::value* previous_item) {
    return std::visit(overloaded_functor {
        [&] (const parsed::constant& c) -> rjson::value {
            return rjson::copy(calculate_value(c));
        },
        [&] (const parsed::value::function_call& f) -> rjson::value {
            auto function_it = function_handlers.find(std::string_view(f._function_name));
            if (function_it == function_handlers.end()) {
                throw api_error::validation(
                        format("{}: unknown function '{}' called.", caller, f._function_name));
            }
            return function_it->second(caller, previous_item, f);
        },
        [&] (const parsed::path& p) -> rjson::value {
            return extract_path(previous_item, p, caller);
        }
    }, v._value);
}

// Same as calculate_value() above, except takes a set_rhs, which may be
// either a single value, or v1+v2 or v1-v2.
rjson::value calculate_value(const parsed::set_rhs& rhs,
        const rjson::value* previous_item) {
    switch (rhs._op) {
    case 'v':
        return calculate_value(rhs._v1, calculate_value_caller::UpdateExpression, previous_item);
    case '+': {
        rjson::value v1 = calculate_value(rhs._v1, calculate_value_caller::UpdateExpression, previous_item);
        rjson::value v2 = calculate_value(rhs._v2, calculate_value_caller::UpdateExpression, previous_item);
        return number_add(v1, v2);
    }
    case '-': {
        rjson::value v1 = calculate_value(rhs._v1, calculate_value_caller::UpdateExpression, previous_item);
        rjson::value v2 = calculate_value(rhs._v2, calculate_value_caller::UpdateExpression, previous_item);
        return number_subtract(v1, v2);
    }
    }
    // Can't happen
    return rjson::null_value();
}

} // namespace alternator
