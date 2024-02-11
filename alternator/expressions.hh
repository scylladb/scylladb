/*
 * Copyright 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <string>
#include <stdexcept>
#include <vector>
#include <unordered_set>
#include <string_view>

#include <seastar/util/noncopyable_function.hh>

#include "expressions_types.hh"
#include "utils/rjson.hh"

namespace alternator {

class expressions_syntax_error : public std::runtime_error {
public:
    using runtime_error::runtime_error;
};

parsed::update_expression parse_update_expression(std::string_view query);
std::vector<parsed::path> parse_projection_expression(std::string_view query);
parsed::condition_expression parse_condition_expression(std::string_view query, const char* caller);

void resolve_update_expression(parsed::update_expression& ue,
        const rjson::value* expression_attribute_names,
        const rjson::value* expression_attribute_values,
        std::unordered_set<std::string>& used_attribute_names,
        std::unordered_set<std::string>& used_attribute_values);
void resolve_projection_expression(std::vector<parsed::path>& pe,
        const rjson::value* expression_attribute_names,
        std::unordered_set<std::string>& used_attribute_names);
void resolve_condition_expression(parsed::condition_expression& ce,
        const rjson::value* expression_attribute_names,
        const rjson::value* expression_attribute_values,
        std::unordered_set<std::string>& used_attribute_names,
        std::unordered_set<std::string>& used_attribute_values);

void validate_value(const rjson::value& v, const char* caller);

bool condition_expression_on(const parsed::condition_expression& ce, std::string_view attribute);

// for_condition_expression_on() runs the given function on the attributes
// that the expression uses. It may run for the same attribute more than once
// if the same attribute is used more than once in the expression.
void for_condition_expression_on(const parsed::condition_expression& ce, const noncopyable_function<void(std::string_view)>& func);

// calculate_value() behaves slightly different (especially, different
// functions supported) when used in different types of expressions, as
// enumerated in this enum:
enum class calculate_value_caller {
    UpdateExpression, ConditionExpression, ConditionExpressionAlone
};

}

template <> struct fmt::formatter<alternator::calculate_value_caller> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(alternator::calculate_value_caller caller, fmt::format_context& ctx) const {
        std::string_view name = "unknown type of expression";
        switch (caller) {
            using enum alternator::calculate_value_caller;
            case UpdateExpression:
                name = "UpdateExpression";
                break;
            case ConditionExpression:
                name = "ConditionExpression";
                break;
            case ConditionExpressionAlone:
                name = "ConditionExpression";
                break;
        }
        return fmt::format_to(ctx.out(), "{}", name);
    }
};

namespace alternator {

rjson::value calculate_value(const parsed::value& v,
        calculate_value_caller caller,
        const rjson::value* previous_item);

rjson::value calculate_value(const parsed::set_rhs& rhs,
        const rjson::value* previous_item);


} /* namespace alternator */
