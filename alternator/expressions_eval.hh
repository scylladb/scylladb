/*
 * Copyright 2020 ScyllaDB
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

#pragma once

#include <string>
#include <unordered_set>

#include "rjson.hh"
#include "schema_fwd.hh"

#include "expressions_types.hh"

namespace alternator {

// calculate_value() behaves slightly different (especially, different
// functions supported) when used in different types of expressions, as
// enumerated in this enum:
enum class calculate_value_caller {
    UpdateExpression, ConditionExpression, ConditionExpressionAlone
};

inline std::ostream& operator<<(std::ostream& out, calculate_value_caller caller) {
    switch (caller) {
        case calculate_value_caller::UpdateExpression:
            out << "UpdateExpression";
            break;
        case calculate_value_caller::ConditionExpression:
            out << "ConditionExpression";
            break;
        case calculate_value_caller::ConditionExpressionAlone:
            out << "ConditionExpression";
            break;
        default:
            out << "unknown type of expression";
            break;
    }
    return out;
}

bool check_CONTAINS(const rjson::value* v1, const rjson::value& v2);

rjson::value calculate_value(const parsed::value& v,
        calculate_value_caller caller,
        const rjson::value* expression_attribute_values,
        std::unordered_set<std::string>& used_attribute_names,
        std::unordered_set<std::string>& used_attribute_values,
        const rjson::value& update_info,
        schema_ptr schema,
        const rjson::value* previous_item);

bool verify_condition_expression(
        const parsed::condition_expression& condition_expression,
        std::unordered_set<std::string>& used_attribute_values,
        std::unordered_set<std::string>& used_attribute_names,
        const rjson::value& req,
        schema_ptr schema,
        const rjson::value* previous_item);

} /* namespace alternator */
