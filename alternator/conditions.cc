/*
 * Copyright 2019 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include <list>
#include <map>
#include "alternator/conditions.hh"
#include "alternator/error.hh"
#include "cql3/constants.hh"
#include <unordered_map>
#include "rjson.hh"

namespace alternator {

static logging::logger clogger("alternator-conditions");

comparison_operator_type get_comparison_operator(const rjson::value& comparison_operator) {
    static std::unordered_map<std::string, comparison_operator_type> ops = {
            {"EQ", comparison_operator_type::EQ},
            {"LE", comparison_operator_type::LE},
            {"LT", comparison_operator_type::LT},
            {"GE", comparison_operator_type::GE},
            {"GT", comparison_operator_type::GT},
            {"BETWEEN", comparison_operator_type::BETWEEN},
            {"BEGINS_WITH", comparison_operator_type::BEGINS_WITH},
    }; //TODO(sarna): NE, IN, CONTAINS, NULL, NOT_NULL
    if (!comparison_operator.IsString()) {
        throw api_error("ValidationException", format("Invalid comparison operator definition {}", rjson::print(comparison_operator)));
    }
    std::string op = comparison_operator.GetString();
    auto it = ops.find(op);
    if (it == ops.end()) {
        throw api_error("ValidationException", format("Unsupported comparison operator {}", op));
    }
    return it->second;
}

static ::shared_ptr<cql3::restrictions::single_column_restriction::contains> make_map_element_restriction(const column_definition& cdef, const std::string& key, const rjson::value& value) {
    bytes raw_key = utf8_type->from_string(sstring(key));
    auto key_value = ::make_shared<cql3::constants::value>(cql3::raw_value::make_value(std::move(raw_key)));
    bytes raw_value = serialize_item(value);
    auto entry_value = ::make_shared<cql3::constants::value>(cql3::raw_value::make_value(std::move(raw_value)));
    return make_shared<cql3::restrictions::single_column_restriction::contains>(cdef, std::move(key_value), std::move(entry_value));
}

static ::shared_ptr<cql3::restrictions::single_column_restriction::EQ> make_key_eq_restriction(const column_definition& cdef, const rjson::value& value) {
    bytes raw_value = get_key_from_typed_value(value, cdef, type_to_string(cdef.type));
    auto restriction_value = ::make_shared<cql3::constants::value>(cql3::raw_value::make_value(std::move(raw_value)));
    return make_shared<cql3::restrictions::single_column_restriction::EQ>(cdef, std::move(restriction_value));
}

::shared_ptr<cql3::restrictions::statement_restrictions> get_filtering_restrictions(schema_ptr schema, const column_definition& attrs_col, const rjson::value& query_filter) {
    clogger.trace("Getting filtering restrictions for: {}", rjson::print(query_filter));
    auto filtering_restrictions = ::make_shared<cql3::restrictions::statement_restrictions>(schema, true);
    for (auto it = query_filter.MemberBegin(); it != query_filter.MemberEnd(); ++it) {
        std::string column_name = it->name.GetString();
        const rjson::value& condition = it->value;

        const rjson::value& comp_definition = rjson::get(condition, "ComparisonOperator");
        const rjson::value& attr_list = rjson::get(condition, "AttributeValueList");
        comparison_operator_type op = get_comparison_operator(comp_definition);

        if (op != comparison_operator_type::EQ) {
            throw api_error("ValidationException", "Filtering is currently implemented for EQ operator only");
        }
        if (attr_list.Size() != 1) {
            throw api_error("ValidationException", format("EQ restriction needs exactly 1 attribute value: {}", rjson::print(attr_list)));
        }
        if (const column_definition* cdef = schema->get_column_definition(to_bytes(column_name))) {
            // Primary key restriction
            filtering_restrictions->add_restriction(make_key_eq_restriction(*cdef, attr_list[0]), false, true);
        } else {
            // Regular column restriction
            filtering_restrictions->add_restriction(make_map_element_restriction(attrs_col, column_name, attr_list[0]), false, true);
        }

    }
    return filtering_restrictions;
}

}
