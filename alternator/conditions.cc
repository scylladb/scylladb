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
#include "alternator/serialization.hh"
#include "alternator/error.hh"
#include "cql3/constants.hh"
#include <unordered_map>

namespace alternator {

static logging::logger clogger("alternator-conditions");

comparison_operator_type get_comparison_operator(const Json::Value& comparison_operator) {
    static std::unordered_map<std::string, comparison_operator_type> ops = {
            {"EQ", comparison_operator_type::EQ},
            {"LE", comparison_operator_type::LE},
            {"LT", comparison_operator_type::LT},
            {"GE", comparison_operator_type::GE},
            {"GT", comparison_operator_type::GT},
            {"BETWEEN", comparison_operator_type::BETWEEN},
            {"BEGINS_WITH", comparison_operator_type::BEGINS_WITH},
    }; //TODO(sarna): NE, IN, CONTAINS, NULL, NOT_NULL
    if (!comparison_operator.isString()) {
        throw api_error("ValidationException", format("Invalid comparison operator definition {}", comparison_operator.toStyledString()));
    }
    std::string op = comparison_operator.asString();
    auto it = ops.find(op);
    if (it == ops.end()) {
        throw api_error("ValidationException", format("Unsupported comparison operator {}", op));
    }
    return it->second;
}

::shared_ptr<cql3::restrictions::single_column_restriction::contains> make_map_element_restriction(const column_definition& cdef, const std::string& key, const Json::Value& value) {
    bytes raw_key = utf8_type->from_string(sstring(key));
    auto key_value = ::make_shared<cql3::constants::value>(cql3::raw_value::make_value(std::move(raw_key)));
    bytes raw_value = serialize_item(value);
    auto entry_value = ::make_shared<cql3::constants::value>(cql3::raw_value::make_value(std::move(raw_value)));
    return make_shared<cql3::restrictions::single_column_restriction::contains>(cdef, std::move(key_value), std::move(entry_value));
}

::shared_ptr<cql3::restrictions::statement_restrictions> get_filtering_restrictions(schema_ptr schema, const column_definition& attrs_col, const Json::Value& query_filter) {
    clogger.trace("Getting filtering restrictions for: {}", query_filter.toStyledString());
    auto filtering_restrictions = ::make_shared<cql3::restrictions::statement_restrictions>(schema, true);
    for (auto it = query_filter.begin(); it != query_filter.end(); ++it) {
        std::string column_name = it.key().asString();
        const Json::Value& condition = *it;

        Json::Value comp_definition = condition.get("ComparisonOperator", Json::Value());
        Json::Value attr_list = condition.get("AttributeValueList", Json::Value(Json::arrayValue));
        comparison_operator_type op = get_comparison_operator(comp_definition);

        if (schema->get_column_definition(to_bytes(column_name))) {
            //TODO(sarna): Implement filtering for keys
            throw api_error("ValidationException", "Filtering on key values is not implemented yet");
        }
        if (op != comparison_operator_type::EQ) {
            throw api_error("ValidationException", "Filtering is currently implemented for EQ operator only");
        }
        if (attr_list.size() != 1) {
            throw api_error("ValidationException", format("EQ restriction needs exactly 1 attribute value: {}", attr_list.toStyledString()));
        }

        filtering_restrictions->add_restriction(make_map_element_restriction(attrs_col, column_name, attr_list[0]), false, true);
    }
    return filtering_restrictions;
}

}
