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

#include <list>
#include <map>
#include <string_view>
#include "alternator/conditions.hh"
#include "alternator/error.hh"
#include "cql3/constants.hh"
#include <unordered_map>
#include "rjson.hh"
#include "serialization.hh"
#include "base64.hh"
#include <stdexcept>
#include <boost/algorithm/cxx11/all_of.hpp>
#include <boost/algorithm/cxx11/any_of.hpp>
#include "utils/overloaded_functor.hh"

#include "expressions_eval.hh"

namespace alternator {

static logging::logger clogger("alternator-conditions");

comparison_operator_type get_comparison_operator(const rjson::value& comparison_operator) {
    static std::unordered_map<std::string, comparison_operator_type> ops = {
            {"EQ", comparison_operator_type::EQ},
            {"NE", comparison_operator_type::NE},
            {"LE", comparison_operator_type::LE},
            {"LT", comparison_operator_type::LT},
            {"GE", comparison_operator_type::GE},
            {"GT", comparison_operator_type::GT},
            {"IN", comparison_operator_type::IN},
            {"NULL", comparison_operator_type::IS_NULL},
            {"NOT_NULL", comparison_operator_type::NOT_NULL},
            {"BETWEEN", comparison_operator_type::BETWEEN},
            {"BEGINS_WITH", comparison_operator_type::BEGINS_WITH},
            {"CONTAINS", comparison_operator_type::CONTAINS},
            {"NOT_CONTAINS", comparison_operator_type::NOT_CONTAINS},
    };
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

static ::shared_ptr<cql3::restrictions::single_column_restriction::contains> make_map_element_restriction(const column_definition& cdef, std::string_view key, const rjson::value& value) {
    bytes raw_key = utf8_type->from_string(sstring_view(key.data(), key.size()));
    auto key_value = ::make_shared<cql3::constants::value>(cql3::raw_value::make_value(std::move(raw_key)));
    bytes raw_value = serialize_item(value);
    auto entry_value = ::make_shared<cql3::constants::value>(cql3::raw_value::make_value(std::move(raw_value)));
    return make_shared<cql3::restrictions::single_column_restriction::contains>(cdef, std::move(key_value), std::move(entry_value));
}

static ::shared_ptr<cql3::restrictions::single_column_restriction::EQ> make_key_eq_restriction(const column_definition& cdef, const rjson::value& value) {
    bytes raw_value = get_key_from_typed_value(value, cdef);
    auto restriction_value = ::make_shared<cql3::constants::value>(cql3::raw_value::make_value(std::move(raw_value)));
    return make_shared<cql3::restrictions::single_column_restriction::EQ>(cdef, std::move(restriction_value));
}

::shared_ptr<cql3::restrictions::statement_restrictions> get_filtering_restrictions(schema_ptr schema, const column_definition& attrs_col, const rjson::value& query_filter) {
    clogger.trace("Getting filtering restrictions for: {}", rjson::print(query_filter));
    auto filtering_restrictions = ::make_shared<cql3::restrictions::statement_restrictions>(schema, true);
    for (auto it = query_filter.MemberBegin(); it != query_filter.MemberEnd(); ++it) {
        std::string_view column_name(it->name.GetString(), it->name.GetStringLength());
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
        if (const column_definition* cdef = schema->get_column_definition(to_bytes(column_name.data()))) {
            // Primary key restriction
            filtering_restrictions->add_restriction(make_key_eq_restriction(*cdef, attr_list[0]), false, true);
        } else {
            // Regular column restriction
            filtering_restrictions->add_restriction(make_map_element_restriction(attrs_col, column_name, attr_list[0]), false, true);
        }

    }
    return filtering_restrictions;
}

namespace {

struct size_check {
    // True iff size passes this check.
    virtual bool operator()(rapidjson::SizeType size) const = 0;
    // Check description, such that format("expected array {}", check.what()) is human-readable.
    virtual sstring what() const = 0;
};

class exact_size : public size_check {
    rapidjson::SizeType _expected;
  public:
    explicit exact_size(rapidjson::SizeType expected) : _expected(expected) {}
    bool operator()(rapidjson::SizeType size) const override { return size == _expected; }
    sstring what() const override { return format("of size {}", _expected); }
};

struct empty : public size_check {
    bool operator()(rapidjson::SizeType size) const override { return size < 1; }
    sstring what() const override { return "to be empty"; }
};

struct nonempty : public size_check {
    bool operator()(rapidjson::SizeType size) const override { return size > 0; }
    sstring what() const override { return "to be non-empty"; }
};

} // anonymous namespace

// Check that array has the expected number of elements
static void verify_operand_count(const rjson::value* array, const size_check& expected, const rjson::value& op) {
    if (!array || !array->IsArray()) {
        throw api_error("ValidationException", "With ComparisonOperator, AttributeValueList must be given and an array");
    }
    if (!expected(array->Size())) {
        throw api_error("ValidationException",
                        format("{} operator requires AttributeValueList {}, instead found list size {}",
                               op, expected.what(), array->Size()));
    }
}

struct rjson_engaged_ptr_comp {
    bool operator()(const rjson::value* p1, const rjson::value* p2) const {
        return rjson::single_value_comp()(*p1, *p2);
    }
};

// It's not enough to compare underlying JSON objects when comparing sets,
// as internally they're stored in an array, and the order of elements is
// not important in set equality. See issue #5021
static bool check_EQ_for_sets(const rjson::value& set1, const rjson::value& set2) {
    if (set1.Size() != set2.Size()) {
        return false;
    }
    std::set<const rjson::value*, rjson_engaged_ptr_comp> set1_raw;
    for (auto it = set1.Begin(); it != set1.End(); ++it) {
        set1_raw.insert(&*it);
    }
    for (const auto& a : set2.GetArray()) {
        if (set1_raw.count(&a) == 0) {
            return false;
        }
    }
    return true;
}

// Check if two JSON-encoded values match with the EQ relation
static bool check_EQ(const rjson::value* v1, const rjson::value& v2) {
    if (!v1) {
        return false;
    }
    if (v1->IsObject() && v1->MemberCount() == 1 && v2.IsObject() && v2.MemberCount() == 1) {
        auto it1 = v1->MemberBegin();
        auto it2 = v2.MemberBegin();
        if ((it1->name == "SS" && it2->name == "SS") || (it1->name == "NS" && it2->name == "NS") || (it1->name == "BS" && it2->name == "BS")) {
            return check_EQ_for_sets(it1->value, it2->value);
        }
    }
    return *v1 == v2;
}

// Check if two JSON-encoded values match with the NE relation
static bool check_NE(const rjson::value* v1, const rjson::value& v2) {
    return !v1 || *v1 != v2; // null is unequal to anything.
}

// Check if two JSON-encoded values match with the BEGINS_WITH relation
static bool check_BEGINS_WITH(const rjson::value* v1, const rjson::value& v2) {
    // BEGINS_WITH requires that its single operand (v2) be a string or
    // binary - otherwise it's a validation error. However, problems with
    // the stored attribute (v1) will just return false (no match).
    if (!v2.IsObject() || v2.MemberCount() != 1) {
        throw api_error("ValidationException", format("BEGINS_WITH operator encountered malformed AttributeValue: {}", v2));
    }
    auto it2 = v2.MemberBegin();
    if (it2->name != "S" && it2->name != "B") {
        throw api_error("ValidationException", format("BEGINS_WITH operator requires String or Binary in AttributeValue, got {}", it2->name));
    }


    if (!v1 || !v1->IsObject() || v1->MemberCount() != 1) {
        return false;
    }
    auto it1 = v1->MemberBegin();
    if (it1->name != it2->name) {
        return false;
    }
    if (it2->name == "S") {
        return rjson::to_string_view(it1->value).starts_with(rjson::to_string_view(it2->value));
    } else /* it2->name == "B" */ {
        return base64_begins_with(rjson::to_string_view(it1->value), rjson::to_string_view(it2->value));
    }
}

static bool is_set_of(const rjson::value& type1, const rjson::value& type2) {
    return (type2 == "S" && type1 == "SS") || (type2 == "N" && type1 == "NS") || (type2 == "B" && type1 == "BS");
}

// Check if two JSON-encoded values match with the CONTAINS relation
bool check_CONTAINS(const rjson::value* v1, const rjson::value& v2) {
    if (!v1) {
        return false;
    }
    const auto& kv1 = *v1->MemberBegin();
    const auto& kv2 = *v2.MemberBegin();
    if (kv2.name != "S" && kv2.name != "N" &&  kv2.name != "B") {
        throw api_error("ValidationException",
                        format("CONTAINS operator requires a single AttributeValue of type String, Number, or Binary, "
                               "got {} instead", kv2.name));
    }
    if (kv1.name == "S" && kv2.name == "S") {
        return rjson::to_string_view(kv1.value).find(rjson::to_string_view(kv2.value)) != std::string_view::npos;
    } else if (kv1.name == "B" && kv2.name == "B") {
        return base64_decode(kv1.value).find(base64_decode(kv2.value)) != bytes::npos;
    } else if (is_set_of(kv1.name, kv2.name)) {
        for (auto i = kv1.value.Begin(); i != kv1.value.End(); ++i) {
            if (*i == kv2.value) {
                return true;
            }
        }
    } else if (kv1.name == "L") {
        for (auto i = kv1.value.Begin(); i != kv1.value.End(); ++i) {
            if (!i->IsObject() || i->MemberCount() != 1) {
                clogger.error("check_CONTAINS received a list whose element is malformed");
                return false;
            }
            const auto& el = *i->MemberBegin();
            if (el.name == kv2.name && el.value == kv2.value) {
                return true;
            }
        }
    }
    return false;
}

// Check if two JSON-encoded values match with the NOT_CONTAINS relation
static bool check_NOT_CONTAINS(const rjson::value* v1, const rjson::value& v2) {
    if (!v1) {
        return false;
    }
    return !check_CONTAINS(v1, v2);
}

// Check if a JSON-encoded value equals any element of an array, which must have at least one element.
static bool check_IN(const rjson::value* val, const rjson::value& array) {
    if (!array[0].IsObject() || array[0].MemberCount() != 1) {
        throw api_error("ValidationException",
                        format("IN operator encountered malformed AttributeValue: {}", array[0]));
    }
    const auto& type = array[0].MemberBegin()->name;
    if (type != "S" && type != "N" && type != "B") {
        throw api_error("ValidationException",
                        "IN operator requires AttributeValueList elements to be of type String, Number, or Binary ");
    }
    if (!val) {
        return false;
    }
    bool have_match = false;
    for (const auto& elem : array.GetArray()) {
        if (!elem.IsObject() || elem.MemberCount() != 1 || elem.MemberBegin()->name != type) {
            throw api_error("ValidationException",
                            "IN operator requires all AttributeValueList elements to have the same type ");
        }
        if (!have_match && *val == elem) {
            // Can't return yet, must check types of all array elements. <sigh>
            have_match = true;
        }
    }
    return have_match;
}

// Another variant of check_IN, this one for ConditionExpression. It needs to
// check whether the first element in the given vector is equal to any of the
// others.
static bool check_IN(const std::vector<rjson::value>& array) {
    const rjson::value* first = &array[0];
    for (unsigned i = 1; i < array.size(); i++) {
        if (check_EQ(first, array[i])) {
            return true;
        }
    }
    return false;
}

static bool check_NULL(const rjson::value* val) {
    return val == nullptr;
}

static bool check_NOT_NULL(const rjson::value* val) {
    return val != nullptr;
}

// Check if two JSON-encoded values match with cmp.
template <typename Comparator>
bool check_compare(const rjson::value* v1, const rjson::value& v2, const Comparator& cmp) {
    if (!v2.IsObject() || v2.MemberCount() != 1) {
        throw api_error("ValidationException",
                        format("{} requires a single AttributeValue of type String, Number, or Binary",
                               cmp.diagnostic));
    }
    const auto& kv2 = *v2.MemberBegin();
    if (kv2.name != "S" && kv2.name != "N" && kv2.name != "B") {
        throw api_error("ValidationException",
                        format("{} requires a single AttributeValue of type String, Number, or Binary",
                               cmp.diagnostic));
    }
    if (!v1 || !v1->IsObject() || v1->MemberCount() != 1) {
        return false;
    }
    const auto& kv1 = *v1->MemberBegin();
    if (kv1.name != kv2.name) {
        return false;
    }
    if (kv1.name == "N") {
        return cmp(unwrap_number(*v1, cmp.diagnostic), unwrap_number(v2, cmp.diagnostic));
    }
    if (kv1.name == "S") {
        return cmp(std::string_view(kv1.value.GetString(), kv1.value.GetStringLength()),
                   std::string_view(kv2.value.GetString(), kv2.value.GetStringLength()));
    }
    if (kv1.name == "B") {
        return cmp(base64_decode(kv1.value), base64_decode(kv2.value));
    }
    clogger.error("check_compare panic: LHS type equals RHS type, but one is in {N,S,B} while the other isn't");
    return false;
}

struct cmp_lt {
    template <typename T> bool operator()(const T& lhs, const T& rhs) const { return lhs < rhs; }
    static constexpr const char* diagnostic = "LT operator";
};

struct cmp_le {
    // bytes only has <, so we cannot use <=.
    template <typename T> bool operator()(const T& lhs, const T& rhs) const { return lhs < rhs || lhs == rhs; }
    static constexpr const char* diagnostic = "LE operator";
};

struct cmp_ge {
    // bytes only has <, so we cannot use >=.
    template <typename T> bool operator()(const T& lhs, const T& rhs) const { return rhs < lhs || lhs == rhs; }
    static constexpr const char* diagnostic = "GE operator";
};

struct cmp_gt {
    // bytes only has <, so we cannot use >.
    template <typename T> bool operator()(const T& lhs, const T& rhs) const { return rhs < lhs; }
    static constexpr const char* diagnostic = "GT operator";
};

// True if v is between lb and ub, inclusive.  Throws if lb > ub.
template <typename T>
bool check_BETWEEN(const T& v, const T& lb, const T& ub) {
    if (ub < lb) {
        throw api_error("ValidationException",
                        format("BETWEEN operator requires lower_bound <= upper_bound, but {} > {}", lb, ub));
    }
    return cmp_ge()(v, lb) && cmp_le()(v, ub);
}

static bool check_BETWEEN(const rjson::value* v, const rjson::value& lb, const rjson::value& ub) {
    if (!v) {
        return false;
    }
    if (!v->IsObject() || v->MemberCount() != 1) {
        throw api_error("ValidationException", format("BETWEEN operator encountered malformed AttributeValue: {}", *v));
    }
    if (!lb.IsObject() || lb.MemberCount() != 1) {
        throw api_error("ValidationException", format("BETWEEN operator encountered malformed AttributeValue: {}", lb));
    }
    if (!ub.IsObject() || ub.MemberCount() != 1) {
        throw api_error("ValidationException", format("BETWEEN operator encountered malformed AttributeValue: {}", ub));
    }

    const auto& kv_v = *v->MemberBegin();
    const auto& kv_lb = *lb.MemberBegin();
    const auto& kv_ub = *ub.MemberBegin();
    if (kv_lb.name != kv_ub.name) {
        throw api_error(
                "ValidationException",
                format("BETWEEN operator requires the same type for lower and upper bound; instead got {} and {}",
                       kv_lb.name, kv_ub.name));
    }
    if (kv_v.name != kv_lb.name) { // Cannot compare different types, so v is NOT between lb and ub.
        return false;
    }
    if (kv_v.name == "N") {
        const char* diag = "BETWEEN operator";
        return check_BETWEEN(unwrap_number(*v, diag), unwrap_number(lb, diag), unwrap_number(ub, diag));
    }
    if (kv_v.name == "S") {
        return check_BETWEEN(std::string_view(kv_v.value.GetString(), kv_v.value.GetStringLength()),
                             std::string_view(kv_lb.value.GetString(), kv_lb.value.GetStringLength()),
                             std::string_view(kv_ub.value.GetString(), kv_ub.value.GetStringLength()));
    }
    if (kv_v.name == "B") {
        return check_BETWEEN(base64_decode(kv_v.value), base64_decode(kv_lb.value), base64_decode(kv_ub.value));
    }
    throw api_error("ValidationException",
        format("BETWEEN operator requires AttributeValueList elements to be of type String, Number, or Binary; instead got {}",
               kv_lb.name));
}

// Verify one Expect condition on one attribute (whose content is "got")
// for the verify_expected() below.
// This function returns true or false depending on whether the condition
// succeeded - it does not throw ConditionalCheckFailedException.
// However, it may throw ValidationException on input validation errors.
static bool verify_expected_one(const rjson::value& condition, const rjson::value* got) {
    const rjson::value* comparison_operator = rjson::find(condition, "ComparisonOperator");
    const rjson::value* attribute_value_list = rjson::find(condition, "AttributeValueList");
    const rjson::value* value = rjson::find(condition, "Value");
    const rjson::value* exists = rjson::find(condition, "Exists");
    // There are three types of conditions that Expected supports:
    // A value, not-exists, and a comparison of some kind. Each allows
    // and requires a different combinations of parameters in the request
    if (value) {
        if (exists && (!exists->IsBool() || exists->GetBool() != true)) {
            throw api_error("ValidationException", "Cannot combine Value with Exists!=true");
        }
        if (comparison_operator) {
            throw api_error("ValidationException", "Cannot combine Value with ComparisonOperator");
        }
        return check_EQ(got, *value);
    } else if (exists) {
        if (comparison_operator) {
            throw api_error("ValidationException", "Cannot combine Exists with ComparisonOperator");
        }
        if (!exists->IsBool() || exists->GetBool() != false) {
            throw api_error("ValidationException", "Exists!=false requires Value");
        }
        // Remember Exists=false, so we're checking that the attribute does *not* exist:
        return !got;
    } else {
        if (!comparison_operator) {
            throw api_error("ValidationException", "Missing ComparisonOperator, Value or Exists");
        }
        comparison_operator_type op = get_comparison_operator(*comparison_operator);
        switch (op) {
        case comparison_operator_type::EQ:
            verify_operand_count(attribute_value_list, exact_size(1), *comparison_operator);
            return check_EQ(got, (*attribute_value_list)[0]);
        case comparison_operator_type::NE:
            verify_operand_count(attribute_value_list, exact_size(1), *comparison_operator);
            return check_NE(got, (*attribute_value_list)[0]);
        case comparison_operator_type::LT:
            verify_operand_count(attribute_value_list, exact_size(1), *comparison_operator);
            return check_compare(got, (*attribute_value_list)[0], cmp_lt{});
        case comparison_operator_type::LE:
            verify_operand_count(attribute_value_list, exact_size(1), *comparison_operator);
            return check_compare(got, (*attribute_value_list)[0], cmp_le{});
        case comparison_operator_type::GT:
            verify_operand_count(attribute_value_list, exact_size(1), *comparison_operator);
            return check_compare(got, (*attribute_value_list)[0], cmp_gt{});
        case comparison_operator_type::GE:
            verify_operand_count(attribute_value_list, exact_size(1), *comparison_operator);
            return check_compare(got, (*attribute_value_list)[0], cmp_ge{});
        case comparison_operator_type::BEGINS_WITH:
            verify_operand_count(attribute_value_list, exact_size(1), *comparison_operator);
            return check_BEGINS_WITH(got, (*attribute_value_list)[0]);
        case comparison_operator_type::IN:
            verify_operand_count(attribute_value_list, nonempty(), *comparison_operator);
            return check_IN(got, *attribute_value_list);
        case comparison_operator_type::IS_NULL:
            verify_operand_count(attribute_value_list, empty(), *comparison_operator);
            return check_NULL(got);
        case comparison_operator_type::NOT_NULL:
            verify_operand_count(attribute_value_list, empty(), *comparison_operator);
            return check_NOT_NULL(got);
        case comparison_operator_type::BETWEEN:
            verify_operand_count(attribute_value_list, exact_size(2), *comparison_operator);
            return check_BETWEEN(got, (*attribute_value_list)[0], (*attribute_value_list)[1]);
        case comparison_operator_type::CONTAINS:
            verify_operand_count(attribute_value_list, exact_size(1), *comparison_operator);
            return check_CONTAINS(got, (*attribute_value_list)[0]);
        case comparison_operator_type::NOT_CONTAINS:
            verify_operand_count(attribute_value_list, exact_size(1), *comparison_operator);
            return check_NOT_CONTAINS(got, (*attribute_value_list)[0]);
        }
        throw std::logic_error(format("Internal error: corrupted operator enum: {}", int(op)));
    }
}

// Check if the existing values of the item (previous_item) match the
// conditions given by the Expected and ConditionalOperator parameters
// (if they exist) in the request (an UpdateItem, PutItem or DeleteItem).
// This function can throw an ValidationException API error if there
// are errors in the format of the condition itself.
bool verify_expected(const rjson::value& req, const std::unique_ptr<rjson::value>& previous_item) {
    const rjson::value* expected = rjson::find(req, "Expected");
    if (!expected) {
        return true;
    }
    if (!expected->IsObject()) {
        throw api_error("ValidationException", "'Expected' parameter, if given, must be an object");
    }
    // ConditionalOperator can be "AND" for requiring all conditions, or
    // "OR" for requiring one condition, and defaults to "AND" if missing.
    const rjson::value* conditional_operator = rjson::find(req, "ConditionalOperator");
    bool require_all = true;
    if (conditional_operator) {
        if (!conditional_operator->IsString()) {
            throw api_error("ValidationException", "'ConditionalOperator' parameter, if given, must be a string");
        }
        std::string_view s(conditional_operator->GetString(), conditional_operator->GetStringLength());
        if (s == "AND") {
            // require_all is already true
        } else if (s == "OR") {
            require_all = false;
        } else {
            throw api_error("ValidationException", "'ConditionalOperator' parameter must be AND, OR or missing");
        }
        if (expected->GetObject().ObjectEmpty()) {
            throw api_error("ValidationException", "'ConditionalOperator' parameter cannot be specified for empty Expression");
        }
    }

    for (auto it = expected->MemberBegin(); it != expected->MemberEnd(); ++it) {
        const rjson::value* got = nullptr;
        if (previous_item && previous_item->IsObject() && previous_item->HasMember("Item")) {
            got = rjson::find((*previous_item)["Item"], rjson::to_string_view(it->name));
        }
        bool success = verify_expected_one(it->value, got);
        if (success && !require_all) {
            // When !require_all, one success is enough!
            return true;
        } else if (!success && require_all) {
            // When require_all, one failure is enough!
            return false;
        }
    }
    // If we got here and require_all, none of the checks failed, so succeed.
    // If we got here and !require_all, all of the checks failed, so fail.
    return require_all;
}

bool calculate_primitive_condition(const parsed::primitive_condition& cond,
        std::unordered_set<std::string>& used_attribute_values,
        std::unordered_set<std::string>& used_attribute_names,
        const rjson::value& req,
        schema_ptr schema,
        const std::unique_ptr<rjson::value>& previous_item) {
    std::vector<rjson::value> calculated_values;
    calculated_values.reserve(cond._values.size());
    for (const parsed::value& v : cond._values) {
        calculated_values.push_back(calculate_value(v,
                cond._op == parsed::primitive_condition::type::VALUE ?
                        calculate_value_caller::ConditionExpressionAlone :
                        calculate_value_caller::ConditionExpression,
                rjson::find(req, "ExpressionAttributeValues"),
                used_attribute_names, used_attribute_values,
                req, schema, previous_item));
    }
    switch (cond._op) {
    case parsed::primitive_condition::type::BETWEEN:
        if (calculated_values.size() != 3) {
            // Shouldn't happen unless we have a bug in the parser
            throw std::logic_error(format("Wrong number of values {} in BETWEEN primitive_condition", cond._values.size()));
        }
        return check_BETWEEN(&calculated_values[0], calculated_values[1], calculated_values[2]);
    case parsed::primitive_condition::type::IN:
        return check_IN(calculated_values);
    case parsed::primitive_condition::type::VALUE:
        if (calculated_values.size() != 1) {
            // Shouldn't happen unless we have a bug in the parser
            throw std::logic_error(format("Unexpected values in primitive_condition", cond._values.size()));
        }
        // Unwrap the boolean wrapped as the value (if it is a boolean)
        if (calculated_values[0].IsObject() && calculated_values[0].MemberCount() == 1) {
            auto it = calculated_values[0].MemberBegin();
            if (it->name == "BOOL" && it->value.IsBool()) {
                return it->value.GetBool();
            }
        }
        throw api_error("ValidationException",
                format("ConditionExpression: condition results in a non-boolean value: {}",
                        calculated_values[0]));
    default:
        // All the rest of the operators have exactly two parameters (and unless
        // we have a bug in the parser, that's what we have in the parsed object:
        if (calculated_values.size() != 2) {
            throw std::logic_error(format("Wrong number of values {} in primitive_condition object", cond._values.size()));
        }
    }
    switch (cond._op) {
    case parsed::primitive_condition::type::EQ:
        return check_EQ(&calculated_values[0], calculated_values[1]);
    case parsed::primitive_condition::type::NE:
        return check_NE(&calculated_values[0], calculated_values[1]);
    case parsed::primitive_condition::type::GT:
        return check_compare(&calculated_values[0], calculated_values[1], cmp_gt{});
    case parsed::primitive_condition::type::GE:
        return check_compare(&calculated_values[0], calculated_values[1], cmp_ge{});
    case parsed::primitive_condition::type::LT:
        return check_compare(&calculated_values[0], calculated_values[1], cmp_lt{});
    case parsed::primitive_condition::type::LE:
        return check_compare(&calculated_values[0], calculated_values[1], cmp_le{});
    default:
        // Shouldn't happen unless we have a bug in the parser
        throw std::logic_error(format("Unknown type {} in primitive_condition object", (int)(cond._op)));
    }
}

// Check if the existing values of the item (previous_item) match the
// conditions given by the given parsed ConditionExpression.
bool verify_condition_expression(
        const parsed::condition_expression& condition_expression,
        std::unordered_set<std::string>& used_attribute_values,
        std::unordered_set<std::string>& used_attribute_names,
        const rjson::value& req,
        schema_ptr schema,
        const std::unique_ptr<rjson::value>& previous_item) {
    if (condition_expression.empty()) {
        return true;
    }
    bool ret = std::visit(overloaded_functor {
        [&] (const parsed::primitive_condition& cond) -> bool {
            return calculate_primitive_condition(cond, used_attribute_values,
                    used_attribute_names, req, schema, previous_item);
        },
        [&] (const parsed::condition_expression::condition_list& list) -> bool {
            auto verify_condition = [&] (const parsed::condition_expression& e) {
                return verify_condition_expression(e, used_attribute_values,
                        used_attribute_names, req, schema, previous_item);
            };
            switch (list.op) {
            case '&':
                return boost::algorithm::all_of(list.conditions, verify_condition);
            case '|':
                return boost::algorithm::any_of(list.conditions, verify_condition);
            default:
                // Shouldn't happen unless we have a bug in the parser
                throw std::logic_error("bad operator in condition_list");
            }
        }
    }, condition_expression._expression);
    return condition_expression._negated ? !ret : ret;
}

}
