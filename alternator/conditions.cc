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

#include <list>
#include <map>
#include <string_view>
#include "alternator/conditions.hh"
#include "alternator/error.hh"
#include "cql3/constants.hh"
#include <unordered_map>
#include "utils/rjson.hh"
#include "serialization.hh"
#include "base64.hh"
#include <stdexcept>
#include <boost/algorithm/cxx11/all_of.hpp>
#include <boost/algorithm/cxx11/any_of.hpp>
#include "utils/overloaded_functor.hh"

#include "expressions.hh"

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
        throw api_error::validation(format("Invalid comparison operator definition {}", rjson::print(comparison_operator)));
    }
    std::string op = comparison_operator.GetString();
    auto it = ops.find(op);
    if (it == ops.end()) {
        throw api_error::validation(format("Unsupported comparison operator {}", op));
    }
    return it->second;
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
    if (!array && expected(0)) {
        // If expected() allows an empty AttributeValueList, it is also fine
        // that it is missing.
        return;
    }
    if (!array || !array->IsArray()) {
        throw api_error::validation("With ComparisonOperator, AttributeValueList must be given and an array");
    }
    if (!expected(array->Size())) {
        throw api_error::validation(
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
    if (!set1.IsArray() || !set2.IsArray() || set1.Size() != set2.Size()) {
        return false;
    }
    std::set<const rjson::value*, rjson_engaged_ptr_comp> set1_raw;
    for (auto it = set1.Begin(); it != set1.End(); ++it) {
        set1_raw.insert(&*it);
    }
    for (const auto& a : set2.GetArray()) {
        if (!set1_raw.contains(&a)) {
            return false;
        }
    }
    return true;
}
// Moreover, the JSON being compared can be a nested document with outer
// layers of lists and maps and some inner set - and we need to get to that
// inner set to compare it correctly with check_EQ_for_sets() (issue #8514).
static bool check_EQ(const rjson::value* v1, const rjson::value& v2);
static bool check_EQ_for_lists(const rjson::value& list1, const rjson::value& list2) {
    if (!list1.IsArray() || !list2.IsArray() || list1.Size() != list2.Size()) {
        return false;
    }
    auto it1 = list1.Begin();
    auto it2 = list2.Begin();
    while (it1 != list1.End()) {
        // Note: Alternator limits an item's depth (rjson::parse() limits
        // it to around 37 levels), so this recursion is safe.
        if (!check_EQ(&*it1, *it2)) {
            return false;
        }
        ++it1;
        ++it2;
    }
    return true;
}
static bool check_EQ_for_maps(const rjson::value& list1, const rjson::value& list2) {
    if (!list1.IsObject() || !list2.IsObject() || list1.MemberCount() != list2.MemberCount()) {
        return false;
    }
    for (auto it1 = list1.MemberBegin(); it1 != list1.MemberEnd(); ++it1) {
        auto it2 = list2.FindMember(it1->name);
        if (it2 == list2.MemberEnd() || !check_EQ(&it1->value, it2->value)) {
            return false;
        }
    }
    return true;
}

// Check if two JSON-encoded values match with the EQ relation
static bool check_EQ(const rjson::value* v1, const rjson::value& v2) {
    if (v1 && v1->IsObject() && v1->MemberCount() == 1 && v2.IsObject() && v2.MemberCount() == 1) {
        auto it1 = v1->MemberBegin();
        auto it2 = v2.MemberBegin();
        if (it1->name != it2->name) {
            return false;
        }
        if (it1->name == "SS" || it1->name == "NS" || it1->name == "BS") {
            return check_EQ_for_sets(it1->value, it2->value);
        } else if(it1->name == "L") {
            return check_EQ_for_lists(it1->value, it2->value);
        } else if(it1->name == "M") {
            return check_EQ_for_maps(it1->value, it2->value);
        } else {
            // Other, non-nested types (number, string, etc.) can be compared
            // literally, comparing their JSON representation.
            return it1->value == it2->value;
        }
    } else {
        // If v1 and/or v2 are missing (IsNull()) the result should be false.
        // In the unlikely case that the object is malformed (issue #8070),
        // let's also return false.
        return false;
    }
}

// Check if two JSON-encoded values match with the NE relation
static bool check_NE(const rjson::value* v1, const rjson::value& v2) {
    return !check_EQ(v1, v2);
}

// Check if two JSON-encoded values match with the BEGINS_WITH relation
bool check_BEGINS_WITH(const rjson::value* v1, const rjson::value& v2,
                       bool v1_from_query, bool v2_from_query) {
    bool bad = false;
    if (!v1 || !v1->IsObject() || v1->MemberCount() != 1) {
        if (v1_from_query) {
            throw api_error::validation("begins_with() encountered malformed argument");
        } else {
            bad = true;
        }
    } else if (v1->MemberBegin()->name != "S" && v1->MemberBegin()->name != "B") {
        if (v1_from_query) {
            throw api_error::validation(format("begins_with supports only string or binary type, got: {}", *v1));
        } else {
            bad = true;
        }
    }
    if (!v2.IsObject() || v2.MemberCount() != 1) {
        if (v2_from_query) {
            throw api_error::validation("begins_with() encountered malformed argument");
        } else {
            bad = true;
        }
    } else if (v2.MemberBegin()->name != "S" && v2.MemberBegin()->name != "B") {
        if (v2_from_query) {
            throw api_error::validation(format("begins_with() supports only string or binary type, got: {}", v2));
        } else {
            bad = true;
        }
    }
    if (bad) {
        return false;
    }
    auto it1 = v1->MemberBegin();
    auto it2 = v2.MemberBegin();
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
        throw api_error::validation(
                        format("IN operator encountered malformed AttributeValue: {}", array[0]));
    }
    const auto& type = array[0].MemberBegin()->name;
    if (type != "S" && type != "N" && type != "B") {
        throw api_error::validation(
                        "IN operator requires AttributeValueList elements to be of type String, Number, or Binary ");
    }
    if (!val) {
        return false;
    }
    bool have_match = false;
    for (const auto& elem : array.GetArray()) {
        if (!elem.IsObject() || elem.MemberCount() != 1 || elem.MemberBegin()->name != type) {
            throw api_error::validation(
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

// Only types S, N or B (string, number or bytes) may be compared by the
// various comparion operators - lt, le, gt, ge, and between.
// Note that in particular, if the value is missing (v->IsNull()), this
// check returns false.
static bool check_comparable_type(const rjson::value& v) {
    if (!v.IsObject() || v.MemberCount() != 1) {
        return false;
    }
    const rjson::value& type = v.MemberBegin()->name;
    return type == "S" || type == "N" || type == "B";
}

// Check if two JSON-encoded values match with cmp.
template <typename Comparator>
bool check_compare(const rjson::value* v1, const rjson::value& v2, const Comparator& cmp,
                   bool v1_from_query, bool v2_from_query) {
    bool bad = false;
    if (!v1 || !check_comparable_type(*v1)) {
        if (v1_from_query) {
            throw api_error::validation(format("{} allow only the types String, Number, or Binary", cmp.diagnostic));
        }
        bad = true;
    }
    if (!check_comparable_type(v2)) {
        if (v2_from_query) {
            throw api_error::validation(format("{} allow only the types String, Number, or Binary", cmp.diagnostic));
        }
        bad = true;
    }
    if (bad) {
        return false;
    }
    const auto& kv1 = *v1->MemberBegin();
    const auto& kv2 = *v2.MemberBegin();
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
    // cannot reach here, as check_comparable_type() verifies the type is one
    // of the above options.
    return false;
}

struct cmp_lt {
    template <typename T> bool operator()(const T& lhs, const T& rhs) const { return lhs < rhs; }
    // We cannot use the normal comparison operators like "<" on the bytes
    // type, because they treat individual bytes as signed but we need to
    // compare them as *unsigned*. So we need a specialization for bytes.
    bool operator()(const bytes& lhs, const bytes& rhs) const { return compare_unsigned(lhs, rhs) < 0; }
    static constexpr const char* diagnostic = "LT operator";
};

struct cmp_le {
    template <typename T> bool operator()(const T& lhs, const T& rhs) const { return lhs <= rhs; }
    bool operator()(const bytes& lhs, const bytes& rhs) const { return compare_unsigned(lhs, rhs) <= 0; }
    static constexpr const char* diagnostic = "LE operator";
};

struct cmp_ge {
    template <typename T> bool operator()(const T& lhs, const T& rhs) const { return lhs >= rhs; }
    bool operator()(const bytes& lhs, const bytes& rhs) const { return compare_unsigned(lhs, rhs) >= 0; }
    static constexpr const char* diagnostic = "GE operator";
};

struct cmp_gt {
    template <typename T> bool operator()(const T& lhs, const T& rhs) const { return lhs > rhs; }
    bool operator()(const bytes& lhs, const bytes& rhs) const { return compare_unsigned(lhs, rhs) > 0; }
    static constexpr const char* diagnostic = "GT operator";
};

// True if v is between lb and ub, inclusive.  Throws or returns false
// (depending on bounds_from_query parameter) if lb > ub.
template <typename T>
static bool check_BETWEEN(const T& v, const T& lb, const T& ub, bool bounds_from_query) {
    if (cmp_lt()(ub, lb)) {
        if (bounds_from_query) {
            throw api_error::validation(
                format("BETWEEN operator requires lower_bound <= upper_bound, but {} > {}", lb, ub));
        } else {
            return false;
        }
    }
    return cmp_ge()(v, lb) && cmp_le()(v, ub);
}

static bool check_BETWEEN(const rjson::value* v, const rjson::value& lb, const rjson::value& ub,
                          bool v_from_query, bool lb_from_query, bool ub_from_query) {
    if ((v && v_from_query && !check_comparable_type(*v)) ||
        (lb_from_query && !check_comparable_type(lb)) ||
        (ub_from_query && !check_comparable_type(ub))) {
        throw api_error::validation("between allow only the types String, Number, or Binary");

    }
    if (!v || !v->IsObject() || v->MemberCount() != 1 ||
        !lb.IsObject() || lb.MemberCount() != 1 ||
        !ub.IsObject() || ub.MemberCount() != 1) {
        return false;
    }

    const auto& kv_v = *v->MemberBegin();
    const auto& kv_lb = *lb.MemberBegin();
    const auto& kv_ub = *ub.MemberBegin();
    bool bounds_from_query = lb_from_query && ub_from_query;
    if (kv_lb.name != kv_ub.name) {
        if (bounds_from_query) {
           throw api_error::validation(
                format("BETWEEN operator requires the same type for lower and upper bound; instead got {} and {}",
                       kv_lb.name, kv_ub.name));
        } else {
            return false;
        }
    }
    if (kv_v.name != kv_lb.name) { // Cannot compare different types, so v is NOT between lb and ub.
        return false;
    }
    if (kv_v.name == "N") {
        const char* diag = "BETWEEN operator";
        return check_BETWEEN(unwrap_number(*v, diag), unwrap_number(lb, diag), unwrap_number(ub, diag), bounds_from_query);
    }
    if (kv_v.name == "S") {
        return check_BETWEEN(std::string_view(kv_v.value.GetString(), kv_v.value.GetStringLength()),
                             std::string_view(kv_lb.value.GetString(), kv_lb.value.GetStringLength()),
                             std::string_view(kv_ub.value.GetString(), kv_ub.value.GetStringLength()),
                             bounds_from_query);
    }
    if (kv_v.name == "B") {
        return check_BETWEEN(base64_decode(kv_v.value), base64_decode(kv_lb.value), base64_decode(kv_ub.value), bounds_from_query);
    }
    if (v_from_query) {
        throw api_error::validation(
            format("BETWEEN operator requires AttributeValueList elements to be of type String, Number, or Binary; instead got {}",
               kv_lb.name));
    } else {
        return false;
    }
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
            throw api_error::validation("Cannot combine Value with Exists!=true");
        }
        if (comparison_operator) {
            throw api_error::validation("Cannot combine Value with ComparisonOperator");
        }
        return check_EQ(got, *value);
    } else if (exists) {
        if (comparison_operator) {
            throw api_error::validation("Cannot combine Exists with ComparisonOperator");
        }
        if (!exists->IsBool() || exists->GetBool() != false) {
            throw api_error::validation("Exists!=false requires Value");
        }
        // Remember Exists=false, so we're checking that the attribute does *not* exist:
        return !got;
    } else {
        if (!comparison_operator) {
            throw api_error::validation("Missing ComparisonOperator, Value or Exists");
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
            return check_compare(got, (*attribute_value_list)[0], cmp_lt{}, false, true);
        case comparison_operator_type::LE:
            verify_operand_count(attribute_value_list, exact_size(1), *comparison_operator);
            return check_compare(got, (*attribute_value_list)[0], cmp_le{}, false, true);
        case comparison_operator_type::GT:
            verify_operand_count(attribute_value_list, exact_size(1), *comparison_operator);
            return check_compare(got, (*attribute_value_list)[0], cmp_gt{}, false, true);
        case comparison_operator_type::GE:
            verify_operand_count(attribute_value_list, exact_size(1), *comparison_operator);
            return check_compare(got, (*attribute_value_list)[0], cmp_ge{}, false, true);
        case comparison_operator_type::BEGINS_WITH:
            verify_operand_count(attribute_value_list, exact_size(1), *comparison_operator);
            return check_BEGINS_WITH(got, (*attribute_value_list)[0], false, true);
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
            return check_BETWEEN(got, (*attribute_value_list)[0], (*attribute_value_list)[1],
                                 false, true, true);
        case comparison_operator_type::CONTAINS:
            {
                verify_operand_count(attribute_value_list, exact_size(1), *comparison_operator);
                // Expected's "CONTAINS" has this artificial limitation.
                // ConditionExpression's "contains()" does not...
                const rjson::value& arg = (*attribute_value_list)[0];
                const auto& argtype = (*arg.MemberBegin()).name;
                if (argtype != "S" && argtype != "N" && argtype != "B") {
                    throw api_error::validation(
                            format("CONTAINS operator requires a single AttributeValue of type String, Number, or Binary, "
                                    "got {} instead", argtype));
                }
                return check_CONTAINS(got, arg);
            }
        case comparison_operator_type::NOT_CONTAINS:
            {
                verify_operand_count(attribute_value_list, exact_size(1), *comparison_operator);
                // Expected's "NOT_CONTAINS" has this artificial limitation.
                // ConditionExpression's "contains()" does not...
                const rjson::value& arg = (*attribute_value_list)[0];
                const auto& argtype = (*arg.MemberBegin()).name;
                if (argtype != "S" && argtype != "N" && argtype != "B") {
                    throw api_error::validation(
                            format("CONTAINS operator requires a single AttributeValue of type String, Number, or Binary, "
                                    "got {} instead", argtype));
                }
                return check_NOT_CONTAINS(got, arg);
            }
        }
        throw std::logic_error(format("Internal error: corrupted operator enum: {}", int(op)));
    }
}

conditional_operator_type get_conditional_operator(const rjson::value& req) {
    const rjson::value* conditional_operator = rjson::find(req, "ConditionalOperator");
    if (!conditional_operator) {
        return conditional_operator_type::MISSING;
    }
    if (!conditional_operator->IsString()) {
        throw api_error::validation("'ConditionalOperator' parameter, if given, must be a string");
    }
    auto s = rjson::to_string_view(*conditional_operator);
    if (s == "AND") {
        return conditional_operator_type::AND;
    } else if (s == "OR") {
        return conditional_operator_type::OR;
    } else {
        throw api_error::validation(
                format("'ConditionalOperator' parameter must be AND, OR or missing. Found {}.", s));
    }
}

// Check if the existing values of the item (previous_item) match the
// conditions given by the Expected and ConditionalOperator parameters
// (if they exist) in the request (an UpdateItem, PutItem or DeleteItem).
// This function can throw an ValidationException API error if there
// are errors in the format of the condition itself.
bool verify_expected(const rjson::value& req, const rjson::value* previous_item) {
    const rjson::value* expected = rjson::find(req, "Expected");
    auto conditional_operator = get_conditional_operator(req);
    if (conditional_operator != conditional_operator_type::MISSING &&
        (!expected || (expected->IsObject() && expected->GetObject().ObjectEmpty()))) {
            throw api_error::validation("'ConditionalOperator' parameter cannot be specified for missing or empty Expression");
    }
    if (!expected) {
        return true;
    }
    if (!expected->IsObject()) {
        throw api_error::validation("'Expected' parameter, if given, must be an object");
    }
    bool require_all = conditional_operator != conditional_operator_type::OR;
    return verify_condition(*expected, require_all, previous_item);
}

bool verify_condition(const rjson::value& condition, bool require_all, const rjson::value* previous_item) {
    for (auto it = condition.MemberBegin(); it != condition.MemberEnd(); ++it) {
        const rjson::value* got = nullptr;
        if (previous_item) {
            got = rjson::find(*previous_item, rjson::to_string_view(it->name));
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

static bool calculate_primitive_condition(const parsed::primitive_condition& cond,
        const rjson::value* previous_item) {
    std::vector<rjson::value> calculated_values;
    calculated_values.reserve(cond._values.size());
    for (const parsed::value& v : cond._values) {
        calculated_values.push_back(calculate_value(v,
                cond._op == parsed::primitive_condition::type::VALUE ?
                        calculate_value_caller::ConditionExpressionAlone :
                        calculate_value_caller::ConditionExpression,
                previous_item));
    }
    switch (cond._op) {
    case parsed::primitive_condition::type::BETWEEN:
        if (calculated_values.size() != 3) {
            // Shouldn't happen unless we have a bug in the parser
            throw std::logic_error(format("Wrong number of values {} in BETWEEN primitive_condition", cond._values.size()));
        }
        return check_BETWEEN(&calculated_values[0], calculated_values[1], calculated_values[2],
                             cond._values[0].is_constant(), cond._values[1].is_constant(), cond._values[2].is_constant());
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
        throw api_error::validation(
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
        return check_compare(&calculated_values[0], calculated_values[1], cmp_gt{},
            cond._values[0].is_constant(), cond._values[1].is_constant());
    case parsed::primitive_condition::type::GE:
        return check_compare(&calculated_values[0], calculated_values[1], cmp_ge{},
            cond._values[0].is_constant(), cond._values[1].is_constant());
    case parsed::primitive_condition::type::LT:
        return check_compare(&calculated_values[0], calculated_values[1], cmp_lt{},
            cond._values[0].is_constant(), cond._values[1].is_constant());
    case parsed::primitive_condition::type::LE:
        return check_compare(&calculated_values[0], calculated_values[1], cmp_le{},
            cond._values[0].is_constant(), cond._values[1].is_constant());
    default:
        // Shouldn't happen unless we have a bug in the parser
        throw std::logic_error(format("Unknown type {} in primitive_condition object", (int)(cond._op)));
    }
}

// Check if the existing values of the item (previous_item) match the
// conditions given by the given parsed ConditionExpression.
bool verify_condition_expression(
        const parsed::condition_expression& condition_expression,
        const rjson::value* previous_item) {
    if (condition_expression.empty()) {
        return true;
    }
    bool ret = std::visit(overloaded_functor {
        [&] (const parsed::primitive_condition& cond) -> bool {
            return calculate_primitive_condition(cond, previous_item);
        },
        [&] (const parsed::condition_expression::condition_list& list) -> bool {
            auto verify_condition = [&] (const parsed::condition_expression& e) {
                return verify_condition_expression(e, previous_item);
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
