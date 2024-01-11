/*
 * Copyright 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "utils/base64.hh"
#include "utils/rjson.hh"
#include "log.hh"
#include "serialization.hh"
#include "error.hh"
#include "concrete_types.hh"
#include "cql3/type_json.hh"
#include "mutation/position_in_partition.hh"

static logging::logger slogger("alternator-serialization");

namespace alternator {

bool is_alternator_keyspace(const sstring& ks_name);

type_info type_info_from_string(std::string_view type) {
    static thread_local const std::unordered_map<std::string_view, type_info> type_infos = {
        {"S", {alternator_type::S, utf8_type}},
        {"B", {alternator_type::B, bytes_type}},
        {"BOOL", {alternator_type::BOOL, boolean_type}},
        {"N", {alternator_type::N, decimal_type}}, //FIXME: Replace with custom Alternator type when implemented
    };
    auto it = type_infos.find(type);
    if (it == type_infos.end()) {
        return {alternator_type::NOT_SUPPORTED_YET, utf8_type};
    }
    return it->second;
}

type_representation represent_type(alternator_type atype) {
    static thread_local const std::unordered_map<alternator_type, type_representation> type_representations = {
        {alternator_type::S, {"S", utf8_type}},
        {alternator_type::B, {"B", bytes_type}},
        {alternator_type::BOOL, {"BOOL", boolean_type}},
        {alternator_type::N, {"N", decimal_type}}, //FIXME: Replace with custom Alternator type when implemented
    };
    auto it = type_representations.find(atype);
    if (it == type_representations.end()) {
        throw std::runtime_error(format("Unknown alternator type {}", int8_t(atype)));
    }
    return it->second;
}

// Get the magnitude and precision of a big_decimal - as these concepts are
// defined by DynamoDB - to allow us to enforce limits on those as explained
// in ssue #6794. The "magnitude" of 9e123 is 123 and of -9e-123 is -123,
// the "precision" of 12.34e56 is the number of significant digits - 4.
//
// Unfortunately it turned out to be quite difficult to take a big_decimal and
// calculate its magnitude and precision from its scale() and unscaled_value().
// So in the following ugly implementation we calculate them from the string
// representation instead. We assume the number was already parsed
// successfully to a big_decimal to it follows its syntax rules.
//
// FIXME: rewrite this function to take a big_decimal, not a string.
// Maybe a snippet like this can help:
// boost::multiprecision::cpp_int digits = boost::multiprecision::log10(num.unscaled_value().convert_to<boost::multiprecision::mpf_float_50>()).convert_to<boost::multiprecision::cpp_int>() + 1;


internal::magnitude_and_precision internal::get_magnitude_and_precision(std::string_view s) {
    size_t e_or_end = s.find_first_of("eE");
    std::string_view base = s.substr(0, e_or_end);
    if (s[0]=='-' || s[0]=='+') {
        base = base.substr(1);
    }
    int magnitude = 0;
    int precision = 0;
    size_t dot_or_end = base.find_first_of(".");
    size_t nonzero = base.find_first_not_of("0");
    if (dot_or_end != std::string_view::npos) {
        if (nonzero == dot_or_end) {
            // 0.000031 => magnitude = -5 (like 3.1e-5), precision = 2.
            std::string_view fraction = base.substr(dot_or_end + 1);
            size_t nonzero2 = fraction.find_first_not_of("0");
            if (nonzero2 != std::string_view::npos) {
                magnitude = -nonzero2 - 1;
                precision = fraction.size() - nonzero2;
            }
        } else {
            // 000123.45678 => magnitude = 2, precision = 8.
            magnitude = dot_or_end - nonzero - 1;
            precision = base.size() - nonzero - 1;
        }
        // trailing zeros don't count to precision, e.g., precision
        // of 1000.0, 1.0 or 1.0000 are just 1.
        size_t last_significant = base.find_last_not_of(".0");
        if (last_significant == std::string_view::npos) {
            precision = 0;
        } else if (last_significant < dot_or_end) {
            // e.g., 1000.00 reduce 5 = 7 - (0+1) - 1 from precision
            precision -= base.size() - last_significant - 2;
        } else {
            // e.g., 1235.60 reduce 5 = 7 - (5+1) from precision
            precision -= base.size() - last_significant - 1;
        }
    } else if (nonzero == std::string_view::npos) {
        // all-zero integer 000000
        magnitude = 0;
        precision = 0;
    } else {
        magnitude = base.size() - 1 - nonzero;
        precision = base.size() - nonzero;
        // trailing zeros don't count to precision, e.g., precision
        // of 1000 is just 1.
        size_t last_significant = base.find_last_not_of("0");
        if (last_significant == std::string_view::npos) {
            precision = 0;
        } else {
            // e.g., 1000 reduce 3 = 4 - (0+1)
            precision -= base.size() - last_significant - 1;
        }
    }
    if (precision && e_or_end != std::string_view::npos) {
        std::string_view exponent = s.substr(e_or_end + 1);
        if (exponent.size() > 4) {
            // don't even bother atoi(), exponent is too large
            magnitude = exponent[0]=='-' ? -9999 : 9999;
        } else {
            try {
                magnitude += boost::lexical_cast<int32_t>(exponent);
            } catch (...) {
                magnitude = 9999;
            }
        }
    }
    return magnitude_and_precision {magnitude, precision};
}

// Parse a number read from user input, validating that it has a valid
// numeric format and also in the allowed magnitude and precision ranges
// (see issue #6794). Throws an api_error::validation if the validation
// failed.
static big_decimal parse_and_validate_number(std::string_view s) {
    try {
        big_decimal ret(s);
        auto [magnitude, precision] = internal::get_magnitude_and_precision(s);
        if (magnitude > 125) {
            throw api_error::validation(format("Number overflow: {}. Attempting to store a number with magnitude larger than supported range.", s));
        }
        if (magnitude < -130) {
            throw api_error::validation(format("Number underflow: {}. Attempting to store a number with magnitude lower than supported range.", s));
        }
        if (precision > 38) {
            throw api_error::validation(format("Number too precise: {}. Attempting to store a number with more significant digits than supported.", s));
        }
        return ret;
    } catch (const marshal_exception& e) {
        throw api_error::validation(format("The parameter cannot be converted to a numeric value: {}", s));
    }

}

struct from_json_visitor {
    const rjson::value& v;
    bytes_ostream& bo;

    void operator()(const reversed_type_impl& t) const { visit(*t.underlying_type(), from_json_visitor{v, bo}); };
    void operator()(const string_type_impl& t) {
        bo.write(t.from_string(rjson::to_string_view(v)));
    }
    void operator()(const bytes_type_impl& t) const {
        // FIXME: it's difficult at this point to get information if value was provided
        // in request or comes from the storage, for now we assume it's user's fault.
        bo.write(*unwrap_bytes(v, true));
    }
    void operator()(const boolean_type_impl& t) const {
        bo.write(boolean_type->decompose(v.GetBool()));
    }
    void operator()(const decimal_type_impl& t) const {
        bo.write(decimal_type->decompose(parse_and_validate_number(rjson::to_string_view(v))));
    }
    // default
    void operator()(const abstract_type& t) const {
        bo.write(from_json_object(t, v));
    }
};

bytes serialize_item(const rjson::value& item) {
    if (item.IsNull() || item.MemberCount() != 1) {
        throw api_error::validation(format("An item can contain only one attribute definition: {}", item));
    }
    auto it = item.MemberBegin();
    type_info type_info = type_info_from_string(rjson::to_string_view(it->name)); // JSON keys are guaranteed to be strings

    if (type_info.atype == alternator_type::NOT_SUPPORTED_YET) {
        slogger.trace("Non-optimal serialization of type {}", it->name);
        return bytes{int8_t(type_info.atype)} + to_bytes(rjson::print(item));
    }

    bytes_ostream bo;
    bo.write(bytes{int8_t(type_info.atype)});
    visit(*type_info.dtype, from_json_visitor{it->value, bo});

    return bytes(bo.linearize());
}

struct to_json_visitor {
    rjson::value& deserialized;
    const std::string& type_ident;
    bytes_view bv;

    void operator()(const reversed_type_impl& t) const { visit(*t.underlying_type(), to_json_visitor{deserialized, type_ident, bv}); };
    void operator()(const decimal_type_impl& t) const {
        auto s = to_json_string(*decimal_type, bytes(bv));
        //FIXME(sarna): unnecessary copy
        rjson::add_with_string_name(deserialized, type_ident, rjson::from_string(s));
    }
    void operator()(const string_type_impl& t) {
        rjson::add_with_string_name(deserialized, type_ident, rjson::from_string(reinterpret_cast<const char *>(bv.data()), bv.size()));
    }
    void operator()(const bytes_type_impl& t) const {
        std::string b64 = base64_encode(bv);
        rjson::add_with_string_name(deserialized, type_ident, rjson::from_string(b64));
    }
    // default
    void operator()(const abstract_type& t) const {
        rjson::add_with_string_name(deserialized, type_ident, rjson::parse(to_json_string(t, bytes(bv))));
    }
};

rjson::value deserialize_item(bytes_view bv) {
    rjson::value deserialized(rapidjson::kObjectType);
    if (bv.empty()) {
        throw api_error::validation("Serialized value empty");
    }

    alternator_type atype = alternator_type(bv[0]);
    bv.remove_prefix(1);

    if (atype == alternator_type::NOT_SUPPORTED_YET) {
        slogger.trace("Non-optimal deserialization of alternator type {}", int8_t(atype));
        return rjson::parse(std::string_view(reinterpret_cast<const char *>(bv.data()), bv.size()));
    }
    type_representation type_representation = represent_type(atype);
    visit(*type_representation.dtype, to_json_visitor{deserialized, type_representation.ident, bv});

    return deserialized;
}

std::string type_to_string(data_type type) {
    static thread_local std::unordered_map<data_type, std::string> types = {
        {utf8_type, "S"},
        {bytes_type, "B"},
        {boolean_type, "BOOL"},
        {decimal_type, "N"}, // FIXME: use a specialized Alternator number type instead of the general decimal_type
    };
    auto it = types.find(type);
    if (it == types.end()) {
        // fall back to string, in order to be able to present
        // internal Scylla types in a human-readable way
        return "S";
    }
    return it->second;
}

bytes get_key_column_value(const rjson::value& item, const column_definition& column) {
    std::string column_name = column.name_as_text();
    const rjson::value* key_typed_value = rjson::find(item, column_name);
    if (!key_typed_value) {
        throw api_error::validation(format("Key column {} not found", column_name));
    }
    return get_key_from_typed_value(*key_typed_value, column);
}

// Parses the JSON encoding for a key value, which is a map with a single
// entry whose key is the type and the value is the encoded value.
// If this type does not match the desired "type_str", an api_error::validation
// error is thrown (the "name" parameter is the name of the column which will
// mentioned in the exception message).
// If the type does match, a reference to the encoded value is returned.
static const rjson::value& get_typed_value(const rjson::value& key_typed_value, std::string_view type_str, std::string_view name, std::string_view value_name) {
    if (!key_typed_value.IsObject() || key_typed_value.MemberCount() != 1 ||
            !key_typed_value.MemberBegin()->value.IsString()) {
        throw api_error::validation(
                format("Malformed value object for {} {}: {}",
                        value_name, name, key_typed_value));
    }

    auto it = key_typed_value.MemberBegin();
    if (rjson::to_string_view(it->name) != type_str) {
        throw api_error::validation(
                format("Type mismatch: expected type {} for {} {}, got type {}",
                        type_str, value_name, name, it->name));
    }
    return it->value;
}

// Parses the JSON encoding for a key value, which is a map with a single
// entry, whose key is the type (expected to match the key column's type)
// and the value is the encoded value.
bytes get_key_from_typed_value(const rjson::value& key_typed_value, const column_definition& column) {
    auto& value = get_typed_value(key_typed_value, type_to_string(column.type), column.name_as_text(), "key column");
    std::string_view value_view = rjson::to_string_view(value);
    if (value_view.empty()) {
        throw api_error::validation(
                format("The AttributeValue for a key attribute cannot contain an empty string value. Key: {}", column.name_as_text()));
    }
    if (column.type == bytes_type) {
        // FIXME: it's difficult at this point to get information if value was provided
        // in request or comes from the storage, for now we assume it's user's fault.
        return *unwrap_bytes(value, true);
    } else if (column.type == decimal_type) {
        return decimal_type->decompose(parse_and_validate_number(rjson::to_string_view(value)));
    } else {
        return column.type->from_string(value_view);
    }

}

rjson::value json_key_column_value(bytes_view cell, const column_definition& column) {
    if (column.type == bytes_type) {
        std::string b64 = base64_encode(cell);
        return rjson::from_string(b64);
    } if (column.type == utf8_type) {
        return rjson::from_string(reinterpret_cast<const char*>(cell.data()), cell.size());
    } else if (column.type == decimal_type) {
        // FIXME: use specialized Alternator number type, not the more
        // general "decimal_type". A dedicated type can be more efficient
        // in storage space and in parsing speed.
        auto s = to_json_string(*decimal_type, bytes(cell));
        return rjson::from_string(s);
    } else {
        // Support for arbitrary key types is useful for parsing values of virtual tables,
        // which can involve any type supported by Scylla.
        // In order to guarantee that the returned type is parsable by alternator clients,
        // they are represented simply as strings.
        return rjson::from_string(column.type->to_string(bytes(cell)));
    }
}


partition_key pk_from_json(const rjson::value& item, schema_ptr schema) {
    std::vector<bytes> raw_pk;
    // FIXME: this is a loop, but we really allow only one partition key column.
    for (const column_definition& cdef : schema->partition_key_columns()) {
        bytes raw_value = get_key_column_value(item, cdef);
        raw_pk.push_back(std::move(raw_value));
    }
   return partition_key::from_exploded(raw_pk);
}

clustering_key ck_from_json(const rjson::value& item, schema_ptr schema) {
    if (schema->clustering_key_size() == 0) {
        return clustering_key::make_empty();
    }
    std::vector<bytes> raw_ck;
    // FIXME: this is a loop, but we really allow only one clustering key column.
    for (const column_definition& cdef : schema->clustering_key_columns()) {
        bytes raw_value = get_key_column_value(item,  cdef);
        raw_ck.push_back(std::move(raw_value));
    }

    return clustering_key::from_exploded(raw_ck);
}

position_in_partition pos_from_json(const rjson::value& item, schema_ptr schema) {
    auto ck = ck_from_json(item, schema);
    if (is_alternator_keyspace(schema->ks_name())) {
        return position_in_partition::for_key(std::move(ck));
    }
    const auto region_item = rjson::find(item, scylla_paging_region);
    const auto weight_item = rjson::find(item, scylla_paging_weight);
    if (bool(region_item) != bool(weight_item)) {
        throw api_error::validation("Malformed value object: region and weight has to be either both missing or both present");
    }
    bound_weight weight;
    if (region_item) {
        auto region_view = rjson::to_string_view(get_typed_value(*region_item, "S", scylla_paging_region, "key region"));
        auto weight_view = rjson::to_string_view(get_typed_value(*weight_item, "N", scylla_paging_weight, "key weight"));
        auto region = parse_partition_region(region_view);
        if (weight_view == "-1") {
            weight = bound_weight::before_all_prefixed;
        } else if (weight_view == "0") {
            weight = bound_weight::equal;
        } else if (weight_view == "1") {
            weight = bound_weight::after_all_prefixed;
        } else {
            throw std::runtime_error(fmt::format("Invalid value for weight: {}", weight_view));
        }
        return position_in_partition(region, weight, region == partition_region::clustered ? std::optional(std::move(ck)) : std::nullopt);
    }
    if (ck.is_empty()) {
        return position_in_partition::for_partition_start();
    }
    return position_in_partition::for_key(std::move(ck));
}

big_decimal unwrap_number(const rjson::value& v, std::string_view diagnostic) {
    if (!v.IsObject() || v.MemberCount() != 1) {
        throw api_error::validation(format("{}: invalid number object", diagnostic));
    }
    auto it = v.MemberBegin();
    if (it->name != "N") {
        throw api_error::validation(format("{}: expected number, found type '{}'", diagnostic, it->name));
    }
    if (!it->value.IsString()) {
        // We shouldn't reach here. Callers normally validate their input
        // earlier with validate_value().
        throw api_error::validation(format("{}: improperly formatted number constant", diagnostic));
    }
    big_decimal ret = parse_and_validate_number(rjson::to_string_view(it->value));
    return ret;
}

std::optional<big_decimal> try_unwrap_number(const rjson::value& v) {
    if (!v.IsObject() || v.MemberCount() != 1) {
        return std::nullopt;
    }
    auto it = v.MemberBegin();
    if (it->name != "N" || !it->value.IsString()) {
        return std::nullopt;
    }
    try {
        return parse_and_validate_number(rjson::to_string_view(it->value));
    } catch (api_error&) {
        return std::nullopt;
    }
}

std::optional<bytes> unwrap_bytes(const rjson::value& value, bool from_query) {
    try {
        return rjson::base64_decode(value);
    } catch (...) {
        if (from_query) {
            throw api_error::serialization(format("Invalid base64 data"));
        }
        return std::nullopt;
    }
}

const std::pair<std::string, const rjson::value*> unwrap_set(const rjson::value& v) {
    if (!v.IsObject() || v.MemberCount() != 1) {
        return {"", nullptr};
    }
    auto it = v.MemberBegin();
    const std::string it_key = it->name.GetString();
    if (it_key != "SS" && it_key != "BS" && it_key != "NS") {
        return {std::move(it_key), nullptr};
    }
    return std::make_pair(it_key, &(it->value));
}

const rjson::value* unwrap_list(const rjson::value& v) {
    if (!v.IsObject() || v.MemberCount() != 1) {
        return nullptr;
    }
    auto it = v.MemberBegin();
    if (it->name != std::string("L")) {
        return nullptr;
    }
    return &(it->value);
}

// Take two JSON-encoded numeric values ({"N": "thenumber"}) and return the
// sum, again as a JSON-encoded number.
rjson::value number_add(const rjson::value& v1, const rjson::value& v2) {
    auto n1 = unwrap_number(v1, "UpdateExpression");
    auto n2 = unwrap_number(v2, "UpdateExpression");
    rjson::value ret = rjson::empty_object();
    sstring str_ret = (n1 + n2).to_string();
    rjson::add(ret, "N", rjson::from_string(str_ret));
    return ret;
}

rjson::value number_subtract(const rjson::value& v1, const rjson::value& v2) {
    auto n1 = unwrap_number(v1, "UpdateExpression");
    auto n2 = unwrap_number(v2, "UpdateExpression");
    rjson::value ret = rjson::empty_object();
    sstring str_ret = (n1 - n2).to_string();
    rjson::add(ret, "N", rjson::from_string(str_ret));
    return ret;
}

// Take two JSON-encoded set values (e.g. {"SS": [...the actual set]}) and
// return the sum of both sets, again as a set value.
rjson::value set_sum(const rjson::value& v1, const rjson::value& v2) {
    auto [set1_type, set1] = unwrap_set(v1);
    auto [set2_type, set2] = unwrap_set(v2);
    if (set1_type != set2_type) {
        throw api_error::validation(format("Mismatched set types: {} and {}", set1_type, set2_type));
    }
    if (!set1 || !set2) {
        throw api_error::validation("UpdateExpression: ADD operation for sets must be given sets as arguments");
    }
    rjson::value sum = rjson::copy(*set1);
    std::set<rjson::value, rjson::single_value_comp> set1_raw;
    for (auto it = sum.Begin(); it != sum.End(); ++it) {
        set1_raw.insert(rjson::copy(*it));
    }
    for (const auto& a : set2->GetArray()) {
        if (!set1_raw.contains(a)) {
            rjson::push_back(sum, rjson::copy(a));
        }
    }
    rjson::value ret = rjson::empty_object();
    rjson::add_with_string_name(ret, set1_type, std::move(sum));
    return ret;
}

// Take two JSON-encoded set values (e.g. {"SS": [...the actual list]}) and
// return the difference of s1 - s2, again as a set value.
// DynamoDB does not allow empty sets, so if resulting set is empty, return
// an unset optional instead.
std::optional<rjson::value> set_diff(const rjson::value& v1, const rjson::value& v2) {
    auto [set1_type, set1] = unwrap_set(v1);
    auto [set2_type, set2] = unwrap_set(v2);
    if (set1_type != set2_type) {
        throw api_error::validation(format("Set DELETE type mismatch: {} and {}", set1_type, set2_type));
    }
    if (!set1 || !set2) {
        throw api_error::validation("UpdateExpression: DELETE operation can only be performed on a set");
    }
    std::set<rjson::value, rjson::single_value_comp> set1_raw;
    for (auto it = set1->Begin(); it != set1->End(); ++it) {
        set1_raw.insert(rjson::copy(*it));
    }
    for (const auto& a : set2->GetArray()) {
        set1_raw.erase(a);
    }
    if (set1_raw.empty()) {
        return std::nullopt;
    }
    rjson::value ret = rjson::empty_object();
    rjson::add_with_string_name(ret, set1_type, rjson::empty_array());
    rjson::value& result_set = ret[set1_type];
    for (const auto& a : set1_raw) {
        rjson::push_back(result_set, rjson::copy(a));
    }
    return ret;
}

// Take two JSON-encoded list values (remember that a list value is
// {"L": [...the actual list]}) and return the concatenation, again as
// a list value.
// Returns a null value if one of the arguments is not actually a list.
rjson::value list_concatenate(const rjson::value& v1, const rjson::value& v2) {
    const rjson::value* list1 = unwrap_list(v1);
    const rjson::value* list2 = unwrap_list(v2);
    if (!list1 || !list2) {
        return rjson::null_value();
    }
    rjson::value cat = rjson::copy(*list1);
    for (const auto& a : list2->GetArray()) {
        rjson::push_back(cat, rjson::copy(a));
    }
    rjson::value ret = rjson::empty_object();
    rjson::add(ret, "L", std::move(cat));
    return ret;
}

}
