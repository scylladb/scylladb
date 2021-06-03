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

#include "base64.hh"
#include "log.hh"
#include "serialization.hh"
#include "error.hh"
#include "rapidjson/writer.h"
#include "concrete_types.hh"
#include "cql3/type_json.hh"

static logging::logger slogger("alternator-serialization");

namespace alternator {

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

struct from_json_visitor {
    const rjson::value& v;
    bytes_ostream& bo;

    void operator()(const reversed_type_impl& t) const { visit(*t.underlying_type(), from_json_visitor{v, bo}); };
    void operator()(const string_type_impl& t) {
        bo.write(t.from_string(rjson::to_string_view(v)));
    }
    void operator()(const bytes_type_impl& t) const {
        bo.write(base64_decode(v));
    }
    void operator()(const boolean_type_impl& t) const {
        bo.write(boolean_type->decompose(v.GetBool()));
    }
    void operator()(const decimal_type_impl& t) const {
        try {
            bo.write(t.from_string(rjson::to_string_view(v)));
        } catch (const marshal_exception& e) {
            throw api_error::validation(format("The parameter cannot be converted to a numeric value: {}", v));
        }
    }
    // default
    void operator()(const abstract_type& t) const {
        bo.write(from_json_object(t, v, cql_serialization_format::internal()));
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
        rjson::set_with_string_name(deserialized, type_ident, rjson::from_string(s));
    }
    void operator()(const string_type_impl& t) {
        rjson::set_with_string_name(deserialized, type_ident, rjson::from_string(reinterpret_cast<const char *>(bv.data()), bv.size()));
    }
    void operator()(const bytes_type_impl& t) const {
        std::string b64 = base64_encode(bv);
        rjson::set_with_string_name(deserialized, type_ident, rjson::from_string(b64));
    }
    // default
    void operator()(const abstract_type& t) const {
        rjson::set_with_string_name(deserialized, type_ident, rjson::parse(to_json_string(t, bytes(bv))));
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
// entry, whose key is the type (expected to match the key column's type)
// and the value is the encoded value.
bytes get_key_from_typed_value(const rjson::value& key_typed_value, const column_definition& column) {
    if (!key_typed_value.IsObject() || key_typed_value.MemberCount() != 1 ||
            !key_typed_value.MemberBegin()->value.IsString()) {
        throw api_error::validation(
                format("Malformed value object for key column {}: {}",
                        column.name_as_text(), key_typed_value));
    }

    auto it = key_typed_value.MemberBegin();
    if (it->name != type_to_string(column.type)) {
        throw api_error::validation(
                format("Type mismatch: expected type {} for key column {}, got type {}",
                        type_to_string(column.type), column.name_as_text(), it->name));
    }
    std::string_view value_view = rjson::to_string_view(it->value);
    if (value_view.empty()) {
        throw api_error::validation(
                format("The AttributeValue for a key attribute cannot contain an empty string value. Key: {}", column.name_as_text()));
    }
    if (column.type == bytes_type) {
        return base64_decode(it->value);
    } else {
        return column.type->from_string(rjson::to_string_view(it->value));
    }

}

rjson::value json_key_column_value(bytes_view cell, const column_definition& column) {
    if (column.type == bytes_type) {
        std::string b64 = base64_encode(cell);
        return rjson::from_string(b64);
    } if (column.type == utf8_type) {
        return rjson::from_string(std::string(reinterpret_cast<const char*>(cell.data()), cell.size()));
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

big_decimal unwrap_number(const rjson::value& v, std::string_view diagnostic) {
    if (!v.IsObject() || v.MemberCount() != 1) {
        throw api_error::validation(format("{}: invalid number object", diagnostic));
    }
    auto it = v.MemberBegin();
    if (it->name != "N") {
        throw api_error::validation(format("{}: expected number, found type '{}'", diagnostic, it->name));
    }
    try {
        if (it->value.IsNumber()) {
             // FIXME(sarna): should use big_decimal constructor with numeric values directly:
            return big_decimal(rjson::print(it->value));
        }
        if (!it->value.IsString()) {
            throw api_error::validation(format("{}: improperly formatted number constant", diagnostic));
        }
        return big_decimal(rjson::to_string_view(it->value));
    } catch (const marshal_exception& e) {
        throw api_error::validation(format("The parameter cannot be converted to a numeric value: {}", it->value));
    }
}

const std::pair<std::string, const rjson::value*> unwrap_set(const rjson::value& v) {
    if (!v.IsObject() || v.MemberCount() != 1) {
        return {"", nullptr};
    }
    auto it = v.MemberBegin();
    const std::string it_key = it->name.GetString();
    if (it_key != "SS" && it_key != "BS" && it_key != "NS") {
        return {"", nullptr};
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
    std::string str_ret = std::string((n1 + n2).to_string());
    rjson::set(ret, "N", rjson::from_string(str_ret));
    return ret;
}

rjson::value number_subtract(const rjson::value& v1, const rjson::value& v2) {
    auto n1 = unwrap_number(v1, "UpdateExpression");
    auto n2 = unwrap_number(v2, "UpdateExpression");
    rjson::value ret = rjson::empty_object();
    std::string str_ret = std::string((n1 - n2).to_string());
    rjson::set(ret, "N", rjson::from_string(str_ret));
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
    rjson::set_with_string_name(ret, set1_type, std::move(sum));
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
        throw api_error::validation(format("Mismatched set types: {} and {}", set1_type, set2_type));
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
    rjson::set_with_string_name(ret, set1_type, rjson::empty_array());
    rjson::value& result_set = ret[set1_type];
    for (const auto& a : set1_raw) {
        rjson::push_back(result_set, rjson::copy(a));
    }
    return ret;
}

}
