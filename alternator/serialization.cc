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

#include "base64.hh"
#include "log.hh"
#include "serialization.hh"
#include "error.hh"
#include "rapidjson/writer.h"
#include "concrete_types.hh"
#include "cql3/type_json.hh"

static logging::logger slogger("alternator-serialization");

namespace alternator {

type_info type_info_from_string(std::string type) {
    static thread_local const std::unordered_map<std::string, type_info> type_infos = {
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
        bo.write(t.from_string(sstring_view(v.GetString(), v.GetStringLength())));
    }
    void operator()(const bytes_type_impl& t) const {
        bo.write(base64_decode(v));
    }
    void operator()(const boolean_type_impl& t) const {
        bo.write(boolean_type->decompose(v.GetBool()));
    }
    void operator()(const decimal_type_impl& t) const {
        bo.write(t.from_string(sstring_view(v.GetString(), v.GetStringLength())));
    }
    // default
    void operator()(const abstract_type& t) const {
        bo.write(from_json_object(t, Json::Value(rjson::print(v)), cql_serialization_format::internal()));
    }
};

bytes serialize_item(const rjson::value& item) {
    if (item.IsNull() || item.MemberCount() != 1) {
        throw api_error("ValidationException", format("An item can contain only one attribute definition: {}", item));
    }
    auto it = item.MemberBegin();
    type_info type_info = type_info_from_string(it->name.GetString()); // JSON keys are guaranteed to be strings

    if (type_info.atype == alternator_type::NOT_SUPPORTED_YET) {
        slogger.trace("Non-optimal serialization of type {}", it->name.GetString());
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
        rjson::set_with_string_name(deserialized, type_ident, rjson::parse(t.to_string(bytes(bv))));
    }
};

rjson::value deserialize_item(bytes_view bv) {
    rjson::value deserialized(rapidjson::kObjectType);
    if (bv.empty()) {
        throw api_error("ValidationException", "Serialized value empty");
    }

    alternator_type atype = alternator_type(bv[0]);
    bv.remove_prefix(1);

    if (atype == alternator_type::NOT_SUPPORTED_YET) {
        slogger.trace("Non-optimal deserialization of alternator type {}", int8_t(atype));
        return rjson::parse_raw(reinterpret_cast<const char *>(bv.data()), bv.size());
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
        throw std::runtime_error(format("Unknown type {}", type->name()));
    }
    return it->second;
}

bytes get_key_column_value(const rjson::value& item, const column_definition& column) {
    std::string column_name = column.name_as_text();
    std::string expected_type = type_to_string(column.type);

    const rjson::value& key_typed_value = rjson::get(item, rjson::value::StringRefType(column_name.c_str()));
    if (!key_typed_value.IsObject() || key_typed_value.MemberCount() != 1) {
        throw api_error("ValidationException",
                format("Missing or invalid value object for key column {}: {}", column_name, item));
    }
    return get_key_from_typed_value(key_typed_value, column, expected_type);
}

bytes get_key_from_typed_value(const rjson::value& key_typed_value, const column_definition& column, const std::string& expected_type) {
    auto it = key_typed_value.MemberBegin();
    if (it->name.GetString() != expected_type) {
        throw api_error("ValidationException",
                format("Type mismatch: expected type {} for key column {}, got type {}",
                        expected_type, column.name_as_text(), it->name.GetString()));
    }
    if (column.type == bytes_type) {
        return base64_decode(it->value);
    } else {
        return column.type->from_string(it->value.GetString());
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
        // We shouldn't get here, we shouldn't see such key columns.
        throw std::runtime_error(format("Unexpected key type: {}", column.type->name()));
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
        throw api_error("ValidationException", format("{}: invalid number object", diagnostic));
    }
    auto it = v.MemberBegin();
    if (it->name != "N") {
        throw api_error("ValidationException", format("{}: expected number, found type '{}'", diagnostic, it->name));
    }
    if (it->value.IsNumber()) {
         // FIXME(sarna): should use big_decimal constructor with numeric values directly:
        return big_decimal(rjson::print(it->value));
    }
    if (!it->value.IsString()) {
        throw api_error("ValidationException", format("{}: improperly formatted number constant", diagnostic));
    }
    return big_decimal(it->value.GetString());
}

}
