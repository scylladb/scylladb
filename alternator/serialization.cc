/*
 * Copyright 2019 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "base64.hh"
#include "log.hh"
#include "serialization.hh"
#include "error.hh"

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

bytes serialize_item(const Json::Value& item) {
    if (item.size() != 1) {
        throw api_error("ValidationException", format("An item can contain only one attribute definition: {}", item.toStyledString()));
    }
    auto it = item.begin();
    type_info type_info = type_info_from_string(it.key().asString()); // JSON keys are guaranteed to be strings

    if (type_info.atype == alternator_type::NOT_SUPPORTED_YET) {
        slogger.trace("Non-optimal serialization of type {}", it.key());
        return bytes{int8_t(type_info.atype)} + to_bytes(item.toStyledString());
    }

    bytes serialized;
    // Alternator bytes representation does not start with "0x" followed by hex digits as Scylla-JSON does,
    // but instead uses base64.
    if (type_info.dtype == bytes_type) {
        std::string raw_value = it->asString();
        serialized = base64_decode(std::string_view(raw_value));
    } else {
        serialized = type_info.dtype->from_json_object(*it, cql_serialization_format::internal());
    }

    //NOTICE: redundant copy here, from_json_object should accept bytes' output iterator too.
    // Or, we could append type info to the end, but that's unorthodox.
    return bytes{int8_t(type_info.atype)} + std::move(serialized);
}

Json::Value deserialize_item(bytes_view bv) {
    Json::Value deserialized;
    if (bv.empty()) {
        throw api_error("ValidationException", "Serialized value empty");
    }

    alternator_type atype = alternator_type(bv[0]);
    bv.remove_prefix(1);

    if (atype == alternator_type::NOT_SUPPORTED_YET) {
        slogger.trace("Non-optimal deserialization of alternator type {}", int8_t(atype));
        return json::to_json_value(sstring(reinterpret_cast<const char *>(bv.data()), bv.size()));
    }

    type_representation type_representation = represent_type(atype);
    if (type_representation.dtype == bytes_type) {
        deserialized[type_representation.ident] = base64_encode(bv);
    } else if (type_representation.dtype == decimal_type) {
        auto s = decimal_type->to_json_string(bytes(bv)); //FIXME(sarna): unnecessary copy
        deserialized[type_representation.ident] = Json::Value(reinterpret_cast<const char*>(s.data()), reinterpret_cast<const char*>(s.data()) + s.size());
    } else {
        deserialized[type_representation.ident] = json::to_json_value(type_representation.dtype->to_json_string(bytes(bv))); //FIXME(sarna): unnecessary copy
    }

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

bytes get_key_column_value(const Json::Value& item, const column_definition& column) {
    std::string column_name = column.name_as_text();
    std::string expected_type = type_to_string(column.type);

    Json::Value key_typed_value = item.get(column_name, Json::nullValue);
    if (!key_typed_value.isObject() || key_typed_value.size() != 1) {
        throw api_error("ValidationException",
                format("Missing or invalid value object for key column {}: {}",
                        column_name, item.toStyledString()));
    }
    auto it = key_typed_value.begin();
    if (it.key().asString() != expected_type) {
        throw api_error("ValidationException",
                format("Expected type {} for key column {}, got type {}",
                        expected_type, column_name, it.key().asString()));
    }
    if (column.type == bytes_type) {
        return base64_decode(it->asString());
    } else {
        return column.type->from_string(it->asString());
    }

}

Json::Value json_key_column_value(bytes_view cell, const column_definition& column) {
    if (column.type == bytes_type) {
        return base64_encode(cell);
    } if (column.type == utf8_type) {
        return Json::Value(reinterpret_cast<const char*>(cell.data()),
                reinterpret_cast<const char*>(cell.data()) + cell.size());
    } else if (column.type == decimal_type) {
        // FIXME: use specialized Alternator number type, not the more
        // general "decimal_type". A dedicated type can be more efficient
        // in storage space and in parsing speed.
        auto s = decimal_type->to_json_string(bytes(cell));
        return Json::Value(reinterpret_cast<const char*>(s.data()),
                reinterpret_cast<const char*>(s.data()) + s.size());
    } else {
        // We shouldn't get here, we shouldn't see such key columns.
        throw std::runtime_error(format("Unexpected key type: {}", column.type->name()));
    }
}


partition_key pk_from_json(const Json::Value& item, schema_ptr schema) {
    std::vector<bytes> raw_pk;
    // FIXME: this is a loop, but we really allow only one partition key column.
    for (const column_definition& cdef : schema->partition_key_columns()) {
        bytes raw_value = get_key_column_value(item, cdef);
        raw_pk.push_back(std::move(raw_value));
    }
   return partition_key::from_exploded(raw_pk);
}

clustering_key ck_from_json(const Json::Value& item, schema_ptr schema) {
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

}
