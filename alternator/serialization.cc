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
    static const std::unordered_map<std::string, type_info> type_infos = {
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
    static const std::unordered_map<alternator_type, type_representation> type_representations = {
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
        deserialized[type_representation.ident] = type_representation.dtype->to_json(bytes(bv)); //FIXME(sarna): unnecessary copy
    }

    return deserialized;
}

}
