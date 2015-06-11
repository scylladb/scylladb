/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#pragma once

#include "core/sstring.hh"

#include <json/json.h>

namespace json {

template<typename Map>
inline sstring to_json(const Map& map) {
    Json::Value root(Json::arrayValue);
    for (auto&& kv : map) {
        Json::Value obj_value(Json::objectValue);
        obj_value[kv.first] = Json::Value(kv.second);
        root.append(obj_value);
    }
    Json::FastWriter writer;
    return writer.write(root);
}

inline std::map<sstring, sstring> to_map(const sstring& raw) {
    Json::Value root;
    Json::Reader reader;
    reader.parse(std::string{raw}, root);
    std::map<sstring, sstring> map;
    for (Json::ArrayIndex idx = 0; idx < root.size(); idx++) {
        auto item = root[idx];
        for (auto&& member : item.getMemberNames()) {
            map.emplace(member, item[member].asString());
        }
    }
    return map;
}

}
