/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#pragma once

#include "core/sstring.hh"

#include <json/json.h>

namespace json {

template<typename Map>
inline sstring to_json(const Map& map) {
    Json::Value root(Json::objectValue);
    for (auto&& kv : map) {
        root[kv.first] = Json::Value(kv.second);
    }
    Json::FastWriter writer;
    // Json::FastWriter unnecessarily adds a newline at the end of string.
    // There is a method omitEndingLineFeed() which prevents that, but it seems
    // to be too recent addition, so, at least for now, a workaround is needed.
    auto str = writer.write(root);
    if (str.length() && str.back() == '\n') {
        str.pop_back();
    }
    return str;
}

inline std::map<sstring, sstring> to_map(const sstring& raw) {
    Json::Value root;
    Json::Reader reader;
    reader.parse(std::string{raw}, root);
    std::map<sstring, sstring> map;
    for (auto&& member : root.getMemberNames()) {
        map.emplace(member, root[member].asString());
    }
    return map;
}

}
