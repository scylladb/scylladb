/*
 * Copyright (C) 2015 ScyllaDB
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
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "core/sstring.hh"
#include "core/print.hh"

#include <json/json.h>

namespace seastar { // FIXME: not ours
namespace json {

// These helpers are needed to workaround issues with int64_t/uint64_t typeders on old
// versions of JsonCpp library - see https://github.com/scylladb/scylla/issues/3208#issuecomment-383763067
inline int64_t to_int64_t(const Json::Value& value) {
    return value.asInt64();
}

inline uint64_t to_uint64_t(const Json::Value& value) {
    return value.asUInt64();
}

inline sstring to_sstring(const Json::Value& value) {
#if defined(JSONCPP_VERSION_HEXA) && (JSONCPP_VERSION_HEXA >= 0x010400) // >= 1.4.0
    Json::StreamWriterBuilder wbuilder;
    wbuilder.settings_["indentation"] = "";
    auto str = Json::writeString(wbuilder, value);
#else
    Json::FastWriter writer;
    // Json::FastWriter unnecessarily adds a newline at the end of string.
    // There is a method omitEndingLineFeed() which prevents that, but it seems
    // to be too recent addition, so, at least for now, a workaround is needed.
    auto str = writer.write(value);
    if (str.length() && str.back() == '\n') {
        str.pop_back();
    }
#endif
    return str;
}

template<typename Map>
inline sstring to_json(const Map& map) {
    Json::Value root(Json::objectValue);
    for (auto&& kv : map) {
        root[kv.first] = Json::Value(kv.second);
    }
    return to_sstring(root);
}

inline Json::Value to_json_value(const sstring& raw) {
    Json::Value root;
#if defined(JSONCPP_VERSION_HEXA) && (JSONCPP_VERSION_HEXA >= 0x010400) // >= 1.4.0
    Json::CharReaderBuilder rbuilder;
    std::unique_ptr<Json::CharReader> reader(rbuilder.newCharReader());
    bool result = reader->parse(raw.begin(), raw.end(), &root, NULL);
    if (!result) {
        throw std::runtime_error(sprint("Failed to parse JSON: %s", raw));
    }
#else
    Json::Reader reader;
    reader.parse(std::string{raw}, root);
#endif
    return root;
}

template<typename Map>
inline Map to_map(const sstring& raw, Map&& map) {
    Json::Value root = to_json_value(raw);
    for (auto&& member : root.getMemberNames()) {
        map.emplace(member, root[member].asString());
    }
    return std::forward<Map>(map);
}

inline std::map<sstring, sstring> to_map(const sstring& raw) {
    return to_map(raw, std::map<sstring, sstring>());
}

sstring value_to_quoted_string(const sstring& value);

}

}
