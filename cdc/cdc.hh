/*
 * Copyright (C) 2019 ScyllaDB
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
#include <seastar/core/sstring.hh>
#include <map>
#include <string>
#include "exceptions/exceptions.hh"
#include "json.hh"

namespace cdc {

class options final {
    bool _enabled = false;
    bool _preimage = false;
    bool _postimage = false;
    int _ttl = 86400; // 24h in seconds
public:
    options() = default;
    options(const std::map<sstring, sstring>& map) {
        if (map.find("enabled") == std::end(map)) {
            throw exceptions::configuration_exception("Missing enabled CDC option");
        }

        for (auto& p : map) {
            if (p.first == "enabled") {
                _enabled = p.second == "true";
            } else if (p.first == "preimage") {
                _preimage = p.second == "true";
            } else if (p.first == "postimage") {
                _postimage = p.second == "true";
            } else if (p.first == "ttl") {
                _ttl = std::stoi(p.second);
            } else {
                throw exceptions::configuration_exception("Invalid CDC option: " + p.first);
            }
        }
    }
    std::map<sstring, sstring> to_map() const {
        return {
            { "enabled", _enabled ? "true" : "false" },
            { "preimage", _preimage ? "true" : "false" },
            { "postimage", _postimage ? "true" : "false" },
            { "ttl", std::to_string(_ttl) },
        };
    }

    sstring to_sstring() const {
        return json::to_json(to_map());
    }

    bool enabled() const { return _enabled; }
    bool preimage() const { return _preimage; }
    bool postimage() const { return _postimage; }
    int ttl() const { return _ttl; }

    bool operator==(const options& o) const {
        return _enabled == o._enabled && _preimage == o._preimage && _postimage == o._postimage && _ttl == o._ttl;
    }
    bool operator!=(const options& o) const {
        return !(*this == o);
    }
};

} // namespace cdc
