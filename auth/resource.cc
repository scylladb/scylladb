/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (C) 2016-present ScyllaDB
 *
 * Modified by ScyllaDB
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

#include "auth/resource.hh"

#include <algorithm>
#include <iterator>
#include <unordered_map>

#include <boost/algorithm/string/join.hpp>
#include <boost/algorithm/string/split.hpp>

#include "service/storage_proxy.hh"

namespace auth {

std::ostream& operator<<(std::ostream& os, resource_kind kind) {
    switch (kind) {
        case resource_kind::data: os << "data"; break;
        case resource_kind::role: os << "role"; break;
        case resource_kind::service_level: os << "service_level"; break;
    }

    return os;
}

static const std::unordered_map<resource_kind, std::string_view> roots{
        {resource_kind::data, "data"},
        {resource_kind::role, "roles"},
        {resource_kind::service_level, "service_levels"}};

static const std::unordered_map<resource_kind, std::size_t> max_parts{
        {resource_kind::data, 2},
        {resource_kind::role, 1},
        {resource_kind::service_level, 0}};

static permission_set applicable_permissions(const data_resource_view& dv) {
    if (dv.table()) {
        return permission_set::of<
                permission::ALTER,
                permission::DROP,
                permission::SELECT,
                permission::MODIFY,
                permission::AUTHORIZE>();
    }

    return permission_set::of<
            permission::CREATE,
            permission::ALTER,
            permission::DROP,
            permission::SELECT,
            permission::MODIFY,
            permission::AUTHORIZE>();
}

static permission_set applicable_permissions(const role_resource_view& rv) {
    if (rv.role()) {
        return permission_set::of<permission::ALTER, permission::DROP, permission::AUTHORIZE>();
    }

    return permission_set::of<
            permission::CREATE,
            permission::ALTER,
            permission::DROP,
            permission::AUTHORIZE,
            permission::DESCRIBE>();
}

static permission_set applicable_permissions(const service_level_resource_view &rv) {
    return permission_set::of<
            permission::CREATE,
            permission::ALTER,
            permission::DROP,
            permission::DESCRIBE,
            permission::AUTHORIZE>();
}

resource::resource(resource_kind kind) : _kind(kind) {
    _parts.emplace_back(roots.at(kind));
}

resource::resource(resource_kind kind, utils::small_vector<sstring, 3> parts) : resource(kind) {
    _parts.insert(_parts.end(), std::make_move_iterator(parts.begin()), std::make_move_iterator(parts.end()));
}

resource::resource(data_resource_t, std::string_view keyspace) : resource(resource_kind::data) {
    _parts.emplace_back(keyspace);
}

resource::resource(data_resource_t, std::string_view keyspace, std::string_view table) : resource(resource_kind::data) {
    _parts.emplace_back(keyspace);
    _parts.emplace_back(table);
}

resource::resource(role_resource_t, std::string_view role) : resource(resource_kind::role) {
    _parts.emplace_back(role);
}

resource::resource(service_level_resource_t): resource(resource_kind::service_level) {
}

sstring resource::name() const {
    return boost::algorithm::join(_parts, "/");
}

std::optional<resource> resource::parent() const {
    if (_parts.size() == 1) {
        return {};
    }

    resource copy = *this;
    copy._parts.pop_back();
    return copy;
}

permission_set resource::applicable_permissions() const {
    permission_set ps;

    switch (_kind) {
        case resource_kind::data: ps = ::auth::applicable_permissions(data_resource_view(*this)); break;
        case resource_kind::role: ps = ::auth::applicable_permissions(role_resource_view(*this)); break;
        case resource_kind::service_level: ps = ::auth::applicable_permissions(service_level_resource_view(*this)); break;
    }

    return ps;
}

bool operator<(const resource& r1, const resource& r2) {
    if (r1._kind != r2._kind) {
        return r1._kind < r2._kind;
    }

    return std::lexicographical_compare(
            r1._parts.cbegin() + 1,
            r1._parts.cend(),
            r2._parts.cbegin() + 1,
            r2._parts.cend());
}

std::ostream& operator<<(std::ostream& os, const resource& r) {
    switch (r.kind()) {
        case resource_kind::data: return os << data_resource_view(r);
        case resource_kind::role: return os << role_resource_view(r);
        case resource_kind::service_level: return os << service_level_resource_view(r);
    }

    return os;
}

service_level_resource_view::service_level_resource_view(const resource &r) {
    if (r._kind != resource_kind::service_level) {
        throw resource_kind_mismatch(resource_kind::service_level, r._kind);
    }
}

std::ostream &operator<<(std::ostream &os, const service_level_resource_view &v) {
    os << "<all service levels>";
    return os;
}

data_resource_view::data_resource_view(const resource& r) : _resource(r) {
    if (r._kind != resource_kind::data) {
        throw resource_kind_mismatch(resource_kind::data, r._kind);
    }
}

std::optional<std::string_view> data_resource_view::keyspace() const {
    if (_resource._parts.size() == 1) {
        return {};
    }

    return _resource._parts[1];
}

std::optional<std::string_view> data_resource_view::table() const {
    if (_resource._parts.size() <= 2) {
        return {};
    }

    return _resource._parts[2];
}

std::ostream& operator<<(std::ostream& os, const data_resource_view& v) {
    const auto keyspace = v.keyspace();
    const auto table = v.table();

    if (!keyspace) {
        os << "<all keyspaces>";
    } else if (!table) {
        os << "<keyspace " << *keyspace << '>';
    } else {
        os << "<table " << *keyspace << '.' << *table << '>';
    }

    return os;
}

role_resource_view::role_resource_view(const resource& r) : _resource(r) {
    if (r._kind != resource_kind::role) {
        throw resource_kind_mismatch(resource_kind::role, r._kind);
    }
}

std::optional<std::string_view> role_resource_view::role() const {
    if (_resource._parts.size() == 1) {
        return {};
    }

    return _resource._parts[1];
}

std::ostream& operator<<(std::ostream& os, const role_resource_view& v) {
    const auto role = v.role();

    if (!role) {
        os << "<all roles>";
    } else {
        os << "<role " << *role << '>';
    }

    return os;
}

resource parse_resource(std::string_view name) {
    static const std::unordered_map<std::string_view, resource_kind> reverse_roots = [] {
        std::unordered_map<std::string_view, resource_kind> result;

        for (const auto& pair : roots) {
            result.emplace(pair.second, pair.first);
        }

        return result;
    }();

    utils::small_vector<sstring, 3> parts;
    boost::split(parts, name, [](char ch) { return ch == '/'; });

    if (parts.empty()) {
        throw invalid_resource_name(name);
    }

    const auto iter = reverse_roots.find(parts[0]);
    if (iter == reverse_roots.end()) {
        throw invalid_resource_name(name);
    }

    const auto kind = iter->second;
    parts.erase(parts.begin());

    if (parts.size() > max_parts.at(kind)) {
        throw invalid_resource_name(name);
    }

    return resource(kind, std::move(parts));
}

static const resource the_root_data_resource{resource_kind::data};

const resource& root_data_resource() {
    return the_root_data_resource;
}

static const resource the_root_role_resource{resource_kind::role};

const resource& root_role_resource() {
    return the_root_role_resource;
}

static const resource the_root_service_level_resource{resource_kind::service_level};

const resource &root_service_level_resource() {
    return the_root_service_level_resource;
}

resource_set expand_resource_family(const resource& rr) {
    resource r = rr;
    resource_set rs;

    while (true) {
        const auto pr = r.parent();
        rs.insert(std::move(r));

        if (!pr) {
            break;
        }

        r = std::move(*pr);
    }

    return rs;
}

}
