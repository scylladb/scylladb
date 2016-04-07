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
 * Copyright (C) 2016 ScyllaDB
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

#include "data_resource.hh"

#include <regex>
#include "service/storage_proxy.hh"

const sstring auth::data_resource::ROOT_NAME("data");

auth::data_resource::data_resource(level l, const sstring& ks, const sstring& cf)
    : _ks(ks), _cf(cf)
{
    if (l != get_level()) {
        throw std::invalid_argument("level/keyspace/column mismatch");
    }
}

auth::data_resource::data_resource()
    : data_resource(level::ROOT)
{}

auth::data_resource::data_resource(const sstring& ks)
    : data_resource(level::KEYSPACE, ks)
{}

auth::data_resource::data_resource(const sstring& ks, const sstring& cf)
    : data_resource(level::COLUMN_FAMILY, ks, cf)
{}

auth::data_resource::level auth::data_resource::get_level() const {
    if (!_cf.empty()) {
        assert(!_ks.empty());
        return level::COLUMN_FAMILY;
    }
    if (!_ks.empty()) {
        return level::KEYSPACE;
    }
    return level::ROOT;
}

auth::data_resource auth::data_resource::from_name(
                const sstring& s) {

    static std::regex slash_regex("/");

    auto i = std::regex_token_iterator<sstring::const_iterator>(s.begin(),
                    s.end(), slash_regex, -1);
    auto e = std::regex_token_iterator<sstring::const_iterator>();
    auto n = std::distance(i, e);

    if (n > 3 || ROOT_NAME != sstring(*i++)) {
        throw std::invalid_argument(sprint("%s is not a valid data resource name", s));
    }

    if (n == 1) {
        return data_resource();
    }
    auto ks = *i++;
    if (n == 2) {
        return data_resource(ks.str());
    }
    auto cf = *i++;
    return data_resource(ks.str(), cf.str());
}

sstring auth::data_resource::name() const {
    switch (get_level()) {
        case level::ROOT:
            return ROOT_NAME;
        case level::KEYSPACE:
            return sprint("%s/%s", ROOT_NAME, _ks);
        case level::COLUMN_FAMILY:
        default:
            return sprint("%s/%s/%s", ROOT_NAME, _ks, _cf);
    }
}

auth::data_resource auth::data_resource::get_parent() const {
    switch (get_level()) {
    case level::KEYSPACE:
        return data_resource();
    case level::COLUMN_FAMILY:
        return data_resource(_ks);
    default:
        throw std::invalid_argument("Root-level resource can't have a parent");
    }
}

const sstring& auth::data_resource::keyspace() const
                throw (std::invalid_argument) {
    if (is_root_level()) {
        throw std::invalid_argument("ROOT data resource has no keyspace");
    }
    return _ks;
}

const sstring& auth::data_resource::column_family() const
                throw (std::invalid_argument) {
    if (!is_column_family_level()) {
        throw std::invalid_argument(sprint("%s data resource has no column family", name()));
    }
    return _cf;
}

bool auth::data_resource::has_parent() const {
    return !is_root_level();
}

bool auth::data_resource::exists() const {
    switch (get_level()) {
        case level::ROOT:
            return true;
        case level::KEYSPACE:
            return service::get_local_storage_proxy().get_db().local().has_keyspace(_ks);
        case level::COLUMN_FAMILY:
        default:
            return service::get_local_storage_proxy().get_db().local().has_schema(_ks, _cf);
    }
}

sstring auth::data_resource::to_string() const {
    return name();
}

bool auth::data_resource::operator==(const data_resource& v) const {
    return _ks == v._ks && _cf == v._cf;
}

bool auth::data_resource::operator<(const data_resource& v) const {
    return _ks < v._ks ? true : (v._ks < _ks ? false : _cf < v._cf);
}

std::ostream& auth::operator<<(std::ostream& os, const data_resource& r) {
    return os << r.name();
}

