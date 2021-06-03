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
 * Modified by ScyllaDB
 * Copyright (C) 2017-present ScyllaDB
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
#include <unordered_map>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/adaptor/sliced.hpp>

#include "database.hh"
#include "service/storage_proxy.hh"
#include "cql3/CqlParser.hpp"
#include "cql3/util.hh"
#include "cql_type_parser.hh"
#include "types.hh"
#include "user_types_metadata.hh"

static ::shared_ptr<cql3::cql3_type::raw> parse_raw(const sstring& str) {
    return cql3::util::do_with_parser(str,
        [] (cql3_parser::CqlParser& parser) {
            return parser.comparator_type(true);
        });
}

data_type db::cql_type_parser::parse(const sstring& keyspace, const sstring& str) {
    static const thread_local std::unordered_map<sstring, cql3::cql3_type> native_types = []{
        std::unordered_map<sstring, cql3::cql3_type> res;
        for (auto& nt : cql3::cql3_type::values()) {
            res.emplace(nt.to_string(), nt);
        }
        return res;
    }();

    auto i = native_types.find(str);
    if (i != native_types.end()) {
        return i->second.get_type();
    }

    const auto& sp = service::get_storage_proxy();
    const user_types_metadata& user_types =
            sp.local_is_initialized() ? sp.local().get_db().local().find_keyspace(keyspace).metadata()->user_types()
                                      : user_types_metadata{};
    auto raw = parse_raw(str);
    return raw->prepare_internal(keyspace, user_types).get_type();
}

class db::cql_type_parser::raw_builder::impl {
public:
    impl(keyspace_metadata &ks)
        : _ks(ks)
    {}

//    static shared_ptr<user_type_impl> get_instance(sstring keyspace, bytes name, std::vector<bytes> field_names, std::vector<data_type> field_types, bool is_multi_cell) {

    struct entry {
        sstring name;
        std::vector<sstring> field_names;
        std::vector<::shared_ptr<cql3::cql3_type::raw>> field_types;

        user_type prepare(const sstring& keyspace, user_types_metadata& user_types) const {
            std::vector<data_type> fields;
            fields.reserve(field_types.size());
            std::transform(field_types.begin(), field_types.end(), std::back_inserter(fields), [&](auto& r) {
                return r->prepare_internal(keyspace, user_types).get_type();
            });
            std::vector<bytes> names;
            names.reserve(field_names.size());
            std::transform(field_names.begin(), field_names.end(), std::back_inserter(names), [](const sstring& s) {
                return to_bytes(s);
            });

            return user_type_impl::get_instance(keyspace, to_bytes(name), std::move(names), std::move(fields), true);
        }

    };

    void add(sstring name, std::vector<sstring> field_names, std::vector<sstring> field_types) {
        entry e{ std::move(name), std::move(field_names) };
        for (auto& t : field_types) {
            e.field_types.emplace_back(parse_raw(t));
        }
        _definitions.emplace_back(std::move(e));
    }

    // See cassandra Types.java
    std::vector<user_type> build() {
        if (_definitions.empty()) {
            return {};
        }

        /*
         * build a DAG of UDT dependencies
         */
        std::unordered_multimap<entry *, entry *> adjacency;
        for (auto& e1 : _definitions) {
            for (auto& e2 : _definitions) {
                if (&e1 != &e2 && std::any_of(e1.field_types.begin(), e1.field_types.end(), [&e2](auto& t) { return t->references_user_type(e2.name); })) {
                    adjacency.emplace(&e2, &e1);
                }
            }
        }
        /*
         * resolve dependencies in topological order, using Kahn's algorithm
         */
        std::unordered_map<entry *, int32_t> vertices; // map values are numbers of referenced types
        for (auto&p : adjacency) {
            vertices[p.second]++;
        }

        std::deque<entry *> resolvable_types;
        for (auto& e : _definitions) {
            if (!vertices.contains(&e)) {
                resolvable_types.emplace_back(&e);
            }
        }

        // Create a copy of the existing types, so that we don't
        // modify the one in the keyspace. It is up to the caller to
        // do that.
        user_types_metadata types = _ks.user_types();

        const auto &ks_name = _ks.name();
        std::vector<user_type> created;

        while (!resolvable_types.empty()) {
            auto* e =  resolvable_types.front();
            auto r = adjacency.equal_range(e);

            while (r.first != r.second) {
                auto* d = r.first->second;
                if (--vertices[d] == 0) {
                    resolvable_types.push_back(d);
                }
                ++r.first;
            }

            created.push_back(e->prepare(ks_name, types));
            types.add_type(created.back());
            resolvable_types.pop_front();
        }

        if (created.size() != _definitions.size()) {
            throw exceptions::configuration_exception(format("Cannot resolve UDTs for keyspace {}: some types are missing", ks_name));
        }

        return created;
    }
private:
    keyspace_metadata& _ks;
    std::vector<entry> _definitions;
};

db::cql_type_parser::raw_builder::raw_builder(keyspace_metadata &ks)
    : _impl(std::make_unique<impl>(ks))
{}

db::cql_type_parser::raw_builder::~raw_builder()
{}

void db::cql_type_parser::raw_builder::add(sstring name, std::vector<sstring> field_names, std::vector<sstring> field_types) {
    _impl->add(std::move(name), std::move(field_names), std::move(field_types));
}

std::vector<user_type> db::cql_type_parser::raw_builder::build() {
    return _impl->build();
}
