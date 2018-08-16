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
 * Copyright (C) 2017 ScyllaDB
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

static ::shared_ptr<cql3::cql3_type::raw> parse_raw(const sstring& str) {
    return cql3::util::do_with_parser(str,
        [] (cql3_parser::CqlParser& parser) {
            return parser.comparator_type(true);
        });
}

data_type db::cql_type_parser::parse(const sstring& keyspace, const sstring& str, lw_shared_ptr<user_types_metadata> user_types) {
    static const thread_local std::unordered_map<sstring, shared_ptr<cql3::cql3_type>> native_types = []{
        std::unordered_map<sstring, shared_ptr<cql3::cql3_type>> res;
        for (auto& nt : cql3::cql3_type::values()) {
            res.emplace(nt->to_string(), nt);
        }
        return res;
    }();

    auto i = native_types.find(str);
    if (i != native_types.end()) {
        return i->second->get_type();
    }

    if (!user_types && service::get_storage_proxy().local_is_initialized()) {
        user_types = service::get_storage_proxy().local().get_db().local().find_keyspace(keyspace).metadata()->user_types();
    }
    // special-case top-level UDTs
    if (user_types) {
        auto& map = user_types->get_all_types();
        auto i = map.find(utf8_type->decompose(str));
        if (i != map.end()) {
            return i->second;
        }
    }

    auto raw = parse_raw(str);
    auto cql = raw->prepare_internal(keyspace, user_types);
    return cql->get_type();
}

class db::cql_type_parser::raw_builder::impl {
public:
    impl(sstring ks_name)
        : _ks_name(std::move(ks_name))
    {}

//    static shared_ptr<user_type_impl> get_instance(sstring keyspace, bytes name, std::vector<bytes> field_names, std::vector<data_type> field_types) {

    struct entry {
        sstring name;
        std::vector<sstring> field_names;
        std::vector<::shared_ptr<cql3::cql3_type::raw>> field_types;

        user_type prepare(const sstring& keyspace, lw_shared_ptr<user_types_metadata> user_types) const {
            std::vector<data_type> fields;
            fields.reserve(field_types.size());
            std::transform(field_types.begin(), field_types.end(), std::back_inserter(fields), [&](auto& r) {
                return r->prepare_internal(keyspace, user_types)->get_type();
            });
            std::vector<bytes> names;
            names.reserve(field_names.size());
            std::transform(field_names.begin(), field_names.end(), std::back_inserter(names), [](const sstring& s) {
                return to_bytes(s);
            });

            return user_type_impl::get_instance(keyspace, to_bytes(name), std::move(names), std::move(fields));
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
            if (!vertices.count(&e)) {
                resolvable_types.emplace_back(&e);
            }
        }

        auto types = ::make_lw_shared<user_types_metadata>();

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

            types->add_type(e->prepare(_ks_name, types));
            resolvable_types.pop_front();
        }

        if (types->get_all_types().size() != _definitions.size()) {
            throw exceptions::configuration_exception(sprint("Cannot resolve UDTs for keyspace %s: some types are missing", _ks_name));
        }

        return boost::copy_range<std::vector<user_type>>(types->get_all_types() | boost::adaptors::map_values);
    }
private:
    sstring _ks_name;
    std::vector<entry> _definitions;
};

db::cql_type_parser::raw_builder::raw_builder(sstring ks_name)
    : _impl(std::make_unique<impl>(std::move(ks_name)))
{}

db::cql_type_parser::raw_builder::~raw_builder()
{}

void db::cql_type_parser::raw_builder::add(sstring name, std::vector<sstring> field_names, std::vector<sstring> field_types) {
    _impl->add(std::move(name), std::move(field_names), std::move(field_types));
}

std::vector<user_type> db::cql_type_parser::raw_builder::build() {
    return _impl->build();
}
