/*
 * Modified by ScyllaDB
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */
#include <unordered_map>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/adaptor/sliced.hpp>
#include <seastar/coroutine/maybe_yield.hh>

#include "replica/database.hh"
#include "cql3/CqlParser.hpp"
#include "cql3/util.hh"
#include "cql_type_parser.hh"
#include "types/types.hh"
#include "data_dictionary/user_types_metadata.hh"
#include "utils/sorting.hh"

static ::shared_ptr<cql3::cql3_type::raw> parse_raw(const sstring& str) {
    return cql3::util::do_with_parser(str,
        [] (cql3_parser::CqlParser& parser) {
            return parser.comparator_type(true);
        });
}

data_type db::cql_type_parser::parse(const sstring& keyspace, const sstring& str, const data_dictionary::user_types_metadata& utm) {
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

    auto raw = parse_raw(str);
    return raw->prepare_internal(keyspace, utm).get_type();
}

data_type db::cql_type_parser::parse(const sstring& keyspace, const sstring& str, const data_dictionary::user_types_storage& uts) {
    return parse(keyspace, str, uts.get(keyspace));
}

class db::cql_type_parser::raw_builder::impl {
public:
    impl(replica::keyspace_metadata &ks)
        : _ks(ks)
    {}

//    static shared_ptr<user_type_impl> get_instance(sstring keyspace, bytes name, std::vector<bytes> field_names, std::vector<data_type> field_types, bool is_multi_cell) {

    struct entry {
        sstring name;
        std::vector<sstring> field_names;
        std::vector<::shared_ptr<cql3::cql3_type::raw>> field_types;

        user_type prepare(const sstring& keyspace, replica::user_types_metadata& user_types) const {
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
    future<std::vector<user_type>> build() {
        struct entry_comparator {
            inline bool operator()(const entry* a, const entry* b) const {
                return a->name < b->name;
            }
        };

        if (_definitions.empty()) {
            co_return std::vector<user_type>();
        }

        /*
         * build a DAG of UDT dependencies
         */
        std::vector<entry *> all_definitions;
        std::multimap<entry *, entry *, entry_comparator> adjacency;
        for (auto& e1 : _definitions) {
            all_definitions.emplace_back(&e1);
            for (auto& e2 : _definitions) {
                if (&e1 != &e2 && std::any_of(e1.field_types.begin(), e1.field_types.end(), [&e2](auto& t) { return t->references_user_type(e2.name); })) {
                    adjacency.emplace(&e2, &e1);
                }
            }
            co_await coroutine::maybe_yield();
        }

        // Create a copy of the existing types, so that we don't
        // modify the one in the keyspace. It is up to the caller to
        // do that.
        replica::user_types_metadata types = _ks.user_types();
        const auto &ks_name = _ks.name();

        auto sorted = co_await utils::topological_sort(all_definitions, adjacency);
        std::vector<user_type> created;
        created.reserve(sorted.size());

        for (auto* e : sorted) {
            created.push_back(e->prepare(ks_name, types));
            types.add_type(created.back());
        }
        co_return created;
    }
private:
    data_dictionary::keyspace_metadata& _ks;
    std::vector<entry> _definitions;
};

db::cql_type_parser::raw_builder::raw_builder(data_dictionary::keyspace_metadata &ks)
    : _impl(std::make_unique<impl>(ks))
{}

db::cql_type_parser::raw_builder::~raw_builder()
{}

void db::cql_type_parser::raw_builder::add(sstring name, std::vector<sstring> field_names, std::vector<sstring> field_types) {
    _impl->add(std::move(name), std::move(field_names), std::move(field_types));
}

future<std::vector<user_type>> db::cql_type_parser::raw_builder::build() {
    return _impl->build();
}
