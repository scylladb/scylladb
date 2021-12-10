/*
 * Copyright (C) 2021-present ScyllaDB
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

#include "user_types_metadata.hh"
#include "keyspace_metadata.hh"
#include <ostream>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/adaptor/filtered.hpp>

namespace data_dictionary {

keyspace_metadata::keyspace_metadata(std::string_view name,
             std::string_view strategy_name,
             locator::replication_strategy_config_options strategy_options,
             bool durable_writes,
             std::vector<schema_ptr> cf_defs)
    : keyspace_metadata(name,
                        strategy_name,
                        std::move(strategy_options),
                        durable_writes,
                        std::move(cf_defs),
                        user_types_metadata{}) { }

keyspace_metadata::keyspace_metadata(std::string_view name,
             std::string_view strategy_name,
             locator::replication_strategy_config_options strategy_options,
             bool durable_writes,
             std::vector<schema_ptr> cf_defs,
             user_types_metadata user_types)
    : _name{name}
    , _strategy_name{locator::abstract_replication_strategy::to_qualified_class_name(strategy_name.empty() ? "NetworkTopologyStrategy" : strategy_name)}
    , _strategy_options{std::move(strategy_options)}
    , _durable_writes{durable_writes}
    , _user_types{std::move(user_types)}
{
    for (auto&& s : cf_defs) {
        _cf_meta_data.emplace(s->cf_name(), s);
    }
}

void keyspace_metadata::validate(const locator::topology& topology) const {
    using namespace locator;
    abstract_replication_strategy::validate_replication_strategy(name(), strategy_name(), strategy_options(), topology);
}

lw_shared_ptr<keyspace_metadata>
keyspace_metadata::new_keyspace(std::string_view name,
                                std::string_view strategy_name,
                                locator::replication_strategy_config_options options,
                                bool durables_writes,
                                std::vector<schema_ptr> cf_defs)
{
    return ::make_lw_shared<keyspace_metadata>(name, strategy_name, options, durables_writes, cf_defs);
}

void keyspace_metadata::add_user_type(const user_type ut) {
    _user_types.add_type(ut);
}

void keyspace_metadata::remove_user_type(const user_type ut) {
    _user_types.remove_type(ut);
}

std::vector<schema_ptr> keyspace_metadata::tables() const {
    return boost::copy_range<std::vector<schema_ptr>>(_cf_meta_data
            | boost::adaptors::map_values
            | boost::adaptors::filtered([] (auto&& s) { return !s->is_view(); }));
}

std::vector<view_ptr> keyspace_metadata::views() const {
    return boost::copy_range<std::vector<view_ptr>>(_cf_meta_data
            | boost::adaptors::map_values
            | boost::adaptors::filtered(std::mem_fn(&schema::is_view))
            | boost::adaptors::transformed([] (auto&& s) { return view_ptr(s); }));
}

std::ostream& operator<<(std::ostream& os, const keyspace_metadata& m) {
    os << "KSMetaData{";
    os << "name=" << m._name;
    os << ", strategyClass=" << m._strategy_name;
    os << ", strategyOptions={";
    int n = 0;
    for (auto& p : m._strategy_options) {
        if (n++ != 0) {
            os << ", ";
        }
        os << p.first << "=" << p.second;
    }
    os << "}";
    os << ", cfMetaData={";
    n = 0;
    for (auto& p : m._cf_meta_data) {
        if (n++ != 0) {
            os << ", ";
        }
        os << p.first << "=" << p.second;
    }
    os << "}";
    os << ", durable_writes=" << m._durable_writes;
    os << ", userTypes=" << m._user_types;
    os << "}";
    return os;
}

std::ostream& operator<<(std::ostream& os, const user_types_metadata& m) {
    os << "org.apache.cassandra.config.UTMetaData@" << &m;
    return os;
}

}