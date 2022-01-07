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

#include "data_dictionary.hh"
#include "impl.hh"
#include "user_types_metadata.hh"
#include "keyspace_metadata.hh"
#include "schema.hh"
#include <fmt/core.h>
#include <ostream>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/adaptor/filtered.hpp>

namespace data_dictionary {

schema_ptr
table::schema() const {
    return _ops->get_table_schema(*this);
}

const std::vector<view_ptr>&
table::views() const {
    return _ops->get_table_views(*this);
}

const secondary_index::secondary_index_manager&
table::get_index_manager() const {
    return _ops->get_index_manager(*this);
}

lw_shared_ptr<keyspace_metadata>
keyspace::metadata() const {
    return _ops->get_keyspace_metadata(*this);
}

const user_types_metadata&
keyspace::user_types() const {
    return metadata()->user_types();
}

const locator::abstract_replication_strategy&
keyspace::get_replication_strategy() const {
    return _ops->get_replication_strategy(*this);
}

std::optional<keyspace>
database::try_find_keyspace(std::string_view name) const {
    return _ops->try_find_keyspace(*this, name);
}

bool
database::has_keyspace(std::string_view name) const {
    return bool(try_find_keyspace(name));
}

keyspace
database::find_keyspace(std::string_view name) const {
    auto ks = try_find_keyspace(name);
    if (!ks) {
        throw no_such_keyspace(name);
    }
    return *ks;
}

std::optional<table>
database::try_find_table(std::string_view ks, std::string_view table) const {
    return _ops->try_find_table(*this, ks, table);
}

table
database::find_table(std::string_view ks, std::string_view table) const {
    auto t = try_find_table(ks, table);
    if (!t) {
        throw no_such_column_family(ks, table);
    }
    return *t;
}

std::optional<table>
database::try_find_table(utils::UUID id) const {
    return _ops->try_find_table(*this, id);
}

table
database::find_column_family(utils::UUID uuid) const {
    auto t = try_find_table(uuid);
    if (!t) {
        throw no_such_column_family(uuid);
    }
    return *t;
}

schema_ptr
database::find_schema(std::string_view ks, std::string_view table) const {
    return find_table(ks, table).schema();
}

schema_ptr
database::find_schema(utils::UUID uuid) const {
    return find_column_family(uuid).schema();
}

bool
database::has_schema(std::string_view ks_name, std::string_view cf_name) const {
    return bool(try_find_table(ks_name, cf_name));
}

table
database::find_column_family(schema_ptr s) const {
    return find_column_family(s->id());
}

schema_ptr
database::find_indexed_table(std::string_view ks_name, std::string_view index_name) const {
    return _ops->find_indexed_table(*this, ks_name, index_name);
}

sstring
database::get_available_index_name(std::string_view ks_name, std::string_view table_name,
        std::optional<sstring> index_name_root) const {
    return _ops->get_available_index_name(*this, ks_name, table_name, index_name_root);
}

std::set<sstring>
database::existing_index_names(std::string_view ks_name, std::string_view cf_to_exclude) const {
    return _ops->existing_index_names(*this, ks_name, cf_to_exclude);
}

schema_ptr
database::get_cdc_base_table(sstring_view ks_name, std::string_view table_name) const {
    return get_cdc_base_table(*find_table(ks_name, table_name).schema());
}

schema_ptr
database::get_cdc_base_table(const schema& s) const {
    return _ops->get_cdc_base_table(*this, s);
}

const db::extensions& 
database::extensions() const {
    return _ops->get_extensions(*this);
}

const gms::feature_service&
database::features() const {
    return _ops->get_features(*this);
}

const db::config&
database::get_config() const {
    return _ops->get_config(*this);
}

replica::database&
database::real_database() const {
    return _ops->real_database(*this);
}

impl::~impl() = default;

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

no_such_keyspace::no_such_keyspace(std::string_view ks_name)
    : runtime_error{format("Can't find a keyspace {}", ks_name)}
{
}

no_such_column_family::no_such_column_family(const utils::UUID& uuid)
    : runtime_error{format("Can't find a column family with UUID {}", uuid)}
{
}

no_such_column_family::no_such_column_family(std::string_view ks_name, std::string_view cf_name)
    : runtime_error{format("Can't find a column family {} in keyspace {}", cf_name, ks_name)}
{
}

no_such_column_family::no_such_column_family(std::string_view ks_name, const utils::UUID& uuid)
    : runtime_error{format("Can't find a column family with UUID {} in keyspace {}", uuid, ks_name)}
{
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
