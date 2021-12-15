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

#pragma once

#include <unordered_map>
#include <vector>
#include <iosfwd>
#include <seastar/core/sstring.hh>

#include "schema.hh"
#include "locator/abstract_replication_strategy.hh"
#include "data_dictionary/user_types_metadata.hh"

namespace data_dictionary {

class keyspace_metadata final {
    sstring _name;
    sstring _strategy_name;
    locator::replication_strategy_config_options _strategy_options;
    std::unordered_map<sstring, schema_ptr> _cf_meta_data;
    bool _durable_writes;
    user_types_metadata _user_types;
public:
    keyspace_metadata(std::string_view name,
                 std::string_view strategy_name,
                 locator::replication_strategy_config_options strategy_options,
                 bool durable_writes,
                 std::vector<schema_ptr> cf_defs = std::vector<schema_ptr>{});
    keyspace_metadata(std::string_view name,
                 std::string_view strategy_name,
                 locator::replication_strategy_config_options strategy_options,
                 bool durable_writes,
                 std::vector<schema_ptr> cf_defs,
                 user_types_metadata user_types);
    static lw_shared_ptr<keyspace_metadata>
    new_keyspace(std::string_view name,
                 std::string_view strategy_name,
                 locator::replication_strategy_config_options options,
                 bool durables_writes,
                 std::vector<schema_ptr> cf_defs = std::vector<schema_ptr>{});
    void validate(const locator::topology&) const;
    const sstring& name() const {
        return _name;
    }
    const sstring& strategy_name() const {
        return _strategy_name;
    }
    const locator::replication_strategy_config_options& strategy_options() const {
        return _strategy_options;
    }
    const std::unordered_map<sstring, schema_ptr>& cf_meta_data() const {
        return _cf_meta_data;
    }
    bool durable_writes() const {
        return _durable_writes;
    }
    user_types_metadata& user_types() {
        return _user_types;
    }
    const user_types_metadata& user_types() const {
        return _user_types;
    }
    void add_or_update_column_family(const schema_ptr& s) {
        _cf_meta_data[s->cf_name()] = s;
    }
    void remove_column_family(const schema_ptr& s) {
        _cf_meta_data.erase(s->cf_name());
    }
    void add_user_type(const user_type ut);
    void remove_user_type(const user_type ut);
    std::vector<schema_ptr> tables() const;
    std::vector<view_ptr> views() const;
    friend std::ostream& operator<<(std::ostream& os, const keyspace_metadata& m);
};

}
