/*
 * Copyright (C) 2020 ScyllaDB
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
#include "db/builtin_routine_registry.hh"
#include "cql3/functions/system_function.hh"
#include "cql3/functions/functions.hh"

extern logging::logger dblog;

namespace db {

void builtin_routine_registry::register_routine(sstring name, seastar::shared_ptr<builtin_routine> routine) {
    _registry.emplace(std::make_pair(std::move(name), routine));
}

// Create a builtin routine. Use with shard local instance of
// database&.
seastar::shared_ptr<builtin_routine> builtin_routine_registry::find_routine(const sstring& name) {
    auto it = _registry.find(name);
    if (it == _registry.end()) {
        return {};
    }
    return it->second;
}

void add_system_function(sstring name, std::function<future<sstring>()> f) {
    dblog.trace("{}", fmt::format("registering function {}", name));
    auto function = ::make_shared<cql3::functions::system_function>(std::move(name), utf8_type, std::vector<data_type>{}, std::move(f));
    cql3::functions::functions::add_function(std::move(function));
}

} // end of namespace db
