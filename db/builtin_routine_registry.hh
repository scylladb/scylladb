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

#pragma once
#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>
#include <functional>
#include "types.hh"
#include <unordered_map>

namespace db {

using namespace seastar;
struct builtin_routine;

class builtin_routine_registry {
public:
    std::unordered_map<sstring, seastar::shared_ptr<builtin_routine>> _registry;
    // Has to be invoked on each shard with shard-local instance of
    // the registry.
    void register_routine(sstring name, seastar::shared_ptr<builtin_routine>);

    // Return a builtin routine. Use with shard local instance of
    // builtin_routine_registry&.
    seastar::shared_ptr<builtin_routine> find_routine(const sstring& name);

};

} // end of namespace db

