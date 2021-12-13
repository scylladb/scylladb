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

#include "builtin_routine.hh"
#include <seastar/core/shared_ptr.hh>

namespace db {

seastar::shared_ptr<builtin_routine> make_void_value_routine(std::function<future<>(builtin_routine::parameter_map&&)> f) {
    return make_shared<builtin_routine>({
        .routine = [f=std::move(f)](builtin_routine::parameter_map&& par){
            return f(std::move(par)).then([] {
                return builtin_routine::output{};
            });
        }, 
        .metadata = {}
    });
}
}
