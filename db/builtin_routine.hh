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
#include "builtin_routine_registry.hh"
#include <seastar/core/shared_ptr.hh>
namespace db {
// An execution context of a builtin stored procedure, e.g.
// system.repair().
// Such procedures are statically defined in the code and invoked
// by CALL statement if the name matches.
struct builtin_routine {
    using parameter_map = std::unordered_map<sstring, sstring>;
    using output = std::vector<std::vector<data_value>>;
    using output_metadata = std::vector<std::pair<sstring, data_type>>;

    std::function<future<output>(parameter_map&&)> routine;
    output_metadata metadata;

    bool has_output() const { return !metadata.empty(); }
    future<output> call(parameter_map&& params) const { return routine(std::move(params)); }
    future<output> operator()(parameter_map&& params) const { return call(std::move(params)); }
};


/*!
 * \brief a helper function to generate a routine that returns a single value
 *
 * This helper function can generate a builtin_routine from a std::function that returns a future to
 * a basic type.
 *
 * The function would set the metadata and output according to the given value.
 * Usage example:
 *
 *  make_single_value_routine<sstring>("value_name", [](builtin_routine::parameter_map&&) {return make_ready_future<sstring>("value");});
 */
template<class T>
seastar::shared_ptr<builtin_routine> make_single_value_routine(const sstring& name, std::function<future<T>(builtin_routine::parameter_map&&)>&& f) {
    return make_shared<builtin_routine>({
        .routine = [f=std::move(f)] (builtin_routine::parameter_map&& par) {
            return f(std::move(par)).then([] (T value) {
                return builtin_routine::output{{data_value(std::move(value))}};
            });
        },
        .metadata = {{name, data_type_for<T>()}}
    });
}

/*!
 * \brief a helper function to generate a routine that do not return any value
 *
 * This helper function can generate a builtin_routine from a std::function that returns a no value future
 *
 * The function would set the metadata and output accordingly.
 * Usage example:
 *
 *  make_void_value_routine("value_name", [](builtin_routine::parameter_map&&) {return make_ready_future<>();});
 */
seastar::shared_ptr<builtin_routine> make_void_value_routine(std::function<future<>(builtin_routine::parameter_map&&)> f);
}
