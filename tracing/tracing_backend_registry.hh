/*
 * Copyright 2018-present ScyllaDB
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

#include <seastar/core/sstring.hh>
#include <functional>
#include <memory>
#include <exception>
#include "seastarx.hh"

template <typename BaseType, typename... Args>
class nonstatic_class_registry;

namespace tracing {

class i_tracing_backend_helper;
class trace_keyspace_helper;
class tracing;

class no_such_tracing_backend : public std::runtime_error {
public:
    no_such_tracing_backend();
};

class backend_registry {
    std::unique_ptr<nonstatic_class_registry<i_tracing_backend_helper, tracing&>> _impl;
private:
    void register_backend_creator(sstring name, std::function<std::unique_ptr<i_tracing_backend_helper> (tracing&)> creator);
public:
    backend_registry();
    std::unique_ptr<i_tracing_backend_helper> create_backend(const sstring& name, tracing& t) const; // may throw no_such_tracing_backend
    template <typename Backend>
    void register_backend(sstring name);
};

template <typename Backend>
void backend_registry::register_backend(sstring name) {
    return register_backend_creator(name, [] (tracing& t) {
        return std::make_unique<Backend>(t);
    });
}

void register_tracing_keyspace_backend(backend_registry&);

}
