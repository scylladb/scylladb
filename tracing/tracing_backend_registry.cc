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

#include "tracing_backend_registry.hh"
#include "tracing/tracing.hh"
#include "utils/class_registrator.hh"

namespace tracing {

no_such_tracing_backend::no_such_tracing_backend() : std::runtime_error("no such tracing backend") {
}

backend_registry::backend_registry()
        : _impl(std::make_unique<nonstatic_class_registry<i_tracing_backend_helper, tracing&>>()) {
}

void
backend_registry::register_backend_creator(sstring name, std::function<std::unique_ptr<i_tracing_backend_helper> (tracing&)> creator) {
    _impl->register_class(std::move(name), std::move(creator));
}

std::unique_ptr<i_tracing_backend_helper>
backend_registry::create_backend(const sstring& name, tracing& t) const {
    try {
        return _impl->create(name, t);
    } catch (no_such_class&) {
        throw no_such_tracing_backend();
    }
}

}
