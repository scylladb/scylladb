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

#include "../../schema_registry.hh"
#include "db/schema_tables.hh"
#include "db/extensions.hh"

namespace tests {

class schema_registry_wrapper {
    db::extensions _extensions;
    std::optional<schema_registry> _stored_registry;
    schema_registry& _registry;

public:
    explicit schema_registry_wrapper(schema_registry& registry)
        : _registry(registry) {
    }
    schema_registry_wrapper()
        : _stored_registry(std::in_place_t{}, db::schema_ctxt(db::schema_ctxt::for_tests{}, _extensions))
        , _registry(*_stored_registry) {
    }
    schema_registry_wrapper(const schema_registry_wrapper&) = delete;
    schema_registry_wrapper(schema_registry_wrapper&&) = delete;
    schema_registry_wrapper& operator=(const schema_registry_wrapper&) = delete;
    schema_registry_wrapper& operator=(schema_registry_wrapper&&) = delete;

    schema_registry& registry() { return _registry; }

    operator schema_registry&() { return _registry; }

    schema_registry& operator*() { return _registry; }
    schema_registry* operator->() { return &_registry; }
};

} // namespace tests
