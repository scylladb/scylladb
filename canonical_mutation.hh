/*
 * Copyright (C) 2015-present ScyllaDB
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

#include "bytes.hh"
#include "schema_fwd.hh"
#include "database_fwd.hh"
#include "mutation_partition_visitor.hh"
#include "mutation_partition_serializer.hh"
#include <iosfwd>

// Immutable mutation form which can be read using any schema version of the same table.
// Safe to access from other shards via const&.
// Safe to pass serialized across nodes.
class canonical_mutation {
    bytes_ostream _data;
public:
    explicit canonical_mutation(bytes_ostream);
    explicit canonical_mutation(const mutation&);

    canonical_mutation(canonical_mutation&&) = default;
    canonical_mutation(const canonical_mutation&) = default;
    canonical_mutation& operator=(const canonical_mutation&) = default;
    canonical_mutation& operator=(canonical_mutation&&) = default;

    // Create a mutation object interpreting this canonical mutation using
    // given schema.
    //
    // Data which is not representable in the target schema is dropped. If this
    // is not intended, user should sync the schema first.
    mutation to_mutation(schema_ptr) const;

    utils::UUID column_family_id() const;

    const bytes_ostream& representation() const { return _data; }

    friend std::ostream& operator<<(std::ostream& os, const canonical_mutation& cm);
};
