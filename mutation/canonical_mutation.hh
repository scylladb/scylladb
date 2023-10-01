/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "bytes.hh"
#include "schema/schema_fwd.hh"
#include "replica/database_fwd.hh"
#include "bytes_ostream.hh"
#include <iosfwd>
#include <fmt/ostream.h>

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

    table_id column_family_id() const;

    const bytes_ostream& representation() const { return _data; }

    friend std::ostream& operator<<(std::ostream& os, const canonical_mutation& cm);
};

template <> struct fmt::formatter<canonical_mutation> : fmt::ostream_formatter {};
