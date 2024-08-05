/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "schema/schema_fwd.hh"
#include "replica/database_fwd.hh"
#include "bytes_ostream.hh"
#include <iosfwd>

// Immutable mutation form which can be read using any schema version of the same table.
// Safe to access from other shards via const&.
// Safe to pass serialized across nodes.
class canonical_mutation {
    bytes_ostream _data;
public:
    canonical_mutation() = default;
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

    partition_key key() const;

    table_id column_family_id() const;

    const bytes_ostream& representation() const { return _data; }
    bytes_ostream& representation() { return _data; }

    friend fmt::formatter<canonical_mutation>;
};

template <> struct fmt::formatter<canonical_mutation> : fmt::formatter<string_view> {
    auto format(const canonical_mutation&, fmt::format_context& ctx) const -> decltype(ctx.out());
};

static inline std::ostream& operator<<(std::ostream& os, const canonical_mutation& cm) {
    fmt::print(os, "{}", cm);
    return os;
}
