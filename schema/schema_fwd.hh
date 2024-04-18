/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/shared_ptr.hh>

#include "utils/UUID.hh"
#include "utils/UUID_gen.hh"

using column_count_type = uint32_t;

// Column ID, unique within column_kind
using column_id = column_count_type;

class schema;
class schema_extension;

using schema_ptr = seastar::lw_shared_ptr<const schema>;

using table_id = utils::tagged_uuid<struct table_id_tag>;

struct table_info {
    sstring name;
    table_id id;
};

namespace std {

std::ostream& operator<<(std::ostream& os, const table_info& ti);

} // namespace std

template <>
struct fmt::formatter<table_info> : fmt::formatter<string_view> {
    auto format(const table_info&, fmt::format_context& ctx) const -> decltype(ctx.out());
};

// Cluster-wide identifier of schema version of particular table.
//
// The version changes the value not only on structural changes but also
// temporal. For example, schemas with the same set of columns but created at
// different times should have different versions. This allows nodes to detect
// if the version they see was already synchronized with or not even if it has
// the same structure as the past versions.
//
// Schema changes merged in any order should result in the same final version.
//
// When table_schema_version changes, schema_tables::calculate_schema_digest() should
// also change when schema mutations are applied.
using table_schema_version = utils::tagged_uuid<struct table_schema_version_tag>;

inline table_schema_version reversed(table_schema_version v) noexcept {
    return table_schema_version(utils::UUID_gen::negate(v.uuid()));
}
