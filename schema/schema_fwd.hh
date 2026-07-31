/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <map>
#include <string>

#include <seastar/core/shared_ptr.hh>
#include <seastar/core/future.hh>

#include "bytes_fwd.hh"
#include "utils/UUID.hh"
#include "utils/UUID_gen.hh"

using column_count_type = uint32_t;

// Column ID, unique within column_kind
using column_id = column_count_type;

class schema;

using schema_ptr = seastar::lw_shared_ptr<const schema>;

/**
 * Schema extension. An opaque type representing
 * entries in the "extensions" part of a table/view (see schema_tables).
 *
 * An extension has a name (the mapping key), and it can re-serialize
 * itself to bytes again, when we write back into schema tables.
 *
 * Code using a particular extension can locate it by name in the schema map,
 * and barring the "is_placeholder" says true, cast it to whatever might
 * be the expected implementation.
 *
 * We allow placeholder object since an extension written to schema tables
 * might be unavailable on next boot/other node. To avoid losing the config data,
 * a placeholder object is put into schema map, which at least can
 * re-serialize the data back.
 *
 */
class schema_extension {
public:
    virtual ~schema_extension() {};
    [[deprecated("Use dedicated columns in system_schema.scylla_tables instead")]]
    schema_extension() = default;

    virtual seastar::future<> validate(const schema&) const {
        return seastar::make_ready_future<>();
    }
    virtual bytes serialize() const = 0;
    virtual bool is_placeholder() const {
        return false;
    }
    using default_map_type = std::map<seastar::sstring, seastar::sstring>;
    // default impl assumes options are in a map.
    // implementations should override if not
    virtual std::string options_to_string() const;
};

using table_id = utils::tagged_uuid<struct table_id_tag>;

// table_id::to_sstring() is the only non-trivial (non-constexpr) member of
// tagged_uuid<Tag>; suppress its per-TU re-instantiation given how broadly
// table_id itself is used. Matching explicit instantiation lives in
// schema/schema.cc.
extern template struct utils::tagged_uuid<table_id_tag>;

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
using table_schema_version = utils::tagged_uuid<struct table_schema_version_tag>;

// See the comment on the analogous declaration above for table_id: this
// suppresses per-TU re-instantiation of table_schema_version::to_sstring(),
// which is odr-used from gms/versioned_value.hh (included by ~60
// translation units). Matching explicit instantiation lives in
// gms/versioned_value.cc.
extern template struct utils::tagged_uuid<table_schema_version_tag>;

inline table_schema_version reversed(table_schema_version v) noexcept {
    return table_schema_version(utils::UUID_gen::negate(v.uuid()));
}
