/*
 * Copyright 2025-present ScyllaDB
 */
/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <map>
#include <fmt/format.h>

#include <seastar/core/sstring.hh>

#include "bytes.hh"
#include "schema/schema.hh"
#include "serializer_impl.hh"
#include "db/tablet_hints.hh"

namespace db {

// Schema extension for per-table tablet hints.
class tablet_hints_extension : public schema_extension {
    tablet_hints _hints;
public:
    static constexpr auto NAME = "tablet_hints";

    tablet_hints_extension() = default;
    tablet_hints_extension(tablet_hints hints) : _hints(std::move(hints)) {}
    explicit tablet_hints_extension(const tablet_hints::map_type& map) : _hints(map) {}
    explicit tablet_hints_extension(const bytes& b) : tablet_hints_extension(tablet_hints_extension::deserialize(b)) {}
    explicit tablet_hints_extension(const seastar::sstring& s) {
        throw std::logic_error("Cannot create tablet_hints_extension info from string");
    }
    bytes serialize() const override {
        return ser::serialize_to_buffer<bytes>(_hints.to_map());
    }
    static tablet_hints::map_type deserialize(const bytes_view& buffer) {
        return ser::deserialize_from_buffer(buffer, std::type_identity<tablet_hints::map_type>());
    }
    const tablet_hints& get_hints() const {
        return _hints;
    }
};

} // namespace db
