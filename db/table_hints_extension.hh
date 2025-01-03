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
#include "db/table_hints.hh"

namespace db {

// Schema extension for per-table hints.
class table_hints_extension : public schema_extension {
    table_hints _hints;
public:
    static constexpr auto NAME = "hints";

    table_hints_extension() = default;
    table_hints_extension(table_hints hints) : _hints(std::move(hints)) {}
    explicit table_hints_extension(const table_hints::map_type& map) : _hints(map) {}
    explicit table_hints_extension(const bytes& b) : table_hints_extension(table_hints_extension::deserialize(b)) {}
    explicit table_hints_extension(const seastar::sstring& s) {
        throw std::logic_error("Cannot create table_hints_extension info from string");
    }
    bytes serialize() const override {
        return ser::serialize_to_buffer<bytes>(_hints.to_map());
    }
    static table_hints::map_type deserialize(const bytes_view& buffer) {
        return ser::deserialize_from_buffer(buffer, std::type_identity<table_hints::map_type>());
    }
    const table_hints& get_hints() const {
        return _hints;
    }
};

} // namespace db
