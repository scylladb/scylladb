/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <map>
#include <string>

#include <seastar/core/future.hh>

#include "bytes_fwd.hh"
#include "schema/schema_fwd.hh"

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
