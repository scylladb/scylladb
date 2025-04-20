/*
 * Copyright 2021-present ScyllaDB
 */
/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <map>

#include <seastar/core/sstring.hh>

#include "bytes.hh"
#include "schema/schema.hh"
#include "serializer_impl.hh"
#include "tombstone_gc_options.hh"

class tombstone_gc_extension : public schema_extension {
    tombstone_gc_options _tombstone_gc_options;
public:
    static constexpr auto NAME = "tombstone_gc";

    // tombstone_gc_extension was written before schema_extension was deprecated, so support it
    // without warnings
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-declarations"
    tombstone_gc_extension() = default;
    tombstone_gc_extension(const tombstone_gc_options& opts) : _tombstone_gc_options(opts) {}
    explicit tombstone_gc_extension(std::map<seastar::sstring, seastar::sstring> tags) : _tombstone_gc_options(std::move(tags)) {}
    explicit tombstone_gc_extension(const bytes& b) : _tombstone_gc_options(tombstone_gc_extension::deserialize(b)) {}
    explicit tombstone_gc_extension(const seastar::sstring& s) {
        throw std::logic_error("Cannot create tombstone_gc_extension info from string");
    }
#pragma clang diagnostic pop
    bytes serialize() const override {
        return ser::serialize_to_buffer<bytes>(_tombstone_gc_options.to_map());
    }
    static std::map<seastar::sstring, seastar::sstring> deserialize(const bytes_view& buffer) {
        return ser::deserialize_from_buffer(buffer, std::type_identity<std::map<seastar::sstring, seastar::sstring>>());
    }
    const tombstone_gc_options& get_options() const {
        return _tombstone_gc_options;
    }
};

