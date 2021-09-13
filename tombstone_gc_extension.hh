/*
 * Copyright 2021-present ScyllaDB
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
 * You should have received a copy of the GNU Affero General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <map>

#include <seastar/core/sstring.hh>

#include "bytes.hh"
#include "serializer.hh"
#include "db/extensions.hh"
#include "schema.hh"
#include "serializer_impl.hh"
#include "tombstone_gc_options.hh"

class tombstone_gc_extension : public schema_extension {
    tombstone_gc_options _tombstone_gc_options;
public:
    static constexpr auto NAME = "tombstone_gc";

    tombstone_gc_extension() = default;
    tombstone_gc_extension(const tombstone_gc_options& opts) : _tombstone_gc_options(opts) {}
    explicit tombstone_gc_extension(std::map<seastar::sstring, seastar::sstring> tags) : _tombstone_gc_options(std::move(tags)) {}
    explicit tombstone_gc_extension(const bytes& b) : _tombstone_gc_options(tombstone_gc_extension::deserialize(b)) {}
    explicit tombstone_gc_extension(const seastar::sstring& s) {
        throw std::logic_error("Cannot create tombstone_gc_extension info from string");
    }
    bytes serialize() const override {
        return ser::serialize_to_buffer<bytes>(_tombstone_gc_options.to_map());
    }
    static std::map<seastar::sstring, seastar::sstring> deserialize(const bytes_view& buffer) {
        return ser::deserialize_from_buffer(buffer, boost::type<std::map<seastar::sstring, seastar::sstring>>());
    }
    const tombstone_gc_options& get_options() const {
        return _tombstone_gc_options;
    }
};

