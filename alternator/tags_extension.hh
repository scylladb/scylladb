/*
 * Copyright 2019-present ScyllaDB
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

#include "serializer.hh"
#include "schema.hh"
#include "db/extensions.hh"

namespace alternator {

class tags_extension : public schema_extension {
public:
    static constexpr auto NAME = "scylla_tags";

    tags_extension() = default;
    explicit tags_extension(const std::map<sstring, sstring>& tags) : _tags(std::move(tags)) {}
    explicit tags_extension(bytes b) : _tags(tags_extension::deserialize(b)) {}
    explicit tags_extension(const sstring& s) {
        throw std::logic_error("Cannot create tags from string");
    }
    bytes serialize() const override {
        return ser::serialize_to_buffer<bytes>(_tags);
    }
    static std::map<sstring, sstring> deserialize(bytes_view buffer) {
        return ser::deserialize_from_buffer(buffer, boost::type<std::map<sstring, sstring>>());
    }
    const std::map<sstring, sstring>& tags() const {
        return _tags;
    }
private:
    std::map<sstring, sstring> _tags;
};

}
