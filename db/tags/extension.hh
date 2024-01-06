/*
 * Copyright 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "serializer.hh"
#include "schema/schema.hh"

namespace db {

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

// Information whether the view updates are synchronous is stored using the
// SYNCHRONOUS_VIEW_UPDATES_TAG_KEY tag. Value of this tag is a stored as a
// serialized boolean value ("true" or "false")
static const sstring SYNCHRONOUS_VIEW_UPDATES_TAG_KEY("system:synchronous_view_updates");

}
