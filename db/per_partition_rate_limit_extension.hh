/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "db/per_partition_rate_limit_options.hh"
#include "schema/schema.hh"
#include "serializer.hh"

namespace db {

class per_partition_rate_limit_extension : public schema_extension {
    per_partition_rate_limit_options _options;
public:
    static constexpr auto NAME = "per_partition_rate_limit";

    per_partition_rate_limit_extension() = default;
    per_partition_rate_limit_extension(const per_partition_rate_limit_options& opts) : _options(opts) {}

    explicit per_partition_rate_limit_extension(const std::map<sstring, sstring>& tags) : _options(tags) {}
    explicit per_partition_rate_limit_extension(const bytes& b) : _options(deserialize(b)) {}
    explicit per_partition_rate_limit_extension(const sstring& s) {
        throw std::logic_error("Cannot create per partition rate limit info from string");
    }

    bytes serialize() const override {
        return ser::serialize_to_buffer<bytes>(_options.to_map());
    }
    static std::map<sstring, sstring> deserialize(const bytes_view& buffer) {
        return ser::deserialize_from_buffer(buffer, boost::type<std::map<sstring, sstring>>());
    }
    const per_partition_rate_limit_options& get_options() const {
        return _options;
    }

};

}

