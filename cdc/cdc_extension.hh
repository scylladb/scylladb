/*
 * Copyright 2020-present ScyllaDB
 */
/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <map>

#include <seastar/core/sstring.hh>

#include "bytes.hh"
#include "cdc/cdc_options.hh"
#include "schema/schema.hh"
#include "serializer_impl.hh"

namespace cdc {

class cdc_extension : public schema_extension {
    cdc::options _cdc_options;
public:
    static constexpr auto NAME = "cdc";

    cdc_extension() = default;
    cdc_extension(const options& opts) : _cdc_options(opts) {}
    explicit cdc_extension(std::map<sstring, sstring> tags) : _cdc_options(std::move(tags)) {}
    explicit cdc_extension(const bytes& b) : _cdc_options(cdc_extension::deserialize(b)) {}
    explicit cdc_extension(const sstring& s) {
        throw std::logic_error("Cannot create cdc info from string");
    }
    bytes serialize() const override {
        return ser::serialize_to_buffer<bytes>(_cdc_options.to_map());
    }
    static std::map<sstring, sstring> deserialize(const bytes_view& buffer) {
        return ser::deserialize_from_buffer(buffer, boost::type<std::map<sstring, sstring>>());
    }
    const options& get_options() const {
        return _cdc_options;
    }
};

}
