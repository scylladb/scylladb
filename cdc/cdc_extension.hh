/*
 * Copyright 2020-present ScyllaDB
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
#include "cdc/cdc_options.hh"
#include "schema.hh"
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
