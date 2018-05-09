/*
 * Copyright 2017 ScyllaDB
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
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

//
// For reference, see https://developers.google.com/protocol-buffers/docs/encoding#varints
//

#pragma once

#include "bytes.hh"

#include <cstdint>

using vint_size_type = bytes::size_type;

static constexpr size_t max_vint_length = 9;

struct unsigned_vint final {
    using value_type = uint64_t;

    struct deserialized_type final {
        value_type value;
        vint_size_type size;
    };

    static vint_size_type serialized_size(value_type) noexcept;

    static vint_size_type serialize(value_type, bytes::iterator out);

    static deserialized_type deserialize(bytes_view v);

    static vint_size_type serialized_size_from_first_byte(bytes::value_type first_byte);
};

struct signed_vint final {
    using value_type = int64_t;

    struct deserialized_type final {
        value_type value;
        vint_size_type size;
    };

    static vint_size_type serialized_size(value_type) noexcept;

    static vint_size_type serialize(value_type, bytes::iterator out);

    static deserialized_type deserialize(bytes_view v);

    static vint_size_type serialized_size_from_first_byte(bytes::value_type first_byte);
};
