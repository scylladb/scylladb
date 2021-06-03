/*
 * Copyright (C) 2019-present ScyllaDB
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

#pragma once

#include "types.hh"
#include "utils/rjson.hh"

bytes from_json_object(const abstract_type &t, const rjson::value& value, cql_serialization_format sf);
sstring to_json_string(const abstract_type &t, bytes_view bv);
sstring to_json_string(const abstract_type &t, const managed_bytes_view& bv);

inline sstring to_json_string(const abstract_type &t, const bytes& b) {
    return to_json_string(t, bytes_view(b));
}

inline sstring to_json_string(const abstract_type& t, const bytes_opt& b) {
    return b ? to_json_string(t, *b) : "null";
}
