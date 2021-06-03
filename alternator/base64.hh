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

#include <string_view>
#include "bytes.hh"
#include "utils/rjson.hh"

std::string base64_encode(bytes_view);

bytes base64_decode(std::string_view);

inline bytes base64_decode(const rjson::value& v) {
  return base64_decode(std::string_view(v.GetString(), v.GetStringLength()));
}

size_t base64_decoded_len(std::string_view str);

bool base64_begins_with(std::string_view base, std::string_view operand);
