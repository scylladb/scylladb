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

#include <string>
#include <string_view>
#include <array>
#include "gc_clock.hh"
#include "utils/loading_cache.hh"

namespace cql3 {
class query_processor;
}

namespace alternator {

using hmac_sha256_digest = std::array<char, 32>;

using key_cache = utils::loading_cache<std::string, std::string>;

std::string get_signature(std::string_view access_key_id, std::string_view secret_access_key, std::string_view host, std::string_view method,
        std::string_view orig_datestamp, std::string_view signed_headers_str, const std::map<std::string_view, std::string_view>& signed_headers_map,
        const std::vector<temporary_buffer<char>>& body_content, std::string_view region, std::string_view service, std::string_view query_string);

future<std::string> get_key_from_roles(cql3::query_processor& qp, std::string username);

}
