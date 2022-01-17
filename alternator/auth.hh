/*
 * Copyright 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <string>
#include <string_view>
#include <array>
#include "gc_clock.hh"
#include "utils/loading_cache.hh"

namespace service {
class storage_proxy;
}

namespace alternator {

using hmac_sha256_digest = std::array<char, 32>;

using key_cache = utils::loading_cache<std::string, std::string, 1>;

std::string get_signature(std::string_view access_key_id, std::string_view secret_access_key, std::string_view host, std::string_view method,
        std::string_view orig_datestamp, std::string_view signed_headers_str, const std::map<std::string_view, std::string_view>& signed_headers_map,
        const std::vector<temporary_buffer<char>>& body_content, std::string_view region, std::string_view service, std::string_view query_string);

future<std::string> get_key_from_roles(service::storage_proxy& proxy, std::string username);

}
