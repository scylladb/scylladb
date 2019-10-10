/*
 * Copyright 2019 ScyllaDB
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

#include "alternator/error.hh"
#include "log.hh"
#include <string>
#include <string_view>
#include <gnutls/crypto.h>
#include <seastar/util/defer.hh>
#include "hashers.hh"
#include "bytes.hh"
#include "alternator/auth.hh"
#include <fmt/format.h>

namespace alternator {

static logging::logger alogger("alternator-auth");

static hmac_sha256_digest hmac_sha256(std::string_view key, std::string_view msg) {
    hmac_sha256_digest digest;
    int ret = gnutls_hmac_fast(GNUTLS_MAC_SHA256, key.data(), key.size(), msg.data(), msg.size(), digest.data());
    if (ret) {
        throw std::runtime_error(fmt::format("Computing HMAC failed ({}): {}", ret, gnutls_strerror(ret)));
    }
    return digest;
}

static hmac_sha256_digest get_signature_key(std::string_view key, std::string_view date_stamp, std::string_view region_name, std::string_view service_name) {
    auto date = hmac_sha256("AWS4" + std::string(key), date_stamp);
    auto region = hmac_sha256(std::string_view(date.data(), date.size()), region_name);
    auto service = hmac_sha256(std::string_view(region.data(), region.size()), service_name);
    auto signing = hmac_sha256(std::string_view(service.data(), service.size()), "aws4_request");
    return signing;
}

static std::string apply_sha256(std::string_view msg) {
    sha256_hasher hasher;
    hasher.update(msg.data(), msg.size());
    return to_hex(hasher.finalize());
}

std::string get_signature(std::string_view access_key_id, std::string_view secret_access_key, std::string_view host, std::string_view method, std::string_view signed_headers_str,
        const std::map<std::string_view, std::string_view>& signed_headers_map, std::string_view body_content, std::string_view region, std::string_view service, std::string_view query_string) {
    auto amz_date_it = signed_headers_map.find("x-amz-date");
    if (amz_date_it == signed_headers_map.end()) {
        throw api_error("InvalidSignatureException", "X-Amz-Date header is mandatory for signature verification");
    }
    std::string_view amz_date = amz_date_it->second;
    std::string_view datestamp = amz_date.substr(0, 8);
    std::string_view canonical_uri = "/";

    std::stringstream canonical_headers;
    for (const auto& header : signed_headers_map) {
        canonical_headers << fmt::format("{}:{}", header.first, header.second) << '\n';
    }

    std::string payload_hash = apply_sha256(body_content);
    std::string canonical_request = fmt::format("{}\n{}\n{}\n{}\n{}\n{}", method, canonical_uri, query_string, canonical_headers.str(), signed_headers_str, payload_hash);

    std::string_view algorithm = "AWS4-HMAC-SHA256";
    std::string credential_scope = fmt::format("{}/{}/{}/aws4_request", datestamp, region, service);
    std::string string_to_sign = fmt::format("{}\n{}\n{}\n{}", algorithm, amz_date, credential_scope,  apply_sha256(canonical_request));

    hmac_sha256_digest signing_key = get_signature_key(secret_access_key, datestamp, region, service);
    hmac_sha256_digest signature = hmac_sha256(std::string_view(signing_key.data(), signing_key.size()), string_to_sign);

    return to_hex(bytes_view(reinterpret_cast<const int8_t*>(signature.data()), signature.size()));
}

}
