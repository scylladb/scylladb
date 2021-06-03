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
#include "auth/common.hh"
#include "auth/password_authenticator.hh"
#include "auth/roles-metadata.hh"
#include "cql3/query_processor.hh"
#include "cql3/untyped_result_set.hh"

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

static std::string apply_sha256(const std::vector<temporary_buffer<char>>& msg) {
    sha256_hasher hasher;
    for (const temporary_buffer<char>& buf : msg) {
        hasher.update(buf.get(), buf.size());
    }
    return to_hex(hasher.finalize());
}

static std::string format_time_point(db_clock::time_point tp) {
    time_t time_point_repr = db_clock::to_time_t(tp);
    std::string time_point_str;
    time_point_str.resize(17);
    ::tm time_buf;
    // strftime prints the terminating null character as well
    std::strftime(time_point_str.data(), time_point_str.size(), "%Y%m%dT%H%M%SZ", ::gmtime_r(&time_point_repr, &time_buf));
    time_point_str.resize(16);
    return time_point_str;
}

void check_expiry(std::string_view signature_date) {
    //FIXME: The default 15min can be changed with X-Amz-Expires header - we should honor it
    std::string expiration_str = format_time_point(db_clock::now() - 15min);
    std::string validity_str = format_time_point(db_clock::now() + 15min);
    if (signature_date < expiration_str) {
        throw api_error::invalid_signature(
                fmt::format("Signature expired: {} is now earlier than {} (current time - 15 min.)",
                signature_date, expiration_str));
    }
    if (signature_date > validity_str) {
        throw api_error::invalid_signature(
                fmt::format("Signature not yet current: {} is still later than {} (current time + 15 min.)",
                signature_date, validity_str));
    }
}

std::string get_signature(std::string_view access_key_id, std::string_view secret_access_key, std::string_view host, std::string_view method,
        std::string_view orig_datestamp, std::string_view signed_headers_str, const std::map<std::string_view, std::string_view>& signed_headers_map,
        const std::vector<temporary_buffer<char>>& body_content, std::string_view region, std::string_view service, std::string_view query_string) {
    auto amz_date_it = signed_headers_map.find("x-amz-date");
    if (amz_date_it == signed_headers_map.end()) {
        throw api_error::invalid_signature("X-Amz-Date header is mandatory for signature verification");
    }
    std::string_view amz_date = amz_date_it->second;
    check_expiry(amz_date);
    std::string_view datestamp = amz_date.substr(0, 8);
    if (datestamp != orig_datestamp) {
        throw api_error::invalid_signature(
                format("X-Amz-Date date does not match the provided datestamp. Expected {}, got {}",
                        orig_datestamp, datestamp));
    }
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

future<std::string> get_key_from_roles(cql3::query_processor& qp, std::string username) {
    static const sstring query = format("SELECT salted_hash FROM {} WHERE {} = ?",
            auth::meta::roles_table::qualified_name, auth::meta::roles_table::role_col_name);

    auto cl = auth::password_authenticator::consistency_for_user(username);
    return qp.execute_internal(query, cl, auth::internal_distributed_query_state(), {sstring(username)}, true).then_wrapped([username = std::move(username)] (future<::shared_ptr<cql3::untyped_result_set>> f) {
        auto res = f.get0();
        auto salted_hash = std::optional<sstring>();
        if (res->empty()) {
            throw api_error::unrecognized_client(fmt::format("User not found: {}", username));
        }
        salted_hash = res->one().get_opt<sstring>("salted_hash");
        if (!salted_hash) {
            throw api_error::unrecognized_client(fmt::format("No password found for user: {}", username));
        }
        return make_ready_future<std::string>(*salted_hash);
    });
}

}
