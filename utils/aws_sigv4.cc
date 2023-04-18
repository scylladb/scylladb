/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <gnutls/crypto.h>
#include "utils/aws_sigv4.hh"
#include "utils/hashers.hh"
#include "db_clock.hh"

using namespace std::chrono_literals;

namespace utils {
namespace aws {

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

std::string format_time_point(db_clock::time_point tp) {
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
        throw std::runtime_error(
                fmt::format("Signature expired: {} is now earlier than {} (current time - 15 min.)",
                signature_date, expiration_str));
    }
    if (signature_date > validity_str) {
        throw std::runtime_error(
                fmt::format("Signature not yet current: {} is still later than {} (current time + 15 min.)",
                signature_date, validity_str));
    }
}

std::string get_signature(std::string_view access_key_id, std::string_view secret_access_key,
        std::string_view host, std::string_view canonical_uri, std::string_view method,
        std::optional<std::string_view> orig_datestamp, std::string_view signed_headers_str, const std::map<std::string_view, std::string_view>& signed_headers_map,
        const std::vector<temporary_buffer<char>>* body_content, std::string_view region, std::string_view service, std::string_view query_string) {
    auto amz_date_it = signed_headers_map.find("x-amz-date");
    if (amz_date_it == signed_headers_map.end()) {
        throw std::runtime_error("X-Amz-Date header is mandatory for signature verification");
    }
    std::string_view amz_date = amz_date_it->second;
    std::string_view datestamp = amz_date.substr(0, 8);
    if (orig_datestamp) {
        check_expiry(amz_date);
        if (datestamp != *orig_datestamp) {
            throw std::runtime_error(
                    format("X-Amz-Date date does not match the provided datestamp. Expected {}, got {}",
                            *orig_datestamp, datestamp));
        }
    }

    std::stringstream canonical_headers;
    for (const auto& header : signed_headers_map) {
        canonical_headers << fmt::format("{}:{}", header.first, header.second) << '\n';
    }

    std::string payload_hash = body_content != nullptr ? apply_sha256(*body_content) : "UNSIGNED-PAYLOAD";
    std::string canonical_request = fmt::format("{}\n{}\n{}\n{}\n{}\n{}", method, canonical_uri, query_string, canonical_headers.str(), signed_headers_str, payload_hash);

    std::string_view algorithm = "AWS4-HMAC-SHA256";
    std::string credential_scope = fmt::format("{}/{}/{}/aws4_request", datestamp, region, service);
    std::string string_to_sign = fmt::format("{}\n{}\n{}\n{}", algorithm, amz_date, credential_scope,  apply_sha256(canonical_request));

    hmac_sha256_digest signing_key = get_signature_key(secret_access_key, datestamp, region, service);
    hmac_sha256_digest signature = hmac_sha256(std::string_view(signing_key.data(), signing_key.size()), string_to_sign);

    return to_hex(bytes_view(reinterpret_cast<const int8_t*>(signature.data()), signature.size()));
}

} // aws namespace
} // utils namespace
