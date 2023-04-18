/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <gnutls/crypto.h>
#include "db_clock.hh"

// The declared below get_signature() method makes the Signature string for AWS
// authenticated requests as described in [1]. It can be used in two ways.
//
// First, if a request is about to be sent, the method can be used to create the
// signature value that'll later be included into Authorization header, Signature
// part. It's up to the caller to provide request with relevant headers and the
// signed_headers_map list.
//
// Second, for a received request this method can be used to calculate the signature
// that can later be compared with the request's Authorization header, Signature
// part for correctness.
//
// [1] https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-auth-using-authorization-header.html

namespace utils {

using hmac_sha256_digest = std::array<char, 32>;

namespace aws {

std::string get_signature(std::string_view access_key_id, std::string_view secret_access_key,
        std::string_view host, std::string_view canonical_uri, std::string_view method,
        std::optional<std::string_view> orig_datestamp, std::string_view signed_headers_str, const std::map<std::string_view, std::string_view>& signed_headers_map,
        const std::vector<temporary_buffer<char>>* body_content, std::string_view region, std::string_view service, std::string_view query_string);

// Convenience alias not to pass obscure nullptr argument to get_signature()
static inline constexpr std::vector<temporary_buffer<char>>* unsigned_content = nullptr;
// Same for datestamp checking
static inline auto omit_datestamp_expiration_check = std::nullopt;

std::string format_time_point(db_clock::time_point tp);

} // aws namespace
} // utils namespace
