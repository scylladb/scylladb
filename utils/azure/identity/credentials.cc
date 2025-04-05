/*
 * Copyright (C) 2025 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <seastar/core/coroutine.hh>

#include "credentials.hh"

logger az_creds_logger("azure_creds");

namespace azure {

access_token::access_token(const sstring& token, const timestamp_type& expiry, const resource_type& resource_uri)
    : token(token)
    , expiry(expiry)
    , resource_uri(resource_uri)
{}

bool access_token::empty() const {
    return token.empty();
}

bool access_token::expired() const {
    if (empty()) {
        return true;
    }
    return timeout_clock::now() >= this->expiry;
}

future<access_token> credentials::get_access_token(const resource_type& resource_uri) {
    if (_token.expired() || _token.resource_uri != resource_uri) {
        co_await refresh(resource_uri);
    }
    co_return _token;
}

}
