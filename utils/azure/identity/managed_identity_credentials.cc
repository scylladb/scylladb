/*
 * Copyright (C) 2025 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "managed_identity_credentials.hh"

namespace azure {

managed_identity_credentials::managed_identity_credentials(const sstring& logctx)
    : credentials(logctx)
{}

future<> managed_identity_credentials::refresh(const resource_type& resource_uri) {
    throw std::logic_error("Not implemented");
}

}