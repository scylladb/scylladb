/*
 * Copyright (C) 2025 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "azure_cli_credentials.hh"

namespace azure {

azure_cli_credentials::azure_cli_credentials(credential_logger logger)
    : credentials(std::move(logger))
{}

future<> azure_cli_credentials::refresh(const resource_type& resource_uri) {
    throw std::logic_error("Not implemented");
}

}