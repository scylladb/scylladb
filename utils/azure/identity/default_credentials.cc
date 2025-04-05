/*
 * Copyright (C) 2025 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "default_credentials.hh"

namespace azure {

default_credentials::default_credentials(const source_set& sources,
        const sstring& imds_endpoint, const sstring& truststore,
        const sstring& priority_string, credential_logger logger)
    : credentials(std::move(logger))
    , _sources(sources)
    , _truststore(truststore)
    , _priority_string(priority_string)
    , _imds_endpoint(imds_endpoint)
{}

future<> default_credentials::refresh(const resource_type& resource_uri) {
    throw std::logic_error("Not implemented");
}

}