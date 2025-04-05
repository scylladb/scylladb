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
        const sstring& truststore, const sstring& priority_string, const sstring& logctx)
    : credentials(logctx)
    , _sources(sources)
    , _truststore(truststore)
    , _priority_string(priority_string)
{}

future<> default_credentials::refresh(const resource_type& resource_uri) {
    throw std::logic_error("Not implemented");
}

}