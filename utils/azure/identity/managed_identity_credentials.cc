/*
 * Copyright (C) 2025 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <boost/regex.hpp>

#include "managed_identity_credentials.hh"

namespace azure {

managed_identity_credentials::managed_identity_credentials(const sstring& endpoint, const sstring& logctx)
    : credentials(logctx)
{
    if (endpoint.empty()) {
        return;
    }
    // Regex for the IMDS endpoint.
    // Expected format: [http://]<host>[:port]
    static const boost::regex uri_pattern(R"((?:http://)?([^/:]+)(?::(\d+))?)");
    boost::match_results<std::string_view::const_iterator> match;
    std::string_view endpoint_view{endpoint};
    if (boost::regex_match(endpoint_view.begin(), endpoint_view.end(), match, uri_pattern)) {
        _host = match[1].str();
        if (match[2].matched) {
            _port = std::stoi(match[2].str());
        }
    } else {
        throw std::invalid_argument(fmt::format("Invalid IMDS endpoint '{}'. Expected format: [http://]<host>[:port]", endpoint));
    }
}

future<> managed_identity_credentials::refresh(const resource_type& resource_uri) {
    throw std::logic_error("Not implemented");
}

}