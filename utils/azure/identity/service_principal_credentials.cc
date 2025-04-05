/*
 * Copyright (C) 2025 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <boost/regex.hpp>

#include "service_principal_credentials.hh"

namespace azure {

service_principal_credentials::service_principal_credentials(const sstring& tenant_id,
        const sstring& client_id, const sstring& client_secret, const sstring& client_cert,
        const sstring& authority, const sstring& truststore, const sstring& priority_string,
        const sstring& logctx)
    : credentials(logctx)
    , _tenant_id(tenant_id)
    , _client_id(client_id)
    , _client_secret(client_secret)
    , _client_cert(client_cert)
    , _truststore(truststore)
    , _priority_string(priority_string)
{
    if (authority.empty()) {
        return;
    }
    // Regex for the authentication authority URL.
    // Expected format: [http(s)://]<host>[:port]
    static const boost::regex uri_pattern(R"((?:(https?)://)?([^/:]+)(?::(\d+))?)");
    boost::match_results<std::string_view::const_iterator> match;
    std::string_view authority_view{authority};
    if (boost::regex_match(authority_view.begin(), authority_view.end(), match, uri_pattern)) {
        if (match[1].matched) {
            _is_secured = (match[1].str() == "https");
        }
        _host = match[2].str();
        if (match[3].matched) {
            _port = std::stoi(match[3].str());
        }
    } else {
        throw std::invalid_argument(fmt::format("Invalid authentication authority URL '{}'. Expected format: [http(s)://]<host>[:port]", authority));
    }
}

future<> service_principal_credentials::refresh(const resource_type& resource_uri) {
    throw std::logic_error("Not implemented");
}

}