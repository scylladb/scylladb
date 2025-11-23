/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once
#include "utils/updateable_value.hh"
#include "utils/log.hh"
#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>
#include <seastar/net/tls.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/gate.hh>
#include <unordered_map>

namespace vector_search {

/// Manages the TLS truststore for secure (HTTPS) connections to the vector store service.
class truststore {
public:
    using options_type = utils::updateable_value<std::unordered_map<seastar::sstring, seastar::sstring>>;
    using invoke_on_others_type = std::function<seastar::future<>(std::function<seastar::future<>(truststore&)>)>;

    explicit truststore(logging::logger& logger, options_type options, invoke_on_others_type invoke_on_others);

    seastar::future<seastar::shared_ptr<seastar::tls::certificate_credentials>> get();
    seastar::future<> stop();

private:
    seastar::future<seastar::tls::credentials_builder> create_builder() const;

    logging::logger& _logger;
    options_type _options;
    seastar::shared_ptr<seastar::tls::certificate_credentials> _credentials;
    invoke_on_others_type _invoke_on_others;
    seastar::gate _gate;
};

} // namespace vector_search
