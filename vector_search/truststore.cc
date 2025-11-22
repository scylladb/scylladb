/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "truststore.hh"
#include <seastar/core/smp.hh>

namespace vector_search {

truststore::truststore(logging::logger& logger, options_type options, invoke_on_others_type invoke_on_others)
    : _logger(logger)
    , _options(std::move(options))
    , _invoke_on_others(std::move(invoke_on_others)) {
}

seastar::future<seastar::shared_ptr<seastar::tls::certificate_credentials>> truststore::get() {
    if (!_credentials) {
        seastar::tls::credentials_builder builder = co_await create_builder();
        // To reduce the number of system file watchers, only shard 0 will watch for changes to the truststore file.
        // When a change is detected, shard 0 will propagate the updated credentials to the other shards.
        if (this_shard_id() == 0) {
            _credentials = co_await builder.build_reloadable_certificate_credentials(
                    [this](const tls::credentials_builder& b, const std::unordered_set<sstring>& files, std::exception_ptr ep) -> future<> {
                        if (ep) {
                            _logger.warn("Exception while reloading truststore {}: {}", files, ep);
                        } else {
                            co_await _invoke_on_others([&](auto& self) {
                                if (self._credentials) {
                                    b.rebuild(*self._credentials);
                                }
                                return make_ready_future();
                            });
                        }
                    });
        } else {
            _credentials = builder.build_certificate_credentials();
        }
    }
    co_return _credentials;
}

seastar::future<> truststore::stop() {
    co_await _gate.close();
}

seastar::future<seastar::tls::credentials_builder> truststore::create_builder() const {
    seastar::tls::credentials_builder builder;
    if (_options.get().contains("truststore")) {
        co_await builder.set_x509_trust_file(_options.get().at("truststore"), seastar::tls::x509_crt_format::PEM);
    } else {
        co_await builder.set_system_trust();
    }
    builder.set_session_resume_mode(seastar::tls::session_resume_mode::TLS13_SESSION_TICKET);
    co_return builder;
}

} // namespace vector_search
