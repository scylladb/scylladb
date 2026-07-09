/*
 * Copyright (C) 2026-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "auth/certificate_or_password_authenticator.hh"

#include "auth/certificate_authenticator.hh"
#include "auth/password_authenticator.hh"
#include "auth/authenticated_user.hh"
#include "auth/config.hh"

namespace auth {

certificate_or_password_authenticator::certificate_or_password_authenticator(
        cql3::query_processor& qp,
        ::service::raft_group0_client& g0,
        ::service::migration_manager& mm,
        cache& c,
        const config& cfg)
    : _cert_auth(std::make_unique<certificate_authenticator>(qp, g0, mm, c, cfg))
    , _pwd_auth(std::make_unique<password_authenticator>(qp, g0, mm, c, cfg))
{}

certificate_or_password_authenticator::~certificate_or_password_authenticator() = default;

future<> certificate_or_password_authenticator::start() {
    co_await _cert_auth->start();
    co_await _pwd_auth->start();
}

future<> certificate_or_password_authenticator::stop() {
    co_await _cert_auth->stop();
    co_await _pwd_auth->stop();
}

std::string_view certificate_or_password_authenticator::qualified_java_name() const {
    return "com.scylladb.auth.CertificateOrPasswordAuthenticator";
}

bool certificate_or_password_authenticator::require_authentication() const {
    return _pwd_auth->require_authentication();
}

authentication_option_set certificate_or_password_authenticator::supported_options() const {
    return _pwd_auth->supported_options();
}

authentication_option_set certificate_or_password_authenticator::alterable_options() const {
    return _pwd_auth->alterable_options();
}

future<std::optional<authenticated_user>>
certificate_or_password_authenticator::authenticate(session_dn_func f) const {
    if (!f) {
        // No TLS on this connection (e.g. plain-text port or maintenance
        // socket): fall back to the SASL/password path.
        co_return std::nullopt;
    }
    auto dninfo = co_await f();
    if (!dninfo) {
        // TLS handshake succeeded but the client did not present a
        // certificate (REQUEST mode).  Fall back to the SASL/password path.
        co_return std::nullopt;
    }
    // The client presented a certificate.  Delegate to certificate_authenticator,
    // passing a lambda that returns the already-fetched certificate info so
    // the TLS socket is not queried a second time.
    auto cached = std::move(dninfo);
    co_return co_await _cert_auth->authenticate(
        [cached = std::move(cached)]() mutable
        -> future<std::optional<certificate_info>> {
            co_return std::move(cached);
        });
}

future<authenticated_user>
certificate_or_password_authenticator::authenticate(const credentials_map& creds) const {
    return _pwd_auth->authenticate(creds);
}

future<> certificate_or_password_authenticator::create(
        std::string_view role_name,
        const authentication_options& options,
        ::service::group0_batch& mc) {
    return _pwd_auth->create(role_name, options, mc);
}

future<> certificate_or_password_authenticator::alter(
        std::string_view role_name,
        const authentication_options& options,
        ::service::group0_batch& mc) {
    return _pwd_auth->alter(role_name, options, mc);
}

future<> certificate_or_password_authenticator::drop(
        std::string_view role_name,
        ::service::group0_batch& mc) {
    return _pwd_auth->drop(role_name, mc);
}

future<custom_options>
certificate_or_password_authenticator::query_custom_options(std::string_view role_name) const {
    return _pwd_auth->query_custom_options(role_name);
}

const resource_set& certificate_or_password_authenticator::protected_resources() const {
    return _pwd_auth->protected_resources();
}

::shared_ptr<sasl_challenge> certificate_or_password_authenticator::new_sasl_challenge() const {
    return _pwd_auth->new_sasl_challenge();
}

bool certificate_or_password_authenticator::uses_password_hashes() const {
    return _pwd_auth->uses_password_hashes();
}

future<std::optional<sstring>>
certificate_or_password_authenticator::get_password_hash(std::string_view role_name) const {
    return _pwd_auth->get_password_hash(role_name);
}

future<> certificate_or_password_authenticator::ensure_superuser_is_created() const {
    return _pwd_auth->ensure_superuser_is_created();
}

} // namespace auth
