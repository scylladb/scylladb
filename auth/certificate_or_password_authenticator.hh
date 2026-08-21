/*
 * Copyright (C) 2026-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <memory>

#include "auth/authenticator.hh"

namespace cql3 {
class query_processor;
} // namespace cql3

namespace service {
class migration_manager;
class raft_group0_client;
} // namespace service

namespace auth {

class certificate_authenticator;
class password_authenticator;
class cache;
struct config;

///
/// An authenticator that accepts either a TLS client certificate **or** a
/// username/password pair on the same CQL port.
///
/// This is designed for use together with a truststore and
/// ``require_client_auth: optional`` (Seastar's REQUEST mode).  In that TLS
/// mode the server requests a client certificate during the handshake but does
/// not mandate one, so connections arrive with or without a certificate:
///
///   * If the client presents a certificate signed by the trusted CA, the
///     certificate's subject and/or ALTNAME (SAN) fields are matched against
///     auth_certificate_role_queries to derive the CQL role name, exactly as
///     CertificateAuthenticator does.  No SASL round-trip is needed.
///
///   * If the client presents no certificate, the server issues a SASL
///     challenge and the client authenticates with a username/password pair,
///     exactly as PasswordAuthenticator does.
///
/// The implementation of this class is not much more than a wrapper around
/// the existing CertificateAuthenticator and PasswordAuthenticator classes,
/// delegating to one or the other depending on whether a certificate is
/// present.
///
class certificate_or_password_authenticator : public authenticator {
    std::unique_ptr<certificate_authenticator> _cert_auth;
    std::unique_ptr<password_authenticator>    _pwd_auth;

public:
    certificate_or_password_authenticator(cql3::query_processor&,
                                          ::service::raft_group0_client&,
                                          ::service::migration_manager&,
                                          cache&,
                                          const config&);

    ~certificate_or_password_authenticator();

    future<> start() override;
    future<> stop() override;

    std::string_view qualified_java_name() const override;
    bool require_authentication() const override;

    authentication_option_set supported_options() const override;
    authentication_option_set alterable_options() const override;

    // If a client certificate is present, extract the role from its subject
    // and/or ALTNAME (SAN) fields (as configured via auth_certificate_role_queries)
    // and return the user without issuing a SASL challenge. If no client
    // certificate is present, return std::nullopt so process_startup falls
    // back to SASL (password path).
    future<std::optional<authenticated_user>> authenticate(session_dn_func) const override;

    // Password path (SASL AUTH_RESPONSE): delegate to password_authenticator.
    future<authenticated_user> authenticate(const credentials_map&) const override;

    future<> create(std::string_view, const authentication_options&, ::service::group0_batch&) override;
    future<> alter(std::string_view, const authentication_options&, ::service::group0_batch&) override;
    future<> drop(std::string_view, ::service::group0_batch&) override;

    future<custom_options> query_custom_options(std::string_view) const override;

    const resource_set& protected_resources() const override;

    ::shared_ptr<sasl_challenge> new_sasl_challenge() const override;

    bool uses_password_hashes() const override;
    future<std::optional<sstring>> get_password_hash(std::string_view) const override;

    future<> ensure_superuser_is_created() const override;
};

} // namespace auth
