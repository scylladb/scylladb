/*
 * Copyright (C) 2020 ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "auth/saslauthd_authenticator.hh"

#include <algorithm>
#include <seastar/core/reactor.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/net/api.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/util/log.hh>
#include <system_error>
#include "common.hh"
#include "cql3/query_processor.hh"
#include "db/config.hh"
#include "utils/log.hh"
#include "seastarx.hh"
#include "utils/class_registrator.hh"

namespace auth {

static logging::logger mylog("saslauthd_authenticator");

// To ensure correct initialization order, we unfortunately need to use a string literal.
static const class_registrator<
        authenticator,
        saslauthd_authenticator,
        cql3::query_processor&,
        ::service::raft_group0_client&,
        ::service::migration_manager&> saslauthd_auth_reg("com.scylladb.auth.SaslauthdAuthenticator");

saslauthd_authenticator::saslauthd_authenticator(cql3::query_processor& qp, ::service::raft_group0_client&, ::service::migration_manager&)
    : _socket_path(qp.db().get_config().saslauthd_socket_path())
{}

future<> saslauthd_authenticator::start() {
    return once_among_shards([this] {
        return file_exists(_socket_path).then([this] (bool exists) {
            if (!exists) {
                mylog.warn("saslauthd socket file {} doesn't exist -- is saslauthd running?", _socket_path);
            }
            return make_ready_future();
        });
    });
}

future<> saslauthd_authenticator::stop() { return make_ready_future(); }

std::string_view saslauthd_authenticator::qualified_java_name() const {
    return "com.scylladb.auth.SaslauthdAuthenticator";
}

bool saslauthd_authenticator::require_authentication() const {
    return true;
}

authentication_option_set saslauthd_authenticator::supported_options() const {
    return authentication_option_set{authentication_option::password, authentication_option::options};
}

authentication_option_set saslauthd_authenticator::alterable_options() const {
    return supported_options();
}

namespace {

// Note the saslauthd protocol description:
// https://github.com/cyrusimap/cyrus-sasl/blob/f769dde423e1b3ae8bfb35b826fca3d5f1e1f6fe/saslauthd/saslauthd-main.c#L74

constexpr size_t len_size = sizeof(htons(0));

char* pack(std::string_view s, char* p) {
    uint16_t size = s.size();
    produce_be(p, size);
    memcpy(p, s.data(), size);
    return p + size;
}

temporary_buffer<char> make_saslauthd_message(const saslauthd_credentials& creds) {
    temporary_buffer<char> message(
            creds.username.size() + creds.password.size() + creds.service.size() + creds.realm.size()
            + 4 * len_size);
    auto p = pack(creds.username, message.get_write());
    p = pack(creds.password, p);
    p = pack(creds.service, p);
    p = pack(creds.realm, p);
    return message;
}

/// An exception handler that reports saslauthd socket IO error.
future<bool> as_authentication_exception(std::exception_ptr ex) {
    return make_exception_future<bool>(
            exceptions::authentication_exception(format("saslauthd socket IO error: {}", ex)));
}

} // anonymous namespace

future<bool> authenticate_with_saslauthd(sstring saslauthd_socket_path, const saslauthd_credentials& creds) {
    socket_address addr((unix_domain_addr(saslauthd_socket_path)));
    // TODO: switch to seastar::connect() when it supports Unix domain sockets.
    return engine().net().connect(addr).then([creds = std::move(creds)] (connected_socket s) {
        return do_with(
                s.input(), s.output(),
                [creds = std::move(creds)] (input_stream<char>& in, output_stream<char>& out) {
                    return out.write(make_saslauthd_message(creds)).then([&in, &out] () mutable {
                        return out.flush().then([&in] () mutable {
                            return in.read_exactly(2).then([&in] (temporary_buffer<char> len) mutable {
                                if (len.size() < 2) {
                                    return make_exception_future<bool>(
                                            exceptions::authentication_exception(
                                                    "saslauthd closed connection before completing response"));
                                }
                                const auto paylen = read_be<uint16_t>(len.get());
                                return in.read_exactly(paylen).then([paylen] (temporary_buffer<char> resp) {
                                    mylog.debug("saslauthd response: {}", std::string_view(resp.get(), resp.size()));
                                    if (resp.size() != paylen) {
                                        return make_exception_future<bool>(
                                            exceptions::authentication_exception(
                                                    // We say "different" here, though we could just as well say
                                                    // "shorter".  A longer response is cut to size by
                                                    // read_exactly().
                                                    "saslauthd response length different than promised"));
                                    }
                                    bool ok = (resp.size() >= 2 && resp[0] == 'O' && resp[1] == 'K');
                                    return make_ready_future<bool>(ok);
                                });
                            }).finally([&in] () mutable { return in.close(); });
                        }).handle_exception(as_authentication_exception).finally([&out] () mutable {
                            return out.close();
                        });
                    });
                });
    }).handle_exception_type([] (std::system_error& e) {
        return make_exception_future<bool>(
                exceptions::authentication_exception(format("saslauthd socket connection error: {}", e.what())));
    });
}

future<authenticated_user> saslauthd_authenticator::authenticate(const credentials_map& credentials) const {
    const auto username_found = credentials.find(USERNAME_KEY);
    if (username_found == credentials.end()) {
        throw exceptions::authentication_exception(format("Required key '{}' is missing", USERNAME_KEY));
    }
    const auto password_found = credentials.find(PASSWORD_KEY);
    if (password_found == credentials.end()) {
        throw exceptions::authentication_exception(format("Required key '{}' is missing", PASSWORD_KEY));
    }
    const auto service_found = credentials.find(SERVICE_KEY);
    const auto realm_found = credentials.find(REALM_KEY);

    sstring username = username_found->second;
    return authenticate_with_saslauthd(_socket_path, {username, password_found->second,
            service_found == credentials.end() ? "" : service_found->second,
            realm_found == credentials.end() ? "" : realm_found->second}).then([username] (bool ok) {
                if (!ok) {
                    throw exceptions::authentication_exception("Incorrect credentials");
                }
                return make_ready_future<authenticated_user>(username);
            });
}

future<> saslauthd_authenticator::create(std::string_view role_name, const authentication_options& options, ::service::group0_batch& mc) {
    if (!options.credentials) {
        return make_ready_future<>();
    }
    throw exceptions::authentication_exception("Cannot create passwords with SaslauthdAuthenticator");
}

future<> saslauthd_authenticator::alter(std::string_view role_name, const authentication_options& options, ::service::group0_batch& mc) {
    if (!options.credentials) {
        return make_ready_future<>();
    }
    throw exceptions::authentication_exception("Cannot modify passwords with SaslauthdAuthenticator");
}

future<> saslauthd_authenticator::drop(std::string_view name, ::service::group0_batch& mc) {
    throw exceptions::authentication_exception("Cannot delete passwords with SaslauthdAuthenticator");
}

future<custom_options> saslauthd_authenticator::query_custom_options(std::string_view role_name) const {
    return make_ready_future<custom_options>();
}

const resource_set& saslauthd_authenticator::protected_resources() const {
    static const resource_set empty;
    return empty;
}

::shared_ptr<sasl_challenge> saslauthd_authenticator::new_sasl_challenge() const {
    return ::make_shared<plain_sasl_challenge>([this](std::string_view username, std::string_view password) {
        return this->authenticate(credentials_map{{USERNAME_KEY, sstring(username)}, {PASSWORD_KEY, sstring(password)}});
    });
}

} // namespace auth
