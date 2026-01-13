/*
 * Copyright (C) 2017-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#include "auth/transitional.hh"
#include "auth/authenticated_user.hh"
#include "auth/default_authorizer.hh"
#include "auth/password_authenticator.hh"
#include "auth/permission.hh"
#include "service/raft/raft_group0_client.hh"

namespace auth {

transitional_authenticator::transitional_authenticator(cql3::query_processor& qp, ::service::raft_group0_client& g0, ::service::migration_manager& mm, cache& cache)
        : transitional_authenticator(std::make_unique<password_authenticator>(qp, g0, mm, cache)) {
}

transitional_authenticator::transitional_authenticator(std::unique_ptr<authenticator> a)
        : _authenticator(std::move(a)) {
}

future<> transitional_authenticator::start() {
    return _authenticator->start();
}

future<> transitional_authenticator::stop() {
    return _authenticator->stop();
}

std::string_view transitional_authenticator::qualified_java_name() const {
    return "com.scylladb.auth.TransitionalAuthenticator";
}

bool transitional_authenticator::require_authentication() const {
    return true;
}

authentication_option_set transitional_authenticator::supported_options() const {
    return _authenticator->supported_options();
}

authentication_option_set transitional_authenticator::alterable_options() const {
    return _authenticator->alterable_options();
}

future<authenticated_user> transitional_authenticator::authenticate(const credentials_map& credentials) const {
    auto i = credentials.find(authenticator::USERNAME_KEY);
    if ((i == credentials.end() || i->second.empty())
            && (!credentials.contains(PASSWORD_KEY) || credentials.at(PASSWORD_KEY).empty())) {
        // return anon user
        return make_ready_future<authenticated_user>(anonymous_user());
    }
    return make_ready_future().then([this, &credentials] {
        return _authenticator->authenticate(credentials);
    }).handle_exception([](auto ep) {
        try {
            std::rethrow_exception(ep);
        } catch (const exceptions::authentication_exception&) {
            // return anon user
            return make_ready_future<authenticated_user>(anonymous_user());
        }
    });
}

future<> transitional_authenticator::create(std::string_view role_name, const authentication_options& options, ::service::group0_batch& mc) {
    return _authenticator->create(role_name, options, mc);
}

future<> transitional_authenticator::alter(std::string_view role_name, const authentication_options& options, ::service::group0_batch& mc) {
    return _authenticator->alter(role_name, options, mc);
}

future<> transitional_authenticator::drop(std::string_view role_name, ::service::group0_batch& mc) {
    return _authenticator->drop(role_name, mc);
}

future<custom_options> transitional_authenticator::query_custom_options(std::string_view role_name) const {
    return _authenticator->query_custom_options(role_name);
}

bool transitional_authenticator::uses_password_hashes() const {
    return _authenticator->uses_password_hashes();
}

future<std::optional<sstring>> transitional_authenticator::get_password_hash(std::string_view role_name) const {
    return _authenticator->get_password_hash(role_name);
}

const resource_set& transitional_authenticator::protected_resources() const {
    return _authenticator->protected_resources();
}

::shared_ptr<sasl_challenge> transitional_authenticator::new_sasl_challenge() const {
    class sasl_wrapper : public sasl_challenge {
    public:
        sasl_wrapper(::shared_ptr<sasl_challenge> sasl)
                : _sasl(std::move(sasl)) {
        }

        virtual bytes evaluate_response(bytes_view client_response) override {
            try {
                return _sasl->evaluate_response(client_response);
            } catch (const exceptions::authentication_exception&) {
                _complete = true;
                return {};
            }
        }

        virtual bool is_complete() const override {
            return _complete || _sasl->is_complete();
        }

        virtual future<authenticated_user> get_authenticated_user() const override {
            return futurize_invoke([this] {
                return _sasl->get_authenticated_user().handle_exception([](auto ep) {
                    try {
                        std::rethrow_exception(ep);
                    } catch (const exceptions::authentication_exception&) {
                        // return anon user
                        return make_ready_future<authenticated_user>(anonymous_user());
                    }
                });
            });
        }

        const sstring& get_username() const override {
            return _sasl->get_username();
        }

    private:
        ::shared_ptr<sasl_challenge> _sasl;

        bool _complete = false;
    };
    return ::make_shared<sasl_wrapper>(_authenticator->new_sasl_challenge());
}

future<> transitional_authenticator::ensure_superuser_is_created() const {
    return _authenticator->ensure_superuser_is_created();
}

transitional_authorizer::transitional_authorizer(cql3::query_processor& qp, ::service::raft_group0_client& g0, ::service::migration_manager& mm)
        : transitional_authorizer(std::make_unique<default_authorizer>(qp, g0, mm)) {
}

transitional_authorizer::transitional_authorizer(std::unique_ptr<authorizer> a)
        : _authorizer(std::move(a)) {
}

transitional_authorizer::~transitional_authorizer() {
}

future<> transitional_authorizer::start() {
    return _authorizer->start();
}

future<> transitional_authorizer::stop() {
    return _authorizer->stop();
}

std::string_view transitional_authorizer::qualified_java_name() const {
    return "com.scylladb.auth.TransitionalAuthorizer";
}

future<permission_set> transitional_authorizer::authorize(const role_or_anonymous&, const resource&) const {
    static const permission_set transitional_permissions =
            permission_set::of<
                    permission::CREATE,
                    permission::ALTER,
                    permission::DROP,
                    permission::SELECT,
                    permission::MODIFY>();

    return make_ready_future<permission_set>(transitional_permissions);
}

future<> transitional_authorizer::grant(std::string_view s, permission_set ps, const resource& r, ::service::group0_batch& mc) {
    return _authorizer->grant(s, std::move(ps), r, mc);
}

future<> transitional_authorizer::revoke(std::string_view s, permission_set ps, const resource& r, ::service::group0_batch& mc) {
    return _authorizer->revoke(s, std::move(ps), r, mc);
}

future<std::vector<permission_details>> transitional_authorizer::list_all() const {
    return _authorizer->list_all();
}

future<> transitional_authorizer::revoke_all(std::string_view s, ::service::group0_batch& mc) {
    return _authorizer->revoke_all(s, mc);
}

future<> transitional_authorizer::revoke_all(const resource& r, ::service::group0_batch& mc) {
    return _authorizer->revoke_all(r, mc);
}

const resource_set& transitional_authorizer::protected_resources() const {
    return _authorizer->protected_resources();
}

}
