/*
 * Copyright (C) 2017-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "auth/authenticated_user.hh"
#include "auth/authenticator.hh"
#include "auth/authorizer.hh"
#include "auth/default_authorizer.hh"
#include "auth/password_authenticator.hh"
#include "auth/permission.hh"
#include "service/raft/raft_group0_client.hh"
#include "utils/class_registrator.hh"

namespace auth {

static const sstring PACKAGE_NAME("com.scylladb.auth.");

static const sstring& transitional_authenticator_name() {
    static const sstring name = PACKAGE_NAME + "TransitionalAuthenticator";
    return name;
}

static const sstring& transitional_authorizer_name() {
    static const sstring name = PACKAGE_NAME + "TransitionalAuthorizer";
    return name;
}

class transitional_authenticator : public authenticator {
    std::unique_ptr<authenticator> _authenticator;

public:
    static const sstring PASSWORD_AUTHENTICATOR_NAME;

    transitional_authenticator(cql3::query_processor& qp, ::service::raft_group0_client& g0, ::service::migration_manager& mm)
            : transitional_authenticator(std::make_unique<password_authenticator>(qp, g0, mm)) {
    }
    transitional_authenticator(std::unique_ptr<authenticator> a)
            : _authenticator(std::move(a)) {
    }

    virtual future<> start() override {
        return _authenticator->start();
    }

    virtual future<> stop() override {
        return _authenticator->stop();
    }

    virtual std::string_view qualified_java_name() const override {
        return transitional_authenticator_name();
    }

    virtual bool require_authentication() const override {
        return true;
    }

    virtual authentication_option_set supported_options() const override {
        return _authenticator->supported_options();
    }

    virtual authentication_option_set alterable_options() const override {
        return _authenticator->alterable_options();
    }

    virtual future<authenticated_user> authenticate(const credentials_map& credentials) const override {
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
            } catch (exceptions::authentication_exception&) {
                // return anon user
                return make_ready_future<authenticated_user>(anonymous_user());
            }
        });
    }

    virtual future<> create(std::string_view role_name, const authentication_options& options, ::service::group0_batch& mc) override {
        return _authenticator->create(role_name, options, mc);
    }

    virtual future<> alter(std::string_view role_name, const authentication_options& options, ::service::group0_batch& mc) override {
        return _authenticator->alter(role_name, options, mc);
    }

    virtual future<> drop(std::string_view role_name, ::service::group0_batch& mc) override {
        return _authenticator->drop(role_name, mc);
    }

    virtual future<custom_options> query_custom_options(std::string_view role_name) const override {
        return _authenticator->query_custom_options(role_name);
    }

    virtual const resource_set& protected_resources() const override {
        return _authenticator->protected_resources();
    }

    virtual ::shared_ptr<sasl_challenge> new_sasl_challenge() const override {
        class sasl_wrapper : public sasl_challenge {
        public:
            sasl_wrapper(::shared_ptr<sasl_challenge> sasl)
                    : _sasl(std::move(sasl)) {
            }

            virtual bytes evaluate_response(bytes_view client_response) override {
                try {
                    return _sasl->evaluate_response(client_response);
                } catch (exceptions::authentication_exception&) {
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
                        } catch (exceptions::authentication_exception&) {
                            // return anon user
                            return make_ready_future<authenticated_user>(anonymous_user());
                        }
                    });
                });
            }

        private:
            ::shared_ptr<sasl_challenge> _sasl;

            bool _complete = false;
        };
        return ::make_shared<sasl_wrapper>(_authenticator->new_sasl_challenge());
    }
};

class transitional_authorizer : public authorizer {
    std::unique_ptr<authorizer> _authorizer;

public:
    transitional_authorizer(cql3::query_processor& qp, ::service::raft_group0_client& g0, ::service::migration_manager& mm)
            : transitional_authorizer(std::make_unique<default_authorizer>(qp, g0, mm)) {
    }
    transitional_authorizer(std::unique_ptr<authorizer> a)
            : _authorizer(std::move(a)) {
    }

    ~transitional_authorizer() {
    }

    virtual future<> start() override {
        return _authorizer->start();
    }

    virtual future<> stop() override {
        return _authorizer->stop();
    }

    virtual std::string_view qualified_java_name() const override {
        return transitional_authorizer_name();
    }

    virtual future<permission_set> authorize(const role_or_anonymous&, const resource&) const override {
        static const permission_set transitional_permissions =
                permission_set::of<
                        permission::CREATE,
                        permission::ALTER,
                        permission::DROP,
                        permission::SELECT,
                        permission::MODIFY>();

        return make_ready_future<permission_set>(transitional_permissions);
    }

    virtual future<> grant(std::string_view s, permission_set ps, const resource& r, ::service::group0_batch& mc)  override {
        return _authorizer->grant(s, std::move(ps), r, mc);
    }

    virtual future<> revoke(std::string_view s, permission_set ps, const resource& r, ::service::group0_batch& mc) override {
        return _authorizer->revoke(s, std::move(ps), r, mc);
    }

    virtual future<std::vector<permission_details>> list_all() const override {
        return _authorizer->list_all();
    }

    virtual future<> revoke_all(std::string_view s, ::service::group0_batch& mc) override {
        return _authorizer->revoke_all(s, mc);
    }

    virtual future<> revoke_all(const resource& r, ::service::group0_batch& mc) override {
        return _authorizer->revoke_all(r, mc);
    }

    virtual const resource_set& protected_resources() const override {
        return _authorizer->protected_resources();
    }
};

}

//
// To ensure correct initialization order, we unfortunately need to use string literals.
//

static const class_registrator<
        auth::authenticator,
        auth::transitional_authenticator,
        cql3::query_processor&,
        ::service::raft_group0_client&,
        ::service::migration_manager&> transitional_authenticator_reg(auth::PACKAGE_NAME + "TransitionalAuthenticator");

static const class_registrator<
        auth::authorizer,
        auth::transitional_authorizer,
        cql3::query_processor&,
        ::service::raft_group0_client&,
        ::service::migration_manager&> transitional_authorizer_reg(auth::PACKAGE_NAME + "TransitionalAuthorizer");
