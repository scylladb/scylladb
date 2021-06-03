/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "auth/service.hh"

#include <algorithm>
#include <map>

#include <seastar/core/future-util.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>

#include "auth/allow_all_authenticator.hh"
#include "auth/allow_all_authorizer.hh"
#include "auth/common.hh"
#include "auth/role_or_anonymous.hh"
#include "cql3/query_processor.hh"
#include "cql3/untyped_result_set.hh"
#include "db/consistency_level_type.hh"
#include "exceptions/exceptions.hh"
#include "log.hh"
#include "service/migration_manager.hh"
#include "utils/class_registrator.hh"
#include "database.hh"

namespace auth {

namespace meta {

static const sstring user_name_col_name("name");
static const sstring superuser_col_name("super");

}

static logging::logger log("auth_service");

class auth_migration_listener final : public ::service::migration_listener {
    authorizer& _authorizer;

public:
    explicit auth_migration_listener(authorizer& a) : _authorizer(a) {
    }

private:
    void on_create_keyspace(const sstring& ks_name) override {}
    void on_create_column_family(const sstring& ks_name, const sstring& cf_name) override {}
    void on_create_user_type(const sstring& ks_name, const sstring& type_name) override {}
    void on_create_function(const sstring& ks_name, const sstring& function_name) override {}
    void on_create_aggregate(const sstring& ks_name, const sstring& aggregate_name) override {}
    void on_create_view(const sstring& ks_name, const sstring& view_name) override {}

    void on_update_keyspace(const sstring& ks_name) override {}
    void on_update_column_family(const sstring& ks_name, const sstring& cf_name, bool) override {}
    void on_update_user_type(const sstring& ks_name, const sstring& type_name) override {}
    void on_update_function(const sstring& ks_name, const sstring& function_name) override {}
    void on_update_aggregate(const sstring& ks_name, const sstring& aggregate_name) override {}
    void on_update_view(const sstring& ks_name, const sstring& view_name, bool columns_changed) override {}

    void on_drop_keyspace(const sstring& ks_name) override {
        // Do it in the background.
        (void)_authorizer.revoke_all(
                auth::make_data_resource(ks_name)).handle_exception_type([](const unsupported_authorization_operation&) {
            // Nothing.
        }).handle_exception([] (std::exception_ptr e) {
            log.error("Unexpected exception while revoking all permissions on dropped keyspace: {}", e);
        });
    }

    void on_drop_column_family(const sstring& ks_name, const sstring& cf_name) override {
        // Do it in the background.
        (void)_authorizer.revoke_all(
                auth::make_data_resource(
                        ks_name, cf_name)).handle_exception_type([](const unsupported_authorization_operation&) {
            // Nothing.
        }).handle_exception([] (std::exception_ptr e) {
            log.error("Unexpected exception while revoking all permissions on dropped table: {}", e);
        });
    }

    void on_drop_user_type(const sstring& ks_name, const sstring& type_name) override {}
    void on_drop_function(const sstring& ks_name, const sstring& function_name) override {}
    void on_drop_aggregate(const sstring& ks_name, const sstring& aggregate_name) override {}
    void on_drop_view(const sstring& ks_name, const sstring& view_name) override {}
};

static future<> validate_role_exists(const service& ser, std::string_view role_name) {
    return ser.underlying_role_manager().exists(role_name).then([role_name](bool exists) {
        if (!exists) {
            throw nonexistant_role(role_name);
        }
    });
}

service::service(
        permissions_cache_config c,
        cql3::query_processor& qp,
        ::service::migration_notifier& mn,
        std::unique_ptr<authorizer> z,
        std::unique_ptr<authenticator> a,
        std::unique_ptr<role_manager> r)
            : _permissions_cache_config(std::move(c))
            , _permissions_cache(nullptr)
            , _qp(qp)
            , _mnotifier(mn)
            , _authorizer(std::move(z))
            , _authenticator(std::move(a))
            , _role_manager(std::move(r))
            , _migration_listener(std::make_unique<auth_migration_listener>(*_authorizer)) {}

service::service(
        permissions_cache_config c,
        cql3::query_processor& qp,
        ::service::migration_notifier& mn,
        ::service::migration_manager& mm,
        const service_config& sc)
            : service(
                      std::move(c),
                      qp,
                      mn,
                      create_object<authorizer>(sc.authorizer_java_name, qp, mm),
                      create_object<authenticator>(sc.authenticator_java_name, qp, mm),
                      create_object<role_manager>(sc.role_manager_java_name, qp, mm)) {
}

future<> service::create_keyspace_if_missing(::service::migration_manager& mm) const {
    auto& db = _qp.db();

    if (!db.has_keyspace(meta::AUTH_KS)) {
        std::map<sstring, sstring> opts{{"replication_factor", "1"}};

        auto ksm = keyspace_metadata::new_keyspace(
                meta::AUTH_KS,
                "org.apache.cassandra.locator.SimpleStrategy",
                opts,
                true);

        // We use min_timestamp so that default keyspace metadata will loose with any manual adjustments.
        // See issue #2129.
        return mm.announce_new_keyspace(ksm, api::min_timestamp);
    }

    return make_ready_future<>();
}

future<> service::start(::service::migration_manager& mm) {
    return once_among_shards([this, &mm] {
        return create_keyspace_if_missing(mm);
    }).then([this] {
        return _role_manager->start().then([this] {
            return when_all_succeed(_authorizer->start(), _authenticator->start()).discard_result();
        });
    }).then([this] {
        _permissions_cache = std::make_unique<permissions_cache>(_permissions_cache_config, *this, log);
    }).then([this] {
        return once_among_shards([this] {
            _mnotifier.register_listener(_migration_listener.get());
            return make_ready_future<>();
        });
    });
}

future<> service::stop() {
    // Only one of the shards has the listener registered, but let's try to
    // unregister on each one just to make sure.
    return _mnotifier.unregister_listener(_migration_listener.get()).then([this] {
        if (_permissions_cache) {
            return _permissions_cache->stop();
        }
        return make_ready_future<>();
    }).then([this] {
        return when_all_succeed(_role_manager->stop(), _authorizer->stop(), _authenticator->stop()).discard_result();
    });
}

future<bool> service::has_existing_legacy_users() const {
    if (!_qp.db().has_schema(meta::AUTH_KS, meta::USERS_CF)) {
        return make_ready_future<bool>(false);
    }

    static const sstring default_user_query = format("SELECT * FROM {}.{} WHERE {} = ?",
            meta::AUTH_KS,
            meta::USERS_CF,
            meta::user_name_col_name);

    static const sstring all_users_query = format("SELECT * FROM {}.{} LIMIT 1",
            meta::AUTH_KS,
            meta::USERS_CF);

    // This logic is borrowed directly from Apache Cassandra. By first checking for the presence of the default user, we
    // can potentially avoid doing a range query with a high consistency level.

    return _qp.execute_internal(
            default_user_query,
            db::consistency_level::ONE,
            {meta::DEFAULT_SUPERUSER_NAME},
            true).then([this](auto results) {
        if (!results->empty()) {
            return make_ready_future<bool>(true);
        }

        return _qp.execute_internal(
                default_user_query,
                db::consistency_level::QUORUM,
                {meta::DEFAULT_SUPERUSER_NAME},
                true).then([this](auto results) {
            if (!results->empty()) {
                return make_ready_future<bool>(true);
            }

            return _qp.execute_internal(
                    all_users_query,
                    db::consistency_level::QUORUM).then([](auto results) {
                return make_ready_future<bool>(!results->empty());
            });
        });
    });
}

future<permission_set>
service::get_uncached_permissions(const role_or_anonymous& maybe_role, const resource& r) const {
    if (is_anonymous(maybe_role)) {
        return _authorizer->authorize(maybe_role, r);
    }

    const std::string_view role_name = *maybe_role.name;

    return has_superuser(role_name).then([this, role_name, &r](bool superuser) {
        if (superuser) {
            return make_ready_future<permission_set>(r.applicable_permissions());
        }

        //
        // Aggregate the permissions from all granted roles.
        //

        return do_with(permission_set(), [this, role_name, &r](auto& all_perms) {
            return get_roles(role_name).then([this, &r, &all_perms](role_set all_roles) {
                return do_with(std::move(all_roles), [this, &r, &all_perms](const auto& all_roles) {
                    return parallel_for_each(all_roles, [this, &r, &all_perms](std::string_view role_name) {
                        return _authorizer->authorize(role_name, r).then([&all_perms](permission_set perms) {
                            all_perms = permission_set::from_mask(all_perms.mask() | perms.mask());
                        });
                    });
                });
            }).then([&all_perms] {
                return all_perms;
            });
        });
    });
}

future<permission_set> service::get_permissions(const role_or_anonymous& maybe_role, const resource& r) const {
    return _permissions_cache->get(maybe_role, r);
}

future<bool> service::has_superuser(std::string_view role_name) const {
    return this->get_roles(std::move(role_name)).then([this](role_set roles) {
        return do_with(std::move(roles), [this](const role_set& roles) {
            return do_with(false, roles.begin(), [this, &roles](bool& any_super, auto& iter) {
                return do_until(
                        [&roles, &any_super, &iter] { return any_super || (iter == roles.end()); },
                        [this, &any_super, &iter] {
                    return _role_manager->is_superuser(*iter++).then([&any_super](bool super) {
                        any_super = super;
                    });
                }).then([&any_super] {
                    return any_super;
                });
            });
        });
    });
}

future<role_set> service::get_roles(std::string_view role_name) const {
    //
    // We may wish to cache this information in the future (as Apache Cassandra does).
    //

    return _role_manager->query_granted(role_name, recursive_role_query::yes);
}

future<bool> service::exists(const resource& r) const {
    switch (r.kind()) {
        case resource_kind::data: {
            const auto& db = _qp.db();

            data_resource_view v(r);
            const auto keyspace = v.keyspace();
            const auto table = v.table();

            if (table) {
                return make_ready_future<bool>(db.has_schema(sstring(*keyspace), sstring(*table)));
            }

            if (keyspace) {
                return make_ready_future<bool>(db.has_keyspace(sstring(*keyspace)));
            }

            return make_ready_future<bool>(true);
        }

        case resource_kind::role: {
            role_resource_view v(r);
            const auto role = v.role();

            if (role) {
                return _role_manager->exists(*role);
            }

            return make_ready_future<bool>(true);
        }
        case resource_kind::service_level:
            return make_ready_future<bool>(true);
    }

    return make_ready_future<bool>(false);
}

//
// Free functions.
//

future<bool> has_superuser(const service& ser, const authenticated_user& u) {
    if (is_anonymous(u)) {
        return make_ready_future<bool>(false);
    }

    return ser.has_superuser(*u.name);
}

future<role_set> get_roles(const service& ser, const authenticated_user& u) {
    if (is_anonymous(u)) {
        return make_ready_future<role_set>();
    }

    return ser.get_roles(*u.name);
}

future<permission_set> get_permissions(const service& ser, const authenticated_user& u, const resource& r) {
    return do_with(role_or_anonymous(), [&ser, &u, &r](auto& maybe_role) {
        maybe_role.name = u.name;
        return ser.get_permissions(maybe_role, r);
    });
}

bool is_enforcing(const service& ser)  {
    const bool enforcing_authorizer = ser.underlying_authorizer().qualified_java_name() != allow_all_authorizer_name;

    const bool enforcing_authenticator = ser.underlying_authenticator().qualified_java_name()
            != allow_all_authenticator_name;

    return enforcing_authorizer || enforcing_authenticator;
}

bool is_protected(const service& ser, command_desc cmd) noexcept {
    if (cmd.type_ == command_desc::type::ALTER_WITH_OPTS) {
        return false; // Table attributes are OK to modify; see #7057.
    }
    return ser.underlying_role_manager().protected_resources().contains(cmd.resource)
            || ser.underlying_authenticator().protected_resources().contains(cmd.resource)
            || ser.underlying_authorizer().protected_resources().contains(cmd.resource);
}

static void validate_authentication_options_are_supported(
        const authentication_options& options,
        const authentication_option_set& supported) {
    const auto check = [&supported](authentication_option k) {
        if (!supported.contains(k)) {
            throw unsupported_authentication_option(k);
        }
    };

    if (options.password) {
        check(authentication_option::password);
    }

    if (options.options) {
        check(authentication_option::options);
    }
}


future<> create_role(
        const service& ser,
        std::string_view name,
        const role_config& config,
        const authentication_options& options) {
    return ser.underlying_role_manager().create(name, config).then([&ser, name, &options] {
        if (!auth::any_authentication_options(options)) {
            return make_ready_future<>();
        }

        return futurize_invoke(
                &validate_authentication_options_are_supported,
                options,
                ser.underlying_authenticator().supported_options()).then([&ser, name, &options] {
            return ser.underlying_authenticator().create(name, options);
        }).handle_exception([&ser, &name](std::exception_ptr ep) {
            // Roll-back.
            return ser.underlying_role_manager().drop(name).then([ep = std::move(ep)] {
                std::rethrow_exception(ep);
            });
        });
    });
}

future<> alter_role(
        const service& ser,
        std::string_view name,
        const role_config_update& config_update,
        const authentication_options& options) {
    return ser.underlying_role_manager().alter(name, config_update).then([&ser, name, &options] {
        if (!any_authentication_options(options)) {
            return make_ready_future<>();
        }

        return futurize_invoke(
                &validate_authentication_options_are_supported,
                options,
                ser.underlying_authenticator().supported_options()).then([&ser, name, &options] {
            return ser.underlying_authenticator().alter(name, options);
        });
    });
}

future<> drop_role(const service& ser, std::string_view name) {
    return do_with(make_role_resource(name), [&ser, name](const resource& r) {
        auto& a = ser.underlying_authorizer();

        return when_all_succeed(
                a.revoke_all(name),
                a.revoke_all(r))
                    .discard_result()
                    .handle_exception_type([](const unsupported_authorization_operation&) {
            // Nothing.
        });
    }).then([&ser, name] {
        return ser.underlying_authenticator().drop(name);
    }).then([&ser, name] {
        return ser.underlying_role_manager().drop(name);
    });
}

future<bool> has_role(const service& ser, std::string_view grantee, std::string_view name) {
    return when_all_succeed(
            validate_role_exists(ser, name),
            ser.get_roles(grantee)).then_unpack([name](role_set all_roles) {
        return make_ready_future<bool>(all_roles.contains(sstring(name)));
    });
}
future<bool> has_role(const service& ser, const authenticated_user& u, std::string_view name) {
    if (is_anonymous(u)) {
        return make_ready_future<bool>(false);
    }

    return has_role(ser, *u.name, name);
}

future<> grant_permissions(
        const service& ser,
        std::string_view role_name,
        permission_set perms,
        const resource& r) {
    return validate_role_exists(ser, role_name).then([&ser, role_name, perms, &r] {
        return ser.underlying_authorizer().grant(role_name, perms, r);
    });
}

future<> grant_applicable_permissions(const service& ser, std::string_view role_name, const resource& r) {
    return grant_permissions(ser, role_name, r.applicable_permissions(), r);
}
future<> grant_applicable_permissions(const service& ser, const authenticated_user& u, const resource& r) {
    if (is_anonymous(u)) {
        return make_ready_future<>();
    }

    return grant_applicable_permissions(ser, *u.name, r);
}

future<> revoke_permissions(
        const service& ser,
        std::string_view role_name,
        permission_set perms,
        const resource& r) {
    return validate_role_exists(ser, role_name).then([&ser, role_name, perms, &r] {
        return ser.underlying_authorizer().revoke(role_name, perms, r);
    });
}

future<std::vector<permission_details>> list_filtered_permissions(
        const service& ser,
        permission_set perms,
        std::optional<std::string_view> role_name,
        const std::optional<std::pair<resource, recursive_permissions>>& resource_filter) {
    return ser.underlying_authorizer().list_all().then([&ser, perms, role_name, &resource_filter](
            std::vector<permission_details> all_details) {

        if (resource_filter) {
            const resource r = resource_filter->first;

            const auto resources = resource_filter->second
                    ? auth::expand_resource_family(r)
                    : auth::resource_set{r};

            std::erase_if(all_details, [&resources](const permission_details& pd) {
                return !resources.contains(pd.resource);
            });
        }

        std::transform(
                std::make_move_iterator(all_details.begin()),
                std::make_move_iterator(all_details.end()),
                all_details.begin(),
                [perms](permission_details pd) {
                    pd.permissions = permission_set::from_mask(pd.permissions.mask() & perms.mask());
                    return pd;
                });

        // Eliminate rows with an empty permission set.
        std::erase_if(all_details, [](const permission_details& pd) {
            return pd.permissions.mask() == 0;
        });

        if (!role_name) {
            return make_ready_future<std::vector<permission_details>>(std::move(all_details));
        }

        //
        // Filter out rows based on whether permissions have been granted to this role (directly or indirectly).
        //

        return do_with(std::move(all_details), [&ser, role_name](auto& all_details) {
            return ser.get_roles(*role_name).then([&all_details](role_set all_roles) {
                std::erase_if(all_details, [&all_roles](const permission_details& pd) {
                    return !all_roles.contains(pd.role_name);
                });

                return make_ready_future<std::vector<permission_details>>(std::move(all_details));
            });
        });
    });
}

}
