/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/core/coroutine.hh>
#include "auth/resource.hh"
#include "auth/service.hh"

#include <algorithm>
#include <boost/algorithm/string/join.hpp>
#include <chrono>

#include <seastar/core/future-util.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>

#include "auth/allow_all_authenticator.hh"
#include "auth/allow_all_authorizer.hh"
#include "auth/common.hh"
#include "auth/role_or_anonymous.hh"
#include "cql3/functions/functions.hh"
#include "cql3/query_processor.hh"
#include "cql3/untyped_result_set.hh"
#include "db/config.hh"
#include "db/consistency_level_type.hh"
#include "db/functions/function_name.hh"
#include "db/system_auth_keyspace.hh"
#include "log.hh"
#include "schema/schema_fwd.hh"
#include "seastar/core/future.hh"
#include "service/migration_manager.hh"
#include "timestamp.hh"
#include "utils/class_registrator.hh"
#include "locator/abstract_replication_strategy.hh"
#include "data_dictionary/keyspace_metadata.hh"
#include "service/storage_service.hh"
#include "service_permit.hh"

using namespace std::chrono_literals;

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
    void on_update_tablet_metadata() override {}

    void on_drop_keyspace(const sstring& ks_name) override {
        // Do it in the background.
        (void)_authorizer.revoke_all(
                auth::make_data_resource(ks_name)).handle_exception_type([](const unsupported_authorization_operation&) {
            // Nothing.
        }).handle_exception([] (std::exception_ptr e) {
            log.error("Unexpected exception while revoking all permissions on dropped keyspace: {}", e);
        });
        (void)_authorizer.revoke_all(
            auth::make_functions_resource(ks_name)).handle_exception_type([](const unsupported_authorization_operation&) {
            // Nothing.
        }).handle_exception([] (std::exception_ptr e) {
            log.error("Unexpected exception while revoking all permissions on functions in dropped keyspace: {}", e);
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
    void on_drop_function(const sstring& ks_name, const sstring& function_name) override {
        (void)_authorizer.revoke_all(
            auth::make_functions_resource(ks_name, function_name)).handle_exception_type([](const unsupported_authorization_operation&) {
            // Nothing.
        }).handle_exception([] (std::exception_ptr e) {
            log.error("Unexpected exception while revoking all permissions on dropped function: {}", e);
        });
    }
    void on_drop_aggregate(const sstring& ks_name, const sstring& aggregate_name) override {
        (void)_authorizer.revoke_all(
            auth::make_functions_resource(ks_name, aggregate_name)).handle_exception_type([](const unsupported_authorization_operation&) {
            // Nothing.
        }).handle_exception([] (std::exception_ptr e) {
            log.error("Unexpected exception while revoking all permissions on dropped aggregate: {}", e);
        });
    }
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
        utils::loading_cache_config c,
        cql3::query_processor& qp,
        ::service::migration_notifier& mn,
        std::unique_ptr<authorizer> z,
        std::unique_ptr<authenticator> a,
        std::unique_ptr<role_manager> r,
        maintenance_socket_enabled used_by_maintenance_socket)
            : _loading_cache_config(std::move(c))
            , _permissions_cache(nullptr)
            , _qp(qp)
            , _mnotifier(mn)
            , _authorizer(std::move(z))
            , _authenticator(std::move(a))
            , _role_manager(std::move(r))
            , _migration_listener(std::make_unique<auth_migration_listener>(*_authorizer))
            , _permissions_cache_cfg_cb([this] (uint32_t) { (void) _permissions_cache_config_action.trigger_later(); })
            , _permissions_cache_config_action([this] { update_cache_config(); return make_ready_future<>(); })
            , _permissions_cache_max_entries_observer(_qp.db().get_config().permissions_cache_max_entries.observe(_permissions_cache_cfg_cb))
            , _permissions_cache_update_interval_in_ms_observer(_qp.db().get_config().permissions_update_interval_in_ms.observe(_permissions_cache_cfg_cb))
            , _permissions_cache_validity_in_ms_observer(_qp.db().get_config().permissions_validity_in_ms.observe(_permissions_cache_cfg_cb))
            , _used_by_maintenance_socket(used_by_maintenance_socket) {}

service::service(
        utils::loading_cache_config c,
        cql3::query_processor& qp,
        ::service::raft_group0_client& g0,
        ::service::migration_notifier& mn,
        ::service::migration_manager& mm,
        const service_config& sc,
        maintenance_socket_enabled used_by_maintenance_socket)
            : service(
                      std::move(c),
                      qp,
                      mn,
                      create_object<authorizer>(sc.authorizer_java_name, qp, g0, mm),
                      create_object<authenticator>(sc.authenticator_java_name, qp, g0, mm),
                      create_object<role_manager>(sc.role_manager_java_name, qp, g0, mm),
                      used_by_maintenance_socket) {
}

future<> service::create_keyspace_if_missing(::service::migration_manager& mm) const {
    assert(this_shard_id() == 0); // once_among_shards makes sure a function is executed on shard 0 only
    auto db = _qp.db();

    while (!db.has_keyspace(meta::legacy::AUTH_KS)) {
        auto group0_guard = co_await mm.start_group0_operation();
        auto ts = group0_guard.write_timestamp();

        if (!db.has_keyspace(meta::legacy::AUTH_KS)) {
            locator::replication_strategy_config_options opts{{"replication_factor", "1"}};

            auto ksm = data_dictionary::keyspace_metadata::new_keyspace(
                    meta::legacy::AUTH_KS,
                    "org.apache.cassandra.locator.SimpleStrategy",
                    opts,
                    std::nullopt);

            try {
                co_return co_await mm.announce(::service::prepare_new_keyspace_announcement(db.real_database(), ksm, ts),
                        std::move(group0_guard), format("auth_service: create {} keyspace", meta::legacy::AUTH_KS));
            } catch (::service::group0_concurrent_modification&) {
                log.info("Concurrent operation is detected while creating {} keyspace, retrying.", meta::legacy::AUTH_KS);
            }
        }
    }
}

future<> service::start(::service::migration_manager& mm) {
    auto create_keyspace_if_missing_or_noop = _used_by_maintenance_socket
        ? make_ready_future<>()
        : once_among_shards([this, &mm] {
            return create_keyspace_if_missing(mm);
        });

    return create_keyspace_if_missing_or_noop.then([this] {
        return _role_manager->start().then([this] {
            return when_all_succeed(_authorizer->start(), _authenticator->start()).discard_result();
        });
    }).then([this] {
        _permissions_cache = std::make_unique<permissions_cache>(_loading_cache_config, *this, log);
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

void service::update_cache_config() {
    auto db = _qp.db();

    utils::loading_cache_config perm_cache_config;
    perm_cache_config.max_size = db.get_config().permissions_cache_max_entries();
    perm_cache_config.expiry = std::chrono::milliseconds(db.get_config().permissions_validity_in_ms());
    perm_cache_config.refresh = std::chrono::milliseconds(db.get_config().permissions_update_interval_in_ms());

    if (!_permissions_cache->update_config(std::move(perm_cache_config))) {
        log.error("Failed to apply permissions cache changes. Please read the documentation of these parameters");
    }
}

void service::reset_authorization_cache() {
    _permissions_cache->reset();
    _qp.reset_cache();
}

future<bool> service::has_existing_legacy_users() const {
    if (!_qp.db().has_schema(meta::legacy::AUTH_KS, meta::legacy::USERS_CF)) {
        return make_ready_future<bool>(false);
    }

    static const sstring default_user_query = format("SELECT * FROM {}.{} WHERE {} = ?",
            meta::legacy::AUTH_KS,
            meta::legacy::USERS_CF,
            meta::user_name_col_name);

    static const sstring all_users_query = format("SELECT * FROM {}.{} LIMIT 1",
            meta::legacy::AUTH_KS,
            meta::legacy::USERS_CF);

    // This logic is borrowed directly from Apache Cassandra. By first checking for the presence of the default user, we
    // can potentially avoid doing a range query with a high consistency level.

    return _qp.execute_internal(
            default_user_query,
            db::consistency_level::ONE,
            {meta::DEFAULT_SUPERUSER_NAME},
            cql3::query_processor::cache_internal::yes).then([this](auto results) {
        if (!results->empty()) {
            return make_ready_future<bool>(true);
        }

        return _qp.execute_internal(
                default_user_query,
                db::consistency_level::QUORUM,
                {meta::DEFAULT_SUPERUSER_NAME},
                cql3::query_processor::cache_internal::yes).then([this](auto results) {
            if (!results->empty()) {
                return make_ready_future<bool>(true);
            }

            return _qp.execute_internal(
                    all_users_query,
                    db::consistency_level::QUORUM,
                    cql3::query_processor::cache_internal::no).then([](auto results) {
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

        case resource_kind::functions: {
            const auto& db = _qp.db();

            functions_resource_view v(r);
            const auto keyspace = v.keyspace();
            if (!keyspace) {
                return make_ready_future<bool>(true);
            }
            const auto function_signature = v.function_signature();
            if (!function_signature) {
                return make_ready_future<bool>(db.has_keyspace(sstring(*keyspace)));
            }
            auto [name, function_args] = auth::decode_signature(*function_signature);
            return make_ready_future<bool>(cql3::functions::functions::find(db::functions::function_name{sstring(*keyspace), name}, function_args));
        }
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
        }).handle_exception([&ser, name](std::exception_ptr ep) {
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

future<> migrate_to_auth_v2(cql3::query_processor& qp, ::service::raft_group0_client& g0, start_operation_func_t start_operation_func, abort_source& as) {
    // FIXME: if this function fails it may leave partial data in the new tables
    // that should be cleared
    auto gen = [&qp] (api::timestamp_type& ts) -> mutations_generator {
        for (const auto& cf_name : std::vector<sstring>{
                "roles", "role_members", "role_attributes", "role_permissions"}) {
            schema_ptr schema;
            try {
                schema = qp.db().find_schema(meta::legacy::AUTH_KS, cf_name);
            } catch (const data_dictionary::no_such_column_family&) {
                continue; // some tables might not have been created if they were not used
            }

            // use longer than usual timeout as we scan the whole table
            // but not infinite or very long as we want to fail reasonably fast
            const auto t = 5min;
            const timeout_config tc{t, t, t, t, t, t, t};
            ::service::client_state cs(::service::client_state::internal_tag{}, tc);
            ::service::query_state qs(cs, empty_service_permit());

            auto rows = co_await qp.execute_internal(
                    format("SELECT * FROM {}.{}", meta::legacy::AUTH_KS, cf_name),
                    db::consistency_level::ALL,
                    qs,
                    {},
                    cql3::query_processor::cache_internal::no);
            if (rows->empty()) {
                continue;
            }
            std::vector<sstring> col_names;
            for (const auto& col : schema->all_columns()) {
                col_names.push_back(col.name_as_cql_string());
            }
            auto col_names_str = boost::algorithm::join(col_names, ", ");
            sstring val_binders_str = "?";
            for (size_t i = 1; i < col_names.size(); ++i) {
                val_binders_str += ", ?";
            }
            for (const auto& row : *rows) {
                std::vector<data_value_or_unset> values;
                for (const auto& col : schema->all_columns()) {
                    if (row.has(col.name_as_text())) {
                        values.push_back(
                                col.type->deserialize(row.get_blob(col.name_as_text())));
                    } else {
                        values.push_back(unset_value{});
                    }
                }
                auto muts = co_await qp.get_mutations_internal(
                        format("INSERT INTO {}.{} ({}) VALUES ({})",
                                db::system_auth_keyspace::NAME,
                                cf_name,
                                col_names_str,
                                val_binders_str),
                        internal_distributed_query_state(),
                        ts,
                        std::move(values));
                if (muts.size() != 1) {
                    on_internal_error(log,
                            format("expecting single insert mutation, got {}", muts.size()));
                }
                co_yield std::move(muts[0]);
            }
        }
    };
    co_await announce_mutations_with_batching(g0,
            start_operation_func,
            std::move(gen),
            &as,
            std::nullopt);
}

}
