/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <exception>
#include <seastar/core/coroutine.hh>
#include "auth/authentication_options.hh"
#include "auth/authorizer.hh"
#include "auth/resource.hh"
#include "auth/service.hh"

#include <algorithm>
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
#include "cql3/description.hh"
#include "cql3/untyped_result_set.hh"
#include "cql3/util.hh"
#include "db/config.hh"
#include "db/consistency_level_type.hh"
#include "db/functions/function_name.hh"
#include "utils/log.hh"
#include "schema/schema_fwd.hh"
#include <seastar/core/future.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <variant>
#include "service/migration_manager.hh"
#include "service/raft/raft_group0_client.hh"
#include "timestamp.hh"
#include "utils/assert.hh"
#include "utils/class_registrator.hh"
#include "locator/abstract_replication_strategy.hh"
#include "data_dictionary/keyspace_metadata.hh"
#include "service/storage_service.hh"
#include "service_permit.hh"
#include "utils/managed_string.hh"

using namespace std::chrono_literals;

namespace auth {

namespace meta {

static const sstring user_name_col_name("name");
static const sstring superuser_col_name("super");

}

static logging::logger log("auth_service");

class auth_migration_listener final : public ::service::migration_listener {
    authorizer& _authorizer;
    cql3::query_processor& _qp;

public:
    explicit auth_migration_listener(authorizer& a, cql3::query_processor& qp) : _authorizer(a),  _qp(qp) {
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
        if (!legacy_mode(_qp)) {
            // in non legacy path revoke is part of schema change statement execution
            return;
        }
        // Do it in the background.
        (void)do_with(::service::group0_batch::unused(), [this, &ks_name] (auto& mc) mutable {
            return _authorizer.revoke_all(auth::make_data_resource(ks_name), mc);
        }).handle_exception([] (std::exception_ptr e) {
            log.error("Unexpected exception while revoking all permissions on dropped keyspace: {}", e);
        });

        (void)do_with(::service::group0_batch::unused(), [this, &ks_name] (auto& mc) mutable {
            return _authorizer.revoke_all(auth::make_functions_resource(ks_name), mc);
        }).handle_exception([] (std::exception_ptr e) {
            log.error("Unexpected exception while revoking all permissions on functions in dropped keyspace: {}", e);
        });
    }

    void on_drop_column_family(const sstring& ks_name, const sstring& cf_name) override {
        if (!legacy_mode(_qp)) {
            // in non legacy path revoke is part of schema change statement execution
            return;
        }
        // Do it in the background.
        (void)do_with(::service::group0_batch::unused(), [this, &ks_name, &cf_name] (auto& mc) mutable {
            return _authorizer.revoke_all(
                    auth::make_data_resource(ks_name, cf_name), mc);
        }).handle_exception([] (std::exception_ptr e) {
            log.error("Unexpected exception while revoking all permissions on dropped table: {}", e);
        });
    }

    void on_drop_user_type(const sstring& ks_name, const sstring& type_name) override {}
    void on_drop_function(const sstring& ks_name, const sstring& function_name) override {
        if (!legacy_mode(_qp)) {
            // in non legacy path revoke is part of schema change statement execution
            return;
        }
        // Do it in the background.
        (void)do_with(::service::group0_batch::unused(), [this, &ks_name, &function_name] (auto& mc) mutable {
            return _authorizer.revoke_all(
                    auth::make_functions_resource(ks_name, function_name), mc);
        }).handle_exception([] (std::exception_ptr e) {
            log.error("Unexpected exception while revoking all permissions on dropped function: {}", e);
        });
    }
    void on_drop_aggregate(const sstring& ks_name, const sstring& aggregate_name) override {
        if (!legacy_mode(_qp)) {
            // in non legacy path revoke is part of schema change statement execution
            return;
        }
        (void)do_with(::service::group0_batch::unused(), [this, &ks_name, &aggregate_name] (auto& mc) mutable {
            return _authorizer.revoke_all(
                    auth::make_functions_resource(ks_name, aggregate_name), mc);
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
        ::service::raft_group0_client& g0,
        ::service::migration_notifier& mn,
        std::unique_ptr<authorizer> z,
        std::unique_ptr<authenticator> a,
        std::unique_ptr<role_manager> r,
        maintenance_socket_enabled used_by_maintenance_socket)
            : _loading_cache_config(std::move(c))
            , _permissions_cache(nullptr)
            , _qp(qp)
            , _group0_client(g0)
            , _mnotifier(mn)
            , _authorizer(std::move(z))
            , _authenticator(std::move(a))
            , _role_manager(std::move(r))
            , _migration_listener(std::make_unique<auth_migration_listener>(*_authorizer, qp))
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
        maintenance_socket_enabled used_by_maintenance_socket,
        utils::alien_worker& hashing_worker)
            : service(
                      std::move(c),
                      qp,
                      g0,
                      mn,
                      create_object<authorizer>(sc.authorizer_java_name, qp, g0, mm),
                      create_object<authenticator>(sc.authenticator_java_name, qp, g0, mm, hashing_worker),
                      create_object<role_manager>(sc.role_manager_java_name, qp, g0, mm),
                      used_by_maintenance_socket) {
}

future<> service::create_legacy_keyspace_if_missing(::service::migration_manager& mm) const {
    SCYLLA_ASSERT(this_shard_id() == 0); // once_among_shards makes sure a function is executed on shard 0 only
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
                        std::move(group0_guard), seastar::format("auth_service: create {} keyspace", meta::legacy::AUTH_KS));
            } catch (::service::group0_concurrent_modification&) {
                log.info("Concurrent operation is detected while creating {} keyspace, retrying.", meta::legacy::AUTH_KS);
            }
        }
    }
}

future<> service::start(::service::migration_manager& mm, db::system_keyspace& sys_ks) {
    auto auth_version = co_await sys_ks.get_auth_version();
    // version is set in query processor to be easily available in various places we call auth::legacy_mode check.
    _qp.auth_version = auth_version;
    if (!_used_by_maintenance_socket) {
        // this legacy keyspace is only used by cqlsh
        // it's needed when executing `list roles` or `list users`
        // it doesn't affect anything except that cqlsh fails if keyspace
        // is not found
        co_await once_among_shards([this, &mm] {
            return create_legacy_keyspace_if_missing(mm);
        });
    }
    co_await _role_manager->start();
    if (this_shard_id() == 0) {
        // Role manager and password authenticator have this odd startup
        // mechanism where they asynchronously create the superuser role
        // in the background. Correct password creation depends on role
        // creation therefore we need to wait here.
        co_await _role_manager->ensure_superuser_is_created();
    }
    co_await when_all_succeed(_authorizer->start(), _authenticator->start()).discard_result();
    _permissions_cache = std::make_unique<permissions_cache>(_loading_cache_config, *this, log);
    co_await once_among_shards([this] {
        _mnotifier.register_listener(_migration_listener.get());
        return make_ready_future<>();
    });
}

future<> service::stop() {
    _as.request_abort();
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

future<> service::ensure_superuser_is_created() {
    co_await _role_manager->ensure_superuser_is_created();
    co_await _authenticator->ensure_superuser_is_created();
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

future<permission_set>
service::get_uncached_permissions(const role_or_anonymous& maybe_role, const resource& r) const {
    if (is_anonymous(maybe_role)) {
        co_return co_await _authorizer->authorize(maybe_role, r);
    }
    const std::string_view role_name = *maybe_role.name;
    auto all_roles = co_await get_roles(role_name);
    auto superuser = co_await has_superuser(role_name, all_roles);
    if (superuser) {
        co_return r.applicable_permissions();
    }
    // Aggregate the permissions from all granted roles.
    permission_set all_perms;
    co_await coroutine::parallel_for_each(all_roles, [this, &r, &all_perms](std::string_view role_name) -> future<> {
        auto perms = co_await _authorizer->authorize(role_name, r);
        all_perms = permission_set::from_mask(all_perms.mask() | perms.mask());
    });
    co_return std::move(all_perms);
}

future<permission_set> service::get_permissions(const role_or_anonymous& maybe_role, const resource& r) const {
    return _permissions_cache->get(maybe_role, r);
}

future<bool> service::has_superuser(std::string_view role_name, const role_set& roles) const {
    for (const auto& role : roles) {
        if (co_await _role_manager->is_superuser(role)) {
            co_return true;
        }
    }
    co_return false;
}

future<bool> service::has_superuser(std::string_view role_name) const {
    auto roles = co_await get_roles(role_name);
    co_return co_await has_superuser(role_name, roles);
}

static void validate_authentication_options_are_supported(
        const authentication_options& options,
        const authentication_option_set& supported) {
    const auto check = [&supported](authentication_option k) {
        if (!supported.contains(k)) {
            throw unsupported_authentication_option(k);
        }
    };

    if (options.credentials) {
        std::visit(make_visitor(
            [&] (const password_option&) { check(authentication_option::password); },
            [&] (const hashed_password_option&) { check(authentication_option::hashed_password); }
        ), *options.credentials);
    }

    if (options.options) {
        check(authentication_option::options);
    }
}

future<> service::create_role(std::string_view name,
        const role_config& config,
        const authentication_options& options,
        ::service::group0_batch& mc) const {
    co_await underlying_role_manager().create(name, config, mc);
    if (!auth::any_authentication_options(options)) {
        co_return;
    }
    std::exception_ptr ep;
    try {
        validate_authentication_options_are_supported(options,
                underlying_authenticator().supported_options());
        co_await underlying_authenticator().create(name, options, mc);
    } catch (...) {
        ep = std::current_exception();
    }
    if (ep) {
        // Rollback only in legacy mode as normally mutations won't be
        // applied in case exception is raised
        if (legacy_mode(_qp)) {
            co_await underlying_role_manager().drop(name, mc);
        }
        std::rethrow_exception(std::move(ep));
    }
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
            return make_ready_future<bool>(cql3::functions::instance().find(db::functions::function_name{sstring(*keyspace), name}, function_args));
        }
    }

    return make_ready_future<bool>(false);
}

future<std::vector<cql3::description>> service::describe_roles(bool with_hashed_passwords) {
    std::vector<cql3::description> result{};

    const auto roles = co_await _role_manager->query_all();
    result.reserve(roles.size());

    const bool authenticator_uses_password_hashes = _authenticator->uses_password_hashes();

    auto produce_create_statement = [with_hashed_passwords] (const sstring& formatted_role_name,
            const std::optional<sstring>& maybe_hashed_password, bool can_login, bool is_superuser) {
        // Even after applying formatting to a role, `formatted_role_name` can only equal `meta::DEFAULT_SUPER_NAME`
        // if the original identifier was equal to it.
        const sstring role_part = formatted_role_name == meta::DEFAULT_SUPERUSER_NAME
                ? seastar::format("IF NOT EXISTS {}", formatted_role_name)
                : formatted_role_name;

        const sstring with_hashed_password_part = with_hashed_passwords && maybe_hashed_password
                // `K_PASSWORD` in Scylla's CQL grammar requires that passwords be quoted
                // with single quotation marks.
                ? seastar::format("WITH HASHED PASSWORD = {} AND", cql3::util::single_quote(*maybe_hashed_password))
                : "WITH";

        return seastar::format("CREATE ROLE {} {} LOGIN = {} AND SUPERUSER = {};",
                role_part, with_hashed_password_part, can_login, is_superuser);
    };

    for (const auto& role : roles) {
        const sstring formatted_role_name = cql3::util::maybe_quote(role);

        std::optional<sstring> maybe_hashed_password;
        if (authenticator_uses_password_hashes) {
            maybe_hashed_password = co_await _authenticator->get_password_hash(role);
        }

        const bool can_login = co_await _role_manager->can_login(role);
        const bool is_superuser = co_await _role_manager->is_superuser(role);

        sstring create_statement = produce_create_statement(formatted_role_name, maybe_hashed_password, can_login, is_superuser);

        result.push_back(cql3::description {
            // Roles do not belong to any keyspace.
            .keyspace = std::nullopt,
            .type = "role",
            .name = role,
            .create_statement = managed_string(create_statement)
        });
    }

    std::ranges::sort(result, std::less<>{}, std::mem_fn(&cql3::description::name));

    co_return result;
}

// The function doesn't assume anything about `role`.
static sstring describe_data_resource(const permission& perm, const resource& r, std::string_view role) {
    const auto permission = permissions::to_string(perm);
    const auto formatted_role = cql3::util::maybe_quote(role);

    const auto view = data_resource_view(r);
    const auto maybe_ks = view.keyspace();
    const auto maybe_cf = view.table();

    // The documentation says:
    //
    //     Both keyspace and table names consist of only alphanumeric characters, cannot be empty,
    //     and are limited in size to 48 characters (that limit exists mostly to avoid filenames,
    //     which may include the keyspace and table name, to go over the limits of certain file systems).
    //     By default, keyspace and table names are case insensitive (myTable is equivalent to mytable),
    //     but case sensitivity can be forced by using double-quotes ("myTable" is different from mytable).
    //
    // That's why we wrap identifiers with quotation marks below.

    if (!maybe_ks) {
        return seastar::format("GRANT {} ON ALL KEYSPACES TO {};", permission, formatted_role);
    }
    const auto ks = cql3::util::maybe_quote(*maybe_ks);

    if (!maybe_cf) {
        return seastar::format("GRANT {} ON KEYSPACE {} TO {};", permission, ks, formatted_role);
    }
    const auto cf = cql3::util::maybe_quote(*maybe_cf);

    return seastar::format("GRANT {} ON {}.{} TO {};", permission, ks, cf, formatted_role);
}

// The function doesn't assume anything about `role`.
static sstring describe_role_resource(const permission& perm, const resource& r, std::string_view role) {
    const auto permission = permissions::to_string(perm);
    const auto formatted_role = cql3::util::maybe_quote(role);

    const auto view = role_resource_view(r);
    const auto maybe_target_role = view.role();

    if (!maybe_target_role) {
        return seastar::format("GRANT {} ON ALL ROLES TO {};", permission, formatted_role);
    }
    return seastar::format("GRANT {} ON ROLE {} TO {};", permission, cql3::util::maybe_quote(*maybe_target_role), formatted_role);
}

// The function doesn't assume anything about `role`.
static sstring describe_udf_resource(const permission& perm, const resource& r, std::string_view role) {
    const auto permission = permissions::to_string(perm);
    const auto formatted_role = cql3::util::maybe_quote(role);

    const auto view = functions_resource_view(r);
    const auto maybe_ks = view.keyspace();
    const auto maybe_fun_sig = view.function_signature();
    const auto maybe_fun_name = view.function_name();
    const auto maybe_fun_args = view.function_args();

    // The documentation says:
    //
    //     Both keyspace and table names consist of only alphanumeric characters, cannot be empty,
    //     and are limited in size to 48 characters (that limit exists mostly to avoid filenames,
    //     which may include the keyspace and table name, to go over the limits of certain file systems).
    //     By default, keyspace and table names are case insensitive (myTable is equivalent to mytable),
    //     but case sensitivity can be forced by using double-quotes ("myTable" is different from mytable).
    //
    // That's why we wrap identifiers with quotation marks below.

    if (!maybe_ks) {
        return seastar::format("GRANT {} ON ALL FUNCTIONS TO {};", permission, formatted_role);
    }
    const auto ks = cql3::util::maybe_quote(*maybe_ks);

    if (!maybe_fun_sig && !maybe_fun_name) {
        return seastar::format("GRANT {} ON ALL FUNCTIONS IN KEYSPACE {} TO {};", permission, ks, formatted_role);
    }

    if (maybe_fun_name) {
        SCYLLA_ASSERT(maybe_fun_args);

        const auto fun_name = cql3::util::maybe_quote(*maybe_fun_name);
        const auto fun_args_range = *maybe_fun_args | std::views::transform([] (const auto& fun_arg) {
            return cql3::util::maybe_quote(fun_arg);
        });

        return seastar::format("GRANT {} ON FUNCTION {}.{}({}) TO {};",
                permission, ks, fun_name, fmt::join(fun_args_range, ", "), formatted_role);
    }

    SCYLLA_ASSERT(maybe_fun_sig);

    auto [fun_name, fun_args] = decode_signature(*maybe_fun_sig);
    fun_name = cql3::util::maybe_quote(fun_name);

    // We don't call `cql3::util::maybe_quote` later because `cql3_type_name_without_frozen` already guarantees
    // that the type will be wrapped within double quotation marks if it's necessary.
    auto parsed_fun_args = fun_args | std::views::transform([] (const data_type& dt) {
        return dt->without_reversed().cql3_type_name_without_frozen();
    });

    return seastar::format("GRANT {} ON FUNCTION {}.{}({}) TO {};",
            permission, ks, fun_name, fmt::join(parsed_fun_args, ", "), formatted_role);
}

// The function doesn't assume anything about `role`.
static sstring describe_resource_kind(const permission& perm, const resource& r, std::string_view role) {
    switch (r.kind()) {
        case resource_kind::data:
            return describe_data_resource(perm, r, role);
        case resource_kind::role:
            return describe_role_resource(perm, r, role);
        case resource_kind::service_level:
            on_internal_error(log, "Granting permissions for service levels is not supported");
        case resource_kind::functions:
            return describe_udf_resource(perm, r, role);
    }
}

future<std::vector<cql3::description>> service::describe_permissions() const {
    std::vector<cql3::description> result{};

    const auto permission_list = co_await std::invoke([&] -> future<std::vector<permission_details>> {
        try {
            co_return co_await _authorizer->list_all();
        } catch (const unsupported_authorization_operation&) {
            // If Scylla uses AllowAllAuthorizer, permissions do not exist and the corresponding authorizer
            // will throw an exception when trying to access them.
            co_return std::vector<permission_details>{};
        }
    });

    for (const auto& permissions : permission_list) {
        for (const auto& permission : permissions.permissions) {
            sstring create_statement = describe_resource_kind(permission, permissions.resource, permissions.role_name);

            result.push_back(cql3::description {
                // Permission grants do not belong to any keyspace.
                .keyspace = std::nullopt,
                .type = "grant_permission",
                .name = permissions.role_name,
                .create_statement = managed_string(create_statement)
            });
        }

        co_await coroutine::maybe_yield();
    }

    std::ranges::sort(result, std::less<>{}, [] (const cql3::description& desc) {
        return std::make_tuple(std::ref(desc.name), std::ref(*desc.create_statement));
    });

    co_return result;
}

future<std::vector<cql3::description>> service::describe_auth(bool with_hashed_passwords) {
    auto role_descs = co_await describe_roles(with_hashed_passwords);
    auto role_grant_descs = co_await _role_manager->describe_role_grants();
    auto permission_descs = co_await describe_permissions();

    auto join_vectors = [] (std::vector<cql3::description>& v1, std::vector<cql3::description>&& v2) {
        v1.insert(v1.end(), std::make_move_iterator(v2.begin()), std::make_move_iterator(v2.end()));
    };

    join_vectors(role_descs, std::move(role_grant_descs));
    join_vectors(role_descs, std::move(permission_descs));

    co_return role_descs;
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

bool is_protected(const service& ser, command_desc cmd) noexcept {
    if (cmd.type_ == command_desc::type::ALTER_WITH_OPTS ||
        cmd.type_ == command_desc::type::ALTER_SYSTEM_WITH_ALLOWED_OPTS) {
        return false; // Table attributes are OK to modify; see #7057.
    }
    return ser.underlying_role_manager().protected_resources().contains(cmd.resource)
            || ser.underlying_authenticator().protected_resources().contains(cmd.resource)
            || ser.underlying_authorizer().protected_resources().contains(cmd.resource);
}

future<> create_role(
        const service& ser,
        std::string_view name,
        const role_config& config,
        const authentication_options& options,
        ::service::group0_batch& mc) {
    return ser.create_role(name, config, options, mc);
}

future<> alter_role(
        const service& ser,
        std::string_view name,
        const role_config_update& config_update,
        const authentication_options& options,
        ::service::group0_batch& mc) {
    co_await ser.underlying_role_manager().alter(name, config_update, mc);
    if (!any_authentication_options(options)) {
        co_return;
    }
    validate_authentication_options_are_supported(options,
            ser.underlying_authenticator().supported_options());
    co_await ser.underlying_authenticator().alter(name, options, mc);
}

future<> drop_role(const service& ser, std::string_view name, ::service::group0_batch& mc) {
    auto& a = ser.underlying_authorizer();
    auto r = make_role_resource(name);
    co_await a.revoke_all(name, mc);
    co_await a.revoke_all(r, mc);
    co_await ser.underlying_authenticator().drop(name, mc);
    co_await ser.underlying_role_manager().drop(name, mc);
}

future<> grant_role(const service& ser, std::string_view grantee_name, std::string_view role_name, ::service::group0_batch& mc) {
    return ser.underlying_role_manager().grant(grantee_name, role_name, mc);
}

future<> revoke_role(const service& ser, std::string_view revokee_name, std::string_view role_name, ::service::group0_batch& mc) {
    return ser.underlying_role_manager().revoke(revokee_name, role_name, mc);
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

future<> set_attribute(const service& ser, std::string_view role_name, std::string_view attribute_name, std::string_view attribute_value, ::service::group0_batch& mc) {
    return ser.underlying_role_manager().set_attribute(role_name, attribute_name, attribute_value, mc);
}

future<> remove_attribute(const service& ser, std::string_view role_name, std::string_view attribute_name, ::service::group0_batch& mc) {
    return ser.underlying_role_manager().remove_attribute(role_name, attribute_name, mc);
}

future<> grant_permissions(
        const service& ser,
        std::string_view role_name,
        permission_set perms,
        const resource& r,
        ::service::group0_batch& mc) {
    co_await validate_role_exists(ser, role_name);
    co_await ser.underlying_authorizer().grant(role_name, perms, r, mc);
}

future<> grant_applicable_permissions(const service& ser, std::string_view role_name, const resource& r, ::service::group0_batch& mc) {
    return grant_permissions(ser, role_name, r.applicable_permissions(), r, mc);
}

future<> grant_applicable_permissions(const service& ser, const authenticated_user& u, const resource& r, ::service::group0_batch& mc) {
    if (is_anonymous(u)) {
        return make_ready_future<>();
    }
    return grant_applicable_permissions(ser, *u.name, r, mc);
}

future<> revoke_permissions(
        const service& ser,
        std::string_view role_name,
        permission_set perms,
        const resource& r,
        ::service::group0_batch& mc) {
    co_await validate_role_exists(ser, role_name);
    co_await ser.underlying_authorizer().revoke(role_name, perms, r, mc);
}

future<> revoke_all(const service& ser, const resource& r, ::service::group0_batch& mc) {
    return ser.underlying_authorizer().revoke_all(r, mc);
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

future<> commit_mutations(service& ser, ::service::group0_batch&& mc) {
    return ser.commit_mutations(std::move(mc));
}

future<> migrate_to_auth_v2(db::system_keyspace& sys_ks, ::service::raft_group0_client& g0, start_operation_func_t start_operation_func, abort_source& as) {
    // FIXME: if this function fails it may leave partial data in the new tables
    // that should be cleared
    auto gen = [&sys_ks] (api::timestamp_type ts) -> ::service::mutations_generator {
        auto& qp = sys_ks.query_processor();
        for (const auto& cf_name : std::vector<sstring>{
                "roles", "role_members", "role_attributes", "role_permissions"}) {
            schema_ptr schema;
            try {
                schema = qp.db().find_schema(meta::legacy::AUTH_KS, cf_name);
            } catch (const data_dictionary::no_such_column_family&) {
                continue; // some tables might not have been created if they were not used
            }

            std::vector<sstring> col_names;
            for (const auto& col : schema->all_columns()) {
                col_names.push_back(col.name_as_cql_string());
            }
            sstring val_binders_str = "?";
            for (size_t i = 1; i < col_names.size(); ++i) {
                val_binders_str += ", ?";
            }

            std::vector<mutation> collected;
            co_await qp.query_internal(
                seastar::format("SELECT * FROM {}.{}", meta::legacy::AUTH_KS, cf_name),
                db::consistency_level::ALL,
                {},
                1000,
                [&qp, &cf_name, &col_names, &val_binders_str, &schema, ts, &collected] (const cql3::untyped_result_set::row& row) -> future<stop_iteration> {
                    std::vector<data_value_or_unset> values;
                    for (const auto& col : schema->all_columns()) {
                        if (row.has(col.name_as_text())) {
                            values.push_back(
                                    col.type->deserialize(row.get_blob_unfragmented(col.name_as_text())));
                        } else {
                            values.push_back(unset_value{});
                        }
                    }
                    auto muts = co_await qp.get_mutations_internal(
                            seastar::format("INSERT INTO {}.{} ({}) VALUES ({})",
                                    db::system_keyspace::NAME,
                                    cf_name,
                                    fmt::join(col_names, ", "),
                                    val_binders_str),
                            internal_distributed_query_state(),
                            ts,
                            std::move(values));
                    if (muts.size() != 1) {
                        on_internal_error(log,
                                format("expecting single insert mutation, got {}", muts.size()));
                    }

                    collected.push_back(std::move(muts[0]));
                    co_return stop_iteration::no;
                });

            for (auto& m : collected) {
                co_yield std::move(m);
            }
        }
        co_yield co_await sys_ks.make_auth_version_mutation(ts,
                db::system_keyspace::auth_version_t::v2);
    };
    co_await announce_mutations_with_batching(g0,
            start_operation_func,
            std::move(gen),
            as,
            std::nullopt);
}

}
