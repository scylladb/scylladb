/*
 * Copyright (C) 2016-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "client_state.hh"
#include "auth/authenticator.hh"
#include "auth/common.hh"
#include "auth/resource.hh"
#include "exceptions/exceptions.hh"
#include "db/system_keyspace.hh"
#include "db/schema_tables.hh"
#include "tracing/trace_keyspace_helper.hh"
#include "db/system_distributed_keyspace.hh"
#include "replica/database.hh"
#include "utils/overloaded_functor.hh"
#include <seastar/core/coroutine.hh>

thread_local api::timestamp_type service::client_state::_last_timestamp_micros = 0;

void service::client_state::set_login(auth::authenticated_user user) {
    _user = std::move(user);
}

future<> service::client_state::check_user_can_login() {
    if (auth::is_anonymous(*_user)) {
        co_return;
    }

    auto& role_manager = _auth_service->underlying_role_manager();

    const bool exists = co_await role_manager.exists(*_user->name);

    if (!exists) {
        throw exceptions::authentication_exception(
                format("User {} doesn't exist - create it with CREATE USER query first", *_user->name));
    }

    bool can_login = co_await role_manager.can_login(*_user->name);
    if (!can_login) {
        throw exceptions::authentication_exception(format("{} is not permitted to log in", *_user->name));
    }
}

void service::client_state::validate_login() const {
    if (!_user) {
        throw exceptions::unauthorized_exception("You have not logged in");
    }
}

void service::client_state::ensure_not_anonymous() const {
    validate_login();
    if (auth::is_anonymous(*_user)) {
        throw exceptions::unauthorized_exception("You have to be logged in and not anonymous to perform this request");
    }
}

future<> service::client_state::has_all_keyspaces_access(
                auth::permission p) const {
    if (_is_internal) {
        co_return;
    }
    validate_login();

    auth::resource r{auth::resource_kind::data};
    co_return co_await ensure_has_permission({p, r});
}

future<> service::client_state::has_keyspace_access(const sstring& ks, auth::permission p) const {
    auth::resource r = auth::make_data_resource(ks);
    co_return co_await has_access(ks, {p, r});
}

future<> service::client_state::has_functions_access(auth::permission p) const {
    auth::resource r = auth::make_functions_resource();
    co_return co_await ensure_has_permission({p, r});
}

future<> service::client_state::has_functions_access(const sstring& ks, auth::permission p) const {
    auth::resource r = auth::make_functions_resource(ks);
    co_return co_await has_access(ks, {p, r});
}

future<> service::client_state::has_function_access(const sstring& ks, const sstring& function_signature, auth::permission p) const {
    auth::resource r = auth::make_functions_resource(ks, function_signature);
    co_return co_await has_access(ks, {p, r});
}

future<> service::client_state::has_column_family_access(const sstring& ks,
                const sstring& cf, auth::permission p, auth::command_desc::type t) const {
    auto r = auth::make_data_resource(ks, cf);
    co_return co_await has_access(ks, {p, r, t});
}

future<> service::client_state::has_schema_access(const schema& s, auth::permission p) const {
    auth::resource r = auth::make_data_resource(s.ks_name(), s.cf_name());
    co_return co_await has_access(s.ks_name(), {p, r});
}

future<> service::client_state::has_schema_access(const sstring& ks_name, const sstring& cf_name, auth::permission p) const {
    auth::resource r = auth::make_data_resource(ks_name, cf_name);
    co_return co_await has_access(ks_name, {p, r});
}

future<> service::client_state::has_access(const sstring& ks, auth::command_desc cmd) const {
    if (ks.empty()) {
        return make_exception_future<>(exceptions::invalid_request_exception("You have not set a keyspace for this session"));
    }
    if (_is_internal) {
        return make_ready_future();
    }

    validate_login();

    static const auto alteration_permissions = auth::permission_set::of<
            auth::permission::CREATE, auth::permission::ALTER, auth::permission::DROP>();

    // we only care about schema modification.
    if (alteration_permissions.contains(cmd.permission)) {
        // prevent system keyspace modification
        auto name = ks;
        std::transform(name.begin(), name.end(), name.begin(), ::tolower);
        if (is_system_keyspace(name)) {
            return make_exception_future<>(exceptions::unauthorized_exception(ks + " keyspace is not user-modifiable."));
        }

        //
        // we want to disallow dropping any contents of TRACING_KS and disallow dropping the `auth::meta::legacy::AUTH_KS`
        // keyspace.
        //

        const bool dropping_anything_in_tracing = (name == tracing::trace_keyspace_helper::KEYSPACE_NAME)
                && (cmd.permission == auth::permission::DROP);

        const bool dropping_auth_keyspace = (cmd.resource == auth::make_data_resource(auth::meta::legacy::AUTH_KS))
                && (cmd.permission == auth::permission::DROP);

        if (dropping_anything_in_tracing || dropping_auth_keyspace) {
            return make_exception_future<>(exceptions::unauthorized_exception(
                    format("Cannot {} {}", auth::permissions::to_string(cmd.permission), cmd.resource)));
        }
    }

    static thread_local std::unordered_set<auth::resource> readable_system_resources = [] {
        std::unordered_set<auth::resource> tmp;
        for (auto cf : { db::system_keyspace::LOCAL, db::system_keyspace::PEERS }) {
            tmp.insert(auth::make_data_resource(db::system_keyspace::NAME, cf));
        }
        for (const auto& cf : db::schema_tables::all_table_names(db::schema_features::full())) {
            tmp.insert(auth::make_data_resource(db::schema_tables::NAME, cf));
        }
        return tmp;
    }();

    if (cmd.permission == auth::permission::SELECT && readable_system_resources.contains(cmd.resource)) {
        return make_ready_future();
    }
    if (alteration_permissions.contains(cmd.permission)) {
        if (auth::is_protected(*_auth_service, cmd)) {
            return make_exception_future<>(exceptions::unauthorized_exception(format("{} is protected", cmd.resource)));
        }
    }

    if (cmd.resource.kind() == auth::resource_kind::data) {
        const auto resource_view = auth::data_resource_view(cmd.resource);
        if (resource_view.table()) {
            static constexpr auto cdc_topology_description_forbidden_permissions = auth::permission_set::of<
                    auth::permission::ALTER, auth::permission::DROP>();

            if (cdc_topology_description_forbidden_permissions.contains(cmd.permission)) {
                if ((ks == db::system_distributed_keyspace::NAME || ks == db::system_distributed_keyspace::NAME_EVERYWHERE)
                        && (resource_view.table() == db::system_distributed_keyspace::CDC_DESC_V2
                        || resource_view.table() == db::system_distributed_keyspace::CDC_TOPOLOGY_DESCRIPTION
                        || resource_view.table() == db::system_distributed_keyspace::CDC_TIMESTAMPS
                        || resource_view.table() == db::system_distributed_keyspace::CDC_GENERATIONS_V2)) {
                    return make_exception_future<>(exceptions::unauthorized_exception(
                            format("Cannot {} {}", auth::permissions::to_string(cmd.permission), cmd.resource)));
                }
            }
        }
    }

    return ensure_has_permission(cmd);
}

future<bool> service::client_state::check_has_permission(auth::command_desc cmd) const {
    if (_is_internal) {
        co_return true;
    }

    std::optional<auth::resource> parent_r = cmd.resource.parent();

    auth::permission_set set = co_await auth::get_permissions(*_auth_service, *_user, cmd.resource);
    if (set.contains(cmd.permission)) {
        co_return true;
    }
    if (parent_r) {
        co_return co_await check_has_permission({cmd.permission, *parent_r});
    }
    co_return false;
}

future<> service::client_state::ensure_has_permission(auth::command_desc cmd) const {
    return check_has_permission(cmd).then([this, cmd](bool ok) {
        if (!ok) {
            return make_exception_future<>(exceptions::unauthorized_exception(
                format("User {} has no {} permission on {} or any of its parents",
                        *_user,
                        auth::permissions::to_string(cmd.permission),
                        cmd.resource)));
        }
        return make_ready_future<>();
    });
}

void service::client_state::set_keyspace(replica::database& db, std::string_view keyspace) {
    // Skip keyspace validation for non-authenticated users. Apparently, some client libraries
    // call set_keyspace() before calling login(), and we have to handle that.
    if (_user && !db.has_keyspace(keyspace)) {
        throw exceptions::invalid_request_exception(format("Keyspace '{}' does not exist", keyspace));
    }
    _keyspace = sstring(keyspace);
}

future<> service::client_state::ensure_exists(const auth::resource& r) const {
    return _auth_service->exists(r).then([&r](bool exists) {
        if (!exists) {
            return make_exception_future<>(exceptions::invalid_request_exception(format("{} doesn't exist.", r)));
        }

        return make_ready_future<>();
    });
}

future<> service::client_state::maybe_update_per_service_level_params() {
    if (_sl_controller && _user && _user->name) {
        auto slo_opt = co_await _sl_controller->find_effective_service_level(_user->name.value());
        if (!slo_opt) {
            co_return;
        }
        auto slo_timeout_or = [&] (const lowres_clock::duration& default_timeout) {
            return std::visit(overloaded_functor{
                [&] (const qos::service_level_options::unset_marker&) -> lowres_clock::duration {
                    return default_timeout;
                },
                [&] (const qos::service_level_options::delete_marker&) -> lowres_clock::duration {
                    return default_timeout;
                },
                [&] (const lowres_clock::duration& d) -> lowres_clock::duration {
                    return d;
                },
            }, slo_opt->timeout);
        };
        _timeout_config.read_timeout = slo_timeout_or(_default_timeout_config.read_timeout);
        _timeout_config.write_timeout = slo_timeout_or(_default_timeout_config.write_timeout);
        _timeout_config.range_read_timeout = slo_timeout_or(_default_timeout_config.range_read_timeout);
        _timeout_config.counter_write_timeout = slo_timeout_or(_default_timeout_config.counter_write_timeout);
        _timeout_config.truncate_timeout = slo_timeout_or(_default_timeout_config.truncate_timeout);
        _timeout_config.cas_timeout = slo_timeout_or(_default_timeout_config.cas_timeout);
        _timeout_config.other_timeout = slo_timeout_or(_default_timeout_config.other_timeout);

        _workload_type = slo_opt->workload;
    }
}
