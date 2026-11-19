/*
 * Copyright 2016-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)
 */

#include <seastar/core/thread.hh>

#include "auth/authorizer.hh"
#include "auth/service.hh"
#include "db/system_keyspace.hh"
#include "permission_altering_statement.hh"
#include "cql3/query_processor.hh"
#include "cql3/role_name.hh"
#include "cql3/statements/cluster_config_props.hh"
#include "db/cluster_config_registry.hh"
#include "gms/feature_service.hh"
#include "service/storage_proxy.hh"

static auth::permission_set filter_applicable_permissions(const auth::permission_set& ps, const auth::resource& r) {
    auto const filtered_permissions = auth::permission_set::from_mask(ps.mask() & r.applicable_permissions().mask());

    if (!filtered_permissions) {
        throw exceptions::syntax_exception(
                format("Resource {} does not support any of the requested permissions.", r));
    }

    return filtered_permissions;
}

cql3::statements::permission_altering_statement::permission_altering_statement(
                auth::permission_set permissions, auth::resource resource,
                const role_name& rn, std::vector<sstring> config_options)
                : _permissions(filter_applicable_permissions(permissions, resource))
                , _resource(std::move(resource))
                , _role_name(rn.to_string())
                , _config_options(std::move(config_options))
                , _all_permissions(permissions.mask() == auth::permissions::ALL.mask()) {
    if (!_config_options.empty() && _permissions.mask() != auth::permission_set::of<auth::permission::CONFIGURE>().mask()) {
        throw exceptions::syntax_exception("Only the CONFIGURE permission takes a list of cluster config options.");
    }
}

future<> cql3::statements::permission_altering_statement::for_each_resource(std::function<future<>(const auth::resource&)> f) const {
    if (_config_options.empty()) {
        co_await f(_resource);
        co_return;
    }
    for (const auto& option : _config_options) {
        co_await f(_resource.with_config_option(option));
    }
}

future<> cql3::statements::permission_altering_statement::check_access(query_processor& qp, const service::client_state& state) const {
    state.ensure_not_anonymous();
    maybe_correct_resource(_resource, state, qp);

    // The resource the permission checks below look at. For a REVOKE on a datacenter, rack
    // or node that is not in the topology it is the cluster: only a role that could have
    // revoked there had the target existed may proceed, and the name does not reach the
    // permissions cache, which has no size limit.
    auto checked_resource = _resource;
    if (is_grant() || _resource.kind() != auth::resource_kind::cluster) {
        co_await state.ensure_exists(_resource);
    } else if (!co_await state.get_auth_service()->exists(_resource)) {
        checked_resource = auth::root_cluster_resource();
        if (const auth::cluster_resource_view v(_resource); v.host_id() && v.datacenter()->empty()) {
            // A node that left the topology: its datacenter and rack, which are part of the
            // resource name, are only known from the grants that still name it.
            try {
                const auto details = co_await state.get_auth_service()->underlying_authorizer().list_granted_to(_role_name);
                for (const auto& pd : details) {
                    if (pd.resource.kind() == auth::resource_kind::cluster && auth::cluster_resource_view(pd.resource).host_id() == v.host_id()) {
                        _resource = pd.resource.without_config_option();
                        break;
                    }
                }
            } catch (const auth::unsupported_authorization_operation& e) {
                throw exceptions::invalid_request_exception(e.what());
            }
        }
    }

    if (_resource.kind() == auth::resource_kind::functions) {
        // Even if the resource exists, it may be a builtin function or all builtin functions, in which case we disallow altering permissions on it.
        auth::functions_resource_view v(_resource);
        if (v.keyspace() && *v.keyspace() == db::system_keyspace::NAME) {
            throw exceptions::invalid_request_exception("Altering permissions on builtin functions is not supported");
        }
    }

    const bool configure = _permissions.contains(auth::permission::CONFIGURE);
    const bool config_scoped = configure || _resource.kind() == auth::resource_kind::cluster;
    if (config_scoped && !qp.proxy().features().cluster_config_rbac) {
        // A node that does not know the permission or the resource fails to read such a row.
        throw exceptions::invalid_request_exception(
                "Permissions for cluster config are not yet supported by this cluster. Upgrade all nodes to use them.");
    }
    if (is_grant()) {
        for (const auto& name : _config_options) {
            const auto* opt = db::cluster_config_registry::find(name);
            if (!opt) {
                throw exceptions::invalid_request_exception(format("Unknown cluster config option '{}'", name));
            }
            if (!opt->scopes.intersects(cluster_config_props::scopes_within(_resource))) {
                throw exceptions::invalid_request_exception(format("Cluster config option '{}' cannot be set on {} or below it", name, _resource));
            }
        }
    }

    // The role needs AUTHORIZE on the resource or its parents, and must itself hold what
    // it grants or revokes. For cluster config both are looked up along the scope chain.
    if (config_scoped) {
        co_await state.ensure_has_config_permission(auth::permission::AUTHORIZE, checked_resource, std::nullopt);
    } else {
        co_await state.ensure_has_permission({auth::permission::AUTHORIZE, _resource});
    }
    for (auth::permission p : _permissions) {
        if (p != auth::permission::CONFIGURE) {
            // TODO: how about we re-write the access check to check a set
            // right away.
            co_await state.ensure_has_permission({p, checked_resource});
        } else if (_config_options.empty()) {
            co_await state.ensure_has_config_permission(p, checked_resource, std::nullopt);
        } else {
            for (const auto& name : _config_options) {
                // REVOKE accepts an option that is not registered. It has no grants a role
                // could hold through a broader scope, so only the all-options grant counts.
                const bool registered = db::cluster_config_registry::find(name);
                co_await state.ensure_has_config_permission(p, checked_resource, registered ? std::optional<std::string_view>(name) : std::nullopt);
            }
        }
    }
}
