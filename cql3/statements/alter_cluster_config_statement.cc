/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "cql3/statements/alter_cluster_config_statement.hh"

#include <seastar/core/coroutine.hh>

#include "cql3/statements/prepared_statement.hh"
#include "cql3/statements/cf_prop_defs.hh"
#include "cql3/query_processor.hh"
#include "cql3/untyped_result_set.hh"
#include "db/cluster_config_registry.hh"
#include "db/schema_tables.hh"
#include "data_dictionary/data_dictionary.hh"
#include "data_dictionary/keyspace_metadata.hh"
#include "exceptions/exceptions.hh"
#include "auth/authenticator.hh"
#include "auth/resource.hh"
#include "auth/service.hh"
#include "locator/token_metadata.hh"
#include "locator/topology.hh"
#include "schema/schema_builder.hh"
#include "service/migration_manager.hh"
#include "service/client_state.hh"
#include "service/query_state.hh"
#include "service/storage_proxy.hh"

namespace cql3 {

namespace statements {

namespace {

db::cluster_config_registry::scope registry_scope(alter_cluster_config_statement::scope scope) {
    switch (scope) {
    case alter_cluster_config_statement::scope::cluster:
        return db::cluster_config_registry::scope::cluster;
    case alter_cluster_config_statement::scope::datacenter:
        return db::cluster_config_registry::scope::datacenter;
    case alter_cluster_config_statement::scope::rack:
        return db::cluster_config_registry::scope::rack;
    case alter_cluster_config_statement::scope::node:
        return db::cluster_config_registry::scope::node;
    }
    __builtin_unreachable();
}

// The resource of the scope target the statement names. A target that is not in the topology
// maps to the cluster: validate() rejects the statement, but only for a role that could
// have configured the target had it existed, and the name does not reach the permissions cache.
auth::resource scope_target_resource(query_processor& qp,
        alter_cluster_config_statement::scope scope,
        const std::optional<sstring>& dc_name,
        const std::optional<sstring>& rack_name,
        const std::optional<utils::UUID>& node_uuid) {
    const auto tmptr = qp.proxy().get_token_metadata_ptr();
    const auto& topology = tmptr->get_topology();
    switch (scope) {
    case alter_cluster_config_statement::scope::cluster:
        return auth::root_cluster_resource();
    case alter_cluster_config_statement::scope::datacenter:
        if (topology.get_datacenters().contains(*dc_name)) {
            return auth::make_cluster_resource(*dc_name);
        }
        return auth::root_cluster_resource();
    case alter_cluster_config_statement::scope::rack: {
        const auto dc_it = topology.get_datacenter_racks().find(*dc_name);
        if (dc_it != topology.get_datacenter_racks().end() && dc_it->second.contains(*rack_name)) {
            return auth::make_cluster_resource(*dc_name, *rack_name);
        }
        return auth::root_cluster_resource();
    }
    case alter_cluster_config_statement::scope::node: {
        if (const auto* node = topology.find_node(locator::host_id(*node_uuid))) {
            return auth::make_cluster_resource(node->dc_rack().dc, node->dc_rack().rack, fmt::to_string(*node_uuid));
        }
        return auth::root_cluster_resource();
    }
    }
    __builtin_unreachable();
}

// Returns the resolved registry option so callers can canonicalize the value before persisting it.
const db::cluster_config_registry::option& validate_cluster_option(query_processor& qp, alter_cluster_config_statement::scope scope, std::string_view config_name, const std::optional<sstring>& value) {
    const auto* option = db::cluster_config_registry::find(config_name);
    if (!option) {
        throw exceptions::invalid_request_exception(format("Unknown cluster config option '{}'", config_name));
    }
    // Known to this node but not yet to every node: cluster config is not enabled, or the
    // option's feature is not yet enabled cluster-wide.
    if (!db::cluster_config_registry::is_supported(*option, qp.proxy().features())) {
        throw exceptions::invalid_request_exception(format(
                "Cluster config option '{}' is not yet supported by this cluster. Upgrade all nodes to use it.", config_name));
    }
    if (!db::cluster_config_registry::supports_scope(*option, registry_scope(scope))) {
        throw exceptions::invalid_request_exception(format("Cluster config '{}' does not support this scope", config_name));
    }
    if (value) {
        if (auto error = db::cluster_config_registry::validate_value(*option, *value)) {
            throw exceptions::invalid_request_exception(fmt::format("Invalid value for cluster config '{}': {}", config_name, *error));
        }
    }
    return *option;
}

void validate_scope_target_exists(query_processor& qp,
        alter_cluster_config_statement::scope scope,
        const std::optional<sstring>& dc_name,
        const std::optional<sstring>& rack_name,
        const std::optional<utils::UUID>& node_uuid) {
    const auto& topology = qp.proxy().get_token_metadata_ptr()->get_topology();

    switch (scope) {
    case alter_cluster_config_statement::scope::cluster:
        return;
    case alter_cluster_config_statement::scope::datacenter:
        if (!topology.get_datacenters().contains(*dc_name)) {
            throw exceptions::invalid_request_exception(format("Datacenter '{}' does not exist", *dc_name));
        }
        return;
    case alter_cluster_config_statement::scope::rack: {
        auto dc_it = topology.get_datacenter_racks().find(*dc_name);
        if (dc_it == topology.get_datacenter_racks().end() || !dc_it->second.contains(*rack_name)) {
            throw exceptions::invalid_request_exception(format("Rack '{}.{}' does not exist", *dc_name, *rack_name));
        }
        return;
    }
    case alter_cluster_config_statement::scope::node:
        if (!topology.has_node(locator::host_id(*node_uuid))) {
            throw exceptions::invalid_request_exception(format("Node '{}' does not exist", *node_uuid));
        }
        return;
    }
    __builtin_unreachable();
}

}

alter_cluster_config_statement::alter_cluster_config_statement(scope statement_scope, std::optional<sstring> dc_name, std::optional<sstring> rack_name, std::optional<utils::UUID> node_uuid, sstring config_name, std::optional<sstring> value)
    : schema_altering_statement()
    , _scope(statement_scope)
    , _dc_name(std::move(dc_name))
    , _rack_name(std::move(rack_name))
    , _node_uuid(std::move(node_uuid))
    , _config_name(std::move(config_name))
    , _value(std::move(value)) {
}

std::unique_ptr<alter_cluster_config_statement> alter_cluster_config_statement::for_cluster(sstring config_name, std::optional<sstring> value) {
    return std::make_unique<alter_cluster_config_statement>(scope::cluster, std::nullopt, std::nullopt, std::nullopt, std::move(config_name), std::move(value));
}

std::unique_ptr<alter_cluster_config_statement> alter_cluster_config_statement::for_datacenter(sstring dc_name, sstring config_name, std::optional<sstring> value) {
    return std::make_unique<alter_cluster_config_statement>(scope::datacenter, std::move(dc_name), std::nullopt, std::nullopt, std::move(config_name), std::move(value));
}

std::unique_ptr<alter_cluster_config_statement> alter_cluster_config_statement::for_rack(sstring dc_name, sstring rack_name, sstring config_name, std::optional<sstring> value) {
    return std::make_unique<alter_cluster_config_statement>(scope::rack, std::move(dc_name), std::move(rack_name), std::nullopt, std::move(config_name), std::move(value));
}

std::unique_ptr<alter_cluster_config_statement> alter_cluster_config_statement::for_node(const sstring& node_uuid, sstring config_name, std::optional<sstring> value) {
    return std::make_unique<alter_cluster_config_statement>(scope::node, std::nullopt, std::nullopt, utils::UUID(node_uuid), std::move(config_name), std::move(value));
}

future<> alter_cluster_config_statement::check_access(query_processor& qp, const service::client_state& state) const {
    // CONFIGURE cannot be granted before every node knows it. Until then these statements
    // require a superuser, as they did before the permission existed.
    if (!qp.proxy().features().cluster_config_rbac) {
        if (auto* auth_service = state.get_auth_service(); auth_service && !auth_service->underlying_authenticator().require_authentication()) {
            co_return;
        }
        if (!co_await state.has_superuser()) {
            throw exceptions::unauthorized_exception("Only superusers can alter cluster configuration.");
        }
        co_return;
    }
    // An unknown option has no grants; do not let its name reach the permissions cache.
    const std::optional<std::string_view> option = db::cluster_config_registry::find(_config_name)
            ? std::optional<std::string_view>(_config_name) : std::nullopt;
    const auto target = scope_target_resource(qp, _scope, _dc_name, _rack_name, _node_uuid);
    co_await state.ensure_has_config_permission(auth::permission::CONFIGURE, target, option);
}

void alter_cluster_config_statement::validate(query_processor& qp, const service::client_state& state) const {
    // Authorization is enforced asynchronously in check_access(); do not repeat it here,
    // because validate() is called synchronously from a coroutine (not a seastar::thread)
    // and the permission check may block on a non-ready future.
    validate_cluster_option(qp, _scope, _config_name, _value);
    validate_scope_target_exists(qp, _scope, _dc_name, _rack_name, _node_uuid);
}

future<std::tuple<::shared_ptr<schema_altering_statement::event_t>, cql3::cql_warnings_vec>>
alter_cluster_config_statement::prepare_schema_mutations(query_processor& qp, service::query_state& state, const query_options&, service::group0_batch& mc) const {
    const auto& option = validate_cluster_option(qp, _scope, _config_name, _value);
    validate_scope_target_exists(qp, _scope, _dc_name, _rack_name, _node_uuid);

    sstring query;
    std::vector<data_value_or_unset> values;
    values.reserve(_value ? 4 : 3);

    // The config option name is the map key and is always the first bind marker.
    values.emplace_back(_config_name);
    if (_value) {
        values.emplace_back(db::cluster_config_registry::canonicalize_value(option, *_value));
    }

    switch (_scope) {
    case scope::cluster:
        if (_value) {
            query = format("UPDATE system_schema.{} SET configs[?] = ? WHERE cluster_name = '{}'",
                    db::schema_tables::SCYLLA_CLUSTERS, db::schema_tables::CLUSTER_CONFIG_SINGLETON_KEY);
        } else {
            query = format("DELETE configs[?] FROM system_schema.{} WHERE cluster_name = '{}'",
                    db::schema_tables::SCYLLA_CLUSTERS, db::schema_tables::CLUSTER_CONFIG_SINGLETON_KEY);
        }
        break;
    case scope::datacenter:
        if (_value) {
            query = format("UPDATE system_schema.{} SET configs[?] = ? WHERE dc_name = ?", db::schema_tables::SCYLLA_DATACENTERS);
        } else {
            query = format("DELETE configs[?] FROM system_schema.{} WHERE dc_name = ?", db::schema_tables::SCYLLA_DATACENTERS);
        }
        values.emplace_back(*_dc_name);
        break;
    case scope::rack:
        if (_value) {
            query = format("UPDATE system_schema.{} SET configs[?] = ? WHERE dc_name = ? AND rack_name = ?", db::schema_tables::SCYLLA_RACKS);
        } else {
            query = format("DELETE configs[?] FROM system_schema.{} WHERE dc_name = ? AND rack_name = ?", db::schema_tables::SCYLLA_RACKS);
        }
        values.emplace_back(*_dc_name);
        values.emplace_back(*_rack_name);
        break;
    case scope::node:
        if (_value) {
            query = format("UPDATE system_schema.{} SET configs[?] = ? WHERE host_id = ?", db::schema_tables::SCYLLA_NODES);
        } else {
            query = format("DELETE configs[?] FROM system_schema.{} WHERE host_id = ?", db::schema_tables::SCYLLA_NODES);
        }
        values.emplace_back(data_value(*_node_uuid));
        break;
    }

    mc.add_mutations(co_await qp.get_mutations_internal(query, state, mc.write_timestamp(), std::move(values)));

    co_return std::make_tuple(::shared_ptr<event_t>(nullptr), cql3::cql_warnings_vec{});
}

std::unique_ptr<prepared_statement> alter_cluster_config_statement::prepare(data_dictionary::database, cql_stats&, const cql_config&) {
    return std::make_unique<prepared_statement>(audit_info(), ::make_shared<alter_cluster_config_statement>(*this));
}

}

}
