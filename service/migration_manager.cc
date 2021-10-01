/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
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

#include <seastar/core/sleep.hh>
#include "schema_registry.hh"
#include "service/migration_manager.hh"
#include "service/storage_proxy.hh"

#include "service/migration_listener.hh"
#include "message/messaging_service.hh"
#include "gms/feature_service.hh"
#include "utils/runtime.hh"
#include "gms/gossiper.hh"
#include "view_info.hh"
#include "schema_builder.hh"
#include "database.hh"
#include "db/schema_tables.hh"
#include "types/user.hh"
#include "db/schema_tables.hh"
#include "cql3/functions/user_aggregate.hh"

namespace service {

static logging::logger mlogger("migration_manager");

using namespace std::chrono_literals;

const std::chrono::milliseconds migration_manager::migration_delay = 60000ms;
static future<schema_ptr> get_schema_definition(table_schema_version v, netw::messaging_service::msg_addr dst, netw::messaging_service& ms);

migration_manager::migration_manager(migration_notifier& notifier, gms::feature_service& feat, netw::messaging_service& ms, gms::gossiper& gossiper) :
        _notifier(notifier), _feat(feat), _messaging(ms), _gossiper(gossiper)
{
}

future<> migration_manager::stop()
{
    if (_as.abort_requested()) {
        return make_ready_future<>();
    }

    mlogger.info("stopping migration service");
    _as.request_abort();

  return uninit_messaging_service().then([this] {
    return parallel_for_each(_schema_pulls.begin(), _schema_pulls.end(), [] (auto&& e) {
        serialized_action& sp = e.second;
        return sp.join();
    }).finally([this] {
        return _background_tasks.close();
    });
  });
}

void migration_manager::init_messaging_service()
{
    auto update_schema = [this] {
        //FIXME: future discarded.
        (void)with_gate(_background_tasks, [this] {
            mlogger.debug("features changed, recalculating schema version");
            return db::schema_tables::recalculate_schema_version(get_storage_proxy(), _feat);
        });
    };

    if (this_shard_id() == 0) {
        _feature_listeners.push_back(_feat.cluster_supports_view_virtual_columns().when_enabled(update_schema));
        _feature_listeners.push_back(_feat.cluster_supports_digest_insensitive_to_expiry().when_enabled(update_schema));
        _feature_listeners.push_back(_feat.cluster_supports_cdc().when_enabled(update_schema));
        _feature_listeners.push_back(_feat.cluster_supports_per_table_partitioners().when_enabled(update_schema));
        _feature_listeners.push_back(_feat.cluster_supports_computed_columns().when_enabled(update_schema));
    }

    _messaging.register_definitions_update([this] (const rpc::client_info& cinfo, std::vector<frozen_mutation> fm, rpc::optional<std::vector<canonical_mutation>> cm) {
        auto src = netw::messaging_service::get_source(cinfo);
        auto f = make_ready_future<>();
        if (cm) {
            f = do_with(std::move(*cm), get_local_shared_storage_proxy(), [this, src] (const std::vector<canonical_mutation>& mutations, shared_ptr<storage_proxy>& p) {
                return merge_schema_in_background(src, mutations);
            });
        } else {
            f = do_with(std::move(fm), get_local_shared_storage_proxy(), [this, src] (const std::vector<frozen_mutation>& mutations, shared_ptr<storage_proxy>& p) {
                return merge_schema_in_background(src, mutations);
            });
        }
        // Start a new fiber.
        (void)f.then_wrapped([src] (auto&& f) {
            if (f.failed()) {
                mlogger.error("Failed to update definitions from {}: {}", src, f.get_exception());
            } else {
                mlogger.debug("Applied definitions update from {}.", src);
            }
        });
        return netw::messaging_service::no_wait();
    });
    _messaging.register_migration_request([this] (const rpc::client_info& cinfo, rpc::optional<netw::schema_pull_options> options) {
        using frozen_mutations = std::vector<frozen_mutation>;
        using canonical_mutations = std::vector<canonical_mutation>;
        const auto cm_retval_supported = options && options->remote_supports_canonical_mutation_retval;

        auto features = _feat.cluster_schema_features();
        auto& proxy = get_storage_proxy();
        return db::schema_tables::convert_schema_to_mutations(proxy, features).then([&proxy, cm_retval_supported] (std::vector<canonical_mutation>&& cm) {
            const auto& db = proxy.local().get_db().local();
            if (cm_retval_supported) {
                return make_ready_future<rpc::tuple<frozen_mutations, canonical_mutations>>(rpc::tuple(frozen_mutations{}, std::move(cm)));
            }
            auto fm = boost::copy_range<std::vector<frozen_mutation>>(cm | boost::adaptors::transformed([&db] (const canonical_mutation& cm) {
                return cm.to_mutation(db.find_column_family(cm.column_family_id()).schema());
            }));
            return make_ready_future<rpc::tuple<frozen_mutations, canonical_mutations>>(rpc::tuple(std::move(fm), std::move(cm)));
        }).finally([p = get_local_shared_storage_proxy()] {
            // keep local proxy alive
        });
    });
    _messaging.register_schema_check([] {
        return make_ready_future<utils::UUID>(service::get_local_storage_proxy().get_db().local().get_version());
    });
    _messaging.register_get_schema_version([this] (unsigned shard, table_schema_version v) {
        auto& proxy = get_local_storage_proxy();
        proxy.get_stats().replica_cross_shard_ops += shard != this_shard_id();
        // FIXME: should this get an smp_service_group? Probably one separate from reads and writes.
        return container().invoke_on(shard, [v, &proxy] (auto&& sp) {
            mlogger.debug("Schema version request for {}", v);
            return proxy.local_db().get_schema_registry().get_frozen(v);
        });
    });
}

future<> migration_manager::uninit_messaging_service()
{
    return when_all_succeed(
        _messaging.unregister_migration_request(),
        _messaging.unregister_definitions_update(),
        _messaging.unregister_schema_check(),
        _messaging.unregister_get_schema_version()
    ).discard_result();
}

void migration_notifier::register_listener(migration_listener* listener)
{
    _listeners.add(listener);
}

future<> migration_notifier::unregister_listener(migration_listener* listener)
{
    return _listeners.remove(listener);
}

future<> migration_manager::schedule_schema_pull(const gms::inet_address& endpoint, const gms::endpoint_state& state)
{
    const auto* value = state.get_application_state_ptr(gms::application_state::SCHEMA);

    if (endpoint != utils::fb_utilities::get_broadcast_address() && value) {
        return maybe_schedule_schema_pull(utils::UUID{value->value}, endpoint);
    }
    return make_ready_future<>();
}

bool migration_manager::have_schema_agreement() {
    const auto known_endpoints = _gossiper.endpoint_state_map;
    if (known_endpoints.size() == 1) {
        // Us.
        return true;
    }
    auto our_version = get_local_storage_proxy().get_db().local().get_version();
    bool match = false;
    for (auto& x : known_endpoints) {
        auto& endpoint = x.first;
        auto& eps = x.second;
        if (endpoint == utils::fb_utilities::get_broadcast_address() || !eps.is_alive()) {
            continue;
        }
        mlogger.debug("Checking schema state for {}.", endpoint);
        auto* schema = eps.get_application_state_ptr(gms::application_state::SCHEMA);
        if (!schema) {
            mlogger.debug("Schema state not yet available for {}.", endpoint);
            return false;
        }
        utils::UUID remote_version{schema->value};
        if (our_version != remote_version) {
            mlogger.debug("Schema mismatch for {} ({} != {}).", endpoint, our_version, remote_version);
            return false;
        } else {
            match = true;
        }
    }
    return match;
}

/**
 * If versions differ this node sends request with local migration list to the endpoint
 * and expecting to receive a list of migrations to apply locally.
 */
future<> migration_manager::maybe_schedule_schema_pull(const utils::UUID& their_version, const gms::inet_address& endpoint)
{
    auto& proxy = get_local_storage_proxy();
    auto& db = proxy.get_db().local();

    if (db.get_version() == their_version || !should_pull_schema_from(endpoint)) {
        mlogger.debug("Not pulling schema because versions match or shouldPullSchemaFrom returned false");
        return make_ready_future<>();
    }

    if (db.get_version() == database::empty_version || runtime::get_uptime() < migration_delay) {
        // If we think we may be bootstrapping or have recently started, submit MigrationTask immediately
        mlogger.debug("Submitting migration task for {}", endpoint);
        return submit_migration_task(endpoint);
    }

    return with_gate(_background_tasks, [this, &db, endpoint] {
        // Include a delay to make sure we have a chance to apply any changes being
        // pushed out simultaneously. See CASSANDRA-5025
        return sleep_abortable(migration_delay, _as).then([this, &db, endpoint] {
            // grab the latest version of the schema since it may have changed again since the initial scheduling
            auto* ep_state = _gossiper.get_endpoint_state_for_endpoint_ptr(endpoint);
            if (!ep_state) {
                mlogger.debug("epState vanished for {}, not submitting migration task", endpoint);
                return make_ready_future<>();
            }
            const auto* value = ep_state->get_application_state_ptr(gms::application_state::SCHEMA);
            if (!value) {
                mlogger.debug("application_state::SCHEMA does not exist for {}, not submitting migration task", endpoint);
                return make_ready_future<>();
            }
            utils::UUID current_version{value->value};
            if (db.get_version() == current_version) {
                mlogger.debug("not submitting migration task for {} because our versions match", endpoint);
                return make_ready_future<>();
            }
            mlogger.debug("submitting migration task for {}", endpoint);
            return submit_migration_task(endpoint);
        });
    }).finally([me = shared_from_this()] {});
}

future<> migration_manager::submit_migration_task(const gms::inet_address& endpoint, bool can_ignore_down_node)
{
    if (!_gossiper.is_alive(endpoint)) {
        auto msg = format("Can't send migration request: node {} is down.", endpoint);
        mlogger.warn("{}", msg);
        return can_ignore_down_node ? make_ready_future<>() : make_exception_future<>(std::runtime_error(msg));
    }
    netw::messaging_service::msg_addr id{endpoint, 0};
    return merge_schema_from(id).handle_exception([](std::exception_ptr e) {
        try {
            std::rethrow_exception(e);
        } catch (const exceptions::configuration_exception& e) {
            mlogger.error("Configuration exception merging remote schema: {}", e.what());
            return make_exception_future<>(e);
        }
    });
}

future<> migration_manager::do_merge_schema_from(netw::messaging_service::msg_addr id)
{
    mlogger.info("Pulling schema from {}", id);
    return _messaging.send_migration_request(std::move(id), netw::schema_pull_options{}).then([this, id] (
            rpc::tuple<std::vector<frozen_mutation>, rpc::optional<std::vector<canonical_mutation>>> frozen_and_canonical_mutations) {
        auto&& [mutations, canonical_mutations] = frozen_and_canonical_mutations;
        if (canonical_mutations) {
            return do_with(std::move(*canonical_mutations), [this, id] (std::vector<canonical_mutation>& mutations) {
                return this->merge_schema_from(id, mutations);
            });
        }
        return do_with(std::move(mutations), [this, id] (auto&& mutations) {
            return this->merge_schema_from(id, mutations);
        });
    }).then([id] {
        mlogger.info("Schema merge with {} completed", id);
    });
}

future<> migration_manager::merge_schema_from(netw::messaging_service::msg_addr id)
{
    mlogger.info("Requesting schema pull from {}", id);
    auto i = _schema_pulls.find(id);
    if (i == _schema_pulls.end()) {
        // FIXME: Drop entries for removed nodes (or earlier).
        i = _schema_pulls.emplace(std::piecewise_construct,
                std::tuple<netw::messaging_service::msg_addr>(id),
                std::tuple<std::function<future<>()>>([id, this] {
                    return do_merge_schema_from(id);
                })).first;
    }
    return i->second.trigger();
}

future<> migration_manager::merge_schema_from(netw::messaging_service::msg_addr src, const std::vector<canonical_mutation>& canonical_mutations) {
    mlogger.debug("Applying schema mutations from {}", src);
    auto& proxy = service::get_storage_proxy();
    const auto& db = proxy.local().get_db().local();

    std::vector<mutation> mutations;
    mutations.reserve(canonical_mutations.size());
    try {
        for (const auto& cm : canonical_mutations) {
            auto& tbl = db.find_column_family(cm.column_family_id());
            mutations.emplace_back(cm.to_mutation(
                    tbl.schema()));
        }
    } catch (no_such_column_family& e) {
        mlogger.error("Error while applying schema mutations from {}: {}", src, e);
        return make_exception_future<>(std::make_exception_ptr<std::runtime_error>(
                    std::runtime_error(fmt::format("Error while applying schema mutations: {}", e))));
    }
    return db::schema_tables::merge_schema(proxy, _feat, std::move(mutations));
}

future<> migration_manager::merge_schema_from(netw::messaging_service::msg_addr src, const std::vector<frozen_mutation>& mutations)
{
    mlogger.debug("Applying schema mutations from {}", src);
    return map_reduce(mutations, [this, src](const frozen_mutation& fm) {
        // schema table's schema is not syncable so just use get_schema_definition()
        return get_schema_definition(fm.schema_version(), src, _messaging).then([&fm](schema_ptr s) {
            s->registry_entry()->mark_synced();
            return fm.unfreeze(std::move(s));
        });
    }, std::vector<mutation>(), [](std::vector<mutation>&& all, mutation&& m) {
        all.emplace_back(std::move(m));
        return std::move(all);
    }).then([this](std::vector<mutation> schema) {
        return db::schema_tables::merge_schema(get_storage_proxy(), _feat, std::move(schema));
    });
}

bool migration_manager::has_compatible_schema_tables_version(const gms::inet_address& endpoint) {
    auto* version = _gossiper.get_application_state_ptr(endpoint, gms::application_state::SCHEMA_TABLES_VERSION);
    return version && version->value == db::schema_tables::version;
}

bool migration_manager::should_pull_schema_from(const gms::inet_address& endpoint) {
    return has_compatible_schema_tables_version(endpoint)
            && !_gossiper.is_gossip_only_member(endpoint);
}

future<> migration_notifier::create_keyspace(const lw_shared_ptr<keyspace_metadata>& ksm) {
    return seastar::async([this, ksm] {
        const auto& name = ksm->name();
        _listeners.for_each([&name] (migration_listener* listener) {
            try {
                listener->on_create_keyspace(name);
            } catch (...) {
                mlogger.warn("Create keyspace notification failed {}: {}", name, std::current_exception());
            }
        });
    });
}

future<> migration_notifier::create_column_family(const schema_ptr& cfm) {
    return seastar::async([this, cfm] {
        const auto& ks_name = cfm->ks_name();
        const auto& cf_name = cfm->cf_name();
        _listeners.for_each([&ks_name, &cf_name] (migration_listener* listener) {
            try {
                listener->on_create_column_family(ks_name, cf_name);
            } catch (...) {
                mlogger.warn("Create column family notification failed {}.{}: {}", ks_name, cf_name, std::current_exception());
            }
        });
    });
}

future<> migration_notifier::create_user_type(const user_type& type) {
    return seastar::async([this, type] {
        const auto& ks_name = type->_keyspace;
        const auto& type_name = type->get_name_as_string();
        _listeners.for_each([&ks_name, &type_name] (migration_listener* listener) {
            try {
                listener->on_create_user_type(ks_name, type_name);
            } catch (...) {
                mlogger.warn("Create user type notification failed {}.{}: {}", ks_name, type_name, std::current_exception());
            }
        });
    });
}

future<> migration_notifier::create_view(const view_ptr& view) {
    return seastar::async([this, view] {
        const auto& ks_name = view->ks_name();
        const auto& view_name = view->cf_name();
        _listeners.for_each([&ks_name, &view_name] (migration_listener* listener) {
            try {
                listener->on_create_view(ks_name, view_name);
            } catch (...) {
                mlogger.warn("Create view notification failed {}.{}: {}", ks_name, view_name, std::current_exception());
            }
        });
    });
}

#if 0
public void notifyCreateFunction(UDFunction udf)
{
    for (IMigrationListener listener : listeners)
        listener.onCreateFunction(udf.name().keyspace, udf.name().name);
}

public void notifyCreateAggregate(UDAggregate udf)
{
    for (IMigrationListener listener : listeners)
        listener.onCreateAggregate(udf.name().keyspace, udf.name().name);
}
#endif

future<> migration_notifier::update_keyspace(const lw_shared_ptr<keyspace_metadata>& ksm) {
    return seastar::async([this, ksm] {
        const auto& name = ksm->name();
        _listeners.for_each([&name] (migration_listener* listener) {
            try {
                listener->on_update_keyspace(name);
            } catch (...) {
                mlogger.warn("Update keyspace notification failed {}: {}", name, std::current_exception());
            }
        });
    });
}

future<> migration_notifier::update_column_family(const schema_ptr& cfm, bool columns_changed) {
    return seastar::async([this, cfm, columns_changed] {
        const auto& ks_name = cfm->ks_name();
        const auto& cf_name = cfm->cf_name();
        _listeners.for_each([&ks_name, &cf_name, columns_changed] (migration_listener* listener) {
            try {
                listener->on_update_column_family(ks_name, cf_name, columns_changed);
            } catch (...) {
                mlogger.warn("Update column family notification failed {}.{}: {}", ks_name, cf_name, std::current_exception());
            }
        });
    });
}

future<> migration_notifier::update_user_type(const user_type& type) {
    return seastar::async([this, type] {
        const auto& ks_name = type->_keyspace;
        const auto& type_name = type->get_name_as_string();
        _listeners.for_each([&ks_name, &type_name] (migration_listener* listener) {
            try {
                listener->on_update_user_type(ks_name, type_name);
            } catch (...) {
                mlogger.warn("Update user type notification failed {}.{}: {}", ks_name, type_name, std::current_exception());
            }
        });
    });
}

future<> migration_notifier::update_view(const view_ptr& view, bool columns_changed) {
    return seastar::async([this, view, columns_changed] {
        const auto& ks_name = view->ks_name();
        const auto& view_name = view->cf_name();
        _listeners.for_each([&ks_name, &view_name, columns_changed] (migration_listener* listener) {
            try {
                listener->on_update_view(ks_name, view_name, columns_changed);
            } catch (...) {
                mlogger.warn("Update view notification failed {}.{}: {}", ks_name, view_name, std::current_exception());
            }
        });
    });
}

#if 0
public void notifyUpdateFunction(UDFunction udf)
{
    for (IMigrationListener listener : listeners)
        listener.onUpdateFunction(udf.name().keyspace, udf.name().name);
}

public void notifyUpdateAggregate(UDAggregate udf)
{
    for (IMigrationListener listener : listeners)
        listener.onUpdateAggregate(udf.name().keyspace, udf.name().name);
}
#endif

future<> migration_notifier::drop_keyspace(const sstring& ks_name) {
    return seastar::async([this, ks_name] {
        _listeners.for_each([&ks_name] (migration_listener* listener) {
            try {
                listener->on_drop_keyspace(ks_name);
            } catch (...) {
                mlogger.warn("Drop keyspace notification failed {}: {}", ks_name, std::current_exception());
            }
        });
    });
}

future<> migration_notifier::drop_column_family(const schema_ptr& cfm) {
    return seastar::async([this, cfm] {
        const auto& cf_name = cfm->cf_name();
        const auto& ks_name = cfm->ks_name();
        _listeners.for_each([&ks_name, &cf_name] (migration_listener* listener) {
            try {
                listener->on_drop_column_family(ks_name, cf_name);
            } catch (...) {
                mlogger.warn("Drop column family notification failed {}.{}: {}", ks_name, cf_name, std::current_exception());
            }
        });
    });
}

future<> migration_notifier::drop_user_type(const user_type& type) {
    return seastar::async([this, type] {
        auto&& ks_name = type->_keyspace;
        auto&& type_name = type->get_name_as_string();
        _listeners.for_each([&ks_name, &type_name] (migration_listener* listener) {
            try {
                listener->on_drop_user_type(ks_name, type_name);
            } catch (...) {
                mlogger.warn("Drop user type notification failed {}.{}: {}", ks_name, type_name, std::current_exception());
            }
        });
    });
}

future<> migration_notifier::drop_view(const view_ptr& view) {
    return seastar::async([this, view] {
        auto&& ks_name = view->ks_name();
        auto&& view_name = view->cf_name();
        _listeners.for_each([&ks_name, &view_name] (migration_listener* listener) {
            try {
                listener->on_drop_view(ks_name, view_name);
            } catch (...) {
                mlogger.warn("Drop view notification failed {}.{}: {}", ks_name, view_name, std::current_exception());
            }
        });
    });
}

void migration_notifier::before_create_column_family(const schema& schema,
        std::vector<mutation>& mutations, api::timestamp_type timestamp) {
    _listeners.for_each([&mutations, &schema, timestamp] (migration_listener* listener) {
        // allow exceptions. so a listener can effectively kill a create-table
        listener->on_before_create_column_family(schema, mutations, timestamp);
    });
}

void migration_notifier::before_update_column_family(const schema& new_schema,
        const schema& old_schema, std::vector<mutation>& mutations, api::timestamp_type ts) {
    _listeners.for_each([&mutations, &new_schema, &old_schema, ts] (migration_listener* listener) {
        // allow exceptions. so a listener can effectively kill an update-column
        listener->on_before_update_column_family(new_schema, old_schema, mutations, ts);
    });
}

void migration_notifier::before_drop_column_family(const schema& schema,
        std::vector<mutation>& mutations, api::timestamp_type ts) {
    _listeners.for_each([&mutations, &schema, ts] (migration_listener* listener) {
        // allow exceptions. so a listener can effectively kill a drop-column
        listener->on_before_drop_column_family(schema, mutations, ts);
    });
}


#if 0
public void notifyDropFunction(UDFunction udf)
{
    for (IMigrationListener listener : listeners)
        listener.onDropFunction(udf.name().keyspace, udf.name().name);
}

public void notifyDropAggregate(UDAggregate udf)
{
    for (IMigrationListener listener : listeners)
        listener.onDropAggregate(udf.name().keyspace, udf.name().name);
}
#endif

future<> migration_manager::announce_keyspace_update(lw_shared_ptr<keyspace_metadata> ksm) {
    auto& proxy = get_local_storage_proxy();
    auto& db = proxy.get_db().local();

    db.validate_keyspace_update(*ksm);
    mlogger.info("Update Keyspace: {}", ksm);
    auto mutations = db::schema_tables::make_create_keyspace_mutations(ksm, api::new_timestamp());
    return announce(std::move(mutations));
}

future<>migration_manager::announce_new_keyspace(lw_shared_ptr<keyspace_metadata> ksm)
{
    return announce_new_keyspace(ksm, api::new_timestamp());
}

future<> migration_manager::announce_new_keyspace(lw_shared_ptr<keyspace_metadata> ksm, api::timestamp_type timestamp)
{
    auto& proxy = get_local_storage_proxy();
    auto& db = proxy.get_db().local();

    db.validate_new_keyspace(*ksm);
    mlogger.info("Create new Keyspace: {}", ksm);
    auto mutations = db::schema_tables::make_create_keyspace_mutations(ksm, timestamp);
    return announce(std::move(mutations));
}

future<> migration_manager::announce_new_column_family(schema_ptr cfm)
{
    return announce_new_column_family(std::move(cfm), api::new_timestamp());
}

future<> migration_manager::include_keyspace_and_announce(
        const keyspace_metadata& keyspace, std::vector<mutation> mutations) {
    // Include the serialized keyspace in case the target node missed a CREATE KEYSPACE migration (see CASSANDRA-5631).
    return db::schema_tables::read_keyspace_mutation(service::get_storage_proxy(), keyspace.name())
            .then([this, mutations = std::move(mutations)] (mutation m) mutable {
                mutations.push_back(std::move(m));
                return announce(std::move(mutations));
            });
}

future<> migration_manager::announce_new_column_family(schema_ptr cfm, api::timestamp_type timestamp) {
#if 0
    cfm.validate();
#endif
    try {
        auto& db = get_local_storage_proxy().get_db().local();
        auto&& keyspace = db.find_keyspace(cfm->ks_name());
        if (db.has_schema(cfm->ks_name(), cfm->cf_name())) {
            throw exceptions::already_exists_exception(cfm->ks_name(), cfm->cf_name());
        }
        if (db.column_family_exists(cfm->id())) {
            throw exceptions::invalid_request_exception(format("Table with ID {} already exists: {}", cfm->id(), db.find_schema(cfm->id())));
        }

        mlogger.info("Create new ColumnFamily: {}", cfm);

        auto ksm = keyspace.metadata();
        return seastar::async([this, cfm, timestamp, ksm] {
            auto mutations = db::schema_tables::make_create_table_mutations(ksm, cfm, timestamp);
            get_notifier().before_create_column_family(*cfm, mutations, timestamp);
            return mutations;
        }).then([this, ksm](std::vector<mutation> mutations) {
            return include_keyspace_and_announce(*ksm, std::move(mutations));
        });
    } catch (const no_such_keyspace& e) {
        throw exceptions::configuration_exception(format("Cannot add table '{}' to non existing keyspace '{}'.", cfm->cf_name(), cfm->ks_name()));
    }
}

future<> migration_manager::announce_column_family_update(schema_ptr cfm, bool from_thrift, std::vector<view_ptr>&& view_updates, std::optional<api::timestamp_type> ts_opt) {
    warn(unimplemented::cause::VALIDATION);
#if 0
    cfm.validate();
#endif
    try {
        auto ts = ts_opt.value_or(api::new_timestamp());
        auto& db = get_local_storage_proxy().get_db().local();
        auto&& old_schema = db.find_column_family(cfm->ks_name(), cfm->cf_name()).schema(); // FIXME: Should we lookup by id?
#if 0
        oldCfm.validateCompatility(cfm);
#endif
        mlogger.info("Update table '{}.{}' From {} To {}", cfm->ks_name(), cfm->cf_name(), *old_schema, *cfm);
        auto&& keyspace = db.find_keyspace(cfm->ks_name()).metadata();

        return seastar::async([this, cfm, old_schema, ts, keyspace, from_thrift, view_updates, &db] {
            auto mutations = map_reduce(view_updates,
                [keyspace, ts] (auto&& view) {
                    auto& old_view = keyspace->cf_meta_data().at(view->cf_name());
                    mlogger.info("Update view '{}.{}' From {} To {}", view->ks_name(), view->cf_name(), *old_view, *view);
                    auto mutations = db::schema_tables::make_update_view_mutations(keyspace, view_ptr(old_view), std::move(view), ts, false);
                    return make_ready_future<std::vector<mutation>>(std::move(mutations));
                }, db::schema_tables::make_update_table_mutations(db, keyspace, old_schema, cfm, ts, from_thrift),
                [] (auto&& result, auto&& view_mutations) {
                    std::move(view_mutations.begin(), view_mutations.end(), std::back_inserter(result));
                    return std::move(result);
                }).get0();

            get_notifier().before_update_column_family(*cfm, *old_schema, mutations, ts);
            return mutations;
        }).then([this, keyspace] (auto&& mutations) {
            return include_keyspace_and_announce(*keyspace, std::move(mutations));
        });
    } catch (const no_such_column_family& e) {
        throw exceptions::configuration_exception(format("Cannot update non existing table '{}' in keyspace '{}'.",
                                                         cfm->cf_name(), cfm->ks_name()));
    }
}

future<> migration_manager::do_announce_new_type(user_type new_type) {
    auto& db = get_local_storage_proxy().get_db().local();
    auto&& keyspace = db.find_keyspace(new_type->_keyspace);
    auto mutations = db::schema_tables::make_create_type_mutations(keyspace.metadata(), new_type, api::new_timestamp());
    return include_keyspace_and_announce(*keyspace.metadata(), std::move(mutations));
}

future<> migration_manager::announce_new_type(user_type new_type) {
    mlogger.info("Create new User Type: {}", new_type->get_name_as_string());
    return do_announce_new_type(new_type);
}

future<> migration_manager::announce_type_update(user_type updated_type) {
    mlogger.info("Update User Type: {}", updated_type->get_name_as_string());
    return do_announce_new_type(updated_type);
}

future<> migration_manager::announce_new_function(shared_ptr<cql3::functions::user_function> func) {
    auto& db = get_local_storage_proxy().get_db().local();
    auto&& keyspace = db.find_keyspace(func->name().keyspace);
    auto mutations = db::schema_tables::make_create_function_mutations(func, api::new_timestamp());
    return include_keyspace_and_announce(*keyspace.metadata(), std::move(mutations));
}

future<> migration_manager::announce_function_drop(
        shared_ptr<cql3::functions::user_function> func) {
    auto& db = get_local_storage_proxy().get_db().local();
    auto&& keyspace = db.find_keyspace(func->name().keyspace);
    auto mutations = db::schema_tables::make_drop_function_mutations(func, api::new_timestamp());
    return include_keyspace_and_announce(*keyspace.metadata(), std::move(mutations));
}

future<> migration_manager::announce_new_aggregate(shared_ptr<cql3::functions::user_aggregate> aggregate) {
    auto& db = get_local_storage_proxy().get_db().local();
    auto&& keyspace = db.find_keyspace(aggregate->name().keyspace);
    auto mutations = db::schema_tables::make_create_aggregate_mutations(aggregate, api::new_timestamp());
    return include_keyspace_and_announce(*keyspace.metadata(), std::move(mutations));
}

future<> migration_manager::announce_aggregate_drop(
        shared_ptr<cql3::functions::user_aggregate> aggregate) {
    auto& db = get_local_storage_proxy().get_db().local();
    auto&& keyspace = db.find_keyspace(aggregate->name().keyspace);
    auto mutations = db::schema_tables::make_drop_aggregate_mutations(aggregate, api::new_timestamp());
    return include_keyspace_and_announce(*keyspace.metadata(), std::move(mutations));
}

#if 0
public static void announceKeyspaceUpdate(KSMetaData ksm) throws ConfigurationException
{
    announceKeyspaceUpdate(ksm, false);
}

public static void announceKeyspaceUpdate(KSMetaData ksm, boolean announceLocally) throws ConfigurationException
{
    ksm.validate();

    KSMetaData oldKsm = Schema.instance.getKSMetaData(ksm.name);
    if (oldKsm == null)
        throw new ConfigurationException(String.format("Cannot update non existing keyspace '%s'.", ksm.name));

    mlogger.info(String.format("Update Keyspace '%s' From %s To %s", ksm.name, oldKsm, ksm));
    announce(LegacySchemaTables.makeCreateKeyspaceMutation(ksm, FBUtilities.timestampMicros()), announceLocally);
}

public static void announceColumnFamilyUpdate(CFMetaData cfm, boolean fromThrift) throws ConfigurationException
{
    announceColumnFamilyUpdate(cfm, fromThrift, false);
}

public static void announceColumnFamilyUpdate(CFMetaData cfm, boolean fromThrift, boolean announceLocally) throws ConfigurationException
{
    cfm.validate();

    CFMetaData oldCfm = Schema.instance.getCFMetaData(cfm.ksName, cfm.cfName);
    if (oldCfm == null)
        throw new ConfigurationException(String.format("Cannot update non existing table '%s' in keyspace '%s'.", cfm.cfName, cfm.ksName));
    KSMetaData ksm = Schema.instance.getKSMetaData(cfm.ksName);

    oldCfm.validateCompatility(cfm);

    mlogger.info(String.format("Update table '%s/%s' From %s To %s", cfm.ksName, cfm.cfName, oldCfm, cfm));
    announce(LegacySchemaTables.makeUpdateTableMutation(ksm, oldCfm, cfm, FBUtilities.timestampMicros(), fromThrift), announceLocally);
}
#endif

future<> migration_manager::announce_keyspace_drop(const sstring& ks_name)
{
    auto& db = get_local_storage_proxy().get_db().local();
    if (!db.has_keyspace(ks_name)) {
        throw exceptions::configuration_exception(format("Cannot drop non existing keyspace '{}'.", ks_name));
    }
    auto& keyspace = db.find_keyspace(ks_name);
    mlogger.info("Drop Keyspace '{}'", ks_name);
    auto&& mutations = db::schema_tables::make_drop_keyspace_mutations(keyspace.metadata(), api::new_timestamp());
    return announce(std::move(mutations));
}

future<> migration_manager::announce_column_family_drop(const sstring& ks_name,
                                                        const sstring& cf_name,
                                                        drop_views drop_views)
{
    try {
        auto& db = get_local_storage_proxy().get_db().local();
        auto& old_cfm = db.find_column_family(ks_name, cf_name);
        auto& schema = old_cfm.schema();
        if (schema->is_view()) {
            throw exceptions::invalid_request_exception("Cannot use DROP TABLE on Materialized View");
        }
        auto keyspace = db.find_keyspace(ks_name).metadata();

        return seastar::async([this, keyspace, schema, &old_cfm, drop_views, &db] {
            // If drop_views is false (the default), we don't allow to delete a
            // table which has views which aren't part of an index. If drop_views
            // is true, we delete those views as well.
            auto&& views = old_cfm.views();
            if (!drop_views && views.size() > schema->all_indices().size()) {
                auto explicit_view_names = views
                                        | boost::adaptors::filtered([&old_cfm](const view_ptr& v) { return !old_cfm.get_index_manager().is_index(v); })
                                        | boost::adaptors::transformed([](const view_ptr& v) { return v->cf_name(); });
                throw exceptions::invalid_request_exception(format("Cannot drop table when materialized views still depend on it ({}.{{{}}})",
                            schema->ks_name(), ::join(", ", explicit_view_names)));
            }
            mlogger.info("Drop table '{}.{}'", schema->ks_name(), schema->cf_name());

            std::vector<mutation> drop_si_mutations;
            if (!schema->all_indices().empty()) {
                auto builder = schema_builder(schema).without_indexes();
                drop_si_mutations = db::schema_tables::make_update_table_mutations(db, keyspace, schema, builder.build(), api::new_timestamp(), false);
            }
            auto ts = api::new_timestamp();
            auto mutations = db::schema_tables::make_drop_table_mutations(keyspace, schema, ts);
            mutations.insert(mutations.end(), std::make_move_iterator(drop_si_mutations.begin()), std::make_move_iterator(drop_si_mutations.end()));
            for (auto& v : views) {
                if (!old_cfm.get_index_manager().is_index(v)) {
                    mlogger.info("Drop view '{}.{}' of table '{}'", v->ks_name(), v->cf_name(), schema->cf_name());
                    auto m = db::schema_tables::make_drop_view_mutations(keyspace, v, api::new_timestamp());
                    mutations.insert(mutations.end(), std::make_move_iterator(m.begin()), std::make_move_iterator(m.end()));
                }
            }

            get_notifier().before_drop_column_family(*schema, mutations, ts);
            return mutations;
        }).then([this, keyspace](std::vector<mutation> mutations) {
            return include_keyspace_and_announce(*keyspace, std::move(mutations));
        });
    } catch (const no_such_column_family& e) {
        throw exceptions::configuration_exception(format("Cannot drop non existing table '{}' in keyspace '{}'.", cf_name, ks_name));
    }

}

future<> migration_manager::announce_type_drop(user_type dropped_type)
{
    auto& db = get_local_storage_proxy().get_db().local();
    auto&& keyspace = db.find_keyspace(dropped_type->_keyspace);
    mlogger.info("Drop User Type: {}", dropped_type->get_name_as_string());
    auto mutations =
            db::schema_tables::make_drop_type_mutations(keyspace.metadata(), dropped_type, api::new_timestamp());
    return include_keyspace_and_announce(*keyspace.metadata(), std::move(mutations));
}

future<> migration_manager::announce_new_view(view_ptr view)
{
#if 0
    view.metadata.validate();
#endif
    auto& db = get_local_storage_proxy().get_db().local();
    try {
        auto&& keyspace = db.find_keyspace(view->ks_name()).metadata();
        if (keyspace->cf_meta_data().contains(view->cf_name())) {
            throw exceptions::already_exists_exception(view->ks_name(), view->cf_name());
        }
        mlogger.info("Create new view: {}", view);
        auto mutations = db::schema_tables::make_create_view_mutations(keyspace, std::move(view), api::new_timestamp());
        return include_keyspace_and_announce(*keyspace, std::move(mutations));
    } catch (const no_such_keyspace& e) {
        throw exceptions::configuration_exception(format("Cannot add view '{}' to non existing keyspace '{}'.", view->cf_name(), view->ks_name()));
    }
}

future<> migration_manager::announce_view_update(view_ptr view)
{
#if 0
    view.metadata.validate();
#endif
    auto& db = get_local_storage_proxy().get_db().local();
    try {
        auto&& keyspace = db.find_keyspace(view->ks_name()).metadata();
        auto& old_view = keyspace->cf_meta_data().at(view->cf_name());
        if (!old_view->is_view()) {
            throw exceptions::invalid_request_exception("Cannot use ALTER MATERIALIZED VIEW on Table");
        }
#if 0
        oldCfm.validateCompatility(cfm);
#endif
        mlogger.info("Update view '{}.{}' From {} To {}", view->ks_name(), view->cf_name(), *old_view, *view);
        auto mutations = db::schema_tables::make_update_view_mutations(keyspace, view_ptr(old_view), std::move(view), api::new_timestamp(), true);
        return include_keyspace_and_announce(*keyspace, std::move(mutations));
    } catch (const std::out_of_range& e) {
        throw exceptions::configuration_exception(format("Cannot update non existing materialized view '{}' in keyspace '{}'.",
                                                         view->cf_name(), view->ks_name()));
    }
}

future<> migration_manager::announce_view_drop(const sstring& ks_name,
                                               const sstring& cf_name)
{
    auto& db = get_local_storage_proxy().get_db().local();
    try {
        auto& view = db.find_column_family(ks_name, cf_name).schema();
        if (!view->is_view()) {
            throw exceptions::invalid_request_exception("Cannot use DROP MATERIALIZED VIEW on Table");
        }
        if (db.find_column_family(view->view_info()->base_id()).get_index_manager().is_index(view_ptr(view))) {
            throw exceptions::invalid_request_exception("Cannot use DROP MATERIALIZED VIEW on Index");
        }
        auto keyspace = db.find_keyspace(ks_name).metadata();
        mlogger.info("Drop view '{}.{}'", view->ks_name(), view->cf_name());
        auto mutations = db::schema_tables::make_drop_view_mutations(keyspace, view_ptr(std::move(view)), api::new_timestamp());
        return include_keyspace_and_announce(*keyspace, std::move(mutations));
    } catch (const no_such_column_family& e) {
        throw exceptions::configuration_exception(format("Cannot drop non existing materialized view '{}' in keyspace '{}'.",
                                                         cf_name, ks_name));
    }
}

#if 0
public static void announceAggregateDrop(UDAggregate udf, boolean announceLocally)
{
    mlogger.info(String.format("Drop aggregate function overload '%s' args '%s'", udf.name(), udf.argTypes()));
    KSMetaData ksm = Schema.instance.getKSMetaData(udf.name().keyspace);
    announce(LegacySchemaTables.makeDropAggregateMutation(ksm, udf, FBUtilities.timestampMicros()), announceLocally);
}
#endif

future<> migration_manager::push_schema_mutation(const gms::inet_address& endpoint, const std::vector<mutation>& schema)
{
    netw::messaging_service::msg_addr id{endpoint, 0};
    auto schema_features = _feat.cluster_schema_features();
    auto adjusted_schema = db::schema_tables::adjust_schema_for_schema_features(schema, schema_features);
    auto fm = std::vector<frozen_mutation>(adjusted_schema.begin(), adjusted_schema.end());
    auto cm = std::vector<canonical_mutation>(adjusted_schema.begin(), adjusted_schema.end());
    return _messaging.send_definitions_update(id, std::move(fm), std::move(cm));
}

// Returns a future on the local application of the schema
future<> migration_manager::announce(std::vector<mutation> schema) {
    auto f = db::schema_tables::merge_schema(get_storage_proxy(), _feat, schema);

    return do_with(std::move(schema), [this, live_members = _gossiper.get_live_members()](auto && schema) {
        return parallel_for_each(live_members.begin(), live_members.end(), [this, &schema](auto& endpoint) {
            // only push schema to nodes with known and equal versions
            if (endpoint != utils::fb_utilities::get_broadcast_address() &&
                _messaging.knows_version(endpoint) &&
                _messaging.get_raw_version(endpoint) ==
                netw::messaging_service::current_version) {
                return push_schema_mutation(endpoint, schema);
            } else {
                return make_ready_future<>();
            }
        });
    }).then([f = std::move(f)] () mutable { return std::move(f); });
}

/**
 * Announce my version passively over gossip.
 * Used to notify nodes as they arrive in the cluster.
 *
 * @param version The schema version to announce
 */
future<> migration_manager::passive_announce(utils::UUID version) {
    return _gossiper.container().invoke_on(0, [version] (auto&& gossiper) {
        mlogger.debug("Gossiping my schema version {}", version);
        return gossiper.add_local_application_state(gms::application_state::SCHEMA, gms::versioned_value::schema(version));
    });
}

#if 0
/**
 * Clear all locally stored schema information and reset schema to initial state.
 * Called by user (via JMX) who wants to get rid of schema disagreement.
 *
 * @throws IOException if schema tables truncation fails
 */
public static void resetLocalSchema() throws IOException
{
    mlogger.info("Starting local schema reset...");

    mlogger.debug("Truncating schema tables...");

    LegacySchemaTables.truncateSchemaTables();

    mlogger.debug("Clearing local schema keyspace definitions...");

    Schema.instance.clear();

    Set<InetAddress> liveEndpoints = Gossiper.instance.getLiveMembers();
    liveEndpoints.remove(FBUtilities.getBroadcastAddress());

    // force migration if there are nodes around
    for (InetAddress node : liveEndpoints)
    {
        if (shouldPullSchemaFrom(node))
        {
            mlogger.debug("Requesting schema from {}", node);
            FBUtilities.waitOnFuture(submitMigrationTask(node));
            break;
        }
    }

    mlogger.info("Local schema reset is complete.");
}

public static class MigrationsSerializer implements IVersionedSerializer<Collection<Mutation>>
{
    public static MigrationsSerializer instance = new MigrationsSerializer();

    public void serialize(Collection<Mutation> schema, DataOutputPlus out, int version) throws IOException
    {
        out.writeInt(schema.size());
        for (Mutation mutation : schema)
            Mutation.serializer.serialize(mutation, out, version);
    }

    public Collection<Mutation> deserialize(DataInput in, int version) throws IOException
    {
        int count = in.readInt();
        Collection<Mutation> schema = new ArrayList<>(count);

        for (int i = 0; i < count; i++)
            schema.add(Mutation.serializer.deserialize(in, version));

        return schema;
    }

    public long serializedSize(Collection<Mutation> schema, int version)
    {
        int size = TypeSizes.NATIVE.sizeof(schema.size());
        for (Mutation mutation : schema)
            size += Mutation.serializer.serializedSize(mutation, version);
        return size;
    }
}
#endif


// Ensure that given schema version 's' was synced with on current node. See schema::is_synced().
//
// The endpoint is the node from which 's' originated.
//
future<> migration_manager::maybe_sync(const schema_ptr& s, netw::messaging_service::msg_addr endpoint) {
    if (s->is_synced()) {
        return make_ready_future<>();
    }

    return s->registry_entry()->maybe_sync([this, s, endpoint] {
        // Serialize schema sync by always doing it on shard 0.
        if (this_shard_id() == 0) {
            mlogger.debug("Syncing schema of {}.{} (v={}) with {}", s->ks_name(), s->cf_name(), s->version(), endpoint);
            return merge_schema_from(endpoint);
        } else {
            return container().invoke_on(0, [gs = global_schema_ptr(s), endpoint] (migration_manager& local_mm) {
                schema_ptr s = gs.get();
                schema_registry_entry& e = *s->registry_entry();
                mlogger.debug("Syncing schema of {}.{} (v={}) with {}", s->ks_name(), s->cf_name(), s->version(), endpoint);
                return local_mm.merge_schema_from(endpoint);
            });
        }
    });
}

// Returns schema of given version, either from cache or from remote node identified by 'from'.
// Doesn't affect current node's schema in any way.
static future<schema_ptr> get_schema_definition(table_schema_version v, netw::messaging_service::msg_addr dst, netw::messaging_service& ms) {
    return local_schema_registry().get_or_load(v, [&ms, dst] (table_schema_version v) {
        mlogger.debug("Requesting schema {} from {}", v, dst);
        return ms.send_get_schema_version(dst, v).then([] (frozen_schema s) {
            auto& proxy = get_storage_proxy();
            // Since the latest schema version is always present in the schema registry
            // we only happen to query already outdated schema version, which is
            // referenced by the incoming request.
            // That means the column mapping for the schema should always be inserted
            // with TTL (refresh TTL in case column mapping already existed prior to that).
            auto us = s.unfreeze(db::schema_ctxt(proxy));
            // if this is a view - we might need to fix it's schema before registering it.
            if (us->is_view()) {
                auto& db = proxy.local().local_db();
                schema_ptr base_schema = db.find_schema(us->view_info()->base_id());
                auto fixed_view = db::schema_tables::maybe_fix_legacy_secondary_index_mv_schema(db, view_ptr(us), base_schema,
                        db::schema_tables::preserve_version::yes);
                if (fixed_view) {
                    us = fixed_view;
                }
            }
            return db::schema_tables::store_column_mapping(proxy, us, true).then([us] {
                return frozen_schema{us};
            });
        });
    }).then([] (schema_ptr s) {
        // If this is a view so this schema also needs a reference to the base
        // table.
        if (s->is_view()) {
            if (!s->view_info()->base_info()) {
                auto& db = service::get_local_storage_proxy().local_db();
                // This line might throw a no_such_column_family
                // It should be fine since if we tried to register a view for which
                // we don't know the base table, our registry is broken.
                schema_ptr base_schema = db.find_schema(s->view_info()->base_id());
                s->view_info()->set_base_info(s->view_info()->make_base_dependent_view_info(*base_schema));
            }
        }
        return s;
    });
}

future<schema_ptr> migration_manager::get_schema_for_read(table_schema_version v, netw::messaging_service::msg_addr dst, netw::messaging_service& ms) {
    return get_schema_for_write(v, dst, ms);
}

future<schema_ptr> migration_manager::get_schema_for_write(table_schema_version v, netw::messaging_service::msg_addr dst, netw::messaging_service& ms) {
    return get_schema_definition(v, dst, ms).then([this, dst] (schema_ptr s) {
        return maybe_sync(s, dst).then([s] {
            return s;
        });
    });
}

future<> migration_manager::sync_schema(const database& db, const std::vector<gms::inet_address>& nodes) {
    using schema_and_hosts = std::unordered_map<utils::UUID, std::vector<gms::inet_address>>;
    return do_with(schema_and_hosts(), db.get_version(), [this, &nodes] (schema_and_hosts& schema_map, utils::UUID& my_version) {
        return parallel_for_each(nodes, [this, &schema_map, &my_version] (const gms::inet_address& node) {
            return _messaging.send_schema_check(netw::msg_addr(node)).then([node, &schema_map, &my_version] (utils::UUID remote_version) {
                if (my_version != remote_version) {
                    schema_map[remote_version].emplace_back(node);
                }
            });
        }).then([this, &schema_map] {
            return parallel_for_each(schema_map, [this] (auto& x) {
                mlogger.debug("Pulling schema {} from {}", x.first, x.second.front());
                bool can_ignore_down_node = false;
                return submit_migration_task(x.second.front(), can_ignore_down_node);
            });
        });
    });
}

future<column_mapping> get_column_mapping(utils::UUID table_id, table_schema_version v) {
    auto& db = get_local_storage_proxy().local_db();
    schema_ptr s = db.get_schema_registry().get_or_null(v);
    if (s) {
        return make_ready_future<column_mapping>(s->get_column_mapping());
    }
    return db::schema_tables::get_column_mapping(table_id, v);
}

}
