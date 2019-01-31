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
 * Copyright (C) 2015 ScyllaDB
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

#include "schema_registry.hh"
#include "service/migration_manager.hh"

#include "service/migration_listener.hh"
#include "message/messaging_service.hh"
#include "service/storage_service.hh"
#include "service/migration_task.hh"
#include "utils/runtime.hh"
#include "gms/gossiper.hh"
#include "view_info.hh"
#include "schema_builder.hh"

namespace service {

static logging::logger mlogger("migration_manager");

distributed<service::migration_manager> _the_migration_manager;

using namespace std::chrono_literals;

const std::chrono::milliseconds migration_manager::migration_delay = 60000ms;

migration_manager::migration_manager()
    : _listeners{}
{
}

future<> migration_manager::stop()
{
    uninit_messaging_service();
    return parallel_for_each(_schema_pulls.begin(), _schema_pulls.end(), [] (auto&& e) {
        serialized_action& sp = e.second;
        return sp.join();
    });
}

void migration_manager::init_messaging_service()
{
    auto& ms = netw::get_local_messaging_service();
    ms.register_definitions_update([this] (const rpc::client_info& cinfo, std::vector<frozen_mutation> m) {
        auto src = netw::messaging_service::get_source(cinfo);
        do_with(std::move(m), get_local_shared_storage_proxy(), [src] (const std::vector<frozen_mutation>& mutations, shared_ptr<storage_proxy>& p) {
            return service::get_local_migration_manager().merge_schema_from(src, mutations);
        }).then_wrapped([src] (auto&& f) {
            if (f.failed()) {
                mlogger.error("Failed to update definitions from {}: {}", src, f.get_exception());
            } else {
                mlogger.debug("Applied definitions update from {}.", src);
            }
        });
        return netw::messaging_service::no_wait();
    });
    ms.register_migration_request([this] (const rpc::client_info& cinfo) {
        auto src = netw::messaging_service::get_source(cinfo);
        if (!has_compatible_schema_tables_version(src.addr)) {
            mlogger.debug("Ignoring schema request from incompatible node: {}", src);
            return make_ready_future<std::vector<frozen_mutation>>(std::vector<frozen_mutation>());
        }
        return db::schema_tables::convert_schema_to_mutations(get_storage_proxy()).finally([p = get_local_shared_storage_proxy()] {
            // keep local proxy alive
        });
    });
    ms.register_schema_check([] {
        return make_ready_future<utils::UUID>(service::get_local_storage_service().db().local().get_version());
    });
}

void migration_manager::uninit_messaging_service()
{
    auto& ms = netw::get_local_messaging_service();
    ms.unregister_migration_request();
    ms.unregister_definitions_update();
    ms.unregister_schema_check();
}

void migration_manager::register_listener(migration_listener* listener)
{
    _listeners.emplace_back(listener);
}

void migration_manager::unregister_listener(migration_listener* listener)
{
    _listeners.erase(std::remove(_listeners.begin(), _listeners.end(), listener), _listeners.end());
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
    const auto known_endpoints = gms::get_local_gossiper().endpoint_state_map;
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
    auto& ss = get_storage_service().local();
    if (db.get_version() == their_version || !should_pull_schema_from(endpoint)) {
        mlogger.debug("Not pulling schema because versions match or shouldPullSchemaFrom returned false");
        return make_ready_future<>();
    }

    // Disable pulls during rolling upgrade from 1.7 to 2.0 to avoid
    // schema version inconsistency. See https://github.com/scylladb/scylla/issues/2802.
    if (!ss.cluster_supports_schema_tables_v3()) {
        mlogger.debug("Delaying pull with {} until cluster upgrade is complete", endpoint);
        return ss.cluster_supports_schema_tables_v3().when_enabled().then([this, their_version, endpoint] {
            return maybe_schedule_schema_pull(their_version, endpoint);
        });
    }

    if (db.get_version() == database::empty_version || runtime::get_uptime() < migration_delay) {
        // If we think we may be bootstrapping or have recently started, submit MigrationTask immediately
        mlogger.debug("Submitting migration task for {}", endpoint);
        return submit_migration_task(endpoint);
    } else {
        // Include a delay to make sure we have a chance to apply any changes being
        // pushed out simultaneously. See CASSANDRA-5025
        return sleep(migration_delay).then([this, &proxy, endpoint] {
            // grab the latest version of the schema since it may have changed again since the initial scheduling
            auto& gossiper = gms::get_local_gossiper();
            auto* ep_state = gossiper.get_endpoint_state_for_endpoint_ptr(endpoint);
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
            auto& db = proxy.get_db().local();
            if (db.get_version() == current_version) {
                mlogger.debug("not submitting migration task for {} because our versions match", endpoint);
                return make_ready_future<>();
            }
            mlogger.debug("submitting migration task for {}", endpoint);
            return submit_migration_task(endpoint);
        });
    }
}

future<> migration_manager::submit_migration_task(const gms::inet_address& endpoint)
{
    return service::migration_task::run_may_throw(get_storage_proxy(), endpoint);
}

future<> migration_manager::do_merge_schema_from(netw::messaging_service::msg_addr id)
{
    auto& ms = netw::get_local_messaging_service();
    mlogger.info("Pulling schema from {}", id);
    return ms.send_migration_request(std::move(id)).then([this, id] (std::vector<frozen_mutation> mutations) {
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

future<> migration_manager::merge_schema_from(netw::messaging_service::msg_addr src, const std::vector<frozen_mutation>& mutations)
{
    mlogger.debug("Applying schema mutations from {}", src);
    return map_reduce(mutations, [src](const frozen_mutation& fm) {
        // schema table's schema is not syncable so just use get_schema_definition()
        return get_schema_definition(fm.schema_version(), src).then([&fm](schema_ptr s) {
            s->registry_entry()->mark_synced();
            return fm.unfreeze(std::move(s));
        });
    }, std::vector<mutation>(), [](std::vector<mutation>&& all, mutation&& m) {
        all.emplace_back(std::move(m));
        return std::move(all);
    }).then([](std::vector<mutation> schema) {
        return db::schema_tables::merge_schema(get_storage_proxy(), std::move(schema));
    });
}

bool migration_manager::has_compatible_schema_tables_version(const gms::inet_address& endpoint) {
    auto* version = gms::get_local_gossiper().get_application_state_ptr(endpoint, gms::application_state::SCHEMA_TABLES_VERSION);
    return version && version->value == db::schema_tables::version;
}

bool migration_manager::should_pull_schema_from(const gms::inet_address& endpoint) {
    return has_compatible_schema_tables_version(endpoint)
            && !gms::get_local_gossiper().is_gossip_only_member(endpoint);
}

future<> migration_manager::notify_create_keyspace(const lw_shared_ptr<keyspace_metadata>& ksm) {
    return seastar::async([this, ksm] {
        auto&& name = ksm->name();
        for (auto&& listener : _listeners) {
            try {
                listener->on_create_keyspace(name);
            } catch (...) {
                mlogger.warn("Create keyspace notification failed {}: {}", name, std::current_exception());
            }
        }
    });
}

future<> migration_manager::notify_create_column_family(const schema_ptr& cfm) {
    return seastar::async([this, cfm] {
        auto&& ks_name = cfm->ks_name();
        auto&& cf_name = cfm->cf_name();
        for (auto&& listener : _listeners) {
            try {
                listener->on_create_column_family(ks_name, cf_name);
            } catch (...) {
                mlogger.warn("Create column family notification failed {}.{}: {}", ks_name, cf_name, std::current_exception());
            }
        }
    });
}

future<> migration_manager::notify_create_user_type(const user_type& type) {
    return seastar::async([this, type] {
        auto&& ks_name = type->_keyspace;
        auto&& type_name = type->get_name_as_string();
        for (auto&& listener : _listeners) {
            try {
                listener->on_create_user_type(ks_name, type_name);
            } catch (...) {
                mlogger.warn("Create user type notification failed {}.{}: {}", ks_name, type_name, std::current_exception());
            }
        }
    });
}

future<> migration_manager::notify_create_view(const view_ptr& view) {
    return seastar::async([this, view] {
        auto&& ks_name = view->ks_name();
        auto&& view_name = view->cf_name();
        for (auto&& listener : _listeners) {
            try {
                listener->on_create_view(ks_name, view_name);
            } catch (...) {
                mlogger.warn("Create view notification failed {}.{}: {}", ks_name, view_name, std::current_exception());
            }
        }
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

future<> migration_manager::notify_update_keyspace(const lw_shared_ptr<keyspace_metadata>& ksm) {
    return seastar::async([this, ksm] {
        auto&& name = ksm->name();
        for (auto&& listener : _listeners) {
            try {
                listener->on_update_keyspace(name);
            } catch (...) {
                mlogger.warn("Update keyspace notification failed {}: {}", name, std::current_exception());
            }
        }
    });
}

future<> migration_manager::notify_update_column_family(const schema_ptr& cfm, bool columns_changed) {
    return seastar::async([this, cfm, columns_changed] {
        auto&& ks_name = cfm->ks_name();
        auto&& cf_name = cfm->cf_name();
        for (auto&& listener : _listeners) {
            try {
                listener->on_update_column_family(ks_name, cf_name, columns_changed);
            } catch (...) {
                mlogger.warn("Update column family notification failed {}.{}: {}", ks_name, cf_name, std::current_exception());
            }
        }
    });
}

future<> migration_manager::notify_update_user_type(const user_type& type) {
    return seastar::async([this, type] {
        auto&& ks_name = type->_keyspace;
        auto&& type_name = type->get_name_as_string();
        for (auto&& listener : _listeners) {
            try {
                listener->on_update_user_type(ks_name, type_name);
            } catch (...) {
                mlogger.warn("Update user type notification failed {}.{}: {}", ks_name, type_name, std::current_exception());
            }
        }
    });
}

future<> migration_manager::notify_update_view(const view_ptr& view, bool columns_changed) {
    return seastar::async([this, view, columns_changed] {
        auto&& ks_name = view->ks_name();
        auto&& view_name = view->cf_name();
        for (auto&& listener : _listeners) {
            try {
                listener->on_update_view(ks_name, view_name, columns_changed);
            } catch (...) {
                mlogger.warn("Update view notification failed {}.{}: {}", ks_name, view_name, std::current_exception());
            }
        }
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

future<> migration_manager::notify_drop_keyspace(const sstring& ks_name) {
    return seastar::async([this, ks_name] {
        for (auto&& listener : _listeners) {
            try {
                listener->on_drop_keyspace(ks_name);
            } catch (...) {
                mlogger.warn("Drop keyspace notification failed {}: {}", ks_name, std::current_exception());
            }
        }
    });
}

future<> migration_manager::notify_drop_column_family(const schema_ptr& cfm) {
    return seastar::async([this, cfm] {
        auto&& cf_name = cfm->cf_name();
        auto&& ks_name = cfm->ks_name();
        for (auto&& listener : _listeners) {
            try {
                listener->on_drop_column_family(ks_name, cf_name);
            } catch (...) {
                mlogger.warn("Drop column family notification failed {}.{}: {}", ks_name, cf_name, std::current_exception());
            }
        }
    });
}

future<> migration_manager::notify_drop_user_type(const user_type& type) {
    return seastar::async([this, type] {
        auto&& ks_name = type->_keyspace;
        auto&& type_name = type->get_name_as_string();
        for (auto&& listener : _listeners) {
            try {
                listener->on_drop_user_type(ks_name, type_name);
            } catch (...) {
                mlogger.warn("Drop user type notification failed {}.{}: {}", ks_name, type_name, std::current_exception());
            }
        }
    });
}

future<> migration_manager::notify_drop_view(const view_ptr& view) {
    return seastar::async([this, view] {
        auto&& ks_name = view->ks_name();
        auto&& view_name = view->cf_name();
        for (auto&& listener : _listeners) {
            try {
                listener->on_drop_view(ks_name, view_name);
            } catch (...) {
                mlogger.warn("Drop view notification failed {}.{}: {}", ks_name, view_name, std::current_exception());
            }
        }
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

future<> migration_manager::announce_keyspace_update(lw_shared_ptr<keyspace_metadata> ksm, bool announce_locally) {
    return announce_keyspace_update(ksm, api::new_timestamp(), announce_locally);
}

future<> migration_manager::announce_keyspace_update(lw_shared_ptr<keyspace_metadata> ksm, api::timestamp_type timestamp, bool announce_locally) {
    ksm->validate();
    auto& proxy = get_local_storage_proxy();
    if (!proxy.get_db().local().has_keyspace(ksm->name())) {
        throw exceptions::configuration_exception(sprint("Cannot update non existing keyspace '%s'.", ksm->name()));
    }
    mlogger.info("Update Keyspace: {}", ksm);
    auto mutations = db::schema_tables::make_create_keyspace_mutations(ksm, timestamp);
    return announce(std::move(mutations), announce_locally);
}

future<>migration_manager::announce_new_keyspace(lw_shared_ptr<keyspace_metadata> ksm, bool announce_locally)
{
    return announce_new_keyspace(ksm, api::new_timestamp(), announce_locally);
}

future<> migration_manager::announce_new_keyspace(lw_shared_ptr<keyspace_metadata> ksm, api::timestamp_type timestamp, bool announce_locally)
{
    ksm->validate();
    auto& proxy = get_local_storage_proxy();
    if (proxy.get_db().local().has_keyspace(ksm->name())) {
        throw exceptions::already_exists_exception{ksm->name()};
    }
    mlogger.info("Create new Keyspace: {}", ksm);
    auto mutations = db::schema_tables::make_create_keyspace_mutations(ksm, timestamp);
    return announce(std::move(mutations), announce_locally);
}

future<> migration_manager::announce_new_column_family(schema_ptr cfm, bool announce_locally)
{
    return announce_new_column_family(std::move(cfm), api::new_timestamp(), announce_locally);
}

future<> migration_manager::announce_new_column_family(schema_ptr cfm, api::timestamp_type timestamp, bool announce_locally) {
#if 0
    cfm.validate();
#endif
    try {
        auto& db = get_local_storage_proxy().get_db().local();
        auto&& keyspace = db.find_keyspace(cfm->ks_name());
        if (db.has_schema(cfm->ks_name(), cfm->cf_name())) {
            throw exceptions::already_exists_exception(cfm->ks_name(), cfm->cf_name());
        }
        mlogger.info("Create new ColumnFamily: {}", cfm);
        return db::schema_tables::make_create_table_mutations(keyspace.metadata(), cfm, timestamp)
            .then([announce_locally, this] (auto&& mutations) {
                return announce(std::move(mutations), announce_locally);
            });
    } catch (const no_such_keyspace& e) {
        throw exceptions::configuration_exception(sprint("Cannot add table '%s' to non existing keyspace '%s'.", cfm->cf_name(), cfm->ks_name()));
    }
}

future<> migration_manager::announce_column_family_update(schema_ptr cfm, bool from_thrift, std::vector<view_ptr>&& view_updates, bool announce_locally) {
    warn(unimplemented::cause::VALIDATION);
#if 0
    cfm.validate();
#endif
    try {
        auto ts = api::new_timestamp();
        auto& db = get_local_storage_proxy().get_db().local();
        auto&& old_schema = db.find_column_family(cfm->ks_name(), cfm->cf_name()).schema(); // FIXME: Should we lookup by id?
#if 0
        oldCfm.validateCompatility(cfm);
#endif
        mlogger.info("Update table '{}.{}' From {} To {}", cfm->ks_name(), cfm->cf_name(), *old_schema, *cfm);
        auto&& keyspace = db.find_keyspace(cfm->ks_name()).metadata();
        return db::schema_tables::make_update_table_mutations(keyspace, old_schema, cfm, ts, from_thrift)
            .then([announce_locally, keyspace, ts, view_updates = std::move(view_updates)] (auto&& mutations) {
                return map_reduce(view_updates,
                    [keyspace = std::move(keyspace), ts] (auto&& view) {
                        auto& old_view = keyspace->cf_meta_data().at(view->cf_name());
                        mlogger.info("Update view '{}.{}' From {} To {}", view->ks_name(), view->cf_name(), *old_view, *view);
                        return db::schema_tables::make_update_view_mutations(keyspace, view_ptr(old_view), std::move(view), ts, false);
                    }, std::move(mutations),
                    [] (auto&& result, auto&& view_mutations) {
                        std::move(view_mutations.begin(), view_mutations.end(), std::back_inserter(result));
                        return std::move(result);
                    })
                .then([announce_locally] (auto&& mutations) {
                    return announce(std::move(mutations), announce_locally);
                });
            });
    } catch (const no_such_column_family& e) {
        throw exceptions::configuration_exception(sprint("Cannot update non existing table '%s' in keyspace '%s'.",
                                                         cfm->cf_name(), cfm->ks_name()));
    }
}

static future<> do_announce_new_type(user_type new_type, bool announce_locally) {
    auto& db = get_local_storage_proxy().get_db().local();
    auto&& keyspace = db.find_keyspace(new_type->_keyspace);
    return db::schema_tables::make_create_type_mutations(keyspace.metadata(), new_type, api::new_timestamp())
        .then([announce_locally] (auto&& mutations) {
            return migration_manager::announce(std::move(mutations), announce_locally);
        });
}

future<> migration_manager::announce_new_type(user_type new_type, bool announce_locally) {
    mlogger.info("Create new User Type: {}", new_type->get_name_as_string());
    return do_announce_new_type(new_type, announce_locally);
}

future<> migration_manager::announce_type_update(user_type updated_type, bool announce_locally) {
    mlogger.info("Update User Type: {}", updated_type->get_name_as_string());
    return do_announce_new_type(updated_type, announce_locally);
}

#if 0
public static void announceNewFunction(UDFunction udf, boolean announceLocally)
{
    mlogger.info(String.format("Create scalar function '%s'", udf.name()));
    KSMetaData ksm = Schema.instance.getKSMetaData(udf.name().keyspace);
    announce(LegacySchemaTables.makeCreateFunctionMutation(ksm, udf, FBUtilities.timestampMicros()), announceLocally);
}

public static void announceNewAggregate(UDAggregate udf, boolean announceLocally)
{
    mlogger.info(String.format("Create aggregate function '%s'", udf.name()));
    KSMetaData ksm = Schema.instance.getKSMetaData(udf.name().keyspace);
    announce(LegacySchemaTables.makeCreateAggregateMutation(ksm, udf, FBUtilities.timestampMicros()), announceLocally);
}

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

future<> migration_manager::announce_keyspace_drop(const sstring& ks_name, bool announce_locally)
{
    auto& db = get_local_storage_proxy().get_db().local();
    if (!db.has_keyspace(ks_name)) {
        throw exceptions::configuration_exception(sprint("Cannot drop non existing keyspace '%s'.", ks_name));
    }
    auto& keyspace = db.find_keyspace(ks_name);
    mlogger.info("Drop Keyspace '{}'", ks_name);
    auto&& mutations = db::schema_tables::make_drop_keyspace_mutations(keyspace.metadata(), api::new_timestamp());
    return announce(std::move(mutations), announce_locally);
}

future<> migration_manager::announce_column_family_drop(const sstring& ks_name,
                                                        const sstring& cf_name,
                                                        bool announce_locally)
{
    try {
        auto& db = get_local_storage_proxy().get_db().local();
        auto& old_cfm = db.find_column_family(ks_name, cf_name);
        auto& schema = old_cfm.schema();
        if (schema->is_view()) {
            throw exceptions::invalid_request_exception("Cannot use DROP TABLE on Materialized View");
        }
        auto&& views = old_cfm.views();
        if (views.size() > schema->all_indices().size()) {
            auto explicit_view_names = views
                                       | boost::adaptors::filtered([&old_cfm](const view_ptr& v) { return !old_cfm.get_index_manager().is_index(v); })
                                       | boost::adaptors::transformed([](const view_ptr& v) { return v->cf_name(); });
            throw exceptions::invalid_request_exception(sprint(
                        "Cannot drop table when materialized views still depend on it (%s.{%s})",
                        ks_name, ::join(", ", explicit_view_names)));
        }
        mlogger.info("Drop table '{}.{}'", schema->ks_name(), schema->cf_name());

        auto maybe_drop_secondary_indexes = make_ready_future<std::vector<mutation>>();
        if (!schema->all_indices().empty()) {
            auto builder = schema_builder(schema).without_indexes();
            maybe_drop_secondary_indexes = db::schema_tables::make_update_table_mutations(db.find_keyspace(ks_name).metadata(), schema, builder.build(), api::new_timestamp(), false);
        }

        return maybe_drop_secondary_indexes.then([announce_locally, ks_name, schema, &db, &old_cfm] (auto&& drop_si_mutations) {
            return db::schema_tables::make_drop_table_mutations(db.find_keyspace(ks_name).metadata(), schema, api::new_timestamp())
                .then([drop_si_mutations = std::move(drop_si_mutations), announce_locally] (auto&& mutations) mutable {
                    mutations.insert(mutations.end(), std::make_move_iterator(drop_si_mutations.begin()), std::make_move_iterator(drop_si_mutations.end()));
                    return announce(std::move(mutations), announce_locally);
                });
        });
    } catch (const no_such_column_family& e) {
        throw exceptions::configuration_exception(sprint("Cannot drop non existing table '%s' in keyspace '%s'.", cf_name, ks_name));
    }

}

future<> migration_manager::announce_type_drop(user_type dropped_type, bool announce_locally)
{
    auto& db = get_local_storage_proxy().get_db().local();
    auto&& keyspace = db.find_keyspace(dropped_type->_keyspace);
    mlogger.info("Drop User Type: {}", dropped_type->get_name_as_string());
    return db::schema_tables::make_drop_type_mutations(keyspace.metadata(), dropped_type, api::new_timestamp())
        .then([announce_locally] (auto&& mutations) {
            return announce(std::move(mutations), announce_locally);
        });
}

future<> migration_manager::announce_new_view(view_ptr view, bool announce_locally)
{
#if 0
    view.metadata.validate();
#endif
    auto& db = get_local_storage_proxy().get_db().local();
    try {
        auto&& keyspace = db.find_keyspace(view->ks_name()).metadata();
        if (keyspace->cf_meta_data().find(view->cf_name()) != keyspace->cf_meta_data().end()) {
            throw exceptions::already_exists_exception(view->ks_name(), view->cf_name());
        }
        mlogger.info("Create new view: {}", view);
        return db::schema_tables::make_create_view_mutations(keyspace, std::move(view), api::new_timestamp())
            .then([announce_locally] (auto&& mutations) {
                return announce(std::move(mutations), announce_locally);
            });
    } catch (const no_such_keyspace& e) {
        throw exceptions::configuration_exception(sprint("Cannot add view '%s' to non existing keyspace '%s'.", view->cf_name(), view->ks_name()));
    }
}

future<> migration_manager::announce_view_update(view_ptr view, bool announce_locally)
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
        return db::schema_tables::make_update_view_mutations(std::move(keyspace), view_ptr(old_view), std::move(view), api::new_timestamp(), true)
            .then([announce_locally] (auto&& mutations) {
                return announce(std::move(mutations), announce_locally);
            });
    } catch (const std::out_of_range& e) {
        throw exceptions::configuration_exception(sprint("Cannot update non existing materialized view '%s' in keyspace '%s'.",
                                                         view->cf_name(), view->ks_name()));
    }
}

future<> migration_manager::announce_view_drop(const sstring& ks_name,
                                               const sstring& cf_name,
                                               bool announce_locally)
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
        return db::schema_tables::make_drop_view_mutations(std::move(keyspace), view_ptr(std::move(view)), api::new_timestamp())
            .then([announce_locally] (auto&& mutations) {
                return announce(std::move(mutations), announce_locally);
            });
    } catch (const no_such_column_family& e) {
        throw exceptions::configuration_exception(sprint("Cannot drop non existing materialized view '%s' in keyspace '%s'.",
                                                         cf_name, ks_name));
    }
}

#if 0
public static void announceFunctionDrop(UDFunction udf, boolean announceLocally)
{
    mlogger.info(String.format("Drop scalar function overload '%s' args '%s'", udf.name(), udf.argTypes()));
    KSMetaData ksm = Schema.instance.getKSMetaData(udf.name().keyspace);
    announce(LegacySchemaTables.makeDropFunctionMutation(ksm, udf, FBUtilities.timestampMicros()), announceLocally);
}

public static void announceAggregateDrop(UDAggregate udf, boolean announceLocally)
{
    mlogger.info(String.format("Drop aggregate function overload '%s' args '%s'", udf.name(), udf.argTypes()));
    KSMetaData ksm = Schema.instance.getKSMetaData(udf.name().keyspace);
    announce(LegacySchemaTables.makeDropAggregateMutation(ksm, udf, FBUtilities.timestampMicros()), announceLocally);
}
#endif

/**
 * actively announce a new version to active hosts via rpc
 * @param schema The schema mutation to be applied
 */
future<> migration_manager::announce(mutation schema, bool announce_locally)
{
    std::vector<mutation> mutations;
    mutations.emplace_back(std::move(schema));
    return announce(std::move(mutations), announce_locally);
}

future<> migration_manager::announce(std::vector<mutation> mutations, bool announce_locally)
{
    if (announce_locally) {
        return db::schema_tables::merge_schema(get_storage_proxy(), std::move(mutations), false);
    } else {
        return announce(std::move(mutations));
    }
}

future<> migration_manager::push_schema_mutation(const gms::inet_address& endpoint, const std::vector<mutation>& schema)
{
    netw::messaging_service::msg_addr id{endpoint, 0};
    auto fm = std::vector<frozen_mutation>(schema.begin(), schema.end());
    return netw::get_local_messaging_service().send_definitions_update(id, std::move(fm));
}

// Returns a future on the local application of the schema
future<> migration_manager::announce(std::vector<mutation> schema) {
    auto f = db::schema_tables::merge_schema(get_storage_proxy(), schema);

    return do_with(std::move(schema), [live_members = gms::get_local_gossiper().get_live_members()](auto && schema) {
        return parallel_for_each(live_members.begin(), live_members.end(), [&schema](auto& endpoint) {
            // only push schema to nodes with known and equal versions
            if (endpoint != utils::fb_utilities::get_broadcast_address() &&
                netw::get_local_messaging_service().knows_version(endpoint) &&
                netw::get_local_messaging_service().get_raw_version(endpoint) ==
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
    return gms::get_gossiper().invoke_on(0, [version] (auto&& gossiper) {
        auto& ss = service::get_local_storage_service();
        mlogger.debug("Gossiping my schema version {}", version);
        return gossiper.add_local_application_state(gms::application_state::SCHEMA, ss.value_factory.schema(version));
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
static future<> maybe_sync(const schema_ptr& s, netw::messaging_service::msg_addr endpoint) {
    if (s->is_synced()) {
        return make_ready_future<>();
    }

    return s->registry_entry()->maybe_sync([s, endpoint] {
        auto merge = [gs = global_schema_ptr(s), endpoint] {
            schema_ptr s = gs.get();
            mlogger.debug("Syncing schema of {}.{} (v={}) with {}", s->ks_name(), s->cf_name(), s->version(), endpoint);
            return get_local_migration_manager().merge_schema_from(endpoint);
        };

        // Serialize schema sync by always doing it on shard 0.
        if (engine().cpu_id() == 0) {
            return merge();
        } else {
            return smp::submit_to(0, [gs = global_schema_ptr(s), endpoint, merge] {
                schema_ptr s = gs.get();
                schema_registry_entry& e = *s->registry_entry();
                return e.maybe_sync(merge);
            });
        }
    });
}

future<schema_ptr> get_schema_definition(table_schema_version v, netw::messaging_service::msg_addr dst) {
    return local_schema_registry().get_or_load(v, [dst] (table_schema_version v) {
        mlogger.debug("Requesting schema {} from {}", v, dst);
        auto& ms = netw::get_local_messaging_service();
        return ms.send_get_schema_version(dst, v);
    });
}

future<schema_ptr> get_schema_for_read(table_schema_version v, netw::messaging_service::msg_addr dst) {
    return get_schema_definition(v, dst);
}

future<schema_ptr> get_schema_for_write(table_schema_version v, netw::messaging_service::msg_addr dst) {
    return get_schema_definition(v, dst).then([dst] (schema_ptr s) {
        return maybe_sync(s, dst).then([s] {
            return s;
        });
    });
}

}
