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

namespace service {

static logging::logger logger("migration_manager");

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
    return make_ready_future<>();
}

void migration_manager::init_messaging_service()
{
    auto& ms = net::get_local_messaging_service();
    ms.register_definitions_update([this] (const rpc::client_info& cinfo, std::vector<frozen_mutation> m) {
        auto src = net::messaging_service::get_source(cinfo);
        do_with(std::move(m), get_local_shared_storage_proxy(), [src] (const std::vector<frozen_mutation>& mutations, shared_ptr<storage_proxy>& p) {
            return service::get_local_migration_manager().merge_schema_from(src, mutations);
        }).then_wrapped([src] (auto&& f) {
            if (f.failed()) {
                logger.error("Failed to update definitions from {}: {}", src, f.get_exception());
            } else {
                logger.debug("Applied definitions update from {}.", src);
            }
        });
        return net::messaging_service::no_wait();
    });
    ms.register_migration_request([this] () {
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
    auto& ms = net::get_local_messaging_service();
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
    const auto& value = state.get_application_state(gms::application_state::SCHEMA);

    if (endpoint != utils::fb_utilities::get_broadcast_address() && value) {
        return maybe_schedule_schema_pull(utils::UUID{value->value}, endpoint);
    }
    return make_ready_future<>();
}

bool migration_manager::is_ready_for_bootstrap() {
    auto our_version = get_local_storage_proxy().get_db().local().get_version();
    bool match = false;
    for (auto& x : gms::get_local_gossiper().endpoint_state_map) {
        auto& endpoint = x.first;
        auto& eps = x.second;
        if (endpoint == utils::fb_utilities::get_broadcast_address() || !eps.is_alive()) {
            continue;
        }
        auto schema = eps.get_application_state(gms::application_state::SCHEMA);
        if (!schema) {
            return false;
        }
        utils::UUID remote_version{schema->value};
        if (our_version != remote_version) {
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
        logger.debug("Not pulling schema because versions match or shouldPullSchemaFrom returned false");
        return make_ready_future<>();
    }

    if (db.get_version() == database::empty_version || runtime::get_uptime() < migration_delay) {
        // If we think we may be bootstrapping or have recently started, submit MigrationTask immediately
        logger.debug("Submitting migration task for {}", endpoint);
        return submit_migration_task(endpoint);
    } else {
        // Include a delay to make sure we have a chance to apply any changes being
        // pushed out simultaneously. See CASSANDRA-5025
        return sleep(migration_delay).then([this, &proxy, endpoint] {
            // grab the latest version of the schema since it may have changed again since the initial scheduling
            auto& gossiper = gms::get_local_gossiper();
            auto ep_state = gossiper.get_endpoint_state_for_endpoint(endpoint);
            if (!ep_state) {
                logger.debug("epState vanished for {}, not submitting migration task", endpoint);
                return make_ready_future<>();
            }
            const auto& value = ep_state->get_application_state(gms::application_state::SCHEMA);
            utils::UUID current_version{value->value};
            auto& db = proxy.get_db().local();
            if (db.get_version() == current_version) {
                logger.debug("not submitting migration task for {} because our versions match", endpoint);
                return make_ready_future<>();
            }
            logger.debug("submitting migration task for {}", endpoint);
            return submit_migration_task(endpoint);
        });
    }
}

future<> migration_manager::submit_migration_task(const gms::inet_address& endpoint)
{
    return service::migration_task::run_may_throw(get_storage_proxy(), endpoint);
}

future<> migration_manager::merge_schema_from(net::messaging_service::msg_addr id)
{
    auto& ms = net::get_local_messaging_service();
    return ms.send_migration_request(std::move(id)).then([this, id] (std::vector<frozen_mutation> mutations) {
        return do_with(std::move(mutations), [this, id] (auto&& mutations) {
            return this->merge_schema_from(id, mutations);
        });
    });
}

future<> migration_manager::merge_schema_from(net::messaging_service::msg_addr src, const std::vector<frozen_mutation>& mutations)
{
    logger.debug("Applying schema mutations from {}", src);
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

bool migration_manager::should_pull_schema_from(const gms::inet_address& endpoint)
{
    /*
     * Don't request schema from nodes with a differnt or unknonw major version (may have incompatible schema)
     * Don't request schema from fat clients
     */
    auto& ms = net::get_local_messaging_service();
    return ms.knows_version(endpoint)
            && ms.get_raw_version(endpoint) == net::messaging_service::current_version
            && !gms::get_local_gossiper().is_gossip_only_member(endpoint);
}

future<> migration_manager::notify_create_keyspace(const lw_shared_ptr<keyspace_metadata>& ksm) {
    return seastar::async([this, ksm] {
        auto&& name = ksm->name();
        for (auto&& listener : _listeners) {
            try {
                listener->on_create_keyspace(name);
            } catch (...) {
                logger.warn("Create keyspace notification failed {}: {}", name, std::current_exception());
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
                logger.warn("Create column family notification failed {}.{}: {}", ks_name, cf_name, std::current_exception());
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
                logger.warn("Create user type notification failed {}.{}: {}", ks_name, type_name, std::current_exception());
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
                logger.warn("Update keyspace notification failed {}: {}", name, std::current_exception());
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
                logger.warn("Update column family notification failed {}.{}: {}", ks_name, cf_name, std::current_exception());
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
                logger.warn("Update user type notification failed {}.{}: {}", ks_name, type_name, std::current_exception());
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
                logger.warn("Drop keyspace notification failed {}: {}", ks_name, std::current_exception());
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
                logger.warn("Drop column family notification failed {}.{}: {}", ks_name, cf_name, std::current_exception());
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
                logger.warn("Drop user type notification failed {}.{}: {}", ks_name, type_name, std::current_exception());
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
    logger.info("Update Keyspace: {}", ksm);
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
    logger.info("Create new Keyspace: {}", ksm);
    auto mutations = db::schema_tables::make_create_keyspace_mutations(ksm, timestamp);
    return announce(std::move(mutations), announce_locally);
}

future<> migration_manager::announce_new_column_family(schema_ptr cfm, bool announce_locally) {
#if 0
    cfm.validate();
#endif
    try {
        auto& db = get_local_storage_proxy().get_db().local();
        auto&& keyspace = db.find_keyspace(cfm->ks_name());
        if (db.has_schema(cfm->ks_name(), cfm->cf_name())) {
            throw exceptions::already_exists_exception(cfm->ks_name(), cfm->cf_name());
        }
        logger.info("Create new ColumnFamily: {}", cfm);
        auto mutations = db::schema_tables::make_create_table_mutations(keyspace.metadata(), cfm, api::new_timestamp());
        return announce(std::move(mutations), announce_locally);
    } catch (const no_such_keyspace& e) {
        throw exceptions::configuration_exception(sprint("Cannot add table '%s' to non existing keyspace '%s'.", cfm->cf_name(), cfm->ks_name()));
    }
}

future<> migration_manager::announce_column_family_update(schema_ptr cfm, bool from_thrift, bool announce_locally) {
    warn(unimplemented::cause::VALIDATION);
#if 0
    cfm.validate();
#endif
    try {
        auto& db = get_local_storage_proxy().get_db().local();
        auto&& old_schema = db.find_column_family(cfm->ks_name(), cfm->cf_name()).schema(); // FIXME: Should we lookup by id?
#if 0
        oldCfm.validateCompatility(cfm);
#endif
        logger.info("Update table '{}.{}' From {} To {}", cfm->ks_name(), cfm->cf_name(), *old_schema, *cfm);
        auto&& keyspace = db.find_keyspace(cfm->ks_name());
        auto mutations = db::schema_tables::make_update_table_mutations(keyspace.metadata(), old_schema, cfm, api::new_timestamp(), from_thrift);
        return announce(std::move(mutations), announce_locally);
    } catch (const no_such_column_family& e) {
        throw exceptions::configuration_exception(sprint("Cannot update non existing table '%s' in keyspace '%s'.",
                                                         cfm->cf_name(), cfm->ks_name()));
    }
}

static future<> do_announce_new_type(user_type new_type, bool announce_locally) {
    auto& db = get_local_storage_proxy().get_db().local();
    auto&& keyspace = db.find_keyspace(new_type->_keyspace);
    auto mutations = db::schema_tables::make_create_type_mutations(keyspace.metadata(), new_type, api::new_timestamp());
    return migration_manager::announce(std::move(mutations), announce_locally);
}

future<> migration_manager::announce_new_type(user_type new_type, bool announce_locally) {
    logger.info("Create new User Type: {}", new_type->get_name_as_string());
    return do_announce_new_type(new_type, announce_locally);
}

future<> migration_manager::announce_type_update(user_type updated_type, bool announce_locally) {
    logger.info("Update User Type: {}", updated_type->get_name_as_string());
    return do_announce_new_type(updated_type, announce_locally);
}

#if 0
public static void announceNewFunction(UDFunction udf, boolean announceLocally)
{
    logger.info(String.format("Create scalar function '%s'", udf.name()));
    KSMetaData ksm = Schema.instance.getKSMetaData(udf.name().keyspace);
    announce(LegacySchemaTables.makeCreateFunctionMutation(ksm, udf, FBUtilities.timestampMicros()), announceLocally);
}

public static void announceNewAggregate(UDAggregate udf, boolean announceLocally)
{
    logger.info(String.format("Create aggregate function '%s'", udf.name()));
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

    logger.info(String.format("Update Keyspace '%s' From %s To %s", ksm.name, oldKsm, ksm));
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

    logger.info(String.format("Update table '%s/%s' From %s To %s", cfm.ksName, cfm.cfName, oldCfm, cfm));
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
#if 0
    logger.info(String.format("Drop Keyspace '%s'", oldKsm.name));
#endif
    auto&& mutations = db::schema_tables::make_drop_keyspace_mutations(keyspace.metadata(), api::new_timestamp());
    return announce(std::move(mutations), announce_locally);
}

future<> migration_manager::announce_column_family_drop(const sstring& ks_name,
                                                        const sstring& cf_name,
                                                        bool announce_locally)
{
    try {
        auto& db = get_local_storage_proxy().get_db().local();
        auto&& old_cfm = db.find_schema(ks_name, cf_name);
        auto&& keyspace = db.find_keyspace(ks_name);
        logger.info("Drop table '{}.{}'", old_cfm->ks_name(), old_cfm->cf_name());
        auto mutations = db::schema_tables::make_drop_table_mutations(keyspace.metadata(), old_cfm, api::new_timestamp());
        return announce(std::move(mutations), announce_locally);
    } catch (const no_such_column_family& e) {
        throw exceptions::configuration_exception(sprint("Cannot drop non existing table '%s' in keyspace '%s'.", cf_name, ks_name));
    }
}

future<> migration_manager::announce_type_drop(user_type dropped_type, bool announce_locally)
{
    auto& db = get_local_storage_proxy().get_db().local();
    auto&& keyspace = db.find_keyspace(dropped_type->_keyspace);
    logger.info("Drop User Type: {}", dropped_type->get_name_as_string());
    auto mutations = db::schema_tables::make_drop_type_mutations(keyspace.metadata(), dropped_type, api::new_timestamp());
    return announce(std::move(mutations), announce_locally);
}

#if 0
public static void announceFunctionDrop(UDFunction udf, boolean announceLocally)
{
    logger.info(String.format("Drop scalar function overload '%s' args '%s'", udf.name(), udf.argTypes()));
    KSMetaData ksm = Schema.instance.getKSMetaData(udf.name().keyspace);
    announce(LegacySchemaTables.makeDropFunctionMutation(ksm, udf, FBUtilities.timestampMicros()), announceLocally);
}

public static void announceAggregateDrop(UDAggregate udf, boolean announceLocally)
{
    logger.info(String.format("Drop aggregate function overload '%s' args '%s'", udf.name(), udf.argTypes()));
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
    net::messaging_service::msg_addr id{endpoint, 0};
    auto fm = std::vector<frozen_mutation>(schema.begin(), schema.end());
    return net::get_local_messaging_service().send_definitions_update(id, std::move(fm));
}

// Returns a future on the local application of the schema
future<> migration_manager::announce(std::vector<mutation> schema) {
    auto f = db::schema_tables::merge_schema(get_storage_proxy(), schema);

    return do_with(std::move(schema), [live_members = gms::get_local_gossiper().get_live_members()](auto && schema) {
        return parallel_for_each(live_members.begin(), live_members.end(), [&schema](auto& endpoint) {
            // only push schema to nodes with known and equal versions
            if (endpoint != utils::fb_utilities::get_broadcast_address() &&
                net::get_local_messaging_service().knows_version(endpoint) &&
                net::get_local_messaging_service().get_raw_version(endpoint) ==
                net::messaging_service::current_version) {
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
        logger.debug("Gossiping my schema version {}", version);
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
    logger.info("Starting local schema reset...");

    logger.debug("Truncating schema tables...");

    LegacySchemaTables.truncateSchemaTables();

    logger.debug("Clearing local schema keyspace definitions...");

    Schema.instance.clear();

    Set<InetAddress> liveEndpoints = Gossiper.instance.getLiveMembers();
    liveEndpoints.remove(FBUtilities.getBroadcastAddress());

    // force migration if there are nodes around
    for (InetAddress node : liveEndpoints)
    {
        if (shouldPullSchemaFrom(node))
        {
            logger.debug("Requesting schema from {}", node);
            FBUtilities.waitOnFuture(submitMigrationTask(node));
            break;
        }
    }

    logger.info("Local schema reset is complete.");
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
// FIXME: Avoid the sync if the source was/is synced by schema_tables::merge_schema().
static future<> maybe_sync(const schema_ptr& s, net::messaging_service::msg_addr endpoint) {
    if (s->is_synced()) {
        return make_ready_future<>();
    }

    // Serialize schema sync by always doing it on shard 0.
    return smp::submit_to(0, [gs = global_schema_ptr(s), endpoint] {
        schema_ptr s = gs.get();
        schema_registry_entry& e = *s->registry_entry();
        return e.maybe_sync([endpoint, s] {
            logger.debug("Syncing schema of {}.{} (v={}) with {}", s->ks_name(), s->cf_name(), s->version(), endpoint);
            return get_local_migration_manager().merge_schema_from(endpoint);
        });
    });
}

future<schema_ptr> get_schema_definition(table_schema_version v, net::messaging_service::msg_addr dst) {
    return local_schema_registry().get_or_load(v, [dst] (table_schema_version v) {
        logger.debug("Requesting schema {} from {}", v, dst);
        auto& ms = net::get_local_messaging_service();
        return ms.send_get_schema_version(dst, v);
    });
}

future<schema_ptr> get_schema_for_read(table_schema_version v, net::messaging_service::msg_addr dst) {
    return get_schema_definition(v, dst);
}

future<schema_ptr> get_schema_for_write(table_schema_version v, net::messaging_service::msg_addr dst) {
    return get_schema_definition(v, dst).then([dst] (schema_ptr s) {
        return maybe_sync(s, dst).then([s] {
            return s;
        });
    });
}

}
