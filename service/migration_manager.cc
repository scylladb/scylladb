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
 * Copyright 2015 Cloudius Systems
 *
 * Modified by Cloudius Systems
 */

#include "service/migration_manager.hh"

#include "message/messaging_service.hh"
#include "gms/gossiper.hh"

namespace service {

#if 0
public void register(IMigrationListener listener)
{
    listeners.add(listener);
}

public void unregister(IMigrationListener listener)
{
    listeners.remove(listener);
}

public void scheduleSchemaPull(InetAddress endpoint, EndpointState state)
{
    VersionedValue value = state.getApplicationState(ApplicationState.SCHEMA);

    if (!endpoint.equals(FBUtilities.getBroadcastAddress()) && value != null)
        maybeScheduleSchemaPull(UUID.fromString(value.value), endpoint);
}

/**
 * If versions differ this node sends request with local migration list to the endpoint
 * and expecting to receive a list of migrations to apply locally.
 */
private static void maybeScheduleSchemaPull(final UUID theirVersion, final InetAddress endpoint)
{
    if ((Schema.instance.getVersion() != null && Schema.instance.getVersion().equals(theirVersion)) || !shouldPullSchemaFrom(endpoint))
    {
        logger.debug("Not pulling schema because versions match or shouldPullSchemaFrom returned false");
        return;
    }

    if (Schema.emptyVersion.equals(Schema.instance.getVersion()) || runtimeMXBean.getUptime() < MIGRATION_DELAY_IN_MS)
    {
        // If we think we may be bootstrapping or have recently started, submit MigrationTask immediately
        logger.debug("Submitting migration task for {}", endpoint);
        submitMigrationTask(endpoint);
    }
    else
    {
        // Include a delay to make sure we have a chance to apply any changes being
        // pushed out simultaneously. See CASSANDRA-5025
        Runnable runnable = new Runnable()
        {
            public void run()
            {
                // grab the latest version of the schema since it may have changed again since the initial scheduling
                EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
                if (epState == null)
                {
                    logger.debug("epState vanished for {}, not submitting migration task", endpoint);
                    return;
                }
                VersionedValue value = epState.getApplicationState(ApplicationState.SCHEMA);
                UUID currentVersion = UUID.fromString(value.value);
                if (Schema.instance.getVersion().equals(currentVersion))
                {
                    logger.debug("not submitting migration task for {} because our versions match", endpoint);
                    return;
                }
                logger.debug("submitting migration task for {}", endpoint);
                submitMigrationTask(endpoint);
            }
        };
        ScheduledExecutors.optionalTasks.schedule(runnable, MIGRATION_DELAY_IN_MS, TimeUnit.MILLISECONDS);
    }
}

private static Future<?> submitMigrationTask(InetAddress endpoint)
{
    /*
     * Do not de-ref the future because that causes distributed deadlock (CASSANDRA-3832) because we are
     * running in the gossip stage.
     */
    return StageManager.getStage(Stage.MIGRATION).submit(new MigrationTask(endpoint));
}

private static boolean shouldPullSchemaFrom(InetAddress endpoint)
{
    /*
     * Don't request schema from nodes with a differnt or unknonw major version (may have incompatible schema)
     * Don't request schema from fat clients
     */
    return MessagingService.instance().knowsVersion(endpoint)
            && MessagingService.instance().getRawVersion(endpoint) == MessagingService.current_version
            && !Gossiper.instance.isGossipOnlyMember(endpoint);
}

public static boolean isReadyForBootstrap()
{
    return ((ThreadPoolExecutor) StageManager.getStage(Stage.MIGRATION)).getActiveCount() == 0;
}

public void notifyCreateKeyspace(KSMetaData ksm)
{
    for (IMigrationListener listener : listeners)
        listener.onCreateKeyspace(ksm.name);
}

public void notifyCreateColumnFamily(CFMetaData cfm)
{
    for (IMigrationListener listener : listeners)
        listener.onCreateColumnFamily(cfm.ksName, cfm.cfName);
}

public void notifyCreateUserType(UserType ut)
{
    for (IMigrationListener listener : listeners)
        listener.onCreateUserType(ut.keyspace, ut.getNameAsString());
}

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

public void notifyUpdateKeyspace(KSMetaData ksm)
{
    for (IMigrationListener listener : listeners)
        listener.onUpdateKeyspace(ksm.name);
}

public void notifyUpdateColumnFamily(CFMetaData cfm)
{
    for (IMigrationListener listener : listeners)
        listener.onUpdateColumnFamily(cfm.ksName, cfm.cfName);
}

public void notifyUpdateUserType(UserType ut)
{
    for (IMigrationListener listener : listeners)
        listener.onUpdateUserType(ut.keyspace, ut.getNameAsString());
}

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

public void notifyDropKeyspace(KSMetaData ksm)
{
    for (IMigrationListener listener : listeners)
        listener.onDropKeyspace(ksm.name);
}

public void notifyDropColumnFamily(CFMetaData cfm)
{
    for (IMigrationListener listener : listeners)
        listener.onDropColumnFamily(cfm.ksName, cfm.cfName);
}

public void notifyDropUserType(UserType ut)
{
    for (IMigrationListener listener : listeners)
        listener.onDropUserType(ut.keyspace, ut.getNameAsString());
}

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

future<>migration_manager::announce_new_keyspace(distributed<service::storage_proxy>& proxy, lw_shared_ptr<keyspace_metadata> ksm, bool announce_locally)
{
    return announce_new_keyspace(proxy, ksm, db_clock::now_in_usecs(), announce_locally);
}

future<> migration_manager::announce_new_keyspace(distributed<service::storage_proxy>& proxy, lw_shared_ptr<keyspace_metadata> ksm, api::timestamp_type timestamp, bool announce_locally)
{
#if 0
    ksm.validate();
#endif
    if (proxy.local().get_db().local().has_keyspace(ksm->name())) {
        throw exceptions::already_exists_exception{ksm->name()};
    }
#if 0
    logger.info(String.format("Create new Keyspace: %s", ksm));
#endif
    auto mutations = db::legacy_schema_tables::make_create_keyspace_mutations(ksm, timestamp);
    return announce(proxy, std::move(mutations), announce_locally);
}

future<> migration_manager::announce_new_column_family(distributed<service::storage_proxy>& proxy, schema_ptr cfm, bool announce_locally) {
#if 0
    cfm.validate();
#endif
    try {
        auto& db = proxy.local().get_db().local();
        auto&& keyspace = db.find_keyspace(cfm->ks_name());
        if (db.has_schema(cfm->ks_name(), cfm->cf_name())) {
            throw exceptions::already_exists_exception(cfm->ks_name(), cfm->cf_name());
        }
#if 0
        logger.info(String.format("Create new table: %s", cfm));
#endif
        auto mutations = db::legacy_schema_tables::make_create_table_mutations(keyspace.metadata(), cfm, db_clock::now_in_usecs());
        return announce(proxy, std::move(mutations), announce_locally);
    } catch (const no_such_keyspace& e) {
        throw exceptions::configuration_exception(sprint("Cannot add table '%s' to non existing keyspace '%s'.", cfm->cf_name(), cfm->ks_name()));
    }
}

future<> migration_manager::announce_column_family_update(distributed<service::storage_proxy>& proxy, schema_ptr cfm, bool from_thrift, bool announce_locally) {
    warn(unimplemented::cause::MIGRATIONS);
    return make_ready_future<>();
#if 0
    cfm.validate();

    CFMetaData oldCfm = Schema.instance.getCFMetaData(cfm.ksName, cfm.cfName);
    if (oldCfm == null)
        throw new ConfigurationException(String.format("Cannot update non existing table '%s' in keyspace '%s'.", cfm.cfName, cfm.ksName));
    KSMetaData ksm = Schema.instance.getKSMetaData(cfm.ksName);

    oldCfm.validateCompatility(cfm);

    logger.info(String.format("Update table '%s/%s' From %s To %s", cfm.ksName, cfm.cfName, oldCfm, cfm));
    announce(LegacySchemaTables.makeUpdateTableMutation(ksm, oldCfm, cfm, FBUtilities.timestampMicros(), fromThrift), announceLocally);
#endif
}

#if 0
public static void announceNewType(UserType newType, boolean announceLocally)
{
    KSMetaData ksm = Schema.instance.getKSMetaData(newType.keyspace);
    announce(LegacySchemaTables.makeCreateTypeMutation(ksm, newType, FBUtilities.timestampMicros()), announceLocally);
}

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

public static void announceTypeUpdate(UserType updatedType, boolean announceLocally)
{
    announceNewType(updatedType, announceLocally);
}
#endif

future<> migration_manager::announce_keyspace_drop(distributed<service::storage_proxy>& proxy, const sstring& ks_name, bool announce_locally)
{
    try {
        auto& db = proxy.local().get_db().local();
        /*auto&& keyspace = */db.find_keyspace(ks_name);
#if 0
        logger.info(String.format("Drop Keyspace '%s'", oldKsm.name));
        announce(LegacySchemaTables.makeDropKeyspaceMutation(oldKsm, FBUtilities.timestampMicros()), announceLocally);
#endif
        // FIXME
        throw std::runtime_error("not implemented");
    } catch (const no_such_keyspace& e) {
        throw exceptions::configuration_exception(sprint("Cannot drop non existing keyspace '%s'.", ks_name));
    }
}

future<> migration_manager::announce_column_family_drop(distributed<service::storage_proxy>& proxy,
                                                        const sstring& ks_name,
                                                        const sstring& cf_name,
                                                        bool announce_locally)
{
    try {
        auto& db = proxy.local().get_db().local();
        /*auto&& cfm = */db.find_schema(ks_name, cf_name);
        /*auto&& ksm = */db.find_keyspace(ks_name);
#if 0
        logger.info(String.format("Drop table '%s/%s'", oldCfm.ksName, oldCfm.cfName));
        announce(LegacySchemaTables.makeDropTableMutation(ksm, oldCfm, FBUtilities.timestampMicros()), announceLocally);
#endif
        // FIXME
        throw std::runtime_error("not implemented");
    } catch (const no_such_column_family& e) {
        throw exceptions::configuration_exception(sprint("Cannot drop non existing table '%s' in keyspace '%s'.", cf_name, ks_name));
    }
}

#if 0
public static void announceTypeDrop(UserType droppedType)
{
    announceTypeDrop(droppedType, false);
}

public static void announceTypeDrop(UserType droppedType, boolean announceLocally)
{
    KSMetaData ksm = Schema.instance.getKSMetaData(droppedType.keyspace);
    announce(LegacySchemaTables.dropTypeFromSchemaMutation(ksm, droppedType, FBUtilities.timestampMicros()), announceLocally);
}

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
future<> migration_manager::announce(distributed<service::storage_proxy>& proxy, mutation schema, bool announce_locally)
{
    std::vector<mutation> mutations;
    mutations.emplace_back(std::move(schema));
    return announce(proxy, std::move(mutations), announce_locally);
}

future<> migration_manager::announce(distributed<service::storage_proxy>& proxy, std::vector<mutation> mutations, bool announce_locally)
{
    if (announce_locally) {
        return db::legacy_schema_tables::merge_schema(proxy.local(), std::move(mutations), false);
    } else {
        return announce(proxy, std::move(mutations));
    }
}

future<> migration_manager::push_schema_mutation(const gms::inet_address& endpoint, const std::vector<mutation>& schema)
{
    net::messaging_service::shard_id id{endpoint, 0};
    auto fm = std::vector<frozen_mutation>(schema.begin(), schema.end());
    return net::get_local_messaging_service().send_definitions_update(id, std::move(fm));
}

// Returns a future on the local application of the schema
future<> migration_manager::announce(distributed<service::storage_proxy>& proxy, std::vector<mutation> schema)
{
    return gms::get_live_members().then([&proxy, schema = std::move(schema)](std::set<gms::inet_address> live_members) {
        return do_with(std::move(schema), [&proxy, live_members] (auto&& schema) {
            return parallel_for_each(live_members.begin(), live_members.end(), [&schema] (auto& endpoint) {
                // only push schema to nodes with known and equal versions
                if (endpoint != utils::fb_utilities::get_broadcast_address() &&
                        net::get_local_messaging_service().knows_version(endpoint) &&
                        net::get_local_messaging_service().get_raw_version(endpoint) == net::messaging_service::current_version) {
                    return push_schema_mutation(endpoint, schema);
                } else {
                    return make_ready_future<>();
                }
            }).then_wrapped([] (future<> f) {
                try {
                    f.get();
                } catch (std::exception& ex) {
                    std::cout << "announce error " << ex.what() << "\n";
                }
            }).then([&proxy, &schema] {
                return db::legacy_schema_tables::merge_schema(proxy.local(), std::move(schema));
            });
        });
    });
}

#if 0
/**
 * Announce my version passively over gossip.
 * Used to notify nodes as they arrive in the cluster.
 *
 * @param version The schema version to announce
 */
public static void passiveAnnounce(UUID version)
{
    Gossiper.instance.addLocalApplicationState(ApplicationState.SCHEMA, StorageService.instance.valueFactory.schema(version));
    logger.debug("Gossiping my schema version {}", version);
}

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

}
