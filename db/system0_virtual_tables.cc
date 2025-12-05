/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "db/system0_virtual_tables.hh"
#include "db/system_keyspace.hh"
#include "db/virtual_table.hh"
#include "replica/database.hh"
#include "replica/tablets.hh"
#include "schema/schema_builder.hh"
#include "cql3/query_processor.hh"
#include "mutation/frozen_mutation.hh"
#include "types/types.hh"
#include "utils/log.hh"
#include <seastar/core/coroutine.hh>

namespace db {

namespace {

static constexpr auto SYSTEM0_KEYSPACE_NAME = "system0";

logging::logger sys0log("system0_virtual_tables");

// Virtual table that mirrors system.topology but allows writes via group0
class system0_topology_table : public memtable_filling_virtual_table {
private:
    cql3::query_processor& _qp;

public:
    explicit system0_topology_table(cql3::query_processor& qp)
        : memtable_filling_virtual_table(build_schema())
        , _qp(qp)
    {}

    static schema_ptr build_schema() {
        // Use the same schema as system.topology but in system0 keyspace
        auto id = generate_legacy_id(SYSTEM0_KEYSPACE_NAME, system_keyspace::TOPOLOGY);
        return schema_builder(SYSTEM0_KEYSPACE_NAME, system_keyspace::TOPOLOGY, std::optional(id))
            .with_column("key", utf8_type, column_kind::partition_key)
            .with_column("host_id", uuid_type, column_kind::clustering_key)
            .with_column("datacenter", utf8_type)
            .with_column("rack", utf8_type)
            .with_column("tokens", set_type_impl::get_instance(utf8_type, true))
            .with_column("node_state", utf8_type)
            .with_column("release_version", utf8_type)
            .with_column("topology_request", utf8_type)
            .with_column("replaced_id", uuid_type)
            .with_column("rebuild_option", utf8_type)
            .with_column("num_tokens", int32_type)
            .with_column("tokens_string", utf8_type)
            .with_column("shard_count", int32_type)
            .with_column("ignore_msb", int32_type)
            .with_column("cleanup_status", utf8_type)
            .with_column("supported_features", set_type_impl::get_instance(utf8_type, true))
            .with_column("request_id", timeuuid_type)
            .with_column("ignore_nodes", set_type_impl::get_instance(uuid_type, true), column_kind::static_column)
            .with_column("new_cdc_generation_data_uuid", timeuuid_type, column_kind::static_column)
            .with_column("new_keyspace_rf_change_ks_name", utf8_type, column_kind::static_column)
            .with_column("new_keyspace_rf_change_data", map_type_impl::get_instance(utf8_type, utf8_type, false), column_kind::static_column)
            .with_column("version", long_type, column_kind::static_column)
            .with_column("fence_version", long_type, column_kind::static_column)
            .with_column("transition_state", utf8_type, column_kind::static_column)
            .with_column("committed_cdc_generations", set_type_impl::get_instance(cdc_generation_ts_id_type, true), column_kind::static_column)
            .with_column("unpublished_cdc_generations", set_type_impl::get_instance(cdc_generation_ts_id_type, true), column_kind::static_column)
            .with_column("global_topology_request", utf8_type, column_kind::static_column)
            .with_column("global_topology_request_id", timeuuid_type, column_kind::static_column)
            .with_column("enabled_features", set_type_impl::get_instance(utf8_type, true), column_kind::static_column)
            .with_column("session", uuid_type, column_kind::static_column)
            .with_column("tablet_balancing_enabled", boolean_type, column_kind::static_column)
            .with_column("upgrade_state", utf8_type, column_kind::static_column)
            .with_column("global_requests", set_type_impl::get_instance(timeuuid_type, true), column_kind::static_column)
            .set_comment("Virtual table for updating system.topology via group0")
            .with_hash_version()
            .build();
    }

    future<> execute(std::function<void(mutation)> mutation_sink) override {
        // For reads, we mirror the actual system.topology table
        // This is a simplified placeholder implementation
        sys0log.debug("system0.topology: read operation");
        co_return;
    }

    future<> apply(const frozen_mutation& fm) override {
        sys0log.info("system0.topology: received write operation");
        
        // Convert mutation from system0.topology schema to system.topology schema
        const mutation m = fm.unfreeze(_s);
        
        // Re-freeze the mutation with the system.topology schema
        auto system_topology_schema = system_keyspace::topology();
        mutation target_m(system_topology_schema, m.key());
        target_m.partition() = m.partition();
        
        // TODO: Submit mutation to group0 via raft_group0_client
        // For now, just log a warning
        sys0log.warn("system0.topology: write operations require group0 integration (not yet implemented)");
        
        co_return;
    }
};

// Virtual table that mirrors system.tablets but allows writes via group0
class system0_tablets_table : public memtable_filling_virtual_table {
private:
    cql3::query_processor& _qp;

public:
    explicit system0_tablets_table(cql3::query_processor& qp)
        : memtable_filling_virtual_table(build_schema())
        , _qp(qp)
    {}

    static schema_ptr build_schema() {
        // Create a simple schema for tablets in system0 keyspace
        // This mirrors system.tablets structure
        auto id = generate_legacy_id(SYSTEM0_KEYSPACE_NAME, system_keyspace::TABLETS);
        auto replica_set_type = replica::get_replica_set_type();
        
        return schema_builder(SYSTEM0_KEYSPACE_NAME, system_keyspace::TABLETS, id)
                .with_column("table_id", uuid_type, column_kind::partition_key)
                .with_column("tablet_count", int32_type, column_kind::static_column)
                .with_column("keyspace_name", utf8_type, column_kind::static_column)
                .with_column("table_name", utf8_type, column_kind::static_column)
                .with_column("last_token", long_type, column_kind::clustering_key)
                .with_column("replicas", replica_set_type)
                .with_column("new_replicas", replica_set_type)
                .with_column("stage", utf8_type)
                .with_column("transition", utf8_type)
                .with_column("session", uuid_type)
                .set_comment("Virtual table for updating system.tablets via group0")
                .with_hash_version()
                .build();
    }

    future<> execute(std::function<void(mutation)> mutation_sink) override {
        // For reads, we mirror the actual system.tablets table
        // This is a simplified placeholder implementation
        sys0log.debug("system0.tablets: read operation");
        co_return;
    }

    future<> apply(const frozen_mutation& fm) override {
        sys0log.info("system0.tablets: received write operation");
        
        // Convert mutation from system0.tablets schema to system.tablets schema
        const mutation m = fm.unfreeze(_s);
        
        // Re-freeze the mutation with the system.tablets schema
        auto system_tablets_schema = system_keyspace::tablets();
        mutation target_m(system_tablets_schema, m.key());
        target_m.partition() = m.partition();
        
        // TODO: Submit mutation to group0 via raft_group0_client
        // For now, just log a warning
        sys0log.warn("system0.tablets: write operations require group0 integration (not yet implemented)");
        
        co_return;
    }
};

} // anonymous namespace

future<> initialize_system0_virtual_tables(
        sharded<service::raft_group_registry>& dist_raft_gr,
        sharded<db::system_keyspace>& sys_ks,
        sharded<cql3::query_processor>& qp) {
    
    auto& virtual_tables_registry = sys_ks.local().get_virtual_tables_registry();
    auto& virtual_tables = *virtual_tables_registry;
    auto& db = sys_ks.local().local_db();

    auto add_table = [&] (std::unique_ptr<virtual_table>&& tbl) -> future<> {
        auto schema = tbl->schema();
        virtual_tables[schema->id()] = std::move(tbl);
        
        // Add the table as a local system table (similar to regular virtual tables)
        // Note: This creates tables in the system0 keyspace which is treated as internal
        co_await db.add_column_family_and_make_directory(schema, replica::database::is_new_cf::yes);
        
        auto& cf = db.find_column_family(schema);
        cf.mark_ready_for_writes(nullptr);
        auto& vt = virtual_tables[schema->id()];
        cf.set_virtual_reader(vt->as_mutation_source());
        cf.set_virtual_writer([&vt = *vt] (const frozen_mutation& m) { return vt.apply(m); });
    };

    // Add system0 virtual tables
    co_await add_table(std::make_unique<system0_topology_table>(qp.local()));
    co_await add_table(std::make_unique<system0_tablets_table>(qp.local()));
    
    sys0log.info("system0 virtual tables initialized");
}

} // namespace db
