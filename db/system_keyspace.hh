/*
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <cstdint>
#include <optional>
#include <unordered_map>
#include <utility>
#include <vector>
#include "gms/gossiper.hh"
#include "schema/schema_fwd.hh"
#include "utils/UUID.hh"
#include "query-result-set.hh"
#include "db_clock.hh"
#include "mutation_query.hh"
#include "system_keyspace_view_types.hh"
#include "sstables/sstables_registry.hh"
#include <seastar/core/distributed.hh>
#include "cdc/generation_id.hh"
#include "locator/host_id.hh"
#include "virtual_tables.hh"
#include "types/types.hh"

namespace sstables {
    struct entry_descriptor;
    class generation_type;
    enum class sstable_state;
}

namespace service {

class storage_service;
class raft_group_registry;
struct topology;
struct topology_features;

namespace paxos {
    class paxos_state;
    class proposal;
} // namespace service::paxos

struct topology_request_state;

class group0_guard;
}

namespace netw {
    class messaging_service;
}

namespace cql3 {
    class query_processor;
    class untyped_result_set;
}

namespace gms {
    class inet_address;
    class feature;
    class feature_service;
}

namespace locator {
    class effective_replication_map_factory;
    class endpoint_dc_rack;
} // namespace locator

namespace gms {
    class gossiper;
}

namespace cdc {
    class topology_description;
}

namespace cql3 {
    class untyped_result_set_row;
}

bool is_system_keyspace(std::string_view ks_name);

namespace db {

sstring system_keyspace_name();

class system_keyspace;
namespace schema_tables {
future<column_mapping> get_column_mapping(db::system_keyspace& sys_ks, ::table_id table_id, table_schema_version version);
future<bool> column_mapping_exists(db::system_keyspace& sys_ks, table_id table_id, table_schema_version version);
future<> drop_column_mapping(db::system_keyspace& sys_ks, table_id table_id, table_schema_version version);
}

class config;
struct local_cache;

using system_keyspace_view_name = std::pair<sstring, sstring>;
class system_keyspace_view_build_progress;

struct replay_position;
typedef std::vector<db::replay_position> replay_positions;


struct compaction_history_entry {
    utils::UUID id;
    sstring ks;
    sstring cf;
    int64_t compacted_at = 0;
    int64_t bytes_in = 0;
    int64_t bytes_out = 0;
    // Key: number of rows merged
    // Value: counter
    std::unordered_map<int32_t, int64_t> rows_merged;
};

class system_keyspace : public seastar::peering_sharded_service<system_keyspace>, public seastar::async_sharded_service<system_keyspace> {
    cql3::query_processor& _qp;
    replica::database& _db;
    std::unique_ptr<local_cache> _cache;
    virtual_tables_registry _virtual_tables_registry;
    bool _peers_table_read_fixup_done = false;

    static schema_ptr raft_snapshot_config();
    static schema_ptr local();
    static schema_ptr peers();
    static schema_ptr peer_events();
    static schema_ptr range_xfers();
    static schema_ptr compactions_in_progress();
    static schema_ptr compaction_history();
    static schema_ptr sstable_activity();
    static schema_ptr large_partitions();
    static schema_ptr large_rows();
    static schema_ptr large_cells();
    static schema_ptr scylla_local();
    future<> force_blocking_flush(sstring cfname);
    // This function is called when the system.peers table is read,
    // and it fixes some types of inconsistencies that can occur
    // due to node crashes:
    //  * missing host_id. This is possible in the old versions of the code. Such records
    //  are removed and the warning is written to the log.
    //  * duplicate IPs for a given host_id. This is possible when some node changes its IP
    //  and this node crashes after adding a new IP but before removing the old one. The
    //  record with older timestamp is removed, the warning is written to the log.
    future<> peers_table_read_fixup();
public:
    static schema_ptr size_estimates();
public:
    static constexpr auto NAME = "system";
    static constexpr auto HINTS = "hints";
    static constexpr auto BATCHLOG = "batchlog";
    static constexpr auto PAXOS = "paxos";
    static constexpr auto BUILT_INDEXES = "IndexInfo";
    static constexpr auto LOCAL = "local";
    static constexpr auto TRUNCATED = "truncated";
    static constexpr auto COMMITLOG_CLEANUPS = "commitlog_cleanups";
    static constexpr auto PEERS = "peers";
    static constexpr auto PEER_EVENTS = "peer_events";
    static constexpr auto RANGE_XFERS = "range_xfers";
    static constexpr auto COMPACTIONS_IN_PROGRESS = "compactions_in_progress";
    static constexpr auto COMPACTION_HISTORY = "compaction_history";
    static constexpr auto SSTABLE_ACTIVITY = "sstable_activity";
    static constexpr auto SIZE_ESTIMATES = "size_estimates";
    static constexpr auto LARGE_PARTITIONS = "large_partitions";
    static constexpr auto LARGE_ROWS = "large_rows";
    static constexpr auto LARGE_CELLS = "large_cells";
    static constexpr auto SCYLLA_LOCAL = "scylla_local";
    static constexpr auto RAFT = "raft";
    static constexpr auto RAFT_SNAPSHOTS = "raft_snapshots";
    static constexpr auto RAFT_SNAPSHOT_CONFIG = "raft_snapshot_config";
    static constexpr auto REPAIR_HISTORY = "repair_history";
    static constexpr auto GROUP0_HISTORY = "group0_history";
    static constexpr auto DISCOVERY = "discovery";
    static constexpr auto BROADCAST_KV_STORE = "broadcast_kv_store";
    static constexpr auto TOPOLOGY = "topology";
    static constexpr auto TOPOLOGY_REQUESTS = "topology_requests";
    static constexpr auto SSTABLES_REGISTRY = "sstables";
    static constexpr auto CDC_GENERATIONS_V3 = "cdc_generations_v3";
    static constexpr auto TABLETS = "tablets";
    static constexpr auto SERVICE_LEVELS_V2 = "service_levels_v2";

    // auth
    static constexpr auto ROLES = "roles";
    static constexpr auto ROLE_MEMBERS = "role_members";
    static constexpr auto ROLE_ATTRIBUTES = "role_attributes";
    static constexpr auto ROLE_PERMISSIONS = "role_permissions";

    struct v3 {
        static constexpr auto BATCHES = "batches";
        static constexpr auto PAXOS = "paxos";
        static constexpr auto BUILT_INDEXES = "IndexInfo";
        static constexpr auto LOCAL = "local";
        static constexpr auto PEERS = "peers";
        static constexpr auto PEER_EVENTS = "peer_events";
        static constexpr auto RANGE_XFERS = "range_xfers";
        static constexpr auto COMPACTION_HISTORY = "compaction_history";
        static constexpr auto SSTABLE_ACTIVITY = "sstable_activity";
        static constexpr auto SIZE_ESTIMATES = "size_estimates";
        static constexpr auto AVAILABLE_RANGES = "available_ranges";
        static constexpr auto VIEWS_BUILDS_IN_PROGRESS = "views_builds_in_progress";
        static constexpr auto BUILT_VIEWS = "built_views";
        static constexpr auto SCYLLA_VIEWS_BUILDS_IN_PROGRESS = "scylla_views_builds_in_progress";
        static constexpr auto CDC_LOCAL = "cdc_local";
        static schema_ptr batches();
        static schema_ptr built_indexes();
        static schema_ptr local();
        static schema_ptr truncated();
        static schema_ptr commitlog_cleanups();
        static schema_ptr peers();
        static schema_ptr peer_events();
        static schema_ptr range_xfers();
        static schema_ptr compaction_history();
        static schema_ptr sstable_activity();
        static schema_ptr size_estimates();
        static schema_ptr large_partitions();
        static schema_ptr scylla_local();
        static schema_ptr available_ranges();
        static schema_ptr views_builds_in_progress();
        static schema_ptr built_views();
        static schema_ptr scylla_views_builds_in_progress();
        static schema_ptr cdc_local();
    };

    struct legacy {
        static constexpr auto HINTS = "hints";
        static constexpr auto BATCHLOG = "batchlog";
        static constexpr auto KEYSPACES = "schema_keyspaces";
        static constexpr auto COLUMNFAMILIES = "schema_columnfamilies";
        static constexpr auto COLUMNS = "schema_columns";
        static constexpr auto TRIGGERS = "schema_triggers";
        static constexpr auto USERTYPES = "schema_usertypes";
        static constexpr auto FUNCTIONS = "schema_functions";
        static constexpr auto AGGREGATES = "schema_aggregates";

        static schema_ptr keyspaces();
        static schema_ptr column_families();
        static schema_ptr columns();
        static schema_ptr triggers();
        static schema_ptr usertypes();
        static schema_ptr functions();
        static schema_ptr aggregates();
        static schema_ptr hints();
        static schema_ptr batchlog();
    };

    // Partition estimates for a given range of tokens.
    struct range_estimates {
        schema_ptr schema;
        bytes range_start_token;
        bytes range_end_token;
        int64_t partitions_count;
        int64_t mean_partition_size;
    };

    using view_name = system_keyspace_view_name;
    using view_build_progress = system_keyspace_view_build_progress;

    static schema_ptr hints();
    static schema_ptr batchlog();
    static schema_ptr paxos();
    static schema_ptr built_indexes(); // TODO (from Cassandra): make private
    static schema_ptr raft();
    static schema_ptr raft_snapshots();
    static schema_ptr repair_history();
    static schema_ptr group0_history();
    static schema_ptr discovery();
    static schema_ptr broadcast_kv_store();
    static schema_ptr topology();
    static schema_ptr topology_requests();
    static schema_ptr sstables_registry();
    static schema_ptr cdc_generations_v3();
    static schema_ptr tablets();
    static schema_ptr service_levels_v2();

    // auth
    static schema_ptr roles();
    static schema_ptr role_members();
    static schema_ptr role_attributes();
    static schema_ptr role_permissions();

    static table_schema_version generate_schema_version(table_id table_id, uint16_t offset = 0);

    future<> build_bootstrap_info();
    future<std::unordered_map<table_id, db_clock::time_point>> load_truncation_times();
    future<> update_schema_version(table_schema_version version);

    /*
    * Save tokens used by this node in the LOCAL table.
    */
    future<> update_tokens(const std::unordered_set<dht::token>& tokens);

    future<std::unordered_map<gms::inet_address, gms::inet_address>> get_preferred_ips();

public:
    struct peer_info {
        std::optional<sstring> data_center;
        std::optional<net::inet_address> preferred_ip;
        std::optional<sstring> rack;
        std::optional<sstring> release_version;
        std::optional<net::inet_address> rpc_address;
        std::optional<utils::UUID> schema_version;
        std::optional<std::unordered_set<dht::token>> tokens;
        std::optional<sstring> supported_features;
    };

    future<> update_peer_info(gms::inet_address ep, locator::host_id hid, const peer_info& info);

    future<> remove_endpoint(gms::inet_address ep);

    // Saves the key-value pair into system.scylla_local table.
    // Pass visible_before_cl_replay = true iff the data should be available before
    // schema commitlog replay. We do table.flush in this case, so it's rather slow and heavyweight.
    future<> set_scylla_local_param(const sstring& key, const sstring& value, bool visible_before_cl_replay);
    future<std::optional<sstring>> get_scylla_local_param(const sstring& key);

    // Saves the key-value pair into system.scylla_local table.
    // Pass visible_before_cl_replay = true iff the data should be available before
    // schema commitlog replay. We do table.flush in this case, so it's rather slow and heavyweight.
    template <typename T>
    future<> set_scylla_local_param_as(const sstring& key, const T& value, bool visible_before_cl_replay);
    template <typename T>
    future<std::optional<T>> get_scylla_local_param_as(const sstring& key);

    static std::vector<schema_ptr> auth_tables();
    static std::vector<schema_ptr> all_tables(const db::config& cfg);

    future<> make(
            locator::effective_replication_map_factory&,
            replica::database&);

    void mark_writable();


    /// overloads

    future<foreign_ptr<lw_shared_ptr<reconcilable_result>>>
    static query_mutations(distributed<replica::database>& db,
                    schema_ptr schema);

    future<foreign_ptr<lw_shared_ptr<reconcilable_result>>>
    static query_mutations(distributed<replica::database>& db,
                    const sstring& ks_name,
                    const sstring& cf_name);

    future<foreign_ptr<lw_shared_ptr<reconcilable_result>>>
    static query_mutations(distributed<replica::database>& db,
                    const sstring& ks_name,
                    const sstring& cf_name,
                    const dht::partition_range& partition_range,
                    query::clustering_range row_ranges = query::clustering_range::make_open_ended_both_sides());

    // Returns all data from given system table.
    // Intended to be used by code which is not performance critical.
    static future<lw_shared_ptr<query::result_set>> query(distributed<replica::database>& db,
                    const sstring& ks_name,
                    const sstring& cf_name);

    // Returns a slice of given system table.
    // Intended to be used by code which is not performance critical.
    static future<lw_shared_ptr<query::result_set>> query(
        distributed<replica::database>& db,
        const sstring& ks_name,
        const sstring& cf_name,
        const dht::decorated_key& key,
        query::clustering_range row_ranges = query::clustering_range::make_open_ended_both_sides());


    /**
     * Return a map of nodes and their loaded_endpoint_state
     */
    future<std::unordered_map<locator::host_id, gms::loaded_endpoint_state>> load_endpoint_state();

    enum class bootstrap_state {
        NEEDS_BOOTSTRAP,
        COMPLETED,
        IN_PROGRESS,
        DECOMMISSIONED
    };

    future<> update_compaction_history(utils::UUID uuid, sstring ksname, sstring cfname, int64_t compacted_at, int64_t bytes_in, int64_t bytes_out,
                                       std::unordered_map<int32_t, int64_t> rows_merged);
    using compaction_history_consumer = noncopyable_function<future<>(const compaction_history_entry&)>;
    future<> get_compaction_history(compaction_history_consumer f);

    struct repair_history_entry {
        tasks::task_id id;
        table_id table_uuid;
        db_clock::time_point ts;
        sstring ks;
        sstring cf;
        int64_t range_start;
        int64_t range_end;
    };

    struct topology_requests_entry {
        utils::UUID id;
        utils::UUID initiating_host;
        std::optional<service::topology_request> request_type;
        db_clock::time_point start_time;
        bool done;
        sstring error;
        db_clock::time_point end_time;
        db_clock::time_point ts;
    };
    using topology_requests_entries = std::unordered_map<utils::UUID, system_keyspace::topology_requests_entry>;

    future<> update_repair_history(repair_history_entry);
    using repair_history_consumer = noncopyable_function<future<>(const repair_history_entry&)>;
    future<> get_repair_history(table_id, repair_history_consumer f);

    future<> save_truncation_record(const replica::column_family&, db_clock::time_point truncated_at, db::replay_position);
    future<replay_positions> get_truncated_positions(table_id);
    future<> drop_truncation_rp_records();

    // Converts a `dht::token_range` object to the left-open integer range (x,y] form.
    //
    // Note: perhaps this should be extracted to `dht/`, or somewhere.
    static std::pair<int64_t, int64_t> canonical_token_range(dht::token_range tr);

    // When a commitlog replay happens after a successful cleanup operation,
    // we have to filter out the mutations affected by the cleanup,
    // to avoid data resurrection.
    //
    // For this purpose, records of cleanup operations (the affected token ranges
    // and commitlog ranges) are kept in a system table.
    //
    // The below functions manipulate these records. 

    // Saves a record of a token range affected by cleanup.
    // After reboot, tokens from this range will be replayed only if they are on replay positions
    // strictly greater than the given one.
    future<> save_commitlog_cleanup_record(table_id, dht::token_range, db::replay_position);
    struct commitlog_cleanup_map_hash {
        size_t operator()(const std::pair<table_id, int32_t>& p) const;
    };
    // For a given token, this map returns the maximum replay position affected by cleanups.
    // A mutation in commitlog should only be replayed if it lies on a replay position
    // greater than that maximum for its token.
    struct commitlog_cleanup_local_map {
        // pimpl to avoid transitive #include of boost/icl.
        class impl;
        std::unique_ptr<impl> _pimpl;
        ~commitlog_cleanup_local_map();
        commitlog_cleanup_local_map();
        std::optional<db::replay_position> get(int64_t token) const;
    };
    using commitlog_cleanup_map = std::unordered_map<
        std::pair<table_id, int32_t>,
        commitlog_cleanup_local_map,
        commitlog_cleanup_map_hash
        >;
    future<commitlog_cleanup_map> get_commitlog_cleanup_records();
    // Drops all cleanup records which apply to positions older than the given one.
    // Used to drop records which only apply to segments which have already been deleted.
    future<> drop_old_commitlog_cleanup_records(replay_position);
    // Cleans all records. Used after a successful replay, since the records only
    // apply to the commitlog of the last boot cycle, and can be wrong in this cycle.
    future<> drop_all_commitlog_cleanup_records();

    /**
     * Return a map of stored tokens to IP addresses
     *
     */
    future<std::unordered_map<gms::inet_address, std::unordered_set<dht::token>>> load_tokens();

    /**
     * Return a map of store host_ids to IP addresses
     *
     */
    future<std::unordered_map<gms::inet_address, locator::host_id>> load_host_ids();

    future<std::vector<gms::inet_address>> load_peers();

    /*
     * Read this node's tokens stored in the LOCAL table.
     * Used to initialize a restarting node.
     */
    future<std::unordered_set<dht::token>> get_saved_tokens();

    /*
     * Gets this node's non-empty set of tokens.
     * TODO: maybe get this data from token_metadata instance?
     */
    future<std::unordered_set<dht::token>> get_local_tokens();

    future<std::unordered_map<gms::inet_address, sstring>> load_peer_features();
    future<std::set<sstring>> load_local_enabled_features();
    // This function stores the features in the system.scylla_local table.
    // We pass visible_before_cl_replay=true iff the features should be available before
    // schema commitlog replay. We do table.flush in this case, so it's rather slow and heavyweight.
    // Features over RAFT are migrated to system.topology table, but
    // we still call this function in that case with visible_before_cl_replay=false
    // for backward compatibility, since some client applications
    // may depend on it.
    future<> save_local_enabled_features(std::set<sstring> features, bool visible_before_cl_replay);

    future<gms::generation_type> increment_and_get_generation();
    bool bootstrap_needed() const;
    bool bootstrap_complete() const;
    bool bootstrap_in_progress() const;
    bootstrap_state get_bootstrap_state() const;
    bool was_decommissioned() const;
    future<> set_bootstrap_state(bootstrap_state state);

    struct local_info {
        locator::host_id host_id;
        sstring cluster_name;
        gms::inet_address listen_address;
    };

    future<local_info> load_local_info();
    future<> save_local_info(local_info, locator::endpoint_dc_rack, gms::inet_address broadcast_address, gms::inet_address broadcast_rpc_address);
public:
    static api::timestamp_type schema_creation_timestamp();

    /**
     * Builds a mutation for SIZE_ESTIMATES_CF containing the specified estimates.
     */
    static mutation make_size_estimates_mutation(const sstring& ks, std::vector<range_estimates> estimates);

    future<> register_view_for_building(sstring ks_name, sstring view_name, const dht::token& token);
    future<> update_view_build_progress(sstring ks_name, sstring view_name, const dht::token& token);
    future<> remove_view_build_progress(sstring ks_name, sstring view_name);
    future<> remove_view_build_progress_across_all_shards(sstring ks_name, sstring view_name);
    future<> mark_view_as_built(sstring ks_name, sstring view_name);
    future<> remove_built_view(sstring ks_name, sstring view_name);
    future<std::vector<view_name>> load_built_views();
    future<std::vector<view_build_progress>> load_view_build_progress();

    // Paxos related functions
    future<service::paxos::paxos_state> load_paxos_state(partition_key_view key, schema_ptr s, gc_clock::time_point now,
            db::timeout_clock::time_point timeout);
    future<> save_paxos_promise(const schema& s, const partition_key& key, const utils::UUID& ballot, db::timeout_clock::time_point timeout);
    future<> save_paxos_proposal(const schema& s, const service::paxos::proposal& proposal, db::timeout_clock::time_point timeout);
    future<> save_paxos_decision(const schema& s, const service::paxos::proposal& decision, db::timeout_clock::time_point timeout);
    future<> delete_paxos_decision(const schema& s, const partition_key& key, const utils::UUID& ballot, db::timeout_clock::time_point timeout);

    // CDC related functions

    /*
    * Save the CDC generation ID announced by this node in persistent storage.
    */
    future<> update_cdc_generation_id(cdc::generation_id);

    /*
    * Read the CDC generation ID announced by this node from persistent storage.
    * Used to initialize a restarting node.
    */
    future<std::optional<cdc::generation_id>> get_cdc_generation_id();

    future<bool> cdc_is_rewritten();
    future<> cdc_set_rewritten(std::optional<cdc::generation_id_v1>);

    // Load Raft Group 0 id from scylla.local
    future<utils::UUID> get_raft_group0_id();

    // Persist Raft Group 0 id. Should be a TIMEUUID.
    future<> set_raft_group0_id(utils::UUID id);

    // Save advertised gossip feature set to system.local
    future<> save_local_supported_features(const std::set<std::string_view>& feats);

    // Get the last (the greatest in timeuuid order) state ID in the group 0 history table.
    // Assumes that the history table exists, i.e. Raft experimental feature is enabled.
    future<utils::UUID> get_last_group0_state_id();

    // Checks whether the group 0 history table contains the given state ID.
    // Assumes that the history table exists, i.e. Raft experimental feature is enabled.
    future<bool> group0_history_contains(utils::UUID state_id);

    // force_load_hosts is a set of hosts which must be loaded even if they are in the left state.
    future<service::topology> load_topology_state(const std::unordered_set<locator::host_id>& force_load_hosts);

    future<std::optional<service::topology_features>> load_topology_features_state();

    // Read CDC generation data with the given UUID as key.
    // Precondition: the data is known to be present in the table (because it was committed earlier through group 0).
    future<cdc::topology_description> read_cdc_generation(utils::UUID id);

    // Read CDC generation data with the given UUID as key.
    // Unlike `read_cdc_generation`, does not require the data to be present.
    // This method is meant to be used after switching back to legacy mode due to raft recovery,
    // as the node will need to fetch definition of a CDC generation that was
    // previously created in raft topology mode.
    future<std::optional<cdc::topology_description>> read_cdc_generation_opt(utils::UUID id);

    // The mutation appends the given state ID to the group 0 history table, with the given description if non-empty.
    //
    // If `gc_older_than` is provided, the mutation will also contain a tombstone that clears all entries whose
    // timestamps (contained in the state IDs) are older than `timestamp(state_id) - gc_older_than`.
    // The duration must be non-negative and smaller than `timestamp(state_id)`.
    //
    // The mutation's timestamp is extracted from the state ID.
    static mutation make_group0_history_state_id_mutation(
            utils::UUID state_id, std::optional<gc_clock::duration> gc_older_than, std::string_view description);

    // Obtain the contents of the group 0 history table in mutation form.
    // Assumes that the history table exists, i.e. Raft feature is enabled.
    static future<mutation> get_group0_history(distributed<replica::database>&);

    // If the `group0_schema_version` key in `system.scylla_local` is present (either live or tombstone),
    // returns the corresponding mutation. Otherwise returns nullopt.
    future<std::optional<mutation>> get_group0_schema_version();

    enum class auth_version_t: int64_t {
        v1 = 1,
        v2 = 2,
    };

    // If the `auth_version` key in `system.scylla_local` is present (either live or tombstone),
    // returns the corresponding mutation. Otherwise returns nullopt.
    future<std::optional<mutation>> get_auth_version_mutation();
    future<mutation> make_auth_version_mutation(api::timestamp_type ts, auth_version_t version);
    future<auth_version_t> get_auth_version();

    future<> sstables_registry_create_entry(sstring location, sstring status, sstables::sstable_state state, sstables::entry_descriptor desc);
    future<> sstables_registry_update_entry_status(sstring location, sstables::generation_type gen, sstring status);
    future<> sstables_registry_update_entry_state(sstring location, sstables::generation_type gen, sstables::sstable_state state);
    future<> sstables_registry_delete_entry(sstring location, sstables::generation_type gen);
    using sstable_registry_entry_consumer = sstables::sstables_registry::entry_consumer;
    future<> sstables_registry_list(sstring location, sstable_registry_entry_consumer consumer);

    future<std::optional<sstring>> load_group0_upgrade_state();
    future<> save_group0_upgrade_state(sstring);

    future<bool> get_must_synchronize_topology();
    future<> set_must_synchronize_topology(bool);

    future<service::topology_request_state> get_topology_request_state(utils::UUID id, bool require_entry);
    topology_requests_entry topology_request_row_to_entry(utils::UUID id, const cql3::untyped_result_set_row& row);
    future<topology_requests_entry> get_topology_request_entry(utils::UUID id, bool require_entry);
    future<topology_requests_entries> get_topology_request_entries(db_clock::time_point end_time_limit);

public:
    future<std::optional<int8_t>> get_service_levels_version();
    
    future<mutation> make_service_levels_version_mutation(int8_t version, const service::group0_guard& guard);
    future<std::optional<mutation>> get_service_levels_version_mutation();

private:
    static std::optional<service::topology_features> decode_topology_features_state(::shared_ptr<cql3::untyped_result_set> rs);

public:

    system_keyspace(cql3::query_processor& qp, replica::database& db) noexcept;
    ~system_keyspace();
    future<> shutdown();

    virtual_tables_registry& get_virtual_tables_registry() { return _virtual_tables_registry; }
private:
    future<::shared_ptr<cql3::untyped_result_set>> execute_cql(const sstring& query_string, const data_value_list& values);
    template <typename... Args>
    future<::shared_ptr<cql3::untyped_result_set>> execute_cql_with_timeout(sstring req, db::timeout_clock::time_point timeout, Args&&... args);

public:
    template <typename... Args>
    future<::shared_ptr<cql3::untyped_result_set>> execute_cql(sstring req, Args&&... args) {
        return execute_cql(req, { data_value(std::forward<Args>(args))... });
    }

    friend future<column_mapping> db::schema_tables::get_column_mapping(db::system_keyspace& sys_ks, ::table_id table_id, table_schema_version version);
    friend future<bool> db::schema_tables::column_mapping_exists(db::system_keyspace& sys_ks, table_id table_id, table_schema_version version);
    friend future<> db::schema_tables::drop_column_mapping(db::system_keyspace& sys_ks, table_id table_id, table_schema_version version);

    const replica::database& local_db() const noexcept {
        return _db;
    }
    cql3::query_processor& query_processor() const noexcept {
        return _qp;
    }
}; // class system_keyspace

} // namespace db
