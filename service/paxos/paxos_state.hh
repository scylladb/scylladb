/*
 * Copyright (C) 2019-present ScyllaDB
 *
 * Modified by ScyllaDB
 */
/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)
 */
#pragma once
#include <chrono>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/timer.hh>
#include <seastar/util/noncopyable_function.hh>
#include "service/paxos/proposal.hh"
#include "mutation/mutation.hh"
#include "utils/chunked_vector.hh"
#include "utils/log.hh"
#include "utils/digest_algorithm.hh"
#include "utils/hash.hh"
#include "db/timeout_clock.hh"
#include <unordered_map>
#include <unordered_set>
#include "utils/UUID_gen.hh"
#include "service/paxos/prepare_response.hh"
#include "service/topology_state_machine_fwd.hh"
#include "service/migration_listener.hh"
#include "cql3/statements/prepared_statement.hh"

namespace cql3 {
    class query_processor;
    class untyped_result_set;
}

namespace gms {
    class feature_service;
}

namespace service {
class storage_proxy;
}
namespace db { class system_keyspace; }
namespace replica { class database; }

namespace service::paxos {

using clock_type = db::timeout_clock;

template <typename Key>
class key_lock_map {
    using semaphore = basic_semaphore<semaphore_default_exception_factory, clock_type>;
    struct lock {
        semaphore sem{1};
        unsigned refs{0};
    };
    using map = std::unordered_map<Key, lock>;
    map _locks;
public:
    key_lock_map(bool preallocate);

    class guard {
        map& _map;
        map::value_type* _entry;
        bool _locked;
    public:
        guard(key_lock_map<Key>& map, const Key& key);
        guard(guard&& o) noexcept;
        guard(const guard&) = delete;
        ~guard();
        future<> lock(clock_type::time_point timeout);
        const Key& key() const { return _entry->first; }
        semaphore& sem() { return _entry->second.sem; }
    };
};
using token_lock_map = key_lock_map<dht::token>;
using token_guard = token_lock_map::guard;

class paxos_store;

// The state of a CAS update of a given primary key as persisted in the paxos table.
class paxos_state {
private:
    // Locks are local to the shard which owns the corresponding token range.
    // Protects concurrent reads and writes of the same row in system.paxos table.
    static thread_local token_lock_map _paxos_table_lock;
    // Taken by the coordinator code to allow only one instance of PAXOS to run for each key.
    // This prevents contantion between multiple clients trying to modify the
    // same key through the same coordinator and stealing the ballot from
    // each other.
    static thread_local token_lock_map _coordinator_lock;

    using token_guard_foreign_ptr = foreign_ptr<lw_shared_ptr<token_guard>>;
    using replica_guard = boost::container::static_vector<token_guard_foreign_ptr, 2>;
    static future<replica_guard> get_replica_lock(const schema& s, const dht::token& token,
        clock_type::time_point timeout);

    utils::UUID _promised_ballot = utils::UUID_gen::min_time_UUID();
    std::optional<proposal> _accepted_proposal;
    std::optional<proposal> _most_recent_commit;

public:

    static future<token_guard> get_cas_lock(const dht::token& key, clock_type::time_point timeout);

    static logging::logger logger;

    paxos_state() {}

    paxos_state(utils::UUID promised, std::optional<proposal> accepted, std::optional<proposal> commit)
        : _promised_ballot(std::move(promised))
        , _accepted_proposal(std::move(accepted))
        , _most_recent_commit(std::move(commit)) {}
    // Replica RPC endpoint for Paxos "prepare" phase.
    static future<prepare_response> prepare(storage_proxy& sp, paxos_store& paxos_store, tracing::trace_state_ptr tr_state, schema_ptr schema,
            const query::read_command& cmd, const partition_key& key, utils::UUID ballot,
            bool only_digest, query::digest_algorithm da, clock_type::time_point timeout);
    // Replica RPC endpoint for Paxos "accept" phase.
    static future<bool> accept(storage_proxy& sp, paxos_store& paxos_store, tracing::trace_state_ptr tr_state, schema_ptr schema, dht::token token, const proposal& proposal,
            clock_type::time_point timeout);
    // Replica RPC endpoint for Paxos "learn".
    static future<> learn(storage_proxy& sp, paxos_store& paxos_store, schema_ptr schema, proposal decision, clock_type::time_point timeout, tracing::trace_state_ptr tr_state);
    // Replica RPC endpoint for pruning Paxos table
    static future<> prune(paxos_store& paxos_store, schema_ptr schema, const partition_key& key, utils::UUID ballot, clock_type::time_point timeout,
            tracing::trace_state_ptr tr_state);
};

// The Paxos state a node copies on the restart which switches its storage mode during a
// vnodes-to-tablets migration.
struct paxos_state_copy {
    // Whether the copy goes from system.paxos into <table>$paxos (a switch to tablets),
    // or back into system.paxos (a switch to vnodes, i.e., a rollback).
    bool to_tablets;
    // The migrating tables whose Paxos state is copied.
    std::unordered_set<table_id> tables;
};

// The progress of the Paxos state copy on one shard.
struct paxos_state_copy_progress {
    // The source tables the shard scanned, plus the fraction of the token ring it passed in
    // the table it scans.
    double completed = 0;
    // The source tables the shard scans.
    double total = 0;
};

class paxos_store:
    public seastar::peering_sharded_service<paxos_store>, 
    public seastar::async_sharded_service<paxos_store>,
    private service::migration_listener::empty_listener
{
    db::system_keyspace& _sys_ks;
    gms::feature_service& _features;
    replica::database& _db;
    migration_manager& _mm;
    bool _stopped = false;
    key_lock_map<table_id> _table_lock_map{false};
    paxos_state_copy_progress _copy_progress;
    // Context for Paxos state copy this node has to make on startup. Set on every shard
    // before the node serves Paxos, and cleared once the copy is done.
    std::optional<paxos_state_copy> _pending_copy;

    enum class paxos_state_query {
        load,
        save_promise,
        save_proposal,
        save_decision,
        delete_decision,
    };

    using prepared_statement_cache_key = std::pair<table_id, paxos_state_query>;
    using prepared_statement_ptr = cql3::statements::prepared_statement::checked_weak_ptr;
    using prepared_statement_cache = std::unordered_map<prepared_statement_cache_key, prepared_statement_ptr, utils::tuple_hash>;

    prepared_statement_cache _prepared_statements;
    static constexpr auto prepared_statements_prune_period = std::chrono::minutes(1);
    seastar::timer<seastar::lowres_clock> _prepared_statements_prune_timer;

    template <typename... Args>
    future<cql3::untyped_result_set> execute_cql_with_timeout(const schema& s, paxos_state_query query,
            noncopyable_function<sstring(const schema&, const schema&)> make_query, db::timeout_clock::time_point timeout,
            Args&&... args);
    future<cql3::untyped_result_set> do_execute_cql_with_timeout(const schema& s, paxos_state_query query,
            noncopyable_function<sstring(const schema&, const schema&)> make_query, db::timeout_clock::time_point timeout,
            std::vector<data_value_or_unset> values);
    future<schema_ptr> get_paxos_state_schema(const schema& s, db::timeout_clock::time_point timeout) const;
    future<> create_paxos_state_table(const schema& s, db::timeout_clock::time_point timeout);
    schema_ptr try_get_paxos_state_schema(const schema& s) const;
    void prune_invalid_prepared_statements();
public:
    explicit paxos_store(db::system_keyspace& sys_ks, gms::feature_service& features, replica::database& db, migration_manager& mm);
    ~paxos_store();
    future<> stop();

    future<> ensure_initialized(const schema& s, db::timeout_clock::time_point timeout);
    static std::optional<std::string_view> try_get_base_table(std::string_view cf_name);
    // The name of the table which keeps the Paxos state of `table_name` when it uses tablets.
    static sstring paxos_state_table_name(std::string_view table_name);
    // The schema of the Paxos state table of `s`, with a newly generated id.
    static schema_ptr create_paxos_state_schema(const schema& s);

    // The percentage of the Paxos state copy done, over all shards.
    future<double> get_copy_progress();

    void set_pending_copy(std::optional<paxos_state_copy> copy) noexcept { _pending_copy = std::move(copy); }
    bool has_pending_copy() const noexcept { return _pending_copy.has_value(); }
    // Makes the pending copy: copies the Paxos state of the migrating tables to the store this
    // node uses after switching storage mode, then clears the pending copy on every shard.
    // Creates the Paxos state table of a table with state to copy if it has none yet, through
    // group0. Runs after the commitlog replay, so that the source is complete.
    future<> copy_pending_state();

    future<column_mapping> get_column_mapping(const schema& query_schema, dht::token token, table_schema_version version, clock_type::time_point timeout);
    future<service::paxos::paxos_state> load_paxos_state(partition_key_view key, schema_ptr s, gc_clock::time_point now,
        db::timeout_clock::time_point timeout);
    future<> save_paxos_promise(const schema& s, const partition_key& key, const utils::UUID& ballot, db::timeout_clock::time_point timeout);
    future<> save_paxos_proposal(const schema& s, const service::paxos::proposal& proposal, db::timeout_clock::time_point timeout);
    future<> save_paxos_decision(const schema& s, const service::paxos::proposal& decision, db::timeout_clock::time_point timeout);
    future<> delete_paxos_decision(const schema& s, const partition_key& key, utils::UUID ballot, db::timeout_clock::time_point timeout);

    void on_before_drop_column_family(const schema& schema, utils::chunked_vector<mutation>& mutations, api::timestamp_type timestamp) override;

    // For testing only
    const prepared_statement_cache& get_prepared_statements_cache() const;
    void set_prepared_statements_prune_period(seastar::lowres_clock::duration period);
};

// Exposed for testing.
namespace internal {

// The live Paxos state kept in a partition of system.paxos, converted to mutations of the
// Paxos state tables in `state_schemas`, keyed by their base table. Rows of the other
// tables, dead cells and tombstones are skipped. Live cells and row markers keep their
// timestamps and expiry.
utils::chunked_vector<mutation> system_paxos_to_paxos_state_tables(const mutation& system_paxos_mutation,
        const std::unordered_map<table_id, schema_ptr>& state_schemas, gc_clock::time_point now);

// The inverse of system_paxos_to_paxos_state_tables(): the live Paxos state of table
// `base` kept in a partition of its Paxos state table, converted to a mutation of
// system.paxos, or nullopt if nothing in it is live.
std::optional<mutation> paxos_state_table_to_system_paxos(const mutation& state_table_mutation, table_id base,
        gc_clock::time_point now);

} // namespace internal

// What this node has to copy on this restart, or nullopt if it doesn't switch storage mode
// or has nothing to copy. `current_storage_mode` is the mode this node last published in
// the topology, if any. Needs the tablet metadata, loaded at startup.
std::optional<paxos_state_copy> plan_paxos_state_copy(replica::database& db, std::optional<storage_mode> current_storage_mode);

} // end of namespace "service::paxos"

