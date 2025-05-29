/*
 * Copyright (C) 2019-present ScyllaDB
 *
 * Modified by ScyllaDB
 */
/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */
#pragma once
#include <seastar/core/semaphore.hh>
#include "service/paxos/proposal.hh"
#include "utils/log.hh"
#include "utils/digest_algorithm.hh"
#include "db/timeout_clock.hh"
#include <unordered_map>
#include "utils/UUID_gen.hh"
#include "service/paxos/prepare_response.hh"

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

namespace service::paxos {

using clock_type = db::timeout_clock;

class paxos_store;

// The state of a CAS update of a given primary key as persisted in the paxos table.
class paxos_state {
private:
    class guard;

    class key_lock_map {
        using semaphore = basic_semaphore<semaphore_default_exception_factory, clock_type>;
        using semaphore_units = semaphore_units<semaphore_default_exception_factory, clock_type>;
        using map = std::unordered_map<dht::token, semaphore>;

        semaphore& get_semaphore_for_key(const dht::token& key);
        void release_semaphore_for_key(const dht::token& key);

        map _locks;
    public:

        friend class guard;
    };

    class guard {
        key_lock_map& _map;
        dht::token _key;
        clock_type::time_point _timeout;
        key_lock_map::semaphore_units _units;
    public:
        future<> lock () {
            return get_units(_map.get_semaphore_for_key(_key), 1, _timeout).then([this] (auto&& u) { _units = std::move(u); });
        }
        guard(key_lock_map& map, const dht::token& key, clock_type::time_point timeout) : _map(map), _key(key), _timeout(timeout) {};
        guard(guard&& o) = default;
        ~guard() {
            _units.return_all();
            _map.release_semaphore_for_key(_key);
        }
    };

    // Locks are local to the shard which owns the corresponding token range.
    // Protects concurrent reads and writes of the same row in system.paxos table.
    static thread_local key_lock_map _paxos_table_lock;
    // Taken by the coordinator code to allow only one instance of PAXOS to run for each key.
    // This prevents contantion between multiple clients trying to modify the
    // same key through the same coordinator and stealing the ballot from
    // each other.
    static thread_local key_lock_map _coordinator_lock;

    using guard_foreign_ptr = foreign_ptr<lw_shared_ptr<guard>>;
    using replica_guard = boost::container::static_vector<guard_foreign_ptr, 2>;
    static future<replica_guard> get_replica_lock(const dht::token& key, clock_type::time_point timeout,
        const dht::shard_replica_set& shards);

    utils::UUID _promised_ballot = utils::UUID_gen::min_time_UUID();
    std::optional<proposal> _accepted_proposal;
    std::optional<proposal> _most_recent_commit;

public:

    static future<guard> get_cas_lock(const dht::token& key, clock_type::time_point timeout);

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

class paxos_store:
    public seastar::peering_sharded_service<paxos_store>, 
    public seastar::async_sharded_service<paxos_store>
{
    db::system_keyspace& _sys_ks;
    gms::feature_service& _features;
    replica::database& _db;
    migration_manager& _mm;

    template <typename... Args>
    future<cql3::untyped_result_set> execute_cql_with_timeout(sstring req, db::timeout_clock::time_point timeout, Args&&... args);
    future<schema_ptr> get_paxos_state_schema(const schema& s, db::timeout_clock::time_point timeout) const;
    future<> create_paxos_state_table(const schema& s, db::timeout_clock::time_point timeout);
    static schema_ptr create_paxos_state_schema(const schema& s);
    schema_ptr try_get_paxos_state_schema(const schema& s) const;
    void check_raft_is_enabled(const schema& s) const;
public:
    explicit paxos_store(db::system_keyspace& sys_ks, gms::feature_service& features, replica::database& db, migration_manager& mm);
    future<> ensure_initialized(const schema& s, db::timeout_clock::time_point timeout);
    static std::optional<std::string_view> try_get_base_table(std::string_view cf_name);
    future<column_mapping> get_column_mapping(table_id, table_schema_version v);
    future<service::paxos::paxos_state> load_paxos_state(partition_key_view key, schema_ptr s, gc_clock::time_point now,
        db::timeout_clock::time_point timeout);
    future<> save_paxos_promise(const schema& s, const partition_key& key, const utils::UUID& ballot, db::timeout_clock::time_point timeout);
    future<> save_paxos_proposal(const schema& s, const service::paxos::proposal& proposal, db::timeout_clock::time_point timeout);
    future<> save_paxos_decision(const schema& s, const service::paxos::proposal& decision, db::timeout_clock::time_point timeout);
    future<> delete_paxos_decision(const schema& s, const partition_key& key, utils::UUID ballot, db::timeout_clock::time_point timeout);
};

} // end of namespace "service::paxos"

