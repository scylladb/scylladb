/*
 * Copyright (C) 2019-present ScyllaDB
 *
 * Modified by ScyllaDB
 */
/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */
#pragma once
#include "service/paxos/proposal.hh"
#include "log.hh"
#include "utils/digest_algorithm.hh"
#include "db/timeout_clock.hh"
#include <unordered_map>
#include "utils/UUID_gen.hh"
#include "service/paxos/prepare_response.hh"

namespace service {
class storage_proxy;
}
namespace db { class system_keyspace; }

namespace service::paxos {

using clock_type = db::timeout_clock;

// The state of a CAS update of a given primary key as persisted in the paxos table.
class paxos_state {
public:
    class guard;
private:

    class key_lock_map {
        using semaphore = basic_semaphore<semaphore_default_exception_factory, clock_type>;
        using map = std::unordered_map<dht::token, semaphore>;

        semaphore& get_semaphore_for_key(const dht::token& key);
        void release_semaphore_for_key(const dht::token& key);

        map _locks;
    public:

        //
        // A thin RAII aware wrapper around the lock map to garbage
        // collect the decorated key from the map on unlock if there
        // are no waiters.
        ///
        template<typename Func>
        futurize_t<std::result_of_t<Func()>> with_locked_key(const dht::token& key, clock_type::time_point timeout, Func func) {
            return with_semaphore(get_semaphore_for_key(key), 1, timeout - clock_type::now(), std::move(func)).finally([key, this] {
                release_semaphore_for_key(key);
            });
        }

        friend class guard;
    };

    // Locks are local to the shard which owns the corresponding token range.
    // Protects concurrent reads and writes of the same row in system.paxos table.
    static thread_local key_lock_map _paxos_table_lock;
    // Taken by the coordinator code to allow only one instance of PAXOS to run for each key.
    // This prevents contantion between multiple clients trying to modify the
    // same key through the same coordinator and stealing the ballot from
    // each other.
    static thread_local key_lock_map _coordinator_lock;


    // protects access to system.paxos
    template<typename Func>
    static
    futurize_t<std::result_of_t<Func()>> with_locked_key(const dht::token& key, clock_type::time_point timeout, Func func) {
        return _paxos_table_lock.with_locked_key(key, timeout, std::move(func));
    }

    utils::UUID _promised_ballot = utils::UUID_gen::min_time_UUID();
    std::optional<proposal> _accepted_proposal;
    std::optional<proposal> _most_recent_commit;

public:

    class guard {
        key_lock_map& _map;
        dht::token _key;
        clock_type::time_point _timeout;
        bool _locked = false;
    public:
        future<> lock() {
            auto f = _map.get_semaphore_for_key(_key).wait(_timeout, 1);
            _locked = true;
            return f;
        }
        guard(key_lock_map& map, const dht::token& key, clock_type::time_point timeout) : _map(map), _key(key), _timeout(timeout) {};
        guard(guard&& o) noexcept : _map(o._map), _key(std::move(o._key)), _timeout(o._timeout), _locked(o._locked) {
            o._locked = false;
        }
        ~guard() {
            if (_locked) {
                _map.get_semaphore_for_key(_key).signal(1);
                _map.release_semaphore_for_key(_key);
            }
        }
    };

    static future<guard> get_cas_lock(const dht::token& key, clock_type::time_point timeout);

    static logging::logger logger;

    paxos_state() {}

    paxos_state(utils::UUID promised, std::optional<proposal> accepted, std::optional<proposal> commit)
        : _promised_ballot(std::move(promised))
        , _accepted_proposal(std::move(accepted))
        , _most_recent_commit(std::move(commit)) {}
    // Replica RPC endpoint for Paxos "prepare" phase.
    static future<prepare_response> prepare(storage_proxy& sp, db::system_keyspace& sys_ks, tracing::trace_state_ptr tr_state, schema_ptr schema,
            const query::read_command& cmd, const partition_key& key, utils::UUID ballot,
            bool only_digest, query::digest_algorithm da, clock_type::time_point timeout);
    // Replica RPC endpoint for Paxos "accept" phase.
    static future<bool> accept(storage_proxy& sp, db::system_keyspace& sys_ks, tracing::trace_state_ptr tr_state, schema_ptr schema, dht::token token, const proposal& proposal,
            clock_type::time_point timeout);
    // Replica RPC endpoint for Paxos "learn".
    static future<> learn(storage_proxy& sp, db::system_keyspace& sys_ks, schema_ptr schema, proposal decision, clock_type::time_point timeout, tracing::trace_state_ptr tr_state);
    // Replica RPC endpoint for pruning Paxos table
    static future<> prune(db::system_keyspace& sys_ks, schema_ptr schema, const partition_key& key, utils::UUID ballot, clock_type::time_point timeout,
            tracing::trace_state_ptr tr_state);
};

} // end of namespace "service::paxos"

