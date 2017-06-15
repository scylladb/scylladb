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

#pragma once

#include "database.hh"
#include "query-request.hh"
#include "query-result.hh"
#include "query-result-set.hh"
#include "core/distributed.hh"
#include "db/consistency_level.hh"
#include "db/write_type.hh"
#include "utils/histogram.hh"
#include "utils/estimated_histogram.hh"
#include "tracing/trace_state.hh"
#include <seastar/core/metrics.hh>
#include "frozen_mutation.hh"

namespace compat {

class one_or_two_partition_ranges;

}

namespace service {

class abstract_write_response_handler;
class abstract_read_executor;
class mutation_holder;

class storage_proxy : public seastar::async_sharded_service<storage_proxy> /*implements StorageProxyMBean*/ {
public:
    using clock_type = lowres_clock;
private:
    struct rh_entry {
        ::shared_ptr<abstract_write_response_handler> handler;
        timer<clock_type> expire_timer;
        rh_entry(::shared_ptr<abstract_write_response_handler>&& h, std::function<void()>&& cb);
    };

    using response_id_type = uint64_t;
    struct unique_response_handler {
        response_id_type id;
        storage_proxy& p;
        unique_response_handler(storage_proxy& p_, response_id_type id_);
        unique_response_handler(const unique_response_handler&) = delete;
        unique_response_handler& operator=(const unique_response_handler&) = delete;
        unique_response_handler(unique_response_handler&& x);
        ~unique_response_handler();
        response_id_type release();
    };

    static const sstring COORDINATOR_STATS_CATEGORY;
    static const sstring REPLICA_STATS_CATEGORY;

public:
    // split statistics counters
    struct split_stats {
        static seastar::metrics::label datacenter_label;
        static seastar::metrics::label op_type_label;
    private:
        struct stats_counter {
            uint64_t val = 0;
        };

        // counter of operations performed on a local Node
        stats_counter _local;
        // counters of operations performed on external Nodes aggregated per Nodes' DCs
        std::unordered_map<sstring, stats_counter> _dc_stats;
        // collectd registrations container
        seastar::metrics::metric_groups _metrics;
        // a prefix string that will be used for a collecd counters' description
        sstring _short_description_prefix;
        sstring _long_description_prefix;
        // a statistics category, e.g. "client" or "replica"
        sstring _category;
        // type of operation (data/digest/mutation_data)
        sstring _op_type;

    public:
        /**
         * @param category a statistics category, e.g. "client" or "replica"
         * @param short_description_prefix a short description prefix
         * @param long_description_prefix a long description prefix
         */
        split_stats(const sstring& category, const sstring& short_description_prefix, const sstring& long_description_prefix, const sstring& op_type);

        /**
         * Get a reference to the statistics counter corresponding to the given
         * destination.
         *
         * @param ep address of a destination
         *
         * @return a reference to the requested counter
         */
        uint64_t& get_ep_stat(gms::inet_address ep);
    };

    struct stats {
        utils::timed_rate_moving_average read_timeouts;
        utils::timed_rate_moving_average read_unavailables;
        utils::timed_rate_moving_average range_slice_timeouts;
        utils::timed_rate_moving_average range_slice_unavailables;
        utils::timed_rate_moving_average write_timeouts;
        utils::timed_rate_moving_average write_unavailables;

        // total write attempts
        split_stats writes_attempts;
        split_stats writes_errors;

        // write attempts due to Read Repair logic
        split_stats read_repair_write_attempts;

        uint64_t read_repair_attempts = 0;
        uint64_t read_repair_repaired_blocking = 0;
        uint64_t read_repair_repaired_background = 0;
        uint64_t global_read_repairs_canceled_due_to_concurrent_write = 0;

        // number of mutations received as a coordinator
        uint64_t received_mutations = 0;

        // number of counter updates received as a leader
        uint64_t received_counter_updates = 0;

        // number of forwarded mutations
        uint64_t forwarded_mutations = 0;
        uint64_t forwarding_errors = 0;

        // number of read requests received as a replica
        uint64_t replica_data_reads = 0;
        uint64_t replica_digest_reads = 0;
        uint64_t replica_mutation_data_reads = 0;

        utils::timed_rate_moving_average_and_histogram read;
        utils::timed_rate_moving_average_and_histogram write;
        utils::timed_rate_moving_average_and_histogram range;
        utils::estimated_histogram estimated_read;
        utils::estimated_histogram estimated_write;
        utils::estimated_histogram estimated_range;
        uint64_t writes = 0;
        uint64_t background_writes = 0; // client no longer waits for the write
        uint64_t background_write_bytes = 0;
        uint64_t queued_write_bytes = 0;
        uint64_t reads = 0;
        uint64_t background_reads = 0; // client no longer waits for the read
        uint64_t read_retries = 0; // read is retried with new limit
        uint64_t throttled_writes = 0; // total number of writes ever delayed due to throttling

        // Data read attempts
        split_stats data_read_attempts;
        split_stats data_read_completed;
        split_stats data_read_errors;

        // Digest read attempts
        split_stats digest_read_attempts;
        split_stats digest_read_completed;
        split_stats digest_read_errors;

        // Mutation data read attempts
        split_stats mutation_data_read_attempts;
        split_stats mutation_data_read_completed;
        split_stats mutation_data_read_errors;

    public:
        stats();
    };
private:
    distributed<database>& _db;
    response_id_type _next_response_id = 1; // 0 is reserved for unique_response_handler
    std::unordered_map<response_id_type, rh_entry> _response_handlers;
    // This buffer hold ids of throttled writes in case resource consumption goes
    // below the threshold and we want to unthrottle some of them. Without this throttled
    // request with dead or slow replica may wait for up to timeout ms before replying
    // even if resource consumption will go to zero. Note that some requests here may
    // be already completed by the point they tried to be unthrottled (request completion does
    // not remove request from the buffer), but this is fine since request ids are unique, so we
    // just skip an entry if request no longer exists.
    circular_buffer<response_id_type> _throttled_writes;
    constexpr static size_t _max_hints_in_progress = 128; // origin multiplies by FBUtilities.getAvailableProcessors() but we already sharded
    size_t _total_hints_in_progress = 0;
    std::unordered_map<gms::inet_address, size_t> _hints_in_progress;
    stats _stats;
    static constexpr float CONCURRENT_SUBREQUESTS_MARGIN = 0.10;
    // for read repair chance calculation
    std::default_random_engine _urandom;
    std::uniform_real_distribution<> _read_repair_chance = std::uniform_real_distribution<>(0,1);
    seastar::metrics::metric_groups _metrics;
private:
    void uninit_messaging_service();
    future<foreign_ptr<lw_shared_ptr<query::result>>> query_singular(lw_shared_ptr<query::read_command> cmd, dht::partition_range_vector&& partition_ranges, db::consistency_level cl, tracing::trace_state_ptr trace_state);
    response_id_type register_response_handler(shared_ptr<abstract_write_response_handler>&& h);
    void remove_response_handler(response_id_type id);
    void got_response(response_id_type id, gms::inet_address from);
    future<> response_wait(response_id_type id, clock_type::time_point timeout);
    ::shared_ptr<abstract_write_response_handler>& get_write_response_handler(storage_proxy::response_id_type id);
    response_id_type create_write_response_handler(keyspace& ks, db::consistency_level cl, db::write_type type, std::unique_ptr<mutation_holder> m, std::unordered_set<gms::inet_address> targets,
            const std::vector<gms::inet_address>& pending_endpoints, std::vector<gms::inet_address>, tracing::trace_state_ptr tr_state);
    response_id_type create_write_response_handler(const mutation&, db::consistency_level cl, db::write_type type, tracing::trace_state_ptr tr_state);
    response_id_type create_write_response_handler(const std::unordered_map<gms::inet_address, std::experimental::optional<mutation>>&, db::consistency_level cl, db::write_type type, tracing::trace_state_ptr tr_state);
    void send_to_live_endpoints(response_id_type response_id, clock_type::time_point timeout);
    template<typename Range>
    size_t hint_to_dead_endpoints(std::unique_ptr<mutation_holder>& mh, const Range& targets) noexcept;
    void hint_to_dead_endpoints(response_id_type, db::consistency_level);
    bool cannot_hint(gms::inet_address target);
    size_t get_hints_in_progress_for(gms::inet_address target);
    bool should_hint(gms::inet_address ep) noexcept;
    bool submit_hint(std::unique_ptr<mutation_holder>& mh, gms::inet_address target);
    std::vector<gms::inet_address> get_live_endpoints(keyspace& ks, const dht::token& token);
    std::vector<gms::inet_address> get_live_sorted_endpoints(keyspace& ks, const dht::token& token);
    db::read_repair_decision new_read_repair_decision(const schema& s);
    ::shared_ptr<abstract_read_executor> get_read_executor(lw_shared_ptr<query::read_command> cmd, dht::partition_range pr, db::consistency_level cl, tracing::trace_state_ptr trace_state);
    future<foreign_ptr<lw_shared_ptr<query::result>>, cache_temperature> query_singular_local(schema_ptr, lw_shared_ptr<query::read_command> cmd, const dht::partition_range& pr,
                                                                           query::result_request request,
                                                                           tracing::trace_state_ptr trace_state,
                                                                           uint64_t max_size = query::result_memory_limiter::maximum_result_size);
    future<query::result_digest, api::timestamp_type, cache_temperature> query_singular_local_digest(schema_ptr, lw_shared_ptr<query::read_command> cmd, const dht::partition_range& pr, tracing::trace_state_ptr trace_state,
                                                                                  uint64_t max_size  = query::result_memory_limiter::maximum_result_size);
    future<foreign_ptr<lw_shared_ptr<query::result>>> query_partition_key_range(lw_shared_ptr<query::read_command> cmd, dht::partition_range_vector partition_ranges, db::consistency_level cl, tracing::trace_state_ptr trace_state);
    dht::partition_range_vector get_restricted_ranges(keyspace& ks, const schema& s, dht::partition_range range);
    float estimate_result_rows_per_range(lw_shared_ptr<query::read_command> cmd, keyspace& ks);
    static std::vector<gms::inet_address> intersection(const std::vector<gms::inet_address>& l1, const std::vector<gms::inet_address>& l2);
    future<std::vector<foreign_ptr<lw_shared_ptr<query::result>>>> query_partition_key_range_concurrent(clock_type::time_point timeout,
            std::vector<foreign_ptr<lw_shared_ptr<query::result>>>&& results, lw_shared_ptr<query::read_command> cmd, db::consistency_level cl, dht::partition_range_vector::iterator&& i,
            dht::partition_range_vector&& ranges, int concurrency_factor, tracing::trace_state_ptr trace_state,
            uint32_t remaining_row_count, uint32_t remaining_partition_count);

    future<foreign_ptr<lw_shared_ptr<query::result>>> do_query(schema_ptr,
        lw_shared_ptr<query::read_command> cmd,
        dht::partition_range_vector&& partition_ranges,
        db::consistency_level cl, tracing::trace_state_ptr trace_state);
    template<typename Range, typename CreateWriteHandler>
    future<std::vector<unique_response_handler>> mutate_prepare(const Range& mutations, db::consistency_level cl, db::write_type type, CreateWriteHandler handler);
    template<typename Range>
    future<std::vector<unique_response_handler>> mutate_prepare(const Range& mutations, db::consistency_level cl, db::write_type type, tracing::trace_state_ptr tr_state);
    future<> mutate_begin(std::vector<unique_response_handler> ids, db::consistency_level cl, stdx::optional<clock_type::time_point> timeout_opt = { });
    future<> mutate_end(future<> mutate_result, utils::latency_counter, tracing::trace_state_ptr trace_state);
    future<> schedule_repair(std::unordered_map<dht::token, std::unordered_map<gms::inet_address, std::experimental::optional<mutation>>> diffs, db::consistency_level cl, tracing::trace_state_ptr trace_state);
    bool need_throttle_writes() const;
    void unthrottle();
    void handle_read_error(std::exception_ptr eptr, bool range);
    template<typename Range>
    future<> mutate_internal(Range mutations, db::consistency_level cl, bool counter_write, tracing::trace_state_ptr tr_state, stdx::optional<clock_type::time_point> timeout_opt = { });
    future<foreign_ptr<lw_shared_ptr<reconcilable_result>>, cache_temperature> query_nonsingular_mutations_locally(
            schema_ptr s, lw_shared_ptr<query::read_command> cmd, const dht::partition_range_vector& pr, tracing::trace_state_ptr trace_state, uint64_t max_size);

    struct frozen_mutation_and_schema {
        frozen_mutation fm;
        schema_ptr s;
    };
    future<> mutate_counters_on_leader(std::vector<frozen_mutation_and_schema> mutations, db::consistency_level cl, clock_type::time_point timeout,
                                       tracing::trace_state_ptr trace_state);
    future<> mutate_counter_on_leader_and_replicate(const schema_ptr& s, frozen_mutation m, db::consistency_level cl, clock_type::time_point timeout,
                                                    tracing::trace_state_ptr trace_state);

    gms::inet_address find_leader_for_counter_update(const mutation& m, db::consistency_level cl);

    future<> do_mutate(std::vector<mutation> mutations, db::consistency_level cl, tracing::trace_state_ptr tr_state, bool);
    friend class mutate_executor;
public:
    storage_proxy(distributed<database>& db);
    ~storage_proxy();
    distributed<database>& get_db() {
        return _db;
    }

    void init_messaging_service();

    // Applies mutation on this node.
    // Resolves with timed_out_error when timeout is reached.
    future<> mutate_locally(const mutation& m, clock_type::time_point timeout = clock_type::time_point::max());
    // Applies mutation on this node.
    // Resolves with timed_out_error when timeout is reached.
    future<> mutate_locally(const schema_ptr&, const frozen_mutation& m, clock_type::time_point timeout = clock_type::time_point::max());
    // Applies mutations on this node.
    // Resolves with timed_out_error when timeout is reached.
    future<> mutate_locally(std::vector<mutation> mutation, clock_type::time_point timeout = clock_type::time_point::max());

    future<> mutate_streaming_mutation(const schema_ptr&, utils::UUID plan_id, const frozen_mutation& m, bool fragmented);

    /**
    * Use this method to have these Mutations applied
    * across all replicas. This method will take care
    * of the possibility of a replica being down and hint
    * the data across to some other replica.
    *
    * @param mutations the mutations to be applied across the replicas
    * @param consistency_level the consistency level for the operation
    * @param tr_state trace state handle
    */
    future<> mutate(std::vector<mutation> mutations, db::consistency_level cl, tracing::trace_state_ptr tr_state, bool raw_counters = false);

    future<> replicate_counter_from_leader(mutation m, db::consistency_level cl, tracing::trace_state_ptr tr_state,
                                           clock_type::time_point timeout);

    template<typename Range>
    future<> mutate_counters(Range&& mutations, db::consistency_level cl, tracing::trace_state_ptr tr_state);

    future<> mutate_with_triggers(std::vector<mutation> mutations, db::consistency_level cl,
                                  bool should_mutate_atomically, tracing::trace_state_ptr tr_state, bool raw_counters = false);

    /**
    * See mutate. Adds additional steps before and after writing a batch.
    * Before writing the batch (but after doing availability check against the FD for the row replicas):
    *      write the entire batch to a batchlog elsewhere in the cluster.
    * After: remove the batchlog entry (after writing hints for the batch rows, if necessary).
    *
    * @param mutations the Mutations to be applied across the replicas
    * @param consistency_level the consistency level for the operation
    * @param tr_state trace state handle
    */
    future<> mutate_atomically(std::vector<mutation> mutations, db::consistency_level cl, tracing::trace_state_ptr tr_state);

    // Send a mutation to one specific remote target.
    // Inspired by Cassandra's StorageProxy.sendToHintedEndpoints but without
    // hinted handoff support, and just one target. See also
    // send_to_live_endpoints() - another take on the same original function.
    future<> send_to_endpoint(mutation m, gms::inet_address target, db::write_type type);

    /**
     * Performs the truncate operatoin, which effectively deletes all data from
     * the column family cfname
     * @param keyspace
     * @param cfname
     */
    future<> truncate_blocking(sstring keyspace, sstring cfname);

    /*
     * Executes data query on the whole cluster.
     *
     * Partitions for each range will be ordered according to decorated_key ordering. Results for
     * each range from "partition_ranges" may appear in any order.
     *
     * IMPORTANT: Not all fibers started by this method have to be done by the time it returns so no
     * parameter can be changed after being passed to this method.
     */
    future<foreign_ptr<lw_shared_ptr<query::result>>> query(schema_ptr,
        lw_shared_ptr<query::read_command> cmd,
        dht::partition_range_vector&& partition_ranges,
        db::consistency_level cl,
        tracing::trace_state_ptr trace_state);

    future<foreign_ptr<lw_shared_ptr<reconcilable_result>>, cache_temperature> query_mutations_locally(
        schema_ptr, lw_shared_ptr<query::read_command> cmd, const dht::partition_range&,
        tracing::trace_state_ptr trace_state = nullptr,
        uint64_t max_size = query::result_memory_limiter::maximum_result_size);


    future<foreign_ptr<lw_shared_ptr<reconcilable_result>>, cache_temperature> query_mutations_locally(
        schema_ptr, lw_shared_ptr<query::read_command> cmd, const compat::one_or_two_partition_ranges&,
        tracing::trace_state_ptr trace_state = nullptr,
        uint64_t max_size = query::result_memory_limiter::maximum_result_size);

    future<foreign_ptr<lw_shared_ptr<reconcilable_result>>, cache_temperature> query_mutations_locally(
            schema_ptr s, lw_shared_ptr<query::read_command> cmd, const dht::partition_range_vector& pr,
            tracing::trace_state_ptr trace_state = nullptr,
            uint64_t max_size = query::result_memory_limiter::maximum_result_size);


    future<> stop();

    const stats& get_stats() const {
        return _stats;
    }

    friend class abstract_read_executor;
    friend class abstract_write_response_handler;
};

extern distributed<storage_proxy> _the_storage_proxy;

inline distributed<storage_proxy>& get_storage_proxy() {
    return _the_storage_proxy;
}

inline storage_proxy& get_local_storage_proxy() {
    return _the_storage_proxy.local();
}

inline shared_ptr<storage_proxy> get_local_shared_storage_proxy() {
    return _the_storage_proxy.local_shared();
}

dht::partition_range_vector get_restricted_ranges(locator::token_metadata&,
    const schema&, dht::partition_range);

}
