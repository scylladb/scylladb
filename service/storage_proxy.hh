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
#include "sstables/estimated_histogram.hh"

namespace service {

class abstract_write_response_handler;
class abstract_read_executor;

class storage_proxy : public seastar::async_sharded_service<storage_proxy> /*implements StorageProxyMBean*/ {
    struct rh_entry {
        std::unique_ptr<abstract_write_response_handler> handler;
        timer<> expire_timer;
        rh_entry(std::unique_ptr<abstract_write_response_handler>&& h, std::function<void()>&& cb);
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

public:
    struct stats {
        uint64_t read_timeouts = 0;
        uint64_t read_unavailables = 0;
        uint64_t range_slice_timeouts = 0;
        uint64_t range_slice_unavailables = 0;
        uint64_t write_timeouts = 0;
        uint64_t write_unavailables = 0;
        uint64_t read_repair_attempts = 0;
        uint64_t read_repair_repaired_blocking = 0;
        uint64_t read_repair_repaired_background = 0;
        utils::ihistogram read;
        utils::ihistogram write;
        utils::ihistogram range;
        sstables::estimated_histogram estimated_read;
        sstables::estimated_histogram estimated_write;
        sstables::estimated_histogram estimated_range;
        uint64_t background_writes = 0; // client no longer waits for the write
        uint64_t reads = 0;
        uint64_t background_reads = 0; // client no longer waits for the read
    };
private:
    distributed<database>& _db;
    response_id_type _next_response_id = 1; // 0 is reserved for unique_response_handler
    std::unordered_map<response_id_type, rh_entry> _response_handlers;
    constexpr static size_t _max_hints_in_progress = 128; // origin multiplies by FBUtilities.getAvailableProcessors() but we already sharded
    size_t _total_hints_in_progress = 0;
    std::unordered_map<gms::inet_address, size_t> _hints_in_progress;
    stats _stats;
    static constexpr float CONCURRENT_SUBREQUESTS_MARGIN = 0.10;
    // for read repair chance calculation
    std::default_random_engine _urandom;
    std::uniform_real_distribution<> _read_repair_chance = std::uniform_real_distribution<>(0,1);
    std::unique_ptr<scollectd::registrations> _collectd_registrations;
private:
    void init_messaging_service();
    void uninit_messaging_service();
    future<foreign_ptr<lw_shared_ptr<query::result>>> query_singular(lw_shared_ptr<query::read_command> cmd, std::vector<query::partition_range>&& partition_ranges, db::consistency_level cl);
    response_id_type register_response_handler(std::unique_ptr<abstract_write_response_handler>&& h);
    void remove_response_handler(response_id_type id);
    void got_response(response_id_type id, gms::inet_address from);
    future<> response_wait(response_id_type id);
    abstract_write_response_handler& get_write_response_handler(storage_proxy::response_id_type id);
    response_id_type create_write_response_handler(keyspace& ks, db::consistency_level cl, db::write_type type, frozen_mutation&& mutation, std::unordered_set<gms::inet_address> targets,
            const std::vector<gms::inet_address>& pending_endpoints, std::vector<gms::inet_address>);
    response_id_type create_write_response_handler(const mutation&, db::consistency_level cl, db::write_type type);
    future<> send_to_live_endpoints(response_id_type response_id);
    template<typename Range>
    size_t hint_to_dead_endpoints(lw_shared_ptr<const frozen_mutation> m, const Range& targets);
    void hint_to_dead_endpoints(response_id_type, db::consistency_level);
    bool cannot_hint(gms::inet_address target);
    size_t get_hints_in_progress_for(gms::inet_address target);
    bool should_hint(gms::inet_address ep);
    bool submit_hint(lw_shared_ptr<const frozen_mutation> m, gms::inet_address target);
    std::vector<gms::inet_address> get_live_sorted_endpoints(keyspace& ks, const dht::token& token);
    db::read_repair_decision new_read_repair_decision(const schema& s);
    ::shared_ptr<abstract_read_executor> get_read_executor(lw_shared_ptr<query::read_command> cmd, query::partition_range pr, db::consistency_level cl);
    future<foreign_ptr<lw_shared_ptr<query::result>>> query_singular_local(lw_shared_ptr<query::read_command> cmd, const query::partition_range& pr);
    future<query::result_digest> query_singular_local_digest(lw_shared_ptr<query::read_command> cmd, const query::partition_range& pr);
    future<foreign_ptr<lw_shared_ptr<query::result>>> query_partition_key_range(lw_shared_ptr<query::read_command> cmd, query::partition_range&& range, db::consistency_level cl);
    std::vector<query::partition_range> get_restricted_ranges(keyspace& ks, const schema& s, query::partition_range range);
    float estimate_result_rows_per_range(lw_shared_ptr<query::read_command> cmd, keyspace& ks);
    static std::vector<gms::inet_address> intersection(const std::vector<gms::inet_address>& l1, const std::vector<gms::inet_address>& l2);
    future<std::vector<foreign_ptr<lw_shared_ptr<query::result>>>> query_partition_key_range_concurrent(std::chrono::high_resolution_clock::time_point timeout,
            std::vector<foreign_ptr<lw_shared_ptr<query::result>>>&& results, lw_shared_ptr<query::read_command> cmd, db::consistency_level cl, std::vector<query::partition_range>::iterator&& i,
            std::vector<query::partition_range>&& ranges, int concurrency_factor);

    future<foreign_ptr<lw_shared_ptr<query::result>>> do_query(schema_ptr,
        lw_shared_ptr<query::read_command> cmd,
        std::vector<query::partition_range>&& partition_ranges,
        db::consistency_level cl);
    template<typename Range, typename CreateWriteHandler>
    future<std::vector<unique_response_handler>> mutate_prepare(const Range& mutations, db::consistency_level cl, db::write_type type, CreateWriteHandler handler);
    future<std::vector<unique_response_handler>> mutate_prepare(std::vector<mutation>& mutations, db::consistency_level cl, db::write_type type);
    future<> mutate_begin(std::vector<unique_response_handler> ids, db::consistency_level cl);
    future<> mutate_end(future<> mutate_result, utils::latency_counter);
    future<> schedule_repair(std::unordered_map<gms::inet_address, std::vector<mutation>> diffs);

public:
    storage_proxy(distributed<database>& db);
    ~storage_proxy();
    distributed<database>& get_db() {
        return _db;
    }

    future<> mutate_locally(const mutation& m);
    future<> mutate_locally(const frozen_mutation& m);
    future<> mutate_locally(std::vector<mutation> mutations);

    /**
    * Use this method to have these Mutations applied
    * across all replicas. This method will take care
    * of the possibility of a replica being down and hint
    * the data across to some other replica.
    *
    * @param mutations the mutations to be applied across the replicas
    * @param consistency_level the consistency level for the operation
    */
    future<> mutate(std::vector<mutation> mutations, db::consistency_level cl);

    future<> mutate_with_triggers(std::vector<mutation> mutations, db::consistency_level cl,
        bool should_mutate_atomically);

    /**
    * See mutate. Adds additional steps before and after writing a batch.
    * Before writing the batch (but after doing availability check against the FD for the row replicas):
    *      write the entire batch to a batchlog elsewhere in the cluster.
    * After: remove the batchlog entry (after writing hints for the batch rows, if necessary).
    *
    * @param mutations the Mutations to be applied across the replicas
    * @param consistency_level the consistency level for the operation
    */
    future<> mutate_atomically(std::vector<mutation> mutations, db::consistency_level cl);

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
     */
    future<foreign_ptr<lw_shared_ptr<query::result>>> query(schema_ptr,
        lw_shared_ptr<query::read_command> cmd,
        std::vector<query::partition_range>&& partition_ranges,
        db::consistency_level cl);

    future<foreign_ptr<lw_shared_ptr<reconcilable_result>>> query_mutations_locally(
        lw_shared_ptr<query::read_command> cmd, const query::partition_range&);

    /*
     * Returns mutation_reader for given column family
     * which combines data from all shards.
     */
    mutation_reader make_local_reader(utils::UUID cf_id, const query::partition_range&);

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

std::vector<query::partition_range> get_restricted_ranges(locator::token_metadata&,
    const schema&, query::partition_range);

}
