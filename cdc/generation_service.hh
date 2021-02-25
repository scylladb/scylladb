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
 *
 * Modified by ScyllaDB
 * Copyright (C) 2021 ScyllaDB
 *
 */

#pragma once

#include "cdc/metadata.hh"
#include "gms/i_endpoint_state_change_subscriber.hh"

namespace db {
class system_distributed_keyspace;
}

namespace gms {
class gossiper;
}

namespace cdc {

class generation_service : public peering_sharded_service<generation_service>
                         , public async_sharded_service<generation_service>
                         , public gms::i_endpoint_state_change_subscriber {

    bool _stopped = false;

    // The node has joined the token ring. Set to `true` on `after_join` call.
    bool _joined = false;

    const db::config& _cfg;
    gms::gossiper& _gossiper;
    sharded<db::system_distributed_keyspace>& _sys_dist_ks;
    abort_source& _abort_src;
    const locator::shared_token_metadata& _token_metadata;
    netw::messaging_service& _ms;

    /* Maintains the set of known CDC generations used to pick streams for log writes (i.e., the partition keys of these log writes).
     * Updated in response to certain gossip events (see the handle_cdc_generation function).
     */
    cdc::metadata _cdc_metadata;

    /* The latest known generation timestamp and the timestamp that we're currently gossiping
     * (as CDC_STREAMS_TIMESTAMP application state).
     *
     * Only shard 0 manages this, hence it will be std::nullopt on all shards other than 0.
     * This timestamp is also persisted in the system.cdc_local table.
     *
     * On shard 0 this may be nullopt only in one special case: rolling upgrade, when we upgrade
     * from an old version of Scylla that didn't support CDC. In that case one node in the cluster
     * will create the first generation and start gossiping it; it may be us, or it may be some
     * different node. In any case, eventually - after one of the nodes gossips the first timestamp
     * - we'll catch on and this variable will be updated with that generation.
     */
    std::optional<db_clock::time_point> _gen_ts;
public:
    generation_service(const db::config&, gms::gossiper&,
            sharded<db::system_distributed_keyspace>&, abort_source&, const locator::shared_token_metadata&,
            netw::messaging_service&);

    /* This function initializes RPC handlers for calls that may start arriving as soon as we join
     * the token ring (or in the middle of it). Hence the function must be called before we join
     * the token ring. */
    void initialize();

    future<> stop();
    ~generation_service();

    /* After the node bootstraps and creates a new CDC generation, or restarts and loads the last
     * known generation timestamp from persistent storage, this function should be called with
     * that generation timestamp moved in as the `startup_gen_ts` parameter.
     * This passes the responsibility of managing generations from the node startup code to this service;
     * until then, the service remains dormant (except for answering generation fetch requests).
     * At the time of writing this comment, the startup code is in `storage_service::join_token_ring`, hence
     * `after_join` should be called at the end of that function.
     * Precondition: the node has completed bootstrapping and system_distributed_keyspace is initialized.
     * Must be called on shard 0 - that's where the generation management happens.
     */
    future<> after_join(std::optional<db_clock::time_point>&& startup_gen_ts);

    cdc::metadata& get_cdc_metadata() {
        return _cdc_metadata;
    }

    virtual void before_change(gms::inet_address, gms::endpoint_state, gms::application_state, const gms::versioned_value&) override {}
    virtual void on_alive(gms::inet_address, gms::endpoint_state) override {}
    virtual void on_dead(gms::inet_address, gms::endpoint_state) override {}
    virtual void on_remove(gms::inet_address) override {}
    virtual void on_restart(gms::inet_address, gms::endpoint_state) override {}

    virtual void on_join(gms::inet_address, gms::endpoint_state) override;
    virtual void on_change(gms::inet_address, gms::application_state, const gms::versioned_value&) override;

    future<> check_and_repair_cdc_streams();

private:
    /* Retrieve the CDC generation which starts at the given timestamp (from a distributed table created for this purpose)
     * and start using it for CDC log writes if it's not obsolete.
     */
    future<> handle_cdc_generation(std::optional<db_clock::time_point>);

    /* If `handle_cdc_generation` fails, it schedules an asynchronous retry in the background
     * using `async_handle_cdc_generation`.
     */
    void async_handle_cdc_generation(db_clock::time_point);

    /* Wrapper around `do_handle_cdc_generation` which intercepts timeout/unavailability exceptions.
     * Returns: do_handle_cdc_generation(ts). */
    future<bool> do_handle_cdc_generation_intercept_nonfatal_errors(db_clock::time_point);

    /* Returns `true` iff we started using the generation (it was not obsolete or already known),
     * which means that this node might write some CDC log entries using streams from this generation. */
    future<bool> do_handle_cdc_generation(db_clock::time_point);

    /* Scan CDC generation timestamps gossiped by other nodes and retrieve the latest one.
     * This function should be called once at the end of the node startup procedure
     * (after the node is started and running normally, it will retrieve generations on gossip events instead).
     */
    future<> scan_cdc_generations();

    /* generation_service code might be racing with system_distributed_keyspace deinitialization
     * (the deinitialization order is broken).
     * Therefore, whenever we want to access sys_dist_ks in a background task,
     * we need to check if the instance is still there. Storing the shared pointer will keep it alive.
     */
    shared_ptr<db::system_distributed_keyspace> get_sys_dist_ks();
};

} // namespace cdc
