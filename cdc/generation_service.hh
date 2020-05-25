/*
 * Copyright (C) 2020 ScyllaDB
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

#include <seastar/core/semaphore.hh>

#include "gms/i_endpoint_state_change_subscriber.hh"

#include "metadata.hh"
#include "generation.hh"

namespace gms {
class feature_service;
}

namespace cdc {

/* Managing CDC generations:
 * - Creating a new generation when joining a cluster or performing a rolling upgrade.
 * - Listening to generation changes.
 * - Communicating generations between nodes.
 *
 * This service really runs its logic only on shard 0, but it stores and updates
 * a cdc::metadata instance on each shard for efficient access by the log service.
 */
class generation_service : public enable_shared_from_this<cdc::generation_service>,
                           public peering_sharded_service<cdc::generation_service>,
                           public gms::i_endpoint_state_change_subscriber {

    /* The highest generation timestamp that we know of.
     * We keep this variable synchronized with our local tables (system.cdc_local)
     * and the timestamp that we're gossiping (CDC_STREAMS_TIMESTAMP). */
    std::optional<db_clock::time_point> _current_ts;

    /* We update _current_ts together with gossiper and system keyspace.
     * We want these three updates to happen atomically if multiple timestamp changes
     * are detected concurrently (should not happen). */
    semaphore _current_ts_sem{1};

    cdc::metadata _metadata;

    bool _stopped = false;

    /* We will be updating the gossiper with the right CDC generation timestamp
     * and listening for other nodes changing their gossiped timestamps. */
    gms::gossiper& _gossiper;

    /* We will be inserting and retrieving generation descriptions
     * to/from the system_distributed.cdc_topology_description table,
     * and updating the user-facing system_distributed.cdc_description table with appropriate
     * lists of streams. */
    sharded<db::system_distributed_keyspace>& _sys_dist_ks;

    /* Some of our tasks will be sleeping. We need the abort source in case of shutting down. */
    abort_source& _abort_source;

    const db::config& _db_cfg;
    const locator::token_metadata& _ring;

    /* To keep the service from stopping if there are any asynchronous tasks still running,
     * such as learning a generation or updating the streams description table. */
    gate _async_gate;

public:
    // See comment in main.cc for the reason of taking sharded<db::sys_dist_ks>& instead of simply db::sys_dist_ks&.
    generation_service(gms::gossiper&, sharded<db::system_distributed_keyspace>&,
            abort_source&, const db::config&, const locator::token_metadata&);

    future<> stop();
    ~generation_service();

    /* This function must be called when the node is bootstrapping, after we have learned the tokens
     * of existing nodes but before we start gossiping our own tokens.
     *
     * It assumes that _ring contains tokens of other nodes in the cluster. It needs bootstrap_tokens:
     * the tokens that this node has chosen.
     *
     * It creates a new CDC generation which includes tokens of all nodes, including ours.
     * */
    void before_join_token_ring(
            const std::unordered_set<dht::token>& bootstrap_tokens,
            const gms::feature_service&, bool in_single_node_test);

    /* This function must be called after the node has started and went into NORMAL status.
     * It enables the node to listen to generation changes in the background.
     *
     * It also handles rolling upgrade and the case of creating a new cluster:
     * in both these cases we need to create the first CDC generation. */
    void after_join(bool in_single_node_test);


    cdc::metadata& get_cdc_metadata() {
        return _metadata;
    }

    /* Gossiper notifications */
    virtual void before_change(gms::inet_address, gms::endpoint_state,
            gms::application_state, const gms::versioned_value&) override {}
    virtual void on_alive(gms::inet_address endpoint, gms::endpoint_state state) override {}
    virtual void on_dead(gms::inet_address endpoint, gms::endpoint_state state) override {}
    virtual void on_remove(gms::inet_address endpoint) override {}
    virtual void on_restart(gms::inet_address endpoint, gms::endpoint_state state) override {}

    virtual void on_join(gms::inet_address, gms::endpoint_state) override;
    virtual void on_change(gms::inet_address, gms::application_state, const gms::versioned_value&) override;

private:
    void new_generation(const std::unordered_set<dht::token>& bootstrap_tokens, bool in_single_node_test);

    void learn_generation(db_clock::time_point);
    bool do_learn_generation(db_clock::time_point);

    void update_generation_timestamp(db_clock::time_point);
    void restore_generation_timestamp();
};

}
