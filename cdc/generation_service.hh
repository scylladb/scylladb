/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * Modified by ScyllaDB
 * Copyright (C) 2021-present ScyllaDB
 *
 */

#pragma once

#include <seastar/core/sharded.hh>
#include "cdc/metadata.hh"
#include "cdc/generation_id.hh"
#include "gms/i_endpoint_state_change_subscriber.hh"

namespace db {
class system_distributed_keyspace;
class system_keyspace;
}

namespace gms {
class gossiper;
class feature_service;
}

namespace seastar {
class abort_source;
}

namespace locator {
class shared_token_metadata;
}

namespace cdc {

class generation_service : public peering_sharded_service<generation_service>
                         , public async_sharded_service<generation_service>
                         , public gms::i_endpoint_state_change_subscriber {
public:
    struct config {
        unsigned ignore_msb_bits;
        std::chrono::milliseconds ring_delay;
        bool dont_rewrite_streams = false;
    };

private:
    bool _stopped = false;

    // The node has joined the token ring. Set to `true` on `after_join` call.
    bool _joined = false;

    config _cfg;
    gms::gossiper& _gossiper;
    sharded<db::system_distributed_keyspace>& _sys_dist_ks;
    sharded<db::system_keyspace>& _sys_ks;
    abort_source& _abort_src;
    const locator::shared_token_metadata& _token_metadata;
    gms::feature_service& _feature_service;
    replica::database& _db;

    /* Maintains the set of known CDC generations used to pick streams for log writes (i.e., the partition keys of these log writes).
     * Updated in response to certain gossip events (see the handle_cdc_generation function).
     */
    cdc::metadata _cdc_metadata;

    /* The latest known generation timestamp and the timestamp that we're currently gossiping
     * (as CDC_GENERATION_ID application state).
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
    std::optional<cdc::generation_id> _gen_id;
    future<> _cdc_streams_rewrite_complete = make_ready_future<>();

    /* Returns true if raft topology changes are enabled.
     * Can only be called from shard 0.
     */
    std::function<bool()> _raft_topology_change_enabled;
public:
    generation_service(config cfg, gms::gossiper&,
            sharded<db::system_distributed_keyspace>&,
            sharded<db::system_keyspace>& sys_ks,
            abort_source&, const locator::shared_token_metadata&,
            gms::feature_service&, replica::database& db,
            std::function<bool()> raft_topology_change_enabled);

    future<> stop();
    ~generation_service();

    /* After the node bootstraps and creates a new CDC generation, or restarts and loads the last
     * known generation timestamp from persistent storage, this function should be called with
     * that generation timestamp moved in as the `startup_gen_id` parameter.
     * This passes the responsibility of managing generations from the node startup code to this service;
     * until then, the service remains dormant.
     * The startup code is in `storage_service::join_topology`, hence
     * `after_join` should be called at the end of that function.
     * Precondition: the node has completed bootstrapping and system_distributed_keyspace is initialized.
     * Must be called on shard 0 - that's where the generation management happens.
     */
    future<> after_join(std::optional<cdc::generation_id>&& startup_gen_id);
    future<> leave_ring();

    cdc::metadata& get_cdc_metadata() {
        return _cdc_metadata;
    }

    virtual future<> on_alive(gms::inet_address, gms::endpoint_state_ptr, gms::permit_id) override { return make_ready_future(); }
    virtual future<> on_dead(gms::inet_address, gms::endpoint_state_ptr, gms::permit_id) override { return make_ready_future(); }
    virtual future<> on_remove(gms::inet_address, gms::permit_id) override { return make_ready_future(); }
    virtual future<> on_restart(gms::inet_address, gms::endpoint_state_ptr, gms::permit_id) override { return make_ready_future(); }

    virtual future<> on_join(gms::inet_address, gms::endpoint_state_ptr, gms::permit_id) override;
    virtual future<> on_change(gms::inet_address, const gms::application_state_map&, gms::permit_id) override;

    future<> check_and_repair_cdc_streams();

    /* Generate a new set of CDC streams and insert it into the internal distributed CDC generations table.
     * Returns the ID of this new generation.
     *
     * Should be called when starting the node for the first time (i.e., joining the ring).
     *
     * Assumes that the system_distributed_keyspace service is initialized.
     * `cluster_supports_generations_v2` must be `true` if and only if the `CDC_GENERATIONS_V2` feature is enabled.
     *
     * If `CDC_GENERATIONS_V2` is enabled, the new generation will be inserted into
     * `system_distributed_everywhere.cdc_generation_descriptions_v2` and the returned ID will be in the v2 format.
     * Otherwise the new generation will be limited in size, causing suboptimal stream distribution, it will be inserted
     * into `system_distributed.cdc_generation_descriptions` and the returned ID will be in the v1 format.
     * The second case should happen only when we create new generations in a mixed cluster.
     *
     * The caller of this function is expected to insert the ID into the gossiper as fast as possible,
     * so that other nodes learn about the generation before their clocks cross the generation's timestamp
     * (not guaranteed in the current implementation, but expected to be the common case;
     *  we assume that `ring_delay` is enough for other nodes to learn about the new generation).
     *
     * Legacy: used for gossiper-based topology changes.
     */
    future<cdc::generation_id> legacy_make_new_generation(
        const std::unordered_set<dht::token>& bootstrap_tokens, bool add_delay);

    /* Retrieve the CDC generation with the given ID from local tables
     * and start using it for CDC log writes if it's not obsolete.
     * Precondition: the generation was committed using group 0 and locally applied.
     */
    future<> handle_cdc_generation(cdc::generation_id_v2);

private:
    /* Retrieve the CDC generation which starts at the given timestamp (from a distributed table created for this purpose)
     * and start using it for CDC log writes if it's not obsolete.
     *
     * Legacy: used for gossiper-based topology changes.
     */
    future<> legacy_handle_cdc_generation(std::optional<cdc::generation_id>);

    /* If `legacy_handle_cdc_generation` fails, it schedules an asynchronous retry in the background
     * using `legacy_async_handle_cdc_generation`.
     *
     * Legacy: used for gossiper-based topology changes.
     */
    void legacy_async_handle_cdc_generation(cdc::generation_id);

    /* Wrapper around `legacy_do_handle_cdc_generation` which intercepts timeout/unavailability exceptions.
     * Returns: legacy_do_handle_cdc_generation(ts).
     *
     * Legacy: used for gossiper-based topology changes.
     */
    future<bool> legacy_do_handle_cdc_generation_intercept_nonfatal_errors(cdc::generation_id);

    /* Returns `true` iff we started using the generation (it was not obsolete or already known),
     * which means that this node might write some CDC log entries using streams from this generation.
     *
     * Legacy: used for gossiper-based topology changes.
     */
    future<bool> legacy_do_handle_cdc_generation(cdc::generation_id);

    /* Scan CDC generation timestamps gossiped by other nodes and retrieve the latest one.
     * This function should be called once at the end of the node startup procedure
     * (after the node is started and running normally, it will retrieve generations on gossip events instead).
     *
     * Legacy: used for gossiper-based topology changes.
     */
    future<> legacy_scan_cdc_generations();

    /* generation_service code might be racing with system_distributed_keyspace deinitialization
     * (the deinitialization order is broken).
     * Therefore, whenever we want to access sys_dist_ks in a background task,
     * we need to check if the instance is still there. Storing the shared pointer will keep it alive.
     */
    shared_ptr<db::system_distributed_keyspace> get_sys_dist_ks();

    /* Part of the upgrade procedure. Useful in case where the version of Scylla that we're upgrading from
     * used the "cdc_streams_descriptions" table. This procedure ensures that the new "cdc_streams_descriptions_v2"
     * table contains streams of all generations that were present in the old table and may still contain data
     * (i.e. there exist CDC log tables that may contain rows with partition keys being the stream IDs from
     * these generations). */
    future<> maybe_rewrite_streams_descriptions();
};

} // namespace cdc
