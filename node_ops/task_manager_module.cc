/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "cdc/generation_service.hh"
#include "db/batchlog_manager.hh"
#include "db/config.hh"
#include "db/schema_tables.hh"
#include "db/system_distributed_keyspace.hh"
#include "dht/boot_strapper.hh"
#include "dht/range_streamer.hh"
#include "gms/gossiper.hh"
#include "node_ops/task_manager_module.hh"
#include "repair/row_level.hh"
#include "service/raft/raft_group0.hh"
#include "service/storage_service.hh"
#include "supervisor.hh"
#include "utils/error_injection.hh"

using namespace std::chrono_literals;

using versioned_value = gms::versioned_value;

extern logging::logger cdc_log;
namespace node_ops {

node_ops_task_impl::node_ops_task_impl(tasks::task_manager::module_ptr module,
        tasks::task_id id,
        unsigned sequence_number,
        std::string scope,
        std::string entity,
        tasks::task_id parent_id,
        streaming::stream_reason reason,
        service::storage_service& ss) noexcept
    : tasks::task_manager::task::impl(std::move(module), id, sequence_number,
        std::move(scope), "", "", std::move(entity), parent_id)
    , _reason(reason)
    , _ss(ss)
{
    // FIXME: add progress units
}

std::string node_ops_task_impl::type() const {
    return fmt::format("{}", _reason);
}

join_token_ring_task_impl::join_token_ring_task_impl(tasks::task_manager::module_ptr module,
        std::string entity,
        service::storage_service& ss,
        sharded<db::system_distributed_keyspace>& sys_dist_ks,
        sharded<service::storage_proxy>& proxy,
        std::unordered_set<gms::inet_address> initial_contact_nodes,
        std::unordered_set<gms::inet_address> loaded_endpoints,
        std::unordered_map<gms::inet_address, sstring> loaded_peer_features,
        std::chrono::milliseconds delay) noexcept
    : node_ops_task_impl(std::move(module), tasks::task_id::create_random_id(), ss.get_task_manager_module().new_sequence_number(),
        "node", std::move(entity), tasks::task_id::create_null_id(), streaming::stream_reason::bootstrap, ss)
    , _sys_dist_ks(sys_dist_ks)
    , _proxy(proxy)
    , _initial_contact_nodes(std::move(initial_contact_nodes))
    , _loaded_endpoints(std::move(loaded_endpoints))
    , _loaded_peer_features(std::move(loaded_peer_features))
    , _delay(delay)
{}

gms::inet_address join_token_ring_task_impl::get_broadcast_address() const {
    return _ss.get_broadcast_address();
}

bool join_token_ring_task_impl::should_bootstrap() {
    return _ss.should_bootstrap();
}

bool join_token_ring_task_impl::is_replacing() {
    return _ss.is_replacing();
}

locator::token_metadata_ptr join_token_ring_task_impl::get_token_metadata_ptr() const noexcept {
    return _ss.get_token_metadata_ptr();
}
const locator::token_metadata& join_token_ring_task_impl::get_token_metadata() const noexcept {
    return _ss.get_token_metadata();
}

future<> join_token_ring_task_impl::run() {
    auto& sys_ks = _ss._sys_ks;
    auto& db = _ss._db;
    auto& gossiper = _ss._gossiper;
    auto& raft_topology_change_enabled = _ss._raft_topology_change_enabled;
    auto& snitch = _ss._snitch;
    auto& feature_service = _ss._feature_service;
    auto& group0 = _ss._group0;
    std::unordered_set<dht::token> bootstrap_tokens;
    std::map<gms::application_state, gms::versioned_value> app_states;
    /* The timestamp of the CDC streams generation that this node has proposed when joining.
     * This value is nullopt only when:
     * 1. this node is being upgraded from a non-CDC version,
     * 2. this node is starting for the first time or restarting with CDC previously disabled,
     *    in which case the value should become populated before we leave the join_token_ring procedure.
     *
     * Important: this variable is using only during the startup procedure. It is moved out from
     * at the end of `join_token_ring`; the responsibility handling of CDC generations is passed
     * to cdc::generation_service.
     *
     * DO NOT use this variable after `join_token_ring` (i.e. after we call `generation_service::after_join`
     * and pass it the ownership of the timestamp.
     */
    std::optional<cdc::generation_id> cdc_gen_id;

    if (sys_ks.local().was_decommissioned()) {
        if (db.local().get_config().override_decommission() && !db.local().get_config().consistent_cluster_management()) {
            tasks::tmlogger.warn("This node was decommissioned, but overriding by operator request.");
            co_await sys_ks.local().set_bootstrap_state(db::system_keyspace::bootstrap_state::COMPLETED);
        } else {
            auto msg = sstring("This node was decommissioned and will not rejoin the ring unless override_decommission=true has been set and consistent cluster management is not in use,"
                               "or all existing data is removed and the node is bootstrapped again");
            tasks::tmlogger.error("{}", msg);
            throw std::runtime_error(msg);
        }
    }

    bool replacing_a_node_with_same_ip = false;
    bool replacing_a_node_with_diff_ip = false;
    std::optional<service::storage_service::replacement_info> ri;
    std::optional<gms::inet_address> replace_address;
    std::optional<locator::host_id> replaced_host_id;
    std::optional<service::raft_group0::replace_info> raft_replace_info;
    auto tmlock = std::make_unique<locator::token_metadata_lock>(co_await _ss.get_token_metadata_lock());
    auto tmptr = co_await _ss.get_mutable_token_metadata_ptr();
    if (is_replacing()) {
        if (sys_ks.local().bootstrap_complete()) {
            throw std::runtime_error("Cannot replace address with a node that is already bootstrapped");
        }
        ri = co_await _ss.prepare_replacement_info(_initial_contact_nodes, _loaded_peer_features);
        replace_address = ri->address;
        raft_replace_info = service::raft_group0::replace_info {
            .ip_addr = *replace_address,
            .raft_id = raft::server_id{ri->host_id.uuid()},
        };
        if (!raft_topology_change_enabled) {
            bootstrap_tokens = std::move(ri->tokens);
            replacing_a_node_with_same_ip = *replace_address == get_broadcast_address();
            replacing_a_node_with_diff_ip = *replace_address != get_broadcast_address();

            tasks::tmlogger.info("Replacing a node with {} IP address, my address={}, node being replaced={}",
                get_broadcast_address() == *replace_address ? "the same" : "a different",
                get_broadcast_address(), *replace_address);
            tmptr->update_topology(*replace_address, std::move(ri->dc_rack), locator::node::state::being_replaced);
            co_await tmptr->update_normal_tokens(bootstrap_tokens, *replace_address);
            replaced_host_id = ri->host_id;
        }
    } else if (should_bootstrap()) {
        co_await _ss.check_for_endpoint_collision(_initial_contact_nodes, _loaded_peer_features);
    } else {
        auto local_features = feature_service.supported_feature_set();
        tasks::tmlogger.info("Checking remote features with gossip, initial_contact_nodes={}", _initial_contact_nodes);
        co_await gossiper.do_shadow_round(_initial_contact_nodes);
        gossiper.check_knows_remote_features(local_features, _loaded_peer_features);
        gossiper.check_snitch_name_matches(snitch.local()->get_name());
        // Check if the node is already removed from the cluster
        auto local_host_id = get_token_metadata().get_my_id();
        auto my_ip = get_broadcast_address();
        if (!gossiper.is_safe_for_restart(my_ip, local_host_id)) {
            throw std::runtime_error(::format("The node {} with host_id {} is removed from the cluster. Can not restart the removed node to join the cluster again!",
                    my_ip, local_host_id));
        }
        co_await gossiper.reset_endpoint_state_map();
        for (auto ep : _loaded_endpoints) {
            co_await gossiper.add_saved_endpoint(ep);
        }
    }
    auto features = feature_service.supported_feature_set();
    tasks::tmlogger.info("Save advertised features list in the 'system.{}' table", db::system_keyspace::LOCAL);
    // Save the advertised feature set to system.local table after
    // all remote feature checks are complete and after gossip shadow rounds are done.
    // At this point, the final feature set is already determined before the node joins the ring.
    co_await sys_ks.local().save_local_supported_features(features);

    // If this is a restarting node, we should update tokens before gossip starts
    auto my_tokens = co_await sys_ks.local().get_saved_tokens();
    bool restarting_normal_node = sys_ks.local().bootstrap_complete() && !is_replacing() && !my_tokens.empty();
    if (restarting_normal_node) {
        tasks::tmlogger.info("Restarting a node in NORMAL status");
        // This node must know about its chosen tokens before other nodes do
        // since they may start sending writes to this node after it gossips status = NORMAL.
        // Therefore we update _token_metadata now, before gossip starts.
        tmptr->update_topology(get_broadcast_address(), snitch.local()->get_location(), locator::node::state::normal);
        co_await tmptr->update_normal_tokens(my_tokens, get_broadcast_address());

        cdc_gen_id = co_await sys_ks.local().get_cdc_generation_id();
        if (!cdc_gen_id) {
            // We could not have completed joining if we didn't generate and persist a CDC streams timestamp,
            // unless we are restarting after upgrading from non-CDC supported version.
            // In that case we won't begin a CDC generation: it should be done by one of the nodes
            // after it learns that it everyone supports the CDC feature.
            cdc_log.warn(
                    "Restarting node in NORMAL status with CDC enabled, but no streams timestamp was proposed"
                    " by this node according to its local tables. Are we upgrading from a non-CDC supported version?");
        }
    }

    // have to start the gossip service before we can see any info on other nodes.  this is necessary
    // for bootstrap to get the load info it needs.
    // (we won't be part of the storage ring though until we add a counterId to our state, below.)
    // Seed the host ID-to-endpoint map with our own ID.
    auto local_host_id = get_token_metadata().get_my_id();
    if (!replacing_a_node_with_diff_ip) {
        auto endpoint = get_broadcast_address();
        auto eps = gossiper.get_endpoint_state_ptr(endpoint);
        if (eps) {
            auto replace_host_id = gossiper.get_host_id(get_broadcast_address());
            tasks::tmlogger.info("Host {}/{} is replacing {}/{} using the same address", local_host_id, endpoint, replace_host_id, endpoint);
        }
        tmptr->update_host_id(local_host_id, get_broadcast_address());
    }

    // Replicate the tokens early because once gossip runs other nodes
    // might send reads/writes to this node. Replicate it early to make
    // sure the tokens are valid on all the shards.
    co_await _ss.replicate_to_all_cores(std::move(tmptr));
    tmlock.reset();

    auto broadcast_rpc_address = utils::fb_utilities::get_broadcast_rpc_address();
    // Ensure we know our own actual Schema UUID in preparation for updates
    co_await db::schema_tables::recalculate_schema_version(sys_ks, _proxy, feature_service);

    app_states.emplace(gms::application_state::NET_VERSION, versioned_value::network_version());
    app_states.emplace(gms::application_state::HOST_ID, versioned_value::host_id(local_host_id));
    app_states.emplace(gms::application_state::RPC_ADDRESS, versioned_value::rpcaddress(broadcast_rpc_address));
    app_states.emplace(gms::application_state::RELEASE_VERSION, versioned_value::release_version());
    app_states.emplace(gms::application_state::SUPPORTED_FEATURES, versioned_value::supported_features(features));
    app_states.emplace(gms::application_state::CACHE_HITRATES, versioned_value::cache_hitrates(""));
    app_states.emplace(gms::application_state::SCHEMA_TABLES_VERSION, versioned_value(db::schema_tables::version));
    app_states.emplace(gms::application_state::RPC_READY, versioned_value::cql_ready(false));
    app_states.emplace(gms::application_state::VIEW_BACKLOG, versioned_value(""));
    app_states.emplace(gms::application_state::SCHEMA, versioned_value::schema(db.local().get_version()));
    if (restarting_normal_node) {
        // Order is important: both the CDC streams timestamp and tokens must be known when a node handles our status.
        // Exception: there might be no CDC streams timestamp proposed by us if we're upgrading from a non-CDC version.
        app_states.emplace(gms::application_state::TOKENS, versioned_value::tokens(my_tokens));
        app_states.emplace(gms::application_state::CDC_GENERATION_ID, versioned_value::cdc_generation_id(cdc_gen_id));
        app_states.emplace(gms::application_state::STATUS, versioned_value::normal(my_tokens));
    }
    if (replacing_a_node_with_same_ip || replacing_a_node_with_diff_ip) {
        app_states.emplace(gms::application_state::TOKENS, versioned_value::tokens(bootstrap_tokens));
    }
    app_states.emplace(gms::application_state::SNITCH_NAME, versioned_value::snitch_name(snitch.local()->get_name()));
    app_states.emplace(gms::application_state::SHARD_COUNT, versioned_value::shard_count(smp::count));
    app_states.emplace(gms::application_state::IGNORE_MSB_BITS, versioned_value::ignore_msb_bits(db.local().get_config().murmur3_partitioner_ignore_msb_bits()));

    for (auto&& s : snitch.local()->get_app_states()) {
        app_states.emplace(s.first, std::move(s.second));
    }

    auto schema_change_announce = db.local().observable_schema_version().observe([&ss = _ss] (table_schema_version schema_version) mutable {
        ss._migration_manager.local().passive_announce(std::move(schema_version));
    });

    _ss._listeners.emplace_back(make_lw_shared(std::move(schema_change_announce)));

    tasks::tmlogger.info("Starting up server gossip");

    auto generation_number = gms::generation_type(co_await sys_ks.local().increment_and_get_generation());
    auto advertise = gms::advertise_myself(!replacing_a_node_with_same_ip);
    co_await gossiper.start_gossiping(generation_number, app_states, advertise);

    if (!raft_topology_change_enabled && should_bootstrap()) {
        // Wait for NORMAL state handlers to finish for existing nodes now, so that connection dropping
        // (happening at the end of `handle_state_normal`: `notify_joined`) doesn't interrupt
        // group 0 joining or repair. (See #12764, #12956, #12972, #13302)
        //
        // But before we can do that, we must make sure that gossip sees at least one other node
        // and fetches the list of peers from it; otherwise `wait_for_normal_state_handled_on_boot`
        // may trivially finish without waiting for anyone.
        co_await gossiper.wait_for_live_nodes_to_show_up(2);

        // Note: in Raft topology mode this is unnecessary.
        // Node state changes are propagated to the cluster through explicit global barriers.
        co_await _ss.wait_for_normal_state_handled_on_boot();

        // NORMAL doesn't necessarily mean UP (#14042). Wait for these nodes to be UP as well
        // to reduce flakiness (we need them to be UP to perform CDC generation write and for repair/streaming).
        //
        // This could be done in Raft topology mode as well, but the calculation of nodes to sync with
        // has to be done based on topology state machine instead of gossiper as it is here;
        // furthermore, the place in the code where we do this has to be different (it has to be coordinated
        // by the topology coordinator after it joins the node to the cluster).
        //
        // We calculate nodes to wait for based on token_metadata. Previously we would use gossiper
        // directly for this, but gossiper may still contain obsolete entries from 1. replaced nodes
        // and 2. nodes that have changed their IPs; these entries are eventually garbage-collected,
        // but here they may still be present if we're performing topology changes in quick succession.
        // `token_metadata` has all host ID / token collisions resolved so in particular it doesn't contain
        // these obsolete IPs. Refs: #14487, #14468
        auto& tm = get_token_metadata();
        auto ignore_nodes = ri
                ? _ss.parse_node_list(db.local().get_config().ignore_dead_nodes_for_replace(), tm)
                // TODO: specify ignore_nodes for bootstrap
                : std::unordered_set<gms::inet_address>{};

        std::vector<gms::inet_address> sync_nodes;
        tm.get_topology().for_each_node([&] (const locator::node* np) {
            auto ep = np->endpoint();
            if (!ignore_nodes.contains(ep) && (!ri || ep != ri->address)) {
                sync_nodes.push_back(ep);
            }
        });

        tasks::tmlogger.info("Waiting for nodes {} to be alive", sync_nodes);
        co_await gossiper.wait_alive(sync_nodes, std::chrono::seconds{30});
        tasks::tmlogger.info("Nodes {} are alive", sync_nodes);
    }

    assert(group0);
    // if the node is bootstrapped the functin will do nothing since we already created group0 in main.cc
    co_await group0->setup_group0(sys_ks.local(), _initial_contact_nodes, raft_replace_info, _ss, *_ss._qp, _ss._migration_manager.local());

    raft::server* raft_server = co_await [&ss = _ss] () -> future<raft::server*> {
        if (!ss._raft_topology_change_enabled) {
            co_return nullptr;
        } else if (ss._sys_ks.local().bootstrap_complete()) {
            auto [upgrade_lock_holder, upgrade_state] = co_await ss._group0->client().get_group0_upgrade_state();
            co_return upgrade_state == service::group0_upgrade_state::use_post_raft_procedures ? &ss._group0->group0_server() : nullptr;
        } else {
            auto upgrade_state = (co_await ss._group0->client().get_group0_upgrade_state()).second;
            if (upgrade_state != service::group0_upgrade_state::use_post_raft_procedures) {
                on_internal_error(tasks::tmlogger, "raft topology: cluster not upgraded to use group 0 after setup_group0");
            }
            co_return &ss._group0->group0_server();
        }
    } ();

    co_await gossiper.wait_for_gossip_to_settle();
    // TODO: Look at the group 0 upgrade state and use it to decide whether to attach or not
    if (!raft_topology_change_enabled) {
        co_await feature_service.enable_features_on_join(gossiper, sys_ks.local());
    }

    _ss.set_mode(service::storage_service::mode::JOINING);

    tasks::task_info parent_info{_status.id, _status.shard};
    if (raft_server) { // Raft is enabled. Check if we need to bootstrap ourself using raft
        tasks::tmlogger.info("topology changes are using raft");

        if (is_replacing()) {
            assert(raft_replace_info);
            auto task = co_await _ss.get_task_manager_module().make_and_start_task<raft_replace_task_impl>(parent_info, "", _status.id, _ss, _sys_dist_ks,
                *raft_server, raft_replace_info->ip_addr, raft_replace_info->raft_id);
            co_await task->done();
        } else {
            auto task = co_await _ss.get_task_manager_module().make_and_start_task<raft_bootstrap_task_impl>(parent_info, "", _status.id, _ss, _sys_dist_ks,
                *raft_server);
            co_await task->done();
        }

        // Node state is enough to know that bootstrap has completed, but to make legacy code happy
        // let it know that the bootstrap is completed as well
        co_await sys_ks.local().set_bootstrap_state(db::system_keyspace::bootstrap_state::COMPLETED);
        _ss.set_mode(service::storage_service::mode::NORMAL);

        if (get_token_metadata().sorted_tokens().empty()) {
            auto err = ::format("join_token_ring: Sorted token in token_metadata is empty");
            tasks::tmlogger.error("{}", err);
            throw std::runtime_error(err);
        }

        co_await group0->finish_setup_after_join(_ss, *_ss._qp, _ss._migration_manager.local());
        co_return;
    }

    // We bootstrap if we haven't successfully bootstrapped before, as long as we are not a seed.
    // If we are a seed, or if the user manually sets auto_bootstrap to false,
    // we'll skip streaming data from other nodes and jump directly into the ring.
    //
    // The seed check allows us to skip the RING_DELAY sleep for the single-node cluster case,
    // which is useful for both new users and testing.
    //
    // We attempted to replace this with a schema-presence check, but you need a meaningful sleep
    // to get schema info from gossip which defeats the purpose.  See CASSANDRA-4427 for the gory details.
    if (should_bootstrap()) {
        bool resume_bootstrap = sys_ks.local().bootstrap_in_progress();
        if (resume_bootstrap) {
            tasks::tmlogger.warn("Detected previous bootstrap failure; retrying");
        } else {
            co_await sys_ks.local().set_bootstrap_state(db::system_keyspace::bootstrap_state::IN_PROGRESS);
        }
        tasks::tmlogger.info("waiting for ring information");

        // if our schema hasn't matched yet, keep sleeping until it does
        // (post CASSANDRA-1391 we don't expect this to be necessary very often, but it doesn't hurt to be careful)
        co_await _ss.wait_for_ring_to_settle();

        if (!replace_address) {
            auto tmptr = get_token_metadata_ptr();

            if (tmptr->is_normal_token_owner(get_broadcast_address())) {
                throw std::runtime_error("This node is already a member of the token ring; bootstrap aborted. (If replacing a dead node, remove the old one from the ring first.)");
            }
            tasks::tmlogger.info("getting bootstrap token");
            if (resume_bootstrap) {
                bootstrap_tokens = co_await sys_ks.local().get_saved_tokens();
                if (!bootstrap_tokens.empty()) {
                    tasks::tmlogger.info("Using previously saved tokens = {}", bootstrap_tokens);
                } else {
                    bootstrap_tokens = dht::boot_strapper::get_bootstrap_tokens(tmptr, db.local().get_config(), dht::check_token_endpoint::yes);
                }
            } else {
                bootstrap_tokens = dht::boot_strapper::get_bootstrap_tokens(tmptr, db.local().get_config(), dht::check_token_endpoint::yes);
            }
        } else {
            if (*replace_address != get_broadcast_address()) {
                // Sleep additionally to make sure that the server actually is not alive
                // and giving it more time to gossip if alive.
                tasks::tmlogger.info("Sleeping before replacing {}...", *replace_address);
                co_await sleep_abortable(2 * _ss.get_ring_delay(), _ss._abort_source);

                // check for operator errors...
                const auto tmptr = get_token_metadata_ptr();
                for (auto token : bootstrap_tokens) {
                    auto existing = tmptr->get_endpoint(token);
                    if (existing) {
                        auto eps = gossiper.get_endpoint_state_ptr(*existing);
                        if (eps && eps->get_update_timestamp() > gms::gossiper::clk::now() - _delay) {
                            throw std::runtime_error("Cannot replace a live node...");
                        }
                    } else {
                        throw std::runtime_error(::format("Cannot replace token {} which does not exist!", token));
                    }
                }
            } else {
                tasks::tmlogger.info("Sleeping before replacing {}...", *replace_address);
                co_await sleep_abortable(_ss.get_ring_delay(), _ss._abort_source);
            }
            tasks::tmlogger.info("Replacing a node with token(s): {}", bootstrap_tokens);
            // bootstrap_tokens was previously set using tokens gossiped by the replaced node
        }
        co_await _sys_dist_ks.invoke_on_all(&db::system_distributed_keyspace::start);
        co_await _ss.mark_existing_views_as_built(_sys_dist_ks);
        co_await sys_ks.local().update_tokens(bootstrap_tokens);
        co_await _ss.bootstrap(bootstrap_tokens, cdc_gen_id, ri);
    } else {
        supervisor::notify("starting system distributed keyspace");
        co_await _sys_dist_ks.invoke_on_all(&db::system_distributed_keyspace::start);
        bootstrap_tokens = co_await sys_ks.local().get_saved_tokens();
        if (bootstrap_tokens.empty()) {
            bootstrap_tokens = dht::boot_strapper::get_bootstrap_tokens(get_token_metadata_ptr(), db.local().get_config(), dht::check_token_endpoint::no);
            co_await sys_ks.local().update_tokens(bootstrap_tokens);
        } else {
            size_t num_tokens = db.local().get_config().num_tokens();
            if (bootstrap_tokens.size() != num_tokens) {
                throw std::runtime_error(::format("Cannot change the number of tokens from {:d} to {:d}", bootstrap_tokens.size(), num_tokens));
            } else {
                tasks::tmlogger.info("Using saved tokens {}", bootstrap_tokens);
            }
        }
    }

    tasks::tmlogger.debug("Setting tokens to {}", bootstrap_tokens);
    co_await _ss.mutate_token_metadata([&ss = _ss, &bootstrap_tokens] (locator::mutable_token_metadata_ptr tmptr) {
        // This node must know about its chosen tokens before other nodes do
        // since they may start sending writes to this node after it gossips status = NORMAL.
        // Therefore, in case we haven't updated _token_metadata with our tokens yet, do it now.
        tmptr->update_topology(ss.get_broadcast_address(), ss._snitch.local()->get_location(), locator::node::state::normal);
        return tmptr->update_normal_tokens(bootstrap_tokens, ss.get_broadcast_address());
    });

    if (!sys_ks.local().bootstrap_complete()) {
        // If we're not bootstrapping then we shouldn't have chosen a CDC streams timestamp yet.
        assert(should_bootstrap() || !cdc_gen_id);

        // Don't try rewriting CDC stream description tables.
        // See cdc.md design notes, `Streams description table V1 and rewriting` section, for explanation.
        co_await sys_ks.local().cdc_set_rewritten(std::nullopt);
    }

    if (!cdc_gen_id) {
        // If we didn't observe any CDC generation at this point, then either
        // 1. we're replacing a node,
        // 2. we've already bootstrapped, but are upgrading from a non-CDC version,
        // 3. we're the first node, starting a fresh cluster.

        // In the replacing case we won't create any CDC generation: we're not introducing any new tokens,
        // so the current generation used by the cluster is fine.

        // In the case of an upgrading cluster, one of the nodes is responsible for creating
        // the first CDC generation. We'll check if it's us.

        // Finally, if we're the first node, we'll create the first generation.

        if (!is_replacing()
                && (!sys_ks.local().bootstrap_complete()
                    || cdc::should_propose_first_generation(get_broadcast_address(), gossiper))) {
            try {
                cdc_gen_id = co_await _ss._cdc_gens.local().legacy_make_new_generation(bootstrap_tokens, !_ss.is_first_node());
            } catch (...) {
                cdc_log.warn(
                    "Could not create a new CDC generation: {}. This may make it impossible to use CDC or cause performance problems."
                    " Use nodetool checkAndRepairCdcStreams to fix CDC.", std::current_exception());
            }
        }
    }

    // Persist the CDC streams timestamp before we persist bootstrap_state = COMPLETED.
    if (cdc_gen_id) {
        co_await sys_ks.local().update_cdc_generation_id(*cdc_gen_id);
    }
    // If we crash now, we will choose a new CDC streams timestamp anyway (because we will also choose a new set of tokens).
    // But if we crash after setting bootstrap_state = COMPLETED, we will keep using the persisted CDC streams timestamp after restarting.

    co_await sys_ks.local().set_bootstrap_state(db::system_keyspace::bootstrap_state::COMPLETED);
    // At this point our local tokens and CDC streams timestamp are chosen (bootstrap_tokens, cdc_gen_id) and will not be changed.

    // start participating in the ring.
    co_await service::set_gossip_tokens(gossiper, bootstrap_tokens, cdc_gen_id);

    _ss.set_mode(service::storage_service::mode::NORMAL);

    if (get_token_metadata().sorted_tokens().empty()) {
        auto err = ::format("join_token_ring: Sorted token in token_metadata is empty");
        tasks::tmlogger.error("{}", err);
        throw std::runtime_error(err);
    }

    assert(group0);
    co_await group0->finish_setup_after_join(_ss, *_ss._qp, _ss._migration_manager.local());
    co_await _ss._cdc_gens.local().after_join(std::move(cdc_gen_id));
}

future<> node_ops_task_impl::prepare_raft_joining(raft::server& raft_server, sharded<db::system_distributed_keyspace>& sys_dist_ks) {
    // start topology coordinator fiber
    _ss._raft_state_monitor = _ss.raft_state_monitor_fiber(raft_server, sys_dist_ks);

    // Need to start system_distributed_keyspace before bootstrap because bootstraping
    // process may access those tables.
    supervisor::notify("starting system distributed keyspace");
    co_await sys_dist_ks.invoke_on_all(&db::system_distributed_keyspace::start);
}

future<> node_ops_task_impl::finish_raft_joining(raft::server& raft_server) {
    // Wait until we enter one of the final states
    co_await _ss._topology_state_machine.event.when([&ss = _ss, &raft_server] {
        return ss._topology_state_machine._topology.normal_nodes.contains(raft_server.id()) ||
        ss._topology_state_machine._topology.left_nodes.contains(raft_server.id());
    });

    if (_ss._topology_state_machine._topology.left_nodes.contains(raft_server.id())) {
        throw std::runtime_error("A node that already left the cluster cannot be restarted");
    }

    co_await _ss.update_topology_with_local_metadata(raft_server);
}

future<> raft_bootstrap_task_impl::run() {
    co_await prepare_raft_joining(_raft_server, _sys_dist_ks);

    // We try to find ourself in the topology without doing read barrier
    // first to not require quorum of live nodes during regular boot. But
    // if we are not in the topology it either means this is the first boot
    // or we failed during bootstrap so do a read barrier (which requires
    // quorum to be alive) and re-check.
    if (!_ss._topology_state_machine._topology.contains(_raft_server.id())) {
        co_await _raft_server.read_barrier(&_ss._abort_source);
    }

    while (!_ss._topology_state_machine._topology.contains(_raft_server.id())) {
        tasks::tmlogger.info("raft topology: adding myself to topology: {}", _raft_server.id());
        // Current topology does not contains this node. Bootstrap is needed!
        auto guard = co_await _ss._group0->client().start_operation(&_ss._abort_source);

        auto change= _ss.build_bootstrap_topology_change(_raft_server, guard);
        service::group0_command g0_cmd = _ss._group0->client().prepare_command(std::move(change), guard, "bootstrap: add myself to topology");
        try {
            co_await _ss._group0->client().add_entry(std::move(g0_cmd), std::move(guard), &_ss._abort_source);
        } catch (service::group0_concurrent_modification&) {
            tasks::tmlogger.info("raft topology: bootstrap: concurrent operation is detected, retrying.");
        }
    }

    co_await finish_raft_joining(_raft_server);
}

future<> raft_replace_task_impl::run() {
    co_await prepare_raft_joining(_raft_server, _sys_dist_ks);

    auto& db = _ss._db.local();
    auto ignore_nodes_strs = utils::split_comma_separated_list(db.get_config().ignore_dead_nodes_for_replace());
    std::list<locator::host_id_or_endpoint> ignore_nodes_params;
    for (const auto& n : ignore_nodes_strs) {
        ignore_nodes_params.emplace_back(n);
    }

    // Read barrier to access the latest topology. Quorum of nodes has to be alive.
    co_await _raft_server.read_barrier(&_ss._abort_source);

    auto it = _ss._topology_state_machine._topology.find(_raft_server.id());
    if (it && it->second.state != service::node_state::replacing) {
        throw std::runtime_error(::format("Cannot do \"replace address\" operation with a node that is in state: {}", it->second.state));
    }

    // add myself to topology with request to replace
    while (!_ss._topology_state_machine._topology.contains(_raft_server.id())) {
        auto guard = co_await _ss._group0->client().start_operation(&_ss._abort_source);

        auto it = _ss._topology_state_machine._topology.normal_nodes.find(_raft_id);
        if (it == _ss._topology_state_machine._topology.normal_nodes.end()) {
            throw std::runtime_error(::format("Cannot replace node {}/{} because it is not in the 'normal' state", _ip_addr, _raft_id));
        }

        auto ignored_ids = _ss.find_raft_nodes_from_hoeps(ignore_nodes_params);

        tasks::tmlogger.info("raft topology: adding myself to topology for replace: {} replacing {}, ignored nodes: {}", _raft_server.id(), _raft_id, ignored_ids);
        auto& rs = it->second;
        auto change = _ss.build_replace_topology_change(_raft_server, _raft_id, rs, guard, std::move(ignored_ids));
        service::group0_command g0_cmd = _ss._group0->client().prepare_command(std::move(change), guard, ::format("replace {}/{}: add myself ({}) to topology", _raft_id, _ip_addr, _raft_server.id()));
        try {
            co_await _ss._group0->client().add_entry(std::move(g0_cmd), std::move(guard), &_ss._abort_source);
        } catch (service::group0_concurrent_modification&) {
            tasks::tmlogger.info("raft topology: replace: concurrent operation is detected, retrying.");
        }
    }

    co_await finish_raft_joining(_raft_server);
}

start_rebuild_task_impl::start_rebuild_task_impl(tasks::task_manager::module_ptr module,
            std::string entity,
            service::storage_service& ss,
            sstring source_dc) noexcept
    : rebuild_node_task_impl(std::move(module), tasks::task_id::create_random_id(), ss.get_task_manager_module().new_sequence_number(), "node",
        std::move(entity), tasks::task_id::create_null_id(), ss)
    , _source_dc(std::move(source_dc))
{}

future<> start_rebuild_task_impl::run() {
    tasks::task_info parent_info{_status.id, _status.shard};
    return _ss.run_with_api_lock(sstring("rebuild"), [&source_dc = _source_dc, parent_info] (service::storage_service& ss) -> future<> {
        if (ss._raft_topology_change_enabled) {
            auto task = co_await ss.get_task_manager_module().make_and_start_task<raft_rebuild_task_impl>(parent_info, "", parent_info.id, ss,
                std::move(source_dc));
            co_await task->done();
        } else {
            tasks::tmlogger.info("rebuild from dc: {}", source_dc == "" ? "(any dc)" : source_dc);
            auto tmptr = ss.get_token_metadata_ptr();
            if (ss.is_repair_based_node_ops_enabled(streaming::stream_reason::rebuild)) {
                co_await ss._repair.local().rebuild_with_repair(tmptr, std::move(source_dc));
            } else {
                auto streamer = make_lw_shared<dht::range_streamer>(ss._db, ss._stream_manager, tmptr, ss._abort_source,
                        ss.get_broadcast_address(), ss._snitch.local()->get_location(), "Rebuild", streaming::stream_reason::rebuild);
                streamer->add_source_filter(std::make_unique<dht::range_streamer::failure_detector_source_filter>(ss._gossiper.get_unreachable_members()));
                if (source_dc != "") {
                    streamer->add_source_filter(std::make_unique<dht::range_streamer::single_datacenter_filter>(source_dc));
                }
                auto ks_erms = ss._db.local().get_non_local_strategy_keyspaces_erms();
                for (const auto& [keyspace_name, erm] : ks_erms) {
                    co_await streamer->add_ranges(keyspace_name, erm, ss.get_ranges_for_endpoint(erm, utils::fb_utilities::get_broadcast_address()), ss._gossiper, false);
                }
                try {
                    co_await streamer->stream_async();
                    tasks::tmlogger.info("Streaming for rebuild successful");
                } catch (...) {
                    auto ep = std::current_exception();
                    // This is used exclusively through JMX, so log the full trace but only throw a simple RTE
                    tasks::tmlogger.warn("Error while rebuilding node: {}", ep);
                    std::rethrow_exception(std::move(ep));
                }
            }
        }
    });
}

future<> raft_rebuild_task_impl::run() {
    auto& raft_server = _ss._group0->group0_server();

    while (true) {
        auto guard = co_await _ss._group0->client().start_operation(&_ss._abort_source);

        auto it = _ss._topology_state_machine._topology.find(raft_server.id());
        if (!it) {
            throw std::runtime_error(::format("local node {} is not a member of the cluster", raft_server.id()));
        }

        const auto& rs = it->second;

        if (rs.state != service::node_state::normal) {
            throw std::runtime_error(::format("local node is not in the normal state (current state: {})", rs.state));
        }

        if (_ss._topology_state_machine._topology.normal_nodes.size() == 1) {
            throw std::runtime_error("Cannot rebuild a single node");
        }

        tasks::tmlogger.info("raft topology: request rebuild for: {}", raft_server.id());
        auto change = _ss.build_rebuild_topology_change(raft_server, guard, _source_dc);
        service::group0_command g0_cmd = _ss._group0->client().prepare_command(std::move(change), guard, ::format("rebuild: request rebuild for {} ({})", raft_server.id(), _source_dc));

        try {
            co_await _ss._group0->client().add_entry(std::move(g0_cmd), std::move(guard), &_ss._abort_source);
        } catch (service::group0_concurrent_modification&) {
            tasks::tmlogger.info("raft topology: rebuild: concurrent operation is detected, retrying.");
            continue;
        }
        break;
    }

    // Wait until rebuild completes. We know it completes when the request parameter is empty
    co_await _ss._topology_state_machine.event.when([&ss = _ss, &raft_server] {
        return !ss._topology_state_machine._topology.req_param.contains(raft_server.id());
    });
}

start_decommission_task_impl::start_decommission_task_impl(tasks::task_manager::module_ptr module,
        std::string entity,
        service::storage_service& ss) noexcept
    : decommission_node_task_impl(std::move(module), tasks::task_id::create_random_id(), ss.get_task_manager_module().new_sequence_number(),
        "node", std::move(entity), tasks::task_id::create_null_id(), ss)
{}

future<> start_decommission_task_impl::run() {
    co_await utils::get_local_injector().inject_with_handler("node_ops_start_decommission_task_impl_run",
            [] (auto& handler) { return handler.wait_for_message(db::timeout_clock::now() + 10s); });

    tasks::task_info parent_info{_status.id, _status.shard};
    co_await _ss.run_with_api_lock(sstring("decommission"), [parent_info] (service::storage_service& ss) {
        return seastar::async([&ss, parent_info] {
            std::exception_ptr leave_group0_ex;
            if (ss._raft_topology_change_enabled) {
                auto task = ss.get_task_manager_module().make_and_start_task<raft_decommission_task_impl>(parent_info, "", parent_info.id, ss).get();
                task->done().get();
            } else {
                bool left_token_ring = false;
                auto uuid = node_ops_id::create_random_id();
                auto& db = ss._db.local();
                node_ops_ctl ctl(ss, node_ops_cmd::decommission_prepare, ss.get_token_metadata().get_my_id(), ss.get_broadcast_address());
                auto stop_ctl = deferred_stop(ctl);

                // Step 1: Decide who needs to sync data
                // TODO: wire ignore_nodes provided by user
                ctl.start("decommission");

                uuid = ctl.uuid();
                auto endpoint = ctl.endpoint;
                const auto& tmptr = ctl.tmptr;
                if (!tmptr->is_normal_token_owner(endpoint)) {
                    throw std::runtime_error("local node is not a member of the token ring yet");
                }
                // We assume that we're a member of group 0 if we're in decommission()` and Raft is enabled.
                // We have no way to check that we're not a member: attempting to perform group 0 operations
                // would simply hang in that case, the leader would refuse to talk to us.
                // If we aren't a member then we shouldn't be here anyway, since it means that either
                // an earlier decommission finished (leave_group0 is the last operation in decommission)
                // or that we were removed using `removenode`.
                //
                // For handling failure scenarios such as a group 0 member that is not a token ring member,
                // there's `removenode`.

                auto temp = tmptr->clone_after_all_left().get0();
                auto num_tokens_after_all_left = temp.sorted_tokens().size();
                temp.clear_gently().get();
                if (num_tokens_after_all_left < 2) {
                    throw std::runtime_error("no other normal nodes in the ring; decommission would be pointless");
                }

                if (ss._operation_mode != service::storage_service::mode::NORMAL) {
                    throw std::runtime_error(::format("Node in {} state; wait for status to become normal or restart", ss._operation_mode));
                }

                ss.update_topology_change_info(::format("decommission {}", endpoint)).get();

                auto non_system_keyspaces = db.get_non_local_vnode_based_strategy_keyspaces();
                for (const auto& keyspace_name : non_system_keyspaces) {
                    if (ss._db.local().find_keyspace(keyspace_name).get_effective_replication_map()->has_pending_ranges(ss.get_broadcast_address())) {
                        throw std::runtime_error("data is currently moving to this node; unable to leave the ring");
                    }
                }

                tasks::tmlogger.info("DECOMMISSIONING: starts");
                ctl.req.leaving_nodes = std::list<gms::inet_address>{endpoint};

                assert(ss._group0);
                bool raft_available = ss._group0->wait_for_raft().get();

                try {
                    // Step 2: Start heartbeat updater
                    ctl.start_heartbeat_updater(node_ops_cmd::decommission_heartbeat);

                    // Step 3: Prepare to sync data
                    ctl.prepare(node_ops_cmd::decommission_prepare).get();

                    // Step 4: Start to sync data
                    tasks::tmlogger.info("DECOMMISSIONING: unbootstrap starts");
                    ss.unbootstrap().get();
                    service::on_streaming_finished();
                    tasks::tmlogger.info("DECOMMISSIONING: unbootstrap done");

                    // Step 5: Become a group 0 non-voter before leaving the token ring.
                    //
                    // Thanks to this, even if we fail after leaving the token ring but before leaving group 0,
                    // group 0's availability won't be reduced.
                    if (raft_available) {
                        tasks::tmlogger.info("decommission[{}]: becoming a group 0 non-voter", uuid);
                        ss._group0->become_nonvoter().get();
                        tasks::tmlogger.info("decommission[{}]: became a group 0 non-voter", uuid);
                    }

                    // Step 6: Verify that other nodes didn't abort in the meantime.
                    // See https://github.com/scylladb/scylladb/issues/12989.
                    ctl.query_pending_op().get();

                    // Step 7: Leave the token ring
                    tasks::tmlogger.info("decommission[{}]: leaving token ring", uuid);
                    ss.leave_ring().get();
                    left_token_ring = true;
                    tasks::tmlogger.info("decommission[{}]: left token ring", uuid);

                    // Step 8: Finish token movement
                    ctl.done(node_ops_cmd::decommission_done).get();
                } catch (...) {
                    ctl.abort_on_error(node_ops_cmd::decommission_abort, std::current_exception()).get();
                }

                // Step 8: Leave group 0
                //
                // If the node failed to leave the token ring, don't remove it from group 0
                // --- hence the `left_token_ring` check.
                try {
                    utils::get_local_injector().inject("decommission_fail_before_leave_group0",
                        [] { throw std::runtime_error("decommission_fail_before_leave_group0"); });

                    if (raft_available && left_token_ring) {
                        tasks::tmlogger.info("decommission[{}]: leaving Raft group 0", uuid);
                        assert(ss._group0);
                        ss._group0->leave_group0().get();
                        tasks::tmlogger.info("decommission[{}]: left Raft group 0", uuid);
                    }
                } catch (...) {
                    // Even though leave_group0 failed, we will finish decommission and shut down everything.
                    // There's nothing smarter we could do. We should not continue operating in this broken
                    // state (we're not a member of the token ring any more).
                    //
                    // If we didn't manage to leave group 0, we will stay as a non-voter
                    // (which is not too bad - non-voters at least do not reduce group 0's availability).
                    // It's possible to remove the garbage member using `removenode`.
                    tasks::tmlogger.error(
                        "decommission[{}]: FAILED when trying to leave Raft group 0: \"{}\". This node"
                        " is no longer a member of the token ring, so it will finish shutting down its services."
                        " It may still be a member of Raft group 0. To remove it, shut it down and use `removenode`."
                        " Consult the `decommission` and `removenode` documentation for more details.",
                        uuid, std::current_exception());
                    leave_group0_ex = std::current_exception();
                }
            }

            ss.stop_transport().get();
            tasks::tmlogger.info("DECOMMISSIONING: stopped transport");

            ss.get_batchlog_manager().invoke_on_all([] (auto& bm) {
                return bm.drain();
            }).get();
            tasks::tmlogger.info("DECOMMISSIONING: stop batchlog_manager done");

            // StageManager.shutdownNow();
            ss._sys_ks.local().set_bootstrap_state(db::system_keyspace::bootstrap_state::DECOMMISSIONED).get();
            tasks::tmlogger.info("DECOMMISSIONING: set_bootstrap_state done");
            ss.set_mode(service::storage_service::mode::DECOMMISSIONED);

            if (leave_group0_ex) {
                std::rethrow_exception(leave_group0_ex);
            }

            tasks::tmlogger.info("DECOMMISSIONING: done");
            // let op be responsible for killing the process
        });
    });
}

future<> raft_decommission_task_impl::run() {
    auto& raft_server = _ss._group0->group0_server();

    auto shutdown_request_future = make_ready_future<>();
    auto disengage_shutdown_promise = defer([&ss = _ss] {
        ss._shutdown_request_promise = std::nullopt;
    });

    while (true) {
        auto guard = co_await _ss._group0->client().start_operation(&_ss._abort_source);

        auto it = _ss._topology_state_machine._topology.find(raft_server.id());
        if (!it) {
            throw std::runtime_error(::format("local node {} is not a member of the cluster", raft_server.id()));
        }

        const auto& rs = it->second;

        if (rs.state != service::node_state::normal) {
            throw std::runtime_error(::format("local node is not in the normal state (current state: {})", rs.state));
        }

        if (_ss._topology_state_machine._topology.normal_nodes.size() == 1) {
            throw std::runtime_error("Cannot decomission last node in the cluster");
        }

        shutdown_request_future = _ss._shutdown_request_promise.emplace().get_future();

        tasks::tmlogger.info("raft topology: request decomission for: {}", raft_server.id());
        service::topology_change change = _ss.build_decommission_topology_change(raft_server, guard);
        service::group0_command g0_cmd = _ss._group0->client().prepare_command(std::move(change), guard, ::format("decomission: request decomission for {}", raft_server.id()));

        try {
            co_await _ss._group0->client().add_entry(std::move(g0_cmd), std::move(guard), &_ss._abort_source);
        } catch (service::group0_concurrent_modification&) {
            tasks::tmlogger.info("raft topology: decomission: concurrent operation is detected, retrying.");
            continue;
        }
        break;
    }

    // Wait for the coordinator to tell us to shut down.
    co_await std::move(shutdown_request_future);

    // Need to set it otherwise gossiper will try to send shutdown on exit
    co_await _ss._gossiper.add_local_application_state({{ gms::application_state::STATUS, gms::versioned_value::left({}, _ss._gossiper.now().time_since_epoch().count()) }});
}

start_remove_node_task_impl::start_remove_node_task_impl(tasks::task_manager::module_ptr module,
        std::string entity,
        service::storage_service& ss,
        locator::host_id host_id,
        std::list<locator::host_id_or_endpoint> ignore_nodes_params) noexcept
    : remove_node_task_impl(std::move(module), tasks::task_id::create_random_id(), ss.get_task_manager_module().new_sequence_number(),
        "node", std::move(entity), tasks::task_id::create_null_id(), ss)
    , _host_id(host_id)
    , _ignore_nodes_params(std::move(ignore_nodes_params))
{}

future<> start_remove_node_task_impl::run() {
    return _ss.run_with_api_lock(sstring("removenode"), [host_id = _host_id, ignore_nodes_params = std::move(_ignore_nodes_params)] (service::storage_service& ss) mutable {
        return seastar::async([&ss, host_id, ignore_nodes_params = std::move(ignore_nodes_params)] () mutable {
            if (ss._raft_topology_change_enabled) {
                ss.raft_removenode(host_id, std::move(ignore_nodes_params)).get();
                return;
            }
            node_ops_ctl ctl(ss, node_ops_cmd::removenode_prepare, host_id, gms::inet_address());
            auto stop_ctl = deferred_stop(ctl);
            auto uuid = ctl.uuid();
            const auto& tmptr = ctl.tmptr;
            auto endpoint_opt = tmptr->get_endpoint_for_host_id(host_id);
            assert(ss._group0);
            auto raft_id = raft::server_id{host_id.uuid()};
            bool raft_available = ss._group0->wait_for_raft().get();
            bool is_group0_member = raft_available && ss._group0->is_member(raft_id, false);
            if (!endpoint_opt && !is_group0_member) {
                throw std::runtime_error(::format("removenode[{}]: Node {} not found in the cluster", uuid, host_id));
            }

            // If endpoint_opt is engaged, the node is a member of the token ring.
            // is_group0_member indicates whether the node is a member of Raft group 0.
            // A node might be a member of group 0 but not a member of the token ring, e.g. due to a
            // previously failed removenode/decommission. The code is written to handle this
            // situation. Parts related to removing this node from the token ring are conditioned on
            // endpoint_opt, while parts related to removing from group 0 are conditioned on
            // is_group0_member.

            if (endpoint_opt && ss._gossiper.is_alive(*endpoint_opt)) {
                const std::string message = ::format(
                    "removenode[{}]: Rejected removenode operation (node={}); "
                    "the node being removed is alive, maybe you should use decommission instead?",
                    uuid, *endpoint_opt);
                tasks::tmlogger.warn(std::string_view(message));
                throw std::runtime_error(message);
            }

            for (auto& hoep : ignore_nodes_params) {
                hoep.resolve(*tmptr);
                ctl.ignore_nodes.insert(hoep.endpoint);
            }

            bool removed_from_token_ring = !endpoint_opt;
            if (endpoint_opt) {
                auto endpoint = *endpoint_opt;
                ctl.endpoint = endpoint;

                // Step 1: Make the node a group 0 non-voter before removing it from the token ring.
                //
                // Thanks to this, even if we fail after removing the node from the token ring
                // but before removing it group 0, group 0's availability won't be reduced.
                if (is_group0_member && ss._group0->is_member(raft_id, true)) {
                    tasks::tmlogger.info("removenode[{}]: making node {} a non-voter in group 0", uuid, raft_id);
                    ss._group0->make_nonvoter(raft_id).get();
                    tasks::tmlogger.info("removenode[{}]: made node {} a non-voter in group 0", uuid, raft_id);
                }

                // Step 2: Decide who needs to sync data
                //
                // By default, we require all nodes in the cluster to participate
                // the removenode operation and sync data if needed. We fail the
                // removenode operation if any of them is down or fails.
                //
                // If the user want the removenode opeartion to succeed even if some of the nodes
                // are not available, the user has to explicitly pass a list of
                // node that can be skipped for the operation.
                ctl.start("removenode", [&] (gms::inet_address node) {
                    return node != endpoint;
                });

                auto tokens = tmptr->get_tokens(endpoint);

                try {
                    // Step 3: Start heartbeat updater
                    ctl.start_heartbeat_updater(node_ops_cmd::removenode_heartbeat);

                    // Step 4: Prepare to sync data
                    ctl.req.leaving_nodes = {endpoint};
                    ctl.prepare(node_ops_cmd::removenode_prepare).get();

                    // Step 5: Start to sync data
                    ctl.send_to_all(node_ops_cmd::removenode_sync_data).get();
                    service::on_streaming_finished();

                    // Step 6: Finish token movement
                    ctl.done(node_ops_cmd::removenode_done).get();

                    // Step 7: Announce the node has left
                    tasks::tmlogger.info("removenode[{}]: Advertising that the node left the ring", uuid);
                    auto permit = ss._gossiper.lock_endpoint(endpoint, gms::null_permit_id).get();
                    const auto& pid = permit.id();
                    ss._gossiper.advertise_token_removed(endpoint, host_id, pid).get();
                    std::unordered_set<dht::token> tmp(tokens.begin(), tokens.end());
                    ss.excise(std::move(tmp), endpoint, pid).get();
                    removed_from_token_ring = true;
                    tasks::tmlogger.info("removenode[{}]: Finished removing the node from the ring", uuid);
                } catch (...) {
                    // we need to revert the effect of prepare verb the removenode ops is failed
                    ctl.abort_on_error(node_ops_cmd::removenode_abort, std::current_exception()).get();
                }
            }

            // Step 8: Remove the node from group 0
            //
            // If the node was a token ring member but we failed to remove it,
            // don't remove it from group 0 -- hence the `removed_from_token_ring` check.
            try {
                utils::get_local_injector().inject("removenode_fail_before_remove_from_group0",
                    [] { throw std::runtime_error("removenode_fail_before_remove_from_group0"); });

                if (is_group0_member && removed_from_token_ring) {
                    tasks::tmlogger.info("removenode[{}]: removing node {} from Raft group 0", uuid, raft_id);
                    ss._group0->remove_from_group0(raft_id).get();
                    tasks::tmlogger.info("removenode[{}]: removed node {} from Raft group 0", uuid, raft_id);
                }
            } catch (...) {
                tasks::tmlogger.error(
                    "removenode[{}]: FAILED when trying to remove the node from Raft group 0: \"{}\". The node"
                    " is no longer a member of the token ring, but it may still be a member of Raft group 0."
                    " Please retry `removenode`. Consult the `removenode` documentation for more details.",
                    uuid, std::current_exception());
                throw;
            }

            tasks::tmlogger.info("removenode[{}]: Finished removenode operation, host id={}", uuid, host_id);
        });
    });
}

}
