/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "replication.hh"
#include "utils/to_string.hh"

// Test Raft library with declarative test definitions


lowres_clock::duration tick_delta = 10ms; // minimum granularity of lowres_clock

#define RAFT_TEST_CASE(test_name, test_body)  \
    SEASTAR_THREAD_TEST_CASE(test_name) { \
        replication_test<lowres_clock>(test_body, false, tick_delta); }  \
    SEASTAR_THREAD_TEST_CASE(test_name ## _drops) { \
        replication_test<lowres_clock>(test_body, false, tick_delta, {.drops = true}); } \
    SEASTAR_THREAD_TEST_CASE(test_name ## _prevote) { \
        replication_test<lowres_clock>(test_body, true, tick_delta); }  \
    SEASTAR_THREAD_TEST_CASE(test_name ## _prevote_drops) { \
        replication_test<lowres_clock>(test_body, true, tick_delta, {.drops = true}); }

// 1 nodes, simple replication, empty, no updates
RAFT_TEST_CASE(simple_replication, (test_case{
         .nodes = 1}))

// 2 nodes, 4 existing leader entries, 4 updates
RAFT_TEST_CASE(non_empty_leader_log, (test_case{
         .nodes = 2,
         .initial_states = {{.le = {{1,0},{1,1},{1,2},{1,3}}}},
         .updates = {entries{4}}}));

// 2 nodes, don't add more entries besides existing log
RAFT_TEST_CASE(non_empty_leader_log_no_new_entries, (test_case{
         .nodes = 2, .total_values = 4,
         .initial_states = {{.le = {{1,0},{1,1},{1,2},{1,3}}}}}));

// 1 nodes, 12 client entries
RAFT_TEST_CASE(simple_1_auto_12, (test_case{
         .nodes = 1,
         .initial_states = {}, .updates = {entries{12}}}));

// 1 nodes, 12 client entries
RAFT_TEST_CASE(simple_1_expected, (test_case{
         .nodes = 1, .initial_states = {},
         .updates = {entries{4}}}));

// 1 nodes, 7 leader entries, 12 client entries
RAFT_TEST_CASE(simple_1_pre, (test_case{
         .nodes = 1,
         .initial_states = {{.le = {{1,0},{1,1},{1,2},{1,3},{1,4},{1,5},{1,6}}}},
         .updates = {entries{12}},}));

// 2 nodes, 7 leader entries, 12 client entries
RAFT_TEST_CASE(simple_2_pre, (test_case{
         .nodes = 2,
         .initial_states = {{.le = {{1,0},{1,1},{1,2},{1,3},{1,4},{1,5},{1,6}}}},
         .updates = {entries{12}},}));

// 3 nodes, 2 leader changes with 4 client entries each
RAFT_TEST_CASE(leader_changes, (test_case{
         .nodes = 3,
         .updates = {entries{4},new_leader{1},entries{4},new_leader{2},entries{4}}}));

//
// NOTE: due to disrupting candidates protection leader doesn't vote for others, and
//       servers with entries vote for themselves, so some tests use 3 servers instead of
//       2 for simplicity and to avoid a stalemate. This behaviour can be disabled.
//

// 3 nodes, 7 leader entries, 12 client entries, change leader, 12 client entries
RAFT_TEST_CASE(simple_3_pre_chg, (test_case{
         .nodes = 3, .initial_term = 2,
         .initial_states = {{.le = {{1,0},{1,1},{1,2},{1,3},{1,4},{1,5},{1,6}}}},
         .updates = {entries{12},new_leader{1},entries{12}},}));

// 3 nodes, leader empty, follower has 3 spurious entries
// node 1 was leader but did not propagate entries, node 0 becomes leader in new term
// NOTE: on first leader election term is bumped to 3
RAFT_TEST_CASE(replace_log_leaders_log_empty, (test_case{
         .nodes = 3, .initial_term = 2,
         .initial_states = {{}, {{{2,10},{2,20},{2,30}}}},
         .updates = {entries{4}}}));

// 3 nodes, 7 leader entries, follower has 9 spurious entries
RAFT_TEST_CASE(simple_3_spurious_1, (test_case{
         .nodes = 3, .initial_term = 2,
         .initial_states = {{.le = {{1,0},{1,1},{1,2},{1,3},{1,4},{1,5},{1,6}}},
                            {{{2,10},{2,11},{2,12},{2,13},{2,14},{2,15},{2,16},{2,17},{2,18}}}},
         .updates = {entries{4}},}));

// 3 nodes, term 3, leader has 9 entries, follower has 5 spurious entries, 4 client entries
RAFT_TEST_CASE(simple_3_spurious_2, (test_case{
         .nodes = 3, .initial_term = 3,
         .initial_states = {{.le = {{1,0},{1,1},{1,2},{1,3},{1,4},{1,5},{1,6}}},
                            {{{2,10},{2,11},{2,12},{2,13},{2,14}}}},
         .updates = {entries{4}},}));

// 3 nodes, term 2, leader has 7 entries, follower has 3 good and 3 spurious entries
RAFT_TEST_CASE(simple_3_follower_4_1, (test_case{
         .nodes = 3, .initial_term = 3,
         .initial_states = {{.le = {{1,0},{1,1},{1,2},{1,3},{1,4},{1,5},{1,6}}},
                            {.le = {{1,0},{1,1},{1,2},{2,20},{2,30},{2,40}}}},
         .updates = {entries{4}}}));

// A follower and a leader have matching logs but leader's is shorter
// 3 nodes, term 2, leader has 2 entries, follower has same and 5 more, 12 updates
RAFT_TEST_CASE(simple_3_short_leader, (test_case{
         .nodes = 3, .initial_term = 3,
         .initial_states = {{.le = {{1,0},{1,1}}},
                            {.le = {{1,0},{1,1},{1,2},{1,3},{1,4},{1,5},{1,6}}}},
         .updates = {entries{12}}}));

RAFT_TEST_CASE(follower_add_entries_01, (test_case{
        .nodes = 3,
        .total_values=6,
        .updates = {
            entries{1},
            entries{1,1},
            entries{2},
            entries{2,2}
        },
    }));

// A follower and a leader have no common entries
// 3 nodes, term 2, leader has 7 entries, follower has non-matching 6 entries, 12 updates
RAFT_TEST_CASE(follower_not_matching, (test_case{
         .nodes = 3, .initial_term = 3,
         .initial_states = {{.le = {{1,0},{1,1},{1,2},{1,3},{1,4},{1,5},{1,6}}},
                            {.le = {{2,10},{2,20},{2,30},{2,40},{2,50},{2,60}}}},
         .updates = {entries{12}},}));

// A follower and a leader have one common entry
// 3 nodes, term 2, leader has 3 entries, follower has non-matching 3 entries, 12 updates
RAFT_TEST_CASE(follower_one_common_1, (test_case{
         .nodes = 3, .initial_term = 4,
         .initial_states = {{.le = {{1,0},{1,1},{1,2}}},
                            {.le = {{1,0},{2,11},{2,12},{2,13}}}},
         .updates = {entries{12}}}));

// A follower and a leader have 2 common entries in different terms
// 3 nodes, term 2, leader has 4 entries, follower has matching but in different term
RAFT_TEST_CASE(follower_one_common_2, (test_case{
         .nodes = 3, .initial_term = 5,
         .initial_states = {{.le = {{1,0},{2,1},{3,2},{3,3}}},
                            {.le = {{1,0},{2,1},{2,2},{2,13}}}},
         .updates = {entries{4}}}));

// 2 nodes both taking snapshot while simple replication
RAFT_TEST_CASE(take_snapshot, (test_case{
         .nodes = 2,
         .config = {{.snapshot_threshold = 10, .snapshot_trailing = 5}, {.snapshot_threshold = 20, .snapshot_trailing = 10}},
         .updates = {entries{100}}}));

// 2 nodes doing simple replication/snapshoting while leader's log size is limited
RAFT_TEST_CASE(backpressure, (test_case{
         .nodes = 2,
         .config = {
             []() {
                 const auto max_command_size = sizeof(size_t);
                 return raft::server::configuration {
                     .snapshot_threshold = 10,
                     .snapshot_threshold_log_size = 2 * (max_command_size + sizeof(raft::log_entry)),
                     .snapshot_trailing = 5,
                     .snapshot_trailing_size = 1 * (max_command_size + sizeof(raft::log_entry)),
                     .max_log_size = 5 * (max_command_size + sizeof(raft::log_entry)),
                     .max_command_size = max_command_size
                 };
             }(),
             {
                 .snapshot_threshold = 20,
                 .snapshot_trailing = 10
             }
         },
         .updates = {entries{100, {}, true}},
         .commutative_hash = true,
         .verify_persisted_snapshots = false}));

// 3 nodes, add entries, drop leader 0, add entries [implicit re-join all]
RAFT_TEST_CASE(drops_01, (test_case{
         .nodes = 3,
         .updates = {entries{4},partition{1,2},entries{4}}}));

// 3 nodes, add entries, drop follower 1, add entries [implicit re-join all]
RAFT_TEST_CASE(drops_02, (test_case{
         .nodes = 3,
         .updates = {entries{4},partition{0,2},entries{4},partition{2,1}}}));

// 3 nodes, add entries, drop leader 0, custom leader, add entries [implicit re-join all]
RAFT_TEST_CASE(drops_03, (test_case{
         .nodes = 3,
         .updates = {entries{4},partition{leader{1},2},entries{4}}}));

// 4 nodes, add entries, drop follower 1, custom leader, add entries [implicit re-join all]
RAFT_TEST_CASE(drops_04, (test_case{
         .nodes = 4,
         .updates = {entries{4},partition{0,2,3},entries{4},partition{1,leader{2},3}}}));

// Repro for dueling candidate for non-prevote scenarios where
// node (0) is dropped and becomes candidate bumping its term over and over.
// Meanwhile another node (2) becomes leader and adds entry.
// When dropped (0) rejoins, followers (1,3) ignore it (leader up and no timeout)
// but they should not ignore vote requests by current leader (2)
// or else the elections never succeed
// Note: for it to hang there has to be 4+ total nodes so
//       2 dueling candidates don't have enough quorum to resolve election
RAFT_TEST_CASE(drops_04_dueling_repro, (test_case{
         .nodes = 4,
         .updates = {entries{1},partition{0,2,3},entries{1},  // 0 leader
                     partition{1,leader{2},3},entries{1},     // 0 dropped, 2 leader
                     tick{40},                                // 0 becomes candidate, bumps term
                     partition{0,1,2,3},entries{1},           // 0 re-joinin and 0 disrupts
                     }}));

// TODO: change to RAFT_TEST_CASE once it's stable for handling packet drops
SEASTAR_THREAD_TEST_CASE(test_take_snapshot_and_stream) {
    replication_test<lowres_clock>(
        // Snapshot automatic take and load
        {.nodes = 3,
         .config = {{.snapshot_threshold = 10, .snapshot_trailing = 5}},
         .updates = {entries{5}, partition{0,1}, entries{10}, partition{0, 2}, entries{20}}}
    , false, tick_delta);
}

// Check removing all followers, add entry, bring back one follower and make it leader
RAFT_TEST_CASE(conf_changes_1, (test_case{
         .nodes = 3,
         .updates = {set_config{0}, entries{1}, set_config{0,1}, entries{1},
                     new_leader{1}, entries{1}}}));

// Check removing leader with entries, add entries, remove follower and add back first node
RAFT_TEST_CASE(conf_changes_2, (test_case{
         .nodes = 3,
         .updates = {entries{1}, new_leader{1}, set_config{1,2}, entries{1},
                     set_config{0,1}, entries{1}}}));

// Check removing a node from configuration, adding entries; cycle for all combinations
SEASTAR_THREAD_TEST_CASE(remove_node_cycle) {
    replication_test<lowres_clock>(
        {.nodes = 4,
         .updates = {set_config{0,1,2}, entries{2}, new_leader{1},
                     set_config{1,2,3}, entries{2}, new_leader{2},
                     set_config{2,3,0}, entries{2}, new_leader{3},
                     // TODO: find out why it breaks in release mode
                     // set_config{3,0,1}, entries{2}, new_leader{0}
                     }}
    ,false, tick_delta);
}

SEASTAR_THREAD_TEST_CASE(test_leader_change_during_snapshot_transfere) {
    replication_test<lowres_clock>(
        {.nodes = 3,
         .initial_snapshots  = {{.snap = {.idx = raft::index_t(10),
                                         .term = raft::term_t(1),
                                         .id = delay_send_snapshot}},
                                {.snap = {.idx = raft::index_t(10),
                                         .term = raft::term_t(1),
                                         .id = delay_apply_snapshot}}},
         .updates = {tick{10} /* ticking starts snapshot transfer */, new_leader{1}, entries{10}}}
    , false, tick_delta);
}

// verifies that each node in a cluster can campaign
// and be elected in turn. This ensures that elections work when not
// starting from a clean slate (as they do in TestLeaderElection)
// TODO: add pre-vote case
RAFT_TEST_CASE(etcd_test_leader_cycle, (test_case{
         .nodes = 2,
         .updates = {new_leader{1},new_leader{0},
                     new_leader{1},new_leader{0},
                     new_leader{1},new_leader{0},
                     new_leader{1},new_leader{0}
                     }}));

///
/// RPC-related tests
///

// 1 node cluster with an initial configuration from a snapshot.
// Test that RPC configuration is set up correctly when the raft server
// instance is started.
RAFT_TEST_CASE(rpc_load_conf_from_snapshot, (test_case{
         .nodes = 1, .total_values = 0,
         .initial_snapshots = {{.snap = {
                .config = config_from_ids({to_raft_id(0)})}}},
         .updates = {check_rpc_config{node_id{0},
                     rpc_address_set{node_id{0}}}
         }}));

// 1 node cluster.
// Initial configuration is taken from the persisted log.
RAFT_TEST_CASE(rpc_load_conf_from_log, (test_case{
         .nodes = 1, .total_values = 0,
         .initial_states = {{.le = {{1, config_from_ids({to_raft_id(0)})}}}},
         .updates = {check_rpc_config{node_id{0},
                     rpc_address_set{node_id{0}}}
         }}));


// 3 node cluster {A, B, C}.
// Shrunk later to 2 nodes and then expanded back to 3 nodes.
// Test that both configuration changes update RPC configuration correspondingly
// on all nodes.
RAFT_TEST_CASE(rpc_propose_conf_change, (test_case{
         .nodes = 3, .total_values = 0,
         .updates = {
            // Remove node C from the cluster configuration.
            set_config{0,1},
            // Check that RPC config is updated both on leader and on follower nodes,
            // i.e. `rpc::remove_server` is called.
            check_rpc_config{{node_id{0},node_id{1}},
                             rpc_address_set{node_id{0},node_id{1}}},
            // Re-add node C to the cluster configuration.
            set_config{0,1,2},
            // Check that both A (leader) and B (follower) call `rpc::on_configuration_change()`,
            // also the newly integrated node gets the actual RPC configuration, too.
            check_rpc_config{{node_id{0},node_id{1},node_id{2}},
                             rpc_address_set{node_id{0},node_id{1},node_id{2}}},
         }}));

// 3 node cluster {A, B, C}.
// Test that leader elections don't change RPC configuration.
RAFT_TEST_CASE(rpc_leader_election, (test_case{
         .nodes = 3, .total_values = 0,
         .updates = {
            check_rpc_config{{node_id{0},node_id{1},node_id{2}},
                             rpc_address_set{node_id{0},node_id{1},node_id{2}}},
            // Elect 2nd node a leader
            new_leader{1},
            check_rpc_config{{node_id{0},node_id{1},node_id{2}},
                             rpc_address_set{node_id{0},node_id{1},node_id{2}}},
         }}));

// 3 node cluster {A, B, C}.
// Test that demoting of node C to learner state and then promoting back
// to voter doesn't involve any RPC configuration changes.
RAFT_TEST_CASE(rpc_voter_non_voter_transision, (test_case{
         .nodes = 3, .total_values = 0,
         .updates = {
            check_rpc_config{{node_id{0},node_id{1},node_id{2}},
                             rpc_address_set{node_id{0},node_id{1},node_id{2}}},
            rpc_reset_counters{{node_id{0},node_id{1},node_id{2}}},
            // Make C a non-voting member.
            set_config{0, 1, set_config_entry(2, false)},
            // Check that RPC configuration didn't change.
            check_rpc_added{{node_id{0},node_id{1},node_id{2}},0},
            check_rpc_removed{{node_id{0},node_id{1},node_id{2}},0},
            // Make C a voting member.
            set_config{0, 1, 2},
            // RPC configuration shouldn't change.
            check_rpc_added{{node_id{0},node_id{1},node_id{2}},0},
            check_rpc_removed{{node_id{0},node_id{1},node_id{2}},0},
         }}));

// 3 node cluster {A, B, C}.
// Issue a configuration change on leader (A): add node D.
// Fail the node before the entry is committed (disconnect from the
// rest of the cluster and restart the node).
//
// In the meanwhile, elect a new leader within the connected part of the
// cluster (B). A becomes an isolated follower in this case.
// A should observe {A, B, C, D} RPC configuration: when in joint
// consensus, we need to account for servers in both configurations.
//
// Heal network partition and observe that A's log is truncated (actually,
// it's empty since B does not have any entries at all, except for dummies).
// The RPC configuration on A is restored from initial snapshot configuration,
// which is {A, B, C}.

RAFT_TEST_CASE(rpc_configuration_truncate_restore_from_snp, (test_case{
         .nodes = 3, .total_values = 0,
         .updates = {

            // Disconnect A from B and C.
            partition{1,2},
            // Emulate a failed configuration change on A (add node D) by
            // restarting A with a modified initial log containing one extraneous
            // configuration entry.
            stop{0},
            // Restart A with a synthetic initial state representing
            // the same initial snapshot config (A, B, C) as before,
            // but with the following things in mind:
            // * log contains only one entry: joint configuration entry
            //   that is equivalent to that of A's before the crash.
            // * The configuration entry would have term=1 so that it'll
            //   be truncated when A gets in contact with other nodes
            // * This will completely erase all entries on A leaving its
            //   log empty.
            reset{.id = 0, .state = {
                .log = { raft::log_entry{raft::term_t(1), raft::index_t(1),
                        config{.curr = {node_id{0},node_id{1},node_id{2},node_id{3}},
                               .prev = {node_id{0},node_id{1},node_id{2}}}}},
                .snapshot = {.config  = raft::configuration{config_set({node_id{0},node_id{1},node_id{2}})}
                }
            }},
            // A should see {A, B, C, D} as RPC config since
            // the latest configuration entry points to joint
            // configuration {.current = {A, B, C, D}, .previous = {A, B, C}}.
            // RPC configuration is computed as a union of current
            // and previous configurations.
            check_rpc_config{node_id{0},
                             rpc_address_set{node_id{0},node_id{1},node_id{2},node_id{3}}},

            // Elect B as leader
            new_leader{1},

            // Heal network partition.
            partition{0,1,2},

            // wait to synchronize logs between current leader (B) and A
            wait_log{0},

            // A should have truncated an offending configuration entry and revert its RPC configuration.
            //
            // Since B's log is effectively empty (does not contain any configuration
            // entries), A's configuration view ({A, B, C}) is restored from
            // initial snapshot.
            check_rpc_config{node_id{0},
                             rpc_address_set{node_id{0},node_id{1},node_id{2}}}}}));

// 4 node cluster {A, B, C, D}.
// Change configuration to {A, B, C} from A and wait for it to become
// committed.
//
// Then, issue a configuration change on leader (A): remove node C.
// Fail the node before the entry is committed (disconnect from the
// rest of the cluster and restart the node). We emulate this behavior by
// just terminating the node and restarting it with a pre-defined state
// that is equivalent to having an uncommitted configuration entry in
// the log.
//
// In the meanwhile, elect a new leader within the connected part of the
// cluster (B). A becomes an isolated follower in this case.
//
// Heal network partition and observe that A's log is truncated and
// replaced with that of B. RPC configuration should not change between
// the crash + network partition and synchronization with B, since
// the effective RPC cfg would be {A, B, C} both for
// joint cfg = {.current = {A, B}, .previous = {A, B, C}}
// and the previously committed cfg = {A, B, C}.
//
// After that, test for the second case: switch leader back to A and
// try to expand the cluster back to initial state (re-add
// node D): {A, B, C, D}.
//
// Try to set configuration {A, B, C, D} on leader A, isolate and crash it.
// Restart with synthetic state containing an uncommitted configuration entry.
//
// This time before healing the network we should observe
// RPC configuration = {A, B, C, D}, accounting for an uncommitted part of the
// configuration.
// After healing the network and synchronizing with new leader B, RPC config
// should be reverted back to committed state {A, B, C}.
RAFT_TEST_CASE(rpc_configuration_truncate_restore_from_log, (test_case{
         .nodes = 4, .total_values = 0,
         .updates = {

            // Remove node D from the cluster configuration.
            set_config{0, 1, 2},
            // {A, B, C} configuration is committed by now.

            //
            // First case: shrink cluster (remove node C).
            //

            // Disconnect A from the rest of the cluster.
            partition{1,2,3},
            // Try to change configuration (remove node C)
            // `set_configuration` call will fail on A because
            // it's cut off the other nodes and it will be waiting for them,
            // but A is terminated before the network is allowed to heal the partition.
            stop{0},
            // Restart A with a synthetic initial state that contains two entries
            // in the log:
            //   1. {A, B, C} configuration committed before crash + partition.
            //   2. uncommitted joint configuration entry that is equivalent
            //      to that of A's before the crash.
            reset{.id = 0, .state = {
                .log = {
                    // First term committed conf {A, B, C}
                    raft::log_entry{raft::term_t(1), raft::index_t(1),
                        config{.curr = {node_id{0},node_id{1},node_id{2}}}},
                    // Second term (uncommitted) {A, B} and prev committed {A, B, C}
                    raft::log_entry{raft::term_t(2), raft::index_t(2),
                        config{.curr = {node_id{0},node_id{1}},
                               .prev = {node_id{0},node_id{1},node_id{2}}}
                        },
                },
                // all nodes in snapshot config {A, B, C, D} (original)
                .snapshot = {.config  = raft::configuration{config_set({node_id{0},node_id{1},node_id{2},node_id{3}})}
                }
            }},

            // A's RPC configuration should stay the same because
            // for both uncommitted joint cfg = {.current = {A, B}, .previous = {A, B, C}}
            // and committed cfg = {A, B, C} the RPC cfg would be equal to {A, B, C}
            check_rpc_config{node_id{0},
                             rpc_address_set{node_id{0},node_id{1},node_id{2}}},

            // Elect B as leader
            new_leader{1},

            // Heal network partition. Connect all.
            partition{0,1,2,3},

            wait_log{0,2},

            // Again, A's RPC configuration is the same as before despite the
            // real cfg being reverted to the committed state as it is the union
            // between current and previous configurations in case of
            // joint cfg, anyway.
            check_rpc_config{{node_id{0},node_id{1},node_id{2}},
                             rpc_address_set{node_id{0},node_id{1},node_id{2}}},

            //
            // Second case: expand cluster (re-add node D).
            //

            // Elect A leader again.
            new_leader{0},
            wait_log{1,2},

            // Disconnect A from the rest of the cluster.
            partition{1,2,3},

            // Try to add D back.
            stop{0},
            reset{.id = 0, .state = {
                .log = {
                    // First term committed conf {A, B, C}
                    raft::log_entry{raft::term_t(1), raft::index_t(1),
                        config{.curr = {node_id{0},node_id{1},node_id{2}}}},
                    // Second term (all) {A, B, C, D} and prev committed {A, B, C}
                    raft::log_entry{raft::term_t(2), raft::index_t(2),
                        config{.curr = {node_id{0},node_id{1},node_id{2},node_id{3}},
                               .prev = {node_id{0},node_id{1},node_id{2}}}
                        },
                },
                // all nodes in snapshot config {A, B, C, D} (original)
                .snapshot = {.config  = raft::configuration{config_set({node_id{0},node_id{1},node_id{2},node_id{3}})}
                }
            }},

            // A should observe RPC configuration = {A, B, C, D} since it's the union
            // of an uncommitted joint config components
            // {.current = {A, B, C, D}, .previous = {A, B, C}}.
            check_rpc_config{node_id{0},
                             rpc_address_set{node_id{0},node_id{1},node_id{2},node_id{3}}},

            // Elect B as leader
            new_leader{1},

            // Heal network partition. Connect all.
            partition{0,1,2,3},

            // wait to synchronize logs between current leader (B) and the rest of the cluster
            wait_log{0,2},

            // A's RPC configuration is reverted to committed configuration {A, B, C}.
            check_rpc_config{{node_id{0},node_id{1},node_id{2}},
                              rpc_address_set{node_id{0},node_id{1},node_id{2}}},
         }}));

// 1 nodes, 4 existing leader entries, try to read
RAFT_TEST_CASE(simple_leader_read, (test_case{
         .nodes = 1,
         .initial_states = {{.le = {{1,0},{1,1},{1,2},{1,3}}}},
         .updates = {read_value{0, 4}}}));

// 2 nodes, 4 existing leader entries, try to read on node 2
RAFT_TEST_CASE(simple_follower_read, (test_case{
         .nodes = 2,
         .initial_states = {{.le = {{1,0},{1,1},{1,2},{1,3}}}},
         .updates = {read_value{1, 4}}}));


// 4 nodes, add some entries, disconnect one node, add more entries, connect back,
// try to read on previously disconnected node
RAFT_TEST_CASE(follower_read_after_partition, (test_case{
         .nodes = 4,
         .updates = {entries{4}, partition{0, 1, 2}, entries{4}, partition{0, 1, 2, 3}, read_value{3, 8}}}));
