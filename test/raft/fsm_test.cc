/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "raft/raft.hh"
#define BOOST_TEST_MODULE raft

#include "raft/tracker.hh"
#include "test/raft/helpers.hh"

namespace raft {
std::ostream& boost_test_print_type(std::ostream& os, const vote_result& v) {
    fmt::print(os, "{}", v);
    return os;
}
}

using namespace raft;

BOOST_AUTO_TEST_CASE(test_votes) {
    auto id1 = id();

    raft::votes votes(config_from_ids({id1}));
    BOOST_CHECK_EQUAL(votes.tally_votes(), raft::vote_result::UNKNOWN);
    BOOST_CHECK_EQUAL(votes.voters().size(), 1);
    // Try a vote from an unknown server, it should be ignored.
    votes.register_vote(id(), true);
    votes.register_vote(id1, false);
    // Quorum votes against the decision
    BOOST_CHECK_EQUAL(votes.tally_votes(), raft::vote_result::LOST);
    // Another vote from the same server is ignored
    votes.register_vote(id1, true);
    votes.register_vote(id1, true);
    BOOST_CHECK_EQUAL(votes.tally_votes(), raft::vote_result::LOST);
    auto id2 = id();
    votes = raft::votes(config_from_ids({id1, id2}));
    BOOST_CHECK_EQUAL(votes.voters().size(), 2);
    votes.register_vote(id1, true);
    // We need a quorum of participants to win an election
    BOOST_CHECK_EQUAL(votes.tally_votes(), raft::vote_result::UNKNOWN);
    votes.register_vote(id2, false);
    // At this point it's clear we don't have enough votes
    BOOST_CHECK_EQUAL(votes.tally_votes(), raft::vote_result::LOST);
    auto id3 = id();
    // Joint configuration
    votes = raft::votes(raft::configuration(config_set({id1}), config_set({id2, id3})));
    BOOST_CHECK_EQUAL(votes.voters().size(), 3);
    BOOST_CHECK_EQUAL(votes.tally_votes(), raft::vote_result::UNKNOWN);
    votes.register_vote(id2, true);
    votes.register_vote(id3, true);
    BOOST_CHECK_EQUAL(votes.tally_votes(), raft::vote_result::UNKNOWN);
    votes.register_vote(id1, false);
    BOOST_CHECK_EQUAL(votes.tally_votes(), raft::vote_result::LOST);
    votes = raft::votes(raft::configuration(config_set({id1}), config_set({id2, id3})));
    votes.register_vote(id2, true);
    votes.register_vote(id3, true);
    votes.register_vote(id1, true);
    BOOST_CHECK_EQUAL(votes.tally_votes(), raft::vote_result::WON);
    votes = raft::votes(raft::configuration(config_set({id1, id2, id3}), config_set({id1})));
    BOOST_CHECK_EQUAL(votes.voters().size(), 3);
    votes.register_vote(id1, true);
    BOOST_CHECK_EQUAL(votes.tally_votes(), raft::vote_result::UNKNOWN);
    // This gives us a majority in both new and old
    // configurations.
    votes.register_vote(id2, true);
    BOOST_CHECK_EQUAL(votes.tally_votes(), raft::vote_result::WON);
    // Basic voting test for 4 nodes
    auto id4 = id();
    votes = raft::votes(config_from_ids({id1, id2, id3, id4}));
    votes.register_vote(id1, true);
    votes.register_vote(id2, true);
    BOOST_CHECK_EQUAL(votes.tally_votes(), raft::vote_result::UNKNOWN);
    votes.register_vote(id3, false);
    BOOST_CHECK_EQUAL(votes.tally_votes(), raft::vote_result::UNKNOWN);
    votes.register_vote(id4, false);
    BOOST_CHECK_EQUAL(votes.tally_votes(), raft::vote_result::LOST);
    auto id5 = id();
    // Basic voting test for 5 nodes
    votes = raft::votes(raft::configuration(config_set({id1, id2, id3, id4, id5}), config_set({id1, id2, id3})));
    votes.register_vote(id1, false);
    votes.register_vote(id2, false);
    BOOST_CHECK_EQUAL(votes.tally_votes(), raft::vote_result::LOST);
    votes.register_vote(id3, true);
    votes.register_vote(id4, true);
    votes.register_vote(id5, true);
    BOOST_CHECK_EQUAL(votes.tally_votes(), raft::vote_result::LOST);
    // Basic voting test with tree voters and one no-voter
    votes = raft::votes(raft::configuration({
            {server_addr_from_id(id1), is_voter::yes}, {server_addr_from_id(id2), is_voter::yes},
            {server_addr_from_id(id3), is_voter::yes}, {server_addr_from_id(id4), is_voter::no}}));
    votes.register_vote(id1, true);
    votes.register_vote(id2, true);
    BOOST_CHECK_EQUAL(votes.tally_votes(), raft::vote_result::WON);
    // Basic test that non-voting votes are ignored
    votes = raft::votes(raft::configuration({
            {server_addr_from_id(id1), is_voter::yes}, {server_addr_from_id(id2), is_voter::yes},
            {server_addr_from_id(id3), is_voter::yes}, {server_addr_from_id(id4), is_voter::no}}));
    votes.register_vote(id1, true);
    votes.register_vote(id4, true);
    BOOST_CHECK_EQUAL(votes.tally_votes(), raft::vote_result::UNKNOWN);
    votes.register_vote(id3, true);
    BOOST_CHECK_EQUAL(votes.tally_votes(), raft::vote_result::WON);
    // Joint configuration with non voting members
    votes = raft::votes(raft::configuration(
            {{server_addr_from_id(id1), is_voter::yes}},
            {{server_addr_from_id(id2), is_voter::yes}, {server_addr_from_id(id3), is_voter::yes}, {server_addr_from_id(id4), is_voter::no}}));
    BOOST_CHECK_EQUAL(votes.voters().size(), 3);
    BOOST_CHECK_EQUAL(votes.tally_votes(), raft::vote_result::UNKNOWN);
    votes.register_vote(id2, true);
    votes.register_vote(id3, true);
    votes.register_vote(id4, true);
    BOOST_CHECK_EQUAL(votes.tally_votes(), raft::vote_result::UNKNOWN);
    votes.register_vote(id1, true);
    BOOST_CHECK_EQUAL(votes.tally_votes(), raft::vote_result::WON);
    // Same node is voting in one config and non voting in another
    votes = raft::votes(raft::configuration(
            {{server_addr_from_id(id1), is_voter::yes}, {server_addr_from_id(id4), is_voter::yes}},
            {{server_addr_from_id(id2), is_voter::yes}, {server_addr_from_id(id3), is_voter::yes}, {server_addr_from_id(id4), is_voter::no}}));
    votes.register_vote(id2, true);
    votes.register_vote(id1, true);
    votes.register_vote(id4, true);
    BOOST_CHECK_EQUAL(votes.tally_votes(), raft::vote_result::UNKNOWN);
    votes.register_vote(id3, true);
    BOOST_CHECK_EQUAL(votes.tally_votes(), raft::vote_result::WON);
}

BOOST_AUTO_TEST_CASE(test_tracker) {
    auto id1 = id();
    raft::tracker tracker;
    raft::configuration cfg = config_from_ids({id1});
    tracker.set_configuration(cfg, index_t{1});
    BOOST_CHECK_NE(tracker.find(id1), nullptr);
    // The node with id set during construction is assumed to be
    // the leader, since otherwise we wouldn't create a tracker
    // in the first place.
    BOOST_CHECK_EQUAL(tracker.committed(index_t{0}), index_t{0});
    // Avoid keeping a reference, follower_progress address may
    // change with configuration change
    auto pr = [&tracker](raft::server_id id) -> raft::follower_progress* {
        return tracker.find(id);
    };
    BOOST_CHECK_EQUAL(pr(id1)->match_idx, index_t{0});
    BOOST_CHECK_EQUAL(pr(id1)->next_idx, index_t{1});

    pr(id1)->accepted(index_t{1});
    BOOST_CHECK_EQUAL(pr(id1)->match_idx, index_t{1});
    BOOST_CHECK_EQUAL(pr(id1)->next_idx, index_t{2});
    BOOST_CHECK_EQUAL(tracker.committed(index_t{0}), index_t{1});

    pr(id1)->accepted(index_t{10});
    BOOST_CHECK_EQUAL(pr(id1)->match_idx, index_t{10});
    BOOST_CHECK_EQUAL(pr(id1)->next_idx, index_t{11});
    BOOST_CHECK_EQUAL(tracker.committed(index_t{0}), index_t{10});

    // Out of order confirmation is OK
    //
    pr(id1)->accepted(index_t{5});
    BOOST_CHECK_EQUAL(pr(id1)->match_idx, index_t{10});
    BOOST_CHECK_EQUAL(pr(id1)->next_idx, index_t{11});
    BOOST_CHECK_EQUAL(tracker.committed(index_t{5}), index_t{10});

    // Enter joint configuration {A,B,C}
    auto id2 = id(), id3 = id();
    cfg.enter_joint(config_set({id1, id2, id3}));
    tracker.set_configuration(cfg, index_t{1});
    BOOST_CHECK_EQUAL(tracker.committed(index_t{10}), index_t{10});
    pr(id2)->accepted(index_t{11});
    BOOST_CHECK_EQUAL(tracker.committed(index_t{10}), index_t{10});
    pr(id3)->accepted(index_t{12});
    BOOST_CHECK_EQUAL(tracker.committed(index_t{10}), index_t{10});
    pr(id1)->accepted(index_t{13});
    BOOST_CHECK_EQUAL(tracker.committed(index_t{10}), index_t{12});
    pr(id1)->accepted(index_t{14});
    BOOST_CHECK_EQUAL(tracker.committed(index_t{13}), index_t{13});

    // Leave joint configuration, final configuration is  {A,B,C}
    cfg.leave_joint();
    tracker.set_configuration(cfg, index_t{1});
    BOOST_CHECK_EQUAL(tracker.committed(index_t{13}), index_t{13});

    auto id4 = id(), id5 = id();
    cfg.enter_joint(config_set({id3, id4, id5}));
    tracker.set_configuration(cfg, index_t{1});
    BOOST_CHECK_EQUAL(tracker.committed(index_t{13}), index_t{13});
    pr(id1)->accepted(index_t{15});
    BOOST_CHECK_EQUAL(tracker.committed(index_t{13}), index_t{13});
    pr(id5)->accepted(index_t{15});
    BOOST_CHECK_EQUAL(tracker.committed(index_t{13}), index_t{13});
    pr(id3)->accepted(index_t{15});
    BOOST_CHECK_EQUAL(tracker.committed(index_t{13}), index_t{15});
    // This does not advance the joint quorum
    pr(id1)->accepted(index_t{16});
    pr(id4)->accepted(index_t{17});
    pr(id5)->accepted(index_t{18});
    BOOST_CHECK_EQUAL(tracker.committed(index_t{15}), index_t{15});

    cfg.leave_joint();
    tracker.set_configuration(cfg, index_t{1});
    // Leaving joint configuration commits more entries
    BOOST_CHECK_EQUAL(tracker.committed(index_t{15}), index_t{17});
    //
    cfg.enter_joint(config_set({id1}));
    cfg.leave_joint();
    cfg.enter_joint(config_set({id2}));
    tracker.set_configuration(cfg, index_t{1});
    // Sic: we're in a weird state. The joint commit index
    // is actually 1, since id2 is at position 1. But in
    // unwinding back the commit index would be weird,
    // so we report back the hint (prev_commit_idx).
    // As soon as the cluster enters joint configuration,
    // and old quorum is insufficient, the leader won't be able to
    // commit new entries until the new members catch up.
    BOOST_CHECK_EQUAL(tracker.committed(index_t{17}), index_t{17});
    pr(id1)->accepted(index_t{18});
    BOOST_CHECK_EQUAL(tracker.committed(index_t{17}), index_t{17});
    pr(id2)->accepted(index_t{19});
    BOOST_CHECK_EQUAL(tracker.committed(index_t{17}), index_t{18});
    pr(id1)->accepted(index_t{20});
    BOOST_CHECK_EQUAL(tracker.committed(index_t{18}), index_t{19});

    // Check that non voting member is not counted for the quorum in simple config
    cfg.enter_joint({{server_addr_from_id(id1), is_voter::yes}, {server_addr_from_id(id2), is_voter::yes}, {server_addr_from_id(id3), is_voter::no}});
    cfg.leave_joint();
    tracker.set_configuration(cfg, index_t{1});
    pr(id1)->accepted(index_t{30});
    pr(id2)->accepted(index_t{25});
    pr(id3)->accepted(index_t{30});
    BOOST_CHECK_EQUAL(tracker.committed(index_t{0}), index_t{25});

    // Check that non voting member is not counted for the quorum in joint config
    cfg.enter_joint({{server_addr_from_id(id4), is_voter::yes}, {server_addr_from_id(id5), is_voter::yes}});
    tracker.set_configuration(cfg, index_t{1});
    pr(id4)->accepted(index_t{30});
    pr(id5)->accepted(index_t{30});
    BOOST_CHECK_EQUAL(tracker.committed(index_t{0}), index_t{25});

    // Check the case where the same node is in both config but different voting rights
    cfg.leave_joint();
    cfg.enter_joint({{server_addr_from_id(id1), is_voter::yes}, {server_addr_from_id(id2), is_voter::yes}, {server_addr_from_id(id5), is_voter::no}});
    BOOST_CHECK_EQUAL(tracker.committed(index_t{0}), index_t{25});
}

// Test that a voter demoted to non-voter during joint configuration
// retains can_vote=true (from the previous config) in the tracker.
// Before the fix, set_configuration processed the current config first
// (setting can_vote=false for the demoted node), then skipped updating
// can_vote when processing the previous config (where the node is still
// a voter). This caused broadcast_read_quorum to skip the node.
BOOST_AUTO_TEST_CASE(test_tracker_voter_demotion_joint_config) {
    auto id1 = id(), id2 = id(), id3 = id();

    // Start with a 3-node all-voter configuration.
    raft::configuration cfg = config_from_ids({id1, id2, id3});

    // Enter joint config: demote id3 from voter to non-voter.
    cfg.enter_joint({
        {server_addr_from_id(id1), is_voter::yes},
        {server_addr_from_id(id2), is_voter::yes},
        {server_addr_from_id(id3), is_voter::no},
    });

    raft::tracker tracker;
    tracker.set_configuration(cfg, index_t{1});

    // id3 is a non-voter in current config but a voter in previous config.
    // During joint consensus it must still be treated as a voter.
    auto pr3 = tracker.find(id3);
    BOOST_CHECK_NE(pr3, nullptr);
    BOOST_CHECK_EQUAL(pr3->can_vote, is_voter::yes);

    // id1 and id2 should remain voters.
    BOOST_CHECK_EQUAL(tracker.find(id1)->can_vote, is_voter::yes);
    BOOST_CHECK_EQUAL(tracker.find(id2)->can_vote, is_voter::yes);
}

BOOST_AUTO_TEST_CASE(test_log_last_conf_idx) {
    // last_conf_idx, prev_conf_idx are initialized correctly,
    // and maintained during truncate head/truncate tail
    server_id id1 = id();
    raft::configuration cfg = config_from_ids({id1});
    raft::log log{raft::snapshot_descriptor{.config = cfg}};
    BOOST_CHECK_EQUAL(log.last_conf_idx(), index_t{0});
    add_entry(log, cfg);
    BOOST_CHECK_EQUAL(log.last_conf_idx(), index_t{1});
    add_entry(log, log_entry::dummy{});
    add_entry(log, cfg);
    BOOST_CHECK_EQUAL(log.last_conf_idx(), index_t{3});
    // apply snapshot truncates the log and resets last_conf_idx()
    log.apply_snapshot(log_snapshot(log, log.last_idx()), 0, 0);
    BOOST_CHECK_EQUAL(log.last_conf_idx(), log.get_snapshot().idx);
    // log::last_term() is maintained correctly by truncate_head/truncate_tail() (snapshotting)
    BOOST_CHECK_EQUAL(log.last_term(), log.get_snapshot().term);
    BOOST_CHECK(log.term_for(log.get_snapshot().idx));
    BOOST_CHECK_EQUAL(log.term_for(log.get_snapshot().idx).value(), log.get_snapshot().term);
    BOOST_CHECK(! log.term_for(log.last_idx() - index_t{1}));
    add_entry(log, log_entry::dummy{});
    BOOST_CHECK(log.term_for(log.last_idx()));
    add_entry(log, log_entry::dummy{});
    const size_t GAP = 10;
    // apply_snapshot with a log gap, this should clear all log
    // entries, despite that trailing is given, a gap
    // between old log entries and a snapshot would violate
    // log continuity.
    log.apply_snapshot(log_snapshot(log, log.last_idx() + index_t{GAP}), GAP * 2, std::numeric_limits<size_t>::max());
    BOOST_CHECK(log.empty());
    BOOST_CHECK_EQUAL(log.next_idx(), log.get_snapshot().idx + index_t{1});
    add_entry(log, log_entry::dummy{});
    BOOST_CHECK_EQUAL(log.in_memory_size(), 1);
    add_entry(log, log_entry::dummy{});
    BOOST_CHECK_EQUAL(log.in_memory_size(), 2);
    // Set trailing longer than the length of the log.
    log.apply_snapshot(log_snapshot(log, log.last_idx()), 3, std::numeric_limits<size_t>::max());
    BOOST_CHECK_EQUAL(log.in_memory_size(), 2);
    // Set trailing the same length as the current log length
    add_entry(log, log_entry::dummy{});
    BOOST_CHECK_EQUAL(log.in_memory_size(), 3);
    log.apply_snapshot(log_snapshot(log, log.last_idx()), 3, std::numeric_limits<size_t>::max());
    BOOST_CHECK_EQUAL(log.in_memory_size(), 3);
    BOOST_CHECK_EQUAL(log.last_conf_idx(), log.get_snapshot().idx);
    add_entry(log, log_entry::dummy{});
    // Set trailing shorter than the length of the log
    log.apply_snapshot(log_snapshot(log, log.last_idx()), 1, std::numeric_limits<size_t>::max());
    BOOST_CHECK_EQUAL(log.in_memory_size(), 1);
    // check that configuration from snapshot is used and not config entries from a trailing
    add_entry(log, cfg);
    add_entry(log, cfg);
    add_entry(log, log_entry::dummy{});
    auto snp_idx = log.last_idx();
    log.apply_snapshot(log_snapshot(log, snp_idx), 10, std::numeric_limits<size_t>::max());
    BOOST_CHECK_EQUAL(log.last_conf_idx(), snp_idx);
    // Check that configuration from the log is used if it has higher index then snapshot idx
    add_entry(log, log_entry::dummy{});
    snp_idx = log.last_idx();
    add_entry(log, cfg);
    add_entry(log, cfg);
    log.apply_snapshot(log_snapshot(log, snp_idx), 10, std::numeric_limits<size_t>::max());
    BOOST_CHECK_EQUAL(log.last_conf_idx(), log.last_idx());
}

void test_election_single_node_helper(raft::fsm_config fcfg) {

    server_id id1 = id();
    raft::configuration cfg = config_from_ids({id1});
    raft::log log{raft::snapshot_descriptor{.config = cfg}};
    fsm_debug fsm(id1, term_t{}, server_id{}, std::move(log), trivial_failure_detector, fcfg);

    election_timeout(fsm);

    // Immediately converts from leader to follower if quorum=1
    BOOST_CHECK(fsm.is_leader());

    auto output = fsm.get_output();

    BOOST_CHECK(output.term_and_vote);
    BOOST_CHECK(output.term_and_vote->first);
    BOOST_CHECK(output.term_and_vote->second);
    BOOST_CHECK(output.messages.empty());
    // A new leader applies one dummy entry
    BOOST_CHECK_EQUAL(output.log_entries.size(), 1);
    if (output.log_entries.size()) {
        BOOST_CHECK(std::holds_alternative<raft::log_entry::dummy>(output.log_entries[0]->data));
    }
    BOOST_CHECK(output.committed.empty());
    // The leader does not become candidate simply because
    // a timeout has elapsed, i.e. there are no spurious
    // elections.
    election_timeout(fsm);
    BOOST_CHECK(fsm.is_leader());
    output = fsm.get_output();
    BOOST_CHECK(!output.term_and_vote);
    BOOST_CHECK(output.messages.empty());
    BOOST_CHECK(output.log_entries.empty());
    // Dummy entry is now committed
    BOOST_CHECK_EQUAL(output.committed.size(), 1);
    if (output.committed.size()) {
        BOOST_CHECK(std::holds_alternative<raft::log_entry::dummy>(output.committed[0]->data));
    }
}

BOOST_AUTO_TEST_CASE(test_election_single_node) {
    test_election_single_node_helper(fsm_cfg);
}
// Test that adding an entry to a single-node cluster
// does not lead to RPC
BOOST_AUTO_TEST_CASE(test_single_node_is_quiet) {

    server_id id1 = id();
    raft::configuration cfg = config_from_ids({id1});
    raft::log log{raft::snapshot_descriptor{.config = cfg}};

    auto fsm = create_follower(id1, std::move(log));

    election_timeout(fsm);

    // Immediately converts from leader to follower if quorum=1
    BOOST_CHECK(fsm.is_leader());

    (void) fsm.get_output();

    fsm.add_entry(raft::command{});

    BOOST_CHECK(fsm.get_output().messages.empty());

    fsm.tick();

    BOOST_CHECK(fsm.get_output().messages.empty());
}

BOOST_AUTO_TEST_CASE(test_snapshot_follower_is_quiet) {
    server_id id1 = id(), id2 = id();

    raft::configuration cfg = config_from_ids({id1, id2});
    raft::log log(raft::snapshot_descriptor{.idx = index_t{999}, .config = cfg});

    log.emplace_back(seastar::make_lw_shared<raft::log_entry>(raft::log_entry{term_t{10}, index_t{1000}}));
    log.stable_to(log.last_idx());

    fsm_debug fsm(id1, term_t{10}, server_id{}, std::move(log), trivial_failure_detector, fsm_cfg);

    // become leader
    election_timeout(fsm);

    fsm.step(id2, raft::vote_reply{fsm.get_current_term(), true});

    BOOST_CHECK(fsm.is_leader());

    // clear output
    (void) fsm.get_output();

    // reply with reject pointing into the snapshot
    fsm.step(id2, raft::append_reply{fsm.get_current_term(), raft::index_t{1}, raft::append_reply::rejected{raft::index_t{1000}, raft::index_t{1}}});

    BOOST_CHECK(fsm.get_progress(id2).state == raft::follower_progress::state::SNAPSHOT);

    // clear output
    (void) fsm.get_output();

    for (int i = 0; i < 100; i++) {
      fsm.tick();
      BOOST_CHECK(fsm.get_output().messages.empty());
    }
}

BOOST_AUTO_TEST_CASE(test_election_two_nodes) {

    discrete_failure_detector fd;

    server_id id1 = id(), id2 = id();

    raft::configuration cfg = config_from_ids({id1, id2});
    raft::log log{raft::snapshot_descriptor{.config = cfg}};

    auto fsm = create_follower(id1, std::move(log), fd);

    // Initial state is follower
    BOOST_CHECK(fsm.is_follower());

    // After election timeout, a follower becomes a candidate
    election_timeout(fsm);
    BOOST_CHECK(fsm.is_candidate());

    // If nothing happens, the candidate stays this way
    election_timeout(fsm);
    BOOST_CHECK(fsm.is_candidate());

    auto output = fsm.get_output();
    BOOST_CHECK(output.term_and_vote);
    // After a favourable reply, we become a leader (quorum is 2)
    fsm.step(id2, raft::vote_reply{output.term_and_vote->first, true});
    BOOST_CHECK(fsm.is_leader());
    // Out of order response to the previous election is ignored
    fsm.step(id2, raft::vote_reply{output.term_and_vote->first - term_t{1}, false});
    BOOST_CHECK(fsm.is_leader());

    // Any message with a newer term after election timeout
    // -> immediately convert to follower
    fd.mark_all_dead();
    election_threshold(fsm);
    // Use current_term + 2 to switch fsm to follower
    // even if it itself switched to a candidate
    fsm.step(id2, raft::vote_request{output.term_and_vote->first + term_t{2}});
    BOOST_CHECK(fsm.is_follower());

    // Check that the candidate converts to a follower as well
    election_timeout(fsm);
    BOOST_CHECK(fsm.is_candidate());
    output = fsm.get_output();
    BOOST_CHECK(output.term_and_vote);
    fsm.step(id2, raft::vote_request{output.term_and_vote->first + term_t{1}});
    BOOST_CHECK(fsm.is_follower());

    // Test that a node doesn't cast a vote if it has voted for
    // self already
    (void) fsm.get_output();
    while (fsm.is_follower()) {
        fsm.tick();
    }
    BOOST_CHECK(fsm.is_candidate());
    output = fsm.get_output();
    auto msg = std::get<raft::vote_request>(output.messages.back().second);
    fsm.step(id2, std::move(msg));
    // We could figure out this round is going to a nowhere, but
    // we're not that smart and simply wait for a vote_reply.
    BOOST_CHECK(fsm.is_candidate());
    output = fsm.get_output();
    auto reply = std::get<raft::vote_reply>(output.messages.back().second);
    BOOST_CHECK(!reply.vote_granted);
}

BOOST_AUTO_TEST_CASE(test_election_four_nodes) {

    discrete_failure_detector fd;

    server_id id1 = id(), id2 = id(), id3 = id(), id4 = id();

    raft::configuration cfg = config_from_ids({id1, id2, id3, id4});
    raft::log log{raft::snapshot_descriptor{.config = cfg}};

    auto fsm = create_follower(id1, std::move(log), fd);

    // Initial state is follower
    BOOST_CHECK(fsm.is_follower());

    // Inform FSM about a new leader at a new term
    fsm.step(id4, raft::append_request{term_t{1}, index_t{1}, term_t{1}});

    (void) fsm.get_output();

    // Request a vote during the same term. Even though
    // we haven't voted, we should deny a vote because we
    // know about a leader for this term.
    fsm.step(id3, raft::vote_request{term_t{1}, index_t{1}, term_t{1}});

    auto output = fsm.get_output();
    auto reply = std::get<raft::vote_reply>(output.messages.back().second);
    BOOST_CHECK(!reply.vote_granted);

    // Run out of steam for this term. Start a new one.
    fd.mark_all_dead();
    election_timeout(fsm);
    BOOST_CHECK(fsm.is_candidate());

    output = fsm.get_output();
    BOOST_CHECK(output.term_and_vote);
    auto current_term = output.term_and_vote->first;
    // Add a favourable reply, not enough for quorum
    fsm.step(id2, raft::vote_reply{current_term, true});
    BOOST_CHECK(fsm.is_candidate());

    // Add another one, this adds up to quorum
    fsm.step(id3, raft::vote_reply{current_term, true});
    BOOST_CHECK(fsm.is_leader());
}

BOOST_AUTO_TEST_CASE(test_election_single_node_prevote) {
    auto fcfg = fsm_cfg;
    fcfg.enable_prevoting = true;
    test_election_single_node_helper(fcfg);
}

BOOST_AUTO_TEST_CASE(test_election_two_nodes_prevote) {
    auto fcfg = fsm_cfg;
    fcfg.enable_prevoting = true;

    server_id id1 = id(), id2 = id();

    raft::configuration cfg = config_from_ids({id1, id2});
    raft::log log{raft::snapshot_descriptor{.config = cfg}};

    fsm_debug fsm(id1, term_t{}, server_id{}, std::move(log), trivial_failure_detector, fcfg);

    // Initial state is follower
    BOOST_CHECK(fsm.is_follower());

    // After election timeout, a follower becomes a prevote candidate
    election_timeout(fsm);
    BOOST_CHECK(fsm.is_prevote_candidate());
    // Term was not increased
    BOOST_CHECK_EQUAL(fsm.get_current_term(), term_t{});

    // If nothing happens, the candidate stays this way
    election_timeout(fsm);
    BOOST_CHECK(fsm.is_prevote_candidate());
    BOOST_CHECK_EQUAL(fsm.get_current_term(), term_t{});

    auto output = fsm.get_output();
    // After a favourable prevote reply, we become a regular candidate (quorum is 2)
    BOOST_CHECK(!output.term_and_vote);
    fsm.step(id2, raft::vote_reply{fsm.get_current_term(), true, true});
    BOOST_CHECK(fsm.is_candidate() && !fsm.is_prevote_candidate());
    // And increased our term this time
    BOOST_CHECK_EQUAL(fsm.get_current_term(), term_t{1});

    election_timeout(fsm);
    // Check that rejected prevote with higher term causes prevote candidate move to follower
    fsm.step(id2, raft::vote_reply{term_t{2}, false, true});
    BOOST_CHECK(fsm.is_follower());
    BOOST_CHECK_EQUAL(fsm.get_current_term(), term_t{2});

    election_timeout(fsm);
    (void)fsm.get_output();
    // Check that receiving prevote with smaller term generate reject with newer term
    fsm.step(id2, raft::vote_request{term_t{1}, index_t{}, term_t{}, true});
    output = fsm.get_output();
    BOOST_CHECK_EQUAL(output.messages.size(), 1);
    auto msg = std::get<raft::vote_reply>(output.messages.back().second);
    BOOST_CHECK(msg.current_term == term_t{2} && !msg.vote_granted);

    // Check that prevote with higher term get a reply with term in the future
    // and does not change local term.
    // Move to follower again
    fsm.step(id2, raft::vote_reply{term_t{3}, false, true});
    BOOST_CHECK(fsm.is_follower());
    // Send prevote with higher term
    fsm.step(id2, raft::vote_request{term_t{4}, index_t{}, term_t{}, true});
    output = fsm.get_output();
    BOOST_CHECK_EQUAL(output.messages.size(), 1);
    // Reply has request's term
    msg = std::get<raft::vote_reply>(output.messages.back().second);
    BOOST_CHECK(msg.current_term == term_t{4} && msg.vote_granted);
    // But fsm current term stays the same
    BOOST_CHECK_EQUAL(fsm.get_current_term(), term_t{3});
}

BOOST_AUTO_TEST_CASE(test_election_four_nodes_prevote) {
    auto fcfg = fsm_cfg;
    fcfg.enable_prevoting = true;

    discrete_failure_detector fd;

    server_id id1 = id(), id2 = id(), id3 = id(), id4 = id();

    raft::configuration cfg = config_from_ids({id1, id2, id3, id4});
    raft::log log{raft::snapshot_descriptor{.config = cfg}};

    fsm_debug fsm(id1, term_t{}, server_id{}, std::move(log), fd, fcfg);

    // Initial state is follower
    BOOST_CHECK(fsm.is_follower());

    // Inform FSM about a new leader at a new term
    fsm.step(id4, raft::append_request{term_t{1}, index_t{1}, term_t{1}});

    (void) fsm.get_output();

    // Request a prevote during the same term. Even though
    // we haven't voted, we should deny a vote because we
    // know about a leader for this term.
    fsm.step(id3, raft::vote_request{term_t{1}, index_t{1}, term_t{1}, true});

    auto output = fsm.get_output();
    auto reply = std::get<raft::vote_reply>(output.messages.back().second);
    BOOST_CHECK(!reply.vote_granted && reply.is_prevote);

    // Run out of steam for this term. Start a new one.
    fd.mark_all_dead();
    election_timeout(fsm);
    BOOST_CHECK(fsm.is_candidate() && fsm.is_prevote_candidate());

    output = fsm.get_output();
    // Add a favourable prevote reply, not enough for quorum
    BOOST_CHECK(!output.term_and_vote);
    fsm.step(id2, raft::vote_reply{fsm.get_current_term() + term_t{1}, true, true});
    BOOST_CHECK(fsm.is_candidate() && fsm.is_prevote_candidate());

    // Add another one, this adds up to quorum
    fsm.step(id3, raft::vote_reply{fsm.get_current_term() + term_t{1}, true, true});
    BOOST_CHECK(fsm.is_candidate() && !fsm.is_prevote_candidate());

    // Check that prevote with future term is answered even if we voted already
    // Request regular vote
    fsm.step(id2, raft::vote_request{fsm.get_current_term(), index_t{1}, term_t{1}, false});
    // Clear message queue
    (void)fsm.get_output();
    // Ask for prevote with future term
    fsm.step(id3, raft::vote_request{fsm.get_current_term() + term_t{1}, index_t{1}, term_t{1}, true});
    output = fsm.get_output();
    BOOST_CHECK_EQUAL(output.messages.size(), 1);
    reply = std::get<raft::vote_reply>(output.messages.back().second);
    BOOST_CHECK(reply.vote_granted && reply.is_prevote);
}

BOOST_AUTO_TEST_CASE(test_log_matching_rule) {

    server_id id1 = id(), id2 = id(), id3 = id();

    raft::configuration cfg = config_from_ids({id1, id2, id3});
    raft::log log(raft::snapshot_descriptor{.idx = index_t{999}, .config = cfg});

    log.emplace_back(seastar::make_lw_shared<raft::log_entry>(raft::log_entry{term_t{10}, index_t{1000}}));
    log.stable_to(log.last_idx());

    fsm_debug fsm(id1, term_t{10}, server_id{}, std::move(log), trivial_failure_detector, fsm_cfg);

    // Initial state is follower
    BOOST_CHECK(fsm.is_follower());

    (void) fsm.get_output();

    fsm.step(id2, raft::vote_request{term_t{9}, index_t{1001}, term_t{11}});
    // Current term is too old - vote is not granted
    auto output = fsm.get_output();
    BOOST_CHECK(output.messages.empty());

    auto request_vote = [&](term_t term, index_t last_log_idx, term_t last_log_term) -> raft::vote_reply {
        fsm.step(id2, raft::vote_request{term, last_log_idx, last_log_term});
        auto output = fsm.get_output();
        return std::get<raft::vote_reply>(output.messages.back().second);
    };

    // Last stable index is too small - vote is not granted
    BOOST_CHECK(!request_vote(term_t{11}, index_t{999}, term_t{10}).vote_granted);
    // Last stable term is too small - vote is not granted
    BOOST_CHECK(!request_vote(term_t{12}, index_t{1002}, term_t{9}).vote_granted);
    // Last stable term and index are equal to the voter's - vote
    // is granted
    BOOST_CHECK(request_vote(term_t{13}, index_t{1000}, term_t{10}).vote_granted);
    // Last stable term is the same, index is greater to the voter's - vote
    // is granted
    BOOST_CHECK(request_vote(term_t{14}, index_t{1001}, term_t{10}).vote_granted);
    // Both term and index are greater than the voter's - vote
    // is granted
    BOOST_CHECK(request_vote(term_t{15}, index_t{1001}, term_t{11}).vote_granted);
}

BOOST_AUTO_TEST_CASE(test_confchange_add_node) {

    server_id id1 = id(), id2 = id(), id3 = id();

    raft::configuration cfg = config_from_ids({id1, id2});
    raft::log log(raft::snapshot_descriptor{.idx = index_t{100}, .config = cfg});

    auto fsm = create_follower(id1, std::move(log));

    // Initial state is follower
    BOOST_CHECK(fsm.is_follower());

    // Turn to a leader
    election_timeout(fsm);
    BOOST_CHECK(fsm.is_candidate());
    auto output = fsm.get_output();
    BOOST_CHECK(output.term_and_vote);
    fsm.step(id2, raft::vote_reply{output.term_and_vote->first, true});
    BOOST_CHECK(fsm.is_leader());

    output = fsm.get_output();
    // A new leader applies one dummy entry
    BOOST_CHECK_EQUAL(output.log_entries.size(), 1);
    if (output.log_entries.size()) {
        BOOST_CHECK(std::holds_alternative<raft::log_entry::dummy>(output.log_entries[0]->data));
    }
    BOOST_CHECK(output.committed.empty());
    // accept dummy entry, otherwise no more entries will be sent
    BOOST_CHECK_EQUAL(output.messages.size(), 1);
    auto msg = std::get<raft::append_request>(output.messages.back().second);
    auto idx = msg.entries.back()->idx;
    fsm.step(id2, raft::append_reply{msg.current_term, idx, raft::append_reply::accepted{idx}});

    raft::configuration newcfg = config_from_ids({id1, id2, id3});
    // Suggest a confchange.
    fsm.add_entry(newcfg);
    // Can't have two confchanges in progress.
    BOOST_CHECK_THROW(fsm.add_entry(newcfg), raft::conf_change_in_progress);
    // Entered joint configuration immediately.
    BOOST_CHECK(fsm.get_configuration().is_joint());
    BOOST_CHECK_EQUAL(fsm.get_configuration().previous.size(), 2);
    BOOST_CHECK_EQUAL(fsm.get_configuration().current.size(), 3);
    output = fsm.get_output();
    // The output contains a log entry to be committed.
    // Once it's committed, it will be replicated.
    // The output must contain messages both for id2 and id3
    BOOST_CHECK_EQUAL(output.log_entries.size(), 1);
    // Append entry for id2 and id3
    BOOST_CHECK_EQUAL(output.messages.size(), 2);
    msg = std::get<raft::append_request>(output.messages.back().second);
    idx = msg.entries.back().get()->idx;
    // In order to accept a configuration change
    // we need one ACK, since there is a quorum overlap.
    // Strictly speaking the new node needs to install a snapshot,
    // first, for simplicity let's assume it's happened already.

    fsm.step(id2, raft::append_reply{msg.current_term, idx, raft::append_reply::accepted{idx}});
    // One reply is enough to commit the joint configuration,
    // since there is a quorum overlap between the two
    // configurations.
    BOOST_CHECK(! fsm.get_configuration().is_joint());
    // Still can't have two confchanges in progress, even though
    // we left joint already, the final configuration is not
    // committed yet.
    BOOST_CHECK_THROW(fsm.add_entry(newcfg), raft::conf_change_in_progress);
    output = fsm.get_output();
    // A log entry for the final configuration
    BOOST_CHECK_EQUAL(output.log_entries.size(), 1);
    // AppendEntries messages for the final configuration
    BOOST_CHECK(output.messages.size() >= 1);
    msg = std::get<raft::append_request>(output.messages.back().second);
    idx = msg.entries.back().get()->idx;
    // Ack AppendEntries for the final configuration
    fsm.step(id2, raft::append_reply{msg.current_term, idx, raft::append_reply::accepted{idx}});
    BOOST_CHECK_EQUAL(fsm.get_configuration().current.size(), 3);
    fsm.step(id3, raft::append_reply{msg.current_term, idx, raft::append_reply::accepted{idx}});
    // Check that we can start a new confchange
    raft::configuration newcfg2 = config_from_ids({id1, id2});
    fsm.add_entry(newcfg);
}

BOOST_AUTO_TEST_CASE(test_confchange_remove_node) {

    server_id id1 = id(), id2 = id(), id3 = id();

    raft::configuration cfg = config_from_ids({id1, id2, id3});
    raft::log log(raft::snapshot_descriptor{.idx = index_t{100}, .config = cfg});

    auto fsm = create_follower(id1, std::move(log));

    // Initial state is follower
    BOOST_CHECK(fsm.is_follower());

    // Turn to a leader
    election_timeout(fsm);
    BOOST_CHECK(fsm.is_candidate());
    auto output = fsm.get_output();
    // Vote requests to id2 and id3
    BOOST_CHECK_EQUAL(output.messages.size(), 2);
    if (output.messages.size() > 0) {
        BOOST_CHECK(std::holds_alternative<raft::vote_request>(output.messages[0].second));
    }
    if (output.messages.size() > 1) {
        BOOST_CHECK(std::holds_alternative<raft::vote_request>(output.messages[1].second));
    }

    BOOST_CHECK(output.term_and_vote);
    fsm.step(id2, raft::vote_reply{output.term_and_vote->first, true});
    BOOST_CHECK(fsm.is_leader());
    output = fsm.get_output();
    BOOST_CHECK_EQUAL(output.log_entries.size(), 1);
    if (output.log_entries.size()) {
        BOOST_CHECK(std::holds_alternative<raft::log_entry::dummy>(output.log_entries[0]->data));
    }
    // accept dummy entry, otherwise no more entries will be sent
    BOOST_CHECK_EQUAL(output.messages.size(), 2);
    auto msg = std::get<raft::append_request>(output.messages.back().second);
    auto idx = msg.entries.back()->idx;
    fsm.step(id2, raft::append_reply{msg.current_term, idx, raft::append_reply::accepted{idx}});
    fsm.step(id3, raft::append_reply{msg.current_term, idx, raft::append_reply::accepted{idx}});

    raft::configuration newcfg = config_from_ids({id1, id2});
    // Suggest a confchange.
    fsm.add_entry(newcfg);
    // Entered joint configuration immediately.
    BOOST_CHECK(fsm.get_configuration().is_joint());
    BOOST_CHECK_EQUAL(fsm.get_configuration().current.size(), 2);
    BOOST_CHECK_EQUAL(fsm.get_configuration().previous.size(), 3);
    output = fsm.get_output();
    // The output contains a log entry to be committed.
    // Once it's committed, it will be replicated.
    BOOST_CHECK_EQUAL(output.log_entries.size(), 1);
    if (output.log_entries.size()) {
        BOOST_CHECK(std::holds_alternative<raft::configuration>(output.log_entries[0]->data));
    }
    BOOST_CHECK_EQUAL(output.messages.size(), 2); // Configuration change sent to id2 and id3
    BOOST_REQUIRE_NO_THROW(msg = std::get<raft::append_request>(output.messages[0].second));
    BOOST_CHECK_EQUAL(msg.entries.size(), 1);
    BOOST_CHECK(std::holds_alternative<raft::configuration>(msg.entries[0]->data));
    idx = msg.entries.back().get()->idx;
    BOOST_CHECK_EQUAL(idx, index_t{102});
    // Ack AppendEntries for the joint configuration
    // In order to accept a configuration change
    // we need one ACK, since there is a quorum overlap.
    fsm.step(id2, raft::append_reply{msg.current_term, idx, raft::append_reply::accepted{idx}});

    // Final configuration is proposed
    output = fsm.get_output();
    // AppendEntries messages for the final configuration
    BOOST_CHECK_EQUAL(output.messages.size(), 1);

    BOOST_REQUIRE_NO_THROW(msg = std::get<raft::append_request>(output.messages[0].second));
    // A log entry for the final configuration
    BOOST_CHECK_EQUAL(msg.entries.size(), 1);
    if (output.log_entries.size()) {
        BOOST_CHECK(std::holds_alternative<raft::configuration>(msg.entries[0]->data));
    }

    idx = msg.entries.back().get()->idx;
    BOOST_CHECK_EQUAL(idx, index_t{103});

    BOOST_CHECK_EQUAL(fsm.get_configuration().current.size(), 2);
    BOOST_CHECK(!fsm.get_configuration().is_joint());

    // Ack AppendEntries for final configuration
    fsm.step(id2, raft::append_reply{msg.current_term, idx, raft::append_reply::accepted{idx}});

    // Check that we can start a new confchange
    raft::configuration newcfg2 = config_from_ids({id1, id2, id3});
    fsm.add_entry(newcfg);
    BOOST_CHECK_EQUAL(fsm.get_configuration().current.size(), 2);
}

BOOST_AUTO_TEST_CASE(test_confchange_replace_node) {

    server_id id1 = id(), id2 = id(), id3 = id(), id4 = id();

    raft::configuration cfg = config_from_ids({id1, id2, id3});
    raft::log log(raft::snapshot_descriptor{.idx = index_t{100}, .config = cfg});

    auto fsm = create_follower(id1, std::move(log));

    // Initial state is follower
    BOOST_CHECK(fsm.is_follower());

    // Turn to a leader
    election_timeout(fsm);
    BOOST_CHECK(fsm.is_candidate());
    auto output = fsm.get_output();
    BOOST_CHECK(output.term_and_vote);
    fsm.step(id2, raft::vote_reply{output.term_and_vote->first, true});
    BOOST_CHECK(fsm.is_leader());
    output = fsm.get_output();
    BOOST_CHECK_EQUAL(output.log_entries.size(), 1);
    if (output.log_entries.size()) {
        BOOST_CHECK(std::holds_alternative<raft::log_entry::dummy>(output.log_entries[0]->data));
    }
    BOOST_CHECK(output.committed.empty());
    // accept dummy entry, otherwise no more entries will be sent
    BOOST_CHECK_EQUAL(output.messages.size(), 2);
    auto msg = std::get<raft::append_request>(output.messages.back().second);
    auto idx = msg.entries.back()->idx;
    fsm.step(id2, raft::append_reply{msg.current_term, idx, raft::append_reply::accepted{idx}});
    fsm.step(id3, raft::append_reply{msg.current_term, idx, raft::append_reply::accepted{idx}});

    raft::configuration newcfg = config_from_ids({id1, id2, id4});
    // Suggest a confchange.
    fsm.add_entry(newcfg);
    // Entered joint configuration immediately.
    BOOST_CHECK(fsm.get_configuration().is_joint());
    BOOST_CHECK_EQUAL(fsm.get_configuration().current.size(), 3);
    BOOST_CHECK_EQUAL(fsm.get_configuration().previous.size(), 3);
    output = fsm.get_output();
    BOOST_REQUIRE_NO_THROW(msg = std::get<raft::append_request>(output.messages[0].second));
    idx = msg.entries.back().get()->idx;
    // In order to accept a configuration change
    // we need two ACK, since there is a quorum overlap.
    fsm.step(id2, raft::append_reply{msg.current_term, idx, raft::append_reply::accepted{idx}});
    BOOST_CHECK(!fsm.get_configuration().is_joint());
    // final config to log
    output = fsm.get_output();
    BOOST_CHECK_EQUAL(output.log_entries.size(), 1);
    if (output.log_entries.size()) {
        BOOST_CHECK(std::holds_alternative<raft::configuration>(output.log_entries[0]->data));
    }
    // AppendEntries messages for the final configuration
    BOOST_CHECK(output.messages.size() >= 1);
    msg = std::get<raft::append_request>(output.messages.back().second);
    idx = msg.entries.back().get()->idx;
    // Ack AppendEntries for the final configuration
    fsm.step(id2, raft::append_reply{msg.current_term, idx, raft::append_reply::accepted{idx}});
    BOOST_CHECK_EQUAL(fsm.get_configuration().current.size(), 3);
    BOOST_CHECK(!fsm.get_configuration().is_joint());
}


BOOST_AUTO_TEST_CASE(test_leader_stepdown) {

    server_id id1 = id(), id2 = id(), id3 = id();

    raft::configuration cfg({
        {server_addr_from_id(id1), is_voter::yes}, {server_addr_from_id(id2), is_voter::yes}, {server_addr_from_id(id3), is_voter::no}});
    raft::log log(raft::snapshot_descriptor{.config = cfg});

    fsm_debug fsm(id1, term_t{1}, /* voted for */ server_id{}, std::move(log), trivial_failure_detector, fsm_cfg);

    // Check that we move to candidate state on timeout_now message
    fsm.step(id2, raft::timeout_now{fsm.get_current_term()});
    BOOST_CHECK(fsm.is_candidate());
    auto output = fsm.get_output();
    auto vote_request = std::get<raft::vote_request>(output.messages.back().second);
    // Check that vote_request has `force` flag set.
    BOOST_CHECK(vote_request.force);

    // Turn to a leader
    fsm.step(id2, raft::vote_reply{fsm.get_current_term(), true});
    BOOST_CHECK(fsm.is_leader());

    // make id2's match idx to be up-to-date
    output = fsm.get_output();
    auto append = std::get<raft::append_request>(output.messages.back().second);
    auto idx = append.entries.back()->idx;
    fsm.step(id2, raft::append_reply{fsm.get_current_term(), index_t{}, raft::append_reply::accepted{idx}});

    // start leadership transfer while there is a fully up-to-date follower
    fsm.transfer_leadership();

    // Check that timeout_now message is sent
    output = fsm.get_output();
    BOOST_CHECK_EQUAL(output.messages.size(), 1);
    BOOST_CHECK(std::holds_alternative<raft::timeout_now>(output.messages.back().second));

    // Turn to a leader again
    // ... first turn to a follower
    fsm.step(id2, raft::vote_request{fsm.get_current_term() + term_t{1}, index_t{10}, term_t{}, false, true});
    BOOST_CHECK(fsm.is_follower());
    (void)fsm.get_output();
    // ... and now leader
    election_timeout(fsm);
    BOOST_CHECK(fsm.is_candidate());
    output = fsm.get_output();
    fsm.step(id2, raft::vote_reply{fsm.get_current_term(), true});
    BOOST_CHECK(fsm.is_leader());
    output = fsm.get_output();
    append = std::get<raft::append_request>(output.messages.back().second);
    idx = append.entries.back()->idx;

    // start leadership transfer while there is no fully up-to-date follower
    // (dummy entry appended by become_leader is not replicated yet)
    fsm.transfer_leadership();

    // check that no timeout_now message was sent
    output = fsm.get_output();
    BOOST_CHECK_EQUAL(output.messages.size(), 0);

    // Now make non voting follower match the log and see that timeout_now is not sent
    fsm.step(id3, raft::append_reply{fsm.get_current_term(), index_t{}, raft::append_reply::accepted{idx}});
    output = fsm.get_output();
    BOOST_CHECK_EQUAL(output.messages.size(), 0);

    // Now make voting follower match the log and see that timeout_now is sent
    fsm.step(id2, raft::append_reply{fsm.get_current_term(), index_t{}, raft::append_reply::accepted{idx}});
    output = fsm.get_output();
    BOOST_CHECK_EQUAL(output.messages.size(), 1);
    BOOST_CHECK(std::holds_alternative<raft::timeout_now>(output.messages.back().second));

    // Turn to a leader yet again
    // ... first turn to a follower
    fsm.step(id2, raft::vote_request{fsm.get_current_term() + term_t{1}, index_t{10}, term_t{}, false, true});
    BOOST_CHECK(fsm.is_follower());
    (void)fsm.get_output();
    // ... and now leader
    election_timeout(fsm);
    BOOST_CHECK(fsm.is_candidate());
    output = fsm.get_output();
    fsm.step(id2, raft::vote_reply{fsm.get_current_term(), true});
    BOOST_CHECK(fsm.is_leader());
    // Commit dummy entry
    output = fsm.get_output();
    append = std::get<raft::append_request>(output.messages.back().second);
    idx = append.entries.back()->idx;
    fsm.step(id2, raft::append_reply{fsm.get_current_term(), idx, raft::append_reply::accepted{idx}});

    // Drop the leader from the current config and see that stepdown message is sent
    raft::configuration newcfg({{server_addr_from_id(id2), is_voter::yes}, {server_addr_from_id(id3), is_voter::no}});
    fsm.add_entry(newcfg);
    output = fsm.get_output();
    append = std::get<raft::append_request>(output.messages.back().second);
    idx = append.entries.back()->idx;
    // Accept joint config entry on id2
    fsm.step(id2, raft::append_reply{fsm.get_current_term(), idx, raft::append_reply::accepted{idx}});
    // fms added new config to the log
    output = fsm.get_output();
    append = std::get<raft::append_request>(output.messages.back().second);
    idx = append.entries.back()->idx;
    // Accept new config entry on id2
    fsm.step(id2, raft::append_reply{fsm.get_current_term(), idx, raft::append_reply::accepted{idx}});

    // And check that the deposed leader sent timeout_now
    output = fsm.get_output();
    BOOST_CHECK_EQUAL(output.messages.size(), 1);
    BOOST_CHECK(std::holds_alternative<raft::timeout_now>(output.messages.back().second));


    /// Check that leader stepdown works when the leader is removed from the config and there are entries above C_new in its log
    raft::configuration cfg2({
        {server_addr_from_id(id1), is_voter::yes}, {server_addr_from_id(id2), is_voter::yes}, {server_addr_from_id(id3), is_voter::yes}});
    raft::log log2(raft::snapshot_descriptor{.config = cfg});

    fsm_debug fsm2(id1, term_t{1}, /* voted for */ server_id{}, std::move(log2), trivial_failure_detector, fsm_cfg);

    election_timeout(fsm2);
    // Turn to a leader
    fsm2.step(id2, raft::vote_reply{fsm2.get_current_term(), true});
    BOOST_CHECK(fsm2.is_leader());
    output = fsm2.get_output();
    append = std::get<raft::append_request>(output.messages.back().second);
    idx = append.entries.back()->idx;
    // Accept the dummy on id2
    fsm2.step(id2, raft::append_reply{fsm2.get_current_term(), idx, raft::append_reply::accepted{idx}});
    // Accept the dummy on id3
    fsm2.step(id3, raft::append_reply{fsm2.get_current_term(), idx, raft::append_reply::accepted{idx}});

    // Drop the leader from the current config and see that stepdown message is sent
    raft::configuration newcfg2({{server_addr_from_id(id2), is_voter::yes}, {server_addr_from_id(id3), is_voter::yes}});
    fsm2.add_entry(newcfg2);
    output = fsm2.get_output();
    append = std::get<raft::append_request>(output.messages.back().second);
    idx = append.entries.back()->idx;
    // Accept joint config entry on id2
    fsm2.step(id2, raft::append_reply{fsm2.get_current_term(), idx, raft::append_reply::accepted{idx}});
    // Accept joint config entry on id3
    fsm2.step(id3, raft::append_reply{fsm2.get_current_term(), idx, raft::append_reply::accepted{idx}});
    // fsm added new config entry
    output = fsm2.get_output();
    append = std::get<raft::append_request>(output.messages.back().second);
    idx = append.entries.back()->idx;

    fsm2.add_entry(raft::command{}); // add one more command that will be not replicated yet

    // Accept new config entry on id2
    fsm2.step(id2, raft::append_reply{fsm2.get_current_term(), idx, raft::append_reply::accepted{idx}});
    // Accept new config entry on id3
    fsm2.step(id3, raft::append_reply{fsm2.get_current_term(), idx, raft::append_reply::accepted{idx}});
    // C_new is now committed
    output = fsm2.get_output(); // this sends out the entry submitted after C_new
    append = std::get<raft::append_request>(output.messages.back().second);
    idx = append.entries.back()->idx;
    // Accept the entry
    fsm2.step(id2, raft::append_reply{fsm2.get_current_term(), idx, raft::append_reply::accepted{idx}});
    // And check that the deposed leader sent timeout_now
    output = fsm2.get_output();
    BOOST_CHECK_EQUAL(output.messages.size(), 1);
    BOOST_CHECK(std::holds_alternative<raft::timeout_now>(output.messages.back().second));
    /// End test
}

BOOST_AUTO_TEST_CASE(test_truncate_rederives_config_indices) {
    // When a leader's AppendEntries overwrites a follower's last
    // configuration entry, log::truncate_uncommitted() must re-derive the
    // two tracked configuration indices from the entries that remain in the
    // log. Rolling _last_conf_idx back to _prev_conf_idx and zeroing
    // _prev_conf_idx is not enough: an older configuration entry may still
    // be in the log, and with _prev_conf_idx zeroed both
    // get_prev_configuration() and last_conf_for() skip it and wrongly fall
    // back to the snapshot configuration. last_conf_for() supplies the
    // configuration stored in snapshots, so a snapshot taken at an index
    // between the two surviving configuration entries would be stamped with
    // a stale configuration.
    server_id A = id(), B = id(), C = id(), D = id();

    // Original cluster {A,B,C,D} captured in the snapshot at idx 0.
    raft::configuration snap_cfg = config_from_ids({A, B, C, D});

    // Log contents:
    //   idx 1 (term 1): committed config {A,B,C}
    //   idx 2 (term 1): dummy entry
    //   idx 3 (term 1): committed config {A,B}
    //   idx 4 (term 2): uncommitted joint config curr={A} prev={A,B}
    raft::log_entries entries;
    entries.push_back(seastar::make_lw_shared<const raft::log_entry>(
            raft::log_entry{term_t{1}, index_t{1}, config_from_ids({A, B, C})}));
    entries.push_back(seastar::make_lw_shared<const raft::log_entry>(
            raft::log_entry{term_t{1}, index_t{2}, raft::log_entry::dummy{}}));
    entries.push_back(seastar::make_lw_shared<const raft::log_entry>(
            raft::log_entry{term_t{1}, index_t{3}, config_from_ids({A, B})}));
    entries.push_back(seastar::make_lw_shared<const raft::log_entry>(
            raft::log_entry{term_t{2}, index_t{4},
                    raft::configuration{config_set({A}), config_set({A, B})}}));

    raft::log log(raft::snapshot_descriptor{.idx = index_t{0}, .config = snap_cfg},
            std::move(entries));
    BOOST_CHECK_EQUAL(log.last_conf_idx(), index_t{4});
    BOOST_CHECK(log.get_configuration().is_joint());

    // A new leader (term 3) overwrites the log from idx 4 with a
    // non-configuration entry, conflicting on term. This truncates the
    // uncommitted joint configuration entry.
    raft::log_entry_ptr_list overwrite;
    overwrite.push_back(seastar::make_lw_shared<const raft::log_entry>(
            raft::log_entry{term_t{3}, index_t{4}, raft::log_entry::dummy{}}));
    log.maybe_append(std::move(overwrite), 0, false);

    // The config at idx 3 is the effective one again, and the older config
    // at idx 1 - not the snapshot config - is the previous one.
    BOOST_CHECK_EQUAL(log.last_conf_idx(), index_t{3});
    BOOST_CHECK(!log.get_configuration().is_joint());
    BOOST_CHECK(log.get_configuration().current == config_from_ids({A, B}).current);
    BOOST_REQUIRE(log.get_prev_configuration() != nullptr);
    BOOST_CHECK(log.get_prev_configuration()->current == config_from_ids({A, B, C}).current);
    BOOST_CHECK(log.last_conf_for(index_t{2}).current == config_from_ids({A, B, C}).current);
    BOOST_CHECK(log.last_conf_for(index_t{1}).current == config_from_ids({A, B, C}).current);
}

BOOST_AUTO_TEST_CASE(test_empty_configuration) {
    // When a server is joining an existing cluster, its configuration is empty.
    // The leader sends its configuration over in AppendEntries or
    // ApplySnapshot RPC. Test this scenario.

    server_id id1 = id();

    raft::configuration cfg{config_member_set()};
    raft::log log(raft::snapshot_descriptor{.idx = index_t{0}, .config = cfg});
    auto follower = create_follower(id1, std::move(log));
    // Initial state is follower
    BOOST_CHECK(follower.is_follower());
    election_timeout(follower);
    BOOST_CHECK(follower.is_follower());
    auto output = follower.get_output();
    BOOST_CHECK_EQUAL(output.log_entries.size(), 0);
    BOOST_CHECK_EQUAL(output.messages.size(), 0);
    BOOST_CHECK_EQUAL(follower.get_current_term(), term_t{0});

    server_id id2 = id();
    auto log2 = raft::log(raft::snapshot_descriptor{.idx = index_t{0}, .config = config_from_ids({id2})});
    auto leader = create_follower(id2, std::move(log2));
    election_timeout(leader);
    BOOST_CHECK(leader.is_leader());
    // Transitioning to an empty configuration is not supported.
    BOOST_CHECK_THROW(leader.add_entry(raft::configuration{config_member_set()}), std::invalid_argument);
    leader.add_entry(config_from_ids({id1, id2}));

    communicate(leader, follower);
    BOOST_CHECK_EQUAL(follower.get_current_term(), term_t{1});
    BOOST_CHECK_EQUAL(follower.in_memory_log_size(), leader.in_memory_log_size());
    BOOST_CHECK_EQUAL(leader.get_configuration().is_joint(), false);
}

BOOST_AUTO_TEST_CASE(test_confchange_a_to_b) {
    // Test we can transition from a single-server configuration
    // {A} to a single server configuration {B}

    server_id A_id = id();

    raft::log log(raft::snapshot_descriptor{.idx = index_t{0}, .config = config_from_ids({A_id})});
    auto A = create_follower(A_id, log);
    election_timeout(A);
    BOOST_CHECK(A.is_leader());
    // Let's have a non-empty log at A
    A.add_entry(log_entry::dummy{});

    server_id B_id = id();

    auto B = create_follower(B_id, log);

    A.add_entry(config_from_ids({B_id}));

    communicate(A, B);
    BOOST_CHECK_EQUAL(A.get_current_term(), term_t{1});
    BOOST_CHECK(A.is_follower());
    // A is not part of the current configuration
    BOOST_CHECK(B.is_leader());
    BOOST_CHECK_EQUAL(B.get_current_term(), term_t{2});
    BOOST_CHECK_EQUAL(B.get_configuration().is_joint(), false);
    BOOST_CHECK_EQUAL(B.get_configuration().current.size(), 1);
    BOOST_CHECK(B.get_configuration().current.contains(config_member_from_id(B_id)));
    // Let's try the same configuration change now, but let's
    // restart the leader after persisting the joint
    // configuration.
    log = raft::log(raft::snapshot_descriptor{.idx = B.log_last_idx(), .term = B.log_last_term(),
        .config = B.get_configuration()});
    // A somewhat awkward way to obtain B's log for restart
    log.emplace_back(make_lw_shared<raft::log_entry>(B.add_entry(config_from_ids({A_id}))));
    log.stable_to(log.last_idx());
    fsm_debug B_1(B_id, B.get_current_term(), B_id, std::move(log), trivial_failure_detector, fsm_cfg);
    election_timeout(B_1);
    communicate(A, B_1);
    BOOST_CHECK(B_1.is_follower());
    election_timeout(A);
    BOOST_CHECK(A.is_leader());
    // B_1 must be quiet after an election timeout and doesn't
    // disrupt the new configuration
    election_timeout(B_1);
    BOOST_CHECK(B_1.is_follower());
    BOOST_CHECK_EQUAL(B_1.get_output().messages.size(), 0);
}


BOOST_AUTO_TEST_CASE(test_confchange_ab_to_cd) {
    // Similar to A -> B change, but with many nodes,
    // so C_new has to campaign after configuration change.
    server_id A_id = id(), B_id = id(), C_id = id(), D_id = id();

    raft::log log(raft::snapshot_descriptor{.idx = index_t{0}, .config = config_from_ids({A_id, B_id})});
    auto A = create_follower(A_id, log);
    auto B = create_follower(B_id, log);
    election_timeout(A);
    communicate(A, B);
    BOOST_CHECK(A.is_leader());

    auto C = create_follower(C_id, log);
    auto D = create_follower(D_id, log);

    A.add_entry(config_from_ids({C_id, D_id}));
    communicate(A, B, C, D);

    BOOST_CHECK_EQUAL(A.get_current_term(), term_t{1});
    // A and B are not part of the current configuration
    BOOST_CHECK(A.is_follower());
    BOOST_CHECK(B.is_follower());

    election_timeout(C);
    election_threshold(D);
    communicate(A, B, C, D);
    BOOST_CHECK_EQUAL(C.get_current_term(), term_t{2});
    BOOST_CHECK(C.is_leader());
    BOOST_CHECK_EQUAL(C.get_configuration().is_joint(), false);
    BOOST_CHECK_EQUAL(C.get_configuration().current.size(), 2);
}


BOOST_AUTO_TEST_CASE(test_confchange_abc_to_cde) {
    // Check configuration changes when C_old and C_new have no
    // common quorum, test leader change during configuration
    // change
    discrete_failure_detector fd;
    server_id A_id = id(), B_id = id(), C_id = id(), D_id = id(), E_id = id();

    raft::log log(raft::snapshot_descriptor{.idx = index_t{0}, .config = config_from_ids({A_id, B_id, C_id})});
    auto A = create_follower(A_id, log);
    auto B = create_follower(B_id, log, fd);
    auto C = create_follower(C_id, log, fd);
    election_timeout(A);
    communicate(A, B, C);
    BOOST_CHECK(A.is_leader());

    auto D = create_follower(D_id, log);
    auto E = create_follower(E_id, log);

    A.add_entry(config_from_ids({C_id, D_id, E_id}));
    // Make sure C gets a new (joint) configuration entry.
    // It is stable, but not committed, because we need D or E
    // to commit it.
    communicate(A, B, C);
    // Leader change while committing a joint configuration
    fd.mark_dead(A_id);
    election_timeout(C);
    BOOST_CHECK(C.is_candidate());
    // Ticking for election_threshold at B is
    // necessary for B to vote for C but not become
    // candidate itself.
    election_threshold(B);
    communicate(B, C, D, E);
    BOOST_CHECK(A.is_leader());
    BOOST_CHECK_EQUAL(A.get_current_term(), term_t{1});
    BOOST_CHECK(B.is_follower());
    BOOST_CHECK(C.is_leader());
    BOOST_CHECK(D.is_follower());
    BOOST_CHECK(E.is_follower());

    BOOST_CHECK(C.get_current_term() >= term_t{2});
    BOOST_CHECK_EQUAL(C.get_configuration().is_joint(), false);
    BOOST_CHECK_EQUAL(C.get_configuration().current.size(), 3);
}


BOOST_AUTO_TEST_CASE(test_confchange_abcdef_to_abcgh) {
    // Test configuration changes in presence of down nodes in C_old
    discrete_failure_detector fd;
    server_id A_id = id(), B_id = id(), C_id = id(), D_id = id(), E_id = id(),
              F_id = id(), G_id = id(), H_id = id();

    raft::log log(raft::snapshot_descriptor{.idx = index_t{0},
        .config = config_from_ids({A_id, B_id, C_id, D_id, E_id, F_id})});
    auto A = create_follower(A_id, log, fd);
    auto B = create_follower(B_id, log, fd);
    auto C = create_follower(C_id, log, fd);
    auto D = create_follower(D_id, log, fd);
    auto E = create_follower(E_id, log, fd);
    auto F = create_follower(F_id, log, fd);
    election_timeout(D);
    communicate(A, D, E, F);
    BOOST_CHECK(D.is_leader());

    auto G = create_follower(G_id, log);
    auto H = create_follower(H_id, log);

    D.add_entry(config_from_ids({A_id, B_id, C_id, G_id, H_id}));
    // We can't transition to C_new in absence of C_old majority
    communicate(B, C, D, G, H);
    BOOST_CHECK(D.is_leader());
    BOOST_CHECK(D.get_configuration().is_joint());
    D.tick();
    communicate(B, C, E, D, G, H);
    BOOST_CHECK(D.is_follower());
    auto leader = select_leader(A, B, C, G, H);
    BOOST_CHECK_EQUAL(leader->get_configuration().is_joint(), false);
    BOOST_CHECK_EQUAL(leader->get_configuration().current.size(), 5);

    fd.mark_all_dead();
    election_timeout(D);
    election_timeout(A);
    communicate(A, B, C, D, E, F, G, H);
    BOOST_CHECK(leader->is_leader());
}

BOOST_AUTO_TEST_CASE(test_confchange_abcde_abcdefg) {
    // Check configuration changes work fine with many nodes down
    discrete_failure_detector fd;
    server_id A_id = id(), B_id = id(), C_id = id(), D_id = id(), E_id = id(),
              F_id = id(), G_id = id();

    raft::log log(raft::snapshot_descriptor{.idx = index_t{0},
        .config = config_from_ids({A_id, B_id, C_id, D_id, E_id})});
    auto A = create_follower(A_id, log, fd);
    auto B = create_follower(B_id, log, fd);
    auto C = create_follower(C_id, log, fd);
    auto D = create_follower(D_id, log, fd);
    auto E = create_follower(E_id, log, fd);
    election_timeout(A);
    communicate(A, D, E);
    BOOST_CHECK(A.is_leader());

    auto F = create_follower(F_id, log);
    auto G = create_follower(G_id, log);

    // Wrap configuration entry into some traffic
    A.add_entry(log_entry::dummy{});
    A.add_entry(config_from_ids({A_id, B_id, C_id, D_id, E_id, F_id, G_id}));
    A.add_entry(log_entry::dummy{});
    // Without tick() A won't re-try communication with nodes it
    // believes are down (B, C).
    A.tick();
    // 4 is enough to transition to the new configuration
    communicate(A, B, C, G);
    BOOST_CHECK(A.is_leader());
    BOOST_CHECK_EQUAL(A.get_configuration().is_joint(), false);
    BOOST_CHECK_EQUAL(A.get_configuration().current.size(), 7);
    A.tick();
    communicate(A, B, C, D, E, F, G);
    BOOST_CHECK_EQUAL(A.log_last_idx(), B.log_last_idx());
    BOOST_CHECK_EQUAL(A.log_last_idx(), C.log_last_idx());
    BOOST_CHECK_EQUAL(A.log_last_idx(), D.log_last_idx());
    BOOST_CHECK_EQUAL(A.log_last_idx(), E.log_last_idx());
    BOOST_CHECK_EQUAL(A.log_last_idx(), F.log_last_idx());
    BOOST_CHECK_EQUAL(A.log_last_idx(), G.log_last_idx());
    BOOST_CHECK(A.is_leader());
    BOOST_CHECK_EQUAL(A.get_configuration().is_joint(), false);
    BOOST_CHECK_EQUAL(A.get_configuration().current.size(), 7);
}

BOOST_AUTO_TEST_CASE(test_election_during_confchange) {
    server_id A_id = id(), B_id = id(), C_id = id(), D_id = id(), E_id = id();

    raft::log log(raft::snapshot_descriptor{.idx = index_t{0}, .config = config_from_ids({A_id, B_id, C_id})});

    // Joint config has reached old majority, the leader is
    // from new majority
    discrete_failure_detector fd;
    auto A = create_follower(A_id, log, fd);
    auto B = create_follower(B_id, log, fd);
    auto C = create_follower(C_id, log, fd);
    election_timeout(A);
    communicate(A, B, C);
    A.add_entry(config_from_ids({C_id, D_id, E_id}));
    communicate(A, B, C);
    fd.mark_dead(A_id);
    auto D = create_follower(D_id, log, fd);
    auto E = create_follower(E_id, log, fd);
    election_timeout(C);
    election_threshold(B);
    communicate_until([&C]() { return C.is_leader(); }, B, C, D, E);
    BOOST_CHECK_EQUAL(C.get_configuration().is_joint(), true);
    fd.mark_alive(A.id());
    communicate(D, A, B, E);
    fd.mark_alive(C.id());
    communicate_until([&C]() { return C.get_configuration().is_joint() == false; }, B, C, D, E);
    communicate(C, D);
    fd.mark_dead(C.id());
    election_timeout(D);
    // E may still be in joint. It must vote for D anyway. D is in C_new
    // and will replicate C_new to E after becoming a leader
    election_threshold(E);
    A.tick();
    communicate(A, D, E);
    BOOST_CHECK(D.is_leader());
    BOOST_CHECK(A.is_follower());
    BOOST_CHECK_EQUAL(D.get_configuration().is_joint(), false);
    BOOST_CHECK_EQUAL(D.get_configuration().current.size(), 3);
}

BOOST_AUTO_TEST_CASE(test_reply_from_removed_follower) {
    // Messages from followers may be delayed. Check they don't
    // upset the leader when they are delivered past configuration
    // change

    server_id A_id = id(), B_id = id();

    raft::log log(raft::snapshot_descriptor{.idx = index_t{0}, .config = config_from_ids({A_id, B_id})});
    auto A = create_follower(A_id, log);
    auto B = create_follower(B_id, log);
    election_timeout(A);
    communicate(A, B);
    A.add_entry(config_from_ids({A_id}));
    communicate(A, B);
    BOOST_CHECK(A.is_leader());
    BOOST_CHECK_EQUAL(A.get_configuration().is_joint(), false);
    BOOST_CHECK_EQUAL(A.get_configuration().current.size(), 1);
    auto idx = A.log_last_idx();
    A.step(B.id(), raft::append_reply{A.get_current_term(), idx, raft::append_reply::accepted{idx}});
    A.step(B.id(), raft::append_reply{A.get_current_term(), idx, raft::append_reply::rejected{idx}});
    A.step(B.id(), raft::snapshot_reply{A.get_current_term(), true});
    BOOST_CHECK(A.is_leader());
}

BOOST_AUTO_TEST_CASE(test_leader_ignores_messages_with_current_term) {
    // Check that the leader properly handles InstallSnapshot/AppendRequest/VoteRequest
    // messages carrying its own term.
    discrete_failure_detector fd;
    server_id A_id = id(), B_id = id();

    raft::log log(raft::snapshot_descriptor{.idx = index_t{0},
        .config = config_from_ids({A_id, B_id})});
    auto A = create_follower(A_id, log, fd);
    auto B = create_follower(B_id, log, fd);
    election_timeout(A);
    communicate(A, B);
    BOOST_CHECK(A.is_leader());
    // Check that InstallSnapshot with current term gets negative reply
    A.step(B.id(), raft::install_snapshot{A.get_current_term()});
    auto output = A.get_output();
    BOOST_CHECK_EQUAL(output.messages.size(), 1);
    raft::snapshot_reply msg;
    BOOST_REQUIRE_NO_THROW(msg = std::get<raft::snapshot_reply>(output.messages[0].second));
    BOOST_CHECK(!msg.success);
    // Check that AppendRequest with current term is ignired by the leader
    A.step(B.id(), raft::append_request{A.get_current_term()});
    output = A.get_output();
    BOOST_CHECK_EQUAL(output.messages.size(), 0);
    // Check that VoteRequest with current term is not granted
    A.step(B.id(), raft::vote_request{A.get_current_term(), index_t{}, term_t{}, false, false});
    output = A.get_output();
    BOOST_CHECK_EQUAL(output.messages.size(), 1);
    raft::vote_reply msg1;
    BOOST_REQUIRE_NO_THROW(msg1 = std::get<raft::vote_reply>(output.messages[0].second));
    BOOST_CHECK(!msg1.vote_granted);
}

BOOST_AUTO_TEST_CASE(test_leader_read_quorum) {
    discrete_failure_detector fd;
    server_id A_id = id(), B_id = id(), C_id = id(), D_id = id();

    // 4 nodes 3 voting 1 non voting (quorum is 2)
    raft::config_member_set nodes{config_member_from_id(A_id), config_member_from_id(B_id),
            config_member_from_id(C_id), raft::config_member{server_addr_from_id(D_id), is_voter::no}};

    raft::log log(raft::snapshot_descriptor{.idx = index_t{0}, .config = raft::configuration(nodes)});
    auto A = create_follower(A_id, log, fd);
    auto B = create_follower(B_id, log, fd);
    auto C = create_follower(C_id, log, fd);
    auto D = create_follower(D_id, log, fd);
    election_timeout(A);
    communicate(A, B, C, D);
    BOOST_CHECK(A.is_leader());
    // Just because timeout passes the leader does not stepdown if quorum of nodes is alive
    election_timeout(A);
    BOOST_CHECK(A.is_leader());
    // One of voting members dies but the leader is still not steepping down because there is
    // a quorum of nodes that are still alive
    fd.mark_dead(C_id);
    election_timeout(A);
    BOOST_CHECK(A.is_leader());
    // Non voting member dies and the leader is still not stepping down (there two voting members still)
    fd.mark_dead(D_id);
    election_timeout(A);
    BOOST_CHECK(A.is_leader());
    // One more voting members dies and the leader becomes a follower now
    fd.mark_dead(B_id);
    election_timeout(A);
    BOOST_CHECK(!A.is_leader());
}

BOOST_AUTO_TEST_CASE(test_zero) {
    server_id id{};
    BOOST_CHECK_THROW(raft::configuration cfg(config_set({id})), std::invalid_argument);
    BOOST_CHECK_THROW(raft::configuration cfg(raft::config_member_set{config_member_from_id(id)}), std::invalid_argument);
    BOOST_CHECK_THROW(create_follower(id, raft::log(raft::snapshot_descriptor{})), std::invalid_argument);
}

BOOST_AUTO_TEST_CASE(test_reordered_reject) {
    auto id1 = id();
    fsm_debug fsm1(id1, term_t{1}, server_id{},
            raft::log{raft::snapshot_descriptor{.config = config_from_ids({id1})}},
            trivial_failure_detector, fsm_cfg);

    while (!fsm1.is_leader()) {
        fsm1.tick();
    }

    fsm1.add_entry(log_entry::dummy{});
    (void)fsm1.get_output();

    auto id2 = id();
    fsm_debug fsm2(id2, term_t{1}, server_id{},
            raft::log{raft::snapshot_descriptor{.config = raft::configuration{}}},
            trivial_failure_detector, fsm_cfg);

    raft_routing_map routes{{fsm1.id(), &fsm1}, {fsm2.id(), &fsm2}};

    fsm1.add_entry(config_from_ids({fsm1.id(), fsm2.id()}));

    fsm1.tick();

    // fsm1 sends append_entries with idx=2 to fsm2
    auto append_idx2_1 = fsm1.get_output();

    fsm1.tick();

    // fsm1 sends append_entries with idx=2 to fsm2 (again)
    auto append_idx2_2 = fsm1.get_output();

    raft::logger.trace("delivering first append idx=2");
    deliver(routes, fsm1.id(), std::move(append_idx2_1.messages));

    // fsm2 rejects the first idx=2 append
    auto reject_1 = fsm2.get_output();

    raft::logger.trace("delivering second append idx=2");
    deliver(routes, fsm1.id(), std::move(append_idx2_2.messages));

    // fsm2 rejects the second idx=2 append
    auto reject_2 = fsm2.get_output();

    raft::logger.trace("delivering first reject");
    deliver(routes, fsm2.id(), std::move(reject_1.messages));

    // fsm1 sends append_entries with idx=1 to fsm2
    auto append_idx1 = fsm1.get_output();

    raft::logger.trace("delivering append idx=1");
    deliver(routes, fsm1.id(), std::move(append_idx1.messages));

    // fsm2 accepts the idx=1 append
    auto accept = fsm2.get_output();

    raft::logger.trace("delivering accept for append idx=1");
    deliver(routes, fsm2.id(), std::move(accept.messages));

    raft::logger.trace("delivering second reject");
    deliver(routes, fsm2.id(), std::move(reject_2.messages));
}

BOOST_AUTO_TEST_CASE(test_non_voter_stays_pipeline) {
    // Check that a node stays in PIPELINE mode
    // through configuration changes.
    server_id A_id = id(), B_id = id();
    raft::config_member_set addrset{
        raft::config_member{server_addr_from_id(A_id), is_voter::yes},
        raft::config_member{server_addr_from_id(B_id), is_voter::no}};
    raft::configuration cfg(addrset);
    raft::log log(raft::snapshot_descriptor{.idx = index_t{0}, .config = cfg});
    auto A = create_follower(A_id, log);
    auto B = create_follower(B_id, log);
    election_timeout(A);
    communicate(A);
    BOOST_CHECK(A.is_leader());
    BOOST_CHECK(A.get_progress(B_id).state == raft::follower_progress::state::PROBE);
    A.add_entry(log_entry::dummy{});
    // We need to deliver a probe from A to B so that B gets
    // a change to respond and A can switch B to PIPELINE mode.
    A.tick();
    communicate(A, B);
    BOOST_CHECK(A.get_progress(B_id).state == raft::follower_progress::state::PIPELINE);
    raft::configuration newcfg = config_from_ids({A_id, B_id});
    A.add_entry(newcfg);
    communicate(A, B);
    BOOST_CHECK_EQUAL(A.get_configuration().is_joint(), false);
    BOOST_CHECK_EQUAL(A.get_configuration().current.find(config_member_from_id(B_id))->can_vote, is_voter::yes);
    BOOST_CHECK(A.get_progress(B_id).state == raft::follower_progress::state::PIPELINE);
    A.add_entry(cfg);
    auto tick_occasionally = [&A, &B]() {
        if (rolladice()) {
            A.tick(); B.tick();
        }
        return false;
    };
    communicate_until(tick_occasionally, A, B);
    BOOST_CHECK_EQUAL(A.get_configuration().is_joint(), false);
    BOOST_CHECK_EQUAL(A.get_configuration().current.find(config_member_from_id(B_id))->can_vote, is_voter::no);
    BOOST_CHECK(A.get_progress(B_id).state == raft::follower_progress::state::PIPELINE);
}

BOOST_AUTO_TEST_CASE(test_leader_change_to_non_voter) {
    // Test a two-node cluster, change a leader to a non-voter.
    server_id A_id = id(), B_id = id();
    raft::config_member_set oldset{
        raft::config_member{server_addr_from_id(A_id), is_voter::yes},
        raft::config_member{server_addr_from_id(B_id), is_voter::no}};
    raft::configuration cfg(oldset);
    raft::log log(raft::snapshot_descriptor{.idx = index_t{0}, .config = cfg});
    auto A = create_follower(A_id, log);
    auto B = create_follower(B_id, log);
    election_timeout(A);
    communicate(A, B);
    BOOST_CHECK(A.is_leader());
    raft::config_member_set newset{
        raft::config_member{server_addr_from_id(A_id), is_voter::no},
        raft::config_member{server_addr_from_id(B_id), is_voter::yes}};
    raft::configuration newcfg(newset);
    A.add_entry(newcfg);
    A.tick();
    communicate(A, B);
    BOOST_CHECK(A.is_follower());
    BOOST_CHECK(B.is_leader());
    // Try to switch the leader to a non-voter, leaving no other voters.
    newset = raft::config_member_set{
        raft::config_member{server_addr_from_id(A_id), is_voter::no},
        raft::config_member{server_addr_from_id(B_id), is_voter::no}};
    newcfg = raft::configuration(newset);
    BOOST_CHECK_THROW(B.add_entry(newcfg), std::invalid_argument);
    // Try to remove the last remaining voter
    newset = raft::config_member_set{raft::config_member{server_addr_from_id(B_id), is_voter::no}};
    newcfg = raft::configuration(newset);
    BOOST_CHECK_THROW(B.add_entry(newcfg), std::invalid_argument);
}

BOOST_AUTO_TEST_CASE(test_non_voter_gets_timeout_now) {
    // Test that even if a non-voter gets timeout now, there is no
    // elections and later this learner can rejoin the cluster,
    // although it does  disrupt the cluster a bit (through
    // leader's having to increase its term).
    server_id A_id = id(), B_id = id(), C_id = id();
    raft::configuration cfg(raft::config_member_set{
            raft::config_member{server_addr_from_id(A_id), is_voter::yes},
            raft::config_member{server_addr_from_id(B_id), is_voter::yes},
            raft::config_member{server_addr_from_id(C_id), is_voter::no}});

    raft::log log(raft::snapshot_descriptor{.idx = index_t{0}, .config = cfg});
    auto A = create_follower(A_id, log);
    auto B = create_follower(B_id, log);
    auto C = create_follower(C_id, log);
    election_timeout(A);
    communicate(A, B, C);
    BOOST_CHECK(A.is_leader());
    C.step(A.id(), raft::timeout_now{.current_term = A.get_current_term()});
    C.tick();
    auto output = C.get_output();
    BOOST_CHECK(C.is_follower());
    BOOST_CHECK_EQUAL(output.messages.size(), 0);
    BOOST_CHECK(!output.term_and_vote);
    A.add_entry(log_entry::dummy{});
    communicate(A, B, C);
    BOOST_CHECK_EQUAL(A.log_last_idx(), C.log_last_idx());
    BOOST_CHECK_EQUAL(A.get_current_term(), C.get_current_term());
    BOOST_CHECK(A.is_leader());
}

BOOST_AUTO_TEST_CASE(test_non_voter_election_timeout) {
    // Test that non-voter doesn't start election even if its
    // election timeout expires and it doesn't see a valid leader.
    discrete_failure_detector fd;
    server_id A_id = id(), B_id = id(), C_id = id();
    raft::configuration cfg(raft::config_member_set{
            raft::config_member{server_addr_from_id(A_id), is_voter::yes},
            raft::config_member{server_addr_from_id(B_id), is_voter::yes},
            raft::config_member{server_addr_from_id(C_id), is_voter::no}});

    raft::log log(raft::snapshot_descriptor{.idx = index_t{0}, .config = cfg});
    auto A = create_follower(A_id, log, fd);
    auto B = create_follower(B_id, log, fd);
    auto C = create_follower(C_id, log, fd);
    election_timeout(A);
    communicate(A, B, C);
    BOOST_CHECK(A.is_leader());
    fd.mark_all_dead();
    auto C_term = C.get_current_term();
    election_timeout(C);
    BOOST_CHECK(C.is_follower());
    BOOST_CHECK_EQUAL(C_term, C.get_current_term());
}

BOOST_AUTO_TEST_CASE(test_non_voter_voter_loop) {
    // Test voter-non-voter change in a loop
    server_id A_id = id(), B_id = id(), C_id = id();

    raft::configuration cfg = config_from_ids({A_id, B_id, C_id});
    raft::configuration cfg_with_non_voter(raft::config_member_set{
            raft::config_member{server_addr_from_id(A_id), is_voter::yes},
            raft::config_member{server_addr_from_id(B_id), is_voter::yes},
            raft::config_member{server_addr_from_id(C_id), is_voter::no}});

    raft::log log(raft::snapshot_descriptor{.idx = index_t{0}, .config = cfg});
    auto A = create_follower(A_id, log);
    auto B = create_follower(B_id, log);
    auto C = create_follower(C_id, log);
    election_timeout(A);
    communicate(A, B, C);
    BOOST_CHECK(A.is_leader());
    for (int i = 0; i < 100; ++i) {
        A.add_entry(i % 2 ? cfg_with_non_voter : cfg);
        if (rolladice()) {
            A.add_entry(log_entry::dummy{});
        }
        communicate(A, B, C);
        if (rolladice()) {
            A.add_entry(log_entry::dummy());
            communicate(A, B, C);
        }
        // If iteration count is large, this helps save some
        // memory
        if (rolladice(1./1000)) {
            A.get_log().apply_snapshot(log_snapshot(A.get_log(), A.log_last_idx()), 0, 0);
        }
        if (rolladice(1./100)) {
            B.get_log().apply_snapshot(log_snapshot(A.get_log(), B.log_last_idx()), 0, 0);
        }
        if (rolladice(1./5000)) {
            C.get_log().apply_snapshot(log_snapshot(A.get_log(), B.log_last_idx()), 0, 0);
        }
    }
    BOOST_CHECK(A.is_leader());
    BOOST_CHECK_EQUAL(A.get_current_term(), C.get_current_term());
    BOOST_CHECK_EQUAL(A.log_last_idx(), C.log_last_idx());
}

BOOST_AUTO_TEST_CASE(test_non_voter_confchange_in_snapshot) {
    // Test non-voter learns it's a non-voter via snapshot
    discrete_failure_detector fd;
    server_id A_id = id(), B_id = id(), C_id = id();

    raft::configuration cfg = config_from_ids({A_id, B_id, C_id});

    raft::log log(raft::snapshot_descriptor{.idx = index_t{0}, .config = cfg});
    auto A = create_follower(A_id, log, fd);
    auto B = create_follower(B_id, log, fd);
    auto C = create_follower(C_id, log, fd);
    election_timeout(A);
    communicate(A, B, C);
    BOOST_CHECK(A.is_leader());
    A.add_entry(log_entry::dummy{});
    raft::configuration cfg_with_non_voter(raft::config_member_set{
            raft::config_member{server_addr_from_id(A_id), is_voter::yes},
            raft::config_member{server_addr_from_id(B_id), is_voter::yes},
            raft::config_member{server_addr_from_id(C_id), is_voter::no}});
    A.tick();
    A.add_entry(cfg_with_non_voter);
    A.tick();
    // Majority commits the configuration change
    communicate(A, B);
    BOOST_CHECK_EQUAL(A.get_configuration().is_joint(), false);
    BOOST_CHECK_EQUAL(A.get_configuration().current.find(config_member_from_id(C_id))->can_vote, is_voter::no);
    A.tick();
    raft::snapshot_descriptor A_snp{.idx = A.log_last_idx(), .term = A.log_last_term(), .config = A.get_configuration()};
    A.apply_snapshot(A_snp, 0, 0, true);
    A.tick();
    communicate(A, B, C);
    BOOST_CHECK(A.is_leader());
    BOOST_CHECK_EQUAL(A.get_current_term(), C.get_current_term());
    BOOST_CHECK_EQUAL(A.log_last_idx(), C.log_last_idx());
    // A non-voter doesn't become candidate on election timeout
    fd.mark_all_dead();
    election_timeout(C);
    BOOST_CHECK(C.is_follower());
    // Now try the same trick, but this time convert a non-voter
    // to a voter with a snapshot
    fd.mark_all_alive();
    A.tick();
    for (int i = 0; i < 100; i++) {
        A.add_entry(log_entry::dummy{});
    }
    A.add_entry(cfg);
    A.tick();
    // Majority commits the configuration change
    communicate(A, B);
    BOOST_CHECK_EQUAL(A.get_configuration().is_joint(), false);
    BOOST_CHECK_EQUAL(A.get_configuration().current.find(config_member_from_id(C_id))->can_vote, is_voter::yes);
    A.tick();
    A_snp = raft::snapshot_descriptor{.idx = A.log_last_idx(), .term = A.log_last_term(), .config = A.get_configuration()};
    A.apply_snapshot(A_snp, 0, 0, true);
    A.tick();
    communicate(A, B, C);
    BOOST_CHECK(A.is_leader());
    BOOST_CHECK_EQUAL(A.get_current_term(), C.get_current_term());
    BOOST_CHECK_EQUAL(A.log_last_idx(), C.log_last_idx());
    fd.mark_all_dead();
    election_timeout(C);
    BOOST_CHECK(C.is_candidate());
    // Check an ex-voter can become a leader alright (LearnerPromotion)
    election_threshold(B);
    communicate(C, B);
    BOOST_CHECK(C.is_leader());
}

BOOST_AUTO_TEST_CASE(test_non_voter_can_vote) {
    // Test non-voter can vote when it is requested to - it may
    // not be aware of the configuration in which it is a voter
    discrete_failure_detector fd;
    server_id A_id = id(), B_id = id(), C_id = id();

    raft::configuration cfg(raft::config_member_set{
            raft::config_member{server_addr_from_id(A_id), is_voter::yes},
            raft::config_member{server_addr_from_id(B_id), is_voter::yes},
            raft::config_member{server_addr_from_id(C_id), is_voter::no}});

    raft::log log(raft::snapshot_descriptor{.idx = index_t{0}, .config = cfg});
    auto A = create_follower(A_id, log, fd);
    auto B = create_follower(B_id, log, fd);
    auto C = create_follower(C_id, log, fd);
    election_timeout(A);
    communicate(A, B, C);
    BOOST_CHECK(A.is_leader());

    raft::configuration cfg_all_voters = config_from_ids({A_id, B_id, C_id});
    A.add_entry(cfg_all_voters);
    // Majority commits the configuration change
    communicate(A, B);
    BOOST_CHECK_EQUAL(A.get_configuration().is_joint(), false);
    BOOST_CHECK_EQUAL(A.get_configuration().current.find(config_member_from_id(C_id))->can_vote, is_voter::yes);
    BOOST_CHECK_EQUAL(A.log_last_idx(), B.log_last_idx());
    fd.mark_dead(A_id);
    election_timeout(B);
    election_threshold(C);
    // B and C are enough to elect B in the new configuration.
    communicate(B, C);
    BOOST_CHECK(B.is_leader());
    BOOST_CHECK_EQUAL(B.get_current_term(), C.get_current_term());
    BOOST_CHECK_EQUAL(B.log_last_idx(), C.log_last_idx());
}

BOOST_AUTO_TEST_CASE(test_leader_transferee_dies_upon_receiving_timeout_now) {
    /// 4-node cluster (A, B, C, D). A is initially elected a leader.
    /// The leader adds a new configuration entry, that removes it from the
    /// cluster (B, C, D).
    /// Communicate the cluster up to the point where A starts to resign
    /// its leadership (calls `transfer_leadership()`).
    /// At this point, A should send a `timeout_now` message to one
    /// the remaining nodes (B, C or D) and the new configuration should be
    /// committed. But no nodes actually have received the `timeout_now` message
    /// yet.
    ///
    /// Determine on which node the message should arrive, accept the
    /// `timeout_now` message and disconnect the target from the rest of the
    /// group.
    ///
    /// Check that after that the cluster, which has only two live members,
    /// could progress and elect a new leader through a normal election process.

    discrete_failure_detector fd;

    raft::server_id A_id = id(), B_id = id(), C_id = id(), D_id = id();
    raft::log log(raft::snapshot_descriptor{.idx = raft::index_t{0},
        .config = config_from_ids({A_id, B_id, C_id, D_id})});
    auto A = create_follower(A_id, log, fd);
    auto B = create_follower(B_id, log, fd);
    auto C = create_follower(C_id, log, fd);
    auto D = create_follower(D_id, log, fd);

    raft_routing_map map;
    map.emplace(A_id, &A);
    map.emplace(B_id, &B);
    map.emplace(C_id, &C);
    map.emplace(D_id, &D);

    // A becomes leader
    election_timeout(A);
    communicate(A, B, C, D);
    BOOST_CHECK(A.is_leader());

    // Add a cfg entry on leader that removes it from the cluster ({B_id, C_id, D_id})
    raft::configuration newcfg = config_from_ids({B_id, C_id, D_id});
    A.add_entry(newcfg);

    // Commit new config and stop communicating right after A steps down due to
    // starting leadership transfer.
    communicate_until([&A] { return !A.is_leader(); }, A, B, C, D);

    // At this point A should have a `timeout_now` message in its message queue.
    BOOST_CHECK(A.is_follower());
    // We cannot assume which node will be selected as the target for
    // `timeout_now` message, because the order in which A should test each
    // follower whether it's an eligible target for `timeout_now` is
    // unspecified. Let's call it X. X can be either B, C, or D.
    //
    // Maintain the routing map state since it will be used later to
    // determine which two nodes will remain in the cluster after partitioning
    // `timeout_now` target node (X) away.
    map.erase(A_id);

    // We don't really care on which node `timeout_now` message arrives so adapt
    // in a dynamic fashion.
    //
    // Check that A has sent the `timeout_now` message and determine to whom it was sent
    auto output = A.get_output();
    BOOST_CHECK_EQUAL(output.messages.size(), 1);
    BOOST_CHECK(std::holds_alternative<raft::timeout_now>(output.messages.back().second));
    auto timeout_now_target_id = output.messages.back().first;
    auto timeout_now_msg = std::get<raft::timeout_now>(output.messages.back().second);

    // Accept the message on target node...
    map[timeout_now_target_id]->step(A_id, std::move(timeout_now_msg));

    // ...and immediately cut it from the rest of the cluster so that others think it's dead.
    fd.mark_dead(timeout_now_target_id);
    map.erase(timeout_now_target_id);

    // Two more nodes should remain in the cluster.
    // Again, we don't care which node from these two would like to become a leader,
    // so just select the first one in the list of remaining nodes.
    //
    // Wait for standard election_timeout() on the first node, and for election_threshold()
    // on the second.
    // Then, check, that the new leader is elected among these two remaining nodes.
    auto first_fsm = map.begin();
    auto second_fsm = ++map.begin();
    election_timeout(*first_fsm->second);
    election_threshold(*second_fsm->second);
    communicate(B, C, D);
    auto final_leader = select_leader(B, C, D);
    BOOST_CHECK(final_leader->id() == first_fsm->first || final_leader->id() == second_fsm->first);
}

BOOST_AUTO_TEST_CASE(test_leader_transfer_lost_timeout_now) {
    /// 3-node cluster (A, B, C). A is initially elected a leader.
    /// The leader adds a new configuration entry, that removes it from the
    /// cluster (B, C).
    ///
    /// Wait up until the former leader commits the new configuration and starts
    /// leader transfer procedure, sending out the `timeout_now` message to
    /// one of the remaining nodes. But at that point it haven't received it yet.
    ///
    /// Lose this message and verify that the rest of the cluster (B, C)
    /// can make progress and elect a new leader.

    raft::server_id A_id = id(), B_id = id(), C_id = id();
    raft::log log(raft::snapshot_descriptor{.idx = raft::index_t{0},
        .config = config_from_ids({A_id, B_id, C_id})});
    auto A = create_follower(A_id, log);
    auto B = create_follower(B_id, log);
    auto C = create_follower(C_id, log);

    // A becomes leader
    election_timeout(A);
    communicate(A, B, C);
    BOOST_CHECK(A.is_leader());

    // Add a cfg entry on leader that removes it from the cluster ({B_id, C_id})
    raft::configuration newcfg = config_from_ids({B_id, C_id});
    A.add_entry(newcfg);

    // Commit new config and stop communicating right after A steps down due to
    // starting leadership transfer.
    communicate_until([&A] { return !A.is_leader(); }, A, B, C);

    // We don't really care on which node `timeout_now` message should arrive,
    // since it'll be lost, anyway.
    //
    // Check that the `timeout_now` message was sent...
    auto output = A.get_output();
    BOOST_CHECK_EQUAL(output.messages.size(), 1);
    BOOST_CHECK(std::holds_alternative<raft::timeout_now>(output.messages.back().second));
    auto timeout_now_msg = std::get<raft::timeout_now>(output.messages.back().second);
    (void)timeout_now_msg;
    // ... and lose it.

    // By now, B and C should both remain in the follower state.
    // Check that and attempt to go forward with a normal election process to see
    // that the cluster operates normally after `timeout_now` has been lost.
    BOOST_CHECK(B.is_follower());
    BOOST_CHECK(C.is_follower());

    // Elect B a leader and check that normal election proceeds as expected.
    election_timeout(B);
    election_threshold(C);
    communicate(B, C);
    BOOST_CHECK(B.is_leader());
}

BOOST_AUTO_TEST_CASE(test_leader_transfer_lost_force_vote_request) {
    /// 3-node cluster (A, B, C). A is initially elected a leader.
    /// The leader adds a new configuration entry, that removes it from the
    /// cluster (B, C).
    ///
    /// Wait up until the former leader commits the new configuration and starts
    /// leader transfer procedure, sending out the `timeout_now` message to
    /// one of the remaining nodes. But at that point it haven't received it yet.
    ///
    /// Deliver the `timeout_now` message to the target but lose all the
    /// `vote_request(force)` messages it attempts to send.
    /// This should halt the election process.
    /// Then wait for election timeout so that candidate node starts another
    /// normal election (without `force` flag for vote requests).
    ///
    /// Check that this candidate further makes progress and is elected a
    /// leader.

    raft::server_id A_id = id(), B_id = id(), C_id = id();
    raft::log log(raft::snapshot_descriptor{.idx = raft::index_t{0},
        .config = config_from_ids({A_id, B_id, C_id})});
    auto A = create_follower(A_id, log);
    auto B = create_follower(B_id, log);
    auto C = create_follower(C_id, log);

    raft_routing_map map;
    map.emplace(A_id, &A);
    map.emplace(B_id, &B);
    map.emplace(C_id, &C);

    // A becomes leader
    election_timeout(A);
    communicate(A, B, C);
    BOOST_CHECK(A.is_leader());

    // Add a cfg entry on leader that removes it from the cluster ({B_id, C_id})
    raft::configuration newcfg = config_from_ids({B_id, C_id});
    A.add_entry(newcfg);

    // Commit new config and stop communicating right after A steps down due to
    // starting leadership transfer.
    communicate_until([&A] { return !A.is_leader(); }, A, B, C);
    map.erase(A_id);

    // We don't really care on which node `timeout_now` message arrives so adapt
    // in a dynamic fashion.
    //
    // Check that A has sent the `timeout_now` message and determine to whom it was sent
    auto output = A.get_output();
    BOOST_CHECK_EQUAL(output.messages.size(), 1);
    BOOST_CHECK(std::holds_alternative<raft::timeout_now>(output.messages.back().second));
    auto timeout_now_target_id = output.messages.back().first;
    auto timeout_now_msg = std::get<raft::timeout_now>(output.messages.back().second);

    // Accept the message on the node selected by A to be eligible for leadership transfer.
    auto& timeout_now_target = *map[timeout_now_target_id];
    timeout_now_target.step(A_id, std::move(timeout_now_msg));
    // New candidate should've sent a vote_request with force flag set
    output = timeout_now_target.get_output();
    BOOST_CHECK_EQUAL(output.messages.size(), 1);
    BOOST_CHECK(std::holds_alternative<raft::vote_request>(output.messages.front().second));
    auto vote_req1 = std::get<raft::vote_request>(output.messages.front().second);
    BOOST_CHECK(vote_req1.force);

    // Lose the forced vote request so that the candidates' election is halted.
    // After election timeout has passed it should become a regular candidate and
    // then proceed with non-force vote requests to elect itself a leader through
    // the normal election process.
    election_timeout(timeout_now_target);
    output = timeout_now_target.get_output();
    BOOST_CHECK_EQUAL(output.messages.size(), 1);
    BOOST_CHECK(std::holds_alternative<raft::vote_request>(output.messages.front().second));
    // These requests will be sent after election threshold passes for other remaining nodes.
    auto vote_req1_regular = std::get<raft::vote_request>(output.messages.front().second);
    auto vote_req1_regular_target = output.messages.front().first;
    BOOST_CHECK(!vote_req1_regular.force);

    // Pass election threshold for remaining node and send pending regular vote request
    election_threshold(*map[vote_req1_regular_target]);
    map[vote_req1_regular_target]->step(timeout_now_target_id, std::move(vote_req1_regular));

    communicate(B, C);
    auto final_leader = select_leader(B, C);
    BOOST_CHECK(final_leader->id() == timeout_now_target_id);
}

// A follower should reject remote snapshots that are behind its current commit index.
BOOST_AUTO_TEST_CASE(test_reject_outdated_remote_snapshot) {
    server_id A_id = id(), B_id = id();
    raft::configuration cfg = config_from_ids({A_id, B_id});
    raft::log log(raft::snapshot_descriptor{.idx = index_t{0}, .config = cfg});
    auto A = create_follower(A_id, log);
    auto B = create_follower(B_id, log);
    election_timeout(A);
    communicate(A, B);
    BOOST_CHECK(A.is_leader());
    A.add_entry(log_entry::dummy{});
    A.add_entry(log_entry::dummy{});
    communicate(A, B);

    auto snp_idx = index_t{1};
    BOOST_CHECK(B.log_last_idx() > snp_idx);
    auto snp_term = B.get_log().term_for(snp_idx);
    BOOST_CHECK(snp_term);
    auto snp = raft::snapshot_descriptor{.idx = index_t{1}, .term = *snp_term, .config = cfg};
    BOOST_CHECK(!B.apply_snapshot(snp, 0, 0, false));
    // But it should apply this snapshot if it's locally generated
    BOOST_CHECK(B.apply_snapshot(snp, 0, 0, true));
}

// A server should sometimes become a candidate even though it is outside the current configuration,
// for example if it's the only server that can become a leader (due to log lengths).
BOOST_AUTO_TEST_CASE(test_candidate_outside_configuration) {
    server_id A_id = id(), B_id = id();
    raft::config_member_set addrset{config_member_from_id(A_id), config_member_from_id(B_id)};
    raft::configuration cfg(addrset);
    raft::log log(raft::snapshot_descriptor{.idx = index_t{0}, .config = cfg});
    discrete_failure_detector fd;
    auto A = create_follower(A_id, log, fd);
    auto B = create_follower(B_id, log, fd);
    election_timeout(A);
    communicate(A, B);
    BOOST_CHECK(A.is_leader());
    raft::configuration newcfg = config_from_ids({B_id});
    A.add_entry(newcfg);
    BOOST_CHECK(!B.get_log().get_configuration().is_joint());
    communicate_until([&A, &B] () { return !A.get_configuration().is_joint() && B.get_log().get_configuration().is_joint(); }, A, B);
    BOOST_CHECK(!A.get_configuration().is_joint());
    BOOST_CHECK(B.get_log().get_configuration().is_joint());
    fd.mark_dead(B_id);
    election_timeout(A);
    // A steps down because it cannot communicate with a quorum in the current configuration ({B}).
    BOOST_CHECK(!A.is_leader());
    fd.mark_alive(B_id);
    election_timeout(A);
    // A should become a candidate - it is the only server that can become a leader;
    // B's configuration is joint and it can't receive a vote from A due to shorter log.
    BOOST_CHECK(A.is_candidate());
    communicate_until([&A] () { return A.is_leader(); }, A, B);
    BOOST_CHECK(A.is_leader());
    communicate(A, B);
    BOOST_CHECK(B.is_leader());
}

BOOST_AUTO_TEST_CASE(test_read_barrier) {
    raft::server_id A_id = id(), B_id = id(), C_id = id(), D_id = id(), E_id = id();
    raft::log log(raft::snapshot_descriptor{.idx = raft::index_t{0},
        .config = config_from_ids({A_id, B_id, C_id, D_id})});
    auto A = create_follower(A_id, log);
    auto B = create_follower(B_id, log);
    auto C = create_follower(C_id, log);
    auto D = create_follower(D_id, log);
    auto E = create_follower(E_id, log);

    // A becomes leader
    election_timeout(A);
    communicate(A, B, C, D);
    BOOST_CHECK(A.is_leader());
    // propagate commit index
    A.tick();
    communicate(A, B, C, D);

    // Check that a node outside of config cannot start read barrier
    BOOST_CHECK_THROW(A.start_read_barrier(E_id), std::runtime_error);

    // start read barrier
    auto rid = A.start_read_barrier(A_id);
    BOOST_CHECK(rid);

    // Check that read_quorum was broadcasted to other nodes
    auto output = A.get_output();
    BOOST_CHECK_EQUAL(output.messages.size(), 3);
    BOOST_CHECK(std::holds_alternative<raft::read_quorum>(output.messages[0].second));
    BOOST_CHECK(std::holds_alternative<raft::read_quorum>(output.messages[1].second));
    BOOST_CHECK(std::holds_alternative<raft::read_quorum>(output.messages[2].second));

    // Check that it gets re-broadcasted on leader's tick
    A.tick();
    output = A.get_output();
    BOOST_CHECK_EQUAL(output.messages.size(), 3);
    BOOST_CHECK(std::holds_alternative<raft::read_quorum>(output.messages[0].second));
    BOOST_CHECK(std::holds_alternative<raft::read_quorum>(output.messages[1].second));
    BOOST_CHECK(std::holds_alternative<raft::read_quorum>(output.messages[2].second));

    auto read_quorum_msg = std::get<raft::read_quorum>(output.messages[0].second);
    // check that read id is correct
    BOOST_CHECK_EQUAL(read_quorum_msg.id, rid->first);

    // Check that a leader ignores read_barrier with its own term
    A.step(B_id, std::move(read_quorum_msg));
    output = A.get_output();
    BOOST_CHECK_EQUAL(output.messages.size(), 0);

    // Check that a follower replies to read_barrier with read_quorum_reply
    B.step(A_id, std::move(read_quorum_msg));
    output = B.get_output();
    BOOST_CHECK_EQUAL(output.messages.size(), 1);
    BOOST_CHECK(std::holds_alternative<raft::read_quorum_reply>(output.messages[0].second));

    auto read_quorum_reply_msg = std::get<raft::read_quorum_reply>(output.messages[0].second);

    // Ack barrier from B and check that this is not enough to complete a read
    A.step(B_id, std::move(read_quorum_reply_msg));
    output = A.get_output();
    BOOST_CHECK(!output.max_read_id_with_quorum);

    // Ack from B one more time and check that ack is not counted twice
    A.step(B_id, std::move(read_quorum_reply_msg));
    output = A.get_output();
    BOOST_CHECK(!output.max_read_id_with_quorum);

    // Ack from C and check that the read barrier is completed
    A.step(C_id, std::move(read_quorum_reply_msg));
    output = A.get_output();
    BOOST_CHECK(output.max_read_id_with_quorum);

    // Enter joint config
    raft::configuration newcfg = config_from_ids({A_id, E_id});
    A.add_entry(newcfg);
    // Process log storing event and drop append_entries messages
    output = A.get_output();

    // start read barrier
    rid = A.start_read_barrier(A_id);
    BOOST_CHECK(rid);

    // check that read_barrier is broadcasted to all nodes
    output = A.get_output();
    BOOST_CHECK_EQUAL(output.messages.size(), 4);
    BOOST_CHECK(std::holds_alternative<raft::read_quorum>(output.messages[0].second));
    BOOST_CHECK(std::holds_alternative<raft::read_quorum>(output.messages[1].second));
    BOOST_CHECK(std::holds_alternative<raft::read_quorum>(output.messages[2].second));
    BOOST_CHECK(std::holds_alternative<raft::read_quorum>(output.messages[3].second));

    // Ack in only old quorum and check that the read is not completed
    A.step(B_id, read_quorum_reply{A.get_current_term(), index_t{0}, rid->first});
    A.step(C_id, read_quorum_reply{A.get_current_term(), index_t{0}, rid->first});
    A.step(D_id, read_quorum_reply{A.get_current_term(), index_t{0}, rid->first});
    output = A.get_output();
    BOOST_CHECK(!output.max_read_id_with_quorum);

    // Ack in new config as well and see that it is committed now
    A.step(E_id, read_quorum_reply{A.get_current_term(), index_t{0}, rid->first});
    output = A.get_output();
    BOOST_CHECK(output.max_read_id_with_quorum);

    // check that read_barrier with lower term does not depose the leader
    A.step(E_id, read_quorum{A.get_current_term() - term_t{1}, index_t{10}, rid->first});
    BOOST_CHECK(A.is_leader());

    // check that read_barrier with higher term leads to leader
    // step down
    A.step(E_id, read_quorum{A.get_current_term() + term_t{1}, index_t{10}, rid->first});
    BOOST_CHECK(!A.is_leader());

    // create one node cluster
    raft::log log1(raft::snapshot_descriptor{.idx = raft::index_t{0}, .config = config_from_ids({A_id})});
    auto AA = create_follower(A_id, log1);
    // Make AA a leader
    election_timeout(AA);
    BOOST_CHECK(AA.is_leader());
    output = AA.get_output();

    // execute read barrier
    rid = AA.start_read_barrier(A_id);
    BOOST_CHECK(rid);

    // check that it completes immediately
    output = AA.get_output();
    BOOST_CHECK(output.max_read_id_with_quorum);
}

BOOST_AUTO_TEST_CASE(test_append_entry_inside_snapshot) {
    server_id A_id = id(), B_id = id(), C_id = id();

    raft::log log(raft::snapshot_descriptor{.idx = index_t{0}, .config = config_from_ids({A_id, B_id, C_id})});

    auto A = create_follower(A_id, log);
    auto B = create_follower(B_id, log);
    auto C = create_follower(C_id, log);
    election_timeout(A);
    communicate(A, B, C);
    A.add_entry(log_entry::dummy{});
    A.add_entry(log_entry::dummy{});
    A.add_entry(log_entry::dummy{});
    communicate(A, B, C);

    // Add new entry and commit it with B
    A.add_entry(log_entry::dummy{});
    auto output = A.get_output();
    BOOST_CHECK_EQUAL(output.messages.size(), 2);
    auto append = std::get<raft::append_request>(output.messages.back().second);
    B.step(A_id, std::move(append));
    output = B.get_output();
    BOOST_CHECK_EQUAL(output.messages.size(), 1);
    auto reply = std::get<raft::append_reply>(output.messages.back().second);
    A.step(B_id, std::move(reply)); // A commits last entry here

    // propagate commit index to B
    A.tick();
    communicate(A, B);

    // generate new message for C, first one will be empty
    // so feed it back to A and get next one
    A.tick();
    output = A.get_output();
    BOOST_CHECK_EQUAL(output.messages.size(), 1);
    append = std::get<raft::append_request>(output.messages.back().second);
    C.step(A_id, std::move(append));
    output = C.get_output();
    BOOST_CHECK_EQUAL(output.messages.size(), 1);
    reply = std::get<raft::append_reply>(output.messages.back().second);
    A.step(C_id, std::move(reply));
    output = A.get_output();
    BOOST_CHECK_EQUAL(output.messages.size(), 1);
    append = std::get<raft::append_request>(output.messages.back().second);

    // Now send it to C and ignore the reply
    C.step(A_id, std::move(append));
    (void)C.get_output();
    // C snapshots the log
    C.apply_snapshot(log_snapshot(C.get_log(), C.log_last_idx()), 0, 0, true);

    // Try to add one more entry
    A.add_entry(log_entry::dummy{});
    A.tick();
    communicate(A, B, C);
    BOOST_CHECK(!C.get_log().empty());
}

BOOST_AUTO_TEST_CASE(test_ping_leader) {
    discrete_failure_detector fd;
    server_id A_id = id(), B_id = id(), C_id = id();
    raft::configuration cfg(raft::config_member_set{
            raft::config_member{server_addr_from_id(A_id), is_voter::yes},
            raft::config_member{server_addr_from_id(B_id), is_voter::yes},
            raft::config_member{server_addr_from_id(C_id), is_voter::no}});

    raft::log log(raft::snapshot_descriptor{.idx = index_t{0}, .config = cfg});
    auto A = create_follower(A_id, log, fd);
    auto B = create_follower(B_id, log, fd);
    auto C = create_follower(C_id, log, fd);
    election_timeout(A);
    communicate(A, B, C);
    BOOST_CHECK(A.is_leader());
    // Check that non voter forgot a leader after election timeout.
    // It does not have to be this way, but currently our impl behaves this
    // way.
    fd.mark_all_dead();
    election_timeout(C);
    BOOST_CHECK(!C.current_leader());
    // Check that without any new input a node will not find out who leader is
    // after network repairs.
    fd.mark_all_alive();
    communicate(A, B, C);
    BOOST_CHECK(!C.current_leader());
    // Check that is we request leader ping then a node is able to find out
    // the leader after communicating with the cluster.
    C.ping_leader();
    C.tick();
    communicate(A, B, C);
    BOOST_CHECK(C.current_leader());
}

BOOST_AUTO_TEST_CASE(test_state_change_notifications) {
    discrete_failure_detector fd;

    server_id id1 = id(), id2 = id();

    raft::configuration cfg(raft::config_member_set{raft::config_member{server_addr_from_id(id1), is_voter::yes},
                                                    raft::config_member{server_addr_from_id(id2), is_voter::yes}});
    raft::log log{raft::snapshot_descriptor{.config = cfg}};

    auto fsm = create_follower(id1, std::move(log), fd);

    // Initial state is follower
    BOOST_CHECK(fsm.is_follower());

    // After election timeout, a follower becomes a candidate
    election_timeout(fsm);
    BOOST_CHECK(fsm.is_candidate());

    // Check that state transition was notified
    auto output = fsm.get_output();
    BOOST_CHECK(output.state_changed);

    // If nothing happens, the candidate stays this way
    election_timeout(fsm);
    BOOST_CHECK(fsm.is_candidate());

    // Check that no state transition is notified
    output = fsm.get_output();
    BOOST_CHECK(!output.state_changed);

    // After a favourable reply, we become a leader (quorum is 2)
    fsm.step(id2, raft::vote_reply{output.term_and_vote->first, true});
    // Check that state transition is notified again
    output = fsm.get_output();
    BOOST_CHECK(output.state_changed);
    BOOST_CHECK(fsm.is_leader());
}

// Test that ping_leader() sends ping messages immediately (not waiting for
// the next tick).
BOOST_AUTO_TEST_CASE(test_ping_leader_sends_immediately) {
    server_id A_id = id(), B_id = id();
    raft::configuration cfg = config_from_ids({A_id, B_id});
    raft::log log(raft::snapshot_descriptor{.config = cfg});

    auto A = create_follower(A_id, log);

    // A is a follower with no known leader. Calling ping_leader() should
    // immediately produce append_reply messages without waiting for a tick.
    A.ping_leader();
    auto output = A.get_output();
    BOOST_CHECK_GE(output.messages.size(), 1);
    // The message should be an append_reply (rejected) sent to B
    auto& msg = output.messages[0];
    BOOST_CHECK_EQUAL(msg.first, B_id);
    BOOST_CHECK(std::holds_alternative<raft::append_reply>(msg.second));
    auto& reply = std::get<raft::append_reply>(msg.second);
    BOOST_CHECK(std::holds_alternative<raft::append_reply::rejected>(reply.result));
}


// Test that with fast bootstrap enabled, a fresh multi-node group (empty log)
// makes the smallest-id voter immediately become a candidate at construction
// time and send a vote request without prevoting. A non-voter with an even
// smaller id is present to verify that non-voters are never selected to start
// the election.
BOOST_AUTO_TEST_CASE(test_start_as_candidate) {
    server_id N_id = id(), A_id = id(), B_id = id();
    // N_id is allocated first, so it has the smallest id overall, but it is a
    // non-voter and must be ignored. A_id is the smallest voter.
    BOOST_CHECK(N_id < A_id);
    BOOST_CHECK(A_id < B_id);
    raft::configuration cfg(raft::config_member_set{
            raft::config_member{server_addr_from_id(N_id), is_voter::no},
            raft::config_member{server_addr_from_id(A_id), is_voter::yes},
            raft::config_member{server_addr_from_id(B_id), is_voter::yes}});

    raft::fsm_config fcfg{.append_request_threshold = 1, .enable_prevoting = true,
                          .fast_bootstrap_seed = 0};

    {
        raft::log log(raft::snapshot_descriptor{.config = cfg});
        fsm_debug A(A_id, term_t{}, server_id{}, std::move(log), trivial_failure_detector, fcfg);

        // Smallest-id voter on a fresh group: immediately a candidate (not a
        // prevote candidate — prevoting is skipped on a fresh group). The
        // smaller-id non-voter N is ignored.
        BOOST_CHECK(A.is_candidate());
        BOOST_CHECK(!A.is_prevote_candidate());
        // Term should have been bumped to 1
        BOOST_CHECK_EQUAL(A.get_current_term(), term_t{1});

        // The output should contain a vote request (not prevote) to B only;
        // the non-voter N is not asked to vote.
        auto output = A.get_output();
        BOOST_CHECK_EQUAL(output.messages.size(), 1);
        BOOST_CHECK_EQUAL(output.messages[0].first, B_id);
        auto& vr = std::get<raft::vote_request>(output.messages[0].second);
        BOOST_CHECK(!vr.is_prevote);
        BOOST_CHECK_EQUAL(vr.current_term, term_t{1});
    }

    {
        // The larger-id voter must NOT start an election; it stays a
        // follower and waits for the election timeout.
        raft::log log(raft::snapshot_descriptor{.config = cfg});
        fsm_debug B(B_id, term_t{}, server_id{}, std::move(log), trivial_failure_detector, fcfg);
        BOOST_CHECK(B.is_follower());
    }

    {
        // The non-voter has the smallest id but is not eligible to start an
        // election; it stays a follower.
        raft::log log(raft::snapshot_descriptor{.config = cfg});
        fsm_debug N(N_id, term_t{}, server_id{}, std::move(log), trivial_failure_detector, fcfg);
        BOOST_CHECK(N.is_follower());
    }
}

// Test that fast bootstrap is gated by fast_bootstrap_seed: when it is unset
// (the default), even the smallest-id voter on a fresh group stays a
// follower. This is what keeps the bare fsm usable in tests.
BOOST_AUTO_TEST_CASE(test_no_start_as_candidate_without_fast_bootstrap) {
    server_id A_id = id(), B_id = id();
    BOOST_CHECK(A_id < B_id);
    raft::configuration cfg = config_from_ids({A_id, B_id});

    raft::log log(raft::snapshot_descriptor{.config = cfg});
    raft::fsm_config fcfg{.append_request_threshold = 1, .enable_prevoting = true};
    fsm_debug A(A_id, term_t{}, server_id{}, std::move(log), trivial_failure_detector, fcfg);

    BOOST_CHECK(A.is_follower());
}

// Test that a non-empty log (a restarted server, not a fresh group) does not
// trigger immediate candidacy even for the smallest-id voter with fast
// bootstrap enabled.
BOOST_AUTO_TEST_CASE(test_no_start_as_candidate_with_nonempty_log) {
    server_id A_id = id(), B_id = id();
    BOOST_CHECK(A_id < B_id);
    raft::configuration cfg = config_from_ids({A_id, B_id});

    // Snapshot at index 1 => non-empty log.
    raft::log log(raft::snapshot_descriptor{.idx = index_t{1}, .config = cfg});
    raft::fsm_config fcfg{.append_request_threshold = 1, .enable_prevoting = true,
                          .fast_bootstrap_seed = 0};
    fsm_debug A(A_id, term_t{}, server_id{}, std::move(log), trivial_failure_detector, fcfg);

    BOOST_CHECK(A.is_follower());
}
// Test that when a candidate's initial vote request is lost (peer not
// available initially), it resends the vote request when the peer pings us
// with a stale-term message.
BOOST_AUTO_TEST_CASE(test_start_as_candidate_resend_on_peer_ping) {
    server_id A_id = id(), B_id = id();
    BOOST_CHECK(A_id < B_id);
    raft::configuration cfg = config_from_ids({A_id, B_id});
    raft::log log(raft::snapshot_descriptor{.config = cfg});

    raft::fsm_config fcfg{.append_request_threshold = 1, .enable_prevoting = true,
                          .fast_bootstrap_seed = 0};
    fsm_debug A(A_id, term_t{}, server_id{}, std::move(log), trivial_failure_detector, fcfg);

    BOOST_CHECK(A.is_candidate());
    BOOST_CHECK_EQUAL(A.get_current_term(), term_t{1});

    // Consume the initial vote request (simulating it being lost)
    auto output = A.get_output();
    BOOST_CHECK_EQUAL(output.messages.size(), 1);

    // Simulate the peer starting later and pinging us with a stale-term
    // append_reply (this is what ping_leader() sends at term 0)
    A.step(B_id, raft::append_reply{term_t{0}, index_t{0},
            raft::append_reply::rejected{index_t{0}, index_t{0}}});

    // The candidate should resend its vote request to B
    output = A.get_output();
    BOOST_CHECK_GE(output.messages.size(), 1);
    bool found_vote_request = false;
    for (auto& [to, msg] : output.messages) {
        if (auto* vr = std::get_if<raft::vote_request>(&msg)) {
            BOOST_CHECK_EQUAL(to, B_id);
            BOOST_CHECK(!vr->is_prevote);
            BOOST_CHECK_EQUAL(vr->current_term, term_t{1});
            found_vote_request = true;
        }
    }
    BOOST_CHECK(found_vote_request);

    // Now B grants the vote — A should become leader
    A.step(B_id, raft::vote_reply{term_t{1}, true});
    BOOST_CHECK(A.is_leader());
}

// Test that a candidate does NOT resend its vote request to a peer that has
// already responded (voted) in the current term, even if that peer sends a
// stale-term message.
BOOST_AUTO_TEST_CASE(test_no_resend_to_responded_peer) {
    server_id A_id = id(), B_id = id(), C_id = id();
    raft::configuration cfg = config_from_ids({A_id, B_id, C_id});
    raft::log log(raft::snapshot_descriptor{.config = cfg});

    auto A = create_follower(A_id, log);
    BOOST_CHECK(A.is_follower());

    // Become a candidate via the normal election timeout.
    election_timeout(A);
    BOOST_CHECK(A.is_candidate());
    auto output = A.get_output();
    BOOST_CHECK(output.term_and_vote);
    auto term = output.term_and_vote->first;

    // B responds to our vote request (rejects it). A stays a candidate since
    // there is no granting quorum (self-vote + rejection), but B has now
    // responded for this term.
    A.step(B_id, raft::vote_reply{term, false});
    BOOST_CHECK(A.is_candidate());
    (void)A.get_output();

    // B (having responded) sends a stale-term ping — no resend to B.
    A.step(B_id, raft::append_reply{term_t{0}, index_t{0},
            raft::append_reply::rejected{index_t{0}, index_t{0}}});
    output = A.get_output();
    for (auto& [to, msg] : output.messages) {
        BOOST_CHECK(to != B_id || !std::holds_alternative<raft::vote_request>(msg));
    }

    // C (which hasn't responded) sends a stale-term ping — we resend to C.
    A.step(C_id, raft::append_reply{term_t{0}, index_t{0},
            raft::append_reply::rejected{index_t{0}, index_t{0}}});
    output = A.get_output();
    bool resent_to_c = false;
    for (auto& [to, msg] : output.messages) {
        if (to == C_id && std::holds_alternative<raft::vote_request>(msg)) {
            resent_to_c = true;
        }
    }
    BOOST_CHECK(resent_to_c);
}

// ---------------------------------------------------------------------------
// LeaseGuard leader-lease tests.
// ---------------------------------------------------------------------------

using namespace std::chrono_literals;

namespace {

// A fixed reference point well after the epoch, so we can subtract the
// uncertainty without underflowing.
const auto lease_t0 = raft::lease_clock::time_point(std::chrono::hours(24 * 365));
const auto lease_err = 1ms;
const auto lease_delta = 10s;

// Elapsed time after which a leader may discharge its deferred commit without a
// synchronized clock: delta plus the safety margin fsm.cc applies to it. Cast to
// the monotonic duration before dividing -- computing it as `lease_delta / 8`
// would be integer division on seconds and silently yield 1s instead of 1.25s.
const auto lease_mono_wait =
        std::chrono::duration_cast<raft::mono_clock::duration>(lease_delta) * 9 / 8;

raft::time_bounds make_bounds(raft::lease_clock::time_point center,
        raft::lease_clock::duration error = lease_err) {
    return raft::time_bounds{center - error, center + error};
}

raft::fsm_config make_lease_cfg(raft::bounded_clock& clock) {
    return raft::fsm_config{
        .append_request_threshold = 1,
        .enable_prevoting = false,
        .leaseguard = raft::fsm_config::leaseguard_config{.clock = clock, .delta = lease_delta},
    };
}

} // anonymous namespace

// The conservative interval comparisons underpinning LeaseGuard's safety, plus
// the mock clock used by the remaining tests.
BOOST_AUTO_TEST_CASE(test_leaseguard_time_bounds) {
    const raft::time_bounds e = make_bounds(lease_t0);

    // At creation the entry is provably younger than delta and not provably older.
    raft::time_bounds now = make_bounds(lease_t0);
    BOOST_CHECK(e.younger_than(lease_delta, now));
    BOOST_CHECK(!e.older_than(lease_delta, now));

    // Exactly delta later, uncertainty means we cannot yet prove it is older.
    now = make_bounds(lease_t0 + lease_delta);
    BOOST_CHECK(!e.older_than(lease_delta, now));

    // Well past delta plus twice the uncertainty: provably older, and no longer
    // provably younger. There is deliberately no instant where both hold.
    now = make_bounds(lease_t0 + lease_delta + 1s);
    BOOST_CHECK(e.older_than(lease_delta, now));
    BOOST_CHECK(!e.younger_than(lease_delta, now));

    // The mock clock reports what it is set to, and nullopt when unsynchronized.
    raft::bounded_clock_mock clock;
    BOOST_CHECK(!clock.interval_now());
    clock.set(lease_t0, lease_err);
    const auto iv = clock.interval_now();
    BOOST_REQUIRE(iv);
    BOOST_CHECK(iv->earliest == lease_t0 - lease_err);
    BOOST_CHECK(iv->latest == lease_t0 + lease_err);
    clock.set_unsynchronized();
    BOOST_CHECK(!clock.interval_now());
}

// A newly elected leader must not commit (and thus apply) its writes until the
// deposed leader's lease has expired.
BOOST_AUTO_TEST_CASE(test_leaseguard_deferred_commit) {
    raft::bounded_clock_mock clock;
    clock.set(lease_t0, lease_err);

    server_id id1 = id();
    raft::configuration cfg = config_from_ids({id1});
    raft::log log{raft::snapshot_descriptor{.config = cfg}};

    // A deposed leader's lease: an entry from a previous term stamped with a
    // recent time interval, already durable in this node's log.
    log.emplace_back(seastar::make_lw_shared<raft::log_entry>(
            raft::log_entry{term_t{1}, index_t{1}, raft::log_entry::dummy{}, make_bounds(lease_t0)}));
    log.stable_to(log.last_idx());

    // A single-node cluster elects itself on construction, in term 2.
    fsm_debug fsm(id1, term_t{1}, server_id{}, std::move(log), trivial_failure_detector, make_lease_cfg(clock));
    BOOST_REQUIRE(fsm.is_leader());

    // Draining the output stabilizes the leader's dummy entry and attempts to
    // commit, but the commit is deferred while the deposed leader's lease (entry
    // 1) is still less than delta old.
    (void)fsm.get_output();
    BOOST_CHECK(fsm.get_output().committed.empty());
    for (int i = 0; i < 5; i++) {
        fsm.tick();
        BOOST_CHECK(fsm.get_output().committed.empty());
    }

    // Once the lease is more than delta old, a tick commits the deferred,
    // already-durable entries without any further messages.
    clock.set(lease_t0 + lease_delta + 1s, lease_err);
    fsm.tick();
    const auto output = fsm.get_output();
    BOOST_CHECK(!output.committed.empty());
    BOOST_CHECK(output.messages.empty());
}

// When the clock is unsynchronized the age of the deposed leader's lease cannot
// be bounded from the recorded interval, so the commit stays deferred until
// either the clock recovers or enough elapsed time has been measured. This
// covers the near side of that boundary: just under the elapsed-time threshold
// nothing may commit. test_leaseguard_commits_after_delta_without_clock covers
// the far side; without both, a broken margin or a missing anchor passes.
BOOST_AUTO_TEST_CASE(test_leaseguard_deferred_commit_unsynchronized) {
    raft::bounded_clock_mock clock;
    clock.set_unsynchronized();

    server_id id1 = id();
    raft::configuration cfg = config_from_ids({id1});
    raft::log log{raft::snapshot_descriptor{.config = cfg}};
    log.emplace_back(seastar::make_lw_shared<raft::log_entry>(
            raft::log_entry{term_t{1}, index_t{1}, raft::log_entry::dummy{}, make_bounds(lease_t0)}));
    log.stable_to(log.last_idx());

    fsm_debug fsm(id1, term_t{1}, server_id{}, std::move(log), trivial_failure_detector, make_lease_cfg(clock));
    BOOST_REQUIRE(fsm.is_leader());

    (void)fsm.get_output();
    for (int i = 0; i < 5; i++) {
        fsm.tick();
        BOOST_CHECK(fsm.get_output().committed.empty());
    }

    // Elapsed time just short of the threshold (delta plus the safety margin)
    // is still not enough, however many ticks it is spread over.
    clock.advance_monotonic(lease_mono_wait - 1ms);
    for (int i = 0; i < 5; i++) {
        fsm.tick();
        BOOST_CHECK(fsm.get_output().committed.empty());
    }

    // Clock recovers and enough time has passed: commit proceeds.
    clock.set(lease_t0 + lease_delta + 1s, lease_err);
    fsm.tick();
    BOOST_CHECK(!fsm.get_output().committed.empty());
}

// A leader holding a valid lease serves reads locally, with no read_quorum
// round-trip; once the lease expires it falls back to the quorum read barrier.
BOOST_AUTO_TEST_CASE(test_leaseguard_local_lease_read) {
    raft::bounded_clock_mock clock;
    clock.set(lease_t0, lease_err);

    server_id id1 = id(), id2 = id();
    raft::configuration cfg = config_from_ids({id1, id2});
    fsm_debug fsm1(id1, term_t{}, server_id{}, raft::log{raft::snapshot_descriptor{.config = cfg}},
            trivial_failure_detector, make_lease_cfg(clock));
    fsm_debug fsm2(id2, term_t{}, server_id{}, raft::log{raft::snapshot_descriptor{.config = cfg}},
            trivial_failure_detector, make_lease_cfg(clock));

    // Elect fsm1 and commit its dummy entry (in its own term), establishing a
    // lease.
    election_timeout(fsm1);
    communicate(fsm1, fsm2);
    BOOST_REQUIRE(fsm1.is_leader());
    (void)fsm1.get_output();

    auto has_read_quorum = [](const raft::fsm_output& o) {
        return std::ranges::any_of(o.messages, [](const auto& m) {
            return std::holds_alternative<raft::read_quorum>(m.second);
        });
    };

    // Valid lease: the read is resolved locally (its id reaches quorum
    // immediately) and no read_quorum message is broadcast.
    auto rid = fsm1.start_read_barrier(id1);
    BOOST_REQUIRE(rid);
    auto output = fsm1.get_output();
    BOOST_REQUIRE(output.max_read_id_with_quorum);
    BOOST_CHECK_EQUAL(*output.max_read_id_with_quorum, rid->first);
    BOOST_CHECK(!has_read_quorum(output));

    // Expired lease: the read falls back to the quorum barrier, broadcasting a
    // read_quorum to the follower and not resolving locally.
    clock.set(lease_t0 + lease_delta + 1s, lease_err);
    auto rid2 = fsm1.start_read_barrier(id1);
    BOOST_REQUIRE(rid2);
    output = fsm1.get_output();
    BOOST_CHECK(!output.max_read_id_with_quorum);
    BOOST_CHECK(has_read_quorum(output));
}

// A leader whose clock never synchronizes still commits. It cannot bound the
// deposed lease from that lease's recorded interval, but it does not need to:
// every prior-term entry predates its election, so delta of *elapsed* time since
// the election proves the lease is gone. Elapsed time needs no synchronized
// clock, so the leader keeps making progress instead of stalling -- and it stays
// leader, since stepping down would only move the same delta wait onto whoever
// replaces it, after an election timeout and an election.
BOOST_AUTO_TEST_CASE(test_leaseguard_commits_after_delta_without_clock) {
    raft::bounded_clock_mock clock;
    clock.set_unsynchronized();

    server_id id1 = id(), id2 = id();
    raft::configuration cfg = config_from_ids({id1, id2});
    // Both nodes carry a prior-term entry, so the new leader has a deposed lease
    // to wait for and will defer committing.
    auto make_log = [&] {
        raft::log log{raft::snapshot_descriptor{.config = cfg}};
        log.emplace_back(seastar::make_lw_shared<raft::log_entry>(
                raft::log_entry{term_t{1}, index_t{1}, raft::log_entry::dummy{}, make_bounds(lease_t0)}));
        log.stable_to(log.last_idx());
        return log;
    };
    fsm_debug fsm1(id1, term_t{1}, server_id{}, make_log(), trivial_failure_detector, make_lease_cfg(clock));
    fsm_debug fsm2(id2, term_t{1}, server_id{}, make_log(), trivial_failure_detector, make_lease_cfg(clock));

    election_timeout(fsm1);
    communicate(fsm1, fsm2);
    BOOST_REQUIRE(fsm1.is_leader());
    (void)fsm1.get_output();

    // No elapsed time yet: the commit is deferred, but the node keeps leading.
    for (int i = 0; i < 3 * 10; i++) {
        fsm1.tick();
        BOOST_CHECK(fsm1.get_output().committed.empty());
        BOOST_REQUIRE(fsm1.is_leader());
    }

    // One millisecond short of the threshold: still nothing.
    clock.advance_monotonic(lease_mono_wait - 1ms);
    fsm1.tick();
    BOOST_CHECK(fsm1.get_output().committed.empty());
    BOOST_REQUIRE(fsm1.is_leader());

    // Past it: the deferred, already-replicated entries commit, with the clock
    // still reporting nothing.
    clock.advance_monotonic(2ms);
    fsm1.tick();
    BOOST_CHECK(!fsm1.get_output().committed.empty());
    BOOST_CHECK(fsm1.is_leader());
    BOOST_CHECK(!clock.interval_now());
}

namespace {

// Drive a single-node-quorum-of-two fsm to leadership with a caught-up follower,
// and tell it whether that follower's clock works. Returns the fsm ready for the
// clock-loss tests below.
struct clock_loss_fixture {
    server_id id1 = id(), id2 = id();
    raft::configuration cfg = config_from_ids({id1, id2});
    fsm_debug fsm;

    clock_loss_fixture(raft::bounded_clock_mock& clock, bool follower_clock_ok)
        : fsm(id1, term_t{}, server_id{}, raft::log{raft::snapshot_descriptor{.config = cfg}},
                trivial_failure_detector, make_lease_cfg(clock)) {
        election_timeout(fsm);
        (void)fsm.get_output();
        fsm.step(id2, raft::vote_reply{fsm.get_current_term(), true});
        BOOST_REQUIRE(fsm.is_leader());
        auto output = fsm.get_output();
        const auto append = std::get<raft::append_request>(output.messages.back().second);
        const auto idx = append.entries.back()->idx;
        // Follower is fully caught up, and reports its clock health.
        fsm.step(id2, raft::append_reply{fsm.get_current_term(), index_t{},
                raft::append_reply::accepted{idx}, follower_clock_ok});
        (void)fsm.get_output();
    }
};

bool sent_timeout_now(const raft::fsm_output& o) {
    return std::ranges::any_of(o.messages, [](const auto& m) {
        return std::holds_alternative<raft::timeout_now>(m.second);
    });
}

// The wait is 2*delta plus a per-group jitter of at most delta, and the jitter is
// drawn randomly, so tests assert either side of that range rather than an exact
// boundary. Both bounds hold for every possible jitter.
const auto lease_below_stepdown_wait =
        std::chrono::duration_cast<raft::mono_clock::duration>(lease_delta) * 2;
const auto lease_above_stepdown_wait =
        std::chrono::duration_cast<raft::mono_clock::duration>(lease_delta) * 3;

} // anonymous namespace

// A leader whose clock fails keeps making progress, but silently loses leases:
// no local reads, no stamped entries, no renewal, for as long as the outage
// lasts. If a caught-up voter's clock still works, hand leadership over so the
// group gets them back.
BOOST_AUTO_TEST_CASE(test_leaseguard_transfers_leadership_on_clock_loss) {
    raft::bounded_clock_mock clock;
    clock.set(lease_t0, lease_err);
    clock_loss_fixture f(clock, true /* follower's clock works */);

    // Our clock fails. The first tick observes it and starts the clock.
    clock.set_unsynchronized();
    f.fsm.tick();
    BOOST_CHECK(!sent_timeout_now(f.fsm.get_output()));

    // Short of the wait, leadership must not move however many ticks pass: a
    // clock that recovers quickly is not worth a transfer, which costs the
    // successor a delta of deferred commits.
    clock.advance_monotonic(lease_below_stepdown_wait - 1ms);
    for (int i = 0; i < 5; i++) {
        f.fsm.tick();
        BOOST_CHECK(!sent_timeout_now(f.fsm.get_output()));
    }
    BOOST_REQUIRE(f.fsm.is_leader());

    // Past it, leadership is handed to the follower.
    clock.advance_monotonic(lease_above_stepdown_wait);
    f.fsm.tick();
    BOOST_CHECK(sent_timeout_now(f.fsm.get_output()));
}

// ... but only if the target can actually do what we cannot. A follower whose
// clock is equally broken is no improvement, so we keep leading.
BOOST_AUTO_TEST_CASE(test_leaseguard_no_transfer_when_follower_clock_also_broken) {
    raft::bounded_clock_mock clock;
    clock.set(lease_t0, lease_err);
    clock_loss_fixture f(clock, false /* follower's clock is broken too */);

    // Arm the wait first, then let the time pass: the wait is measured from the
    // tick that observes the failure, so advancing before that first tick would
    // leave zero elapsed time and the test would pass without proving anything.
    clock.set_unsynchronized();
    f.fsm.tick();
    BOOST_CHECK(!sent_timeout_now(f.fsm.get_output()));

    clock.advance_monotonic(lease_above_stepdown_wait * 2);
    for (int i = 0; i < 5; i++) {
        f.fsm.tick();
        BOOST_CHECK(!sent_timeout_now(f.fsm.get_output()));
    }
    BOOST_CHECK(f.fsm.is_leader());
}

// A clock that flaps must not ping-pong leadership: every recovery cancels the
// wait, so a node that is unsynchronized half the time never accumulates enough
// consecutive failure to trigger a transfer.
BOOST_AUTO_TEST_CASE(test_leaseguard_clock_recovery_cancels_transfer) {
    raft::bounded_clock_mock clock;
    clock.set(lease_t0, lease_err);
    clock_loss_fixture f(clock, true);

    for (int i = 0; i < 5; i++) {
        clock.set_unsynchronized();
        f.fsm.tick();
        BOOST_CHECK(!sent_timeout_now(f.fsm.get_output()));
        // Nearly long enough, but the clock comes back before the wait elapses.
        clock.advance_monotonic(lease_below_stepdown_wait - 1ms);
        clock.set(lease_t0, lease_err);
        f.fsm.tick();
        BOOST_CHECK(!sent_timeout_now(f.fsm.get_output()));
        clock.advance_monotonic(lease_above_stepdown_wait);
    }
    BOOST_CHECK(f.fsm.is_leader());
}

// LeaseGuard automatic lease extension (arXiv:2512.15659, Section 5.1). Under a
// read-only workload the lease would otherwise expire (no writes to refresh it)
// and every read would fall back to a quorum barrier. While reads are flowing,
// tick_leader() proactively commits a no-op before the newest entry reaches
// delta, so every read keeps being served locally with no read_quorum.
BOOST_AUTO_TEST_CASE(test_leaseguard_lease_renewal) {
    raft::bounded_clock_mock clock;
    clock.set(lease_t0, lease_err);

    server_id id1 = id(), id2 = id();
    raft::configuration cfg = config_from_ids({id1, id2});
    fsm_debug fsm1(id1, term_t{}, server_id{}, raft::log{raft::snapshot_descriptor{.config = cfg}},
            trivial_failure_detector, make_lease_cfg(clock));
    fsm_debug fsm2(id2, term_t{}, server_id{}, raft::log{raft::snapshot_descriptor{.config = cfg}},
            trivial_failure_detector, make_lease_cfg(clock));

    // Elect fsm1 and commit its dummy entry, establishing a lease (genesis log,
    // so there is no deposed lease to defer for).
    election_timeout(fsm1);
    communicate(fsm1, fsm2);
    BOOST_REQUIRE(fsm1.is_leader());
    (void)fsm1.get_output();

    auto has_read_quorum = [](const raft::fsm_output& o) {
        return std::ranges::any_of(o.messages, [](const auto& m) {
            return std::holds_alternative<raft::read_quorum>(m.second);
        });
    };

    // Simulate a read-only workload for well over delta. Each step advances the
    // clock by more than delta/2 (the renewal threshold) and performs one read.
    auto t = lease_t0;
    for (int step = 0; step < 6; step++) {
        t += lease_delta / 2 + 1s;
        clock.set(t, lease_err);

        // The read is served locally from the (renewed) committed entry: its id
        // reaches quorum immediately and no read_quorum message is broadcast.
        auto rid = fsm1.start_read_barrier(id1);
        BOOST_REQUIRE(rid);
        auto output = fsm1.get_output();
        BOOST_REQUIRE(output.max_read_id_with_quorum);
        BOOST_CHECK_EQUAL(*output.max_read_id_with_quorum, rid->first);
        BOOST_CHECK(!has_read_quorum(output));

        // The following tick renews the lease with a no-op; replicate and commit
        // it so it becomes the new, fresh basis for the next step's lease read.
        const auto idx_before = fsm1.log_last_idx();
        fsm1.tick();
        BOOST_CHECK_EQUAL(fsm1.log_last_idx(), idx_before + index_t{1});
        communicate(fsm1, fsm2);
        (void)fsm1.get_output();
    }
}

// Without reads there is nothing to keep warm, so an idle leader must not append
// renewal no-ops; and once the lease has expired a read falls back to the quorum
// barrier as usual.
BOOST_AUTO_TEST_CASE(test_leaseguard_no_renewal_without_reads) {
    raft::bounded_clock_mock clock;
    clock.set(lease_t0, lease_err);

    server_id id1 = id(), id2 = id();
    raft::configuration cfg = config_from_ids({id1, id2});
    fsm_debug fsm1(id1, term_t{}, server_id{}, raft::log{raft::snapshot_descriptor{.config = cfg}},
            trivial_failure_detector, make_lease_cfg(clock));
    fsm_debug fsm2(id2, term_t{}, server_id{}, raft::log{raft::snapshot_descriptor{.config = cfg}},
            trivial_failure_detector, make_lease_cfg(clock));

    election_timeout(fsm1);
    communicate(fsm1, fsm2);
    BOOST_REQUIRE(fsm1.is_leader());
    (void)fsm1.get_output();

    const auto idx_before = fsm1.log_last_idx();

    // Advance far past delta and tick repeatedly, but issue no reads.
    clock.set(lease_t0 + 10 * lease_delta, lease_err);
    for (int i = 0; i < 20; i++) {
        fsm1.tick();
        communicate(fsm1, fsm2);
    }
    // No read activity => no renewal no-ops were appended.
    BOOST_CHECK_EQUAL(fsm1.log_last_idx(), idx_before);

    // The lease is now stale, so a read falls back to the quorum read barrier.
    auto has_read_quorum = [](const raft::fsm_output& o) {
        return std::ranges::any_of(o.messages, [](const auto& m) {
            return std::holds_alternative<raft::read_quorum>(m.second);
        });
    };
    auto rid = fsm1.start_read_barrier(id1);
    BOOST_REQUIRE(rid);
    auto output = fsm1.get_output();
    BOOST_CHECK(!output.max_read_id_with_quorum);
    BOOST_CHECK(has_read_quorum(output));
}

// Renewal must RE-ESTABLISH a lapsed lease, not only extend a live one
// (arXiv:2512.15659, Section 5.1: a no-op is written "whenever needed to serve a
// read"). A leader can end up leaseless with no write traffic to fix it -- right
// after a failover, after a snapshot, or after a clock outage spanning delta --
// and if renewal required an already-valid lease, a read-only workload would
// then fall back to a quorum barrier for every read, permanently.
BOOST_AUTO_TEST_CASE(test_leaseguard_renewal_reestablishes_expired_lease) {
    raft::bounded_clock_mock clock;
    clock.set(lease_t0, lease_err);

    server_id id1 = id(), id2 = id();
    raft::configuration cfg = config_from_ids({id1, id2});
    fsm_debug fsm1(id1, term_t{}, server_id{}, raft::log{raft::snapshot_descriptor{.config = cfg}},
            trivial_failure_detector, make_lease_cfg(clock));
    fsm_debug fsm2(id2, term_t{}, server_id{}, raft::log{raft::snapshot_descriptor{.config = cfg}},
            trivial_failure_detector, make_lease_cfg(clock));

    election_timeout(fsm1);
    communicate(fsm1, fsm2);
    BOOST_REQUIRE(fsm1.is_leader());
    (void)fsm1.get_output();

    auto has_read_quorum = [](const raft::fsm_output& o) {
        return std::ranges::any_of(o.messages, [](const auto& m) {
            return std::holds_alternative<raft::read_quorum>(m.second);
        });
    };

    // Let the lease lapse completely: far past delta, so the committed entry is
    // not merely stale but provably older than a full lease duration.
    clock.set(lease_t0 + 10 * lease_delta, lease_err);

    // The first read after the lapse cannot be served locally.
    auto rid = fsm1.start_read_barrier(id1);
    BOOST_REQUIRE(rid);
    auto output = fsm1.get_output();
    BOOST_CHECK(!output.max_read_id_with_quorum);
    BOOST_CHECK(has_read_quorum(output));

    // But it marks the term as read-active, so the next tick appends a renewal
    // no-op even though there is no lease left to extend.
    const auto idx_before = fsm1.log_last_idx();
    fsm1.tick();
    BOOST_REQUIRE_EQUAL(fsm1.log_last_idx(), idx_before + index_t{1});
    communicate(fsm1, fsm2);
    (void)fsm1.get_output();

    // The no-op is committed and freshly stamped, so the lease is back and reads
    // are served locally again with no quorum round-trip.
    auto rid2 = fsm1.start_read_barrier(id1);
    BOOST_REQUIRE(rid2);
    output = fsm1.get_output();
    BOOST_REQUIRE(output.max_read_id_with_quorum);
    BOOST_CHECK_EQUAL(*output.max_read_id_with_quorum, rid2->first);
    BOOST_CHECK(!has_read_quorum(output));
}

// A leader that is stepping down must not append renewal no-ops (3.10). It is
// still the leader, so tick_leader() keeps running and every other renewal
// condition can hold -- but add_entry() refuses to append during stepdown by
// throwing not_a_leader, and tick() has no caller that would catch it.
BOOST_AUTO_TEST_CASE(test_leaseguard_no_renewal_during_stepdown) {
    raft::bounded_clock_mock clock;
    clock.set(lease_t0, lease_err);

    server_id id1 = id(), id2 = id();
    raft::configuration cfg = config_from_ids({id1, id2});
    fsm_debug fsm1(id1, term_t{}, server_id{}, raft::log{raft::snapshot_descriptor{.config = cfg}},
            trivial_failure_detector, make_lease_cfg(clock));
    fsm_debug fsm2(id2, term_t{}, server_id{}, raft::log{raft::snapshot_descriptor{.config = cfg}},
            trivial_failure_detector, make_lease_cfg(clock));

    election_timeout(fsm1);
    communicate(fsm1, fsm2);
    BOOST_REQUIRE(fsm1.is_leader());
    (void)fsm1.get_output();

    // Set up the state renewal wants: the log is fully committed, a read has
    // happened this term, and the lease is past its renewal threshold.
    BOOST_REQUIRE(fsm1.start_read_barrier(id1));
    (void)fsm1.get_output();
    clock.set(lease_t0 + 10 * lease_delta, lease_err);

    // Now start a leadership transfer. Use a timeout long enough that the tick
    // below does not cancel the stepdown before reaching the renewal code.
    const auto idx_before = fsm1.log_last_idx();
    fsm1.transfer_leadership(raft::logical_clock::duration(5));
    BOOST_REQUIRE(fsm1.is_leader());
    (void)fsm1.get_output();

    // The tick must neither throw nor append.
    BOOST_REQUIRE_NO_THROW(fsm1.tick());
    BOOST_CHECK_EQUAL(fsm1.log_last_idx(), idx_before);
}

// Tests for the follower-side in-memory log limit (max_follower_log_size).
//
// A leader bounds its log with log_limiter_semaphore, but a follower has no such
// admission control: it used to append whatever a leader sent it, so a follower
// whose state machine applied slowly (and therefore never snapshotted) grew its
// log without bound. A follower now appends only the prefix of an append_request
// which fits into max_follower_log_size and reports back the index it actually
// reached, plus an advisory append_reply::log_full flag which makes the leader
// stop replicating to it entirely.

namespace {

// With the default max_command_size (sizeof(log_entry)), log::memory_usage_of
// charges a command exactly its serialized size, which keeps the arithmetic in
// these tests exact.
size_t command_memory_usage(const raft::command& cmd) {
    return raft::log::memory_usage_of(cmd, sizeof(raft::log_entry));
}

struct follower_log_limit_fixture {
    server_id follower_id = id();
    server_id leader_id = id();
    raft::configuration cfg = config_from_ids({follower_id, leader_id});
    raft::command cmd = create_command(1);
    size_t entry_size = command_memory_usage(cmd);

    raft::fsm_config config(size_t entries_budget) {
        auto fcfg = fsm_cfg;
        fcfg.max_follower_log_size = entries_budget * entry_size;
        return fcfg;
    }

    // A log with `entries` command entries of term 1 at indexes 1..entries.
    // The snapshot term has to be 1 as well: add_entry() derives an entry's term
    // from log::last_term(), which for an empty log is the snapshot term.
    raft::log make_log(size_t entries) {
        raft::log log{raft::snapshot_descriptor{
                .idx = index_t{0}, .term = term_t{1}, .config = cfg}};
        for (size_t i = 0; i < entries; i++) {
            add_entry(log, cmd);
        }
        return log;
    }

    raft::append_request request(index_t prev_idx, term_t prev_term, size_t entries,
            index_t leader_commit_idx, term_t term = term_t{1}) {
        raft::append_request req{
            .current_term = term,
            .prev_log_idx = prev_idx,
            .prev_log_term = prev_term,
            .leader_commit_idx = leader_commit_idx,
            .entries = {},
        };
        for (size_t i = 0; i < entries; i++) {
            req.entries.push_back(make_lw_shared<const raft::log_entry>(
                    raft::log_entry{term, index_t{prev_idx.value() + 1 + i}, cmd}));
        }
        return req;
    }

    // The single append_reply the follower produced in response to the last step().
    raft::append_reply reply_of(raft::fsm_output& output) {
        BOOST_REQUIRE_EQUAL(output.messages.size(), 1);
        return std::get<raft::append_reply>(output.messages.back().second);
    }

    index_t accepted_idx(const raft::append_reply& reply) {
        BOOST_REQUIRE(std::holds_alternative<raft::append_reply::accepted>(reply.result));
        return std::get<raft::append_reply::accepted>(reply.result).last_new_idx;
    }
};

} // anonymous namespace

// A follower appends only what fits, reports the index it actually reached, and
// sets log_full. Once a snapshot shrinks the log it accepts entries again.
BOOST_AUTO_TEST_CASE(test_follower_log_size_limit) {
    follower_log_limit_fixture f;
    // Room for exactly 3 command entries.
    fsm_debug fsm(f.follower_id, term_t{1}, server_id{}, f.make_log(0),
            trivial_failure_detector, f.config(3));
    BOOST_CHECK(fsm.is_follower());

    // Send 5 entries and commit all of them: only 3 fit.
    fsm.step(f.leader_id, f.request(index_t{0}, term_t{0}, 5, index_t{5}));
    auto output = fsm.get_output();
    BOOST_CHECK_EQUAL(fsm.get_log().last_idx(), index_t{3});
    BOOST_CHECK_LE(fsm.get_log().memory_usage(), 3 * f.entry_size);
    auto reply = f.reply_of(output);
    // The reply reports what we have, not what was sent.
    BOOST_CHECK_EQUAL(f.accepted_idx(reply), index_t{3});
    BOOST_CHECK(reply.log_full);
    BOOST_CHECK(fsm.log_is_full());
    // The commit index still advanced, capped at what we actually have. This is
    // what lets a throttled follower drain its log and eventually free memory.
    BOOST_CHECK_EQUAL(fsm.commit_idx(), index_t{3});

    // We are now full and we do have something to drain, so further entries are
    // refused outright and the log does not grow at all.
    fsm.step(f.leader_id, f.request(index_t{3}, term_t{1}, 2, index_t{5}));
    output = fsm.get_output();
    BOOST_CHECK_EQUAL(fsm.get_log().last_idx(), index_t{3});
    BOOST_CHECK_LE(fsm.get_log().memory_usage(), 3 * f.entry_size);
    reply = f.reply_of(output);
    BOOST_CHECK_EQUAL(f.accepted_idx(reply), index_t{3});
    BOOST_CHECK(reply.log_full);

    // An empty request (the leader's throttled heartbeat) is always answered and
    // never grows the log.
    fsm.step(f.leader_id, f.request(index_t{3}, term_t{1}, 0, index_t{5}));
    output = fsm.get_output();
    BOOST_CHECK_EQUAL(fsm.get_log().last_idx(), index_t{3});
    reply = f.reply_of(output);
    BOOST_CHECK_EQUAL(f.accepted_idx(reply), index_t{3});
    BOOST_CHECK(reply.log_full);

    // Snapshotting frees the log, and replication resumes.
    BOOST_CHECK(fsm.apply_snapshot(
            raft::snapshot_descriptor{.idx = index_t{3}, .term = term_t{1}, .config = f.cfg},
            0, 0, true));
    BOOST_CHECK_EQUAL(fsm.get_log().memory_usage(), 0);
    BOOST_CHECK(!fsm.log_is_full());
    output = fsm.get_output();

    fsm.step(f.leader_id, f.request(index_t{3}, term_t{1}, 2, index_t{5}));
    output = fsm.get_output();
    BOOST_CHECK_EQUAL(fsm.get_log().last_idx(), index_t{5});
    reply = f.reply_of(output);
    BOOST_CHECK_EQUAL(f.accepted_idx(reply), index_t{5});
    BOOST_CHECK(!reply.log_full);
}

// The limit must never be applied to entries which are already in the log: a
// duplicate costs nothing, and a conflicting entry *frees* memory by truncating
// the log. Refusing those would make it impossible for a new leader to overwrite
// a full follower's uncommitted tail, which deadlocks the group.
BOOST_AUTO_TEST_CASE(test_follower_log_limit_allows_overlapping_entries) {
    follower_log_limit_fixture f;
    // Log is exactly at the limit: 3 entries of term 1, budget for 3.
    fsm_debug fsm(f.follower_id, term_t{1}, server_id{}, f.make_log(3),
            trivial_failure_detector, f.config(3));
    // Commit one entry, so that the log can shrink and the escape hatch in
    // fsm::append_entries stays closed - we want to exercise the overlap path.
    fsm.step(f.leader_id, f.request(index_t{3}, term_t{1}, 0, index_t{1}));
    (void)fsm.get_output();
    BOOST_CHECK_EQUAL(fsm.commit_idx(), index_t{1});
    BOOST_CHECK(fsm.log_is_full());

    // Re-sending entries the follower already has is accepted in full even
    // though it is at the limit, and does not grow the log.
    fsm.step(f.leader_id, f.request(index_t{0}, term_t{0}, 3, index_t{1}));
    auto output = fsm.get_output();
    BOOST_CHECK_EQUAL(fsm.get_log().last_idx(), index_t{3});
    BOOST_CHECK_EQUAL(fsm.get_log().memory_usage(), 3 * f.entry_size);
    BOOST_CHECK_EQUAL(f.accepted_idx(f.reply_of(output)), index_t{3});

    // A new leader in term 2 overwrites the uncommitted tail at idx 2..3. The
    // conflict at idx 2 truncates two entries, which makes room for both new
    // ones, so the whole request is accepted.
    fsm.step(f.leader_id, f.request(index_t{1}, term_t{1}, 2, index_t{1}, term_t{2}));
    output = fsm.get_output();
    BOOST_CHECK_EQUAL(fsm.get_log().last_idx(), index_t{3});
    BOOST_CHECK_EQUAL(fsm.get_log().memory_usage(), 3 * f.entry_size);
    BOOST_CHECK_EQUAL(fsm.get_log()[3]->term, term_t{2});
    BOOST_CHECK_EQUAL(f.accepted_idx(f.reply_of(output)), index_t{3});
}

// A follower which has nothing committed beyond its snapshot cannot shrink its
// log by snapshotting, so refusing entries would deadlock: the leader needs it
// to accept in order to commit anything at all, including the dummy entry a new
// leader appends on election. Note that the dummy being zero-sized does not help
// - it sits above the entries being refused, and skipping those would leave a
// gap in the log - so the follower has to let entries through one at a time.
BOOST_AUTO_TEST_CASE(test_follower_log_limit_forces_progress_when_stuck) {
    follower_log_limit_fixture f;
    // Log is already at the limit and nothing is committed.
    fsm_debug fsm(f.follower_id, term_t{1}, server_id{}, f.make_log(3),
            trivial_failure_detector, f.config(3));
    BOOST_CHECK_EQUAL(fsm.commit_idx(), index_t{0});
    BOOST_CHECK_EQUAL(fsm.get_log().get_snapshot().idx, index_t{0});
    BOOST_CHECK_EQUAL(fsm.get_log().memory_usage(), 3 * f.entry_size);
    // We are at the byte limit, but since we cannot shrink we are going to let
    // entries through rather than refuse them - so we must not report ourselves
    // full. Telling a leader to stop sending here would stop it from ever
    // committing anything, and it is the leader's commit index that we need in
    // order to become able to shrink.
    BOOST_CHECK(!fsm.log_is_full());

    // Two entries are offered; exactly one is let through, over the limit. This
    // is what walks a new leader up to its dummy entry one round trip at a time.
    fsm.step(f.leader_id, f.request(index_t{3}, term_t{1}, 2, index_t{0}));
    auto output = fsm.get_output();
    BOOST_CHECK_EQUAL(fsm.get_log().last_idx(), index_t{4});
    BOOST_CHECK_GT(fsm.get_log().memory_usage(), 3 * f.entry_size);
    BOOST_CHECK_EQUAL(f.accepted_idx(f.reply_of(output)), index_t{4});

    // Still nothing committed, so the next request lets one more entry through.
    // The growth is bounded by the leader's own log, which is bounded by its
    // log_limiter_semaphore.
    fsm.step(f.leader_id, f.request(index_t{4}, term_t{1}, 2, index_t{0}));
    output = fsm.get_output();
    BOOST_CHECK_EQUAL(fsm.commit_idx(), index_t{0});
    BOOST_CHECK_EQUAL(fsm.get_log().last_idx(), index_t{5});
    BOOST_CHECK_EQUAL(f.accepted_idx(f.reply_of(output)), index_t{5});
}

// The counterpart of the above: as soon as the follower has something committed
// beyond its snapshot it can shrink on its own, so the escape hatch closes and
// the limit is enforced strictly. If it did not, the limit would be worthless -
// the common case for a full follower is precisely a large backlog of committed
// but not yet applied entries.
BOOST_AUTO_TEST_CASE(test_follower_log_limit_enforced_when_able_to_shrink) {
    follower_log_limit_fixture f;
    fsm_debug fsm(f.follower_id, term_t{1}, server_id{}, f.make_log(3),
            trivial_failure_detector, f.config(3));
    // At the byte limit, but not reporting full yet - nothing is committed
    // beyond the snapshot, so we cannot shrink and will not refuse.
    BOOST_CHECK(!fsm.log_is_full());

    // An empty request commits what we already have without growing the log.
    fsm.step(f.leader_id, f.request(index_t{3}, term_t{1}, 0, index_t{3}));
    (void)fsm.get_output();
    // Now we can drain and snapshot, so we will refuse - and say so.
    BOOST_CHECK(fsm.log_is_full());
    BOOST_CHECK_EQUAL(fsm.commit_idx(), index_t{3});
    BOOST_CHECK_EQUAL(fsm.get_log().last_idx(), index_t{3});

    // Now we can shrink, so entries are refused and the log stays at the limit.
    fsm.step(f.leader_id, f.request(index_t{3}, term_t{1}, 2, index_t{3}));
    auto output = fsm.get_output();
    BOOST_CHECK_EQUAL(fsm.get_log().last_idx(), index_t{3});
    BOOST_CHECK_EQUAL(fsm.get_log().memory_usage(), 3 * f.entry_size);
    auto reply = f.reply_of(output);
    BOOST_CHECK_EQUAL(f.accepted_idx(reply), index_t{3});
    BOOST_CHECK(reply.log_full);
}

// The leader stops replicating to a follower which reported a full log, and
// probes it with an entry-less append_request once per tick instead.
BOOST_AUTO_TEST_CASE(test_leader_throttles_full_follower) {
    server_id A_id = id(), B_id = id();

    raft::log log{raft::snapshot_descriptor{.idx = index_t{0},
            .config = config_from_ids({A_id, B_id})}};
    fsm_debug A(A_id, term_t{1}, server_id{}, std::move(log), trivial_failure_detector, fsm_cfg);

    election_timeout(A);
    auto output = A.get_output();
    BOOST_REQUIRE(output.term_and_vote);
    A.step(B_id, raft::vote_reply{output.term_and_vote->first, true});
    BOOST_REQUIRE(A.is_leader());

    // Ack the dummy entry so that the leader leaves PROBE mode.
    output = A.get_output();
    auto req = std::get<raft::append_request>(output.messages.back().second);
    const index_t dummy_idx = req.entries.back()->idx;
    A.step(B_id, raft::append_reply{req.current_term, dummy_idx,
            raft::append_reply::accepted{dummy_idx}});
    (void)A.get_output();

    // B reports a full log while acking an entry.
    A.add_entry(create_command(1));
    output = A.get_output();
    req = std::get<raft::append_request>(output.messages.back().second);
    const index_t acked_idx = req.entries.back()->idx;
    A.step(B_id, raft::append_reply{req.current_term, acked_idx,
            raft::append_reply::accepted{acked_idx}, /* clock_ok = */ false,
            /* log_full = */ true});
    BOOST_CHECK(A.get_progress(B_id).log_full);
    BOOST_CHECK_EQUAL(A.throttled_followers(), 1);

    // New entries are no longer replicated to B.
    A.add_entry(create_command(2));
    A.add_entry(create_command(3));
    output = A.get_output();
    for (auto& [to, msg] : output.messages) {
        BOOST_CHECK(!std::holds_alternative<raft::append_request>(msg));
    }

    // A tick produces exactly one entry-less request, which carries the commit
    // index so that B can drain what it already has.
    A.tick();
    output = A.get_output();
    BOOST_REQUIRE_EQUAL(output.messages.size(), 1);
    req = std::get<raft::append_request>(output.messages.back().second);
    BOOST_CHECK(req.entries.empty());
    BOOST_CHECK_EQUAL(req.leader_commit_idx, A.commit_idx());

    // Answering with log_full cleared resumes replication with real entries.
    A.step(B_id, raft::append_reply{req.current_term, acked_idx,
            raft::append_reply::accepted{req.prev_log_idx}, /* clock_ok = */ false,
            /* log_full = */ false});
    BOOST_CHECK(!A.get_progress(B_id).log_full);
    BOOST_CHECK_EQUAL(A.throttled_followers(), 0);
    output = A.get_output();
    bool sent_entries = false;
    for (auto& [to, msg] : output.messages) {
        if (auto* r = std::get_if<raft::append_request>(&msg); r && !r->entries.empty()) {
            sent_entries = true;
        }
    }
    BOOST_CHECK(sent_entries);
}

// The decision whether we may refuse entries has to take into account what the
// request being processed says is committed, not only what the previous one did.
// A follower whose log is full of entries it believes to be uncommitted cannot
// shrink, so it lets one entry through to guarantee progress; but if this very
// request tells it those entries are committed, it can shrink after all and the
// entry must be refused.
BOOST_AUTO_TEST_CASE(test_follower_log_limit_uses_commit_idx_of_current_request) {
    follower_log_limit_fixture f;
    // At the limit, with nothing known to be committed beyond the snapshot.
    fsm_debug fsm(f.follower_id, term_t{1}, server_id{}, f.make_log(3),
            trivial_failure_detector, f.config(3));
    BOOST_CHECK_EQUAL(fsm.commit_idx(), index_t{0});
    BOOST_CHECK_EQUAL(fsm.get_log().get_snapshot().idx, index_t{0});

    // This request both carries new entries and tells us that everything we
    // already have is committed. The commit index it brings is what decides:
    // we can drain and snapshot, so nothing is appended.
    fsm.step(f.leader_id, f.request(index_t{3}, term_t{1}, 2, index_t{3}));
    auto output = fsm.get_output();
    BOOST_CHECK_EQUAL(fsm.commit_idx(), index_t{3});
    BOOST_CHECK_EQUAL(fsm.get_log().last_idx(), index_t{3});
    BOOST_CHECK_EQUAL(fsm.get_log().memory_usage(), 3 * f.entry_size);
    BOOST_CHECK_EQUAL(f.accepted_idx(f.reply_of(output)), index_t{3});
}

// A leader must not stop replicating to a follower which is at its log limit but
// cannot shrink: such a follower lets one entry through per request rather than
// refusing, and it is the leader's entries - its dummy, and anything that
// overwrites a diverged tail - that it needs in order to commit, apply and
// finally snapshot. Throttling it there deadlocks the group: the leader cannot
// commit without the follower, and the follower cannot free memory without the
// leader. That is why log_is_full() requires can_shrink_log().
BOOST_AUTO_TEST_CASE(test_leader_keeps_replicating_to_follower_that_cannot_shrink) {
    follower_log_limit_fixture f;
    const auto A_id = f.leader_id, B_id = f.follower_id;
    const auto cfg = f.config(6);

    // B is at its limit with nothing committed beyond its snapshot, so it cannot
    // take a snapshot and cannot free anything on its own.
    fsm_debug B(B_id, term_t{1}, server_id{}, f.make_log(6), trivial_failure_detector, cfg);
    BOOST_REQUIRE_EQUAL(B.commit_idx(), index_t{0});
    BOOST_REQUIRE_EQUAL(B.get_log().memory_usage(), 6 * f.entry_size);
    // At the limit, but not "full": it will let entries through, not refuse them.
    BOOST_CHECK(!B.log_is_full());

    // A has a longer log; its first 6 entries are the same as B's.
    //
    // Note the 10 entries deliberately exceed what a real leader could hold: a
    // leader is capped at max_log_size by log_limiter_semaphore, which is below
    // max_follower_log_size, whereas here fsm_cfg leaves max_log_size at 0 and
    // fsm::add_entry() bypasses wait_for_memory_permit() anyway. So B's growth
    // below is not the production bound - see fsm_config::max_follower_log_size
    // for that. What does hold here, and in production, is that B ends up with
    // no more than A's log plus its own trailing entries, which is asserted
    // after the loop.
    fsm_debug A(A_id, term_t{1}, server_id{}, f.make_log(10), trivial_failure_detector, cfg);
    election_timeout(A);
    auto output = A.get_output();
    BOOST_REQUIRE(output.term_and_vote);
    A.step(B_id, raft::vote_reply{output.term_and_vote->first, true});
    BOOST_REQUIRE(A.is_leader());
    // The dummy A appends on election is the entry it has to commit before it can
    // commit anything else at all (see fsm::maybe_commit).
    const index_t dummy_idx = A.log_last_idx();
    BOOST_REQUIRE_EQUAL(dummy_idx, index_t{11});

    // Only A is ticked: B must not start an election, and A's heartbeats keep it
    // from stepping down.
    for (int i = 0; i < 50 && B.commit_idx() < dummy_idx; i++) {
        A.tick();
        communicate(A, B);
    }

    // A walked B up to its dummy one entry at a time, committed it, and told B.
    BOOST_CHECK_EQUAL(A.commit_idx(), dummy_idx);
    BOOST_CHECK_EQUAL(B.commit_idx(), dummy_idx);
    BOOST_CHECK_EQUAL(B.get_log().last_idx(), dummy_idx);
    // Having committed, B can finally shrink - and now it does report itself full.
    BOOST_CHECK_GT(B.commit_idx(), B.get_log().get_snapshot().idx);
    BOOST_CHECK(B.log_is_full());
    BOOST_CHECK(A.get_progress(B_id).log_full);

    // The escape hatch let B past its budget, but only up to what A had to give
    // it: B's snapshot is at 0, so it keeps no trailing entries and should hold
    // exactly A's log and nothing more.
    BOOST_CHECK_LE(B.get_log().memory_usage(), A.get_log().memory_usage());
}

// A throttled follower which has fallen below the leader's snapshot cannot be
// probed with an append_request at all: the leader has no way to name the term
// of the entry preceding next_idx, so it cannot verify the log matching
// property. It transfers the snapshot instead, which also truncates the
// follower's log - exactly what a full follower needs.
BOOST_AUTO_TEST_CASE(test_leader_snapshots_throttled_follower_that_fell_behind) {
    server_id A_id = id(), B_id = id(), C_id = id();

    raft::log log{raft::snapshot_descriptor{.idx = index_t{0},
            .config = config_from_ids({A_id, B_id, C_id})}};
    fsm_debug A(A_id, term_t{1}, server_id{}, std::move(log), trivial_failure_detector, fsm_cfg);

    election_timeout(A);
    auto output = A.get_output();
    BOOST_REQUIRE(output.term_and_vote);
    A.step(B_id, raft::vote_reply{output.term_and_vote->first, true});
    BOOST_REQUIRE(A.is_leader());
    const term_t term = A.get_current_term();

    // Both followers ack the dummy, so they leave PROBE with match_idx at 1.
    output = A.get_output();
    const index_t dummy_idx = A.log_last_idx();
    A.step(C_id, raft::append_reply{term, dummy_idx, raft::append_reply::accepted{dummy_idx}});
    // B additionally reports a full log, so the leader stops replicating to it
    // and it stays behind at the dummy.
    A.step(B_id, raft::append_reply{term, dummy_idx, raft::append_reply::accepted{dummy_idx},
            /* clock_ok = */ false, /* log_full = */ true});
    BOOST_REQUIRE(A.get_progress(B_id).log_full);
    BOOST_REQUIRE_EQUAL(A.get_progress(B_id).next_idx, dummy_idx + index_t{1});

    // The group keeps making progress on A and C alone.
    for (int i = 0; i < 5; i++) {
        A.add_entry(create_command(i));
    }
    (void)A.get_output();
    const index_t last_idx = A.log_last_idx();
    A.step(C_id, raft::append_reply{term, last_idx, raft::append_reply::accepted{last_idx}});
    BOOST_REQUIRE_EQUAL(A.commit_idx(), last_idx);
    (void)A.get_output();
    // B was never sent any of it.
    BOOST_REQUIRE_EQUAL(A.get_progress(B_id).next_idx, dummy_idx + index_t{1});

    // A snapshots its whole log away, so it can no longer name the term of the
    // entry preceding B's next_idx.
    BOOST_REQUIRE(A.apply_snapshot(
            raft::snapshot_descriptor{.idx = last_idx, .term = term,
                    .config = A.get_configuration()},
            0, 0, true));
    (void)A.get_output();

    // Throttled, so nothing goes out until a tick - and then it must be the
    // snapshot, not an append_request.
    A.tick();
    output = A.get_output();
    bool snapshot_to_b = false;
    for (auto& [to, msg] : output.messages) {
        if (to == B_id) {
            BOOST_CHECK(std::holds_alternative<raft::install_snapshot>(msg));
            snapshot_to_b = std::holds_alternative<raft::install_snapshot>(msg);
        }
    }
    BOOST_CHECK(snapshot_to_b);
    BOOST_CHECK(A.get_progress(B_id).state == raft::follower_progress::state::SNAPSHOT);
}

// A full follower whose tail diverges from the leader's must still be sent the
// conflicting entry: log::truncate_uncommitted() is the only thing that can
// remove uncommitted entries, since the follower can neither apply nor snapshot
// them away. Throttling it there blocks the one thing that frees its log, and
// blocks it for as long as its applier is behind - which is precisely when the
// log_full flag is set in the first place. So a reject must not throttle us.
BOOST_AUTO_TEST_CASE(test_leader_replicates_to_full_follower_with_diverged_tail) {
    follower_log_limit_fixture f;
    const auto A_id = f.leader_id, B_id = f.follower_id;
    const auto cfg = f.config(6);
    const auto old_leader = id();

    auto entry = [&f] (term_t t, index_t i) {
        return seastar::make_lw_shared<const raft::log_entry>(raft::log_entry{t, i, f.cmd});
    };
    // Both logs agree on 1..3 at term 1 and diverge from index 4 on.
    auto make_log = [&] (term_t tail_term, size_t upto) {
        raft::log_entries entries;
        for (size_t i = 1; i <= 3; i++) {
            entries.push_back(entry(term_t{1}, index_t{i}));
        }
        for (size_t i = 4; i <= upto; i++) {
            entries.push_back(entry(tail_term, index_t{i}));
        }
        return raft::log{raft::snapshot_descriptor{.idx = index_t{0}, .term = term_t{1},
                .config = config_from_ids({A_id, B_id})}, std::move(entries)};
    };

    // B is at its budget with a diverged, uncommitted tail at term 1.
    fsm_debug B(B_id, term_t{1}, server_id{}, make_log(term_t{1}, 6),
            trivial_failure_detector, cfg);
    BOOST_REQUIRE_EQUAL(B.get_log().memory_usage(), 6 * f.entry_size);
    // A previous leader told B that its prefix is committed, so it has something
    // to apply - and therefore reports itself full. Its applier never runs here,
    // which models the slow state machine this whole limit exists for: the
    // snapshot index stays at 0, so it can never stop reporting full on its own.
    B.step(old_leader, raft::append_request{.current_term = term_t{1},
            .prev_log_idx = index_t{3}, .prev_log_term = term_t{1},
            .leader_commit_idx = index_t{3}, .entries = {}});
    (void)B.get_output();
    BOOST_REQUIRE_EQUAL(B.commit_idx(), index_t{3});
    BOOST_REQUIRE(B.log_is_full());

    // A holds a longer log whose tail is from a later term, so it conflicts with
    // B's from index 4 on. It is elected in a later term still.
    fsm_debug A(A_id, term_t{1}, server_id{}, make_log(term_t{2}, 10),
            trivial_failure_detector, cfg);
    election_timeout(A);
    auto output = A.get_output();
    BOOST_REQUIRE(output.term_and_vote);
    A.step(B_id, raft::vote_reply{output.term_and_vote->first, true});
    BOOST_REQUIRE(A.is_leader());

    // Only A is ticked, so B does not call an election; A's heartbeats keep it
    // from stepping down.
    for (int i = 0; i < 50 && B.get_log()[4]->term != term_t{2}; i++) {
        A.tick();
        communicate(A, B);
    }

    // B's diverged tail was truncated and replaced by A's. That is what frees a
    // full follower, and it is the only thing that can: those entries were
    // uncommitted, so B could neither apply nor snapshot them away.
    BOOST_CHECK_EQUAL(B.get_log()[4]->term, term_t{2});
    BOOST_CHECK_GT(A.get_progress(B_id).match_idx, index_t{3});
    for (auto i = index_t{4}; i <= B.get_log().last_idx(); ++i) {
        BOOST_CHECK_EQUAL(B.get_log()[i.value()]->term, term_t{2});
    }

    // From here B is throttled again, legitimately: it refilled to its budget and
    // still has a commit index above its snapshot, and its applier never runs in
    // this test, so it cannot make room for A's remaining entries. That is the
    // damper working, not the deadlock this test is about.
    BOOST_CHECK(B.log_is_full());
    BOOST_CHECK(A.get_progress(B_id).log_full);
}
