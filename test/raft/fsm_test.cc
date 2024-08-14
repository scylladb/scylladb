/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#define BOOST_TEST_MODULE raft

#include "raft/tracker.hh"
#include "test/raft/helpers.hh"

using namespace raft;

namespace raft {

std::ostream& boost_test_print_type(std::ostream& os, const vote_result& v) {
    fmt::print(os, "{}", v);
    return os;
}

}

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
            {server_addr_from_id(id1), true}, {server_addr_from_id(id2), true},
            {server_addr_from_id(id3), true}, {server_addr_from_id(id4), false}}));
    votes.register_vote(id1, true);
    votes.register_vote(id2, true);
    BOOST_CHECK_EQUAL(votes.tally_votes(), raft::vote_result::WON);
    // Basic test that non-voting votes are ignored
    votes = raft::votes(raft::configuration({
            {server_addr_from_id(id1), true}, {server_addr_from_id(id2), true},
            {server_addr_from_id(id3), true}, {server_addr_from_id(id4), false}}));
    votes.register_vote(id1, true);
    votes.register_vote(id4, true);
    BOOST_CHECK_EQUAL(votes.tally_votes(), raft::vote_result::UNKNOWN);
    votes.register_vote(id3, true);
    BOOST_CHECK_EQUAL(votes.tally_votes(), raft::vote_result::WON);
    // Joint configuration with non voting members
    votes = raft::votes(raft::configuration(
            {{server_addr_from_id(id1), true}},
            {{server_addr_from_id(id2), true}, {server_addr_from_id(id3), true}, {server_addr_from_id(id4), false}}));
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
            {{server_addr_from_id(id1), true}, {server_addr_from_id(id4), true}},
            {{server_addr_from_id(id2), true}, {server_addr_from_id(id3), true}, {server_addr_from_id(id4), false}}));
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
    cfg.enter_joint({{server_addr_from_id(id1), true}, {server_addr_from_id(id2), true}, {server_addr_from_id(id3), false}});
    cfg.leave_joint();
    tracker.set_configuration(cfg, index_t{1});
    pr(id1)->accepted(index_t{30});
    pr(id2)->accepted(index_t{25});
    pr(id3)->accepted(index_t{30});
    BOOST_CHECK_EQUAL(tracker.committed(index_t{0}), index_t{25});

    // Check that non voting member is not counted for the quorum in joint config
    cfg.enter_joint({{server_addr_from_id(id4), true}, {server_addr_from_id(id5), true}});
    tracker.set_configuration(cfg, index_t{1});
    pr(id4)->accepted(index_t{30});
    pr(id5)->accepted(index_t{30});
    BOOST_CHECK_EQUAL(tracker.committed(index_t{0}), index_t{25});

    // Check the case where the same node is in both config but different voting rights
    cfg.leave_joint();
    cfg.enter_joint({{server_addr_from_id(id1), true}, {server_addr_from_id(id2), true}, {server_addr_from_id(id5), false}});
    BOOST_CHECK_EQUAL(tracker.committed(index_t{0}), index_t{25});
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
        {server_addr_from_id(id1), true}, {server_addr_from_id(id2), true}, {server_addr_from_id(id3), false}});
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
    raft::configuration newcfg({{server_addr_from_id(id2), true}, {server_addr_from_id(id3), false}});
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
        {server_addr_from_id(id1), true}, {server_addr_from_id(id2), true}, {server_addr_from_id(id3), true}});
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
    raft::configuration newcfg2({{server_addr_from_id(id2), true}, {server_addr_from_id(id3), true}});
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
            config_member_from_id(C_id), raft::config_member{server_addr_from_id(D_id), false}};

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
        raft::config_member{server_addr_from_id(A_id), true},
        raft::config_member{server_addr_from_id(B_id), false}};
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
    BOOST_CHECK_EQUAL(A.get_configuration().current.find(config_member_from_id(B_id))->can_vote, true);
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
    BOOST_CHECK_EQUAL(A.get_configuration().current.find(config_member_from_id(B_id))->can_vote, false);
    BOOST_CHECK(A.get_progress(B_id).state == raft::follower_progress::state::PIPELINE);
}

BOOST_AUTO_TEST_CASE(test_leader_change_to_non_voter) {
    // Test a two-node cluster, change a leader to a non-voter.
    server_id A_id = id(), B_id = id();
    raft::config_member_set oldset{
        raft::config_member{server_addr_from_id(A_id), true},
        raft::config_member{server_addr_from_id(B_id), false}};
    raft::configuration cfg(oldset);
    raft::log log(raft::snapshot_descriptor{.idx = index_t{0}, .config = cfg});
    auto A = create_follower(A_id, log);
    auto B = create_follower(B_id, log);
    election_timeout(A);
    communicate(A, B);
    BOOST_CHECK(A.is_leader());
    raft::config_member_set newset{
        raft::config_member{server_addr_from_id(A_id), false},
        raft::config_member{server_addr_from_id(B_id), true}};
    raft::configuration newcfg(newset);
    A.add_entry(newcfg);
    A.tick();
    communicate(A, B);
    BOOST_CHECK(A.is_follower());
    BOOST_CHECK(B.is_leader());
    // Try to switch the leader to a non-voter, leaving no other voters.
    newset = raft::config_member_set{
        raft::config_member{server_addr_from_id(A_id), false},
        raft::config_member{server_addr_from_id(B_id), false}};
    newcfg = raft::configuration(newset);
    BOOST_CHECK_THROW(B.add_entry(newcfg), std::invalid_argument);
    // Try to remove the last remaining voter
    newset = raft::config_member_set{raft::config_member{server_addr_from_id(B_id), false}};
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
            raft::config_member{server_addr_from_id(A_id), true},
            raft::config_member{server_addr_from_id(B_id), true},
            raft::config_member{server_addr_from_id(C_id), false}});

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
            raft::config_member{server_addr_from_id(A_id), true},
            raft::config_member{server_addr_from_id(B_id), true},
            raft::config_member{server_addr_from_id(C_id), false}});

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
            raft::config_member{server_addr_from_id(A_id), true},
            raft::config_member{server_addr_from_id(B_id), true},
            raft::config_member{server_addr_from_id(C_id), false}});

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
            raft::config_member{server_addr_from_id(A_id), true},
            raft::config_member{server_addr_from_id(B_id), true},
            raft::config_member{server_addr_from_id(C_id), false}});
    A.tick();
    A.add_entry(cfg_with_non_voter);
    A.tick();
    // Majority commits the configuration change
    communicate(A, B);
    BOOST_CHECK_EQUAL(A.get_configuration().is_joint(), false);
    BOOST_CHECK_EQUAL(A.get_configuration().current.find(config_member_from_id(C_id))->can_vote, false);
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
    BOOST_CHECK_EQUAL(A.get_configuration().current.find(config_member_from_id(C_id))->can_vote, true);
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
            raft::config_member{server_addr_from_id(A_id), true},
            raft::config_member{server_addr_from_id(B_id), true},
            raft::config_member{server_addr_from_id(C_id), false}});

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
    BOOST_CHECK_EQUAL(A.get_configuration().current.find(config_member_from_id(C_id))->can_vote, true);
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
            raft::config_member{server_addr_from_id(A_id), true},
            raft::config_member{server_addr_from_id(B_id), true},
            raft::config_member{server_addr_from_id(C_id), false}});

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

    raft::configuration cfg(raft::config_member_set{raft::config_member{server_addr_from_id(id1), true},
                                                    raft::config_member{server_addr_from_id(id2), true}});
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
