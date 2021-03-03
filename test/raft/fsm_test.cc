/*
 * Copyright (c) 2020, Arm Limited and affiliates. All rights reserved.
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

#include "test/raft/helpers.hh"

BOOST_AUTO_TEST_CASE(test_votes) {
    auto id = []() -> raft::server_address { return raft::server_address{utils::make_random_uuid()}; };
    auto id1 = id();

    raft::votes votes(raft::configuration({id1}));
    BOOST_CHECK_EQUAL(votes.tally_votes(), raft::vote_result::UNKNOWN);
    BOOST_CHECK_EQUAL(votes.voters().size(), 1);
    // Try a vote from an unknown server, it should be ignored.
    votes.register_vote(id().id, true);
    votes.register_vote(id1.id, false);
    // Quorum votes against the decision
    BOOST_CHECK_EQUAL(votes.tally_votes(), raft::vote_result::LOST);
    // Another vote from the same server is ignored
    votes.register_vote(id1.id, true);
    votes.register_vote(id1.id, true);
    BOOST_CHECK_EQUAL(votes.tally_votes(), raft::vote_result::LOST);
    auto id2 = id();
    votes = raft::votes(raft::configuration({id1, id2}));
    BOOST_CHECK_EQUAL(votes.voters().size(), 2);
    votes.register_vote(id1.id, true);
    // We need a quorum of participants to win an election
    BOOST_CHECK_EQUAL(votes.tally_votes(), raft::vote_result::UNKNOWN);
    votes.register_vote(id2.id, false);
    // At this point it's clear we don't have enough votes
    BOOST_CHECK_EQUAL(votes.tally_votes(), raft::vote_result::LOST);
    auto id3 = id();
    // Joint configuration
    votes = raft::votes(raft::configuration({id1}, {id2, id3}));
    BOOST_CHECK_EQUAL(votes.voters().size(), 3);
    BOOST_CHECK_EQUAL(votes.tally_votes(), raft::vote_result::UNKNOWN);
    votes.register_vote(id2.id, true);
    votes.register_vote(id3.id, true);
    BOOST_CHECK_EQUAL(votes.tally_votes(), raft::vote_result::UNKNOWN);
    votes.register_vote(id1.id, false);
    BOOST_CHECK_EQUAL(votes.tally_votes(), raft::vote_result::LOST);
    votes = raft::votes(raft::configuration({id1}, {id2, id3}));
    votes.register_vote(id2.id, true);
    votes.register_vote(id3.id, true);
    votes.register_vote(id1.id, true);
    BOOST_CHECK_EQUAL(votes.tally_votes(), raft::vote_result::WON);
    votes = raft::votes(raft::configuration({id1, id2, id3}, {id1}));
    BOOST_CHECK_EQUAL(votes.voters().size(), 3);
    votes.register_vote(id1.id, true);
    BOOST_CHECK_EQUAL(votes.tally_votes(), raft::vote_result::UNKNOWN);
    // This gives us a majority in both new and old
    // configurations.
    votes.register_vote(id2.id, true);
    BOOST_CHECK_EQUAL(votes.tally_votes(), raft::vote_result::WON);
    // Basic voting test for 4 nodes
    auto id4 = id();
    votes = raft::votes(raft::configuration({id1, id2, id3, id4}));
    votes.register_vote(id1.id, true);
    votes.register_vote(id2.id, true);
    BOOST_CHECK_EQUAL(votes.tally_votes(), raft::vote_result::UNKNOWN);
    votes.register_vote(id3.id, false);
    BOOST_CHECK_EQUAL(votes.tally_votes(), raft::vote_result::UNKNOWN);
    votes.register_vote(id4.id, false);
    BOOST_CHECK_EQUAL(votes.tally_votes(), raft::vote_result::LOST);
    auto id5 = id();
    // Basic voting test for 5 nodes
    votes = raft::votes(raft::configuration({id1, id2, id3, id4, id5}, {id1, id2, id3}));
    votes.register_vote(id1.id, false);
    votes.register_vote(id2.id, false);
    BOOST_CHECK_EQUAL(votes.tally_votes(), raft::vote_result::LOST);
    votes.register_vote(id3.id, true);
    votes.register_vote(id4.id, true);
    votes.register_vote(id5.id, true);
    BOOST_CHECK_EQUAL(votes.tally_votes(), raft::vote_result::LOST);
    // Basic voting test with tree voters and one no-voter
    votes = raft::votes(raft::configuration({id1, id2, id3, {id4.id, false}}));
    votes.register_vote(id1.id, true);
    votes.register_vote(id2.id, true);
    BOOST_CHECK_EQUAL(votes.tally_votes(), raft::vote_result::WON);
    // Basic test that non-voting votes are ignored
    votes = raft::votes(raft::configuration({id1, id2, id3, {id4.id, false}}));
    votes.register_vote(id1.id, true);
    votes.register_vote(id4.id, true);
    BOOST_CHECK_EQUAL(votes.tally_votes(), raft::vote_result::UNKNOWN);
    votes.register_vote(id3.id, true);
    BOOST_CHECK_EQUAL(votes.tally_votes(), raft::vote_result::WON);
    // Joint configuration with non voting members
    votes = raft::votes(raft::configuration({id1}, {id2, id3, {id4.id, false}}));
    BOOST_CHECK_EQUAL(votes.voters().size(), 3);
    BOOST_CHECK_EQUAL(votes.tally_votes(), raft::vote_result::UNKNOWN);
    votes.register_vote(id2.id, true);
    votes.register_vote(id3.id, true);
    votes.register_vote(id4.id, true);
    BOOST_CHECK_EQUAL(votes.tally_votes(), raft::vote_result::UNKNOWN);
    votes.register_vote(id1.id, true);
    BOOST_CHECK_EQUAL(votes.tally_votes(), raft::vote_result::WON);
    // Same node is voting in one config and non voting in another
    votes = raft::votes(raft::configuration({id1, id4}, {id2, id3, {id4.id, false}}));
    votes.register_vote(id2.id, true);
    votes.register_vote(id1.id, true);
    votes.register_vote(id4.id, true);
    BOOST_CHECK_EQUAL(votes.tally_votes(), raft::vote_result::UNKNOWN);
    votes.register_vote(id3.id, true);
    BOOST_CHECK_EQUAL(votes.tally_votes(), raft::vote_result::WON);
}

BOOST_AUTO_TEST_CASE(test_tracker) {
    auto id = []() -> raft::server_address { return raft::server_address{utils::make_random_uuid()}; };
    auto id1 = id();
    raft::tracker tracker(id1.id);
    raft::configuration cfg({id1});
    tracker.set_configuration(cfg, index_t{1});
    BOOST_CHECK_NE(tracker.find(id1.id), nullptr);
    // The node with id set during construction is assumed to be
    // the leader, since otherwise we wouldn't create a tracker
    // in the first place.
    BOOST_CHECK_EQUAL(tracker.find(id1.id), tracker.leader_progress());
    BOOST_CHECK_EQUAL(tracker.committed(index_t{0}), index_t{0});
    // Avoid keeping a reference, follower_progress address may
    // change with configuration change
    auto pr = [&tracker](raft::server_address address) -> raft::follower_progress* {
        return tracker.find(address.id);
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
    cfg.enter_joint({id1, id2, id3});
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
    cfg.enter_joint({id3, id4, id5});
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
    cfg.enter_joint({id1});
    cfg.leave_joint();
    cfg.enter_joint({id2});
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
    cfg.enter_joint({id1, id2, {id3.id, false}});
    cfg.leave_joint();
    tracker.set_configuration(cfg, index_t{1});
    pr(id1)->accepted(index_t{30});
    pr(id2)->accepted(index_t{25});
    pr(id3)->accepted(index_t{30});
    BOOST_CHECK_EQUAL(tracker.committed(index_t{0}), index_t{25});

    // Check that non voting member is not counted for the quorum in joint config
    cfg.enter_joint({id4, id5});
    tracker.set_configuration(cfg, index_t{1});
    pr(id4)->accepted(index_t{30});
    pr(id5)->accepted(index_t{30});
    BOOST_CHECK_EQUAL(tracker.committed(index_t{0}), index_t{25});

    // Check the case where the same node is in both config but different voting rights
    cfg.leave_joint();
    cfg.enter_joint({id1, id2, {id5.id, false}});
    BOOST_CHECK_EQUAL(tracker.committed(index_t{0}), index_t{25});
}

BOOST_AUTO_TEST_CASE(test_log_last_conf_idx) {
    // last_conf_idx, prev_conf_idx are initialized correctly,
    // and maintained during truncate head/truncate tail
    server_id id1{utils::make_random_uuid()};
    raft::configuration cfg({id1});
    raft::log log{raft::snapshot{.config = cfg}};
    BOOST_CHECK_EQUAL(log.last_conf_idx(), 0);
    add_entry(log, cfg);
    BOOST_CHECK_EQUAL(log.last_conf_idx(), 1);
    add_entry(log, log_entry::dummy{});
    add_entry(log, cfg);
    BOOST_CHECK_EQUAL(log.last_conf_idx(), 3);
    // apply snapshot truncates the log and resets last_conf_idx()
    log.apply_snapshot(log_snapshot(log, log.last_idx()), 0);
    BOOST_CHECK_EQUAL(log.last_conf_idx(), 0);
    // log::last_term() is maintained correctly by truncate_head/truncate_tail() (snapshotting)
    BOOST_CHECK_EQUAL(log.last_term(), log.get_snapshot().term);
    BOOST_CHECK(log.term_for(log.get_snapshot().idx).has_value());
    BOOST_CHECK_EQUAL(log.term_for(log.get_snapshot().idx).value(), log.get_snapshot().term);
    BOOST_CHECK(! log.term_for(log.last_idx() - index_t{1}).has_value());
    add_entry(log, log_entry::dummy{});
    BOOST_CHECK(log.term_for(log.last_idx()).has_value());
    add_entry(log, log_entry::dummy{});
    const size_t GAP = 10;
    // apply_snapshot with a log gap, this should clear all log
    // entries, despite that trailing is given, a gap
    // between old log entries and a snapshot would violate
    // log continuity.
    log.apply_snapshot(log_snapshot(log, log.last_idx() + index_t{GAP}), GAP * 2);
    BOOST_CHECK(log.empty());
    BOOST_CHECK_EQUAL(log.next_idx(), log.get_snapshot().idx + index_t{1});
    add_entry(log, log_entry::dummy{});
    BOOST_CHECK_EQUAL(log.in_memory_size(), 1);
    add_entry(log, log_entry::dummy{});
    BOOST_CHECK_EQUAL(log.in_memory_size(), 2);
    // Set trailing longer than the length of the log.
    log.apply_snapshot(log_snapshot(log, log.last_idx()), 3);
    BOOST_CHECK_EQUAL(log.in_memory_size(), 2);
    // Set trailing the same length as the current log length
    add_entry(log, log_entry::dummy{});
    BOOST_CHECK_EQUAL(log.in_memory_size(), 3);
    log.apply_snapshot(log_snapshot(log, log.last_idx()), 3);
    BOOST_CHECK_EQUAL(log.in_memory_size(), 3);
    BOOST_CHECK_EQUAL(log.last_conf_idx(), 0);
    add_entry(log, log_entry::dummy{});
    // Set trailing shorter than the length of the log
    log.apply_snapshot(log_snapshot(log, log.last_idx()), 1);
    BOOST_CHECK_EQUAL(log.in_memory_size(), 1);
}

void test_election_single_node_helper(raft::fsm_config fcfg) {

    failure_detector fd;
    server_id id1{utils::make_random_uuid()};
    raft::configuration cfg({id1});
    raft::log log{raft::snapshot{.config = cfg}};
    raft::fsm fsm(id1, term_t{}, server_id{}, std::move(log), fd, fcfg);

    BOOST_CHECK(fsm.is_follower());

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

    failure_detector fd;
    server_id id1{utils::make_random_uuid()};
    raft::configuration cfg({id1});
    raft::log log{raft::snapshot{.config = cfg}};

    raft::fsm fsm(id1, term_t{}, server_id{}, std::move(log), fd, fsm_cfg);

    election_timeout(fsm);

    // Immediately converts from leader to follower if quorum=1
    BOOST_CHECK(fsm.is_leader());

    (void) fsm.get_output();

    fsm.add_entry(raft::command{});

    BOOST_CHECK(fsm.get_output().messages.empty());

    fsm.tick();

    BOOST_CHECK(fsm.get_output().messages.empty());
}

BOOST_AUTO_TEST_CASE(test_election_two_nodes) {

    failure_detector fd;

    server_id id1{utils::make_random_uuid()}, id2{utils::make_random_uuid()};

    raft::configuration cfg({id1, id2});
    raft::log log{raft::snapshot{.config = cfg}};

    raft::fsm fsm(id1, term_t{}, server_id{}, std::move(log), fd, fsm_cfg);

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

    // Vote request within the election timeout is ignored
    // (avoiding disruptive leaders).
    fsm.step(id2, raft::vote_request{output.term_and_vote->first + term_t{1}});
    BOOST_CHECK(fsm.is_leader());
    // Any message with a newer term after election timeout
    // -> immediately convert to follower
    fd.alive = false;
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

    failure_detector fd;

    server_id id1{utils::make_random_uuid()},
              id2{utils::make_random_uuid()},
              id3{utils::make_random_uuid()},
              id4{utils::make_random_uuid()};

    raft::configuration cfg({id1, id2, id3, id4});
    raft::log log{raft::snapshot{.config = cfg}};

    raft::fsm fsm(id1, term_t{}, server_id{}, std::move(log), fd, fsm_cfg);

    // Initial state is follower
    BOOST_CHECK(fsm.is_follower());

    // Inform FSM about a new leader at a new term
    fsm.step(id4, raft::append_request{term_t{1}, id4, index_t{1}, term_t{1}});

    (void) fsm.get_output();

    // Request a vote during the same term. Even though
    // we haven't voted, we should deny a vote because we
    // know about a leader for this term.
    fsm.step(id3, raft::vote_request{term_t{1}, index_t{1}, term_t{1}});

    auto output = fsm.get_output();
    auto reply = std::get<raft::vote_reply>(output.messages.back().second);
    BOOST_CHECK(!reply.vote_granted);

    // Run out of steam for this term. Start a new one.
    fd.alive = false;
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

    failure_detector fd;

    server_id id1{utils::make_random_uuid()}, id2{utils::make_random_uuid()};

    raft::configuration cfg({id1, id2});
    raft::log log{raft::snapshot{.config = cfg}};

    raft::fsm fsm(id1, term_t{}, server_id{}, std::move(log), fd, fcfg);

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

    // Check that prevote with higer term get a reply with term in the future
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

    failure_detector fd;

    server_id id1{utils::make_random_uuid()},
              id2{utils::make_random_uuid()},
              id3{utils::make_random_uuid()},
              id4{utils::make_random_uuid()};

    raft::configuration cfg({id1, id2, id3, id4});
    raft::log log{raft::snapshot{.config = cfg}};

    raft::fsm fsm(id1, term_t{}, server_id{}, std::move(log), fd, fcfg);

    // Initial state is follower
    BOOST_CHECK(fsm.is_follower());

    // Inform FSM about a new leader at a new term
    fsm.step(id4, raft::append_request{term_t{1}, id4, index_t{1}, term_t{1}});

    (void) fsm.get_output();

    // Request a prevote during the same term. Even though
    // we haven't voted, we should deny a vote because we
    // know about a leader for this term.
    fsm.step(id3, raft::vote_request{term_t{1}, index_t{1}, term_t{1}, true});

    auto output = fsm.get_output();
    auto reply = std::get<raft::vote_reply>(output.messages.back().second);
    BOOST_CHECK(!reply.vote_granted && reply.is_prevote);

    // Run out of steam for this term. Start a new one.
    fd.alive = false;
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

    failure_detector fd;

    server_id id1{utils::make_random_uuid()},
              id2{utils::make_random_uuid()},
              id3{utils::make_random_uuid()};

    raft::configuration cfg({id1, id2, id3});
    raft::log log(raft::snapshot{.idx = index_t{999}, .config = cfg});

    log.emplace_back(seastar::make_lw_shared<raft::log_entry>(raft::log_entry{term_t{10}, index_t{1000}}));
    log.stable_to(log.last_idx());

    raft::fsm fsm(id1, term_t{10}, server_id{}, std::move(log), fd, fsm_cfg);

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

    failure_detector fd;

    server_id id1{utils::make_random_uuid()},
              id2{utils::make_random_uuid()},
              id3{utils::make_random_uuid()};

    raft::configuration cfg({id1, id2});
    raft::log log(raft::snapshot{.idx = index_t{100}, .config = cfg});

    raft::fsm fsm(id1, term_t{1}, /* voted for */ server_id{}, std::move(log), fd, fsm_cfg);

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

    raft::configuration newcfg({id1, id2, id3});
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
    // Calling get_output() again indicates the previous output
    // is handled, i.e. the log entry is committed, so now
    // the leader will replicate the confchange
    output = fsm.get_output();
    // Append entry for id2
    BOOST_CHECK_EQUAL(output.messages.size(), 1);
    auto msg = std::get<raft::append_request>(output.messages.back().second);
    auto idx = msg.entries.back().get()->idx;
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
    output = fsm.get_output();
    // AppendEntries messages for the final configuration
    BOOST_CHECK(output.messages.size() >= 1);
    msg = std::get<raft::append_request>(output.messages.back().second);
    idx = msg.entries.back().get()->idx;
    // Ack AppendEntries for the final configuration
    fsm.step(id2, raft::append_reply{msg.current_term, idx, raft::append_reply::accepted{idx}});
    BOOST_CHECK_EQUAL(fsm.get_configuration().current.size(), 3);
    fsm.step(id3, raft::append_reply{msg.current_term, idx, raft::append_reply::accepted{idx}});
    // Check that we can start a new confchange
    raft::configuration newcfg2({id1, id2});
    fsm.add_entry(newcfg);
}

BOOST_AUTO_TEST_CASE(test_confchange_remove_node) {

    failure_detector fd;

    server_id id1{utils::make_random_uuid()},
              id2{utils::make_random_uuid()},
              id3{utils::make_random_uuid()};

    raft::configuration cfg({id1, id2, id3});
    raft::log log(raft::snapshot{.idx = index_t{100}, .config = cfg});

    raft::fsm fsm(id1, term_t{1}, /* voted for */ server_id{}, std::move(log), fd, fsm_cfg);

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

    raft::configuration newcfg({id1, id2});
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
    raft::append_request msg;
    BOOST_REQUIRE_NO_THROW(msg = std::get<raft::append_request>(output.messages[0].second));
    BOOST_CHECK_EQUAL(msg.entries.size(), 1);
    auto idx = msg.entries.back().get()->idx;
    // In order to accept a configuration change
    // we need one ACK, since there is a quorum overlap.
    fsm.step(id2, raft::append_reply{msg.current_term, idx, raft::append_reply::accepted{idx}});

    output = fsm.get_output();
    // AppendEntries messages for the final configuration
    BOOST_CHECK(output.messages.size() >= 1);

    BOOST_REQUIRE_NO_THROW(msg = std::get<raft::append_request>(output.messages[0].second));
    BOOST_CHECK_EQUAL(msg.entries.size(), 1);
    if (msg.entries.size()) {
        BOOST_CHECK(std::holds_alternative<raft::configuration>(msg.entries[0]->data));
    }
    idx = msg.entries.back().get()->idx;
    BOOST_CHECK_EQUAL(idx, 102);
    // Ack AppendEntries for the joint configuration
    fsm.step(id2, raft::append_reply{msg.current_term, idx, raft::append_reply::accepted{idx}});

    output = fsm.get_output();
    // A log entry for the final configuration
    BOOST_CHECK_EQUAL(output.log_entries.size(), 1);
    if (output.log_entries.size()) {
        BOOST_CHECK(std::holds_alternative<raft::configuration>(output.log_entries[0]->data));
    }

    BOOST_CHECK_EQUAL(fsm.get_configuration().current.size(), 2);
    BOOST_CHECK(!fsm.get_configuration().is_joint());

    output = fsm.get_output();
    BOOST_CHECK_EQUAL(output.messages.size(), 1);
    BOOST_REQUIRE_NO_THROW(msg = std::get<raft::append_request>(output.messages[0].second));
    BOOST_CHECK_EQUAL(msg.entries.size(), 1);
    if (msg.entries.size()) {
        BOOST_CHECK(std::holds_alternative<raft::configuration>(msg.entries[0]->data));
    }
    idx = msg.entries.back().get()->idx;
    BOOST_CHECK_EQUAL(idx, 103);
    // Ack AppendEntries for the final configuration
    fsm.step(id2, raft::append_reply{msg.current_term, idx, raft::append_reply::accepted{idx}});

    // Check that we can start a new confchange
    raft::configuration newcfg2({id1, id2, id3});
    fsm.add_entry(newcfg);
    BOOST_CHECK_EQUAL(fsm.get_configuration().current.size(), 2);
}

BOOST_AUTO_TEST_CASE(test_confchange_replace_node) {

    failure_detector fd;

    server_id id1{utils::make_random_uuid()},
              id2{utils::make_random_uuid()},
              id3{utils::make_random_uuid()},
              id4{utils::make_random_uuid()};

    raft::configuration cfg({id1, id2, id3});
    raft::log log(raft::snapshot{.idx = index_t{100}, .config = cfg});

    raft::fsm fsm(id1, term_t{1}, /* voted for */ server_id{}, std::move(log), fd, fsm_cfg);

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

    raft::configuration newcfg({id1, id2, id4});
    // Suggest a confchange.
    fsm.add_entry(newcfg);
    // Entered joint configuration immediately.
    BOOST_CHECK(fsm.get_configuration().is_joint());
    BOOST_CHECK_EQUAL(fsm.get_configuration().current.size(), 3);
    BOOST_CHECK_EQUAL(fsm.get_configuration().previous.size(), 3);
    output = fsm.get_output();
    BOOST_CHECK_EQUAL(output.messages.size(), 2);
    raft::append_request msg;
    if (output.messages.size() > 0) {
        BOOST_REQUIRE_NO_THROW(msg = std::get<raft::append_request>(output.messages[0].second));
        BOOST_CHECK(std::holds_alternative<raft::log_entry::dummy>(msg.entries[0]->data));
    }
    if (output.messages.size() > 1) {
        BOOST_REQUIRE_NO_THROW(msg = std::get<raft::append_request>(output.messages[1].second));
        BOOST_CHECK(std::holds_alternative<raft::log_entry::dummy>(msg.entries[0]->data));
    }

    output = fsm.get_output();
    BOOST_REQUIRE_NO_THROW(msg = std::get<raft::append_request>(output.messages[0].second));
    auto idx = msg.entries.back().get()->idx;
    // In order to accept a configuration change
    // we need two ACK, since there is a quorum overlap.
    fsm.step(id2, raft::append_reply{msg.current_term, idx, raft::append_reply::accepted{idx}});
    BOOST_CHECK(!fsm.get_configuration().is_joint());
    // Joint config to log
    output = fsm.get_output();
    BOOST_CHECK_EQUAL(output.log_entries.size(), 1);
    if (output.log_entries.size()) {
        BOOST_CHECK(std::holds_alternative<raft::configuration>(output.log_entries[0]->data));
    }
    output = fsm.get_output();
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
    failure_detector fd;

    server_id id1{utils::make_random_uuid()},
              id2{utils::make_random_uuid()},
              id3{utils::make_random_uuid()};

    raft::configuration cfg({raft::server_address{id1.id}, raft::server_address{id2.id}, raft::server_address{id3.id, false}});
    raft::log log(raft::snapshot{.config = cfg});

    raft::fsm fsm(id1, term_t{1}, /* voted for */ server_id{}, std::move(log), fd, fsm_cfg);

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
    (void)fsm.get_output(); // Replication will not start until first get_output() call
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
    (void)fsm.get_output(); // causes dummy to be replicate
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
    (void)fsm.get_output(); // causes dummy to be replicate
    output = fsm.get_output();
    append = std::get<raft::append_request>(output.messages.back().second);
    idx = append.entries.back()->idx;
    fsm.step(id2, raft::append_reply{fsm.get_current_term(), idx, raft::append_reply::accepted{idx}});

    // Drop the leader from the current config and see that stepdown message is sent
    raft::configuration newcfg({raft::server_address{id2.id}, raft::server_address{id3.id, false}});
    fsm.add_entry(newcfg);
    (void)fsm.get_output(); // send it out
    output = fsm.get_output();
    append = std::get<raft::append_request>(output.messages.back().second);
    idx = append.entries.back()->idx;
    // Accept joint config entry on id2
    fsm.step(id2, raft::append_reply{fsm.get_current_term(), idx, raft::append_reply::accepted{idx}});
    // fms added new config to the log
    (void)fsm.get_output(); // send it out
    output = fsm.get_output();
    append = std::get<raft::append_request>(output.messages.back().second);
    idx = append.entries.back()->idx;
    // Accept new config entry on id2
    fsm.step(id2, raft::append_reply{fsm.get_current_term(), idx, raft::append_reply::accepted{idx}});

    // And check that the deposed leader sent timeout_now
    output = fsm.get_output();
    BOOST_CHECK_EQUAL(output.messages.size(), 1);
    BOOST_CHECK(std::holds_alternative<raft::timeout_now>(output.messages.back().second));
}
