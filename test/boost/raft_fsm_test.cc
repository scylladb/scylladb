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

#define BOOST_TEST_MODULE raft

#include <boost/test/unit_test.hpp>
#include "test/lib/log.hh"

#include "raft/fsm.hh"

using raft::term_t, raft::index_t, raft::server_id;
using raft::log_entry;
using seastar::make_lw_shared;

void election_threshold(raft::fsm& fsm) {
    for (int i = 0; i <= raft::ELECTION_TIMEOUT.count(); i++) {
        fsm.tick();
    }
}

void election_timeout(raft::fsm& fsm) {
    for (int i = 0; i <= 2 * raft::ELECTION_TIMEOUT.count(); i++) {
        fsm.tick();
    }
}

struct failure_detector: public raft::failure_detector {
    bool alive = true;
    bool is_alive(raft::server_id from) override {
        return alive;
    }
};

template <typename T> void add_entry(raft::log& log, T cmd) {
    log.emplace_back(make_lw_shared<log_entry>(log_entry{log.last_term(), log.next_idx(), cmd}));
}

raft::snapshot log_snapshot(raft::log& log, index_t idx) {
    return raft::snapshot{.idx = idx, .term = log.last_term(), .config = log.get_snapshot().config};
}

raft::fsm_config fsm_cfg{.append_request_threshold = 1};

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

BOOST_AUTO_TEST_CASE(test_election_single_node) {

    failure_detector fd;
    server_id id1{utils::make_random_uuid()};
    raft::configuration cfg({id1});
    raft::log log{raft::snapshot{.config = cfg}};
    raft::fsm fsm(id1, term_t{}, server_id{}, std::move(log), fd, fsm_cfg);

    BOOST_CHECK(fsm.is_follower());

    election_timeout(fsm);

    // Immediately converts from leader to follower if quorum=1
    BOOST_CHECK(fsm.is_leader());

    auto output = fsm.get_output();

    BOOST_CHECK(output.term);
    BOOST_CHECK(output.vote);
    BOOST_CHECK(output.messages.empty());
    // A new leader applies one dummy entry
    BOOST_CHECK(output.log_entries.size() == 1 && std::holds_alternative<raft::log_entry::dummy>(output.log_entries[0]->data));
    BOOST_CHECK(output.committed.empty());
    // The leader does not become candidate simply because
    // a timeout has elapsed, i.e. there are no spurious
    // elections.
    election_timeout(fsm);
    BOOST_CHECK(fsm.is_leader());
    output = fsm.get_output();
    BOOST_CHECK(!output.term);
    BOOST_CHECK(!output.vote);
    BOOST_CHECK(output.messages.empty());
    BOOST_CHECK(output.log_entries.empty());
    // Dummy entry is now commited
    BOOST_CHECK(output.committed.size() == 1 && std::holds_alternative<raft::log_entry::dummy>(output.committed[0]->data));
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
    // After a favourable reply, we become a leader (quorum is 2)
    fsm.step(id2, raft::vote_reply{output.term, true});
    BOOST_CHECK(fsm.is_leader());
    // Out of order response to the previous election is ignored
    fsm.step(id2, raft::vote_reply{output.term - term_t{1}, false});
    assert(fsm.is_leader());

    // Vote request within the election timeout is ignored
    // (avoiding disruptive leaders).
    fsm.step(id2, raft::vote_request{output.term + term_t{1}});
    BOOST_CHECK(fsm.is_leader());
    // Any message with a newer term after election timeout
    // -> immediately convert to follower
    fd.alive = false;
    election_threshold(fsm);
    // Use current_term + 2 to switch fsm to follower
    // even if it itself switched to a candidate
    fsm.step(id2, raft::vote_request{output.term + term_t{2}});
    BOOST_CHECK(fsm.is_follower());

    // Check that the candidate converts to a follower as well
    election_timeout(fsm);
    BOOST_CHECK(fsm.is_candidate());
    output = fsm.get_output();
    fsm.step(id2, raft::vote_request{output.term + term_t{1}});
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
    // Add a favourable reply, not enough for quorum
    fsm.step(id2, raft::vote_reply{output.term, true});
    BOOST_CHECK(fsm.is_candidate());

    // Add another one, this adds up to quorum
    fsm.step(id3, raft::vote_reply{output.term, true});
    BOOST_CHECK(fsm.is_leader());
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
    fsm.step(id2, raft::vote_reply{output.term, true});
    BOOST_CHECK(fsm.is_leader());

    output = fsm.get_output();
    // A new leader applies one dummy entry
    BOOST_CHECK(output.log_entries.size() == 1 && std::holds_alternative<raft::log_entry::dummy>(output.log_entries[0]->data));
    BOOST_CHECK(output.committed.empty());

    raft::configuration newcfg({id1, id2, id3});
    // Suggest a confchange.
    fsm.add_entry(newcfg);
    // Can't have two confchanges in progress.
    BOOST_CHECK_THROW(fsm.add_entry(newcfg), raft::conf_change_in_progress);
    // Entered joint configuration immediately.
    BOOST_CHECK(fsm.get_configuration().is_joint());
    BOOST_CHECK(fsm.get_configuration().previous.size() == 2);
    BOOST_CHECK(fsm.get_configuration().current.size() == 3);
    output = fsm.get_output();
    // The output contains a log entry to be committed.
    // Once it's committed, it will be replicated.
    // The output must contain messages both for id2 and id3
    BOOST_CHECK(output.log_entries.size() == 1);
    // Calling get_output() again indicates the previous output
    // is handled, i.e. the log entry is committed, so now
    // the leader will replicate the confchange
    output = fsm.get_output();
    // Append entry for id2
    BOOST_CHECK(output.messages.size() == 1);
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
    BOOST_CHECK(output.log_entries.size() == 1);
    output = fsm.get_output();
    // AppendEntries messages for the final configuration
    BOOST_CHECK(output.messages.size() >= 1);
    msg = std::get<raft::append_request>(output.messages.back().second);
    idx = msg.entries.back().get()->idx;
    // Ack AppendEntries for the final configuration
    fsm.step(id2, raft::append_reply{msg.current_term, idx, raft::append_reply::accepted{idx}});
    BOOST_CHECK(fsm.get_configuration().current.size() == 3);
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
    BOOST_CHECK(output.messages.size() == 2);
    BOOST_CHECK(std::holds_alternative<raft::vote_request>(output.messages[0].second));
    BOOST_CHECK(std::holds_alternative<raft::vote_request>(output.messages[1].second));

    fsm.step(id2, raft::vote_reply{output.term, true});
    BOOST_CHECK(fsm.is_leader());
    output = fsm.get_output();
    BOOST_CHECK(output.log_entries.size() == 1);
    BOOST_CHECK(std::holds_alternative<raft::log_entry::dummy>(output.log_entries[0]->data));

    raft::configuration newcfg({id1, id2});
    // Suggest a confchange.
    fsm.add_entry(newcfg);
    // Entered joint configuration immediately.
    BOOST_CHECK(fsm.get_configuration().is_joint());
    BOOST_CHECK(fsm.get_configuration().current.size() == 2);
    BOOST_CHECK(fsm.get_configuration().previous.size() == 3);
    output = fsm.get_output();
    // The output contains a log entry to be committed.
    // Once it's committed, it will be replicated.
    BOOST_CHECK(output.log_entries.size() == 1);
    BOOST_CHECK(std::holds_alternative<raft::configuration>(output.log_entries[0]->data));
    BOOST_CHECK(output.messages.size() == 2); // Configuration change sent to id2 and id3
    raft::append_request msg;
    BOOST_REQUIRE_NO_THROW(msg = std::get<raft::append_request>(output.messages[0].second));
    BOOST_CHECK(msg.entries.size() == 1);
    auto idx = msg.entries.back().get()->idx;
    // In order to accept a configuration change
    // we need one ACK, since there is a quorum overlap.
    fsm.step(id2, raft::append_reply{msg.current_term, idx, raft::append_reply::accepted{idx}});

    output = fsm.get_output();
    // AppendEntries messages for the final configuration
    BOOST_CHECK(output.messages.size() >= 1);

    BOOST_REQUIRE_NO_THROW(msg = std::get<raft::append_request>(output.messages[0].second));
    BOOST_CHECK(msg.entries.size() == 1);
    BOOST_CHECK(std::holds_alternative<raft::configuration>(msg.entries[0]->data));
    idx = msg.entries.back().get()->idx;
    BOOST_CHECK(idx == 102);
    // Ack AppendEntries for the joint configuration
    fsm.step(id2, raft::append_reply{msg.current_term, idx, raft::append_reply::accepted{idx}});

    output = fsm.get_output();
    // A log entry for the final configuration
    BOOST_CHECK(output.log_entries.size() == 1);
    BOOST_CHECK(std::holds_alternative<raft::configuration>(output.log_entries[0]->data));

    BOOST_CHECK(fsm.get_configuration().current.size() == 2);
    BOOST_CHECK(!fsm.get_configuration().is_joint());

    output = fsm.get_output();
    BOOST_CHECK(output.messages.size() == 1);
    BOOST_REQUIRE_NO_THROW(msg = std::get<raft::append_request>(output.messages[0].second));
    BOOST_CHECK(msg.entries.size() == 1);
    BOOST_CHECK(std::holds_alternative<raft::configuration>(msg.entries[0]->data));
    idx = msg.entries.back().get()->idx;
    BOOST_CHECK(idx == 103);
    // Ack AppendEntries for the final configuration
    fsm.step(id2, raft::append_reply{msg.current_term, idx, raft::append_reply::accepted{idx}});

    // Check that we can start a new confchange
    raft::configuration newcfg2({id1, id2, id3});
    fsm.add_entry(newcfg);
    BOOST_CHECK(fsm.get_configuration().current.size() == 2);
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
    fsm.step(id2, raft::vote_reply{output.term, true});
    BOOST_CHECK(fsm.is_leader());
    output = fsm.get_output();
    BOOST_CHECK(output.log_entries.size() == 1 && std::holds_alternative<raft::log_entry::dummy>(output.log_entries[0]->data));
    BOOST_CHECK(output.committed.empty());

    raft::configuration newcfg({id1, id2, id4});
    // Suggest a confchange.
    fsm.add_entry(newcfg);
    // Entered joint configuration immediately.
    BOOST_CHECK(fsm.get_configuration().is_joint());
    BOOST_CHECK(fsm.get_configuration().current.size() == 3);
    BOOST_CHECK(fsm.get_configuration().previous.size() == 3);
    output = fsm.get_output();
    BOOST_CHECK(output.messages.size() == 2);
    raft::append_request msg;
    BOOST_REQUIRE_NO_THROW(msg = std::get<raft::append_request>(output.messages[0].second));
    BOOST_CHECK(std::holds_alternative<raft::log_entry::dummy>(msg.entries[0]->data));
    BOOST_REQUIRE_NO_THROW(msg = std::get<raft::append_request>(output.messages[1].second));
    BOOST_CHECK(std::holds_alternative<raft::log_entry::dummy>(msg.entries[0]->data));

    output = fsm.get_output();
    BOOST_REQUIRE_NO_THROW(msg = std::get<raft::append_request>(output.messages[0].second));
    auto idx = msg.entries.back().get()->idx;
    // In order to accept a configuration change
    // we need two ACK, since there is a quorum overlap.
    fsm.step(id2, raft::append_reply{msg.current_term, idx, raft::append_reply::accepted{idx}});
    BOOST_CHECK(!fsm.get_configuration().is_joint());
    // Joint config to log
    output = fsm.get_output();
    BOOST_CHECK(output.log_entries.size() == 1);
    BOOST_CHECK(std::holds_alternative<raft::configuration>(output.log_entries[0]->data));
    output = fsm.get_output();
    // AppendEntries messages for the final configuration
    BOOST_CHECK(output.messages.size() >= 1);
    msg = std::get<raft::append_request>(output.messages.back().second);
    idx = msg.entries.back().get()->idx;
    // Ack AppendEntries for the final configuration
    fsm.step(id2, raft::append_reply{msg.current_term, idx, raft::append_reply::accepted{idx}});
    BOOST_CHECK(fsm.get_configuration().current.size() == 3);
    BOOST_CHECK(!fsm.get_configuration().is_joint());
}
