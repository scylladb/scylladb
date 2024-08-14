/*
 * Copyright 2015 The etcd Authors
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

// Port of etcd Raft implementation unit tests

#define BOOST_TEST_MODULE raft

#include "test/raft/helpers.hh"

using namespace raft;

// TestProgressLeader
// Checks a leader's own progress is updated correctly as entries are added.
BOOST_AUTO_TEST_CASE(test_progress_leader) {

    server_id id1{utils::UUID(0, 1)}, id2{utils::UUID(0, 2)};

    raft::configuration cfg = config_from_ids({id1, id2}); // 2 nodes
    raft::log log{raft::snapshot_descriptor{.config = cfg}};

    fsm_debug fsm(id1, term_t{}, server_id{}, std::move(log), trivial_failure_detector, fsm_cfg);

    election_timeout(fsm);
    BOOST_CHECK(fsm.is_candidate());

    auto output = fsm.get_output();
    BOOST_CHECK(output.term_and_vote);
    fsm.step(id2, raft::vote_reply{output.term_and_vote->first, true});
    BOOST_CHECK(fsm.is_leader());

    // Dummy entry local
    output = fsm.get_output();
    BOOST_CHECK(output.log_entries.size() == 1);
    BOOST_CHECK(output.messages.size() == 1);
    BOOST_CHECK(std::holds_alternative<raft::log_entry::dummy>(output.log_entries[0]->data));
    // accept dummy entry
    auto msg = std::get<raft::append_request>(output.messages.back().second);
    auto idx = msg.entries.back()->idx;
    fsm.step(id2, raft::append_reply{msg.current_term, idx, raft::append_reply::accepted{idx}});

    const raft::follower_progress& fprogress = fsm.get_progress(id1);

    for (unsigned i = 1; i < 6; ++i) {
        // NOTE: in etcd leader's own progress seems to be PIPELINE
        BOOST_CHECK(fprogress.state == raft::follower_progress::state::PROBE);
        BOOST_CHECK(fprogress.match_idx == index_t{i});
        BOOST_CHECK(fprogress.next_idx == index_t{i + 1});

        raft::command cmd = create_command(i);
        raft::log_entry le = fsm.add_entry(std::move(cmd));

        output = fsm.get_output();

        msg = std::get<raft::append_request>(output.messages.back().second);
        idx = msg.entries.back()->idx;
        BOOST_CHECK_EQUAL(idx, index_t{i + 1});
        fsm.step(id2, raft::append_reply{msg.current_term, idx, raft::append_reply::accepted{idx}});
    }
}

// TestProgressResumeByHeartbeatResp
// TBD once we have failure_detector.is_alive() heartbeat

// Similar but with append reply
BOOST_AUTO_TEST_CASE(test_progress_resume_by_append_resp) {

    server_id id1{utils::UUID(0, 1)}, id2{utils::UUID(0, 2)};

    raft::configuration cfg = config_from_ids({id1, id2}); // 2 nodes
    raft::log log{raft::snapshot_descriptor{.config = cfg}};

    fsm_debug fsm(id1, term_t{}, server_id{}, std::move(log), trivial_failure_detector, fsm_cfg);

    election_timeout(fsm);
    auto output = fsm.get_output();
    BOOST_CHECK(output.term_and_vote);
    fsm.step(id2, raft::vote_reply{output.term_and_vote->first, true});
    BOOST_CHECK(fsm.is_leader());

    const raft::follower_progress& fprogress = fsm.get_progress(id2);
    BOOST_CHECK(fprogress.state == raft::follower_progress::state::PROBE);

    const raft::follower_progress& fprogress2 = fsm.get_progress(id2);
    // get dummy entry
    output = fsm.get_output();
    BOOST_CHECK(fprogress2.probe_sent); // dummy entry
    BOOST_CHECK_EQUAL(output.messages.size(), 1);
    auto dummy = std::get<raft::append_request>(output.messages.back().second);

    raft::command cmd = create_command(1);
    raft::log_entry le = fsm.add_entry(std::move(cmd));
    output = fsm.get_output();
    BOOST_CHECK(output.messages.empty());

    // ack dummy entry
    fsm.step(id2, raft::append_reply{dummy.current_term, dummy.entries[0]->idx, raft::append_reply::accepted{dummy.entries[0]->idx}});

    // After the ack mode becomes pipeline and ssending resumes
    BOOST_CHECK(fprogress.state == raft::follower_progress::state::PIPELINE);
    output = fsm.get_output();
    BOOST_CHECK_EQUAL(output.messages.size(), 1);
}

// TestProgressPaused
BOOST_AUTO_TEST_CASE(test_progress_paused) {

    server_id id1{utils::UUID(0, 1)}, id2{utils::UUID(0, 2)};
    raft::configuration cfg = config_from_ids({id1, id2});
    raft::log log{raft::snapshot_descriptor{.config = cfg}};

    fsm_debug fsm(id1, term_t{}, server_id{}, std::move(log), trivial_failure_detector, fsm_cfg);

    election_timeout(fsm);
    auto output = fsm.get_output();
    BOOST_CHECK(output.term_and_vote);
    auto current_term = output.term_and_vote->first;
    fsm.step(id2, raft::vote_reply{current_term, true});
    BOOST_CHECK(fsm.is_leader());

    fsm.add_entry(create_command(1));
    fsm.add_entry(create_command(2));
    fsm.add_entry(create_command(3));
    output = fsm.get_output();
    BOOST_CHECK_EQUAL(output.messages.size(), 1);
}

// TestProgressFlowControl
BOOST_AUTO_TEST_CASE(test_progress_flow_control) {

    server_id id1{utils::UUID(0, 1)}, id2{utils::UUID(0, 2)};
    raft::configuration cfg = config_from_ids({id1, id2}); // 2 nodes
    raft::log log{raft::snapshot_descriptor{.config = cfg}};

    // Fit 2 x 1000 sized blobs
    raft::fsm_config fsm_cfg_8{.append_request_threshold = 2048};   // Threshold 2k
    fsm_debug fsm(id1, term_t{}, server_id{}, std::move(log), trivial_failure_detector, fsm_cfg_8);

    election_timeout(fsm);
    auto output = fsm.get_output();
    BOOST_CHECK(output.term_and_vote);
    auto current_term = output.term_and_vote->first;
    fsm.step(id2, raft::vote_reply{current_term, true});
    BOOST_CHECK(fsm.is_leader());

    fsm.step(id2, raft::vote_reply{current_term, true});
    const raft::follower_progress& fprogress = fsm.get_progress(id2);
    BOOST_CHECK(fprogress.state == raft::follower_progress::state::PROBE);

    // While node 2 is in probe state, propose a bunch of entries.
    sstring blob(1000, 'a');
    raft::command cmd_blob = create_command(blob);
    for (auto i = 0; i < 10; ++i) {
        fsm.add_entry(cmd_blob);
    }
    output = fsm.get_output();

    BOOST_CHECK(output.messages.size() == 1);

    raft::append_request msg;
    BOOST_REQUIRE_NO_THROW(msg = std::get<raft::append_request>(output.messages.back().second));
    // The first proposal (only one proposal gets sent because follower is in probe state)
    // Also, the first append request by new leader is dummy
    BOOST_CHECK(msg.entries.size() == 1);
    const raft::log_entry_ptr le = msg.entries.back();
    size_t current_entry = 0;
    BOOST_CHECK(le->idx == index_t{++current_entry});
    BOOST_REQUIRE_NO_THROW(std::get<raft::log_entry::dummy>(le->data));

    // When this append is acked, we change to replicate state and can
    // send multiple messages at once. (PIPELINE)
    fsm.step(id2, raft::append_reply{msg.current_term, le->idx, raft::append_reply::accepted{le->idx}});
    const raft::follower_progress& fprogress2 = fsm.get_progress(id2);
    BOOST_CHECK(fprogress2.state == raft::follower_progress::state::PIPELINE);

    output = fsm.get_output();
    // 10 entries: first in 1 msg, then 10 remaining 2 per msg = 5
    BOOST_CHECK(output.messages.size() == 5);

    size_t committed = 1;
    BOOST_CHECK(output.committed.size() == 1);

    for (size_t i = 0; i < output.messages.size(); ++i) {
        raft::append_request msg;
        BOOST_REQUIRE_NO_THROW(msg = std::get<raft::append_request>(output.messages[i].second));
        BOOST_CHECK(msg.entries.size() == 2);
        for (size_t m = 0; m < msg.entries.size(); ++m) {
            const raft::log_entry_ptr le = msg.entries[m];
            BOOST_CHECK(le->idx == index_t{++current_entry});
            raft::command cmd;
            BOOST_REQUIRE_NO_THROW(cmd = std::get<raft::command>(le->data));
            BOOST_CHECK(cmd.size() == cmd_blob.size());
        }
    }

    raft::index_t ack_idx{current_entry}; // Acknowledge all entries at once
    fsm.step(id2, raft::append_reply{msg.current_term, ack_idx, raft::append_reply::accepted{ack_idx}});
    output = fsm.get_output();
    // Check fsm outputs the remaining 10 entries
    committed = current_entry - committed;
    BOOST_CHECK(output.committed.size() == committed);
}

// TestUncommittedEntryLimit
// TBD once we have this configuration feature implemented
// TestLearnerElectionTimeout
// TBD once we have learner state implemented (and add_server)

// TestLeaderElectionOverwriteNewerLogs tests a scenario in which a
// newly-elected leader does *not* have the newest (i.e. highest term)
// log entries, and must overwrite higher-term log entries with
// lower-term ones.
// NOTE: code actually overwrites entries with higher term
// TODO: add pre-vote case
BOOST_AUTO_TEST_CASE(test_leader_election_overwrite_newer_logs) {
    raft::fsm_output output1, output2, output3, output4, output5;
    raft::vote_request vreq;

    discrete_failure_detector fd;

    // 5 nodes
    server_id id1{utils::UUID(0, 1)}, id2{utils::UUID(0, 2)}, id3{utils::UUID(0, 3)},
            id4{utils::UUID(0, 4)}, id5{utils::UUID(0, 5)};
    raft::configuration cfg = config_from_ids({id1, id2, id3, id4, id5});
    //                        idx  term
    // node 1:    log entries  1:   1                 won first got logs
    // node 2:    log entries  1:   1                 got logs from node 1
    // node 3:    log entries  1:   2                 wond second election
    // node 4:    log entries  (at term 2 voted for 3)
    // node 5:    log entries  (at term 2 voted for 3)
    raft::log_entry_ptr lep1 = seastar::make_lw_shared<log_entry>(log_entry{term_t{1}, index_t{1}, raft::log_entry::dummy{}});
    raft::log log1{raft::snapshot_descriptor{.config = cfg}, raft::log_entries{lep1}};
    fsm_debug fsm1(id1, term_t{1}, server_id{}, std::move(log1), fd, fsm_cfg);
    raft::log_entry_ptr lep2 = seastar::make_lw_shared<log_entry>(log_entry{term_t{1}, index_t{1}, raft::log_entry::dummy{}});
    raft::log log2{raft::snapshot_descriptor{.config = cfg}, raft::log_entries{lep2}};
    fsm_debug fsm2(id2, term_t{1}, server_id{}, std::move(log2), fd, fsm_cfg);

    raft::log_entry_ptr lep3 = seastar::make_lw_shared<log_entry>(log_entry{term_t{2}, index_t{1}, raft::log_entry::dummy{}});
    raft::log log3{raft::snapshot_descriptor{.config = cfg}, raft::log_entries{lep3}};
    fsm_debug fsm3(id3, term_t{2}, server_id{id3}, std::move(log3), fd, fsm_cfg);

    raft::log log4{raft::snapshot_descriptor{.config = cfg}};
    fsm_debug fsm4(id4, term_t{2}, server_id{id3}, std::move(log4), fd, fsm_cfg);
    raft::log log5{raft::snapshot_descriptor{.config = cfg}};
    fsm_debug fsm5(id5, term_t{2}, server_id{id3}, std::move(log5), fd, fsm_cfg);

    fsm2.become_follower(id1);
    fsm4.become_follower(id3);
    fsm5.become_follower(id3);

    // Node 1 is reconnected and campaigns. The election fails because a quorum of nodes
    // know about the election that already happened at term 2. Node 1's term is pushed ahead to 2.
    fd.mark_dead(id3);
    make_candidate(fsm1);                     // XXX change to make_candidate() to be sure term=2
    BOOST_CHECK(fsm1.is_candidate());
    BOOST_CHECK(fsm1.get_current_term() == term_t{2});
    election_threshold(fsm2);
    election_threshold(fsm3);
    election_threshold(fsm4);
    election_threshold(fsm5);
    communicate(fsm1, fsm2, fsm3, fsm4, fsm5);
    BOOST_CHECK(fsm1.is_follower());  // Rejected


    // Node 1 campaigns again with a higher term. This time it succeeds.
    make_candidate(fsm1);
    communicate(fsm1, fsm2, fsm3, fsm4, fsm5);

    // BOOST_CHECK(compare_log_entries(fsm1.get_log(), fsm3.get_log(), index_t{1}, index_t{2}));
    BOOST_CHECK(compare_log_entries(fsm1.get_log(), fsm3.get_log(), index_t{1}, index_t{2}));
    BOOST_CHECK(compare_log_entries(fsm1.get_log(), fsm4.get_log(), index_t{1}, index_t{2}));
    BOOST_CHECK(compare_log_entries(fsm1.get_log(), fsm5.get_log(), index_t{1}, index_t{2}));
}

// TestVoteFromAnyState
BOOST_AUTO_TEST_CASE(test_vote_from_any_state) {

    discrete_failure_detector fd;

    server_id id1{utils::UUID(0, 1)}, id2{utils::UUID(0, 2)}, id3{utils::UUID(0, 3)};
    raft::configuration cfg = config_from_ids({id1, id2, id3});
    raft::log log{raft::snapshot_descriptor{.config = cfg}};
    fsm_debug fsm(id1, term_t{}, server_id{}, std::move(log), fd, fsm_cfg);

    // Follower
    BOOST_CHECK(fsm.is_follower());
    fsm.step(id2, raft::vote_request{term_t{1}, index_t{1}, term_t{1}});
    auto output = fsm.get_output();
    BOOST_CHECK(output.messages.size() == 1);
    raft::vote_reply vrepl;
    BOOST_REQUIRE_NO_THROW(vrepl = std::get<raft::vote_reply>(output.messages.back().second));
    BOOST_CHECK(vrepl.vote_granted);
    BOOST_CHECK(vrepl.current_term = term_t{1});

    // TODO: pre candidate when (if) implemented

    // Candidate
    BOOST_CHECK(!fsm.is_candidate());
    election_timeout(fsm);
    BOOST_CHECK(fsm.is_candidate());
    output = fsm.get_output();
    // Node 2 requests vote for a later term
    BOOST_CHECK(output.term_and_vote);
    fsm.step(id2, raft::vote_request{output.term_and_vote->first + term_t{1}, index_t{1}, term_t{1}});
    output = fsm.get_output();
    BOOST_CHECK(output.messages.size() == 1);
    BOOST_REQUIRE_NO_THROW(vrepl = std::get<raft::vote_reply>(output.messages.back().second));
    BOOST_CHECK(vrepl.vote_granted);
    BOOST_CHECK(vrepl.current_term = term_t{1});

    // Leader
    election_timeout(fsm);
    BOOST_CHECK(fsm.is_candidate());
    output = fsm.get_output();
    BOOST_CHECK(output.term_and_vote);
    term_t current_term = output.term_and_vote->first;
    fsm.step(id2, raft::vote_reply{current_term, true});
    BOOST_CHECK(fsm.is_leader());
    output = fsm.get_output();
    // Node 2 requests vote for a later term
    fd.mark_all_dead();
    election_threshold(fsm);
    output = fsm.get_output();
    fsm.step(id2, raft::vote_request{current_term + term_t{2}, index_t{42}, current_term + term_t{1}});
    output = fsm.get_output();
    BOOST_CHECK(fsm.is_follower());
    BOOST_CHECK(output.messages.size() == 1);
    BOOST_CHECK(vrepl.vote_granted);
    BOOST_CHECK(vrepl.current_term = term_t{1});
}

// TODO: TestPreVoteFromAnyState

// TestLogReplication
BOOST_AUTO_TEST_CASE(test_log_replication_1) {
    raft::fsm_output output;
    raft::append_request areq;
    raft::log_entry_ptr lep;
    raft::term_t current_term;
    raft::index_t idx;

    server_id id1{utils::UUID(0, 1)}, id2{utils::UUID(0, 2)}, id3{utils::UUID(0, 3)};
    raft::configuration cfg = config_from_ids({id1, id2, id3});
    raft::log log{raft::snapshot_descriptor{.config = cfg}};
    fsm_debug fsm(id1, term_t{}, server_id{}, std::move(log), trivial_failure_detector, fsm_cfg);

    election_timeout(fsm);
    BOOST_CHECK(fsm.is_candidate());
    output = fsm.get_output();
    BOOST_CHECK(output.term_and_vote);
    current_term = output.term_and_vote->first;
    BOOST_CHECK(current_term != term_t{0});
    fsm.step(id2, raft::vote_reply{current_term, true});
    BOOST_CHECK(fsm.is_leader());
    output = fsm.get_output();
    BOOST_CHECK(output.log_entries.size() == 1);
    BOOST_CHECK(std::holds_alternative<raft::log_entry::dummy>(output.log_entries[0]->data));
    BOOST_CHECK(output.committed.size() == 0);
    BOOST_CHECK(output.messages.size() == 2);
    index_t dummy_idx{1};     // Nothing before it
    for (auto& [id, msg] : output.messages) {
        BOOST_REQUIRE_NO_THROW(areq = std::get<raft::append_request>(msg));
        BOOST_CHECK(areq.prev_log_idx == index_t{0});
        BOOST_CHECK(areq.prev_log_term == term_t{0});
        BOOST_CHECK(areq.entries.size() == 1);
        lep =  areq.entries.back();
        BOOST_CHECK(lep->idx == dummy_idx);
        BOOST_CHECK(lep->term == current_term);
        BOOST_CHECK(std::holds_alternative<raft::log_entry::dummy>(lep->data));
        // Reply
        fsm.step(id, raft::append_reply{areq.current_term, dummy_idx, raft::append_reply::accepted{dummy_idx}});
    }
    output = fsm.get_output();
    BOOST_CHECK(output.committed.size() == 1);  // Dummy was committed

    // Add data entry
    raft::command cmd = create_command(1);
    fsm.add_entry(std::move(cmd));
    output = fsm.get_output();
    BOOST_CHECK(output.log_entries.size() == 1); // Entry added to local log
    BOOST_CHECK(output.messages.size() == 2);
    index_t entry_idx{2};     // Nothing before it
    for (auto& [id, msg] : output.messages) {
        BOOST_REQUIRE_NO_THROW(areq = std::get<raft::append_request>(msg));
        BOOST_CHECK(areq.prev_log_idx == index_t{1});
        BOOST_CHECK(areq.prev_log_term == current_term);
        BOOST_CHECK(areq.entries.size() == 1);
        lep =  areq.entries.back();
        BOOST_CHECK(lep->idx == entry_idx);
        BOOST_CHECK(lep->term == current_term);
        BOOST_CHECK(std::holds_alternative<raft::command>(lep->data));
        idx = lep->idx;
        // Reply
        fsm.step(id, raft::append_reply{areq.current_term, lep->idx, raft::append_reply::accepted{idx}});
    }
    output = fsm.get_output();
    BOOST_CHECK(output.committed.size() == 1);  // Entry was committed
}

BOOST_AUTO_TEST_CASE(test_log_replication_2) {
    raft::fsm_output output;
    raft::append_request areq;
    raft::log_entry_ptr lep;
    raft::term_t current_term;
    raft::index_t idx;

    server_id id1{utils::UUID(0, 1)}, id2{utils::UUID(0, 2)}, id3{utils::UUID(0, 3)};
    raft::configuration cfg = config_from_ids({id1, id2, id3});
    raft::log log{raft::snapshot_descriptor{.config = cfg}};
    fsm_debug fsm(id1, term_t{}, server_id{}, std::move(log), trivial_failure_detector, fsm_cfg);

    election_timeout(fsm);
    output = fsm.get_output();
    BOOST_CHECK(output.term_and_vote);
    current_term = output.term_and_vote->first;
    fsm.step(id2, raft::vote_reply{current_term, true});
    BOOST_CHECK(fsm.is_leader());
    output = fsm.get_output();
    BOOST_CHECK(output.messages.size() == 2);
    index_t dummy_idx{1};     // Nothing before it
    for (auto& [id, msg] : output.messages) {
        BOOST_REQUIRE_NO_THROW(areq = std::get<raft::append_request>(msg));
        fsm.step(id, raft::append_reply{areq.current_term, dummy_idx, raft::append_reply::accepted{dummy_idx}});
    }
    output = fsm.get_output();
    BOOST_CHECK(output.committed.size() == 1);  // Dummy was committed

    // Add 1st data entry
    raft::command cmd = create_command(1);
    fsm.add_entry(std::move(cmd));
    output = fsm.get_output();
    BOOST_CHECK(output.log_entries.size() == 1); // Entry added to local log
    BOOST_CHECK(output.messages.size() == 2);
    // ACK 1st entry
    fsm.step(id2, raft::append_reply{current_term, index_t{2}, raft::append_reply::accepted{index_t{2}}});
    output = fsm.get_output();
    BOOST_CHECK(output.committed.size() == 1);  // Entry 1 was committed

    // Add 2nd data entry
    cmd = create_command(2);
    fsm.add_entry(std::move(cmd));
    output = fsm.get_output();
    BOOST_CHECK(output.log_entries.size() == 1); // Entry added to local log
    BOOST_CHECK(output.messages.size() == 2);
    // ACK 2nd entry
    index_t second_idx{3};
    for (auto& [id, msg] : output.messages) {
        BOOST_REQUIRE_NO_THROW(areq = std::get<raft::append_request>(msg));
        BOOST_CHECK(areq.prev_log_idx == index_t{2});
        BOOST_CHECK(areq.prev_log_term == current_term);
        BOOST_CHECK(areq.entries.size() == 1);
        lep =  areq.entries.back();
        BOOST_CHECK(lep->idx == second_idx);
        BOOST_CHECK(lep->term == current_term);
        // Reply
        fsm.step(id, raft::append_reply{areq.current_term, second_idx, raft::append_reply::accepted{second_idx}});
    }
    output = fsm.get_output();
    BOOST_CHECK(output.committed.size() == 1);  // Entry 2 was committed
}

// TestSingleNodeCommit
BOOST_AUTO_TEST_CASE(test_single_node_commit) {
    raft::fsm_output output;
    raft::log_entry_ptr lep;

    server_id id1{utils::UUID(0, 1)};
    raft::configuration cfg = config_from_ids({id1});
    raft::log log{raft::snapshot_descriptor{.config = cfg}};
    fsm_debug fsm(id1, term_t{}, server_id{}, std::move(log), trivial_failure_detector, fsm_cfg);

    BOOST_CHECK(fsm.is_leader());  // Single node skips candidate state
    output = fsm.get_output();
    BOOST_CHECK(output.log_entries.size() == 1);
    lep = output.log_entries.back();
    BOOST_REQUIRE_NO_THROW(std::get<raft::log_entry::dummy>(lep->data));
    output = fsm.get_output();
    BOOST_CHECK(output.committed.size() == 1);  // Dummy was committed

    // Add 1st data entry
    raft::command cmd = create_command(1);
    fsm.add_entry(std::move(cmd));
    output = fsm.get_output();
    BOOST_CHECK(output.log_entries.size() == 1); // Entry added to local log
    output = fsm.get_output();
    BOOST_CHECK(output.committed.size() == 1);  // Entry 1 was committed

    // Add 2nd data entry
    cmd = create_command(2);
    fsm.add_entry(std::move(cmd));
    output = fsm.get_output();
    BOOST_CHECK(output.log_entries.size() == 1); // Entry added to local log
    output = fsm.get_output();
    BOOST_CHECK(output.committed.size() == 1);  // Entry 2 was committed  (3 total)
}

// TODO: rewrite with communicate and filter append message
// TestCannotCommitWithoutNewTermEntry
BOOST_AUTO_TEST_CASE(test_cannot_commit_without_new_term_entry) {

    discrete_failure_detector fd;

    server_id id1{utils::UUID(0, 1)}, id2{utils::UUID(0, 2)}, id3{utils::UUID(0, 3)},
              id4{utils::UUID(0, 4)}, id5{utils::UUID(0, 5)};
    raft::configuration cfg = config_from_ids({id1, id2, id3, id4, id5});
    raft::log log1{raft::snapshot_descriptor{.config = cfg}};
    fsm_debug fsm1(id1, term_t{}, server_id{}, std::move(log1), fd, fsm_cfg);
    raft::log log2{raft::snapshot_descriptor{.config = cfg}};
    fsm_debug fsm2(id2, term_t{}, server_id{}, std::move(log2), fd, fsm_cfg);
    raft::log log3{raft::snapshot_descriptor{.config = cfg}};
    fsm_debug fsm3(id3, term_t{}, server_id{}, std::move(log3), fd, fsm_cfg);
    raft::log log4{raft::snapshot_descriptor{.config = cfg}};
    fsm_debug fsm4(id4, term_t{}, server_id{}, std::move(log4), fd, fsm_cfg);
    raft::log log5{raft::snapshot_descriptor{.config = cfg}};
    fsm_debug fsm5(id5, term_t{}, server_id{}, std::move(log5), fd, fsm_cfg);

    make_candidate(fsm1);
    communicate(fsm1, fsm2, fsm3, fsm4, fsm5);
    BOOST_CHECK(fsm1.is_leader());
    communicate(fsm1, fsm2, fsm3, fsm4, fsm5);

    // fsm1 now is "disconnected" from 3, 4, 5 (but not fsm2)

    // add 2 entries to fsm1  (2, 3)
    for (int i = 2; i < 4; ++i) {
        raft::command cmd = create_command(i);
        fsm1.add_entry(std::move(cmd));
    }
    // fsm2 gets these 2 entries, but 3, 4, 5 don't get append request
    communicate(fsm1, fsm2);

    // elect 2 as the new leader with term 2
    // after append a ChangeTerm entry from the current term, all entries
    // should be committed
    fd.mark_dead(id1);
    make_candidate(fsm2);
    election_threshold(fsm3);
    election_threshold(fsm4);
    election_threshold(fsm5);
    BOOST_CHECK(fsm2.is_candidate());
    communicate(fsm2, fsm1, fsm3, fsm4, fsm5);
    fd.mark_alive(id1);
    BOOST_CHECK(fsm2.is_leader());
    // add 1 entries to fsm2  (2, 3)
    raft::command cmd = create_command(5);
    fsm2.add_entry(std::move(cmd));
    communicate(fsm2, fsm1, fsm3, fsm4, fsm5);
    BOOST_CHECK(compare_log_entries(fsm2.get_log(), fsm1.get_log(), index_t{1}, index_t{5}));
    BOOST_CHECK(compare_log_entries(fsm2.get_log(), fsm3.get_log(), index_t{1}, index_t{5}));
    BOOST_CHECK(compare_log_entries(fsm2.get_log(), fsm4.get_log(), index_t{1}, index_t{5}));
    BOOST_CHECK(compare_log_entries(fsm2.get_log(), fsm5.get_log(), index_t{1}, index_t{5}));
}

// TODO TestCommitWithoutNewTermEntry tests the entries could be committed
// when leader changes, no new proposal comes in.

// TestDuelingCandidates
BOOST_AUTO_TEST_CASE(test_dueling_candidates) {

    server_id id1{utils::UUID(0, 1)}, id2{utils::UUID(0, 2)}, id3{utils::UUID(0, 3)};
    raft::configuration cfg = config_from_ids({id1, id2, id3});
    raft::log log1{raft::snapshot_descriptor{.config = cfg}};
    fsm_debug fsm1(id1, term_t{}, server_id{}, std::move(log1), trivial_failure_detector, fsm_cfg);
    raft::log log2{raft::snapshot_descriptor{.config = cfg}};
    fsm_debug fsm2(id2, term_t{}, server_id{}, std::move(log2), trivial_failure_detector, fsm_cfg);
    raft::log log3{raft::snapshot_descriptor{.config = cfg}};
    fsm_debug fsm3(id3, term_t{}, server_id{}, std::move(log3), trivial_failure_detector, fsm_cfg);

    // fsm1 and fsm3 don't see each other
    make_candidate(fsm1);
    make_candidate(fsm3);

    communicate(fsm1, fsm2);
    BOOST_CHECK(fsm1.is_leader());

    BOOST_CHECK(fsm3.is_candidate());
    // fsm1 doesn't see the vote request and fsm2 rejects vote
    communicate(fsm3, fsm2);
    // 3 stays as candidate since it receives a vote from 3 and a rejection from 2
    BOOST_CHECK(fsm3.is_candidate());

    // recover (now 1 and 3 see each other)

    // candidate 3 now increases its term and tries to vote again
    // we expect it to disrupt the leader 1 since it has a higher term
    // 3 will be follower again since both 1 and 2 rejects its vote
    // request since 3 does not have a long enough log
    // NOTE: this is not the case, 1 rejects due to election timeout
    election_timeout(fsm3);
    communicate(fsm3, fsm1, fsm2);
    // NOTE: in our implementation term does not get bumped to 2 and 1 stays leader
    BOOST_CHECK(fsm1.log_last_idx() == index_t{1});
    BOOST_CHECK(fsm2.log_last_idx() == index_t{1});
    BOOST_CHECK(fsm3.log_last_idx() == index_t{0});
    BOOST_CHECK(fsm1.log_last_term() == term_t{1});
    BOOST_CHECK(fsm2.log_last_term() == term_t{1});
    BOOST_CHECK(fsm3.log_last_term() == term_t{0});
}

// TestDuelingPreCandidates
BOOST_AUTO_TEST_CASE(test_dueling_pre_candidates) {

    server_id id1{utils::UUID(0, 1)}, id2{utils::UUID(0, 2)}, id3{utils::UUID(0, 3)};
    raft::configuration cfg = config_from_ids({id1, id2, id3});
    raft::log log1{raft::snapshot_descriptor{.config = cfg}};
    fsm_debug fsm1(id1, term_t{}, server_id{}, std::move(log1), trivial_failure_detector, fsm_cfg_pre);
    raft::log log2{raft::snapshot_descriptor{.config = cfg}};
    fsm_debug fsm2(id2, term_t{}, server_id{}, std::move(log2), trivial_failure_detector, fsm_cfg_pre);
    raft::log log3{raft::snapshot_descriptor{.config = cfg}};
    fsm_debug fsm3(id3, term_t{}, server_id{}, std::move(log3), trivial_failure_detector, fsm_cfg_pre);

    // fsm1 and fsm3 don't see each other
    make_candidate(fsm1);
    make_candidate(fsm3);

    communicate(fsm1, fsm2);
    // 1 becomes leader since it receives votes from 1 and 2
    BOOST_CHECK(fsm1.is_leader());

    // 3 campaigns then reverts to follower when its PreVote is rejected
    BOOST_CHECK(fsm3.is_candidate());
    communicate(fsm3, fsm2);
    BOOST_CHECK(fsm3.is_follower());

    // network recovers, now 1 and 3 see each other (1 will reject prevote req)

    // Candidate 3 now increases its term and tries to vote again.
    // With PreVote, it does not disrupt the leader.
    election_timeout(fsm3);
    BOOST_CHECK(fsm3.is_candidate());
    communicate(fsm3, fsm1, fsm2);
    BOOST_CHECK(fsm1.log_last_idx() == index_t{1}); BOOST_CHECK(fsm1.log_last_term() == term_t{1});
    BOOST_CHECK(fsm2.log_last_idx() == index_t{1}); BOOST_CHECK(fsm2.log_last_term() == term_t{1});
    BOOST_CHECK(fsm3.log_last_idx() == index_t{0}); BOOST_CHECK(fsm3.log_last_term() == term_t{0});
    BOOST_CHECK(fsm1.get_current_term() == term_t{1});
    BOOST_CHECK(fsm2.get_current_term() == term_t{1});
    BOOST_CHECK(fsm3.get_current_term() == term_t{1});
}

// TestCandidateConcede
// TBD once we have heartbeat

// TestSingleNodeCandidate (fsm test)

// TestSingleNodePreCandidate
BOOST_AUTO_TEST_CASE(test_single_node_pre_candidate) {
    raft::fsm_output output1;

    server_id id1{utils::UUID(0, 1)};
    raft::configuration cfg = config_from_ids({id1});
    raft::log log1{raft::snapshot_descriptor{.config = cfg}};
    fsm_debug fsm1(id1, term_t{}, server_id{}, std::move(log1), trivial_failure_detector, fsm_cfg_pre);

    BOOST_CHECK(fsm1.is_leader());
}

// TestOldMessages
BOOST_AUTO_TEST_CASE(test_old_messages) {

    discrete_failure_detector fd;
    server_id id1{utils::UUID(0, 1)}, id2{utils::UUID(0, 2)}, id3{utils::UUID(0, 3)};
    raft::configuration cfg = config_from_ids({id1, id2, id3});
    raft::log log1{raft::snapshot_descriptor{.config = cfg}};
    fsm_debug fsm1(id1, term_t{}, server_id{}, std::move(log1), fd, fsm_cfg);
    raft::log log2{raft::snapshot_descriptor{.config = cfg}};
    fsm_debug fsm2(id2, term_t{}, server_id{}, std::move(log2), fd, fsm_cfg);
    raft::log log3{raft::snapshot_descriptor{.config = cfg}};
    fsm_debug fsm3(id3, term_t{}, server_id{}, std::move(log3), fd, fsm_cfg);

    make_candidate(fsm1);
    communicate(fsm1, fsm2, fsm3);  // Term 1
    BOOST_CHECK(fsm1.is_leader());
    fd.mark_dead(id1);
    election_threshold(fsm3);
    make_candidate(fsm2);
    BOOST_CHECK(fsm2.is_candidate());
    communicate(fsm2, fsm1, fsm3);  // Term 2
    fd.mark_alive(id1);
    BOOST_CHECK(fsm2.is_leader());
    fd.mark_dead(id2);
    election_threshold(fsm3);
    make_candidate(fsm1);
    communicate(fsm1, fsm2, fsm3);  // Term 3
    BOOST_CHECK(fsm1.is_leader());
    fd.mark_alive(id2);
    BOOST_CHECK(fsm1.get_current_term() == term_t{3});

    // pretend [id2's] an old leader trying to make progress; this entry is expected to be ignored.
    fsm1.step(id2, raft::append_request{term_t{2}, index_t{2}, term_t{2}});

    raft::command cmd = create_command(4);
    fsm1.add_entry(std::move(cmd));
    communicate(fsm2, fsm1, fsm3);  // Term 3

    // Check entries 1, 2, 3 are respective terms; 4 is term 3 and command(4)
    auto res1 = fsm1.get_log();
    for (size_t i = 1; i < 5; ++i) {
        BOOST_CHECK(res1[i]->idx == index_t{i});
        if (i < 4) {
            BOOST_CHECK(res1[i]->term == term_t{i});
            BOOST_REQUIRE_NO_THROW(std::get<raft::log_entry::dummy>(res1[i]->data));
        } else {
            BOOST_CHECK(res1[i]->term == term_t{3});
            BOOST_REQUIRE_NO_THROW(std::get<raft::command>(res1[i]->data));
        }
    }
    BOOST_CHECK(compare_log_entries(res1, fsm2.get_log(), index_t{1}, index_t{4}));
    BOOST_CHECK(compare_log_entries(res1, fsm3.get_log(), index_t{1}, index_t{4}));
}

void handle_proposal(unsigned nodes, std::vector<int> accepting_int) {
    std::unordered_set<raft::server_id> accepting;
    raft::fsm_output output1;
    raft::append_request areq;
    raft::log_entry_ptr lep;

    for (auto id: accepting_int) {
        accepting.insert(raft::server_id{utils::UUID(0, id)});
    }

    std::vector<raft::server_id> ids;     // ids of leader 1 .. #nodes
    for (unsigned i = 1; i < nodes + 1; ++i) {
        ids.push_back(raft::server_id{utils::UUID(0, i)});
    }

    raft::configuration cfg = config_from_ids(ids);
    raft::log log1{raft::snapshot_descriptor{.config = cfg}};
    fsm_debug fsm1(raft::server_id{utils::UUID(0, 1)}, term_t{}, server_id{}, std::move(log1),
            trivial_failure_detector, fsm_cfg);

    // promote 1 to become leader (i.e. gets votes)
    election_timeout(fsm1);
    output1 = fsm1.get_output();
    BOOST_CHECK(output1.messages.size() >= nodes - 1);
    BOOST_CHECK(output1.term_and_vote);
    for (unsigned i = 2; i < nodes + 1; ++i) {
        fsm1.step(raft::server_id{utils::UUID(0, i)}, raft::vote_reply{output1.term_and_vote->first, true, false});
    }
    BOOST_CHECK(fsm1.is_leader());
    output1 = fsm1.get_output();
    lep = output1.log_entries.back();
    BOOST_REQUIRE_NO_THROW(std::get<raft::log_entry::dummy>(lep->data));

    // fsm1 dummy, send, gets specified number of replies (would commit if quorum)
    BOOST_CHECK(output1.messages.size() == nodes - 1);
    for (auto& [id, msg] : output1.messages) {
        BOOST_REQUIRE_NO_THROW(areq = std::get<raft::append_request>(msg));
        const raft::log_entry_ptr le = areq.entries.back();
        BOOST_CHECK(le->idx == index_t{1});   // Dummy is 1st entry in log
        BOOST_REQUIRE_NO_THROW(std::get<raft::log_entry::dummy>(le->data));
        if (accepting.contains(id)) { // Only gets votes from specified nodes
            fsm1.step(id, raft::append_reply{areq.current_term, index_t{1},
                    raft::append_reply::accepted{index_t{1}}});
        }
    }
    output1 = fsm1.get_output();
    // Dummy can only be committed if there were quorum votes (fsm1 counts for quorum)
    auto commit_dummy = (accepting.size() + 1) >= (nodes/2 + 1);
    BOOST_CHECK(output1.committed.size() == commit_dummy);

    // Add entry to leader fsm1
    raft::command cmd = create_command(1);
    raft::log_entry le = fsm1.add_entry(std::move(cmd));
    output1 = fsm1.get_output();
    BOOST_CHECK(output1.log_entries.size() == 1);

    // Send append to nodes who accepted dummy
    BOOST_CHECK(output1.messages.size() == accepting.size());
    for (auto& [id, msg] : output1.messages) {
        BOOST_REQUIRE_NO_THROW(areq = std::get<raft::append_request>(msg));
        const raft::log_entry_ptr le = areq.entries.back();
        BOOST_CHECK(le->idx == index_t{2});   // Entry is 2nd entry in log (after dummy)
        BOOST_REQUIRE_NO_THROW(std::get<raft::command>(le->data));
        // Only followers who accepted dummy should get 2nd append request
        BOOST_CHECK(accepting.contains(id));
        fsm1.step(id, raft::append_reply{areq.current_term, index_t{2},
                raft::append_reply::accepted{index_t{2}}});
    }
    output1 = fsm1.get_output();
    // Entry can only be committed if there were quorum votes (fsm1 counts for quorum)
    auto commit_entry = (accepting.size() + 1) >= (nodes/2 + 1);
    BOOST_CHECK(output1.committed.size() == commit_entry);

    // TODO: using communicate_until() propagate log to followers an check it matches
};

// TestProposal
// Checks follower logs with 3, 4, 5, and accepts
BOOST_AUTO_TEST_CASE(test_proposal_1) {

    handle_proposal(3, {2, 3});  // 3 nodes, 2  responsive followers
}

BOOST_AUTO_TEST_CASE(test_proposal_2) {
    handle_proposal(3, {2});     // 3 nodes, 1  responsive follower
}

BOOST_AUTO_TEST_CASE(test_proposal_3) {
    handle_proposal(3, {});      // 3 nodes, no responsive followers
}

BOOST_AUTO_TEST_CASE(test_proposal_4) {
    handle_proposal(4, {4});     // 4 nodes, 1  responsive follower
}

BOOST_AUTO_TEST_CASE(test_proposal_5) {
    handle_proposal(5, {4, 5});  // 5 nodes, 2  responsive followers
}

// TestProposalByProxy
// TBD when we support add_entry() on follower

// TestLeaderTransferIgnoreProposal
BOOST_AUTO_TEST_CASE(test_leader_transfer_ignore_proposal) {
    /// Test that a leader which has entered leader stepdown mode rejects
    /// new append requests.

    raft::server_id A_id = id(), B_id = id();
    raft::log log(raft::snapshot_descriptor{.idx = raft::index_t{0},
        .config = config_from_ids({A_id, B_id})});
    auto A = create_follower(A_id, log);
    auto B = create_follower(B_id, log);

    // Elect A leader.
    election_timeout(A);
    communicate(A, B);
    BOOST_CHECK(A.is_leader());

    // Move A to "stepdown" state.
    A.transfer_leadership();

    // At this point it should reject calls to `add_entry` by throwing `not_a_leader` exception.
    BOOST_CHECK_THROW(A.add_entry(raft::log_entry::dummy()), raft::not_a_leader);
    // Nonetheless, the leader itself hasn't yet stepped down.
    BOOST_CHECK(A.is_leader());
}

// TestTransferNonMember
BOOST_AUTO_TEST_CASE(test_transfer_non_member) {
    /// Test that a node outside configuration, that receives `timeout_now`
    /// message, doesn't disrupt operation of the rest of the cluster.
    ///
    /// That is, `timeout_now` has no effect and the outsider stays in
    /// the follower state without promoting to the candidate.

    raft::server_id A_id = id(), B_id = id(), C_id = id(), D_id = id();
    raft::log log(raft::snapshot_descriptor{.idx = raft::index_t{0},
        .config = config_from_ids({B_id, C_id, D_id})});
    auto A = create_follower(A_id, log);

    A.step(B_id, timeout_now{A.get_current_term()});
    // Check that a node outside of configuration doesn't even get a chance
    // to transition to the candidate state.
    BOOST_CHECK(!A.is_candidate());
}

// TestLeaderTransferTimeout
BOOST_AUTO_TEST_CASE(test_leader_transfer_timeout) {
    discrete_failure_detector fd;
    raft::server_id A_id = id(), B_id = id(), C_id = id();

    raft::log log(raft::snapshot_descriptor{.idx = raft::index_t{0},
        .config = config_from_ids({A_id, B_id, C_id})});
    auto A = create_follower(A_id, log, fd);
    auto B = create_follower(B_id, log, fd);
    auto C = create_follower(C_id, log, fd);
    // Elect A leader.
    election_timeout(A);
    communicate(A, B, C);
    BOOST_REQUIRE(A.is_leader());
    // tick and communicate to propagate commit index
    A.tick();
    communicate(A, B, C);
    // Start leadership transfer
    A.transfer_leadership(raft::logical_clock::duration(5));
    BOOST_CHECK(A.leadership_transfer_active());
    // Wait for election timeout so that A aborts leadership transfer procedure.
    for (int i = 0; i < 5; i++) {
        auto output = A.get_output();
        BOOST_REQUIRE_EQUAL(output.messages.size(), 1);
        BOOST_REQUIRE_NO_THROW(std::get<raft::timeout_now>(output.messages.back().second));
        BOOST_REQUIRE(!output.abort_leadership_transfer);
        A.tick();
    }
    // By now A should abort leadership transfer procedure.
    // We can check that leadership abort succeeded by checking that A is still
    // a leader.
    auto output = A.get_output();
    BOOST_REQUIRE(output.abort_leadership_transfer);
    BOOST_CHECK(A.is_leader());
    BOOST_CHECK(!A.leadership_transfer_active());
}

BOOST_AUTO_TEST_CASE(test_leader_transfer_one_node_cluster) {
    discrete_failure_detector fd;
    raft::server_id A_id = id();

    raft::log log(raft::snapshot_descriptor{.idx = raft::index_t{0}, .config = config_from_ids({A_id})});
    auto A = create_follower(A_id, log, fd);
    // Elect A leader.
    election_timeout(A);
    BOOST_REQUIRE(A.is_leader());
    // Start leadership transfer
    BOOST_REQUIRE_THROW(A.transfer_leadership(raft::logical_clock::duration(5)), raft::no_other_voting_member);
    BOOST_CHECK(!A.leadership_transfer_active());
}

BOOST_AUTO_TEST_CASE(test_leader_transfer_one_voter) {
    discrete_failure_detector fd;
    raft::server_id A_id = id(), B_id = id();
    raft::config_member_set set{{server_addr_from_id(A_id), true}, {server_addr_from_id(B_id), false}};
    raft::configuration cfg(set);


    raft::log log(raft::snapshot_descriptor{.idx = raft::index_t{0}, .config = cfg});
    auto A = create_follower(A_id, log, fd);
    auto B = create_follower(B_id, log, fd);
    // Elect A leader.
    election_timeout(A);
    BOOST_REQUIRE(A.is_leader());
    // Start leadership transfer
    BOOST_REQUIRE_THROW(A.transfer_leadership(raft::logical_clock::duration(5)), raft::no_other_voting_member);
    BOOST_CHECK(!A.leadership_transfer_active());
}
