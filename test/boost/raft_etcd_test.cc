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

// Port of etcd Raft implementation unit tests

#define BOOST_TEST_MODULE raft

#include <boost/test/unit_test.hpp>
#include "test/lib/log.hh"
#include "serializer_impl.hh"
#include <limits>

#include "raft/fsm.hh"

using raft::term_t, raft::index_t, raft::server_id, raft::log_entry;

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

template <typename T>
raft::command create_command(T val) {
    raft::command command;
    ser::serialize(command, val);

    return std::move(command);
}

raft::fsm_config fsm_cfg{.append_request_threshold = 1};

class fsm_debug : public raft::fsm {
public:
    using raft::fsm::fsm;
    raft::follower_progress& get_progress(server_id id) {
        raft::follower_progress& progress = _tracker->find(id);
        return progress;
    }
};


// TestProgressLeader
BOOST_AUTO_TEST_CASE(test_progress_leader) {

    failure_detector fd;

    server_id id1{utils::UUID(0, 1)}, id2{utils::UUID(0, 2)};

    raft::configuration cfg({id1, id2});                 // 2 nodes
    raft::log log{raft::snapshot{.config = cfg}};

    raft::fsm fsm(id1, term_t{}, server_id{}, std::move(log), fd, fsm_cfg);

    election_timeout(fsm);
    BOOST_CHECK(fsm.is_candidate());

    auto output = fsm.get_output();
    fsm.step(id2, raft::vote_reply{output.term, true});
    BOOST_CHECK(fsm.is_leader());

    for (int i = 0; i < 30; ++i) {
        raft::command cmd = create_command(i + 1);
        raft::log_entry le = fsm.add_entry(std::move(cmd));

        do {
            output = fsm.get_output();
        } while (output.messages.size() == 0);  // Should only loop twice

        auto msg = std::get<raft::append_request>(output.messages.back().second);
        auto idx = msg.entries.back()->idx;
        BOOST_CHECK(idx == i + 1);
        fsm.step(id2, raft::append_reply{msg.current_term, idx, raft::append_reply::accepted{idx}});
    }
}

// TestProgressResumeByHeartbeatResp
// TBD once we have failure_detector.is_alive() heartbeat

// Similar but with append reply
BOOST_AUTO_TEST_CASE(test_progress_resume_by_append_resp) {

    failure_detector fd;

    server_id id1{utils::UUID(0, 1)}, id2{utils::UUID(0, 2)};

    raft::configuration cfg({id1, id2});                 // 2 nodes
    raft::log log{raft::snapshot{.config = cfg}};

    fsm_debug fsm(id1, term_t{}, server_id{}, std::move(log), fd, fsm_cfg);

    election_timeout(fsm);
    auto output = fsm.get_output();
    fsm.step(id2, raft::vote_reply{output.term, true});
    BOOST_CHECK(fsm.is_leader());

    raft::follower_progress& fprogress = fsm.get_progress(id2);
    BOOST_CHECK(fprogress.state == raft::follower_progress::state::PROBE);

    fprogress = fsm.get_progress(id2);
    BOOST_CHECK(!fprogress.probe_sent);
    raft::command cmd = create_command(1);
    raft::log_entry le = fsm.add_entry(std::move(cmd));
    do {
        output = fsm.get_output();
    } while (output.messages.size() == 0);

    BOOST_CHECK(fprogress.probe_sent);
}

// TestProgressPaused
BOOST_AUTO_TEST_CASE(test_progress_paused) {

    failure_detector fd;
    server_id id1{utils::UUID(0, 1)}, id2{utils::UUID(0, 2)};
    raft::configuration cfg({id1, id2});                 // 2 nodes
    raft::log log{raft::snapshot{.config = cfg}};

    fsm_debug fsm(id1, term_t{}, server_id{}, std::move(log), fd, fsm_cfg);

    election_timeout(fsm);
    auto output = fsm.get_output();
    fsm.step(id2, raft::vote_reply{output.term, true});
    BOOST_CHECK(fsm.is_leader());

    fsm.step(id2, raft::vote_reply{output.term, true});

    fsm.add_entry(create_command(1));
    fsm.add_entry(create_command(2));
    fsm.add_entry(create_command(3));
    fsm.tick();
    do {
        output = fsm.get_output();
    } while (output.messages.size() == 0);
    BOOST_CHECK(output.messages.size() == 1);
}

// TestProgressFlowControl
BOOST_AUTO_TEST_CASE(test_progress_flow_control) {

    failure_detector fd;
    server_id id1{utils::UUID(0, 1)}, id2{utils::UUID(0, 2)};
    raft::configuration cfg({id1, id2});                 // 2 nodes
    raft::log log{raft::snapshot{.config = cfg}};

    // Fit 2 x 1000 sized blobs
    raft::fsm_config fsm_cfg_8{.append_request_threshold = 2048};   // Threshold 2k
    fsm_debug fsm(id1, term_t{}, server_id{}, std::move(log), fd, fsm_cfg_8);

    election_timeout(fsm);
    auto output = fsm.get_output();
    fsm.step(id2, raft::vote_reply{output.term, true});
    BOOST_CHECK(fsm.is_leader());

    fsm.step(id2, raft::vote_reply{output.term, true});
    // Throw away all the messages relating to the initial election.
    output = fsm.get_output();
    raft::follower_progress& fprogress = fsm.get_progress(id2);
    BOOST_CHECK(fprogress.state == raft::follower_progress::state::PROBE);

    // While node 2 is in probe state, propose a bunch of entries.
    sstring blob(1000, 'a');
    raft::command cmd_blob = create_command(blob);
    for (auto i = 0; i < 10; ++i) {
        fsm.add_entry(cmd_blob);
    }
    do {
        output = fsm.get_output();
    } while (output.messages.size() == 0);

    BOOST_CHECK(output.messages.size() == 1);

    raft::append_request msg;
    BOOST_REQUIRE_NO_THROW(msg = std::get<raft::append_request>(output.messages.back().second));
    // The first proposal (only one proposal gets sent because follower is in probe state)
    // Also, the first append request by new leader is dummy
    BOOST_CHECK(msg.entries.size() == 1);
    const raft::log_entry_ptr le = msg.entries.back();
    size_t current_entry = 0;
    BOOST_CHECK(le->idx == ++current_entry);
    BOOST_REQUIRE_NO_THROW(auto dummy = std::get<raft::log_entry::dummy>(le->data));

    // When this append is acked, we change to replicate state and can
    // send multiple messages at once. (PIPELINE)
    fsm.step(id2, raft::append_reply{msg.current_term, le->idx, raft::append_reply::accepted{le->idx}});
    fprogress = fsm.get_progress(id2);
    BOOST_CHECK(fprogress.state == raft::follower_progress::state::PIPELINE);

    do {
        output = fsm.get_output();
    } while (output.messages.size() == 0);
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
            BOOST_CHECK(le->idx == ++current_entry);
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
// TODO: add pre-vote case
BOOST_AUTO_TEST_CASE(test_leader_election_overwrite_newer_logs) {

    failure_detector fd;
    // 5 nodes
    server_id id1{utils::UUID(0, 1)}, id2{utils::UUID(0, 2)}, id3{utils::UUID(0, 3)},
            id4{utils::UUID(0, 4)}, id5{utils::UUID(0, 5)};
    raft::configuration cfg({id1, id2, id3, id4, id5});
    //                        idx  term
    // node 1:    log entries  1:   1                 won first got logs
    // node 2:    log entries  1:   1                 got logs from node 1
    // node 3:    log entries  1:   2                 wond second election
    // node 4:    log entries  (at term 2 voted for 3)
    // node 5:    log entries  (at term 2 voted for 3)
    raft::log_entry_ptr lep1 = seastar::make_lw_shared<log_entry>(log_entry{term_t{1}, index_t{1}, raft::log_entry::dummy{}});
    raft::log log1{raft::snapshot{.config = cfg}, raft::log_entries{lep1}};
    raft::fsm fsm1(id1, term_t{1}, server_id{}, std::move(log1), fd, fsm_cfg);

    // Here node 1 is disconnected and 3 campaigns, becomes leader, adds dummy entry
    raft::log log3{raft::snapshot{.config = cfg}};
    raft::fsm fsm3(id3, term_t{1}, server_id{}, std::move(log3), fd, fsm_cfg);
    BOOST_CHECK(fsm3.is_follower());
    election_timeout(fsm3);
    BOOST_CHECK(fsm3.is_candidate());
    auto output3 = fsm3.get_output();
    BOOST_CHECK(output3.term == term_t{2});
    fsm3.step(id4, raft::vote_reply{term_t{output3.term}, true});
    fsm3.step(id5, raft::vote_reply{term_t{output3.term}, true});
    BOOST_CHECK(fsm3.is_leader());
    output3 = fsm3.get_output();
    // Node 3 [gets] a different entry on idx 1 (term 2)
    // (We'll later check this entry is properly replaced)
    BOOST_CHECK(output3.log_entries.size() == 1);
    BOOST_CHECK(std::holds_alternative<raft::log_entry::dummy>(output3.log_entries[0]->data));
    output3 = fsm3.get_output();
    BOOST_CHECK(output3.messages.size() == 4);   // Sends 4 dummy entries

    // Node 1 is reconnected and campaigns. The election fails because a quorum of nodes
    // know about the election that already happened at term 2. Node 1's term is pushed ahead to 2.
    election_timeout(fsm1);
    BOOST_CHECK(fsm1.is_candidate());
    auto output1 = fsm1.get_output();
    BOOST_CHECK(output1.term == term_t{2});
    BOOST_CHECK(output1.messages.size() == 4);  // Request votes to all 4 other nodes
    raft::vote_request vreq;
    BOOST_REQUIRE_NO_THROW(vreq = std::get<raft::vote_request>(output1.messages.back().second));

    // Node 3 rejects this election
    fsm3.step(id1, raft::vote_request{vreq});
    output3 = fsm3.get_output();
    BOOST_CHECK(output3.messages.size() == 1);
    raft::vote_reply vrepl;
    BOOST_REQUIRE_NO_THROW(vrepl = std::get<raft::vote_reply>(output3.messages.back().second));
    BOOST_CHECK(!vrepl.vote_granted);
    BOOST_CHECK(vrepl.current_term = term_t{2});

    fsm1.step(id2, raft::vote_reply{term_t{1}, true});   // Doesn't know about new term, accepts
    fsm1.step(id3, raft::vote_reply{term_t{2}, false});  // Knows about new term, rejects
    fsm1.step(id4, raft::vote_reply{term_t{2}, false});  // Knows about new term, rejects
    fsm1.step(id5, raft::vote_reply{term_t{2}, false});  // Knows about new term, rejects
    BOOST_CHECK(!fsm1.is_leader());

    // Node 1 campaigns again with a higher term. This time it succeeds.
    election_timeout(fsm1);
    BOOST_CHECK(fsm1.is_candidate());
    output1 = fsm1.get_output();
    // NOTE: election timeout might trigger 2 elections so term might be 3 or 4
    //       and there could be 4 or 8 vote request ouputs, so use last one
    BOOST_CHECK(output1.term > term_t{2});      // After learning about term 2, move to 3+
    auto current_term = output1.term;
    BOOST_CHECK(output1.messages.size() >= 4);  // Request votes to all 4 other nodes
    BOOST_REQUIRE_NO_THROW(vreq = std::get<raft::vote_request>(output1.messages.back().second));

    fsm1.step(id2, raft::vote_reply{output1.term, true});
    fsm1.step(id4, raft::vote_reply{output1.term, true});  // Now term is fine

    BOOST_CHECK(fsm1.is_leader());

    // Node 1 stores dummy entry and sends it to other nodes
    output1 = fsm1.get_output();
    BOOST_CHECK(output1.log_entries.size() == 1);
    BOOST_CHECK(output1.log_entries[0]->idx == 2);
    BOOST_CHECK(output1.log_entries[0]->term == current_term);
    BOOST_CHECK(std::holds_alternative<raft::log_entry::dummy>(output1.log_entries[0]->data));
    output1 = fsm1.get_output();
    BOOST_CHECK(output1.messages.size() >= 1);  // Request votes to all 4 other nodes
    raft::append_request areq;
    index_t dummy_idx{2};     // Log index of dummy entry of this term, after term 1's entry
    for (auto& [id, msg] : output1.messages) {
        // BOOST_CHECK(std::holds_alternative<raft::append_request>(msg));
        BOOST_REQUIRE_NO_THROW(areq = std::get<raft::append_request>(msg));
        BOOST_CHECK(areq.prev_log_idx == 1);
        BOOST_CHECK(areq.prev_log_term == 1);
        BOOST_CHECK(areq.entries.size() == 1);
        auto lep =  areq.entries.back();
        BOOST_CHECK(lep->idx == dummy_idx);
        BOOST_CHECK(lep->term == current_term);
        BOOST_CHECK(std::holds_alternative<raft::log_entry::dummy>(lep->data));
    }

    // Node 3 steps down and accepts the new leader when it gets entries
    fsm3.step(id1, raft::append_request{areq});
    BOOST_CHECK(fsm3.is_follower());
    output3 = fsm3.get_output();
    BOOST_CHECK(output3.term == current_term);
    BOOST_CHECK(output3.messages.size() == 1);  // Node 3 tells Node 1 there is no match at idx 1
    raft::append_reply arepl;
    BOOST_REQUIRE_NO_THROW(arepl = std::get<raft::append_reply>(output3.messages.back().second));
    // Node 1 needs to discover where Node 3 matches, then make it truncate,
    // and then send replaced entries (idx=1,term=1)(idx=2,term=current_term)

    fsm1.step(id3, raft::append_reply{arepl});
    output1 = fsm1.get_output();
    BOOST_CHECK(output1.messages.size() == 1);
    BOOST_REQUIRE_NO_THROW(areq = std::get<raft::append_request>(output1.messages.back().second));
    fsm3.step(id1, raft::append_request{areq});
    output3 = fsm3.get_output();
    BOOST_CHECK(output3.log_entries.size() == 1);
    BOOST_CHECK(output3.log_entries[0]->idx == 1);
    BOOST_CHECK(output3.log_entries[0]->term == term_t{1});
    BOOST_CHECK(std::holds_alternative<raft::log_entry::dummy>(output3.log_entries[0]->data));
    BOOST_REQUIRE_NO_THROW(arepl = std::get<raft::append_reply>(output3.messages.back().second));

    fsm1.step(id3, raft::append_reply{arepl});
    output1 = fsm1.get_output();
    BOOST_CHECK(output1.messages.size() == 1);
    BOOST_REQUIRE_NO_THROW(areq = std::get<raft::append_request>(output1.messages.back().second));
    fsm3.step(id1, raft::append_request{areq});
    output3 = fsm3.get_output();
    BOOST_CHECK(output3.log_entries.size() == 1);
    BOOST_CHECK(output3.log_entries[0]->idx == 2);
    BOOST_CHECK(output3.log_entries[0]->term == current_term);
    BOOST_CHECK(std::holds_alternative<raft::log_entry::dummy>(output3.log_entries[0]->data));
    BOOST_REQUIRE_NO_THROW(arepl = std::get<raft::append_reply>(output3.messages.back().second));
}

// TestVoteFromAnyState
BOOST_AUTO_TEST_CASE(test_vote_from_any_state) {

    failure_detector fd;
    server_id id1{utils::UUID(0, 1)}, id2{utils::UUID(0, 2)}, id3{utils::UUID(0, 3)};
    raft::configuration cfg({id1, id2, id3});
    raft::log log{raft::snapshot{.config = cfg}};
    raft::fsm fsm(id1, term_t{}, server_id{}, std::move(log), fd, fsm_cfg);

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
    fsm.step(id2, raft::vote_request{output.term + term_t{1}, index_t{1}, term_t{1}});
    output = fsm.get_output();
    BOOST_CHECK(output.messages.size() == 1);
    BOOST_REQUIRE_NO_THROW(vrepl = std::get<raft::vote_reply>(output.messages.back().second));
    BOOST_CHECK(vrepl.vote_granted);
    BOOST_CHECK(vrepl.current_term = term_t{1});

    // Leader
    election_timeout(fsm);
    BOOST_CHECK(fsm.is_candidate());
    output = fsm.get_output();
    term_t current_term = output.term;
    fsm.step(id2, raft::vote_reply{current_term, true});
    BOOST_CHECK(fsm.is_leader());
    output = fsm.get_output();
    // Node 2 requests vote for a later term
    fd.alive = false;
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

