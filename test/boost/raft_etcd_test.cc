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

using raft::term_t, raft::index_t, raft::server_id;

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
