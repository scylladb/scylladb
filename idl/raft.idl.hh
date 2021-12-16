/*
 * Copyright 2020-present ScyllaDB
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

namespace raft {

namespace internal {

template<typename Tag>
struct tagged_id {
    utils::UUID id;
};

template<typename Tag>
struct tagged_uint64 {
    uint64_t get_value();
};

} // namespace internal

struct server_address {
    raft::server_id id;
    bool can_vote;
    bytes info;
};

struct configuration {
    std::unordered_set<raft::server_address> current;
    std::unordered_set<raft::server_address> previous;
};

struct snapshot_descriptor {
    raft::index_t idx;
    raft::term_t term;
    raft::configuration config;
    raft::snapshot_id id;
};

struct vote_request {
    raft::term_t current_term;
    raft::index_t last_log_idx;
    raft::term_t last_log_term;
    bool is_prevote;
    bool force;
};

struct vote_reply {
    raft::term_t current_term;
    bool vote_granted;
    bool is_prevote;
};

struct install_snapshot {
    raft::term_t current_term;
    raft::snapshot_descriptor snp;
};

struct snapshot_reply {
    raft::term_t current_term;
    bool success;
};

struct append_reply {
    struct rejected {
        raft::index_t non_matching_idx;
        raft::index_t last_idx;
    };
    struct accepted {
        raft::index_t last_new_idx;
    };
    raft::term_t current_term;
    raft::index_t commit_idx;
    std::variant<raft::append_reply::rejected, raft::append_reply::accepted> result;
};

struct log_entry {
    struct dummy {};

    raft::term_t term;
    raft::index_t idx;
    std::variant<bytes_ostream, raft::configuration, raft::log_entry::dummy> data;
};

struct append_request {
    raft::term_t current_term;
    raft::index_t prev_log_idx;
    raft::term_t prev_log_term;
    raft::index_t leader_commit_idx;
    std::vector<lw_shared_ptr<const raft::log_entry>> entries;
};

struct timeout_now {
    raft::term_t current_term;
};

struct read_quorum {
    raft::term_t current_term;
    raft::index_t leader_commit_idx;
    raft::read_id id;
};

struct read_quorum_reply {
    raft::term_t current_term;
    raft::index_t commit_idx;
    raft::read_id id;
};

struct not_a_leader {
    raft::server_id leader;
};

struct commit_status_unknown {
};

struct entry_id {
    raft::term_t term;
    raft::index_t idx;
};

} // namespace raft
