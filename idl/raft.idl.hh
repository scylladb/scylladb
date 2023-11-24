/*
 * Copyright 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "raft/raft.hh"

#include "idl/raft_storage.idl.hh"

namespace raft {

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

struct not_a_member {
    sstring message();
};

struct transient_error {
    sstring message();
    raft::server_id leader;
};

struct commit_status_unknown {
};

struct entry_id {
    raft::term_t term;
    raft::index_t idx;
};

verb [[with_client_info, with_timeout]] raft_send_snapshot (raft::group_id, raft::server_id from_id, raft::server_id dst_id, raft::install_snapshot snap [[ref]]) -> raft::snapshot_reply;
verb [[with_client_info, with_timeout, one_way]] raft_append_entries (raft::group_id, raft::server_id from_id, raft::server_id dst_id, raft::append_request req [[ref]]);
verb [[with_client_info, with_timeout, one_way]] raft_append_entries_reply (raft::group_id, raft::server_id from_id, raft::server_id dst_id, raft::append_reply);
verb [[with_client_info, with_timeout, one_way]] raft_vote_request (raft::group_id, raft::server_id from_id, raft::server_id dst_id, raft::vote_request);
verb [[with_client_info, with_timeout, one_way]] raft_vote_reply (raft::group_id, raft::server_id from_id, raft::server_id dst_id, raft::vote_reply);
verb [[with_client_info, with_timeout, one_way]] raft_timeout_now (raft::group_id, raft::server_id from_id, raft::server_id dst_id, raft::timeout_now);
verb [[with_client_info, with_timeout, one_way]] raft_read_quorum (raft::group_id, raft::server_id from_id, raft::server_id dst_id, raft::read_quorum);
verb [[with_client_info, with_timeout, one_way]] raft_read_quorum_reply (raft::group_id, raft::server_id from_id, raft::server_id dst_id, raft::read_quorum_reply);
verb [[with_client_info, with_timeout]] raft_execute_read_barrier_on_leader (raft::group_id, raft::server_id from_id, raft::server_id dst_id) -> raft::read_barrier_reply;
verb [[with_client_info, with_timeout]] raft_add_entry (raft::group_id, raft::server_id from_id, raft::server_id dst_id, raft::command cmd [[ref]]) -> raft::add_entry_reply;
verb [[with_client_info, with_timeout]] raft_modify_config (raft::group_id gid, raft::server_id from_id, raft::server_id dst_id, std::vector<raft::config_member> add [[ref]], std::vector<raft::server_id> del [[ref]]) -> raft::add_entry_reply;

} // namespace raft

namespace service {

struct wrong_destination {
    raft::server_id reached_id;
};

struct group_liveness_info {
    bool group0_alive;
};

struct direct_fd_ping_reply {
    std::variant<std::monostate, service::wrong_destination, service::group_liveness_info> result;
};

verb [[with_client_info, cancellable]] direct_fd_ping (raft::server_id dst_id) -> service::direct_fd_ping_reply;

} // namespace service
