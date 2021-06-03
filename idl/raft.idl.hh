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

struct server_id_tag stub {};
struct snapshot_id_tag stub {};
struct index_tag stub {};
struct term_tag stub {};

struct server_address {
    raft::internal::tagged_id<raft::server_id_tag> id;
    bool can_vote;
    bytes info;
};

struct configuration {
    std::unordered_set<raft::server_address> current;
    std::unordered_set<raft::server_address> previous;
};

struct snapshot {
    raft::internal::tagged_uint64<raft::index_tag> idx;
    raft::internal::tagged_uint64<raft::term_tag> term;
    raft::configuration config;
    raft::internal::tagged_id<raft::snapshot_id_tag> id;
};

struct vote_request {
    raft::internal::tagged_uint64<raft::term_tag> current_term;
    raft::internal::tagged_uint64<raft::index_tag> last_log_idx;
    raft::internal::tagged_uint64<raft::term_tag> last_log_term;
    bool is_prevote;
    bool force;
};

struct vote_reply {
    raft::internal::tagged_uint64<raft::term_tag> current_term;
    bool vote_granted;
    bool is_prevote;
};

struct install_snapshot {
    raft::internal::tagged_uint64<raft::term_tag> current_term;
    raft::snapshot snp;
};

struct snapshot_reply {
    raft::internal::tagged_uint64<raft::term_tag> current_term;
    bool success;
};

struct append_reply {
    struct rejected {
        raft::internal::tagged_uint64<raft::index_tag> non_matching_idx;
        raft::internal::tagged_uint64<raft::index_tag> last_idx;
    };
    struct accepted {
        raft::internal::tagged_uint64<raft::index_tag> last_new_idx;
    };
    raft::internal::tagged_uint64<raft::term_tag> current_term;
    raft::internal::tagged_uint64<raft::index_tag> commit_idx;
    std::variant<raft::append_reply::rejected, raft::append_reply::accepted> result;
};

struct log_entry {
    struct dummy {};

    raft::internal::tagged_uint64<raft::term_tag> term;
    raft::internal::tagged_uint64<raft::index_tag> idx;
    std::variant<bytes_ostream, raft::configuration, raft::log_entry::dummy> data;
};

struct append_request {
    raft::internal::tagged_uint64<raft::term_tag> current_term;
    raft::internal::tagged_id<raft::server_id_tag> leader_id;
    raft::internal::tagged_uint64<raft::index_tag> prev_log_idx;
    raft::internal::tagged_uint64<raft::term_tag> prev_log_term;
    raft::internal::tagged_uint64<raft::index_tag> leader_commit_idx;
    std::vector<lw_shared_ptr<const raft::log_entry>> entries;
};

struct timeout_now {
    raft::internal::tagged_uint64<raft::term_tag> current_term;
};

} // namespace raft
