/*
 * Copyright 2021-present ScyllaDB
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

struct log_entry {
    struct dummy {};

    raft::term_t term;
    raft::index_t idx;
    std::variant<bytes_ostream, raft::configuration, raft::log_entry::dummy> data;
};

}
