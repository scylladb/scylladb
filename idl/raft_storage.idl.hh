/*
 * Copyright 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "raft/raft.hh"

#include "idl/uuid.idl.hh"

namespace raft {

namespace internal {

template<typename Tag>
struct tagged_id {
    utils::UUID id;
};

template<typename Tag>
struct tagged_uint64 {
    uint64_t value();
};

} // namespace internal

struct server_address {
    raft::server_id id;
    bytes info;
};

struct config_member {
    raft::server_address addr;
    bool can_vote;
};

struct configuration {
    std::unordered_set<raft::config_member, raft::config_member_hash, std::equal_to<void>> current;
    std::unordered_set<raft::config_member, raft::config_member_hash, std::equal_to<void>> previous;
};

struct log_entry {
    struct dummy {};

    raft::term_t term;
    raft::index_t idx;
    std::variant<bytes_ostream, raft::configuration, raft::log_entry::dummy> data;
};

}
