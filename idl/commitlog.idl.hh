/*
 * Copyright 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "idl/mutation.idl.hh"
#include "idl/frozen_mutation.idl.hh"
#include "raft/raft.hh"
#include "idl/raft_storage.idl.hh"

class mutation_entry [[writable]] {
    std::optional<column_mapping> mapping();
    frozen_mutation mutation();
};

struct raft_commitlog_entry [[writable]] {
    raft::group_id group_id;
    raft::log_entry_ptr entry;
};

struct commitlog_entry [[writable]] {
    std::variant<raft_commitlog_entry, mutation_entry> item;
};
