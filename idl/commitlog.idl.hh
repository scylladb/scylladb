/*
 * Copyright 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "idl/mutation.idl.hh"
#include "idl/frozen_mutation.idl.hh"
#include "idl/raft_storage.idl.hh"

class commitlog_entry [[writable]] {
    std::optional<column_mapping> mapping();
    frozen_mutation mutation();
};

class raft_commit_log_entry [[writable]] {
    utils::UUID group_id();
    std::optional<raft::log_entry> entry();
    std::optional<raft::index_t> truncate_idx();
    std::optional<raft::index_t> truncate_prefix_idx();
};
