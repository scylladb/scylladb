/*
 * Copyright 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "raft/raft.hh"
#include "idl/frozen_mutation.idl.hh"
#include "idl/uuid.idl.hh"
#include "idl/raft_storage.idl.hh"
#include "mutation/timestamp.hh"

namespace service {
namespace strong_consistency {

struct write_mutation {
    frozen_mutation mutation;
};

enum class resize_marker_kind : uint8_t {
    start_resize,
    end_resize,
};

struct resize_marker {
    service::strong_consistency::resize_marker_kind kind;
    api::timestamp_type timestamp;
};

struct truncate_command {
    api::timestamp_type truncated_at;
    utils::UUID request_id;
};

struct seal_command {
    api::timestamp_type timestamp;
};

struct promote_command {
    raft::index_t seal_idx;
};

struct raft_command {
    std::variant<service::strong_consistency::resize_marker, service::strong_consistency::write_mutation, service::strong_consistency::truncate_command, service::strong_consistency::seal_command, service::strong_consistency::promote_command> change;
};

} // namespace strong_consistency
} // namespace service
