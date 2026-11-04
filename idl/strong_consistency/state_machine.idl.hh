/*
 * Copyright 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "idl/frozen_mutation.idl.hh"
#include "idl/uuid.idl.hh"
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

// The variant index is the command's format version. Alternatives are append-only:
// never reorder, remove or re-encode one in place. A new encoding is a new alternative,
// written only once a cluster feature says every node can decode it. A new field of an
// existing alternative is added at its end with [[version]]. An alternative holding a
// serialized format of its own (e.g. SSTable) carries that format's identifier.
struct raft_command {
    std::variant<service::strong_consistency::resize_marker, service::strong_consistency::write_mutation, service::strong_consistency::truncate_command> change;
};

} // namespace strong_consistency
} // namespace service
