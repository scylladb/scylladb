/*
 * Copyright 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "idl/uuid.idl.hh"
#include "mutation/timestamp.hh"

namespace service {
namespace strong_consistency {

// Index 1 of raft_command::change held write_mutation { frozen_mutation mutation; }.
// Kept empty so no other alternative takes the index: an older version would decode
// its bytes as a frozen_mutation.
struct retired_write_mutation {
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

// A write to the group's tablet. `data` is a format byte, the delta-encoding bases, the
// clustering key value lengths, then the partition in the SSTable mx data-file format;
// see encode_write_mutation(). No size frame (final), so the format byte is the only
// versioning point.
struct write_mutation_v2 final {
    table_schema_version schema_version;
    bytes_ostream data;
};

// The variant index is the command's format version. Alternatives are append-only:
// never reorder, remove or re-encode one in place. A new encoding is a new alternative,
// written only once a cluster feature says every node can decode it. A new field of an
// existing alternative is added at its end with [[version]]. An alternative holding a
// serialized format of its own (e.g. SSTable) carries that format's identifier.
//
// Exception: write_mutation_v2 replaced write_mutation without a cluster feature.
// Strongly consistent tables are experimental, and this format change was made a hard
// cut: mixed-version groups and commitlog entries written before it are unsupported.
struct raft_command {
    std::variant<service::strong_consistency::resize_marker, service::strong_consistency::retired_write_mutation, service::strong_consistency::truncate_command, service::strong_consistency::write_mutation_v2> change;
};

} // namespace strong_consistency
} // namespace service
