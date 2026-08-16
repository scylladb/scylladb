/*
 * Copyright 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "idl/frozen_mutation.idl.hh"
#include "idl/uuid.idl.hh"

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
};

struct no_op {};

struct raft_command {
    std::variant<service::strong_consistency::no_op, service::strong_consistency::write_mutation, service::strong_consistency::resize_marker> change;
};

} // namespace strong_consistency
} // namespace service
