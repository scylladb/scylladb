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

struct truncate_command {
    api::timestamp_type truncated_at;
    utils::UUID request_id;
};

struct raft_command {
    std::variant<service::strong_consistency::truncate_command, service::strong_consistency::write_mutation> change;
};

} // namespace strong_consistency
} // namespace service
