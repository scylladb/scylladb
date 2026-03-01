/*
 * Copyright 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "idl/frozen_schema.idl.hh"
#include "mutation/canonical_mutation.hh"

namespace replica {
namespace logstor {

struct index_key {
    std::array<uint8_t, replica::logstor::index_key::digest_size> digest;
};

struct group_id {
    table_id table;
    size_t compaction_group_id;
};

class log_record {
    replica::logstor::index_key key;
    replica::logstor::record_generation generation;
    replica::logstor::group_id group;
    canonical_mutation mut;
};

}
}
