/*
 * Copyright 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

// Include only the dependencies needed for types, not the header that defines these structs
#include "utils/chunked_vector.hh"
#include "bytes.hh"
#include "idl/paging_state.idl.hh"

// The C++ types column_specification_serialized, metadata_serialized, and result_set_serialized
// are defined in cql3/result_set.hh. This IDL generates the serialization code for them.

namespace cql3 {

struct column_specification_serialized {
    sstring ks_name;
    sstring cf_name;
    sstring column_name;
    sstring type_name;
};

struct metadata_serialized {
    uint32_t flags;
    std::vector<cql3::column_specification_serialized> column_specs;
    uint32_t column_count;
    std::optional<service::pager::paging_state> paging_state;
};

struct result_set_serialized {
    cql3::metadata_serialized metadata;
    utils::chunked_vector<std::vector<bytes_opt>> rows;
};

}
