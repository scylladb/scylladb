/*
 * Copyright 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "db/consistency_level_type.hh"
#include "db/write_type.hh"
#include "db/operation_type.hh"
#include "idl/consistency_level.idl.hh"

namespace db {

enum class write_type : uint8_t {
    SIMPLE,
    BATCH,
    UNLOGGED_BATCH,
    COUNTER,
    BATCH_LOG,
    CAS,
    VIEW,
};

enum class operation_type : uint8_t {
    read = 0,
    write = 1
};

}

namespace exceptions {

// Serializable representation of mutation_write_timeout_exception
struct mutation_write_timeout_exception_serialized {
    sstring message;
    db::consistency_level consistency;
    int32_t received;
    int32_t block_for;
    db::write_type type;
};

// Serializable representation of read_timeout_exception
struct read_timeout_exception_serialized {
    sstring message;
    db::consistency_level consistency;
    int32_t received;
    int32_t block_for;
    bool data_present;
};

// Serializable representation of read_failure_exception
struct read_failure_exception_serialized {
    sstring message;
    db::consistency_level consistency;
    int32_t received;
    int32_t failures;
    int32_t block_for;
    bool data_present;
};

// Serializable representation of rate_limit_exception
struct rate_limit_exception_serialized {
    sstring ks_name;
    sstring cf_name;
    db::operation_type op_type;
    bool rejected_by_coordinator;
};

// Serializable representation of overloaded_exception
struct overloaded_exception_serialized {
    sstring message;
};

// Variant to hold any coordinator exception
struct coordinator_exception_serialized {
    std::variant<
        exceptions::mutation_write_timeout_exception_serialized,
        exceptions::read_timeout_exception_serialized,
        exceptions::read_failure_exception_serialized,
        exceptions::rate_limit_exception_serialized,
        exceptions::overloaded_exception_serialized
    > exception;
};

}
