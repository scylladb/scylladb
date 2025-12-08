/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "exceptions/coordinator_result.hh"
#include "utils/overloaded_functor.hh"

namespace exceptions {

coordinator_exception_serialized serialize_coordinator_exception(const coordinator_exception_container& ex) {
    auto serialize_read_failure = [](const auto& e) -> coordinator_exception_serialized {
        return coordinator_exception_serialized{
            .exception = read_failure_exception_serialized{
                .message = e.what(),
                .consistency = e.consistency,
                .received = e.received,
                .failures = e.failures,
                .block_for = e.block_for,
                .data_present = e.data_present
            }
        };
    };

    return ex.accept(overloaded_functor{
        [](const mutation_write_timeout_exception& e) -> coordinator_exception_serialized {
            return coordinator_exception_serialized{
                .exception = mutation_write_timeout_exception_serialized{
                    .message = e.what(),
                    .consistency = e.consistency,
                    .received = e.received,
                    .block_for = e.block_for,
                    .type = e.type
                }
            };
        },
        [](const read_timeout_exception& e) -> coordinator_exception_serialized {
            return coordinator_exception_serialized{
                .exception = read_timeout_exception_serialized{
                    .message = e.what(),
                    .consistency = e.consistency,
                    .received = e.received,
                    .block_for = e.block_for,
                    .data_present = e.data_present
                }
            };
        },
        [&serialize_read_failure](const read_failure_exception& e) -> coordinator_exception_serialized {
            return serialize_read_failure(e);
        },
        [&serialize_read_failure](const read_failure_exception_with_timeout& e) -> coordinator_exception_serialized {
            return serialize_read_failure(e);
        },
        [](const rate_limit_exception& e) -> coordinator_exception_serialized {
            return coordinator_exception_serialized{
                .exception = rate_limit_exception_serialized{
                    .ks_name = "",  // rate_limit_exception doesn't expose ks/cf names, use empty
                    .cf_name = "",
                    .op_type = e.op_type,
                    .rejected_by_coordinator = e.rejected_by_coordinator
                }
            };
        },
        [](const overloaded_exception& e) -> coordinator_exception_serialized {
            return coordinator_exception_serialized{
                .exception = overloaded_exception_serialized{
                    .message = e.what()
                }
            };
        },
        [](const utils::bad_exception_container_access&) -> coordinator_exception_serialized {
            return coordinator_exception_serialized{
                .exception = overloaded_exception_serialized{
                    .message = "Unknown exception"
                }
            };
        }
    });
}

coordinator_exception_container deserialize_coordinator_exception(const coordinator_exception_serialized& ex) {
    return std::visit(overloaded_functor{
        [](const mutation_write_timeout_exception_serialized& e) -> coordinator_exception_container {
            return coordinator_exception_container(mutation_write_timeout_exception(e.message, e.consistency, e.received, e.block_for, e.type));
        },
        [](const read_timeout_exception_serialized& e) -> coordinator_exception_container {
            return coordinator_exception_container(read_timeout_exception(e.message, e.consistency, e.received, e.block_for, e.data_present));
        },
        [](const read_failure_exception_serialized& e) -> coordinator_exception_container {
            return coordinator_exception_container(read_failure_exception(e.message, e.consistency, e.received, e.failures, e.block_for, e.data_present));
        },
        [](const rate_limit_exception_serialized& e) -> coordinator_exception_container {
            return coordinator_exception_container(rate_limit_exception(e.ks_name, e.cf_name, e.op_type, e.rejected_by_coordinator));
        },
        [](const overloaded_exception_serialized& e) -> coordinator_exception_container {
            return coordinator_exception_container(overloaded_exception(e.message));
        }
    }, ex.exception);
}

}
