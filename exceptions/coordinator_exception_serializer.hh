/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "exceptions/coordinator_result.hh"
#include "serializer_impl.hh"
#include "utils/overloaded_functor.hh"

namespace ser {

template<>
struct serializer<exceptions::coordinator_exception_container> {
    enum class exception_type : uint8_t {
        mutation_write_timeout = 0,
        read_timeout = 1,
        read_failure = 2,
        rate_limit = 3,
        overloaded = 4,
        read_failure_with_timeout = 5,
    };

    template<typename Input>
    static exceptions::coordinator_exception_container read(Input& in) {
        auto type_index = deserialize(in, std::type_identity<uint8_t>());

        switch (static_cast<exception_type>(type_index)) {
        case exception_type::mutation_write_timeout: {
            auto message = deserialize(in, std::type_identity<sstring>());
            auto consistency = deserialize(in, std::type_identity<db::consistency_level>());
            auto received = deserialize(in, std::type_identity<int32_t>());
            auto block_for = deserialize(in, std::type_identity<int32_t>());
            auto type = static_cast<db::write_type>(deserialize(in, std::type_identity<uint8_t>()));
            return exceptions::coordinator_exception_container(
                exceptions::mutation_write_timeout_exception(message, consistency, received, block_for, type));
        }
        case exception_type::read_timeout: {
            auto message = deserialize(in, std::type_identity<sstring>());
            auto consistency = deserialize(in, std::type_identity<db::consistency_level>());
            auto received = deserialize(in, std::type_identity<int32_t>());
            auto block_for = deserialize(in, std::type_identity<int32_t>());
            auto data_present = deserialize(in, std::type_identity<bool>());
            return exceptions::coordinator_exception_container(
                exceptions::read_timeout_exception(message, consistency, received, block_for, data_present));
        }
        case exception_type::read_failure: {
            auto message = deserialize(in, std::type_identity<sstring>());
            auto consistency = deserialize(in, std::type_identity<db::consistency_level>());
            auto received = deserialize(in, std::type_identity<int32_t>());
            auto failures = deserialize(in, std::type_identity<int32_t>());
            auto block_for = deserialize(in, std::type_identity<int32_t>());
            auto data_present = deserialize(in, std::type_identity<bool>());
            return exceptions::coordinator_exception_container(
                exceptions::read_failure_exception(message, consistency, received, failures, block_for, data_present));
        }
        case exception_type::rate_limit: {
            auto message = deserialize(in, std::type_identity<sstring>());
            auto op_type = static_cast<db::operation_type>(deserialize(in, std::type_identity<uint8_t>()));
            auto rejected_by_coordinator = deserialize(in, std::type_identity<bool>());
            return exceptions::coordinator_exception_container(
                exceptions::rate_limit_exception(message, op_type, rejected_by_coordinator));
        }
        case exception_type::overloaded: {
            auto message = deserialize(in, std::type_identity<sstring>());
            return exceptions::coordinator_exception_container(
                exceptions::overloaded_exception(message));
        }
        case exception_type::read_failure_with_timeout: {
            auto message = deserialize(in, std::type_identity<sstring>());
            auto consistency = deserialize(in, std::type_identity<db::consistency_level>());
            auto received = deserialize(in, std::type_identity<int32_t>());
            auto failures = deserialize(in, std::type_identity<int32_t>());
            auto block_for = deserialize(in, std::type_identity<int32_t>());
            auto data_present = deserialize(in, std::type_identity<bool>());
            auto timeout = deserialize(in, std::type_identity<seastar::lowres_clock::time_point>());
            return exceptions::coordinator_exception_container(
                exceptions::read_failure_exception_with_timeout(message, consistency, received, failures, block_for, data_present, timeout));
        }
        default:
            throw std::runtime_error("Unknown coordinator exception type index");
        }
    }

    template<typename Output>
    static void write(Output& out, const exceptions::coordinator_exception_container& ex) {
        ex.accept(overloaded_functor{
            [&out](const exceptions::mutation_write_timeout_exception& e) {
                serialize(out, static_cast<uint8_t>(exception_type::mutation_write_timeout));
                serialize(out, sstring(e.what()));
                serialize(out, e.consistency);
                serialize(out, e.received);
                serialize(out, e.block_for);
                serialize(out, static_cast<uint8_t>(e.type));
            },
            [&out](const exceptions::read_timeout_exception& e) {
                serialize(out, static_cast<uint8_t>(exception_type::read_timeout));
                serialize(out, sstring(e.what()));
                serialize(out, e.consistency);
                serialize(out, e.received);
                serialize(out, e.block_for);
                serialize(out, e.data_present);
            },
            [&out](const exceptions::read_failure_exception& e) {
                serialize(out, static_cast<uint8_t>(exception_type::read_failure));
                serialize(out, sstring(e.what()));
                serialize(out, e.consistency);
                serialize(out, e.received);
                serialize(out, e.failures);
                serialize(out, e.block_for);
                serialize(out, e.data_present);
            },
            [&out](const exceptions::rate_limit_exception& e) {
                serialize(out, static_cast<uint8_t>(exception_type::rate_limit));
                serialize(out, sstring(e.what()));
                serialize(out, static_cast<uint8_t>(e.op_type));
                serialize(out, e.rejected_by_coordinator);
            },
            [&out](const exceptions::overloaded_exception& e) {
                serialize(out, static_cast<uint8_t>(exception_type::overloaded));
                serialize(out, sstring(e.what()));
            },
            [&out](const exceptions::read_failure_exception_with_timeout& e) {
                serialize(out, static_cast<uint8_t>(exception_type::read_failure_with_timeout));
                serialize(out, sstring(e.what()));
                serialize(out, e.consistency);
                serialize(out, e.received);
                serialize(out, e.failures);
                serialize(out, e.block_for);
                serialize(out, e.data_present);
                serialize(out, e._timeout);
            },
            [&out](const utils::bad_exception_container_access&) {
                // Serialize as overloaded exception with generic message
                serialize(out, static_cast<uint8_t>(exception_type::overloaded));
                serialize(out, sstring("Unknown exception"));
            }
        });
    }

    template<typename Input>
    static void skip(Input& in) {
        read(in);
    }
};

}
