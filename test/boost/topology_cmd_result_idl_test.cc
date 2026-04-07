/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

// Unit test for raft_topology_cmd_result IDL serialization, verifying
// the error_message field added in 2026.2 survives a round-trip and
// that backward-compatible deserialization of old-format payloads
// (without error_message) defaults to an empty string.

#define BOOST_TEST_MODULE core

#include <boost/test/unit_test.hpp>

#include "bytes_ostream.hh"
#include "serializer_impl.hh"
#include "service/topology_state_machine.hh"

// Instead of including the monolithic idl/storage_service.dist.hh and
// idl/storage_service.dist.impl.hh (which drag in serializers for every
// type in the IDL plus messaging-verb registration code that requires
// the full RPC stack), we declare and define only the two serializer
// specializations needed for raft_topology_cmd_result.  The bodies are
// identical to the idl-compiler output.

namespace ser {

template <>
struct serializer<service::raft_topology_cmd_result::command_status> {
    template <typename Output>
    static void write(Output& buf, const service::raft_topology_cmd_result::command_status& v) {
        serialize(buf, static_cast<uint8_t>(v));
    }
    template <typename Input>
    static service::raft_topology_cmd_result::command_status read(Input& buf) {
        return static_cast<service::raft_topology_cmd_result::command_status>(
            deserialize(buf, std::type_identity<uint8_t>()));
    }
    template <typename Input>
    static void skip(Input& buf) {
        buf.skip(sizeof(uint8_t));
    }
};

template <>
struct serializer<service::raft_topology_cmd_result> {
    template <typename Output>
    static void write(Output& buf, const service::raft_topology_cmd_result& obj) {
        set_size(buf, obj);
        serialize(buf, obj.status);
        serialize(buf, obj.error_message);
    }
    template <typename Input>
    static service::raft_topology_cmd_result read(Input& buf) {
        return seastar::with_serialized_stream(buf, [] (auto& buf) {
            size_type size = deserialize(buf, std::type_identity<size_type>());
            auto in = buf.read_substream(size - sizeof(size_type));
            auto status = deserialize(in, std::type_identity<service::raft_topology_cmd_result::command_status>());
            auto error_message = (in.size() > 0)
                ? deserialize(in, std::type_identity<sstring>()) : sstring();
            return service::raft_topology_cmd_result{std::move(status), std::move(error_message)};
        });
    }
    template <typename Input>
    static void skip(Input& buf) {
        seastar::with_serialized_stream(buf, [] (auto& buf) {
            size_type size = deserialize(buf, std::type_identity<size_type>());
            buf.skip(size - sizeof(size_type));
        });
    }
};

} // namespace ser

using status_t = service::raft_topology_cmd_result::command_status;

// Verify that a raft_topology_cmd_result with a non-empty error_message
// survives a serialization round-trip.
BOOST_AUTO_TEST_CASE(test_raft_topology_cmd_result_round_trip_with_error) {
    service::raft_topology_cmd_result original {
        .status = status_t::fail,
        .error_message = "rebuild from dc2 would lose data for keyspace ks1: "
                         "lost=2, rf=1 in dc2; consider using a different "
                         "source datacenter or --force"
    };

    bytes_ostream buf;
    ser::serialize(buf, original);

    auto bv = buf.linearize();
    auto in = ser::as_input_stream(bv);
    auto result = ser::deserialize(in, std::type_identity<service::raft_topology_cmd_result>());

    BOOST_CHECK(result.status == status_t::fail);
    BOOST_CHECK_EQUAL(result.error_message, original.error_message);
}

// Verify round-trip for the success case with an empty error_message.
BOOST_AUTO_TEST_CASE(test_raft_topology_cmd_result_round_trip_success) {
    service::raft_topology_cmd_result original {
        .status = status_t::success,
        .error_message = {}
    };

    bytes_ostream buf;
    ser::serialize(buf, original);

    auto bv = buf.linearize();
    auto in = ser::as_input_stream(bv);
    auto result = ser::deserialize(in, std::type_identity<service::raft_topology_cmd_result>());

    BOOST_CHECK(result.status == status_t::success);
    BOOST_CHECK(result.error_message.empty());
}

// Simulate the pre-2026.2 wire format that lacks the error_message field.
// The generated deserializer uses a size-prefixed envelope for non-final
// structs, checking `in.size() > 0` before reading the versioned field.
// Old format: [uint32_t size = 5] [uint8_t status]
BOOST_AUTO_TEST_CASE(test_raft_topology_cmd_result_backward_compat) {
    bytes_ostream old_buf;
    ser::serialize(old_buf, uint32_t(sizeof(uint32_t) + sizeof(uint8_t)));
    ser::serialize(old_buf, uint8_t(1)); // command_status::success

    auto bv = old_buf.linearize();
    auto in = ser::as_input_stream(bv);
    auto result = ser::deserialize(in, std::type_identity<service::raft_topology_cmd_result>());

    BOOST_CHECK(result.status == status_t::success);
    BOOST_CHECK(result.error_message.empty());
}

// Same backward-compat test but with fail status.
BOOST_AUTO_TEST_CASE(test_raft_topology_cmd_result_backward_compat_fail) {
    bytes_ostream old_buf;
    ser::serialize(old_buf, uint32_t(sizeof(uint32_t) + sizeof(uint8_t)));
    ser::serialize(old_buf, uint8_t(0)); // command_status::fail

    auto bv = old_buf.linearize();
    auto in = ser::as_input_stream(bv);
    auto result = ser::deserialize(in, std::type_identity<service::raft_topology_cmd_result>());

    BOOST_CHECK(result.status == status_t::fail);
    BOOST_CHECK(result.error_message.empty());
}
