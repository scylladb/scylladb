/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "test/lib/scylla_test_case.hh"

#include <fmt/ranges.h>
#include <fmt/std.h>

#include "transport/request.hh"
#include "transport/response.hh"
#include "cql3/column_identifier.hh"
#include "utils/memory_data_sink.hh"
#include "test/lib/random_utils.hh"
#include "test/lib/test_utils.hh"

namespace cql3 {

bool operator==(const cql3::raw_value_view& a, const cql3::raw_value_view& b) {
    if (a.is_value()) {
        return b.is_value() && b.with_value([&] (const FragmentedView auto& v2) {
            return a.with_value([&] (const FragmentedView auto& v1) {
                return equal_unsigned(v1, v2);
            });
        });
    } else {
        return a.is_null() == b.is_null();
    }
}

} // namespace cql3

static memory_data_sink_buffers write_message_to_buffers(cql_transport::response& res, uint8_t version, cql_transport::cql_compression compression, size_t compression_threshold) {
    memory_data_sink_buffers buffers;
    output_stream<char> out(data_sink(std::make_unique<memory_data_sink>(buffers)));
    res.write_message(out, version, compression, compression_threshold, deleter()).get();
    return buffers;
}

SEASTAR_THREAD_TEST_CASE(test_response_request_reader) {
    auto stream_id = tests::random::get_int<int16_t>();
    auto opcode = tests::random::get_int<uint8_t>(uint8_t(cql_transport::cql_binary_opcode::AUTH_SUCCESS));
    auto res = cql_transport::response(stream_id, cql_transport::cql_binary_opcode(opcode), tracing::trace_state_ptr());

    // Null value
    res.write_value(bytes_opt());

    // Unset value
    res.write_int(-2);

    // "Value" value
    auto value = tests::random::get_bytes(tests::random::get_int<int16_t>(1024));
    res.write_value(bytes_opt(value));

    // Name and value list
    auto names_and_values =
        std::views::iota(0, tests::random::get_int<int>(16) + 16)
        | std::views::transform([] (int) {
            return std::pair(
                tests::random::get_sstring(),
                !tests::random::get_int(4) ? bytes_opt() : bytes_opt(tests::random::get_bytes(tests::random::get_int<int16_t>(1024)))
            );
        })
        | std::ranges::to<std::vector<std::pair<sstring, bytes_opt>>>();
    res.write_short(names_and_values.size());
    for (auto& [ name, value ] : names_and_values) {
        res.write_string(name);
        res.write_value(value);
    }

    // String list
    auto string_list =
        std::views::iota(0, tests::random::get_int<int>(16) + 16)
        | std::views::transform([] (int) {
            return tests::random::get_sstring();
        })
        | std::ranges::to<std::vector<sstring>>();
    res.write_string_list(string_list);

    // String map
    auto string_map =
        std::views::iota(0, tests::random::get_int<int>(16) + 16)
        | std::views::transform([] (int) {
            return std::pair(tests::random::get_sstring(), tests::random::get_sstring());
        })
        | std::ranges::to<std::map>();
    res.write_string_map(string_map);
    auto string_unordered_map = std::unordered_map<sstring, sstring>(string_map.begin(), string_map.end());

    static constexpr auto version = 4;

    using sc = cql_transport::event::schema_change;
    res.serialize({sc::change_type::CREATED, sc::target_type::KEYSPACE, "foo"}, version);
    res.serialize({sc::change_type::CREATED, sc::target_type::TABLE, "foo", "bar"}, version);
    res.serialize({sc::change_type::CREATED, sc::target_type::TYPE, "foo", "bar"}, version);
    res.serialize({sc::change_type::CREATED, sc::target_type::FUNCTION, "foo", "bar", "zed"}, version);
    res.serialize({sc::change_type::CREATED, sc::target_type::AGGREGATE, "foo", "bar", "zed"}, version);

    auto buffers = write_message_to_buffers(res, version, cql_transport::cql_compression::none, 0);
    auto total_length = buffers.size();
    auto fbufs = fragmented_temporary_buffer(buffers.buffers() | std::views::as_rvalue | std::ranges::to<std::vector>(), total_length);

    bytes_ostream linearization_buffer;
    auto req = cql_transport::request_reader(fbufs.get_istream(), linearization_buffer);
    BOOST_CHECK_EQUAL(unsigned(uint8_t(req.read_byte().value())), version | 0x80);
    BOOST_CHECK_EQUAL(unsigned(req.read_byte().value()), 0); // flags
    BOOST_CHECK_EQUAL(req.read_short().value(), stream_id);
    BOOST_CHECK_EQUAL(unsigned(req.read_byte().value()), unsigned(opcode));
    BOOST_CHECK_EQUAL(req.read_int().value() + 9, total_length);

    auto v1 = req.read_value_view(version).value();
    BOOST_CHECK(!v1.unset && v1.value.is_null());
    auto v2 = req.read_value_view(version).value();
    BOOST_CHECK(v2.unset);
    BOOST_CHECK_EQUAL(to_bytes(req.read_value_view(version).value().value), value);

    std::vector<std::string_view> names;
    std::vector<cql3::raw_value_view> values;
    cql3::unset_bind_variable_vector unset;
    BOOST_CHECK(req.read_name_and_value_list(version, names, values, unset));
    BOOST_CHECK(std::none_of(unset.begin(), unset.end(), std::identity()));
    BOOST_CHECK(std::ranges::equal(names, names_and_values | std::views::transform([] (auto& name_and_value) {
        return std::string_view(name_and_value.first);
    })));
    BOOST_CHECK(std::ranges::equal(values, names_and_values | std::views::transform([] (auto& name_and_value) {
        if (!name_and_value.second) {
            return cql3::raw_value_view::make_null();
        }
        return cql3::raw_value_view::make_value(fragmented_temporary_buffer::view(*name_and_value.second));
    })));

    auto received_string_list = std::vector<sstring>();
    BOOST_CHECK(req.read_string_list(received_string_list));
    BOOST_CHECK_EQUAL(received_string_list, string_list);

    auto received_string_map = req.read_string_map().value();
    BOOST_CHECK_EQUAL(received_string_map, string_unordered_map);

    BOOST_CHECK_EQUAL(req.read_string().value(), "CREATED");
    BOOST_CHECK_EQUAL(req.read_string().value(), "KEYSPACE");
    BOOST_CHECK_EQUAL(req.read_string().value(), "foo");

    BOOST_CHECK_EQUAL(req.read_string().value(), "CREATED");
    BOOST_CHECK_EQUAL(req.read_string().value(), "TABLE");
    BOOST_CHECK_EQUAL(req.read_string().value(), "foo");
    BOOST_CHECK_EQUAL(req.read_string().value(), "bar");

    BOOST_CHECK_EQUAL(req.read_string().value(), "CREATED");
    BOOST_CHECK_EQUAL(req.read_string().value(), "TYPE");
    BOOST_CHECK_EQUAL(req.read_string().value(), "foo");
    BOOST_CHECK_EQUAL(req.read_string().value(), "bar");

    BOOST_CHECK_EQUAL(req.read_string().value(), "CREATED");
    BOOST_CHECK_EQUAL(req.read_string().value(), "FUNCTION");
    BOOST_CHECK_EQUAL(req.read_string().value(), "foo");
    BOOST_CHECK_EQUAL(req.read_string().value(), "bar");
    BOOST_CHECK_EQUAL(req.read_short().value(), 1);
    BOOST_CHECK_EQUAL(req.read_string().value(), "zed");

    BOOST_CHECK_EQUAL(req.read_string().value(), "CREATED");
    BOOST_CHECK_EQUAL(req.read_string().value(), "AGGREGATE");
    BOOST_CHECK_EQUAL(req.read_string().value(), "foo");
    BOOST_CHECK_EQUAL(req.read_string().value(), "bar");
    BOOST_CHECK_EQUAL(req.read_short().value(), 1);
    BOOST_CHECK_EQUAL(req.read_string().value(), "zed");
}

// Reflects the response as written to the wire: (frame flags, frame body).
static std::pair<uint8_t, bytes> write_response_frame(cql_transport::response& res, cql_transport::cql_compression compression, size_t compression_threshold) {
    auto buffers = write_message_to_buffers(res, 4, compression, compression_threshold);
    bytes frame(bytes::initialized_later(), buffers.size());
    size_t off = 0;
    for (auto& buf : buffers.buffers()) {
        std::copy_n(buf.get(), buf.size(), reinterpret_cast<char*>(frame.data()) + off);
        off += buf.size();
    }
    BOOST_REQUIRE_GE(frame.size(), 9);
    return {uint8_t(frame[1]), frame.substr(9)};
}

static cql_transport::response make_response_with_body(size_t size) {
    auto res = cql_transport::response(0, cql_transport::cql_binary_opcode::RESULT, tracing::trace_state_ptr());
    for (size_t i = 0; i < size / 8; ++i) {
        res.write_long(0x0101010101010101);
    }
    return res;
}

SEASTAR_THREAD_TEST_CASE(test_response_compression_threshold) {
    constexpr size_t threshold = 1024;
    constexpr uint8_t compression_flag = 0x01;

    // Body below the threshold: sent uncompressed, no compression flag.
    auto small = make_response_with_body(512);
    auto [flags, body] = write_response_frame(small, cql_transport::cql_compression::lz4, threshold);
    BOOST_CHECK(!(flags & compression_flag));
    BOOST_CHECK_EQUAL(body.size(), 512);

    // Body at/above the threshold: compressed, flag set, 4-byte length prefix.
    auto large = make_response_with_body(2048);
    std::tie(flags, body) = write_response_frame(large, cql_transport::cql_compression::lz4, threshold);
    BOOST_CHECK(flags & compression_flag);
    BOOST_REQUIRE_GE(body.size(), 4);
    uint32_t uncompressed_len = (uint8_t(body[0]) << 24) | (uint8_t(body[1]) << 16) | (uint8_t(body[2]) << 8) | uint8_t(body[3]);
    BOOST_CHECK_EQUAL(uncompressed_len, 2048);
    BOOST_CHECK_LT(body.size(), 2048); // repetitive content must shrink

    // Threshold 0 compresses everything.
    auto small2 = make_response_with_body(512);
    std::tie(flags, body) = write_response_frame(small2, cql_transport::cql_compression::lz4, 0);
    BOOST_CHECK(flags & compression_flag);
}

SEASTAR_THREAD_TEST_CASE(test_response_metadata_changed_for_empty_request_metadata_id) {
    auto col = make_lw_shared<cql3::column_specification>(
            "ks", "cf", ::make_shared<cql3::column_identifier>("v", true), utf8_type);
    cql3::metadata m({col});
    auto calculated_metadata_id = m.calculate_metadata_id();
    auto expected_metadata_id = calculated_metadata_id.to_bytes_view();

    // Create a different (zero-filled) metadata_id for request to trigger METADATA_CHANGED
    bytes dummy_request_bytes(bytes::initialized_later(), 16);
    std::fill(dummy_request_bytes.begin(), dummy_request_bytes.end(), int8_t(0));
    auto res = cql_transport::response(0, cql_transport::cql_binary_opcode::RESULT, tracing::trace_state_ptr());
    res.write(m, cql_transport::cql_metadata_id_wrapper(
            cql3::cql_metadata_id_type(bytes_view(dummy_request_bytes)),
            cql3::cql_metadata_id_type(bytes_view(expected_metadata_id))), true);

    auto buffers = write_message_to_buffers(res, 4, cql_transport::cql_compression::none, 0);
    auto total_length = buffers.size();
    auto fbufs = fragmented_temporary_buffer(buffers.buffers() | std::views::as_rvalue | std::ranges::to<std::vector>(), total_length);

    bytes_ostream linearization_buffer;
    auto req = cql_transport::request_reader(fbufs.get_istream(), linearization_buffer);
    BOOST_REQUIRE(req.read_byte());
    BOOST_REQUIRE(req.read_byte());
    BOOST_REQUIRE(req.read_short());
    BOOST_REQUIRE(req.read_byte());
    BOOST_REQUIRE(req.read_int());

    auto flags = req.read_int().value();
    BOOST_CHECK(flags & cql3::metadata::flag_enum_set::mask_for<cql3::metadata::flag::METADATA_CHANGED>());
    BOOST_CHECK(!(flags & cql3::metadata::flag_enum_set::mask_for<cql3::metadata::flag::NO_METADATA>()));
    BOOST_CHECK_EQUAL(req.read_int().value(), 1);
    BOOST_CHECK_EQUAL(req.read_short_bytes().value(), expected_metadata_id);
}
