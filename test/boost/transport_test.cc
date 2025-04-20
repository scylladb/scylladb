/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "test/lib/scylla_test_case.hh"

#include <fmt/ranges.h>
#include <fmt/std.h>

#include "transport/request.hh"
#include "transport/response.hh"

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

    auto msg = res.make_message(version, cql_transport::cql_compression::none).release();
    auto total_length = msg.len();
    auto fbufs = fragmented_temporary_buffer(msg.release(), total_length);

    bytes_ostream linearization_buffer;
    auto req = cql_transport::request_reader(fbufs.get_istream(), linearization_buffer);
    BOOST_CHECK_EQUAL(unsigned(uint8_t(req.read_byte())), version | 0x80);
    BOOST_CHECK_EQUAL(unsigned(req.read_byte()), 0); // flags
    BOOST_CHECK_EQUAL(req.read_short(), stream_id);
    BOOST_CHECK_EQUAL(unsigned(req.read_byte()), unsigned(opcode));
    BOOST_CHECK_EQUAL(req.read_int() + 9, total_length);

    auto v1 = req.read_value_view(version);
    BOOST_CHECK(!v1.unset && v1.value.is_null());
    auto v2 = req.read_value_view(version);
    BOOST_CHECK(v2.unset);
    BOOST_CHECK_EQUAL(to_bytes(req.read_value_view(version).value), value);

    std::vector<std::string_view> names;
    std::vector<cql3::raw_value_view> values;
    cql3::unset_bind_variable_vector unset;
    req.read_name_and_value_list(version, names, values, unset);
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
    req.read_string_list(received_string_list);
    BOOST_CHECK_EQUAL(received_string_list, string_list);

    auto received_string_map = req.read_string_map();
    BOOST_CHECK_EQUAL(received_string_map, string_unordered_map);

    BOOST_CHECK_EQUAL(req.read_string(), "CREATED");
    BOOST_CHECK_EQUAL(req.read_string(), "KEYSPACE");
    BOOST_CHECK_EQUAL(req.read_string(), "foo");

    BOOST_CHECK_EQUAL(req.read_string(), "CREATED");
    BOOST_CHECK_EQUAL(req.read_string(), "TABLE");
    BOOST_CHECK_EQUAL(req.read_string(), "foo");
    BOOST_CHECK_EQUAL(req.read_string(), "bar");

    BOOST_CHECK_EQUAL(req.read_string(), "CREATED");
    BOOST_CHECK_EQUAL(req.read_string(), "TYPE");
    BOOST_CHECK_EQUAL(req.read_string(), "foo");
    BOOST_CHECK_EQUAL(req.read_string(), "bar");

    BOOST_CHECK_EQUAL(req.read_string(), "CREATED");
    BOOST_CHECK_EQUAL(req.read_string(), "FUNCTION");
    BOOST_CHECK_EQUAL(req.read_string(), "foo");
    BOOST_CHECK_EQUAL(req.read_string(), "bar");
    BOOST_CHECK_EQUAL(req.read_short(), 1);
    BOOST_CHECK_EQUAL(req.read_string(), "zed");

    BOOST_CHECK_EQUAL(req.read_string(), "CREATED");
    BOOST_CHECK_EQUAL(req.read_string(), "AGGREGATE");
    BOOST_CHECK_EQUAL(req.read_string(), "foo");
    BOOST_CHECK_EQUAL(req.read_string(), "bar");
    BOOST_CHECK_EQUAL(req.read_short(), 1);
    BOOST_CHECK_EQUAL(req.read_string(), "zed");
}
