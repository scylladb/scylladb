/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <seastar/testing/thread_test_case.hh>

#include "transport/request.hh"
#include "transport/response.hh"

#include "test/lib/random_utils.hh"

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
    auto names_and_values = boost::copy_range<std::vector<std::pair<sstring, bytes_opt>>>(
        boost::irange<int16_t>(0, tests::random::get_int<int16_t>(16) + 16)
        | boost::adaptors::transformed([] (int) {
            return std::pair(
                tests::random::get_sstring(),
                !tests::random::get_int(4) ? bytes_opt() : bytes_opt(tests::random::get_bytes(tests::random::get_int<int16_t>(1024)))
            );
        })
    );
    res.write_short(names_and_values.size());
    for (auto& [ name, value ] : names_and_values) {
        res.write_string(name);
        res.write_value(value);
    }

    // String list
    auto string_list = boost::copy_range<std::vector<sstring>>(
        boost::irange<int16_t>(0, tests::random::get_int<int16_t>(16) + 16)
        | boost::adaptors::transformed([] (int) {
            return tests::random::get_sstring();
        })
    );
    res.write_string_list(string_list);

    // String map
    auto string_map = boost::copy_range<std::map<sstring, sstring>>(
        boost::irange<int16_t>(0, tests::random::get_int<int16_t>(16) + 16)
        | boost::adaptors::transformed([] (int) {
            return std::pair(tests::random::get_sstring(), tests::random::get_sstring());
        })
    );
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

    BOOST_CHECK(req.read_value_view(version).is_null());
    BOOST_CHECK(req.read_value_view(version).is_unset_value());
    BOOST_CHECK_EQUAL(to_bytes(req.read_value_view(version)), value);

    std::vector<sstring_view> names;
    std::vector<cql3::raw_value_view> values;
    req.read_name_and_value_list(version, names, values);
    BOOST_CHECK_EQUAL(names, names_and_values | boost::adaptors::transformed([] (auto& name_and_value) {
        return sstring_view(name_and_value.first);
    }));
    BOOST_CHECK_EQUAL(values, names_and_values | boost::adaptors::transformed([] (auto& name_and_value) {
        if (!name_and_value.second) {
            return cql3::raw_value_view::make_null();
        }
        return cql3::raw_value_view::make_value(fragmented_temporary_buffer::view(*name_and_value.second));
    }));

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
