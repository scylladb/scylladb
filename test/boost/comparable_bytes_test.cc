/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "test/lib/scylla_test_case.hh"

#include <seastar/util/lazy.hh>

#include "bytes_ostream.hh"
#include "test/lib/log.hh"
#include "types/types.hh"
#include "utils/assert.hh"
#include "utils/comparable_bytes.hh"

BOOST_AUTO_TEST_CASE(test_comparable_bytes_opt) {
    BOOST_REQUIRE(comparable_bytes::from_data_value(data_value::make_null(int32_type)) == comparable_bytes_opt());
    BOOST_REQUIRE(comparable_bytes::from_managed_bytes(*int32_type, managed_bytes_opt()) == comparable_bytes_opt());
}

// abstract data generator for the testcases
struct test_data_generator {
    const std::vector<data_value>& test_data() const {
        SCYLLA_ASSERT(!_test_data.empty());
        return _test_data;
    }

    const data_type& data_type() const {
        return _test_data.at(0).type();
    }

protected:
    std::vector<data_value> _test_data;
};

void byte_comparable_test(test_data_generator&& gen) {
    struct test_item {
        bytes serialized_bytes;
        comparable_bytes comparable_bytes;
    };
    std::vector<test_item> test_items;

    // test encode/decode
    const auto test_data = gen.test_data();
    const auto test_data_type = gen.data_type();
    testlog.info("testing type '{}' with {} items...", gen.data_type().get()->cql3_type_name(), test_data.size());
    testlog.trace("test data : {}", test_data);
    for (const data_value& value : test_data) {
        // verify comparable bytes encode/decode
        auto comparable_bytes = comparable_bytes::from_data_value(value);
        auto decoded_value = comparable_bytes->to_data_value(test_data_type);
        BOOST_REQUIRE_MESSAGE(value == decoded_value, seastar::value_of([&] () {
            return fmt::format("comparable bytes encode/decode failed; expected : {}; actual : {}", value, decoded_value);
        }));

        // collect the data in a vector to verify ordering later
        test_items.emplace_back(value.serialize_nonnull(), comparable_bytes.value());
    };

    // Verify that decoding succeeds even when the comparable bytes contain
    // extra data appended after the value to be converted.
    // This required for decode to work on composite types.
    bytes_ostream bos;
    // Select an item from the middle to test this case as front and back items
    // are often edge cases (e.g. min/max values).
    const auto item_id = test_items.size() / 2;
    auto test_value = test_items.at(item_id);
    bos.write(test_value.comparable_bytes.as_managed_bytes_view());
    bos.write(bytes("this-still-should-work"));
    auto cb = comparable_bytes(std::move(bos).to_managed_bytes());
    auto decoded_value = cb.to_data_value(test_data_type);
    BOOST_REQUIRE_MESSAGE(test_data.at(item_id) == decoded_value, seastar::value_of([&] () {
        return fmt::format("comparable bytes decode failed with appended bytes; expected : {}; actual : {}", test_data.at(0), decoded_value);
    }));

    // Sort the items based on comparable bytes
    std::ranges::sort(test_items, [] (const test_item& a, const test_item& b) {
        return a.comparable_bytes < b.comparable_bytes;
    });

    // Verify that ordering them based on comparable bytes, sorts the values as expected
    BOOST_REQUIRE_MESSAGE(std::ranges::is_sorted(test_items, [&test_data_type] (const test_item& a, const test_item& b) {
        return test_data_type->compare(a.serialized_bytes, b.serialized_bytes) == std::strong_ordering::less;
    }), "sorting items based on comparable bytes failed");
}

template <std::integral int_type>
struct integer_test_data_generator : test_data_generator {
    integer_test_data_generator()
    {
        // Generates test values by shifting bit(1) through all possible positions and then deriving
        // multiple test cases from each value. This helps test edge cases and boundary conditions
        // by covering values with different bit patterns across the entire range of the type.
        auto num = int_type(1);
        auto num_bits = sizeof(int_type) * 8;
        _test_data.reserve(num_bits * 4);
        while(num_bits-- > 0) {
            for (int_type n : std::initializer_list<int_type>{num, num - 1, ~num, ~(num - 1)}) {
                _test_data.emplace_back(n);
            }
            num <<= 1;
        }
    }
};

BOOST_AUTO_TEST_CASE(test_tinyint) {
    byte_comparable_test(integer_test_data_generator<int8_t>());
}

BOOST_AUTO_TEST_CASE(test_smallint) {
    byte_comparable_test(integer_test_data_generator<int16_t>());
}

BOOST_AUTO_TEST_CASE(test_int) {
    byte_comparable_test(integer_test_data_generator<int32_t>());
}

BOOST_AUTO_TEST_CASE(test_bigint) {
    byte_comparable_test(integer_test_data_generator<int64_t>());
}
