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

BOOST_AUTO_TEST_CASE(test_bool) {
    for (bool value : {true, false}) {
        auto comparable_bytes = comparable_bytes::from_data_value(value);
        BOOST_REQUIRE_EQUAL(comparable_bytes->size(), 1);
        BOOST_REQUIRE_MESSAGE(comparable_bytes->as_managed_bytes_view().front() == uint8_t(value ? 1 : 0),
                              fmt::format("comparable bytes encode failed for bool value : {}", value));
        BOOST_REQUIRE_MESSAGE(value == comparable_bytes->to_data_value(boolean_type),
                              fmt::format("comparable bytes decode failed for bool value : {}", value));
    }
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
    integer_test_data_generator(
        // Function to create a data_value from the underlying integer type.
        std::function<data_value(int_type)> create_data_value_func = {},
        // Function to filter out values that should not be included in the test data.
        std::function<bool(int_type)> filter_func = {}) requires std::is_integral_v<int_type>
    {
        if (!create_data_value_func) {
            if constexpr (std::is_signed_v<int_type>) {
                // If a custom create_data_value_fn is not provided, create data_value
                // directly from the underlying integer type.
                create_data_value_func = [] (int_type num) {
                    return data_value(num);
                };
            } else {
                // For unsigned integer types, the caller must provide a custom create_data_value_fn,
                // as the data_value class doesn't have an unambiguous constructor for unsigned values.
                SCYLLA_ASSERT(false);
            }
        }

        // Generates test values by shifting bit(1) through all possible positions and then deriving
        // multiple test cases from each value. This helps test edge cases and boundary conditions
        // by covering values with different bit patterns across the entire range of the type.
        auto num = int_type(1);
        auto num_bits = sizeof(int_type) * 8;
        _test_data.reserve(num_bits * 4);
        while(num_bits-- > 0) {
            for (int_type n : std::initializer_list<int_type>{num, num - 1, ~num, ~(num - 1)}) {
                if (!filter_func || filter_func(n)) {
                    _test_data.push_back(create_data_value_func(n));
                }
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

BOOST_AUTO_TEST_CASE(test_date) {
    byte_comparable_test(integer_test_data_generator<uint32_t>([] (uint32_t days) {
        return data_value(simple_date_native_type{days});
    }));
}

BOOST_AUTO_TEST_CASE(test_time) {
    constexpr int64_t max_ns_in_a_day = 24L * 60 * 60 * 1000 * 1000 * 1000;
    byte_comparable_test(integer_test_data_generator<int64_t>([] (int64_t nanoseconds) {
        return data_value(time_native_type{nanoseconds});
    }, [] (int64_t ns_candidate) {
        // allow only valid nanosecond values
        return ns_candidate >= 0 && ns_candidate <= max_ns_in_a_day;
    }));
}

BOOST_AUTO_TEST_CASE(test_timestamp) {
    byte_comparable_test(integer_test_data_generator<db_clock::rep>([] (db_clock::rep milliseconds) {
        return data_value(db_clock::time_point(db_clock::duration(milliseconds)));
    }));
}

template <std::floating_point fp_type>
struct floating_point_test_data_generator : test_data_generator {
    floating_point_test_data_generator() {
        for (fp_type n : {-1e30f, -1e3f, -1.0f, -0.001f, -1e-30f, -0.0f, 0.0f, 1e-30f, 0.001f, 1.0f, 1e3f, 1e30f,
                    -std::numeric_limits<float>::min(), std::numeric_limits<float>::min(),
                    -std::numeric_limits<float>::max(), std::numeric_limits<float>::max(),
                    -std::numeric_limits<float>::infinity(), std::numeric_limits<float>::infinity(),
                    std::numeric_limits<float>::quiet_NaN()}) {
            _test_data.emplace_back(n);
        }

        // double has a few more test items
        if constexpr (std::is_same_v<fp_type, double>) {
            for (fp_type n : std::vector<double>{-1e200, -1e100, 1e100, 1e200,
                        -std::numeric_limits<double>::min(), std::numeric_limits<double>::min(),
                        -std::numeric_limits<double>::max(), std::numeric_limits<double>::max()}) {
                _test_data.emplace_back(n);
            }
        }
    }
};

BOOST_AUTO_TEST_CASE(test_float) {
    byte_comparable_test(floating_point_test_data_generator<float>());
}

BOOST_AUTO_TEST_CASE(test_double) {
    byte_comparable_test(floating_point_test_data_generator<double>());
}
