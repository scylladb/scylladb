/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */
#define BOOST_TEST_MODULE core

#include <boost/test/unit_test.hpp>
#include <seastar/util/lazy.hh>

#include "bytes_ostream.hh"
#include "test/lib/log.hh"
#include "test/lib/random_utils.hh"
#include "types/types.hh"
#include "types/comparable_bytes.hh"

BOOST_AUTO_TEST_CASE(test_comparable_bytes_opt) {
    BOOST_REQUIRE(comparable_bytes::from_data_value(data_value::make_null(int32_type)) == comparable_bytes_opt());
    BOOST_REQUIRE(comparable_bytes::from_serialized_bytes(*int32_type, managed_bytes_opt()) == comparable_bytes_opt());
}

BOOST_AUTO_TEST_CASE(test_bool) {
    auto test_bool_value = [] (comparable_bytes_opt& comparable_bytes, bool value) {
        BOOST_REQUIRE_EQUAL(comparable_bytes->size(), 1);
        BOOST_REQUIRE_MESSAGE(comparable_bytes->as_managed_bytes_view().front() == uint8_t(value),
                              fmt::format("comparable bytes encode failed for bool value : {}", value));
        BOOST_REQUIRE_MESSAGE(value == comparable_bytes->to_data_value(boolean_type),
                              fmt::format("comparable bytes decode failed for bool value : {}", value));
    };

    auto cb_false = comparable_bytes::from_data_value(false);
    test_bool_value(cb_false, false);
    auto cb_true = comparable_bytes::from_data_value(true);
    test_bool_value(cb_true, true);
    // Verify order
    BOOST_REQUIRE(cb_false < cb_true);
}

void byte_comparable_test(std::vector<data_value>&& test_data) {
    struct test_item {
        managed_bytes serialized_bytes;
        comparable_bytes comparable_bytes;
    };
    std::vector<test_item> test_items;

    // test encode/decode
    const auto test_data_type = test_data.at(0).type();
    testlog.info("testing type '{}' with {} items...", test_data_type.get()->cql3_type_name(), test_data.size());
    testlog.trace("test data : {}", test_data);
    for (const data_value& value : test_data) {
        // verify comparable bytes encode/decode
        auto original_serialized_bytes = managed_bytes(value.serialize_nonnull());
        comparable_bytes comparable_bytes(*test_data_type, original_serialized_bytes);
        auto decoded_serialized_bytes = comparable_bytes.to_serialized_bytes(*test_data_type).value();
        BOOST_REQUIRE_MESSAGE(original_serialized_bytes == decoded_serialized_bytes, seastar::value_of([&] () {
            return fmt::format("comparable bytes encode/decode failed for value : {}", value);
        }));

        // collect the data in a vector to verify ordering later
        test_items.emplace_back(original_serialized_bytes, comparable_bytes);
    };

    // Verify that decoding succeeds even when the comparable bytes contain
    // extra data appended after the value to be converted.
    // This required for decode to work on composite types.
    bytes_ostream bos;
    // Select an item from the middle to test this case as front and back items
    // are often edge cases (e.g. min/max values).
    const auto item_id = test_items.size() / 2;
    auto test_value = test_items.at(item_id);
    auto cb_view = test_value.comparable_bytes.as_managed_bytes_view();
    bos.write(cb_view);
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
static std::vector<data_value> generate_integer_test_data(
        // Function to create a data_value from the underlying integer type.
        std::function<data_value(int_type)> create_data_value_func = {},
        // Function to filter out values that should not be included in the test data.
        std::function<bool(int_type)> filter_func = {}) {
    if (!create_data_value_func) {
        if constexpr (std::is_signed_v<int_type>) {
            // If a custom create_data_value_fn is not provided, create data_value
            // directly from the underlying integer type.
            create_data_value_func = [](int_type num) {
                return data_value(num);
            };
        } else {
            // For unsigned integer types, the caller must provide a custom create_data_value_fn,
            // as the data_value class doesn't have an unambiguous constructor for unsigned values.
            SCYLLA_ASSERT(false);
        }
    }

    std::vector<data_value> test_data;
    auto push_to_test_data = [&] (int_type num) {
        for (int_type n : std::initializer_list<int_type>{num, ~num}) {
            if (!filter_func || filter_func(n)) {
                test_data.push_back(create_data_value_func(n));
            }
        }
    };

    // Generates test values by shifting bit(1) through all possible positions and then deriving
    // multiple test cases from each value. This helps test edge cases and boundary conditions
    // by covering values with different bit patterns across the entire range of the type.
    auto num = int_type(1);
    auto num_bits = sizeof(int_type) * 8;
    test_data.reserve(num_bits * 4);
    while (num_bits-- > 0) {
        // for every num, we push [num, ~num, num - 1, ~(num - 1)] to the test data.
        push_to_test_data(num);
        if (num != std::numeric_limits<int_type>::min()) {
            push_to_test_data(num - 1);
        }

        num <<= 1;
    }

    return test_data;
}

BOOST_AUTO_TEST_CASE(test_tinyint) {
    byte_comparable_test(generate_integer_test_data<int8_t>());
}

BOOST_AUTO_TEST_CASE(test_smallint) {
    byte_comparable_test(generate_integer_test_data<int16_t>());
}

BOOST_AUTO_TEST_CASE(test_int) {
    byte_comparable_test(generate_integer_test_data<int32_t>());
}

BOOST_AUTO_TEST_CASE(test_bigint) {
    byte_comparable_test(generate_integer_test_data<int64_t>());
}

BOOST_AUTO_TEST_CASE(test_date) {
    byte_comparable_test(generate_integer_test_data<uint32_t>([] (uint32_t days) {
        return data_value(simple_date_native_type{days});
    }));
}

BOOST_AUTO_TEST_CASE(test_time) {
    constexpr int64_t max_ns_in_a_day = 24L * 60 * 60 * 1000 * 1000 * 1000;
    byte_comparable_test(generate_integer_test_data<int64_t>([] (int64_t nanoseconds) {
        return data_value(time_native_type{nanoseconds});
    }, [] (int64_t ns_candidate) {
        // allow only valid nanosecond values
        return ns_candidate >= 0 && ns_candidate <= max_ns_in_a_day;
    }));
}

BOOST_AUTO_TEST_CASE(test_timestamp) {
    byte_comparable_test(generate_integer_test_data<db_clock::rep>([] (db_clock::rep milliseconds) {
        return data_value(db_clock::time_point(db_clock::duration(milliseconds)));
    }));
}

template <std::floating_point fp_type>
static std::vector<data_value> generate_floating_point_test_data() {
    std::vector<data_value> test_data;
    for (fp_type n : {-1e30f, -1e3f, -1.0f, -0.001f, -1e-30f, -0.0f, 0.0f, 1e-30f, 0.001f, 1.0f, 1e3f, 1e30f,
                -std::numeric_limits<float>::min(), std::numeric_limits<float>::min(),
                -std::numeric_limits<float>::max(), std::numeric_limits<float>::max(),
                -std::numeric_limits<float>::infinity(), std::numeric_limits<float>::infinity(),
                std::numeric_limits<float>::quiet_NaN()}) {
        test_data.emplace_back(n);
    }

    // double has a few more test items
    int random_exponent_min = -30, random_exponent_max = 30;
    if constexpr (std::is_same_v<fp_type, double>) {
        for (fp_type n : std::vector<double>{-1e200, -1e100, 1e100, 1e200,
                    -std::numeric_limits<double>::min(), std::numeric_limits<double>::min(),
                    -std::numeric_limits<double>::max(), std::numeric_limits<double>::max()}) {
            test_data.emplace_back(n);
        }
        random_exponent_min = -300;
        random_exponent_max = 300;
    }

    // generate some random test data
    for (int i = 0; i < 100; i++) {
        const auto significand = tests::random::get_int<int64_t>(std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::max());
        const auto scale = std::pow(10, tests::random::get_int<int>(random_exponent_min, random_exponent_max));
        test_data.push_back(fp_type(significand * scale));
    }

    return test_data;
}

BOOST_AUTO_TEST_CASE(test_float) {
    byte_comparable_test(generate_floating_point_test_data<float>());
}

BOOST_AUTO_TEST_CASE(test_double) {
    byte_comparable_test(generate_floating_point_test_data<double>());
}
