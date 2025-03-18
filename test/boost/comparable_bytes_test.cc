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
#include "utils/big_decimal.hh"
#include "utils/comparable_bytes.hh"
#include "utils/multiprecision_int.hh"
#include "utils/UUID.hh"
#include "utils/UUID_gen.hh"

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

extern void encode_varint_length(uint64_t length, uint8_t sign_only_byte, bytes_ostream& out);
extern uint64_t decode_varint_length(managed_bytes_view& src, uint8_t sign_only_byte);
BOOST_AUTO_TEST_CASE(test_varint_length_encoding) {
    for (int shift = 0; shift <= 64; shift++) {
        uint64_t length = (uint64_t(1) << shift) - 1;
        for (uint8_t sign_only_byte : {0, 0xFF}) {
            bytes_ostream out;
            encode_varint_length(length, sign_only_byte, out);
            auto mbv = managed_bytes_view(std::move(out).to_managed_bytes());
            BOOST_REQUIRE_EQUAL(length, decode_varint_length(mbv, sign_only_byte));
        }
    }
}

BOOST_AUTO_TEST_CASE(test_varint) {
    struct multiprecision_test_data_generator : integer_test_data_generator<int64_t> {
        multiprecision_test_data_generator()
            : integer_test_data_generator<int64_t>([](int64_t n) {
                return data_value(utils::multiprecision_int(n));
            }) {
            // include more large numbers in the testcase
            _test_data.reserve(_test_data.size() + (20 * 4 * 4));
            auto multiprecision_one = utils::multiprecision_int(1);
            for (int shift = 1; shift <= 20; shift++) {
                for (auto shift_prod : {64, 100, 256, 512}) {
                    auto mp_num = multiprecision_one << shift * shift_prod;
                    for (auto n : std::initializer_list<utils::multiprecision_int>{mp_num, mp_num - 1, -mp_num, -(mp_num - 1)}) {
                        _test_data.emplace_back(n);
                    }
                }
            }
        }
    };

    byte_comparable_test(multiprecision_test_data_generator());
}

struct uuid_test_data_generator : test_data_generator {
    void generate_timeuuids(bool create_timeuuid_native_type) {
        std::function<data_value(utils::UUID&&)> create_data_value;
        if (create_timeuuid_native_type) {
            // create data_value for timeuuid data type
            create_data_value = [] (utils::UUID&& time_uuid) { return data_value(timeuuid_native_type(std::move(time_uuid))); };
        } else {
            // create data_value for uuid data type
            create_data_value = [] (utils::UUID&& time_uuid) { return data_value(std::move(time_uuid)); };
        }

        std::random_device rd;
        std::mt19937 gen(rd());
        // timestamp in timeuuid should fit within 60 bits and rest is reserved for version, so use int32 to generate them
        std::uniform_int_distribution<int32_t> dist(std::numeric_limits<int32_t>::min());

        // test uuids with same timestamp but random clock sequences
        auto timestamp = std::chrono::milliseconds{dist(gen)};
        for (auto i = 0; i < 20; i++) {
            _test_data.push_back(create_data_value(utils::UUID_gen::get_random_time_UUID_from_micros(timestamp)));
        }

        // test uuids with random timestamp but same clock sequence
        for (auto i = 0; i < 20; i++) {
            _test_data.push_back(create_data_value(utils::UUID_gen::get_time_UUID(std::chrono::milliseconds{dist(gen)}, 28051990)));
        }

        // test uuids with random timestamp and clock sequences
        for (auto i = 0; i < 20; i++) {
            _test_data.push_back(create_data_value(
                utils::UUID_gen::get_random_time_UUID_from_micros(std::chrono::milliseconds{dist(gen)})));
        }
    }

    void generate_uuids() {
        // test few edge cases
        _test_data.emplace_back(utils::null_uuid());
        _test_data.emplace_back(utils::UUID(std::numeric_limits<int64_t>::max(), std::numeric_limits<int64_t>::max()));
        _test_data.emplace_back(utils::UUID(std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::min()));
        _test_data.emplace_back(utils::UUID("ffffffff-ffff-ffff-ffff-ffffffffffff"));
        // test name based, type 3 uuids
        _test_data.emplace_back(utils::UUID_gen::get_name_UUID("scylladb"));
        _test_data.emplace_back(utils::UUID_gen::get_name_UUID("lakshminarayanansreethar"));

        // generate few random uuids
        for (auto i = 0; i < 50; i++) {
            _test_data.emplace_back(utils::make_random_uuid());
        }
    }
};

BOOST_AUTO_TEST_CASE(test_timeuuid) {
    uuid_test_data_generator tdg;
    tdg.generate_timeuuids(true);
    byte_comparable_test(std::move(tdg));
}

BOOST_AUTO_TEST_CASE(test_uuid) {
    uuid_test_data_generator tdg;
    tdg.generate_uuids();
    tdg.generate_timeuuids(false);
    byte_comparable_test(std::move(tdg));
}

extern std::size_t count_digits(const boost::multiprecision::cpp_int& value);
BOOST_AUTO_TEST_CASE(test_count_digits) {
    auto test_precision = [] (boost::multiprecision::cpp_int&& num) {
        const auto expected_length = num.str().length();
        BOOST_REQUIRE_EQUAL(count_digits(num), expected_length);
        BOOST_REQUIRE_EQUAL(count_digits(-num), expected_length);
    };

    test_precision(boost::multiprecision::cpp_int("0"));
    test_precision(boost::multiprecision::cpp_int("123"));
    test_precision(boost::multiprecision::cpp_int("123456"));
    test_precision(boost::multiprecision::cpp_int("12345600"));
    test_precision(boost::multiprecision::cpp_int("9999999"));
    test_precision(boost::multiprecision::cpp_int(
        "123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"));
}

BOOST_AUTO_TEST_CASE(test_decimal) {
    struct big_decimal_test_data_generator : test_data_generator {
        big_decimal_test_data_generator() {
            // generate few multiprecision ints to be used as unscaled_values in the big_decimal
            std::vector<boost::multiprecision::cpp_int> unscaled_values;
            auto multiprecision_one = utils::multiprecision_int(1);
            for (int shift = 1; shift <= 10; shift++) {
                for (auto shift_prod : {1, 2, 4, 8, 10, 32, 64, 100, 256}) {
                    auto mp_num = multiprecision_one << shift * shift_prod;
                    for (auto n : std::initializer_list<utils::multiprecision_int>{mp_num, mp_num - 1, -mp_num, -(mp_num - 1)}) {
                        unscaled_values.push_back(std::move(n));
                    }
                }
            }
            std::vector<int32_t> scales{1, 2, 4, 5, 10, 100, 1000};

            _test_data.reserve(unscaled_values.size() * scales.size() * 5);
            for (const auto &unscaled_value : unscaled_values) {
                _test_data.emplace_back(big_decimal(0, unscaled_value));
                _test_data.emplace_back(big_decimal(std::numeric_limits<int32_t>::min(), unscaled_value));
                _test_data.emplace_back(big_decimal(std::numeric_limits<int32_t>::max(), unscaled_value));
                for (const auto &scale : scales) {
                    _test_data.emplace_back(big_decimal(scale, unscaled_value));
                    _test_data.emplace_back(big_decimal(-scale, unscaled_value));
                }
            }
        }
    };

    byte_comparable_test(big_decimal_test_data_generator());
}
