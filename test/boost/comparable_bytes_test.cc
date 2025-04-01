/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */
#define BOOST_TEST_MODULE core

#include <boost/test/unit_test.hpp>
#include <seastar/net/inet_address.hh>
#include <seastar/net/ipv4_address.hh>
#include <seastar/util/lazy.hh>

#include "bytes_ostream.hh"
#include "test/lib/log.hh"
#include "test/lib/random_utils.hh"
#include "types/types.hh"
#include "types/comparable_bytes.hh"
#include "utils/big_decimal.hh"
#include "utils/multiprecision_int.hh"
#include "utils/UUID.hh"
#include "utils/UUID_gen.hh"

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
        if (test_data_type == decimal_type) {
            // The `decimal_type` requires special handling because its comparable byte representation
            // normalizes the scale and unscaled value. This means the serialized bytes after
            // decoding from comparable bytes might not be identical to the original serialized bytes,
            // despite them representing the same decimal value.
            // For instance, 2e-1 (scale=1, unscaled_value=2) and 20e-2 (scale=2, unscaled_value=20)
            // are equivalent decimals but have different serialized forms. Comparable byte encoding
            // will normalize them. So, instead of directly comparing serialized bytes, compare the
            // deserialized decoded value against the original decimal value.
            auto decoded_value = decimal_type->deserialize_value(managed_bytes_view(decoded_serialized_bytes));
            BOOST_REQUIRE_MESSAGE(value == decoded_value, seastar::value_of([&] () {
                return fmt::format("comparable bytes encode/decode failed for value : {}", value);
            }));
        } else {
            // Compare the serialized bytes directly
            BOOST_REQUIRE_MESSAGE(original_serialized_bytes == decoded_serialized_bytes, seastar::value_of([&] () {
                return fmt::format("comparable bytes encode/decode failed for value : {}", value);
            }));
        }

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

void encode_varint_length(uint64_t length, int64_t sign_mask, bytes_ostream& out);
uint64_t decode_varint_length(managed_bytes_view& src, int64_t sign_only_byte);
BOOST_AUTO_TEST_CASE(test_varint_length_encoding) {
    for (int shift = 0; shift < 64; shift++) {
        uint64_t length = (uint64_t(1) << shift) - 1;
        for (int64_t sign_mask : {0, -1}) {
            bytes_ostream out;
            encode_varint_length(length, sign_mask, out);
            auto mb = std::move(out).to_managed_bytes();
            auto mbv = managed_bytes_view(mb);
            BOOST_REQUIRE_EQUAL(length, decode_varint_length(mbv, sign_mask));
        }
    }
}

BOOST_AUTO_TEST_CASE(test_varint) {
    // Generate small integers
    std::vector<data_value> test_data = generate_integer_test_data<int64_t>([] (int64_t n) {
        return data_value(utils::multiprecision_int(n));
    });

    // Generate more large numbers
    test_data.reserve(test_data.size() + (20 * 4 * 4));
    auto multiprecision_one = utils::multiprecision_int(1);
    for (int shift = 1; shift <= 20; shift++) {
        for (auto shift_multiplier : {64, 100, 256, 512}) {
            auto large_number = multiprecision_one << shift * shift_multiplier;
            for (auto number : std::initializer_list<utils::multiprecision_int>{large_number, large_number - 1, -large_number, -(large_number - 1)}) {
                test_data.emplace_back(number);
            }
        }
    }
    byte_comparable_test(std::move(test_data));
}

static int64_t msb_with_version(int64_t msb, int version) {
    // Set the version bits in the msb of the UUID
    return (msb & ~(0xF << 12)) | (version << 12);
}

static void test_uuid_and_flipped_uuid(utils::UUID&& uuid, std::vector<data_value>& test_data,
        std::function<data_value(utils::UUID&&)>& create_data_value) {
    auto uuid_dv = create_data_value(std::move(uuid));
    // negate the uuid to create a flipped version
    auto flipped_uuid = utils::UUID_gen::negate(uuid);
    auto flipped_uuid_dv = create_data_value(std::move(flipped_uuid));
    // verify that the original and flipped uuids compare correctly in byte-comparable format
    BOOST_REQUIRE(uuid <=> flipped_uuid == comparable_bytes::from_data_value(uuid_dv) <=> comparable_bytes::from_data_value(flipped_uuid_dv));
    // add both original and flipped uuids to the test data
    test_data.push_back(std::move(uuid_dv));
    test_data.push_back(std::move(flipped_uuid_dv));
}

static std::vector<data_value> generate_timeuuid_test_data(bool create_timeuuid_native_type) {
    std::function<data_value(utils::UUID&&)> create_data_value;
    if (create_timeuuid_native_type) {
        // create data_value for timeuuid data type
        create_data_value = [] (utils::UUID&& time_uuid) {
            return data_value(timeuuid_native_type(std::move(time_uuid)));
        };
    } else {
        // create data_value for uuid data type
        create_data_value = [] (utils::UUID&& time_uuid) {
            return data_value(std::move(time_uuid));
        };
    }

    std::vector<data_value> test_data;
    for (auto [msb, lsb] : std::initializer_list<std::pair<int64_t, int64_t>>{
                 {0, 0},
                 {std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::min()},
                 {std::numeric_limits<int64_t>::max(), std::numeric_limits<int64_t>::max()},
         }) {
        test_uuid_and_flipped_uuid(utils::UUID(msb_with_version(msb, 1), lsb), test_data, create_data_value);
    }

    for (int i = 0; i < 500; i++) {
        // Generate a random msb with version set to 1 (time-based UUID)
        test_uuid_and_flipped_uuid(
                utils::UUID(msb_with_version(tests::random::get_int<int64_t>(std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::max()), 1),
                        tests::random::get_int<int64_t>(std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::max())),
                test_data, create_data_value);
    }

    return test_data;
}

BOOST_AUTO_TEST_CASE(test_timeuuid) {
    byte_comparable_test(generate_timeuuid_test_data(true));
}

BOOST_AUTO_TEST_CASE(test_uuid) {
    // generate time uuids
    auto test_data = generate_timeuuid_test_data(false);

    // test few edge cases
    test_data.emplace_back(utils::null_uuid());
    test_data.emplace_back(utils::UUID(std::numeric_limits<int64_t>::max(), std::numeric_limits<int64_t>::max()));
    test_data.emplace_back(utils::UUID(std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::min()));
    test_data.emplace_back(utils::UUID("ffffffff-ffff-ffff-ffff-ffffffffffff"));
    // test name based, type 3 uuids
    test_data.emplace_back(utils::UUID_gen::get_name_UUID("scylladb"));
    test_data.emplace_back(utils::UUID_gen::get_name_UUID("lakshminarayanansreethar"));

    // generate few random uuids
    std::function<data_value(utils::UUID&&)> create_data_value = [] (utils::UUID&& time_uuid) {
        return data_value(std::move(time_uuid));
    };
    for (auto i = 0; i < 500; i++) {
        // Generate a random msb with version set to 4
        test_uuid_and_flipped_uuid(
                utils::UUID(msb_with_version(tests::random::get_int<int64_t>(std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::max()), 4),
                        tests::random::get_int<int64_t>(std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::max())),
                test_data, create_data_value);
    }

    byte_comparable_test(std::move(test_data));
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
    // scales to generate the big_decimal
    std::vector<int32_t> scales{1, 2, 4, 5, 10, 100, 1000};

    std::vector<data_value> _test_data;
    _test_data.reserve(unscaled_values.size() * scales.size() * 5);
    for (const auto& unscaled_value : unscaled_values) {
        _test_data.emplace_back(big_decimal(0, unscaled_value));
        _test_data.emplace_back(big_decimal(std::numeric_limits<int32_t>::min(), unscaled_value));
        _test_data.emplace_back(big_decimal(std::numeric_limits<int32_t>::max(), unscaled_value));
        for (const auto& scale : scales) {
            _test_data.emplace_back(big_decimal(scale, unscaled_value));
            _test_data.emplace_back(big_decimal(-scale, unscaled_value));
        }
    }

    byte_comparable_test(std::move(_test_data));
}

BOOST_AUTO_TEST_CASE(test_blob) {
    auto random_bytes = [] (size_t length) {
        std::vector<int8_t> data(length);
        for (auto& byte : data) {
            byte = tests::random::get_int<uint8_t>();
        }
        return bytes(reinterpret_cast<const int8_t*>(data.data()), length);
    };

    std::vector<data_value> test_data;
    test_data.reserve(500);
    for (int i = 0; i < 100; i++) {
        for (int length : {1, 10, 100, 1000}) {
            test_data.emplace_back(random_bytes(length));
        }
    }

    // test a few cases that are stored across multiple fragments
    for (int i = 0; i < 10; i++) {
        for (int frag_count = 1; frag_count <= 10; frag_count++) {
            const size_t length = 128 * 1024 * frag_count;
            test_data.emplace_back(random_bytes(length));
        }
    }

    byte_comparable_test(std::move(test_data));
}

static std::vector<data_value> generate_string_test_data(
    std::function<data_value(std::string&&)> create_data_value_func) {
    const std::string charset = "0123456789"
                                "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                "abcdefghijklmnopqrstuvwxyz";

    auto random_text = [&charset] (size_t length) {
        std::string generated_text;
        generated_text.reserve(length);
        for (size_t i = 0; i < length; ++i) {
            generated_text += charset[tests::random::get_int<size_t>(0, charset.size() - 1)];
        }
        return generated_text;
    };

    std::vector<data_value> test_data;
    test_data.reserve(500);
    for (int i = 0; i < 100; i++) {
        for (int length : {1, 10, 100, 1000}) {
            test_data.push_back(create_data_value_func(random_text(length)));
        }
    }

    // test a few cases that are stored across multiple fragments
    for (int i = 0; i < 10; i++) {
        for (int frag_count = 1; frag_count <= 10; frag_count++) {
            const size_t length = 128 * 1024 * frag_count;
            test_data.push_back(create_data_value_func(random_text(length)));
        }
    }

    return test_data;
}

BOOST_AUTO_TEST_CASE(test_ascii) {
    byte_comparable_test(generate_string_test_data([] (std::string&& str) {
        return data_value(ascii_native_type(str));
    }));
}

BOOST_AUTO_TEST_CASE(test_text) {
    byte_comparable_test(generate_string_test_data([] (std::string&& str) {
        return data_value(str);
    }));
}

BOOST_AUTO_TEST_CASE(test_duration) {
    constexpr int64_t max_ns_in_a_day = 24L * 60 * 60 * 1000 * 1000 * 1000;
    std::vector<data_value> test_data;
    test_data.reserve(1000);
    for (int i = 0; i < 1000; i++) {
        const auto months = months_counter{tests::random::get_int<int32_t>(0, 12)};
        const auto days = days_counter{tests::random::get_int<int32_t>(0, 28)};
        const auto ns = nanoseconds_counter{tests::random::get_int<int64_t>(0, max_ns_in_a_day)};
        test_data.emplace_back(cql_duration(months, days, ns));
    }

    byte_comparable_test(std::move(test_data));
}

BOOST_AUTO_TEST_CASE(test_inet) {
    auto test_data = generate_integer_test_data<uint32_t>([](uint32_t value) {
        return data_value(seastar::net::ipv4_address(value));
    });

    // Include few more addresses
    for (const std::string& addr : {
                 // IPv4
                 "127.0.0.1",
                 "10.0.0.1",
                 "172.16.1.1",
                 "192.168.2.2",
                 "224.3.3.3",
                 // IPv6
                 "0000:0000:0000:0000:0000:0000:0000:0000",
                 "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
                 "fe80:1:23:456:7890:1:23:456",
         }) {
        test_data.emplace_back(seastar::net::inet_address(addr));
    }

    byte_comparable_test(std::move(test_data));
}
