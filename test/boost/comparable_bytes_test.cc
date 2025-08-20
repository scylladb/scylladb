/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */
#include "test/lib/scylla_test_case.hh"

#include <seastar/net/inet_address.hh>
#include <seastar/net/ipv4_address.hh>
#include <seastar/util/lazy.hh>
#include <vector>

#include "bytes_ostream.hh"
#include "test/lib/log.hh"
#include "test/lib/random_utils.hh"
#include "test/lib/sstable_test_env.hh"
#include "types/comparable_bytes.hh"
#include "types/list.hh"
#include "types/map.hh"
#include "types/set.hh"
#include "types/types.hh"
#include "types/vector.hh"
#include "utils/big_decimal.hh"
#include "utils/fragment_range.hh"
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

void byte_comparable_test(std::vector<data_value>&& test_data, bool test_reversed_type = false) {
    struct test_item {
        managed_bytes serialized_bytes;
        comparable_bytes comparable_bytes;
    };
    std::vector<test_item> test_items;

    // test encode/decode
    const auto test_data_type = test_reversed_type ? reversed(test_data.at(0).type()) : test_data.at(0).type();
    testlog.info("testing type '{}' with {} items...",
        test_reversed_type ? format("reversed<{}>", test_data_type.get()->cql3_type_name()) : test_data_type.get()->cql3_type_name(),
        test_data.size());
    testlog.trace("test data : {}", test_data);
    for (const data_value& value : test_data) {
        // verify comparable bytes encode/decode
        auto original_serialized_bytes = managed_bytes(value.serialize_nonnull());
        comparable_bytes comparable_bytes(*test_data_type, original_serialized_bytes);
        auto decoded_serialized_bytes = comparable_bytes.to_serialized_bytes(*test_data_type).value();
        if (test_data_type == decimal_type || test_data_type->is_tuple()) {
            // 1. The `decimal_type` requires special handling because its comparable byte representation
            // normalizes the scale and unscaled value. This means the serialized bytes after
            // decoding from comparable bytes might not be identical to the original serialized bytes,
            // despite them representing the same decimal value.
            // For instance, 2e-1 (scale=1, unscaled_value=2) and 20e-2 (scale=2, unscaled_value=20)
            // are equivalent decimals but have different serialized forms. Comparable byte encoding
            // will normalize them. So, instead of directly comparing serialized bytes, compare the
            // deserialized decoded value against the original decimal value.
            // 2. When encoding `tuple_type`, any trailing nulls are trimmed, so the serialized bytes
            // cannot be compared directly.
            auto decoded_value = test_data_type->deserialize_value(managed_bytes_view(decoded_serialized_bytes));
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

static data_value make_random_data_value_uuid() { return data_value(utils::make_random_uuid()); }
static data_value make_random_data_value_bytes() {
    constexpr size_t max_bytes_size = 128 * 1024; // 128 KB
    return data_value(tests::random::get_bytes(tests::random::get_int<size_t>(1, max_bytes_size)));
}

extern void encode_component(const abstract_type& type, managed_bytes_view serialized_bytes_view, bytes_ostream& out);
extern void decode_component(const abstract_type& type, managed_bytes_view& comparable_bytes_view, bytes_ostream& out);
BOOST_AUTO_TEST_CASE(test_encode_decode_component) {
    // Verify encode and decode works
    bytes_ostream out;
    constexpr uint8_t NEXT_COMPONENT = 0x40;
    for (const auto& test_value : {
        make_random_data_value_uuid(), // data type with fixed length
        make_random_data_value_bytes(), // data type with variable length
    }) {
        const auto& type = *test_value.type();
        out.clear();
        auto serialized_bytes = test_value.serialize_nonnull();
        encode_component(type, managed_bytes_view(serialized_bytes), out);
        auto comparable_bytes = std::move(out).to_managed_bytes();
        auto comparable_bytes_view = managed_bytes_view(comparable_bytes);
        // encoded component should begin with a NEXT_COMPONENT marker
        BOOST_REQUIRE_EQUAL(read_simple_native<uint8_t>(comparable_bytes_view), NEXT_COMPONENT);
        out.clear();
        decode_component(type, comparable_bytes_view, out);
        auto decoded_bytes = std::move(out).to_managed_bytes();
        auto decoded_bytes_view = managed_bytes_view(decoded_bytes);
        // decoded bytes should match the serialized form
        BOOST_REQUIRE_EQUAL(read_simple<int32_t>(decoded_bytes_view), test_value.serialized_size());
        BOOST_REQUIRE(decoded_bytes_view == managed_bytes_view(serialized_bytes));
    }
}

// Generates a vector of vectors of data_value, where each inner vector represents a collection of data_values.
template<size_t collection_size = 0>
static auto generate_collection_test_data(const std::function<data_value()>& create_data_value) {
    constexpr size_t test_data_size = 500, max_collection_size = 25;
    std::vector<std::vector<data_value>> test_data;
    test_data.reserve(test_data_size + 21);
    for (size_t i = 0; i < test_data_size; i++) {
        // Generate a single collection and add it to test data
        std::vector<data_value> collection;
        if constexpr (collection_size == 0) {
            collection.reserve(tests::random::get_int<size_t>(1, max_collection_size));
        } else {
            collection.reserve(collection_size);
        }
        for (size_t j = 0; j < collection.capacity(); j++) {
            collection.push_back(create_data_value());
        }
        test_data.push_back(std::move(collection));
    }

    // Include few duplicates in the test data with variations
    for (int i = 0; i < 10; i++) {
        test_data.emplace_back(test_data.at(tests::random::get_int<size_t>(test_data_size - 1)));
        // include a partial duplicate
        auto test_item = test_data.at(tests::random::get_int<size_t>(test_data_size - 1));
        test_data.emplace_back(test_item.begin(), test_item.begin() + tests::random::get_int<size_t>(1, test_item.size()));
        if constexpr (collection_size != 0) {
            // For fixed-size collections, the partial duplicate must be padded with random data to meet the required size.
            auto& partial_duplicate = test_data.back();
            while (partial_duplicate.size() < collection_size) {
                partial_duplicate.push_back(create_data_value());
            }
        }
    }

    if constexpr (collection_size == 0) {
        // Add an empty collection to the test data
        test_data.push_back({});
    }

    return test_data;
}

// Common test method for lists and sets. Note that a set is expected to be sorted and unique,
// but it doesn't matter during tests, as both lists and sets internally use the same underlying
// implementation based on std::vectors.
static void test_set_or_list(const std::function<data_type(data_type, bool)>& get_collection_type,
                               const std::function<data_value(data_type, std::vector<data_value>)>& make_collection_value) {
    // Generate vector of collections for each underlying type, with and without
    // multi-cell enabled and run the tests on them.
    auto do_test = [&] (const data_type& underlying_type, std::vector<std::vector<data_value>>&& test_data) {
        for (bool is_multi_cell : {false, true}) {
            std::vector<data_value> collection_test_data;
            collection_test_data.reserve(test_data.size());
            auto collection_type = get_collection_type(underlying_type, is_multi_cell);
            for (const auto& data : test_data) {
                collection_test_data.emplace_back(make_collection_value(collection_type, data));
            }

            byte_comparable_test(std::move(collection_test_data));
        }
    };

    // Test the collection with a data type that has fixed length : UUID (128 bits)
    do_test(uuid_type, generate_collection_test_data(make_random_data_value_uuid));
    // Test the collection with a data type that has variable length : bytes
    do_test(bytes_type, generate_collection_test_data(make_random_data_value_bytes));
}

BOOST_AUTO_TEST_CASE(test_set) {
    test_set_or_list(set_type_impl::get_instance, make_set_value);
}

BOOST_AUTO_TEST_CASE(test_list) {
    test_set_or_list(list_type_impl::get_instance, make_list_value);
}

BOOST_AUTO_TEST_CASE(test_map) {
    // Generate the test data for a map with UUID keys and bytes values.
    constexpr size_t test_data_size = 500, max_entries_per_map = 25;
    std::vector<map_type_impl::native_type> map_test_data;
    map_test_data.reserve(test_data_size + 21);
    for (size_t i = 0; i < test_data_size; i++) {
        map_type_impl::native_type test_item;
        size_t num_entries = tests::random::get_int<size_t>(1, max_entries_per_map);
        for (size_t j = 0; j < num_entries; j++) {
            // Generate a random UUID and a random bytes value
            test_item.emplace_back(make_random_data_value_uuid(), make_random_data_value_bytes());
        }

        // Add the map to the test data
        map_test_data.emplace_back(test_item.begin(), test_item.end());
    }

    // Include duplicates with some variants
    for (int i = 0; i < 10; i++) {
        auto test_item = map_test_data.at(tests::random::get_int<size_t>(test_data_size - 1));
        map_test_data.emplace_back(test_item);
        map_type_impl::native_type duplicate_with_different_values;
        for (const auto& [key, value] : test_item) {
            duplicate_with_different_values.emplace_back(key, make_random_data_value_bytes());
        }
        map_test_data.emplace_back(std::move(duplicate_with_different_values));
    }

    // Add an empty entry to the map
    map_test_data.emplace_back();

    for (bool is_multi_cell : {false, true}) {
        const auto map_type = map_type_impl::get_instance(uuid_type, bytes_type, is_multi_cell);
        std::vector<data_value> collection_test_data;
        collection_test_data.reserve(map_test_data.size());
        for (const auto& data : map_test_data) {
            collection_test_data.emplace_back(make_map_value(map_type, data));
        }

        byte_comparable_test(std::move(collection_test_data));
    }
}

BOOST_AUTO_TEST_CASE(test_tuple) {
    // Generate the test data for tuple with UUID and bytes types
    constexpr int test_data_size = 1000;
    std::vector<data_value> tuple_test_data;
    tuple_test_data.reserve(test_data_size + 30 + 3);
    const auto test_tuple_type = tuple_type_impl::get_instance({uuid_type, bytes_type});
    for (int i = 0; i < test_data_size; i++) {
        tuple_test_data.emplace_back(make_tuple_value(test_tuple_type, {make_random_data_value_uuid(), make_random_data_value_bytes()}));
    }

    // Include few duplicates in the test data with variations
    for (int i = 0; i < 10; i++) {
        auto test_item = value_cast<tuple_type_impl::native_type>(
            tuple_test_data.at(tests::random::get_int<size_t>(test_data_size - 1)));
        tuple_test_data.emplace_back(make_tuple_value(test_tuple_type, {test_item.at(0), make_random_data_value_bytes()}));
        tuple_test_data.emplace_back(make_tuple_value(test_tuple_type, {make_random_data_value_uuid(), test_item.at(1)}));
        tuple_test_data.emplace_back(make_tuple_value(test_tuple_type, {test_item.at(0), test_item.at(1)}));
    }

    // Include tuples with nulls in the testdata
    tuple_test_data.emplace_back(make_tuple_value(test_tuple_type, {make_random_data_value_uuid(), data_value::make_null(bytes_type)}));
    tuple_test_data.emplace_back(make_tuple_value(test_tuple_type, {data_value::make_null(uuid_type), make_random_data_value_bytes()}));
    tuple_test_data.emplace_back(make_tuple_value(test_tuple_type, {data_value::make_null(uuid_type), data_value::make_null(bytes_type)}));

    byte_comparable_test(std::move(tuple_test_data));
}

BOOST_AUTO_TEST_CASE(test_udt) {
    // Generate data for UDT with following types : uuid, bytes, int64_t
    constexpr int test_data_size = 1000;
    std::vector<user_type_impl::native_type> udt_test_data;
    udt_test_data.reserve(test_data_size + 100);
    auto make_random_data_value_int64 = [] () {
        return data_value(tests::random::get_int<int64_t>(std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::max()));
    };
    for (int i = 0; i < test_data_size; i++) {
        udt_test_data.emplace_back(user_type_impl::native_type{
            make_random_data_value_uuid(), make_random_data_value_bytes(), make_random_data_value_int64()});
    }

    // Include few duplicates in the test data with variations
    for (int i = 0; i < 10; i ++) {
        auto test_item = udt_test_data.at(tests::random::get_int<size_t>(test_data_size - 1));
        udt_test_data.emplace_back(user_type_impl::native_type{test_item.at(0), test_item.at(1), make_random_data_value_int64()});
        udt_test_data.emplace_back(user_type_impl::native_type{test_item.at(0), make_random_data_value_bytes(), test_item.at(2)});
        udt_test_data.emplace_back(user_type_impl::native_type{make_random_data_value_uuid(), test_item.at(1), test_item.at(2)});
        udt_test_data.emplace_back(user_type_impl::native_type{test_item.at(0), make_random_data_value_bytes(), make_random_data_value_int64()});
        udt_test_data.emplace_back(user_type_impl::native_type{make_random_data_value_uuid(), test_item.at(1), make_random_data_value_int64()});
        udt_test_data.emplace_back(user_type_impl::native_type{make_random_data_value_uuid(), make_random_data_value_bytes(), test_item.at(2)});
        udt_test_data.emplace_back(test_item);
    }

    // Include tuples with nulls in the testdata
    udt_test_data.emplace_back(user_type_impl::native_type{make_random_data_value_uuid(), make_random_data_value_bytes(), data_value::make_null(long_type)});
    udt_test_data.emplace_back(user_type_impl::native_type{make_random_data_value_uuid(), data_value::make_null(bytes_type), make_random_data_value_int64()});
    udt_test_data.emplace_back(user_type_impl::native_type{data_value::make_null(uuid_type), make_random_data_value_bytes(), make_random_data_value_int64()});
    udt_test_data.emplace_back(user_type_impl::native_type{make_random_data_value_uuid(), data_value::make_null(bytes_type), data_value::make_null(long_type)});
    udt_test_data.emplace_back(user_type_impl::native_type{data_value::make_null(uuid_type), make_random_data_value_bytes(), data_value::make_null(long_type)});
    udt_test_data.emplace_back(user_type_impl::native_type{data_value::make_null(uuid_type), data_value::make_null(bytes_type), make_random_data_value_int64()});
    udt_test_data.emplace_back(user_type_impl::native_type{data_value::make_null(uuid_type), data_value::make_null(bytes_type), data_value::make_null(long_type)});


    // Run the test for both frozen and non frozen types
    for (auto is_multi_cell : {false, true}) {
        const auto test_udt_type = user_type_impl::get_instance("ks_test", "cb_test_udt",
        std::vector<bytes>{"field1", "field2", "field3"},
        std::vector<data_type>{uuid_type, bytes_type, long_type}, is_multi_cell);
        std::vector<data_value> collection_test_data;
        collection_test_data.reserve(udt_test_data.size());
        for (const auto& data : udt_test_data) {
            collection_test_data.emplace_back(make_user_value(test_udt_type, data));
        }

        byte_comparable_test(std::move(collection_test_data));
    }
}

BOOST_AUTO_TEST_CASE(test_vector) {
    auto do_test = [&] (const data_type& underlying_type, std::vector<std::vector<data_value>>&& test_data) {
        std::vector<data_value> collection_test_data;
        collection_test_data.reserve(test_data.size());
        auto collection_type = vector_type_impl::get_instance(underlying_type, test_data.at(0).size());
        for (const auto& data : test_data) {
            collection_test_data.emplace_back(make_vector_value(collection_type, data));
        }

        byte_comparable_test(std::move(collection_test_data));
    };

    // Test the collection with a data type that has fixed length : UUID (128 bits)
    do_test(uuid_type, generate_collection_test_data<128>(make_random_data_value_uuid));
        // Test the collection with a data type that has variable length : bytes
    do_test(bytes_type, generate_collection_test_data<16>(make_random_data_value_bytes));
}

BOOST_AUTO_TEST_CASE(test_reversed) {
    // Test reversed with native types
    byte_comparable_test(generate_integer_test_data<int64_t>(), true);
    byte_comparable_test(generate_string_test_data([] (std::string&& str) {
        return data_value(str);
    }), true);

    // Test reversed with a collection
    const auto list_type = list_type_impl::get_instance(bytes_type, false);
    std::vector<data_value> collection_test_data;
    collection_test_data.reserve(510);
    for (const auto& test_case : generate_collection_test_data(make_random_data_value_bytes)) {
        collection_test_data.emplace_back(make_list_value(list_type, test_case));
    }
    byte_comparable_test(std::move(collection_test_data), true);
}

BOOST_AUTO_TEST_CASE(test_empty) {
    auto test_data = data_value(empty_type_representation{});
    auto test_data_cb = comparable_bytes::from_data_value(test_data);
    BOOST_REQUIRE(test_data_cb->size() == 0);
    BOOST_REQUIRE(test_data == test_data_cb->to_data_value(empty_type));
}

// Test Scylla's byte-comparable encoding compatibility with Cassandra's implementation by
// verifying that serialized values produce the same comparable bytes as those generated by Cassandra.
// The test data was generated using the cassandra unit test pushed to the following branch:
// https://github.com/scylladb/scylla-dev/blob/byte-comparable-compatibility-generator
SEASTAR_TEST_CASE(test_compatibility) {
    return sstables::test_env::do_with_async([] (sstables::test_env&) {
        auto file = open_file_dma("test/resource/byte_comparable_compatibility_data.csv", open_flags::ro).get();
        auto fs = make_file_input_stream(file);
        temporary_buffer<char> buf = fs.read().get();
        // Read file contents in a loop and handle them line by line.
        data_type type;
        std::string input_buffer;
        while (!buf.empty()) {
            input_buffer.append(buf.get(), buf.size());
            size_t pos = 0;
            while (pos != input_buffer.size()) {
                // Extract the CSV entry from the next line
                size_t end = input_buffer.find('\n', pos);
                if (end == std::string::npos) {
                    // no \n in the input, need to read more data from the file
                    break;
                }
                std::string curr_line = input_buffer.substr(pos, end - pos);
                pos = end + 1;

                // Extract test data from curr_line
                std::vector<std::string> csv_values;
                std::istringstream line_stream(curr_line);
                std::string current_value;
                while (std::getline(line_stream, current_value, ',')) {
                    csv_values.push_back(std::move(current_value));
                }

                // Test data has `type` followed by the test data in subsequent lines.
                if (csv_values.size() == 1) {
                    // This is the type line, parse it and continue to the next line.
                    type = abstract_type::parse_type(csv_values[0]);
                    testlog.info("testing compatibility of type: {}", type->cql3_type_name());
                    continue;
                }

                // Test data has two columns: actual value and expected comparable bytes.
                const auto serialized_bytes = type->from_string(csv_values[0]);
                const auto expected_comparable_bytes = bytes_type->from_string(csv_values[1]);

                comparable_bytes comparable_bytes(*type, managed_bytes_view(serialized_bytes));
                BOOST_REQUIRE_MESSAGE(comparable_bytes.as_managed_bytes_view() == managed_bytes_view(expected_comparable_bytes), seastar::value_of([&] () {
                    return fmt::format("failed for type : {} and value : {}", type->cql3_type_name(), csv_values[0]);
                }));
            }

            // Remove the lines that were processed from the input buffer.
            input_buffer.erase(0, pos);

            buf = fs.read().get();
        }
        file.close().get();
    });
}
