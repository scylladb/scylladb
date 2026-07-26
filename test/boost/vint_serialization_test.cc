/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#define BOOST_TEST_MODULE core

#include "vint-serialization.hh"

#include "bytes.hh"

#include <boost/test/unit_test.hpp>

#include <array>
#include <cstdint>
#include <random>

#include <sys/mman.h>
#include <unistd.h>

using namespace seastar;

namespace {

typename bytes::value_type operator ""_b(unsigned long long value) {
    return static_cast<bytes::value_type>(value);
}

template <class Vint>
void test_serialized_size_from_first_byte(vint_size_type size, bytes_view bytes) {
}

template <>
void test_serialized_size_from_first_byte<unsigned_vint>(vint_size_type size, bytes_view bytes) {
    BOOST_REQUIRE_EQUAL(size, unsigned_vint::serialized_size_from_first_byte(bytes[0]));
}

// Check that the encoded value decodes back to the value. Also allows inspecting the encoded bytes.
template <class Vint, class BytesInspector>
void check_bytes_and_roundtrip(typename Vint::value_type value, BytesInspector&& f) {
    static std::array<int8_t, 9> encoding_buffer({});

    const auto size = Vint::serialize(value, encoding_buffer.begin());
    const auto view = bytes_view(encoding_buffer.data(), size);
    f(view);

    const auto deserialized = Vint::deserialize(view);
    BOOST_REQUIRE_EQUAL(deserialized, value);
    test_serialized_size_from_first_byte<Vint>(size, view);
};

// Check that the encoded value decodes back to the value.
template <class Vint>
void check_roundtrip(typename Vint::value_type value) {
    check_bytes_and_roundtrip<Vint>(value, [](const bytes_view&) {});
}

template <class Integer, class RandomEngine, class Callback>
void with_random_samples(RandomEngine& rng, std::size_t count, Callback&& f) {
    std::uniform_int_distribution<Integer> distribution;

    for (std::size_t i = 0; i < count; ++i) {
        f(distribution(rng));
    }
}

template <class Vint, class RandomEngine>
void check_roundtrip_sweep(std::size_t count, RandomEngine& rng) {
    with_random_samples<typename Vint::value_type>(rng, count, &check_roundtrip<Vint>);
};


auto& random_engine() {
    static std::random_device rd;
    static std::mt19937 rng(rd());
    return rng;
}

}

BOOST_AUTO_TEST_CASE(sanity_unsigned_examples) {
    using vint = unsigned_vint;

    check_bytes_and_roundtrip<vint>(0, [](bytes_view v) {
        BOOST_REQUIRE_EQUAL(v, (bytes{0_b}));
    });

    check_bytes_and_roundtrip<vint>(5, [](bytes_view v) {
        BOOST_REQUIRE_EQUAL(v, (bytes{0b0000'0101_b}));
    });

    check_bytes_and_roundtrip<vint>(1111, [](bytes_view v) {
        BOOST_REQUIRE_EQUAL(v, (bytes{0b1000'0100_b, 0b0101'0111_b}));
    });

    check_bytes_and_roundtrip<vint>(256'000, [](bytes_view v) {
        BOOST_REQUIRE_EQUAL(v, (bytes{0b1100'0011_b, 0b1110'1000_b, 0b0000'0000_b}));
    });

    check_bytes_and_roundtrip<vint>(0xff'ee'dd'cc'bb'aa'99'88, [](bytes_view v) {
        BOOST_REQUIRE_EQUAL(v, (bytes{0xff_b, 0xff_b, 0xee_b, 0xdd_b, 0xcc_b, 0xbb_b, 0xaa_b, 0x99_b, 0x88_b}));
    });
}

BOOST_AUTO_TEST_CASE(sanity_unsigned_sweep) {
    check_roundtrip_sweep<unsigned_vint>(100'000, random_engine());
}

BOOST_AUTO_TEST_CASE(sanity_signed_examples) {
    using vint = signed_vint;

    check_bytes_and_roundtrip<vint>(0, [](bytes_view v) {
        BOOST_REQUIRE_EQUAL(v, (bytes{0_b}));
    });

    check_bytes_and_roundtrip<vint>(-1, [](bytes_view v) {
        BOOST_REQUIRE_EQUAL(v, (bytes{0b0000'0001_b}));
    });

    check_bytes_and_roundtrip<vint>(1, [](bytes_view v) {
        BOOST_REQUIRE_EQUAL(v, (bytes{0b0000'0010_b}));
    });

    check_bytes_and_roundtrip<vint>(-2, [](bytes_view v) {
        BOOST_REQUIRE_EQUAL(v, (bytes{0b0000'0011_b}));
    });

    check_bytes_and_roundtrip<vint>(2, [](bytes_view v) {
        BOOST_REQUIRE_EQUAL(v, (bytes{0b0000'0100_b}));
    });

    check_bytes_and_roundtrip<vint>(-256'000, [](bytes_view v) {
        BOOST_REQUIRE_EQUAL(v, (bytes{0b1100'0111_b, 0b1100'1111_b, 0b1111'1111_b}));
    });
}

BOOST_AUTO_TEST_CASE(sanity_signed_sweep) {
    check_roundtrip_sweep<signed_vint>(100'000, random_engine());
}

// Regression test for the case where a 1-byte vint sits at the very end of a
// mapped page and the following page is unmapped (as it would be for the last
// page of shard memory). deserialize() reads 8 bytes past the first byte as an
// optimization; the page-crossing guard must be anchored on the first byte (which
// is guaranteed mapped) rather than on the byte after it. If it is anchored wrong,
// this reads into the unmapped page and crashes with SIGSEGV.
BOOST_AUTO_TEST_CASE(deserialize_at_end_of_mapped_page) {
    const size_t page_size = ::sysconf(_SC_PAGESIZE);

    // Map two pages worth of address space, then unmap the second page so that
    // any access past the end of the first page faults.
    void* base = ::mmap(nullptr, 2 * page_size, PROT_READ | PROT_WRITE,
                        MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    BOOST_REQUIRE(base != MAP_FAILED);
    auto* bytes_ptr = static_cast<int8_t*>(base);

    BOOST_REQUIRE_EQUAL(::munmap(bytes_ptr + page_size, page_size), 0);

    // Place a 1-byte vint (any value in [0, 127] encodes to a single byte) at the
    // last byte of the first, still-mapped, page. src+1 then points at the first
    // byte of the unmapped page.
    int8_t* last_byte = bytes_ptr + page_size - 1;
    const uint64_t expected = 0x42;
    const auto size = unsigned_vint::serialize(expected, last_byte);
    BOOST_REQUIRE_EQUAL(size, 1u);

    const auto deserialized = unsigned_vint::deserialize(bytes_view(last_byte, size));
    BOOST_REQUIRE_EQUAL(deserialized, expected);

    BOOST_REQUIRE_EQUAL(::munmap(base, page_size), 0);
}
