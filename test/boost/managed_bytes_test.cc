/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#define BOOST_TEST_MODULE core

#include "utils/managed_bytes.hh"
#include "utils/chunked_string.hh"
#include "utils/serialization.hh"
#include "test/lib/random_utils.hh"
#include <boost/test/unit_test.hpp>
#include <iterator>
#include <memory>
#include <unordered_set>

struct fragmenting_allocation_strategy : standard_allocation_strategy {
    size_t allocated_bytes = 0;
    std::unordered_set<void*> allocations;

    fragmenting_allocation_strategy(size_t n) : standard_allocation_strategy(n) {
    }
    virtual void* alloc(migrate_fn mf, size_t size, size_t alignment) override {
        BOOST_CHECK_LE(size, preferred_max_contiguous_allocation());
        void* addr = standard_allocation_strategy::alloc(mf, size, alignment);
        allocated_bytes += size;
        allocations.insert(addr);
        return addr;
    }
    virtual void free(void* ptr, size_t size) override {
        BOOST_CHECK_EQUAL(allocations.erase(ptr), 1);
        standard_allocation_strategy::free(ptr, size);
    }
    virtual void free(void* ptr) override {
        BOOST_CHECK_EQUAL(allocations.erase(ptr), 1);
        standard_allocation_strategy::free(ptr);
    }
    ~fragmenting_allocation_strategy() {
        BOOST_CHECK_EQUAL(allocations.size(), 0);
    }
};

constexpr size_t alloc_size = 63;
const std::vector<size_t> sizes = {
    0, 1, 15, 16,
    alloc_size - 1, alloc_size, alloc_size + 1,
    alloc_size * 4 - 1, alloc_size * 4, alloc_size * 4 + 1,
};

using mbv = managed_bytes_view;

BOOST_AUTO_TEST_CASE(test_uses_current_allocator) {
    fragmenting_allocation_strategy fragmenting_allocator(alloc_size);
    with_allocator(fragmenting_allocator, [&] {
        auto m = managed_bytes(managed_bytes::initialized_later(), 123);
        BOOST_CHECK_GT(fragmenting_allocator.allocated_bytes, 0);
        for (bytes_view frag : fragment_range(mbv(m))) {
            BOOST_CHECK_LE(frag.size(), alloc_size);
        }
    });
}

BOOST_AUTO_TEST_CASE(test_default_constructor) {
    fragmenting_allocation_strategy fragmenting_allocator(alloc_size);
    with_allocator(fragmenting_allocator, [] {
        auto m = managed_bytes();
        BOOST_CHECK_EQUAL(to_bytes(m), bytes());
    });
}

BOOST_AUTO_TEST_CASE(test_constructor_from_bytes) {
    fragmenting_allocation_strategy fragmenting_allocator(alloc_size);
    with_allocator(fragmenting_allocator, [] {
        for (size_t size : sizes) {
            auto b = tests::random::get_bytes(size);
            auto m = managed_bytes(b);
            BOOST_CHECK_EQUAL(to_bytes(m), b);
        }
    });
}

BOOST_AUTO_TEST_CASE(test_copy_constructor) {
    fragmenting_allocation_strategy alloc_1(alloc_size);
    fragmenting_allocation_strategy alloc_2(alloc_size + 1);

    for (size_t size : sizes) {
        auto b = tests::random::get_bytes(size);
        with_allocator(alloc_1, [&] {
            auto m = managed_bytes(b);
            auto m_copy_same_allocator = m;
            with_allocator(alloc_2, [&] {
                auto m_copy_different_allocator = m;
                BOOST_CHECK_EQUAL(m, m_copy_same_allocator);
                BOOST_CHECK_EQUAL(m, m_copy_different_allocator);
            });
        });
    }
}

// Reproducer for #23781.
// Simulates a copy of a large cell from the standard allocator to lsa.
BOOST_AUTO_TEST_CASE(test_copy_constructor_standard_to_lsa) {
    fragmenting_allocation_strategy alloc_1(128 * 1024);
    fragmenting_allocation_strategy alloc_2(128 * 1024 / 10);

    for (size_t size : {16*1024, 32*1024, 64*1024, 128*1024, 256*1024}) {
        auto b = tests::random::get_bytes(size);
        with_allocator(alloc_1, [&] {
            auto m = managed_bytes(b);
            with_allocator(alloc_2, [&] {
                auto m_copy_different_allocator = m;
                BOOST_CHECK_EQUAL(m, m_copy_different_allocator);
            });
        });
    }
}

BOOST_AUTO_TEST_CASE(test_move_constructor) {
    fragmenting_allocation_strategy alloc_1(alloc_size);
    fragmenting_allocation_strategy alloc_2(alloc_size + 1);

    for (size_t size : sizes) {
        auto b = tests::random::get_bytes(size);
        with_allocator(alloc_1, [&] {
            auto m = managed_bytes(b);
            auto m_copy_same_allocator = m;
            with_allocator(alloc_2, [&] {
                auto m_copy_different_allocator = m;
                BOOST_CHECK_EQUAL(m, m_copy_same_allocator);
                BOOST_CHECK_EQUAL(m, m_copy_different_allocator);
            });
        });
    }
}

BOOST_AUTO_TEST_CASE(test_copy_assignment) {
    fragmenting_allocation_strategy fragmenting_allocator(alloc_size);
    with_allocator(fragmenting_allocator, [] {
        for (size_t size : sizes) {
            auto b = tests::random::get_bytes(size);
            auto m1 = managed_bytes(b);
            auto m2 = m1;
            BOOST_CHECK_EQUAL(to_bytes(m2), b);
        }
    });
}

BOOST_AUTO_TEST_CASE(test_move_assigment) {
    fragmenting_allocation_strategy fragmenting_allocator(alloc_size);
    with_allocator(fragmenting_allocator, [] {
        for (size_t size : sizes) {
            auto b = tests::random::get_bytes(size);
            auto m1 = managed_bytes(b);
            managed_bytes m2;
            m2 = std::move(m1);
            BOOST_CHECK_EQUAL(to_bytes(m2), b);
        }
    });
}

BOOST_AUTO_TEST_CASE(test_operator_bracket) {
    fragmenting_allocation_strategy fragmenting_allocator(alloc_size);
    with_allocator(fragmenting_allocator, [] {
        for (size_t size : sizes) {
            if (size == 0) {
                continue;
            }
            auto b = tests::random::get_bytes(size);
            auto m = managed_bytes(b);
            BOOST_CHECK_EQUAL(m[0], b[0]);
            BOOST_CHECK_EQUAL(m[b.size() - 1], b[b.size() - 1]);
            BOOST_CHECK_EQUAL(mbv(m)[0], b[0]);
            BOOST_CHECK_EQUAL(mbv(m)[b.size() - 1], b[b.size() - 1]);
        }
    });
}

BOOST_AUTO_TEST_CASE(test_size) {
    fragmenting_allocation_strategy fragmenting_allocator(alloc_size);
    with_allocator(fragmenting_allocator, [] {
        for (size_t size : sizes) {
            auto b = tests::random::get_bytes(size);
            auto m = managed_bytes(b);
            BOOST_CHECK_EQUAL(m.size(), b.size());
            BOOST_CHECK_EQUAL(mbv(m).size(), b.size());
        }
    });
}

BOOST_AUTO_TEST_CASE(test_external_memory_usage) {
    for (size_t size : sizes) {
        fragmenting_allocation_strategy fragmenting_allocator(alloc_size);
        with_allocator(fragmenting_allocator, [&] {
            auto b = tests::random::get_bytes(size);
            auto m = managed_bytes(b);
            BOOST_CHECK_EQUAL(m.external_memory_usage(), fragmenting_allocator.allocated_bytes);
        });
    }
}

BOOST_AUTO_TEST_CASE(test_with_linearized) {
    fragmenting_allocation_strategy fragmenting_allocator(alloc_size);
    for (size_t size : sizes) {
        with_allocator(fragmenting_allocator, [&] {
            auto b = tests::random::get_bytes(size);
            auto m = managed_bytes(b);
            m.with_linearized([&] (bytes_view bv) {
                BOOST_CHECK_EQUAL(bv, b);
            });
        });
    }
}

BOOST_AUTO_TEST_CASE(test_equality) {
    fragmenting_allocation_strategy alloc_1(alloc_size);
    fragmenting_allocation_strategy alloc_2(alloc_size + 1);

    for (size_t size : sizes) {
        auto b = tests::random::get_bytes(size);
        with_allocator(alloc_1, [&] {
            auto m1 = managed_bytes(b);
            with_allocator(alloc_2, [&] {
                auto m2 = managed_bytes(b);
                using mbv = managed_bytes_view;
                BOOST_CHECK_EQUAL(m1, m1);
                BOOST_CHECK_EQUAL(mbv(m1), mbv(m1));
                BOOST_CHECK_EQUAL(m1, m2);
                BOOST_CHECK_EQUAL(mbv(m1), mbv(m2));
                BOOST_CHECK_EQUAL(m2, m1);
                BOOST_CHECK_EQUAL(mbv(m2), mbv(m1));
            });
        });
    }
}

BOOST_AUTO_TEST_CASE(test_inequality) {
    for (size_t size : sizes) {
        if (size == 0) {
            continue;
        }

        auto b1 = tests::random::get_bytes(size);

        // Same size, different content
        auto b2 = b1;
        b2.back() ^= 1;

        // Different size, same prefix
        auto b3 = b1;
        b3.append(reinterpret_cast<const bytes::value_type*>("$"), 1);

        fragmenting_allocation_strategy fragmenting_allocator(alloc_size);
        with_allocator(fragmenting_allocator, [&] {
            auto m1 = managed_bytes(b1);
            auto m2 = managed_bytes(b2);
            auto m3 = managed_bytes(b3);
            BOOST_CHECK_NE(m1, m2);
            BOOST_CHECK_NE(m2, m1);
            BOOST_CHECK_NE(m1, m3);
            BOOST_CHECK_NE(m3, m1);
            BOOST_CHECK_NE(mbv(m1), mbv(m2));
            BOOST_CHECK_NE(mbv(m2), mbv(m1));
            BOOST_CHECK_NE(mbv(m1), mbv(m3));
            BOOST_CHECK_NE(mbv(m3), mbv(m1));
        });
    }
}

BOOST_AUTO_TEST_CASE(test_prefix) {
    fragmenting_allocation_strategy fragmenting_allocator(alloc_size);
    with_allocator(fragmenting_allocator, [&] {
        for (size_t bytes_size : sizes) {
            auto b = tests::random::get_bytes(bytes_size);
            auto m = managed_bytes(b);
            for (size_t prefix_size : sizes) {
                if (prefix_size > bytes_size) {
                    continue;
                }
                mbv prefix = mbv(m).prefix(prefix_size);
                mbv suffix = mbv(m);
                suffix.remove_prefix(prefix_size);
                BOOST_CHECK_EQUAL(prefix.size(), prefix_size);
                BOOST_CHECK_EQUAL(suffix.size(), bytes_size - prefix_size);
                BOOST_CHECK_EQUAL(to_bytes(prefix) + to_bytes(suffix), b);
            }
        }
    });
}

BOOST_AUTO_TEST_CASE(test_remove_current) {
    fragmenting_allocation_strategy fragmenting_allocator(alloc_size);
    with_allocator(fragmenting_allocator, [&] {
        for (size_t bytes_size : sizes) {
            if (bytes_size == 0) {
                continue;
            }
            auto b = tests::random::get_bytes(bytes_size);
            auto m = managed_bytes(b);
            auto v = mbv(m);
            size_t prefix_size = v.current_fragment().size();
            mbv prefix = v.current_fragment();
            mbv suffix = v;
            suffix.remove_current();
            BOOST_CHECK_EQUAL(prefix.size(), prefix_size);
            BOOST_CHECK_EQUAL(suffix.size(), bytes_size - prefix_size);
            BOOST_CHECK_EQUAL(to_bytes(prefix) + to_bytes(suffix), b);
        }
    });
}

BOOST_AUTO_TEST_CASE(test_empty_mbv) {
    mbv v;
    BOOST_CHECK_EQUAL(to_bytes(v), bytes());
    BOOST_CHECK_EQUAL(v.current_fragment(), bytes_view());
}

BOOST_AUTO_TEST_CASE(test_hash) {
    fragmenting_allocation_strategy fragmenting_allocator(alloc_size);
    with_allocator(fragmenting_allocator, [&] {
        for (size_t size : sizes) {
            auto b = tests::random::get_bytes(size);
            auto m = managed_bytes(b);
            auto v = managed_bytes_view(m);
            auto hash = std::hash<bytes>()(b);
            BOOST_CHECK_EQUAL(std::hash<bytes_view>()(bytes_view(b)), hash);
            BOOST_CHECK_EQUAL(std::hash<managed_bytes>()(m), hash);
            BOOST_CHECK_EQUAL(std::hash<managed_bytes_view>()(v), hash);
        }
    });
}

BOOST_AUTO_TEST_CASE(test_write_fragmented) {
    fragmenting_allocation_strategy fragmenting_allocator(alloc_size);
    with_allocator(fragmenting_allocator, [&] {
        for (size_t size : sizes) {
            auto b = tests::random::get_bytes(size);
            auto m1 = managed_bytes(b);
            auto m1v = managed_bytes_view(m1);
            auto m2 = managed_bytes(managed_bytes::initialized_later(), b.size());
            auto m2v = managed_bytes_mutable_view(m2);
            write_fragmented(m2v, m1v);
            BOOST_CHECK_EQUAL(m1, m2);
        }
    });
}

BOOST_AUTO_TEST_CASE(test_write) {
    bytes b1(bytes::initialized_later(), 64);
    {
        auto out = b1.begin();
        for (uint64_t i = 0; i < 8; ++i) {
            write<uint64_t>(out, i * 0x0101010101010101ull);
        }
    }
    // As of now, managed_bytes has 24 bytes of overhead per fragment.
    for (int fragment_size : {25, 32, 48, 128}) {
        fragmenting_allocation_strategy fragmenting_allocator(fragment_size);
        with_allocator(fragmenting_allocator, [&] {
            managed_bytes b2(managed_bytes::initialized_later(), 64);
            auto out = managed_bytes_mutable_view(b2);
            for (uint64_t i = 0; i < 8; ++i) {
                write<uint64_t>(out, i * 0x0101010101010101ull);
            }
            BOOST_CHECK_EQUAL(b1, to_bytes(b2));
        });
    }
}

BOOST_AUTO_TEST_CASE(test_appending_hash) {
    struct identity_hasher {
        bytes b;
        void update(const char *ptr, size_t size) noexcept {
            b += bytes(reinterpret_cast<const bytes::value_type*>(ptr), size);
        }
        bytes finalize() {
            return b;
        }
    };
    fragmenting_allocation_strategy fragmenting_allocator(alloc_size);
    with_allocator(fragmenting_allocator, [&] {
        for (size_t size : sizes) {
            auto b = tests::random::get_bytes(size);
            managed_bytes m(b);

            identity_hasher h1;
            feed_hash(h1, mbv(m));

            identity_hasher h2;
            feed_hash(h2, b);

            BOOST_CHECK_EQUAL(h1.finalize(), h2.finalize());
        }
    });
}

BOOST_AUTO_TEST_CASE(test_to_hex) {
    fragmenting_allocation_strategy fragmenting_allocator(alloc_size);
    with_allocator(fragmenting_allocator, [&] {
        for (size_t size : sizes) {
            auto b = tests::random::get_bytes(size);
            managed_bytes m(b);
            BOOST_CHECK_EQUAL(to_hex(b), to_hex(m));
        }
    });
}

static constexpr bool constexpr_managed_bytes() {
    managed_bytes mb1;
    auto mb2 = std::move(mb1);
    return std::is_constant_evaluated();
}

static_assert(constexpr_managed_bytes());

// Factory for managed_bytes instances whose backing memory is allocated
// through a fragmenting_allocation_strategy.  The factory must outlive
// all managed_bytes it produces.  Returned pointers use a custom deleter
// that frees through the factory's allocator.
class managed_bytes_factory {
    fragmenting_allocation_strategy _alloc;

    struct deleter {
        fragmenting_allocation_strategy* alloc;
        void operator()(managed_bytes* mb) const {
            with_allocator(*alloc, [&] {
                delete mb;
            });
        }
    };
public:
    using pointer = std::unique_ptr<managed_bytes, deleter>;

    // frag_size is the desired *data payload* per fragment.  The allocator
    // limit must also account for the per-fragment metadata overhead so that
    // managed_bytes' internal max_seg() computes back to frag_size.
    managed_bytes_factory(size_t frag_size)
        : _alloc(frag_size + std::max(sizeof(multi_chunk_blob_storage), sizeof(single_chunk_blob_storage)))
    {}

    // Build a managed_bytes holding `data`, fragmented so that each
    // fragment is exactly the configured frag_size bytes (the last
    // fragment may be shorter).
    pointer make(bytes_view data) {
        managed_bytes* mb;
        with_allocator(_alloc, [&] {
            mb = new managed_bytes(data);
        });
        return pointer(mb, deleter{&_alloc});
    }
};

// Fragment size used by byte_iterator tests: small enough to guarantee many
// chunk boundaries for 100-byte inputs (4-byte payload → 25 fragments, 24
// cross-chunk transitions).
static constexpr size_t iter_frag_size = 4;

// Convenience: cast a managed_bytes value_type (int8_t) to uint8_t for
// comparison against bytes literals without sign-extension surprises.
static uint8_t as_u8(managed_bytes_view::value_type v) {
    return static_cast<uint8_t>(v);
}

BOOST_AUTO_TEST_CASE(test_byte_iterator_empty) {
    // An empty managed_bytes view has begin() == end() and yields no elements.
    managed_bytes mb;
    auto v = managed_bytes_view(mb);
    BOOST_CHECK(v.begin() == v.end());
    BOOST_CHECK(!(v.begin() != v.end()));
}

BOOST_AUTO_TEST_CASE(test_byte_iterator_forward_traversal) {
    // Forward iteration with pre-increment visits every byte in order,
    // correctly crossing chunk boundaries.
    managed_bytes_factory factory(iter_frag_size);
    auto b = tests::random::get_bytes(100);
    auto mb = factory.make(b);
    auto v = managed_bytes_view(*mb);

    size_t idx = 0;
    for (auto it = v.begin(); it != v.end(); ++it, ++idx) {
        BOOST_CHECK_EQUAL(as_u8(*it), as_u8(b[idx]));
    }
    BOOST_CHECK_EQUAL(idx, b.size());
}

BOOST_AUTO_TEST_CASE(test_byte_iterator_post_increment) {
    // Post-increment returns a copy of the iterator before advancing, while
    // the original moves forward.
    managed_bytes_factory factory(iter_frag_size);
    auto b = tests::random::get_bytes(100);
    auto mb = factory.make(b);
    auto v = managed_bytes_view(*mb);

    auto it = v.begin();
    for (size_t i = 0; i < b.size(); ++i) {
        auto prev = it++;
        BOOST_CHECK_EQUAL(as_u8(*prev), as_u8(b[i]));
    }
    BOOST_CHECK(it == v.end());
}

BOOST_AUTO_TEST_CASE(test_byte_iterator_equality) {
    managed_bytes_factory factory(iter_frag_size);
    auto b = tests::random::get_bytes(100);
    auto mb = factory.make(b);
    auto v = managed_bytes_view(*mb);

    // begin() == begin(), end() == end()
    BOOST_CHECK(v.begin() == v.begin());
    BOOST_CHECK(v.end()   == v.end());
    // begin() != end() for non-empty view
    BOOST_CHECK(v.begin() != v.end());

    // Two iterators advanced the same number of steps compare equal.
    auto it1 = v.begin();
    auto it2 = v.begin();
    for (size_t i = 0; i < 13; ++i) { ++it1; ++it2; }
    BOOST_CHECK(it1 == it2);
    ++it1;
    BOOST_CHECK(it1 != it2);
}

BOOST_AUTO_TEST_CASE(test_byte_iterator_distance) {
    // operator- returns the signed distance between two iterators.
    managed_bytes_factory factory(iter_frag_size);
    auto b = tests::random::get_bytes(100);
    auto mb = factory.make(b);
    auto v = managed_bytes_view(*mb);

    auto begin = v.begin();
    auto end   = v.end();
    // Distance from end to begin equals -(size). Note the subtraction is
    // defined as (lhs._pos - rhs._pos) cast to ptrdiff_t — see the
    // implementation — so end - begin == size.
    BOOST_CHECK_EQUAL(end - begin, static_cast<std::ptrdiff_t>(b.size()));
    BOOST_CHECK_EQUAL(begin - end, -static_cast<std::ptrdiff_t>(b.size()));

    // Advance to a mid-point and verify partial distances.
    auto mid = v.begin();
    for (size_t i = 0; i < 37; ++i) {
        ++mid;
    }
    BOOST_CHECK_EQUAL(mid - begin,  37);
    BOOST_CHECK_EQUAL(end  - mid,   static_cast<std::ptrdiff_t>(b.size()) - 37);
}

BOOST_AUTO_TEST_CASE(test_byte_iterator_mutable) {
    // The mutable iterator (iterator, not const_iterator) allows writing
    // through operator* and must traverse correctly too.
    managed_bytes_factory factory(iter_frag_size);
    auto b = tests::random::get_bytes(100);
    auto mb = factory.make(b);
    auto mv = managed_bytes_mutable_view(*mb);

    // Increment every byte by 1 using the mutable forward iterator.
    for (auto it = mv.begin(); it != mv.end(); ++it) {
        ++(*it);
    }

    // Verify via the immutable view.
    auto v = managed_bytes_view(*mb);
    size_t idx = 0;
    for (auto it = v.begin(); it != v.end(); ++it, ++idx) {
        BOOST_CHECK_EQUAL(as_u8(*it), static_cast<uint8_t>(as_u8(b[idx]) + 1));
    }
}

// Round-trip helper: encode `data` as hex, store it as a managed_bytes
// fragmented at `frag_size`, run the fragmented from_hex(), and check the
// result equals the original bytes.
static void check_fragmented_from_hex(bytes_view data, size_t frag_size) {
    sstring hex = to_hex(data);
    bytes_view hex_bv(reinterpret_cast<const int8_t*>(hex.data()), hex.size());
    managed_bytes_factory factory(frag_size);
    auto fragmented_hex = factory.make(hex_bv);
    managed_bytes result = utils::from_hex(utils::chunked_string_view(managed_bytes_view(*fragmented_hex)));
    BOOST_CHECK_EQUAL(to_bytes(result), bytes(data));
}

BOOST_AUTO_TEST_CASE(test_fragmented_from_hex_empty) {
    // Empty input should produce an empty managed_bytes.
    managed_bytes result = utils::from_hex(utils::chunked_string_view());
    BOOST_CHECK(result.empty());
}

BOOST_AUTO_TEST_CASE(test_fragmented_from_hex_single_fragment) {
    // The entire hex string lives in one fragment — baseline correctness.
    for (size_t size : sizes) {
        auto b = tests::random::get_bytes(size);
        // Large fragment so it never splits.
        check_fragmented_from_hex(b, 8192);
    }
}

BOOST_AUTO_TEST_CASE(test_fragmented_from_hex_even_fragment_size) {
    // Fragment size is even: hex pairs always land inside a single fragment.
    for (size_t size : sizes) {
        auto b = tests::random::get_bytes(size);
        // Fragment size of 4 hex chars == 2 decoded bytes.
        check_fragmented_from_hex(b, 4);
        check_fragmented_from_hex(b, 8);
        check_fragmented_from_hex(b, 64);
    }
}

BOOST_AUTO_TEST_CASE(test_fragmented_from_hex_odd_fragment_size) {
    // Fragment size is odd: hex pairs regularly straddle fragment boundaries.
    // This is the critical case fixed by the carry-over logic.
    for (size_t size : sizes) {
        auto b = tests::random::get_bytes(size);
        check_fragmented_from_hex(b, 1);  // every nibble is its own fragment
        check_fragmented_from_hex(b, 3);
        check_fragmented_from_hex(b, 5);
        check_fragmented_from_hex(b, 7);
        check_fragmented_from_hex(b, alloc_size);      // 63 — odd
        check_fragmented_from_hex(b, alloc_size + 2);  // 65 — odd
    }
}

BOOST_AUTO_TEST_CASE(test_fragmented_from_hex_nibble_per_fragment) {
    // Extreme case: fragment size = 1, so every character is its own fragment.
    // All pairs straddle boundaries.
    auto b = tests::random::get_bytes(16);
    check_fragmented_from_hex(b, 1);
}

BOOST_AUTO_TEST_CASE(test_fragmented_from_hex_known_value) {
    // Sanity check against a known hex/bytes pair.
    // "deadbeef" -> {0xde, 0xad, 0xbe, 0xef}
    const sstring hex = "deadbeef";
    const bytes expected = {'\xde', '\xad', '\xbe', '\xef'};

    for (size_t frag_size : {1, 2, 3, 4, 7, 8, 16}) {
        bytes_view hex_bv(reinterpret_cast<const int8_t*>(hex.data()), hex.size());
        managed_bytes_factory factory(frag_size);
        auto fragmented_hex = factory.make(hex_bv);
        managed_bytes result = utils::from_hex(utils::chunked_string_view(managed_bytes_view(*fragmented_hex)));
        BOOST_CHECK_EQUAL(to_bytes(result), expected);
    }
}

BOOST_AUTO_TEST_CASE(test_fragmented_from_hex_odd_length_throws) {
    // A hex string with odd total length must always throw, regardless of
    // fragmentation.
    const sstring hex = "abc";  // 3 chars — odd
    bytes_view hex_bv(reinterpret_cast<const int8_t*>(hex.data()), hex.size());

    for (size_t frag_size : {1, 2, 3, 8}) {
        managed_bytes_factory factory(frag_size);
        auto fragmented_hex = factory.make(hex_bv);
        BOOST_CHECK_THROW(utils::from_hex(utils::chunked_string_view(managed_bytes_view(*fragmented_hex))), std::invalid_argument);
    }
}
