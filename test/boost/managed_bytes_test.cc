/*
 * Copyright (C) 2021-present ScyllaDB
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

#define BOOST_TEST_MODULE core

#include "utils/managed_bytes.hh"
#include "utils/serialization.hh"
#include "test/lib/random_utils.hh"
#include <boost/test/unit_test.hpp>
#include <unordered_set>

struct fragmenting_allocation_strategy : standard_allocation_strategy {
    size_t allocated_bytes = 0;
    std::unordered_set<void*> allocations;

    fragmenting_allocation_strategy(size_t n) {
        _preferred_max_contiguous_allocation = n;
    }
    virtual void* alloc(migrate_fn mf, size_t size, size_t alignment) override {
        BOOST_CHECK_LE(size, _preferred_max_contiguous_allocation);
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

