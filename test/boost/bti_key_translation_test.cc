/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#define BOOST_TEST_MODULE core

#include <boost/test/unit_test.hpp>
#include "seastar/util/defer.hh"
#include "schema/schema_builder.hh"
#include "sstables/trie/bti_key_translation.hh"
#include "test/lib/key_utils.hh"
#include "test/lib/log.hh"
#include "test/lib/nondeterministic_choice_stack.hh"
 
static std::vector<std::byte> linearize(comparable_bytes_iterator auto&& it) {
    auto result = std::vector<std::byte>(); 
    while (it != std::default_sentinel) {
        result.append_range(*it);
        ++it;
    }
    return result;
}

// Generate an ordered list of various ring position views,
// which together should cover all kinds of various pairwise comparisons.
static std::generator<dht::ring_position_view> generate_rpvs(const schema& string_pair_pk_schema) {
    constexpr auto alphabet = std::string_view("\x00\xff", 2);
    constexpr auto max_len = 2;
    auto all_strings = tests::generate_all_strings(alphabet, max_len);
    co_yield dht::ring_position_view::min();
    for (const auto& token : {dht::token::first(), dht::token::last()}) {
        using token_bound = dht::ring_position_view::token_bound;
        co_yield dht::ring_position_view(token, token_bound::start);
        for (const auto& pk1 : all_strings)
        for (const auto& pk2 : all_strings) {
            std::vector<data_value> components;
            components.emplace_back(pk1);
            components.emplace_back(pk2);
            auto pk = partition_key::from_deeply_exploded(string_pair_pk_schema, components);
            for (int weight = -1; weight <= 1; ++weight) {
                co_yield dht::ring_position_view(token, &pk, weight);
            }
        }
        co_yield dht::ring_position_view(token, token_bound::end);
    }
}

// Performs key conversion to BTI format for various keys,
// and checks that this encoding doesn't change the ordering of keys.
BOOST_AUTO_TEST_CASE(test_lazy_comparable_bytes_from_ring_position_preserves_order) {
    auto s = schema_builder("ks", "t")
        .with_column("pk1", utf8_type, column_kind::partition_key)
        .with_column("pk2", utf8_type, column_kind::partition_key)
        .build();
    using encoding = sstables::trie::lazy_comparable_bytes_from_ring_position;
    std::unique_ptr<encoding> prev;
    for (const auto& rpv : generate_rpvs(*s)) {
        if (!prev) {
            prev = std::make_unique<encoding>(*s, rpv);
        } else {
            auto curr = std::make_unique<encoding>(*s, rpv);
            auto prev_bytes = linearize(prev->begin());
            auto curr_bytes = linearize(curr->begin());
            testlog.debug("prev_bytes={}, curr_bytes={}", fmt_hex(prev_bytes), fmt_hex(curr_bytes));
            SCYLLA_ASSERT(prev_bytes < curr_bytes);
            std::swap(prev, curr);
        }
    }
    auto prev_bytes = linearize(prev->begin());
    SCYLLA_ASSERT(prev_bytes <= linearize(encoding(*s, dht::ring_position_view(dht::maximum_token(), nullptr, -1)).begin()));
    SCYLLA_ASSERT(prev_bytes <= linearize(encoding(*s, dht::ring_position_view::max()).begin()));
}

// Tests lazy_comparable_bytes_from_ring_position::trim().
//
// Description:
// 1. Constructs some single arbitrary ring position `dk`.
// 2. For all legal possibilities:
// 2.1 Constructs `enc := lazy_comparable_bytes_from_ring_position(dk)`.
// 2.2 Reads N bytes from `enc`.
// 2.3 Optionally modifies some chosen byte on the way.
// 2.4 Trims `enc` to length M.
// 2.5 Checks that reading `enc` again gives a result consistent with the
//     modification and the trim.
BOOST_AUTO_TEST_CASE(test_lazy_comparable_bytes_from_ring_position_trim) {
    auto s = schema_builder("ks", "t")
    .with_column("pk1", utf8_type, column_kind::partition_key)
    .with_column("pk2", utf8_type, column_kind::partition_key)
    .build();
    auto pk = ({
        std::vector<data_value> components;
        components.emplace_back("ab");
        components.emplace_back("cd");
        partition_key::from_deeply_exploded(*s, components);
    });
    using encoding = sstables::trie::lazy_comparable_bytes_from_ring_position;
    const auto dk = dht::decorated_key(dht::token::from_int64(42), std::move(pk));
    const auto encoded = linearize(encoding(*s, dk).begin());

    constexpr auto modification_byte = std::byte('z');
    for (size_t view_position = 0; view_position <= encoded.size(); ++view_position)
    for (size_t trim_position = 0; trim_position <= view_position; ++trim_position)
    for (size_t modification_position = 0; modification_position <= trim_position; ++modification_position) {
        auto enc = encoding(*s, std::move(dk));
        {
            size_t n_seen = 0;
            auto it = enc.begin();
            while (it != std::default_sentinel) {
                auto fragment_start = n_seen;
                n_seen += (*it).size_bytes();
                if (fragment_start <= modification_position && modification_position < n_seen) {
                    (*it)[modification_position - fragment_start] = modification_byte;
                }
                if (view_position <= n_seen) {
                    break;
                }
                ++it;
            }
            enc.trim(trim_position);
        }
        auto expected_encoded = encoded;
        if (modification_position < trim_position) {
            expected_encoded[modification_position] = modification_byte;
        }
        expected_encoded.resize(trim_position);
        auto new_encoded = linearize(enc.begin());
        testlog.debug("view_position={}, trim_position={}, modification_position={}", view_position, trim_position, modification_position);
        testlog.debug("new_encoded={}, expected_encoded={}", fmt_hex(new_encoded), fmt_hex(expected_encoded));
        SCYLLA_ASSERT(new_encoded == expected_encoded);
    }
}

// Generate an ordered list of various clustering positions views,
// which together should cover all kinds of various pairwise comparisons.
static std::generator<position_in_partition_view> generate_pipvs(const schema& string_pair_ck_schema) {
    constexpr auto alphabet = std::string_view("\x00\xff", 2);
    constexpr auto max_len = 2;
    auto all_strings = tests::generate_all_strings(alphabet, max_len);
    co_yield position_in_partition_view::before_all_clustered_rows();
    for (const auto& ck1 : all_strings) {
        std::vector<data_value> components;
        components.emplace_back(ck1);
        auto ckp = clustering_key_prefix::from_deeply_exploded(string_pair_ck_schema, components);
        co_yield position_in_partition_view(ckp, bound_weight(-1));
        for (const auto& ck2 : all_strings) {
            components.emplace_back(ck2);
            auto _pop = defer([&] { components.pop_back(); });

            auto ck = clustering_key_prefix::from_deeply_exploded(string_pair_ck_schema, components);
            for (int weight = -1; weight <= 1; ++weight) {
                auto ret = position_in_partition_view(ck, bound_weight(weight));
                testlog.debug("pipv={}", ret);
                co_yield ret;
            }
        }
        co_yield position_in_partition_view(ckp, bound_weight(1));
    }
    co_yield position_in_partition_view::after_all_clustered_rows();
}

// Performs key conversion to BTI format for various keys,
// and checks that this encoding doesn't change the ordering of keys.
BOOST_AUTO_TEST_CASE(test_lazy_comparable_bytes_from_clustering_position_preserves_order) {
    auto s = schema_builder("ks", "t")
        .with_column("pk", long_type, column_kind::partition_key)
        .with_column("ck1", utf8_type, column_kind::clustering_key)
        .with_column("ck2", utf8_type, column_kind::clustering_key)
        .build();
    using encoding = sstables::trie::lazy_comparable_bytes_from_clustering_position;
    std::unique_ptr<encoding> prev;
    for (const auto& pipv : generate_pipvs(*s)) {
        if (!prev) {
            prev = std::make_unique<encoding>(*s, pipv);
        } else {
            auto curr = std::make_unique<encoding>(*s, pipv);
            auto prev_bytes = linearize(prev->begin());
            auto curr_bytes = linearize(curr->begin());
            testlog.debug("prev_bytes={}, curr_bytes={}", fmt_hex(prev_bytes), fmt_hex(curr_bytes));
            SCYLLA_ASSERT(prev_bytes < curr_bytes);
            std::swap(prev, curr);
        }
    }
}

// Tests lazy_comparable_bytes_from_ckp::trim().
// Just like test_lazy_comparable_bytes_from_ring_position_trim,
// but for clustering positions.
BOOST_AUTO_TEST_CASE(test_lazy_comparable_bytes_from_clustering_position_trim) {
    auto s = schema_builder("ks", "t")
    .with_column("pk", long_type, column_kind::partition_key)
    .with_column("ck1", utf8_type, column_kind::clustering_key)
    .with_column("ck2", utf8_type, column_kind::clustering_key)
    .build();
    auto ck = ({
        std::vector<data_value> components;
        components.emplace_back("ab");
        components.emplace_back("cd");
        clustering_key_prefix::from_deeply_exploded(*s, components);
    });
    using encoding = sstables::trie::lazy_comparable_bytes_from_clustering_position;
    const auto pipv = position_in_partition_view(ck, bound_weight(0));
    const auto encoded = linearize(encoding(*s, pipv).begin());

    constexpr auto modification_byte = std::byte('z');
    for (size_t view_position = 0; view_position <= encoded.size(); ++view_position)
    for (size_t trim_position = 0; trim_position <= view_position; ++trim_position)
    for (size_t modification_position = 0; modification_position <= trim_position; ++modification_position) {
        auto enc = encoding(*s, pipv);
        {
            size_t n_seen = 0;
            auto it = enc.begin();
            while (it != std::default_sentinel) {
                auto fragment_start = n_seen;
                n_seen += (*it).size_bytes();
                if (fragment_start <= modification_position && modification_position < n_seen) {
                    (*it)[modification_position - fragment_start] = modification_byte;
                }
                if (view_position <= n_seen) {
                    break;
                }
                ++it;
            }
            enc.trim(trim_position);
        }
        auto expected_encoded = encoded;
        if (modification_position < trim_position) {
            expected_encoded[modification_position] = modification_byte;
        }
        expected_encoded.resize(trim_position);
        auto new_encoded = linearize(enc.begin());
        SCYLLA_ASSERT(new_encoded == expected_encoded);
    }
}

// A pretty printer for a string split into several bytespans,
// as happens in a test below.
struct fmt_fragmented_buffer {
    std::span<std::span<const std::byte>> frags;
};
template <>
struct fmt::formatter<fmt_fragmented_buffer> : fmt::formatter<fmt_hex> {
    template <typename FormatContext>
    auto format(const fmt_fragmented_buffer& s, FormatContext& ctx) const {
        fmt::format_to(ctx.out(), "[");
        const char* separator = "";
        for (const auto& frag : s.frags) {
            fmt::format_to(ctx.out(), "{}{}", separator, fmt_hex(frag));
            separator = "|";
        }
        fmt::format_to(ctx.out(), "]");
        return ctx.out();
    }
};

// Tests the lcb_mismatch function.
// It generates all possible pairs of strings up to some complexity
// (alphabet size, max length), splits them into fragments in all possible ways
// (up to some max number of empty fragments),
// and checks that the lcb_mismatch function gives results consistent with
// std::ranges::mismatch.
BOOST_AUTO_TEST_CASE(test_lcb_mismatch) {
    // The set of all strings up to the given complexity.
    auto all_strings = tests::generate_all_strings("ab", 3);
    nondeterministic_choice_stack ncs;
    do {
        // Choose a pair of strings to compare, nondeterministically.
        auto a = std::as_bytes(std::span(all_strings[ncs.choose_up_to(all_strings.size() - 1)]));
        auto b = std::as_bytes(std::span(all_strings[ncs.choose_up_to(all_strings.size() - 1)]));
        auto expected_mismatch = std::ranges::mismatch(a, b);
        uint64_t expected_mismatch_offset = expected_mismatch.in2 - b.begin();
        auto expected_mismatch_ptr = &*expected_mismatch.in2;

        using const_bytes = sstables::trie::const_bytes;
        // Split the byte span into fragments, nondeterministically.
        auto gen_fragments = [&ncs] (const_bytes sp) -> std::generator<const_bytes> {
            int empty_spans_in_a_row = 0;
            if (sp.empty()) {
                co_yield sp;
                co_return;
            }
            while (sp.size() > 0) {
                auto next_fragment_size = empty_spans_in_a_row < 2
                ? ncs.choose_up_to(sp.size())
                : 1 + ncs.choose_up_to(sp.size() - 1);
                empty_spans_in_a_row = (next_fragment_size == 0) ? empty_spans_in_a_row + 1 : 0;
                co_yield sp.subspan(0, next_fragment_size);
                sp = sp.subspan(next_fragment_size);
            }
        };
        auto [mismatch_offset, mismatch_ptr] = std::invoke([&] {
            // Extracting the full generator to a vector is expensive,
            // so do it only if there's a need for nice debug logs.
            constexpr bool debug = false;
            if constexpr (debug) {
                // This branch is effectively the same as the other branch, but it nicely logs the split into fragments.
                auto fragments_a = std::ranges::to<std::vector>(gen_fragments(a));
                auto fragments_b = std::ranges::to<std::vector>(gen_fragments(b));
                testlog.debug("a={}, b={}",
                    fmt_fragmented_buffer{fragments_a},
                    fmt_fragmented_buffer{fragments_b});
                auto generator_from_range = [&](auto& r) -> std::generator<std::ranges::range_value_t<decltype(r)>> {
                    for (auto e : r) {
                        co_yield e;
                    }
                };
                return sstables::trie::lcb_mismatch(
                    generator_from_range(fragments_a).begin(),
                    generator_from_range(fragments_b).begin()
                );
            } else {
                return sstables::trie::lcb_mismatch(
                    gen_fragments(a).begin(),
                    gen_fragments(b).begin()
                );
            }
        });
        SCYLLA_ASSERT(mismatch_offset == expected_mismatch_offset);
        SCYLLA_ASSERT(mismatch_ptr == expected_mismatch_ptr);
    } while (ncs.rewind());
}
