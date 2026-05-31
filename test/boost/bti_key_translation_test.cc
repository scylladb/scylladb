/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#define BOOST_TEST_MODULE core

#include <boost/test/unit_test.hpp>
#include <seastar/util/defer.hh>
#include "schema/schema_builder.hh"
#include "sstables/trie/bti_key_translation.hh"
#include "test/lib/key_utils.hh"
#include "test/lib/log.hh"
#include "test/lib/nondeterministic_choice_stack.hh"
#include <generator>
 
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
//
// The expected ordering depends on `sst_ver`:
// `mt` sorts partition keys within the same token by a bytewise comparison of their "legacy encoding",
// `ms` sorts them by a typed column-by-column comparison.
static std::generator<dht::ring_position_view> generate_rpvs(
        const schema& string_pair_pk_schema,
        sstables::sstable_version_types sst_ver) {
    constexpr auto alphabet = std::string_view("\x00\xff", 2);
    constexpr auto max_len = 2;
    auto all_strings = tests::generate_all_strings(alphabet, max_len);

    std::vector<partition_key> pks;
    for (const auto& pk1 : all_strings) {
        for (const auto& pk2 : all_strings) {
            std::vector<data_value> components;
            components.emplace_back(pk1);
            components.emplace_back(pk2);
            pks.push_back(partition_key::from_deeply_exploded(string_pair_pk_schema, components));
        }
    }

    std::vector<dht::token> tokens{
        dht::token::first(),
        dht::token::last()
    };

    std::vector<dht::ring_position_view> rpvs;
    for (const auto& token : tokens) {
        for (const auto& pk : pks) {
            for (int weight = -1; weight <= 1; ++weight) {
                rpvs.push_back(dht::ring_position_view(token, &pk, weight));
            }
        }
    }
    if (sst_ver == sstables::sstable_version_types::ms) {
        // ms encodes the partition key component-wise, so for two same-token
        // rpvs the expected order is given by partition_key::tri_compare
        // (NOT the ring comparator, which compares pks by their legacy form).
        partition_key::tri_compare pk_cmp(string_pair_pk_schema);
        std::ranges::sort(rpvs, [&pk_cmp] (const dht::ring_position_view& a, const dht::ring_position_view& b) {
            if (auto cmp = a.token() <=> b.token(); cmp != 0) {
                return cmp < 0;
            }
            if (auto cmp = pk_cmp(*a.key(), *b.key()); cmp != 0) {
                return cmp < 0;
            }
            return a.weight() < b.weight();
        });
    } else {
        std::ranges::sort(rpvs, dht::ring_position_less_comparator(string_pair_pk_schema));
    }

    for (const auto& rpv : rpvs) {
        co_yield rpv;
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
    for (auto sst_ver : {sstables::sstable_version_types::ms, sstables::sstable_version_types::mt}) {
        testlog.info("checking sstable_version={}", sst_ver);
        std::unique_ptr<encoding> prev;
        for (const auto& rpv : generate_rpvs(*s, sst_ver)) {
            if (!prev) {
                prev = std::make_unique<encoding>(sst_ver, *s, rpv);
            } else {
                auto curr = std::make_unique<encoding>(sst_ver, *s, rpv);
                auto prev_bytes = linearize(prev->begin());
                auto curr_bytes = linearize(curr->begin());
                testlog.debug("prev_bytes={}, curr_bytes={}", fmt_hex(prev_bytes), fmt_hex(curr_bytes));
                SCYLLA_ASSERT(prev_bytes < curr_bytes);
                std::swap(prev, curr);
            }
        }
        auto prev_bytes = linearize(prev->begin());
        SCYLLA_ASSERT(prev_bytes <= linearize(encoding(sst_ver, *s, dht::ring_position_view(dht::maximum_token(), nullptr, -1)).begin()));
        SCYLLA_ASSERT(prev_bytes <= linearize(encoding(sst_ver, *s, dht::ring_position_view::max()).begin()));
    }
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
    for (auto sst_ver : {sstables::sstable_version_types::ms, sstables::sstable_version_types::mt}) {
        testlog.info("checking sstable_version={}", sst_ver);
        const auto dk = dht::decorated_key(dht::token::from_int64(42), pk);
        const auto encoded = linearize(encoding(sst_ver, *s, dk).begin());

        constexpr auto modification_byte = std::byte('z');
        for (size_t view_position = 0; view_position <= encoded.size(); ++view_position)
        for (size_t trim_position = 0; trim_position <= view_position; ++trim_position)
        for (size_t modification_position = 0; modification_position <= trim_position; ++modification_position) {
            auto enc = encoding(sst_ver, *s, dk);
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
}

// Compatibility test for partition key encoding.
//
// The encoding has to stay stable across Scylla versions,
// so it's good to have a test with fixed inputs and outputs.
BOOST_AUTO_TEST_CASE(test_lazy_comparable_bytes_from_ring_position_fixed_cases_mt) {
    struct fixed_case {
        // Name of the test case, for logging.
        const char* name;
        // Columns of the partition key.
        std::vector<data_type> col_types;
        std::vector<bytes> col_values;
        int64_t token;
        // Hex strings representing the encodings of various
        // ring positions around the token and key.
        const char* at_key;
        const char* before_token;
        const char* after_token;
        const char* before_key;
        const char* after_key;
    };

    std::vector<fixed_case> cases = {
        {
            "bigint",
            {long_type},
            {from_hex("0123456789abcdef")},
            0x6f24ac460b9c0027,
            "40ef24ac460b9c0027400123456789abcdef0038",
            "40ef24ac460b9c002720",
            "40ef24ac460b9c002760",
            "40ef24ac460b9c0027400123456789abcdef0020",
            "40ef24ac460b9c0027400123456789abcdef0060",
        },
        {
            "int",
            {int32_type},
            {from_hex("01020304")},
            0x0a0090a9da040fe3,
            "408a0090a9da040fe340010203040038",
            "408a0090a9da040fe320",
            "408a0090a9da040fe360",
            "408a0090a9da040fe340010203040020",
            "408a0090a9da040fe340010203040060",
        },
        {
            "blob_all_nuls",
            {bytes_type},
            {from_hex("00000000")},
            0xcfa0f7ddd84c76bc,
            "404fa0f7ddd84c76bc4000fefefefe38",
            "404fa0f7ddd84c76bc20",
            "404fa0f7ddd84c76bc60",
            "404fa0f7ddd84c76bc4000fefefefe20",
            "404fa0f7ddd84c76bc4000fefefefe60",
        },
        {
            "blob_no_nuls",
            {bytes_type},
            {from_hex("0102030405")},
            0xfe94a31bab98860e,
            "407e94a31bab98860e4001020304050038",
            "407e94a31bab98860e20",
            "407e94a31bab98860e60",
            "407e94a31bab98860e4001020304050020",
            "407e94a31bab98860e4001020304050060",
        },
        {
            "bigint_blob_pair",
            {long_type, bytes_type},
            {from_hex("0123456789abcdef"), from_hex("0100020003")},
            0x79086c2c2e019e3e,
            "40f9086c2c2e019e3e4000ff080123456789abcdef00feff050100ff0200ff0300fe38",
            "40f9086c2c2e019e3e20",
            "40f9086c2c2e019e3e60",
            "40f9086c2c2e019e3e4000ff080123456789abcdef00feff050100ff0200ff0300fe20",
            "40f9086c2c2e019e3e4000ff080123456789abcdef00feff050100ff0200ff0300fe60",
        },
    };

    using encoding = sstables::trie::lazy_comparable_bytes_from_ring_position;
    auto sst_ver = sstables::sstable_version_types::mt;

    for (const auto& tc : cases) {
        auto sb = schema_builder("ks", "t");
        for (size_t i = 0; i < tc.col_types.size(); ++i) {
            sb.with_column(to_bytes(fmt::format("pk{}", i)), tc.col_types[i], column_kind::partition_key);
        }
        auto s = sb.build();

        auto pk = partition_key::from_exploded(*s, tc.col_values);
        auto tok = dht::token::from_int64(tc.token);

        struct subcase {
            const char* kind;
            const partition_key* pk_ptr;
            int8_t weight;
            const char* expected;
        };
        std::array<subcase, 5> subs = {{
            {"at_key",       &pk,     0, tc.at_key},
            {"before_token", nullptr, -1, tc.before_token},
            {"after_token",  nullptr, +1, tc.after_token},
            {"before_key",   &pk,    -1, tc.before_key},
            {"after_key",    &pk,    +1, tc.after_key},
        }};
        for (const auto& sub : subs) {
            auto rpv = dht::ring_position_view(tok, sub.pk_ptr, sub.weight);
            encoding enc(sst_ver, *s, rpv);
            auto got_hex = fmt::format("{}", fmt_hex(linearize(enc.begin())));
            BOOST_REQUIRE_MESSAGE(got_hex == sub.expected,
                fmt::format("case '{}' position_kind={}: expected={} got={}",
                    tc.name, sub.kind, sub.expected, got_hex));
        }
    }
}

// Generate an ordered list of various clustering positions views,
// which together should cover all kinds of various pairwise comparisons.
static std::generator<position_in_partition_view> generate_pipvs(const schema& string_pair_ck_schema) {
    constexpr auto alphabet = std::string_view("\x00\xff", 2);
    constexpr auto max_len = 2;
    auto all_strings = tests::generate_all_strings(alphabet, max_len);
    co_yield position_in_partition_view::for_partition_start();
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
    co_yield position_in_partition_view::for_partition_end();
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
