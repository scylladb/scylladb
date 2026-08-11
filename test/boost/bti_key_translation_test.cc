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
#include "test/lib/random_schema.hh"
#include "test/lib/random_utils.hh"
#include <generator>
#include "marshal_exception.hh"
#include "exceptions/exceptions.hh"
#include <fstream>
#include "db/marshal/type_parser.hh"
#include "types/list.hh"
#include "types/map.hh"
#include "types/set.hh"
#include "types/tuple.hh"
#include "utils/rjson.hh"
 
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
    auto s = schema_builder(1, "ks", "t")
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
    auto s = schema_builder(1, "ks", "t")
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
        auto sb = schema_builder(1, "ks", "t");
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

// Compatibility test for partition key encoding, ms format.
//
// Same shape as test_lazy_comparable_bytes_from_ring_position_fixed_cases,
// but uses sstable_version_types::ms.
// In ms the partition key is encoded component-wise via comparable_bytes_from_compound
// (each column prefixed with separator 0x40 and encoded via to_comparable_bytes for the column's type),
// instead of the "legacy form" used by mt.
//
// Note: the encoding used by ms was a mistake, because the index ordering implied by it
// is not compatible with the ordering of Data files.
// However, since `ms` files might exist out there, and for a given file index readers
// must use the same encoding as index writers, we need to support this encoding
// and keep it stable.
BOOST_AUTO_TEST_CASE(test_lazy_comparable_bytes_from_ring_position_fixed_cases_ms) {
    struct fixed_case {
        const char* name;
        std::vector<data_type> col_types;
        std::vector<bytes> col_values;
        int64_t token;
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
            "40ef24ac460b9c002740ff8123456789abcdef38",
            "40ef24ac460b9c002720",
            "40ef24ac460b9c002760",
            "40ef24ac460b9c002740ff8123456789abcdef20",
            "40ef24ac460b9c002740ff8123456789abcdef60",
        },
        {
            "int",
            {int32_type},
            {from_hex("01020304")},
            0x0a0090a9da040fe3,
            "408a0090a9da040fe3408102030438",
            "408a0090a9da040fe320",
            "408a0090a9da040fe360",
            "408a0090a9da040fe3408102030420",
            "408a0090a9da040fe3408102030460",
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
            "40f9086c2c2e019e3e40ff8123456789abcdef400100ff0200ff030038",
            "40f9086c2c2e019e3e20",
            "40f9086c2c2e019e3e60",
            "40f9086c2c2e019e3e40ff8123456789abcdef400100ff0200ff030020",
            "40f9086c2c2e019e3e40ff8123456789abcdef400100ff0200ff030060",
        },
    };

    using encoding = sstables::trie::lazy_comparable_bytes_from_ring_position;
    auto sst_ver = sstables::sstable_version_types::ms;

    for (const auto& tc : cases) {
        auto sb = schema_builder(1, "ks", "t");
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
            auto _pop = defer([&] noexcept { components.pop_back(); });

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
    auto s = schema_builder(1, "ks", "t")
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
    auto s = schema_builder(1, "ks", "t")
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


// Cross-check of the clustering position encoding against Cassandra.
//
// The samples were generated by a tool added to a Cassandra codebase, which drives
// Cassandra's own test data generators and encodes the resulting clustering positions
// with Cassandra's ClusteringComparator::asByteComparable.
//
// They are Cassandra's output verbatim, except for the samples covering the two places
// where we deliberately deviate from Cassandra, whose expected encoding was corrected
// by hand:
// * an "empty" value nested inside a multi-component type, which Cassandra encodes
//   exactly like a null element (see encode_component()),
// * a zero-sized value of a tuple (or UDT) type, which Cassandra encodes as an "empty"
//   value rather than as the all-null tuple it is (see
//   has_encoding_for_zero_sized_value_visitor).
//
// Two things aren't covered at all, because Cassandra has no valid encoding of them to
// compare against: a null *component* of the clustering key itself (a Cassandra-only
// COMPACT STORAGE relic, which our compound keys can't even represent), and a
// collection with a null element (which CQL rejects, and on which Cassandra's own
// encoder throws a NullPointerException).
//
// Each sample holds:
// * `types`: the types of all clustering key columns, as Cassandra type names,
// * `values`: the serialized (CQL binary format) values of the clustering prefix,
// * `weight`: -1 (before), 0 (at), +1 (after),
// * `encoded`: the expected byte-comparable encoding.
//
// Checks every sample in `path`, and returns the number of mismatches.
static size_t check_cassandra_samples(const char* path) {
    std::ifstream file(path, std::ios::binary);
    BOOST_REQUIRE_MESSAGE(file.is_open(), fmt::format("couldn't open sample file {}", path));
    const auto contents = std::string(std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>());
    const auto samples = rjson::parse(contents);
    BOOST_REQUIRE(samples.IsArray());
    testlog.info("loaded {} samples from {}", samples.Size(), path);

    size_t n_checked = 0;
    size_t n_mismatched = 0;
    for (const auto& sample : samples.GetArray()) {
        auto sb = schema_builder(1, "ks", "t");
        sb.with_column("pk", long_type, column_kind::partition_key);
        size_t n_types = 0;
        for (const auto& type_name : sample["types"].GetArray()) {
            auto type = db::marshal::type_parser::parse(rjson::to_string_view(type_name));
            sb.with_column(to_bytes(fmt::format("ck{}", n_types)), std::move(type), column_kind::clustering_key);
            ++n_types;
        }
        auto s = sb.build();

        std::vector<managed_bytes> components;
        for (const auto& value : sample["values"].GetArray()) {
            components.push_back(managed_bytes(managed_bytes_view(
                bytes_view(from_hex(std::string(rjson::to_string_view(value)))))));
        }
        auto ckp = clustering_key_prefix::from_range(components);
        const auto weight = sample["weight"].GetInt();
        const auto pipv = position_in_partition_view(ckp, bound_weight(weight));

        const auto expected = std::string(rjson::to_string_view(sample["encoded"]));
        // A sample which the encoder refuses to encode at all is just a mismatch;
        // it mustn't abort the run, so that all samples get checked.
        std::string got;
        try {
            got = fmt::format("{}", fmt_hex(linearize(
                sstables::trie::lazy_comparable_bytes_from_clustering_position(*s, pipv).begin())));
        } catch (const std::exception& e) {
            got = fmt::format("<{}>", e.what());
        }
        ++n_checked;
        if (got != expected) {
            ++n_mismatched;
            // Don't spam the log with all of them; the first few are enough to work with.
            if (n_mismatched <= 10) {
                testlog.error("mismatch for sample {}: types={} values={} weight={}\nexpected={}\ngot     ={}",
                    n_checked - 1, rjson::print(sample["types"]), rjson::print(sample["values"]), weight,
                    expected, got);
            }
        }
    }
    BOOST_CHECK_MESSAGE(n_mismatched == 0,
        fmt::format("{} of {} samples in {} don't match Cassandra's encoding", n_mismatched, n_checked, path));
    return n_mismatched;
}

BOOST_AUTO_TEST_CASE(test_clustering_position_encoding_matches_cassandra) {
    const char* path = std::getenv("BTI_COMPAT_SAMPLES");
    if (path) {
        BOOST_REQUIRE(check_cassandra_samples(path) == 0);
        return;
    }
    BOOST_REQUIRE(check_cassandra_samples("test/resource/bti_clustering_position_encoding_compatibility.json") == 0);
}

// Below are some randomized robustness tests for comparable_bytes_from_compound().
//
// It's desirable that comparable_bytes_from_compound() safely handles any
// well-typed input.
// It must encode any legal input (including "empty" values),
// and it is desirable that it doesn't explode on illegal input.
//
// The two tests below approach this from two directions:
// * `..._random_valid_data` builds well-formed values (including "empty" ones)
//   for random clustering key types, and additionally checks that the encoding
//   preserves the ordering of the positions,
// * `..._random_garbage_data` throws completely random bytes at random
//   clustering key types. It can't check the ordering (the input doesn't have
//   a meaningful one), so it only requires that the encoder terminates and
//   doesn't obviously misbehave.

// A random clustering key schema, with a number of columns in range [1; 8].
static schema_ptr make_random_clustering_key_schema(std::mt19937& engine) {
    auto spec = tests::make_random_schema_specification("ks");
    tests::type_generator type_gen(*spec);

    auto sb = schema_builder(1, "ks", "t");
    sb.with_column("pk", long_type, column_kind::partition_key);
    const auto n_columns = tests::random::get_int<size_t>(1, 8, engine);
    for (size_t i = 0; i < n_columns; ++i) {
        auto type = type_gen(engine, tests::type_generator::is_multi_cell::no);
        // Clustering key columns can be declared with a reversed ordering,
        // which changes the byte-comparable encoding, so cover that too.
        if (tests::random::get_int<int>(0, 1, engine)) {
            type = reversed_type_impl::get_instance(std::move(type));
        }
        sb.with_column(to_bytes(fmt::format("ck{}", i)), std::move(type), column_kind::clustering_key);
    }
    return sb.build();
}

// True if a zero-sized ("empty") value is a legal value of `type`.
static bool allows_empty_value(const abstract_type& type) {
    // Validation failures aren't reported through a single exception type:
    // most types throw marshal_exception, but vectors throw
    // invalid_request_exception.
    try {
        type.validate(managed_bytes_view());
        return true;
    } catch (const marshal_exception&) {
        return false;
    } catch (const exceptions::invalid_request_exception&) {
        return false;
    }
}

// Roughly one in five.
static bool sometimes(std::mt19937& engine) {
    return tests::random::get_int<int>(0, 4, engine) == 0;
}

// Appends a 4-byte big-endian integer to `out`.
static void append_be32(std::vector<int8_t>& out, int32_t value) {
    for (int shift : {24, 16, 8, 0}) {
        out.push_back(int8_t(uint8_t(uint32_t(value) >> shift)));
    }
}

// Appends one element of a multi-component value to `out`: its length as a
// 4-byte big-endian integer (-1 for a null element), followed by its bytes.
static void append_element(std::vector<int8_t>& out, const std::optional<managed_bytes>& value) {
    append_be32(out, int32_t(value ? value->size() : -1));
    if (value) {
        auto linearized = to_bytes(managed_bytes_view(*value));
        out.insert(out.end(), linearized.begin(), linearized.end());
    }
}

// Generates a valid serialized value for `type`.
//
// Unlike tests::value_generator, this also generates the values which the
// byte-comparable encoding treats specially:
// * "empty" values -- the zero-sized values which are legal for CQL types that
//   normally have a fixed size (int, bigint, ...). They compare before all
//   other values.
// * null and "empty" elements nested inside a multi-component type, and tuples
//   (or UDTs) with their trailing fields left out entirely. tests::value_generator
//   builds multi-component values out of ordinary values only, so they are
//   assembled here instead, recursively.
//
// Null elements are only generated where the serialized format can carry them:
// as tuple and UDT fields, and as set and list elements. A map has no encoding
// for a null key or value.
static managed_bytes make_random_value(std::mt19937& engine, tests::value_generator& value_gen, const abstract_type& type) {
    const abstract_type& underlying = type.is_reversed() ? *type.underlying_type() : type;
    // Roughly a fifth of the values are "empty", where that's legal.
    if (allows_empty_value(underlying) && sometimes(engine)) {
        return managed_bytes();
    }

    auto element = [&] (const abstract_type& element_type) {
        return make_random_value(engine, value_gen, element_type);
    };
    auto to_managed_bytes = [] (const std::vector<int8_t>& v) {
        return managed_bytes(managed_bytes_view(bytes_view(v.data(), v.size())));
    };

    // user_type_impl derives from tuple_type_impl, so this covers UDTs too.
    if (auto tuple_type = dynamic_cast<const tuple_type_impl*>(&underlying)) {
        const auto& field_types = tuple_type->all_types();
        // A tuple value may hold fewer fields than its type has; the missing
        // trailing ones are null.
        const auto n_fields = sometimes(engine)
                ? tests::random::get_int<size_t>(0, field_types.size(), engine)
                : field_types.size();
        std::vector<int8_t> out;
        for (size_t i = 0; i < n_fields; ++i) {
            append_element(out, sometimes(engine) ? std::nullopt : std::optional(element(*field_types[i])));
        }
        return to_managed_bytes(out);
    }

    if (auto listlike_type = dynamic_cast<const listlike_collection_type_impl*>(&underlying)) {
        const auto n_elements = tests::random::get_int<size_t>(0, 4, engine);
        std::vector<int8_t> out;
        append_be32(out, int32_t(n_elements));
        for (size_t i = 0; i < n_elements; ++i) {
            append_element(out, sometimes(engine)
                    ? std::nullopt
                    : std::optional(element(*listlike_type->get_elements_type())));
        }
        return to_managed_bytes(out);
    }

    if (auto map_type = dynamic_cast<const map_type_impl*>(&underlying)) {
        const auto n_entries = tests::random::get_int<size_t>(0, 4, engine);
        std::vector<int8_t> out;
        append_be32(out, int32_t(n_entries));
        for (size_t i = 0; i < n_entries; ++i) {
            append_element(out, element(*map_type->get_keys_type()));
            append_element(out, element(*map_type->get_values_type()));
        }
        return to_managed_bytes(out);
    }

    auto value = value_gen.generate_atomic_value(engine, underlying);
    return managed_bytes(value.serialize_nonnull());
}

// Feeds valid (but adversarially chosen) clustering keys of random types
// to the BTI clustering key encoder, and checks that it handles them gracefully
// and that the encoding is order-preserving.
BOOST_AUTO_TEST_CASE(test_comparable_bytes_from_compound_random_valid_data) {
    auto engine = std::mt19937(tests::random::get_int<uint32_t>());
    tests::value_generator value_gen;
    using encoding = sstables::trie::lazy_comparable_bytes_from_clustering_position;

    // A generated position, together with its encoding.
    struct entry {
        clustering_key_prefix ckp;
        bound_weight weight;
        std::vector<std::byte> encoded;

        position_in_partition_view pos() const {
            return position_in_partition_view(ckp, weight);
        }
    };

    constexpr int n_schemas = 30;
    constexpr int n_keys_per_schema = 100;
    for (int i = 0; i < n_schemas; ++i) {
        auto s = make_random_clustering_key_schema(engine);
        const auto& types = s->clustering_key_prefix_type()->types();
        testlog.info("schema {}: ck types={}", i, fmt::join(
            types | std::views::transform([] (const data_type& t) { return t->name(); }), ", "));

        std::vector<entry> entries;
        for (int j = 0; j < n_keys_per_schema; ++j) {
            // Vary the length to get clustering prefixes, not just full keys.
            const auto n_components = tests::random::get_int<size_t>(0, types.size(), engine);
            std::vector<managed_bytes> components;
            for (size_t k = 0; k < n_components; ++k) {
                components.push_back(make_random_value(engine, value_gen, *types[k]));
            }
            auto ckp = clustering_key_prefix::from_range(components);
            testlog.debug("key={}", ckp);
            for (int weight = -1; weight <= 1; ++weight) {
                auto e = entry{ckp, bound_weight(weight), {}};
                e.encoded = linearize(encoding(*s, e.pos()).begin());
                entries.push_back(std::move(e));
            }
        }

        // The encoding must be order-preserving: a bytewise comparison of two
        // encoded positions must give the same result as a typed comparison of
        // the positions themselves.
        auto pos_cmp = position_in_partition::tri_compare(*s);
        std::ranges::sort(entries, [&pos_cmp] (const entry& a, const entry& b) {
            return pos_cmp(a.pos(), b.pos()) < 0;
        });
        for (size_t a = 1; a < entries.size(); ++a) {
            const size_t b = a - 1;
            const auto expected = pos_cmp(entries[b].pos(), entries[a].pos());
            const auto actual = entries[b].encoded <=> entries[a].encoded;
            if (expected != actual) {
                BOOST_FAIL(fmt::format(
                    "encoding doesn't preserve the order of {} (encoded as {}) and {} (encoded as {}): "
                    "expected {}, got {}",
                    entries[b].pos(), fmt_hex(entries[b].encoded),
                    entries[a].pos(), fmt_hex(entries[a].encoded),
                    expected < 0 ? "<" : expected > 0 ? ">" : "==",
                    actual < 0 ? "<" : actual > 0 ? ">" : "=="));
            }
        }
    }
}

// Encodes `ckp` and checks that the result is sane.
// Doesn't validate the result, only demonstrates that nothing crashed or hanged.
//
// Since the input is random, and therefore not guaranteed to hold valid
// values of the key's types, so the encoder is allowed to reject it
// with marshal_exception or invalid_request_exception, since those seem to be the only
// two exceptions used for rejecting malformed input.
static void check_encoding_is_well_behaved(const schema& s, const clustering_key_prefix& ckp) {
    using encoding = sstables::trie::lazy_comparable_bytes_from_clustering_position;
    for (int weight = -1; weight <= 1; ++weight) {
        auto pipv = position_in_partition_view(ckp, bound_weight(weight));
        try {
            auto encoded = linearize(encoding(s, pipv).begin());
            // The encoding always ends with the terminator byte, so it's never empty.
            BOOST_REQUIRE(!encoded.empty());
        } catch (const marshal_exception&) {
            // Okay
        } catch (const exceptions::invalid_request_exception&) {
            // Okay
        }
    }
}

// Feeds completely random bytes (with random splits into components)
// to the BTI clustering key encoder, and checks it handles them gracefully.
BOOST_AUTO_TEST_CASE(test_comparable_bytes_from_compound_random_garbage_data) {
    auto engine = std::mt19937(tests::random::get_int<uint32_t>());

    constexpr int n_schemas = 30;
    constexpr int n_keys_per_schema = 30;
    for (int i = 0; i < n_schemas; ++i) {
        auto s = make_random_clustering_key_schema(engine);
        const auto& types = s->clustering_key_prefix_type()->types();
        testlog.info("schema {}: ck types={}", i, fmt::join(
            types | std::views::transform([] (const data_type& t) { return t->name(); }), ", "));

        for (int j = 0; j < n_keys_per_schema; ++j) {
            const auto n_components = tests::random::get_int<size_t>(0, types.size(), engine);
            std::vector<managed_bytes> components;
            for (size_t k = 0; k < n_components; ++k) {
                // Random size (including 0, i.e. "empty") and random contents.
                const auto size = tests::random::get_int<size_t>(0, 24, engine);
                components.push_back(managed_bytes(managed_bytes_view(
                    bytes_view(tests::random::get_bytes(size, engine)))));
            }
            auto ckp = clustering_key_prefix::from_range(components);
            testlog.debug("key={}", ckp);
            check_encoding_is_well_behaved(*s, ckp);
        }
    }
}
