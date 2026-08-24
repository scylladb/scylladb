/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// This file contains a test of BTI index writers and readers.
//
// It generates a random dataset (or sstable index entries),
// writes it to a BTI index file, and then runs a sequence of
// BTI index reader operations on it, checking that the results
// are consistent with a "reference" index on the same dataset.

#include <generator>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/seastar.hh>
#include <seastar/util/closeable.hh>
#include <seastar/util/defer.hh>
#include "sstables/compress.hh"
#include "sstables/framed_chunk.hh"
#include "sstables/mx/types.hh"
#include "sstables/trie/bti_index.hh"
#include "sstables/trie/bti_index_internal.hh"
#include "schema/schema_builder.hh"
#include "test/lib/log.hh"
#include "test/lib/nondeterministic_choice_stack.hh"
#include "test/lib/random_utils.hh"
#include "test/lib/tmpdir.hh"
#include "test/lib/reader_concurrency_semaphore.hh"
#include "utils/cached_file.hh"
#include "utils/i_filter.hh"
#include "utils/memory_data_sink.hh"
#include <fmt/std.h>

// Focused coverage for the BTI physical-payload packing primitives. These
// are pure functions with no I/O, so a plain BOOST_AUTO_TEST_CASE suffices.
BOOST_AUTO_TEST_CASE(test_bti_extent_block_count_roundtrip) {
    using namespace sstables::trie;
    // For every (chunk_position, extent), blocks -> recovered is a lossy upper
    // bound: recovered >= extent, and it never over-rounds by more than a full
    // block.
    for (int64_t chunk_position : {int64_t{0}, int64_t{1}, int64_t{7}, int64_t{511},
                                   int64_t{512}, int64_t{513}, int64_t{1024},
                                   int64_t{100'000}, int64_t{100'001}}) {
        for (int64_t extent : {int64_t{1}, int64_t{2}, int64_t{511}, int64_t{512},
                               int64_t{513}, int64_t{4096}, int64_t{65536}}) {
            const auto blocks = bti_extent_to_block_count(chunk_position, extent);
            const auto recovered = bti_extent_hint_from_block_count(chunk_position, blocks);
            BOOST_CHECK_GE(recovered, extent);
            BOOST_CHECK_LT(recovered - extent, sstable_compressed_block_size);
        }
    }
}

BOOST_AUTO_TEST_CASE(test_bti_pack_unpack_length_and_offset_roundtrip) {
    using namespace sstables::trie;
    for (uint32_t uncompressed_chunk_length : {uint32_t{4096}, uint32_t{16384}, uint32_t{65536}}) {
        const auto layout = bti_physical_layout(sstables::framed_chunk_codec::for_framed_chunks(uncompressed_chunk_length));
        const auto max_len = layout.max_block_count();
        const auto packed_bits = layout.packed_bits();
        // The packed value never exceeds its advertised bit width / byte width.
        BOOST_CHECK_LE(layout.packed_bytes() * 8, 64u);
        for (uint64_t block_count : {uint64_t{1}, uint64_t{2}, uint64_t{7}, max_len}) {
            for (uint64_t offset : {uint64_t{0}, uint64_t{1}, uint64_t{uncompressed_chunk_length - 1}}) {
                const auto packed = layout.pack(block_count, offset);
                BOOST_CHECK_LT(packed, uint64_t{1} << packed_bits);
                auto [decoded_len, decoded_offset] = layout.unpack(packed);
                // The block count and offset are both stored exactly.
                BOOST_CHECK_EQUAL(decoded_offset, offset);
                BOOST_CHECK_EQUAL(decoded_len, block_count);
            }
        }
    }
}

BOOST_AUTO_TEST_CASE(test_bti_max_block_count_covers_largest_chunk) {
    using namespace sstables::trie;
    // The largest chunk holds compressed_chunk_length_limit bytes of compressed
    // data. Its block count must stay within the bound, and fit in the block-count
    // field, at every alignment and for every chunk length that the schema allows.
    for (uint32_t uncompressed_chunk_length = 1024; uncompressed_chunk_length <= 128 * 1024; uncompressed_chunk_length *= 2) {
        BOOST_TEST_CONTEXT("uncompressed_chunk_length=" << uncompressed_chunk_length) {
            const auto codec = sstables::framed_chunk_codec::for_framed_chunks(uncompressed_chunk_length);
            const auto max_extent = codec.extent(sstables::compressed_chunk_length_limit(uncompressed_chunk_length));
            const auto layout = bti_physical_layout(codec);
            const auto max_blocks = layout.max_block_count();
            const auto length_bits = layout.block_count_bits();
            BOOST_CHECK_LE(max_blocks - 1, (uint64_t{1} << length_bits) - 1);
            for (int64_t chunk_position = 0; chunk_position < sstable_compressed_block_size; ++chunk_position) {
                const auto blocks = bti_extent_to_block_count(chunk_position, max_extent);
                BOOST_CHECK_LE(uint64_t(blocks), max_blocks);
            }
        }
    }
}

BOOST_AUTO_TEST_CASE(test_bti_physical_layout_matches_format_doc) {
    using namespace sstables::trie;
    // The table of N_max, W_N and W_O in docs/architecture/sstable/sstable3/sstable-mu-format.rst.
    struct expected_layout {
        uint32_t uncompressed_chunk_length;
        uint64_t max_block_count;
        unsigned block_count_bits;
        unsigned offset_within_chunk_bits;
    };
    for (const auto& e : {
            expected_layout{1024, 5, 3, 10},
            expected_layout{2048, 8, 3, 11},
            expected_layout{4096, 14, 4, 12},
            expected_layout{8192, 26, 5, 13},
            expected_layout{16384, 50, 6, 14},
            expected_layout{32768, 98, 7, 15},
            expected_layout{65536, 194, 8, 16},
            expected_layout{131072, 386, 9, 17},
    }) {
        BOOST_TEST_CONTEXT("uncompressed_chunk_length=" << e.uncompressed_chunk_length) {
            const auto layout = bti_physical_layout(sstables::framed_chunk_codec::for_framed_chunks(e.uncompressed_chunk_length));
            BOOST_CHECK_EQUAL(layout.max_block_count(), e.max_block_count);
            BOOST_CHECK_EQUAL(layout.block_count_bits(), e.block_count_bits);
            BOOST_CHECK_EQUAL(layout.offset_within_chunk_bits(), e.offset_within_chunk_bits);
            // The largest values of N and O use every bit of the packed field.
            const auto packed = layout.pack(e.max_block_count, e.uncompressed_chunk_length - 1);
            BOOST_CHECK_EQUAL(unsigned(std::bit_width(packed)), layout.packed_bits());
        }
    }
}

// A partition for the payload characterization tests below.
// All positions are in Data.db.
struct payload_test_partition {
    struct block {
        int16_t first_ck;
        int16_t last_ck;
        sstables::sstable_position start;
        sstables::deletion_time range_tombstone_before_first_ck;
    };
    // Also the value of the partition key.
    int16_t token;
    sstables::sstable_position start;
    // The position of the END_OF_PARTITION byte.
    sstables::sstable_position end;
    sstables::deletion_time partition_tombstone;
    // Partitions with no blocks have no row index.
    std::vector<block> blocks;
};

struct index_files_hex {
    std::string partitions_db;
    std::string rows_db;
};

static std::string to_hex(const memory_data_sink_buffers& bufs) {
    std::string retval;
    for (const auto& frag : bufs.buffers()) {
        retval += fmt::to_string(fmt_hex(std::as_bytes(std::span(frag))));
    }
    return retval;
}

// Writes Partitions.db and Rows.db for `partitions` the way the sstable writer
// does, and returns their contents in hex.
static index_files_hex write_index_for_payload_test(
    sstables::sstable_version_types sst_ver,
    const sstables::trie::bti_position_encoding& encoding,
    const std::vector<payload_test_partition>& partitions
) {
    auto s = schema_builder(this_smp_shard_count(), "ks", "t")
        .with_column("pk", short_type, column_kind::partition_key)
        .with_column("ck", short_type, column_kind::clustering_key)
        .build();
    auto make_ck = [&] (int16_t value) {
        return sstables::clustering_info{
            clustering_key::from_deeply_exploded(*s, {data_value(value)}),
            sstables::bound_kind_m::clustering};
    };
    memory_data_sink_buffers partitions_bufs;
    memory_data_sink_buffers rows_bufs;
    {
        sstables::file_writer partitions_db_writer(data_sink(std::make_unique<memory_data_sink>(partitions_bufs)));
        sstables::file_writer rows_db_writer(data_sink(std::make_unique<memory_data_sink>(rows_bufs)));
        auto close_partitions_db = defer([&] noexcept { partitions_db_writer.close(); });
        auto close_rows_db = defer([&] noexcept { rows_db_writer.close(); });
        auto partition_index_writer = sstables::trie::bti_partition_index_writer(sst_ver, partitions_db_writer, encoding);
        auto row_index_writer = sstables::trie::bti_row_index_writer(sst_ver, rows_db_writer, encoding);
        std::vector<sstables::key> keys;
        for (const auto& p : partitions) {
            auto pk = partition_key::from_deeply_exploded(*s, {data_value(p.token)});
            auto dk = dht::decorated_key(dht::token::from_int64(p.token), pk);
            auto key = sstables::key::from_partition_key(*s, pk);
            for (const auto& b : p.blocks) {
                row_index_writer.add(*s, make_ck(b.first_ck), make_ck(b.last_ck), p.start, b.start, b.range_tombstone_before_first_ck);
            }
            auto target = row_index_writer.finish(*s, p.start, p.end, key, p.partition_tombstone);
            partition_index_writer.add(*s, dk, utils::make_hashed_key(bytes_view(key)), target);
            keys.push_back(std::move(key));
        }
        std::move(partition_index_writer).finish(keys.front(), keys.back());
    }
    return index_files_hex{
        .partitions_db = to_hex(partitions_bufs),
        .rows_db = to_hex(rows_bufs),
    };
}

// Checks the exact bytes of the BTI index files of `mt`.
// A change in these bytes is a change of the on-disk format.
SEASTAR_THREAD_TEST_CASE(test_mt_index_payload_encoding) {
    auto pos = [] (uint64_t p) { return sstables::sstable_position::from_logical(p); };
    const auto live = sstables::deletion_time::make_live();
    const auto partitions = std::vector<payload_test_partition>{
        {.token = 1, .start = pos(127), .end = pos(150), .partition_tombstone = live},
        {.token = 2, .start = pos(200), .end = pos(1199), .partition_tombstone = {789, 1011}, .blocks = {
            {.first_ck = 1, .last_ck = 2, .start = pos(210), .range_tombstone_before_first_ck = live},
            {.first_ck = 3, .last_ck = 4, .start = pos(500), .range_tombstone_before_first_ck = {123, 456}},
        }},
        {.token = 3, .start = pos(0x123456789a), .end = pos(0x12345678ff), .partition_tombstone = live},
    };
    auto files = write_index_for_payload_test(
        sstables::sstable_version_types::mt,
        sstables::trie::bti_position_encoding::logical(),
        partitions);
    // Each payload node starts with a byte holding the node type (high 4 bits)
    // and the payload field (low 4 bits). A partition payload field is
    // 0x8 (hash byte present) | (position width - 1).
    BOOST_CHECK_EQUAL(files.partitions_db,
        // Partition 1: Data.db position 127.
        // The payload is the hash byte and ~127 in 1 byte.
        "08" "44" "80"
        // Partition 2: Rows.db position 30, not negated.
        "08" "c1" "1e"
        // Partition 3: Data.db position 0x123456789a.
        // The payload is the hash byte and ~0x123456789a in 5 bytes.
        "0c" "9f" "edcba98765"
        // The trie nodes for the token bytes, from the leaves to the root.
        "50" "03" "010203" "0d0a07"
        "1800" "1200" "1200" "1200" "1200" "1200" "1280" "1240"
        // The footer: the first and last keys, the footer position,
        // the partition count, and the root position.
        "0002" "0001" "0002" "0003" "0000000000000025" "0000000000000003" "0000000000000023");
    // A row payload field is 0x8 (range tombstone present) | (offset width).
    BOOST_CHECK_EQUAL(files.rows_db,
        // Partition 2, block 2: offset 300 from the partition start, then the
        // range tombstone: marked_for_delete_at and local_deletion_time.
        "0a" "012c" "00000000000001c8" "0000007b"
        // Partition 2, the end of the partition: offset 999.
        "02" "03e7"
        // The trie nodes for the separators.
        "50" "02" "0305" "1203"
        "1680"
        // The root is a SINGLE_8 node with a payload: the transition byte,
        // the child offset, then the payload of block 1: offset 10.
        "21" "40" "02" "0a"
        // The row index header of partition 2, at Rows.db position 30: the key,
        // Data.db position 200 as a vint, the root position relative to the
        // previous field (-8) as a signed vint, the block count, and the
        // partition tombstone.
        "0002" "0002" "80c8" "0f" "02" "00000000000003f3" "00000315");
}

// Checks the exact bytes of the BTI index files of a compressed `mu` sstable.
// The names C, N, O, LO, delta_C, T, P, and V are from
// docs/architecture/sstable/sstable3/sstable-mu-format.rst.
SEASTAR_THREAD_TEST_CASE(test_mu_index_payload_encoding) {
    auto pos = [] (uint64_t chunk_position, uint64_t extent_hint, uint64_t offset_within_chunk) {
        return sstables::sstable_position::from_physical(chunk_position, extent_hint, offset_within_chunk);
    };
    const auto live = sstables::deletion_time::make_live();
    const auto partitions = std::vector<payload_test_partition>{
        {.token = 1, .start = pos(0, 100, 0), .end = pos(0, 100, 23), .partition_tombstone = live},
        {.token = 2, .start = pos(1000, 700, 50), .end = pos(2300, 900, 2000), .partition_tombstone = {789, 1011}, .blocks = {
            {.first_ck = 1, .last_ck = 2, .start = pos(1000, 700, 300), .range_tombstone_before_first_ck = live},
            {.first_ck = 3, .last_ck = 4, .start = pos(1700, 600, 10), .range_tombstone_before_first_ck = {123, 456}},
        }},
        {.token = 3, .start = pos(0x123456789a, 1500, 4095), .end = pos(0x123456789a + 1500, 1000, 50), .partition_tombstone = live},
    };
    // With L = 4096, LO has W_N = 4 bits of N - 1 and W_O = 12 bits of O.
    auto files = write_index_for_payload_test(
        sstables::sstable_version_types::mu,
        sstables::trie::bti_position_encoding::physical(sstables::framed_chunk_codec::for_framed_chunks(4096)),
        partitions);
    // The payload field of a partition payload is the payload size.
    BOOST_CHECK_EQUAL(files.partitions_db,
        // Partition 1: (C, N, O) = (0, 1, 0).
        // The payload is the hash byte, then V: the top bit, C and LO, in 3 bytes.
        "04" "44" "800000"
        // Partition 2: Rows.db position 36. The top bit of V is clear.
        "02" "c1" "24"
        // Partition 3: (C, N, O) = (0x123456789a, 4, 4095).
        "08" "9f" "923456789a3fff"
        // The trie nodes for the token bytes, from the leaves to the root.
        "50" "03" "010203" "110c09"
        "1800" "1200" "1200" "1200" "1200" "1200" "1280" "1240"
        // The footer: the first and last keys, the footer position,
        // the partition count, and the root position.
        "0002" "0001" "0002" "0003" "0000000000000029" "0000000000000003" "0000000000000027");
    // The payload field of a row payload is P.
    BOOST_CHECK_EQUAL(files.rows_db,
        // Partition 2, block 2: (C, N, O) = (1700, 2, 10), so delta_C = 700, and T = 1.
        // Then the range tombstone: marked_for_delete_at and local_deletion_time.
        "04" "05782015" "00000000000001c8" "0000007b"
        // Partition 2, the end of the partition: (C, N, O) = (2300, 3, 2000),
        // so delta_C = 1300, and T = 0.
        "04" "0a284fa0"
        // The trie nodes for the separators.
        "50" "02" "0305" "1605"
        "1680"
        // The root is a SINGLE_8 node with a payload: the transition byte,
        // the child offset, then the payload of block 1:
        // (C, N, O) = (1000, 3, 300), so delta_C = 0, and T = 0.
        "23" "40" "02" "004258"
        // The row index header of partition 2, at Rows.db position 36: the key,
        // C = 1000 as a vint, LO for (N, O) = (3, 50) in 2 bytes, the root
        // position relative to the C field (-10) as a signed vint, the block
        // count, and the partition tombstone.
        "0002" "0002" "83e8" "2032" "13" "02" "00000000000003f3" "00000315");
}

struct clustering_index_entry {
    sstables::clustering_info first_ck;
    sstables::clustering_info last_ck;
    uint64_t data_file_offset;
    sstables::deletion_time range_tombstone_before_first_ck;
};

struct partition_index_entry {
    dht::decorated_key dk;
    uint64_t data_file_offset;
    sstables::deletion_time partition_tombstone;
};

struct eof_index_entry {
    uint64_t data_file_offset;
};

struct partition_end_entry {
    uint64_t data_file_offset;
};

using index_entry = std::variant<partition_index_entry, clustering_index_entry, partition_end_entry, eof_index_entry>;

// Represents a sequence of entries written to the sstable index file,
// and sets of possible arguments for index reader operations on this index.
struct index_entry_dataset {
    const schema& s;
    // The set of ring positions participating in the test.
    // Will be used as the set of potential arguments for index reader operations
    // that take a ring position, and to construct partition ranges for `advance_to`.
    //
    // Assumed to contain all decorated keys used in `entries`.
    std::vector<dht::ring_position> ring_position_universe;
    // The set of clustering positions participating in the test.
    // Will be used as the set of potential arguments for index reader operations
    // that take a position in partition.
    std::vector<position_in_partition> position_in_partition_universe;
    // The sequence of entries written to the sstable index file.
    // in order.
    // It's assumed that the sequence contains at least one partition index entry,
    // and that it's of the form:
    // (partition_index_entry, clustering_index_entry*, partition_end_entry)+, eof_index_entry
    std::vector<index_entry> entries;
};

struct random_dataset_config {
    // Number of possible distinct values for each "component" of the decorated partition key.
    // (In a loose meaning -- in particular, we consider some individual bytes of the token
    // "components").
    // Needs to be big enough to have enough "power" to create enough trie branches to make for
    // intersesting trie shapes, but small enough to keep the values similar enough to
    // create interesting relationships between them.
    // The number of generated partition keys grows polynomially with this value,
    // but only a certain number of them will be picked to particpate in the test.
    int partition_key_component_values = 3;
    // The number of ring positions participating in the test.
    // The number of partition keys inserted into the index
    // will be some subset of that.
    // The complexity of the test grows exponentially with this value.
    int partition_universe_size = 3;
    // Like partition_key_component_values, but for clustering positions.
    int clustering_key_component_values = 3;
    // Like partition_universe_size, but for clustering positions.
    int clustering_universe_size = 6;
    // The max number of clustering blocks inserted into the index
    // for each partition.
    // Should be at most as big as clustering_universe_size / 2,
    // otherwise there isn't enough clustering positions to fill the blocks.
    int max_clustering_blocks = clustering_universe_size / 2;
};

// A recursive helper for generate_pips().
void generate_pips_impl(
    const schema& s,
    std::vector<data_value>& prefix,
    std::vector<position_in_partition>& result,
    std::span<data_value> possible_values
) {
    auto ckp = clustering_key_prefix::from_deeply_exploded(s, prefix);
    result.push_back(position_in_partition(partition_region::clustered, bound_weight(-1), ckp));
    if (prefix.size() == s.clustering_key_size()) {
        result.push_back(position_in_partition(partition_region::clustered, bound_weight(0), ckp));
    } else {
        for (const auto& value : possible_values) {
            prefix.push_back(value);
            generate_pips_impl(s, prefix, result, possible_values);
            prefix.pop_back();
        }
    }
    result.push_back(position_in_partition(partition_region::clustered, bound_weight(1), ckp));
}

// Generate all partition_in_position values,
// which use the given possible_values as values for clustering key columns. 
// Assumes a clustering key of type `(short, short)`.
std::vector<position_in_partition> generate_pips(const schema& the_schema, std::span<data_value> possible_values) {
    std::vector<data_value> prefix;
    std::vector<position_in_partition> result;
    generate_pips_impl(the_schema, prefix, result, possible_values);
    return result;
}
static sstables::deletion_time make_random_tombstone() {
    if (tests::random::get_bool()) {
        return sstables::deletion_time::make_live();
    }
    return sstables::deletion_time{
        tests::random::get_int<int32_t>(std::numeric_limits<int32_t>::min(), std::numeric_limits<int32_t>::max()),
        tests::random::get_int<int64_t>(std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::max()),
    };
}

static sstables::bound_kind_m pip_bound_weight_to_m_bound_weight(const bound_weight bw) {
    switch (bw) {
    case bound_weight::before_all_prefixed:
        switch (tests::random::get_int<int>(0, 2)) {
            case 0:
                return sstables::bound_kind_m::incl_start;
            case 1:
                return sstables::bound_kind_m::excl_end;
            case 2:
                return sstables::bound_kind_m::excl_end_incl_start;
        }
        break;
    case bound_weight::after_all_prefixed:
        switch (tests::random::get_int<int>(0, 2)) {
            case 0:
                return sstables::bound_kind_m::incl_end;
            case 1:
                return sstables::bound_kind_m::excl_start;
            case 2:
                return sstables::bound_kind_m::incl_end_excl_start;
        }
        break;
    case bound_weight::equal:
        return sstables::bound_kind_m::clustering;
    default:
        abort();
    }
    abort();
}

// Generate some interesting dataset of sstable index entries and related key positions.
// Assumes a primary key of type `(short, (short, short))`.
index_entry_dataset generate_random_dataset(const schema& the_schema, const random_dataset_config& cfg) {
    const auto& s = the_schema;
    std::vector<index_entry> result;

    // Generate a few partition keys.
    std::vector<partition_key> pks;
    {
        std::set<int16_t> pks_set;
        while (pks_set.size() < static_cast<size_t>(cfg.partition_key_component_values)) {
            pks_set.insert(tests::random::get_int<int16_t>());
        }
        for (auto& x : pks_set) {
            pks.push_back(partition_key::from_deeply_exploded(s, std::vector<data_value>{data_value(x)}));
        }
    }

    // Generate the set of decorated keys participating in the test.
    // Some subset of them will be inserted into the index,
    // and ring positions related to them will be used as arguments for index reader operations.
    std::vector<dht::decorated_key> dk_universe;
    {
        // Generate a big set of possible decorated keys,
        // by taking a cartesian product of several values for each "component".
        // (We do that instead of generating completely random keys
        // because having some common prefixes between keys should explore
        // more logic, because the length of common prefixes is important for tries). 
        std::vector<dht::decorated_key> dk_universe_candidates;
        std::vector<int> component_values;

        for (int i = 0; i < cfg.partition_key_component_values; ++i) {
            for (int j = 0; j < cfg.partition_key_component_values; ++j) {
                for (int k = 0; k < cfg.partition_key_component_values; ++k) {
                    auto token = dht::token::from_int64(i << 8 | j);
                    auto dk = dht::decorated_key(token, pks[k]);
                    dk_universe_candidates.push_back(dk);
                }
            }
        }
        // Select the configured number of decorated keys, at random, from the big set. 
        auto indexes = std::vector(std::from_range, std::views::iota(size_t(0), std::size(dk_universe_candidates)));
        auto chosen_indexes = std::vector<size_t>(cfg.partition_universe_size);
        std::sample(indexes.begin(), indexes.end(), chosen_indexes.begin(), chosen_indexes.size(), tests::random::gen());
        for (const auto& i : chosen_indexes) {
            dk_universe.push_back(dk_universe_candidates[i]);
        }
    }

    // Generate some interesting partition_in_position values.
    // A subset of them will be inserted into the index,
    // and they will be used as arguments for index reader operations.
    std::vector<position_in_partition> pip_universe;
    {
        std::vector<data_value> clustering_key_value_set;
        for (int i = 0; i < cfg.clustering_key_component_values - 1; ++i) {
            clustering_key_value_set.push_back(data_value(int16_t(i)));
        }
        // Put some `0xff` bytes in the keys to stress the `nudge()` logic
        // for the last clustering key in the partition.
        clustering_key_value_set.push_back(data_value(int16_t(0xff)));
        auto pip_universe_candidates = generate_pips(s, clustering_key_value_set);
        auto pip_indexes = std::vector(std::from_range, std::views::iota(size_t(0), std::size(pip_universe_candidates)));
        auto chosen_pips = std::vector<size_t>(cfg.clustering_universe_size);
        std::sample(pip_indexes.begin(), pip_indexes.end(), chosen_pips.begin(), chosen_pips.size(), tests::random::gen());
        for (const auto& i : chosen_pips) {
            pip_universe.push_back(pip_universe_candidates[i]);
        }
    }

    // The exact value of this is unimportant,
    // but it must strictly grow with each entry.
    int64_t data_file_offset = 0;

    const int inserted_partitions = tests::random::get_int(1, cfg.partition_universe_size);
    std::vector<dht::decorated_key> inserted_dks;
    std::sample(dk_universe.begin(), dk_universe.end(), std::back_inserter(inserted_dks), inserted_partitions, tests::random::gen());
    for (int p = 0; p < inserted_partitions; ++p) {
        const auto dk = inserted_dks[p];

        const auto tombstone = make_random_tombstone();
        result.push_back(partition_index_entry{
            .dk = dk,
            .data_file_offset = data_file_offset++,
            .partition_tombstone = tombstone,
        });

        const int inserted_clustering_blocks = tests::random::get_int(0, cfg.max_clustering_blocks);
        std::vector<position_in_partition> inserted_pips;
        std::sample(pip_universe.begin(), pip_universe.end(), std::back_inserter(inserted_pips), inserted_clustering_blocks * 2, tests::random::gen());
        for (int c = 0; c < inserted_clustering_blocks; ++c) {
            auto tombstone = make_random_tombstone();
            auto first_pip = inserted_pips[c * 2];
            auto first_pip_weight = pip_bound_weight_to_m_bound_weight(first_pip.get_bound_weight());
            auto last_pip = inserted_pips[c * 2 + 1];
            auto last_pip_weight = pip_bound_weight_to_m_bound_weight(last_pip.get_bound_weight());
            result.push_back(clustering_index_entry{
                .first_ck = sstables::clustering_info(first_pip.key(), first_pip_weight),
                .last_ck = sstables::clustering_info(last_pip.key(), last_pip_weight),
                .data_file_offset = data_file_offset++,
                .range_tombstone_before_first_ck = tombstone,
            });
        }
        result.push_back(partition_end_entry{
            .data_file_offset = data_file_offset++,
        });
    }
    result.push_back(eof_index_entry{
        .data_file_offset = data_file_offset,
    });
    std::vector<dht::ring_position> rp_universe;
    // We only use the decorated keys as ring positions,
    // we don't bother with token positions or min/max positions.
    // They shouldnt be distinguishable from a non-inserted key
    // at the same position relative to inserted keys.
    for (const auto& dk : dk_universe) {
        rp_universe.push_back(dk);
    }
    return index_entry_dataset {
        .s = s,
        .ring_position_universe = std::move(rp_universe),
        .position_in_partition_universe = std::move(pip_universe),
        .entries = std::move(result),
    };
}

// A reference index that can be used to check the results of actual index reader operations.
// Tries to implement the contract of abstact index_reader in a relatively simple way.
// 
struct reference_index {
    enum entry_idx : uint64_t {};
    enum rp_idx : uint64_t {};

    // These are directly from the generated index_entry_dataset.
    const schema& _s;
    std::vector<dht::ring_position> _ring_position_universe;
    std::vector<position_in_partition> _position_in_partition_universe;
    std::vector<index_entry> _entries;

    // These are some helpers for navigating the dataset above.
    
    // The set of decorated keys present in _entries.
    std::vector<dht::decorated_key> _present_dks;
    // `_present_dk_indices_in_rp_universe[i]`
    // is the index of `_present_dks[i]` within `_ring_position_universe`.
    std::vector<rp_idx> _present_dk_indices_in_rp_universe;
    // `_present_dk_indices[i]`
    // is the index of `_present_dks[i]` within `_entries`.
    // `_present_dk_indices[_present_dks.size()]` equals `_entries.size() - 1`.
    std::vector<entry_idx> _present_dk_indices;
    // `_data_file_offsets[i]` is the `_entries[i].data_file_offset`.
    // (Extracted for convenience, because `_entries[i]` is a variant).
    std::vector<sstables::sstable_position> _data_file_offsets;

    // The mutable state of this index reader.
    // An index reader is expected to behave like a (lower bound, upper bound) pair,
    // this is a "model" of that.
    entry_idx _lower = entry_idx(0);
    std::optional<entry_idx> _upper;
    bool _initialized = false;

    reference_index(const index_entry_dataset& raw)
        : _s(raw.s)
        , _ring_position_universe(raw.ring_position_universe)
        , _position_in_partition_universe(raw.position_in_partition_universe)
        , _entries(raw.entries)
    {
        for (const auto& e : _entries) {
            std::visit([this](const auto& entry) {
                _data_file_offsets.push_back(sstables::sstable_position::from_logical(entry.data_file_offset));
            }, e);
        }
        {
            uint64_t idx = 0;
            for (const auto& e : _entries) {
                if (auto p = std::get_if<partition_index_entry>(&e)) {
                    _present_dks.push_back(p->dk);
                    _present_dk_indices.push_back(entry_idx(idx));
                } else if (std::get_if<eof_index_entry>(&e)) {
                    _present_dk_indices.push_back(entry_idx(idx));
                }
                ++idx;
            }
        }
        {
            auto cmp = dht::ring_position_comparator(_s);
            auto it = _present_dks.begin();
            uint64_t idx = 0;
            while (it != _present_dks.end() && idx < _ring_position_universe.size()) {
                if (cmp(_ring_position_universe[idx], *it) == std::strong_ordering::equal) {
                    _present_dk_indices_in_rp_universe.push_back(rp_idx(idx));
                    ++it;
                }
                ++idx;
            }
            SCYLLA_ASSERT(_present_dks.size() == _present_dk_indices_in_rp_universe.size());
        }
    }

    entry_idx entry_idx_from_data_position(sstables::sstable_position data_position) const {
        auto it = std::find(_data_file_offsets.begin(), _data_file_offsets.end(), data_position);
        if (it != _data_file_offsets.end()) {
            return static_cast<entry_idx>(std::distance(_data_file_offsets.begin(), it));
        }
        abort();
    }

    entry_idx get_partition_of_entry(entry_idx idx) const {
        for (int64_t i = std::to_underlying(idx); i >= 0; --i) {
            if (std::holds_alternative<partition_index_entry>(_entries[i])
                || std::holds_alternative<eof_index_entry>(_entries[i])
            ) {
                return entry_idx(i);
            }
        }
        abort();
    }

    entry_idx get_current_partition() const {
        return get_partition_of_entry(_lower);
    }

    void recalibrate(sstables::sstable_positions_range r) {
        _lower = entry_idx_from_data_position(r.start);
        if (r.end) {
            _upper = entry_idx_from_data_position(*r.end);
        } else {
            _upper.reset();
        }
    }

    std::span<const dht::ring_position> valid_targets_for_advance_lower_and_check_if_present() const {
        return _ring_position_universe;
    }
    bool advance_lower_and_check_if_present(dht::ring_position_view key) {
        _initialized = true;
        auto cmp = dht::ring_position_less_comparator(_s);
        auto tricmp = dht::ring_position_comparator(_s);
        auto it = std::ranges::lower_bound(_present_dks, key, cmp);
        _lower = entry_idx(_present_dk_indices[it - _present_dks.begin()]);
        if (it == _present_dks.end() || tricmp(*it, key) != std::strong_ordering::equal) {
            return false;
        }
        return true;
    }

    std::span<const dht::decorated_key> valid_targets_for_advance_past_definitely_present_partition() const {
        return _present_dks;
    }
    void advance_past_definitely_present_partition(const dht::decorated_key& dk) {
        _initialized = true;
        auto cmp = dht::ring_position_less_comparator(_s);
        auto it = std::ranges::lower_bound(_present_dks, dk, cmp);
        _lower = entry_idx(_present_dk_indices[it - _present_dks.begin() + 1]);
    }
    void advance_to_definitely_present_partition(const dht::decorated_key& dk) {
        _initialized = true;
        auto cmp = dht::ring_position_less_comparator(_s);
        auto it = std::ranges::lower_bound(_present_dks, dk, cmp);
        _lower = entry_idx(_present_dk_indices[it - _present_dks.begin()]);
    }
    std::span<const dht::ring_position> valid_lb_targets_for_advance_to() const {
        auto cmp = dht::ring_position_less_comparator(_s);
        auto boundary_idx = _upper ? *_upper : _lower;
        auto it = std::ranges::lower_bound(_ring_position_universe, ring_position_of_entry(boundary_idx), cmp);
        return std::span<const dht::ring_position>(it, _ring_position_universe.end());
    }
    std::span<const dht::ring_position> valid_ub_targets_for_advance_to(const dht::ring_position& lb) const {
        auto cmp = dht::ring_position_less_comparator(_s);
        auto it = std::ranges::lower_bound(_ring_position_universe, lb, cmp);
        return std::span<const dht::ring_position>(it, _ring_position_universe.end());
    }
    void advance_to(const dht::partition_range& range) {
        _initialized = true;
        auto cmp = dht::ring_position_less_comparator(_s);
        {
            auto rpv = dht::ring_position_view::for_range_start(range);
            auto it = std::ranges::lower_bound(_present_dks, rpv, cmp);
            _lower = entry_idx(_present_dk_indices[it - _present_dks.begin()]);
        }
        {
            auto rpv = dht::ring_position_view::for_range_end(range);
            auto it = std::ranges::lower_bound(_present_dks, rpv, cmp);
            _upper = entry_idx(_present_dk_indices[it - _present_dks.begin()]);
        }
    }
    void advance_to_next_partition() {
        _initialized = true;
        auto idx = std::to_underlying(_lower);
        if (std::holds_alternative<partition_index_entry>(_entries[idx])) {
            ++idx;
        }
        while (std::holds_alternative<clustering_index_entry>(_entries[idx])
            || std::holds_alternative<partition_end_entry>(_entries[idx])
        ) {
            ++idx;
        }
        _lower = entry_idx(idx);
    }
    void advance_reverse_to_next_partition() {
        _initialized = true;
        auto idx = std::to_underlying(_lower);
        if (std::holds_alternative<partition_index_entry>(_entries[idx])) {
            ++idx;
        }
        while (std::holds_alternative<clustering_index_entry>(_entries[idx])
            || std::holds_alternative<partition_end_entry>(_entries[idx])
        ) {
            ++idx;
        }
        _upper = entry_idx(idx);
    }
    entry_idx advance_bound_before(entry_idx eidx, position_in_partition_view pos) {
        auto less = position_in_partition::less_compare(_s);
        auto idx = std::to_underlying(get_current_partition());
        if (std::holds_alternative<eof_index_entry>(_entries[idx])) {
            return entry_idx(idx);
        }
        if (std::holds_alternative<clustering_index_entry>(_entries[idx + 1])) {
            ++idx;
            while (auto e = std::get_if<clustering_index_entry>(&_entries[idx])) {
                if (less(pip_of_clustering_info(e->last_ck), pos)) {
                    ++idx;
                } else {
                    break;
                }
            }
        }
        return entry_idx(idx);
    }
    static int weight_of_bound_kind_m(sstables::bound_kind_m b){
        using sstables::bound_kind_m;
        switch (b) {
            case bound_kind_m::incl_start:
            case bound_kind_m::excl_end_incl_start:
            case bound_kind_m::excl_end:
                return -1;
            case bound_kind_m::static_clustering:
            case bound_kind_m::clustering:
                return 0;
            case bound_kind_m::incl_end_excl_start:
            case bound_kind_m::incl_end:
            case bound_kind_m::excl_start:
                return 1;
        }
        abort();
    }
    static position_in_partition pip_of_clustering_info(const sstables::clustering_info& e) {
        int weight = weight_of_bound_kind_m(e.kind);
        return position_in_partition(partition_region::clustered, bound_weight(weight), e.clustering);
    }
    position_in_partition past_previous_pip() const {
        if (std::holds_alternative<eof_index_entry>(_entries[_lower])) {
            return position_in_partition::for_partition_end();
        }
        if (std::holds_alternative<partition_index_entry>(_entries[_lower])) {
            return position_in_partition::after_static_row_tag_t();
        }
        if (auto e = std::get_if<clustering_index_entry>(&_entries[_lower - 1])) {
            return pip_of_clustering_info(e->last_ck);
        } 
        if (std::holds_alternative<partition_end_entry>(_entries[_lower])) {
            return position_in_partition::for_partition_end();
        }
        return position_in_partition::after_static_row_tag_t();
    }
    std::span<const position_in_partition> valid_targets_for_advance_to_pip() const {
        auto cmp = position_in_partition::less_compare(_s);
        auto cp = past_previous_pip();
        testlog.debug("valid_targets_for_advance_to_pip: current_pip={}", cp);
        auto it = std::ranges::upper_bound(_position_in_partition_universe, cp, cmp);
        return std::span<const position_in_partition>(it, _position_in_partition_universe.end());
    }
    std::span<const position_in_partition> valid_targets_for_advance_reverse() const {
        return _position_in_partition_universe;
    }
    void advance_to(position_in_partition_view pos) {
        _initialized = true;
        _lower = advance_bound_before(_lower, pos);
    }
    void advance_upper_past(position_in_partition_view pos) {
        _initialized = true;
        _upper = advance_bound_before(_lower, pos);
    }
    void advance_reverse(position_in_partition_view pos) {
        _initialized = true;
        auto less = position_in_partition::less_compare(_s);
        auto idx = std::to_underlying(get_partition_of_entry(_lower));
        if (std::holds_alternative<partition_index_entry>(_entries[idx])) {
            ++idx;
        }
        while (auto e = std::get_if<clustering_index_entry>(&_entries[idx])) {
            if (less(pos, pip_of_clustering_info(e->first_ck))) {
                break;
            } else {
                ++idx;
            }
        }
        _upper = entry_idx(idx);
    }

    bool has_row_index() const {
        auto curpar = get_current_partition();
        if (std::holds_alternative<eof_index_entry>(_entries[curpar])) {
            return false;
        }
        if (std::holds_alternative<clustering_index_entry>(_entries[curpar + 1])) {
            return true;
        }
        return false;
    }
    std::optional<sstables::deletion_time> partition_tombstone() const {
        if (const auto& hdr = std::get_if<partition_index_entry>(&_entries[get_current_partition()])) {
            return hdr->partition_tombstone;
        }
        return std::nullopt;
    }
    std::optional<partition_key> get_partition_key() const {
        if (const auto& hdr = std::get_if<partition_index_entry>(&_entries[get_current_partition()])) {
            return hdr->dk.key();
        }
        return std::nullopt;
    }
    sstables::sstable_positions_range sstable_positions() const {
        auto lo = _data_file_offsets[_lower];
        std::optional<sstables::sstable_position> hi;
        if (_upper) {
            hi = _data_file_offsets[*_upper];
        }
        return {lo, hi};
    }
    std::optional<sstables::sstable_position_offset> last_block_offset() {
        auto curpar = get_current_partition();
        if (std::holds_alternative<eof_index_entry>(_entries[curpar])) {
            return std::nullopt;
        }
        if (!std::holds_alternative<clustering_index_entry>(_entries[curpar + 1])) {
            return std::nullopt;
        }
        for (uint64_t idx = curpar + 1; idx < _entries.size(); ++idx) {
            if (std::holds_alternative<partition_end_entry>(_entries[idx + 1])) {
                return sstables::sstable_position_offset::from_logical(
                    _data_file_offsets[idx].to_logical() - _data_file_offsets[curpar].to_logical());
            }
        }
        abort();
    }
    sstables::indexable_element element_kind_for_entry(entry_idx idx) const {
        if (std::holds_alternative<clustering_index_entry>(_entries[idx])
            || std::holds_alternative<partition_end_entry>(_entries[idx])) {
            return sstables::indexable_element::cell;
        }
        return sstables::indexable_element::partition;
    }
    sstables::indexable_element element_kind() const {
        return element_kind_for_entry(_lower);
    }
    sstables::indexable_element element_kind_for_position(std::optional<sstables::sstable_position> pos) const {
        if (!pos) {
            return sstables::indexable_element::partition;
        }
        auto entry = entry_idx_from_data_position(*pos);
        return element_kind_for_entry(entry);
    }
    std::optional<sstables::open_rt_marker> end_open_marker() const {
        if (auto e = std::get_if<clustering_index_entry>(&_entries[_lower])) {
            auto tomb = tombstone(e->range_tombstone_before_first_ck);
            if (!tomb) {
                return std::nullopt;
            }
            return sstables::open_rt_marker{
                .pos = {position_in_partition::after_static_row_tag_t()},
                .tomb = tomb,
            };
        }
        return std::nullopt;
    }
    std::optional<sstables::open_rt_marker> reverse_end_open_marker() const {
        if (!_upper) {
            return std::nullopt;
        }
        if (auto e = std::get_if<clustering_index_entry>(&_entries[*_upper])) {
            auto tomb = tombstone(e->range_tombstone_before_first_ck);
            if (!tomb) {
                return std::nullopt;
            }
            return sstables::open_rt_marker{
                .pos = {position_in_partition::after_static_row_tag_t()},
                .tomb = tomb,
            };
        }
        return std::nullopt;
    }
    bool eof() const {
        return _lower == _entries.size() - 1;
    }
    bool partition_data_ready() const {
        return _initialized;
    }
    void read_partition_data() {
        _initialized = true;
    }
    void reset() {
        _initialized = false;
        _lower = entry_idx(0);
        _upper.reset();
    }
    dht::ring_position_view ring_position_of_entry(entry_idx eidx) const {
        auto curpar = get_partition_of_entry(eidx);
        if (std::holds_alternative<eof_index_entry>(_entries[curpar])) {
            return dht::ring_position_view::max();
        }
        auto pe = std::get_if<partition_index_entry>(&_entries[curpar]);
        if (curpar == eidx) {
            return dht::ring_position_view(pe->dk);
        } else {
            return dht::ring_position_view::for_after_key(pe->dk);
        }
    }
};

// Test all possible legal sequences of `abstract_index_reader` method calls,
// up to a certain sequence length (max_ops),
// on the given index dataset.
//
// The general structore of the test is:
// 1. We create a real index reader and a reference index reader.
// 2. For max_ops iterations:
// 2.1 We nondeterministically choose an operation to perform and its argument.
// 2.2 We call the operation on both readers.
// 2.3 We check that the real reader's positions after the method are
//     consistent (possibly less accurate, but within the contract of the method)
//     with the reference reader's positions.
// 2.4 We adjust the reference reader to point to exactly the same positions as the real reader,
//     so that their states match for the next method calls.
// 2.5 We check that all the const getters return the same thing for both readers.
//
// And nondeterministic_choice_stack is used to explore all possible outcomes of the nondeterministic choices.
void test_index(const index_entry_dataset& dataset, std::function<std::unique_ptr<sstables::abstract_index_reader>(void)> reader_factory, const int max_ops) {
    auto ri = reference_index(dataset);

    uint64_t n_cases = 0;
    nondeterministic_choice_stack ndcs;
    do {
        ++n_cases;
        auto reader = reader_factory();
        ri.reset();
        auto check_integrity = [&] {
            testlog.debug("check_integrity: reader->sstable_positions()={},{}", reader->sstable_positions().start, reader->sstable_positions().end);
            auto positions = reader->sstable_positions();
            // Adjust the reference index to match the reader's positions exactly.
            // (Before this call, the positions may be different, because the real
            // reader is allowed some degree of inexactness/suboptimality after some method calls).
            ri.recalibrate(positions);
            if (ri.partition_data_ready()) {
                SCYLLA_ASSERT(reader->partition_data_ready());
            }
            SCYLLA_ASSERT(ri.sstable_positions().start == positions.start);
            SCYLLA_ASSERT(ri.sstable_positions().end == positions.end);
            SCYLLA_ASSERT(ri.element_kind() == reader->element_kind());
            SCYLLA_ASSERT(ri.eof() == reader->eof());
            auto get_tombstone = [] (const sstables::open_rt_marker& marker) {
                return marker.tomb;
            };
            SCYLLA_ASSERT(ri.end_open_marker().transform(get_tombstone) == reader->end_open_marker().transform(get_tombstone));
            SCYLLA_ASSERT(ri.reverse_end_open_marker().transform(get_tombstone) == reader->reverse_end_open_marker().transform(get_tombstone));
            if (reader->partition_data_ready()) {
                // This test uses logical positions only.
                auto to_logical = [] (const sstables::sstable_position_offset& off) {
                    return off.to_logical();
                };
                SCYLLA_ASSERT(ri.last_block_offset().transform(to_logical) == reader->last_block_offset().get().transform(to_logical));
                if (ri.has_row_index()) {
                    SCYLLA_ASSERT(reader->partition_tombstone());
                    SCYLLA_ASSERT(reader->get_partition_key());
                    SCYLLA_ASSERT(ri.partition_tombstone() == reader->partition_tombstone());
                    SCYLLA_ASSERT(ri.get_partition_key() == reader->get_partition_key());
                } else {
                    SCYLLA_ASSERT(!reader->partition_tombstone());
                    SCYLLA_ASSERT(!reader->get_partition_key());
                }
            }
        };
        testlog.debug("Initial check_integrity");
        check_integrity();
        for (int op = 0; op < max_ops; ++op) {
            testlog.debug("op={}, start={}, end={}",
                op, reader->sstable_positions().start, reader->sstable_positions().end);
            if (auto vt = ri.valid_targets_for_advance_lower_and_check_if_present(); !vt.empty() && !ndcs.choose_bool()) {
                auto target = ndcs.choose_up_to(vt.size() - 1);
                auto rp = vt[target];
                testlog.debug("advance_lower_and_check_if_present(rp={})", rp);
                
                auto upper_before = reader->sstable_positions().end;
                auto possible_match = reader->advance_lower_and_check_if_present(rp).get();
                auto reference_match = ri.advance_lower_and_check_if_present(rp);
                if (!possible_match) {
                    testlog.debug("No match");
                    SCYLLA_ASSERT(!reference_match);
                    // After a mismatch in the advance_lower_and_check_if_present,
                    // the reader is in a broken state, and can't be used anymore.
                    break;
                }
                SCYLLA_ASSERT(reader->element_kind() == sstables::indexable_element::partition);
                testlog.debug("reader->sstable_positions()={},{}, ri.sstable_positions()={},{}, upper_before={}",
                    reader->sstable_positions().start,
                    reader->sstable_positions().end,
                    ri.sstable_positions().start,
                    ri.sstable_positions().end,
                    upper_before);
                SCYLLA_ASSERT(reader->sstable_positions().start <= ri.sstable_positions().start);
                if (reference_match) {
                    SCYLLA_ASSERT(possible_match);
                    SCYLLA_ASSERT(reader->sstable_positions().start == ri.sstable_positions().start);
                }
                SCYLLA_ASSERT(reader->sstable_positions().end == upper_before);
            } else if (auto vt = ri.valid_targets_for_advance_past_definitely_present_partition(); !vt.empty() && !ndcs.choose_bool()) {
                auto target = ndcs.choose_up_to(vt.size() - 1);
                auto dk = vt[target];
                auto upper_before = reader->sstable_positions().end;
                testlog.debug("advance_to_definitely_present_partition(dk={})", dk);
                reader->advance_to_definitely_present_partition(dk).get();
                ri.advance_to_definitely_present_partition(dk);
                SCYLLA_ASSERT(reader->sstable_positions().start == ri.sstable_positions().start);
                SCYLLA_ASSERT(reader->sstable_positions().end == upper_before);
            } else if (auto vt = ri.valid_targets_for_advance_past_definitely_present_partition(); !vt.empty() && !ndcs.choose_bool()) {
                auto target = ndcs.choose_up_to(vt.size() - 1);
                auto dk = vt[target];
                auto upper_before = reader->sstable_positions().end;
                testlog.debug("advance_past_definitely_present_partition(dk={})", dk);
                reader->advance_past_definitely_present_partition(dk).get();
                ri.advance_past_definitely_present_partition(dk);
                SCYLLA_ASSERT(reader->sstable_positions().start == ri.sstable_positions().start);
                SCYLLA_ASSERT(reader->sstable_positions().end == upper_before);
            } else if (!ndcs.choose_bool()) {
                std::optional<dht::partition_range::bound> lb;
                if (auto vt = ri.valid_lb_targets_for_advance_to(); !vt.empty() && !ndcs.choose_bool()) {
                    auto target = ndcs.choose_up_to(vt.size() - 1);
                    const auto& rp = vt[target];
                    auto inclusive = ndcs.choose_bool();
                    lb = dht::partition_range::bound(rp, inclusive);
                }
                std::optional<dht::partition_range::bound> ub;
                const auto& lb_rp = lb ? lb.value().value() : dht::ring_position::min();
                if (auto vt = ri.valid_ub_targets_for_advance_to(lb_rp); !vt.empty() && !ndcs.choose_bool()) {
                    auto target = ndcs.choose_up_to(vt.size() - 1);
                    const auto& rp = vt[target];
                    bool inclusive;
                    auto tricmp = dht::ring_position_comparator(ri._s);
                    if (lb.has_value() && lb.value().is_inclusive() && tricmp(lb_rp, rp) == std::strong_ordering::equal) {
                        inclusive = false;
                    } else {
                        inclusive = ndcs.choose_bool();
                    }
                    lb = dht::partition_range::bound(rp, inclusive);
                }
                auto pr = dht::partition_range(lb, ub);
                testlog.debug("advance_to(pr={})", pr);
                reader->advance_to(pr).get();
                ri.advance_to(pr);
                auto positions = reader->sstable_positions();
                SCYLLA_ASSERT(reader->element_kind() == sstables::indexable_element::partition);
                SCYLLA_ASSERT(positions.start <= ri.sstable_positions().start);
                if (ri.sstable_positions().end) {
                    SCYLLA_ASSERT(positions.end);
                    SCYLLA_ASSERT(*positions.end >= *ri.sstable_positions().end);
                } else {
                    SCYLLA_ASSERT(!positions.end);
                }
            } else if (!reader->eof() && !ndcs.choose_bool()) {
                testlog.debug("advance_to_next_partition()");
                reader->advance_to_next_partition().get();
                ri.advance_to_next_partition();
                SCYLLA_ASSERT(reader->sstable_positions().start == ri.sstable_positions().start);
                SCYLLA_ASSERT(reader->sstable_positions().end == ri.sstable_positions().end);
            } else if (!ndcs.choose_bool()) {
                testlog.debug("advance_reverse_to_next_partition()");
                reader->advance_reverse_to_next_partition().get();
                ri.advance_reverse_to_next_partition();
                SCYLLA_ASSERT(reader->sstable_positions().start == ri.sstable_positions().start);
                SCYLLA_ASSERT(reader->sstable_positions().end == ri.sstable_positions().end);
            } else if (auto vt = ri.valid_targets_for_advance_to_pip(); !reader->eof() && !vt.empty() && !ndcs.choose_bool()) {
                auto target = ndcs.choose_up_to(vt.size() - 1);
                const auto& pos = vt[target];
                testlog.debug("advance_to({})", pos);
                reader->advance_to(pos).get();
                ri.advance_to(pos);
                SCYLLA_ASSERT(reader->sstable_positions().start <= ri.sstable_positions().start);
                SCYLLA_ASSERT(reader->sstable_positions().end == ri.sstable_positions().end);
            } else if (auto vt = ri.valid_targets_for_advance_to_pip(); !reader->eof() && !vt.empty() && !ndcs.choose_bool()) {
                auto target = ndcs.choose_up_to(vt.size() - 1);
                const auto& pos = vt[target];
                testlog.debug("advance_upper_past({})", pos);
                reader->advance_upper_past(pos).get();
                ri.advance_upper_past(pos);
                SCYLLA_ASSERT(reader->sstable_positions().end.value() >= ri.sstable_positions().end.value());
                SCYLLA_ASSERT(reader->sstable_positions().start == ri.sstable_positions().start);
            } else if (auto vt = ri.valid_targets_for_advance_reverse(); !reader->eof() && !vt.empty() && !ndcs.choose_bool()) {
                auto target = ndcs.choose_up_to(vt.size() - 1);
                const auto& pos = vt[target];
                testlog.debug("advance_upper_past({})", pos);
                reader->advance_reverse(pos).get();
                ri.advance_reverse(pos);
                SCYLLA_ASSERT(reader->sstable_positions().end.value() >= ri.sstable_positions().end.value());
                SCYLLA_ASSERT(reader->sstable_positions().start == ri.sstable_positions().start);
            } else {
                testlog.debug("read_partition_data()");
                auto positions_before = reader->sstable_positions();
                reader->read_partition_data().get();
                ri.read_partition_data();
                SCYLLA_ASSERT(reader->partition_data_ready());
                auto positions_after = reader->sstable_positions();
                SCYLLA_ASSERT(positions_before.start == positions_after.start);
                SCYLLA_ASSERT(positions_before.end == positions_after.end);
            }
            check_integrity();
        }
    } while (ndcs.rewind());
    testlog.info("Number of run method sequences: {}", n_cases);
};

SEASTAR_THREAD_TEST_CASE(test_exhaustive) {
    auto the_schema = schema_builder(this_smp_shard_count(), "ks", "t")
        .with_column("pk", short_type, column_kind::partition_key)
        .with_column("ck1", short_type, column_kind::clustering_key)
        .with_column("ck2", short_type, column_kind::clustering_key)
        .build();
    auto sst_ver = sstables::sstable_version_types::mt;

    random_dataset_config cfg;
    const int max_ops = 3;
#ifdef SEASTAR_DEBUG
    // The test generates millions of futures,
    // so it's extremely slow in debug mode which induces a preemption
    // after every future.
    // So we downsize the test.
    // FIXME: that's not big enough for full coverage.
    // E.g. there are branches in partition index writer which are only taken
    // since third partition key onward.
    cfg.partition_universe_size = 2;
    cfg.clustering_universe_size = 4;
    cfg.max_clustering_blocks = 2;
#endif
    // Step 1: generate the test dataset.
    testlog.debug("Generating a random dataset.");
    auto dataset = generate_random_dataset(*the_schema, cfg);

    // Log the contents of the dataset for debugging purposes.
    for (const auto& entry : dataset.entries) {
        std::visit(overloaded_functor {
            [](const partition_index_entry& e) {
                testlog.debug("Partition index entry: dk={}, data_file_offset={}, partition_tombstone={}",
                    e.dk, e.data_file_offset, e.partition_tombstone);
            },
            [](const clustering_index_entry& e) {
                sstables::clustering_info first = e.first_ck;
                sstables::clustering_info last = e.last_ck;
                testlog.debug("Clustering index entry: first={}@{}, last={}@{}, data_file_offset={}, tombstone_before_first_ck={}",
                    first.kind, first.clustering, last.kind, last.clustering, e.data_file_offset, e.range_tombstone_before_first_ck);
            },
            [](const partition_end_entry& e) {
                testlog.debug("Partition end entry: data_file_offset={}", e.data_file_offset);
            },
            [](const eof_index_entry& e) {
                testlog.debug("Eof index entry: data_file_offset={}", e.data_file_offset);
            },
        }, entry);
    }

    // Step 2: write the index to BTI files.
    tmpdir dir;
    auto partitions_path = dir.path() / "Partitions.db";
    auto rows_path = dir.path() / "Rows.db";
    testlog.debug("Writing index to {} and {}", partitions_path.c_str(), rows_path.c_str());
    {
        file partitions_db = open_file_dma(partitions_path.c_str(), open_flags::create | open_flags::wo).get();
        file rows_db = open_file_dma(rows_path.c_str(), open_flags::create | open_flags::wo).get();

        sstables::file_writer partitions_db_writer(make_file_output_stream(partitions_db).get());
        sstables::file_writer rows_db_writer(make_file_output_stream(rows_db).get());

        auto close_partitions_db = defer([&] noexcept { partitions_db_writer.close(); });
        auto close_rows_db = defer([&] noexcept { rows_db_writer.close(); });

        // This test uses the `mt` (logical) format, so the payloads use the legacy
        // (non-physical) serialization.
        auto partition_index_writer = sstables::trie::bti_partition_index_writer(sst_ver, partitions_db_writer, sstables::trie::bti_position_encoding::logical());
        auto row_index_writer = sstables::trie::bti_row_index_writer(sst_ver, rows_db_writer, sstables::trie::bti_position_encoding::logical());

        // This test uses the `mt` (logical) format, so the positions are logical;
        // the post-compression (physical) position isn't exercised here.
        auto pos = [] (int64_t uncompressed) {
            return sstables::sstable_position::from_logical(uncompressed);
        };

        std::optional<partition_index_entry> last_partition_entry;
        std::optional<partition_end_entry> last_partition_end_entry;
        auto push_partition = [&] () {
            if (last_partition_entry) {
                auto& last = *last_partition_entry;
                auto pk = sstables::key::from_partition_key(*the_schema, last.dk.key());
                auto hash = utils::make_hashed_key(bytes_view(pk));
                auto payload = row_index_writer.finish(
                    *the_schema,
                    pos(last.data_file_offset),
                    pos(last_partition_end_entry.value().data_file_offset),
                    pk,
                    last.partition_tombstone);
                partition_index_writer.add(*the_schema, last.dk, hash, payload);
            }
            last_partition_entry.reset();
        };

        for (const auto& entry : dataset.entries) {
            std::visit(overloaded_functor {
                [&](const partition_index_entry& e) {
                    push_partition();
                    last_partition_entry = e;
                },
                [&](const clustering_index_entry& e) {
                    row_index_writer.add(
                        *the_schema,
                        e.first_ck,
                        e.last_ck,
                        pos(last_partition_entry.value().data_file_offset),
                        pos(e.data_file_offset),
                        e.range_tombstone_before_first_ck);
                },
                [&](const partition_end_entry& e) {
                    last_partition_end_entry = e;
                },
                [&](const eof_index_entry& e) {
                    push_partition();
                },
            }, entry);
        }
        std::move(partition_index_writer).finish(sstables::key::from_bytes({}), sstables::key::from_bytes({}));
    }

    // Step 3: create the reader (or, more precisely, a factory of readers) over the index files.
    testlog.debug("Opening index from {} and {}", partitions_path.c_str(), rows_path.c_str());
    {
        file partitions_db = open_file_dma(partitions_path.c_str(), open_flags::create | open_flags::ro).get();
        file rows_db = open_file_dma(rows_path.c_str(), open_flags::create | open_flags::ro).get();
        auto close_partitions_db = deferred_close(partitions_db);
        auto close_rows_db = deferred_close(rows_db);

        auto stats = cached_file_stats();
        auto cached_file_lru = lru();
        auto region = logalloc::region();
        auto partitions_db_size = partitions_db.size().get();
        auto rows_db_size = rows_db.size().get();
        auto partitions_db_cached = seastar::make_shared<cached_file>(partitions_db, stats, cached_file_lru, region, partitions_db_size, "Partitions.db");
        auto rows_db_cached = seastar::make_shared<cached_file>(rows_db, stats, cached_file_lru, region, rows_db_size, "Rows.db");

        auto partitions_db_footer = sstables::trie::read_bti_partitions_db_footer(*the_schema, sst_ver, partitions_db, partitions_db_size).get();
        auto partitions_db_root_pos = partitions_db_footer.trie_root_position;

        auto semaphore = tests::reader_concurrency_semaphore_wrapper();
        auto trace_state = tracing::trace_state_ptr();

        // Step 4: run the reader test on the opened readers.
        test_index(dataset, [&] {
            return sstables::trie::make_bti_index_reader(
                partitions_db_cached,
                rows_db_cached,
                partitions_db_root_pos,
                sstables::sstable_position::from_logical(0),
                sstables::sstable_position::from_logical(std::get<eof_index_entry>(dataset.entries.back()).data_file_offset),
                sst_ver,
                the_schema,
                sstables::trie::bti_position_encoding::logical(),
                semaphore.make_permit(),
                trace_state
            );
        }, max_ops);
    }
}

static std::vector<std::byte> linearize(const memory_data_sink_buffers& bufs) {
    std::vector<std::byte> retval;
    for (const auto& frag : bufs.buffers()) {
        auto v = std::as_bytes(std::span(frag));
        retval.insert(retval.end(), v.begin(), v.end());
    }
    return retval;
}

static temporary_buffer<char> make_temporary_buffer(const std::span<const std::byte> data) {
    return temporary_buffer<char>(reinterpret_cast<const char*>(data.data()), data.size());
}

static std::span<const char> char_span(const temporary_buffer<char>& buf) {
    return std::span<const char>(buf.get(), buf.size());
}

static std::generator<future<temporary_buffer<char>>> make_fragmented_generator(
    nondeterministic_choice_stack& ndcs,
    std::span<const std::byte> raw,
    size_t max_cuts
) {
    auto buf = make_temporary_buffer(raw);
    size_t n_cuts = 0;
    while (true) {
        future<> potential_yield = ndcs.choose_bool() ? seastar::yield() : make_ready_future<>();
        size_t next_frag_size = buf.size();
        if (buf.size() >= 1) {
            if (n_cuts < max_cuts) {
                ++n_cuts;
                next_frag_size = 1 + ndcs.choose_up_to(buf.size() - 1);
            }
        }
        auto result = temporary_buffer<char>(buf.get(), next_frag_size);
        testlog.trace("Next fragment: {}", fmt_hex(std::as_bytes(char_span(result))));
        buf.trim_front(result.size());
        co_yield potential_yield.then([r = std::move(result)] () mutable {
            return make_ready_future<temporary_buffer<char>>(std::move(r));
        });
    }
}

static data_source make_fragmented(nondeterministic_choice_stack& ndcs, std::span<const std::byte> raw, size_t max_cuts) {
    // Wraps `make_fragmented_generator` into a `data_source` for an `input_stream<char>`.
    struct source : data_source_impl {
        std::generator<future<temporary_buffer<char>>> _gen;
        decltype(_gen.begin()) _gen_it;
        source(nondeterministic_choice_stack& ndcs, std::span<const std::byte> raw, size_t max_cuts)
            : _gen(make_fragmented_generator(ndcs, raw, max_cuts))
            , _gen_it(_gen.begin())
        {}
        future<temporary_buffer<char>> get() override {
            auto result = std::move(*_gen_it);
            ++_gen_it;
            return result;
        }
    };
    return data_source(std::make_unique<source>(ndcs, raw, max_cuts));
}

// The per-partition headers in Rows.db can cross page boundaries,
// so parsing those headers involves handling fragmented buffers.
//
// This test tries writes a header and parses it with all possible
// fragmentations (up to a given number of cuts) and yield points.
SEASTAR_THREAD_TEST_CASE(test_read_row_index_header) {
    auto pk = sstables::key(tests::random::get_bytes(4));
    constexpr uint32_t uncompressed_chunk_length = 4096;
    // extent_hint is a byte-length upper bound; the index stores it internally
    // as a block count bounded by max_block_count(). Position equality ignores
    // extent_hint, so the exact recovered value is not asserted here; we only
    // need a value the writer accepts (>= 1 byte, and within the block-count bound).
    const uint64_t max_extent_hint = sstables::trie::bti_physical_layout(sstables::framed_chunk_codec::for_framed_chunks(uncompressed_chunk_length)).max_block_count();
    uint64_t partition_data_start = tests::random::get_int<int64_t>(0, std::numeric_limits<int64_t>::max());
    uint64_t chunk_start = tests::random::get_int<int64_t>(0, std::numeric_limits<int64_t>::max());
    uint64_t extent_hint = tests::random::get_int<uint64_t>(1, max_extent_hint);
    uint64_t offset_within_chunk = tests::random::get_int<uint64_t>(0, uncompressed_chunk_length - 1);
    uint64_t number_of_blocks = tests::random::get_int<uint64_t>();
    uint64_t root_pos = tests::random::get_int<uint64_t>();
    auto tomb = make_random_tombstone();

    nondeterministic_choice_stack ndcs;
    size_t n_cases = 0;
    do {
        // `mu` stores the full physical position; `mt` stores only the uncompressed offset.
        auto sst_ver = ndcs.choose_bool()
            ? sstables::sstable_version_types::mu
            : sstables::sstable_version_types::mt;
        auto encoding = sstables::index_holds_physical_positions(sst_ver)
            ? sstables::trie::bti_position_encoding::physical(sstables::framed_chunk_codec::for_framed_chunks(uncompressed_chunk_length))
            : sstables::trie::bti_position_encoding::logical();
        auto position_to_write = encoding.is_physical()
            ? sstables::sstable_position::from_physical(chunk_start, extent_hint, offset_within_chunk)
            : sstables::sstable_position::from_logical(partition_data_start);
        auto expected_position = position_to_write;
        memory_data_sink_buffers bufs;
        {
            sstables::file_writer fw(data_sink(std::make_unique<memory_data_sink>(bufs)));
            auto close_fw = defer([&] noexcept { fw.close(); });
            sstables::trie::write_row_index_header(
                sst_ver,
                fw,
                pk,
                position_to_write,
                encoding,
                number_of_blocks,
                root_pos,
                tomb
            );
        }
        auto vec = linearize(bufs);
        vec.append_range(std::as_bytes(std::span(std::string_view("some_suffix"))));
        uint64_t stream_size = ndcs.choose_bool() ? bufs.size() : vec.size();
        constexpr size_t max_cuts = 2;
        auto in = seastar::input_stream<char>(make_fragmented(ndcs, vec, max_cuts));
        auto semaphore = tests::reader_concurrency_semaphore_wrapper();
        auto result = sstables::trie::read_row_index_header(
            encoding,
            std::move(in),
            0,
            stream_size,
            semaphore.make_permit()).get();
        SCYLLA_ASSERT(bytes_view(result.partition_key) == bytes_view(pk));
        SCYLLA_ASSERT(result.data_file_position == expected_position);
        SCYLLA_ASSERT(result.number_of_blocks == number_of_blocks);
        SCYLLA_ASSERT(result.trie_root == root_pos);
        SCYLLA_ASSERT(result.partition_tombstone == tomb);
        ++n_cases;
    } while (ndcs.rewind());
    testlog.debug("Executed test cases: {}", n_cases);
}

SEASTAR_THREAD_TEST_CASE(test_physical_partition_payload_encoding) {
    auto the_schema = schema_builder(this_smp_shard_count(), "ks", "t")
        .with_column("pk", short_type, column_kind::partition_key)
        .with_column("ck", short_type, column_kind::clustering_key)
        .build();

    constexpr uint32_t uncompressed_chunk_length = 4096;
    // extent_hint is a byte-length upper bound; the index stores it internally
    // as a block count. Position equality ignores extent_hint, so the exact
    // recovered value is not asserted below.
    auto physical_pos = [] (uint64_t chunk_position, uint64_t extent_hint, uint64_t offset_within_chunk) {
        return sstables::sstable_position::from_physical(chunk_position, extent_hint, offset_within_chunk);
    };

    auto make_dk = [&] (int64_t token, int16_t pk_value) {
        auto pk = partition_key::from_deeply_exploded(*the_schema, {data_value(pk_value)});
        return dht::decorated_key(dht::token::from_int64(token), pk);
    };
    auto dk_direct = make_dk(1, 1);
    auto dk_rows = make_dk(2, 2);
    auto key_direct = sstables::key::from_partition_key(*the_schema, dk_direct.key());
    auto key_rows = sstables::key::from_partition_key(*the_schema, dk_rows.key());
    auto hash_direct = utils::make_hashed_key(bytes_view(key_direct));
    auto hash_rows = utils::make_hashed_key(bytes_view(key_rows));

    // extent_hint values below are byte-length hints; the writer accepts any value
    // that fits the block-count bound once converted. Equality ignores extent_hint,
    // so the recovered upper bounds aren't asserted.
    auto direct_actual = physical_pos(8192, 20, 17);
    auto direct_expected = physical_pos(8192, 20, 17);
    // The partition start's chunk isn't block-aligned, and the row block is in a
    // later chunk, so that the block's chunk position relative to the partition
    // start's chunk has a different block misalignment than the absolute one.
    auto rows_partition_start = physical_pos(12300, 8, 64);
    auto rows_partition_end = physical_pos(13000, 20, 512);
    auto row_block_actual = physical_pos(13000, 20, 128);
    auto row_block_expected = physical_pos(13000, 20, 128);
    auto eof = physical_pos(16384, 8, 0);
    auto partition_tomb = sstables::deletion_time::make_live();
    auto row_tomb = sstables::deletion_time{123, 456};
    auto ck = clustering_key::from_deeply_exploded(*the_schema, {data_value(int16_t(0))});

    tmpdir dir;
    auto partitions_path = dir.path() / "Partitions.db";
    auto rows_path = dir.path() / "Rows.db";
    {
        file partitions_db = open_file_dma(partitions_path.c_str(), open_flags::create | open_flags::wo).get();
        file rows_db = open_file_dma(rows_path.c_str(), open_flags::create | open_flags::wo).get();

        sstables::file_writer partitions_db_writer(make_file_output_stream(partitions_db).get());
        sstables::file_writer rows_db_writer(make_file_output_stream(rows_db).get());

        auto close_partitions_db = defer([&] noexcept { partitions_db_writer.close(); });
        auto close_rows_db = defer([&] noexcept { rows_db_writer.close(); });

        auto partition_index_writer = sstables::trie::bti_partition_index_writer(
            sstables::sstable_version_types::mu,
            partitions_db_writer,
            sstables::trie::bti_position_encoding::physical(sstables::framed_chunk_codec::for_framed_chunks(uncompressed_chunk_length)));
        auto row_index_writer = sstables::trie::bti_row_index_writer(
            sstables::sstable_version_types::mu,
            rows_db_writer,
            sstables::trie::bti_position_encoding::physical(sstables::framed_chunk_codec::for_framed_chunks(uncompressed_chunk_length)));

        partition_index_writer.add(*the_schema, dk_direct, hash_direct, direct_actual);

        // The row index stores the block's offset from its partition's start; the
        // reader recomposes the absolute position (row_block_expected) from the
        // partition start it reads back.
        row_index_writer.add(
            *the_schema,
            sstables::clustering_info{ck, sstables::bound_kind_m::clustering},
            sstables::clustering_info{ck, sstables::bound_kind_m::clustering},
            rows_partition_start,
            row_block_actual,
            row_tomb);
        auto rows_payload = row_index_writer.finish(
            *the_schema,
            rows_partition_start,
            rows_partition_end,
            key_rows,
            partition_tomb);
        partition_index_writer.add(*the_schema, dk_rows, hash_rows, rows_payload);
        std::move(partition_index_writer).finish(key_direct, key_rows);
    }

    file partitions_db = open_file_dma(partitions_path.c_str(), open_flags::ro).get();
    file rows_db = open_file_dma(rows_path.c_str(), open_flags::ro).get();
    auto close_partitions_db = deferred_close(partitions_db);
    auto close_rows_db = deferred_close(rows_db);

    auto stats = cached_file_stats();
    auto cached_file_lru = lru();
    auto region = logalloc::region();
    auto partitions_db_size = partitions_db.size().get();
    auto rows_db_size = rows_db.size().get();
    auto partitions_db_cached = seastar::make_shared<cached_file>(partitions_db, stats, cached_file_lru, region, partitions_db_size, "Partitions.db");
    auto rows_db_cached = seastar::make_shared<cached_file>(rows_db, stats, cached_file_lru, region, rows_db_size, "Rows.db");

    auto partitions_db_footer = sstables::trie::read_bti_partitions_db_footer(
        *the_schema,
        sstables::sstable_version_types::mu,
        partitions_db,
        partitions_db_size).get();
    auto semaphore = tests::reader_concurrency_semaphore_wrapper();
    auto trace_state = tracing::trace_state_ptr();
    auto reader = sstables::trie::make_bti_index_reader(
        partitions_db_cached,
        rows_db_cached,
        partitions_db_footer.trie_root_position,
        direct_expected,
        eof,
        sstables::sstable_version_types::mu,
        the_schema,
        sstables::trie::bti_position_encoding::physical(sstables::framed_chunk_codec::for_framed_chunks(uncompressed_chunk_length)),
        semaphore.make_permit(),
        trace_state);

    BOOST_REQUIRE(reader->advance_lower_and_check_if_present(dht::ring_position_view(dk_direct), hash_direct).get());
    BOOST_REQUIRE(reader->sstable_positions().start == direct_expected);
    BOOST_REQUIRE(!reader->partition_tombstone());

    reader->advance_to_definitely_present_partition(dk_rows).get();
    BOOST_REQUIRE(reader->sstable_positions().start == rows_partition_start);
    BOOST_REQUIRE(reader->partition_tombstone());
    BOOST_REQUIRE(reader->get_partition_key());
    BOOST_REQUIRE(bytes_view(sstables::key::from_partition_key(*the_schema, *reader->get_partition_key())) == bytes_view(key_rows));

    reader->advance_to(position_in_partition(partition_region::clustered, bound_weight(0), ck)).get();
    BOOST_REQUIRE(reader->sstable_positions().start == row_block_expected);
    // The recovered extent hint is an upper bound which rounds the chunk end up
    // to the next block boundary in the file.
    auto row_block_recovered = reader->sstable_positions().start.as_physical();
    BOOST_REQUIRE_GE(row_block_recovered.extent_hint, row_block_actual.as_physical().extent_hint);
    BOOST_REQUIRE_EQUAL((row_block_recovered.chunk_position + row_block_recovered.extent_hint) % 512, 0);
    auto marker = reader->end_open_marker();
    BOOST_REQUIRE(marker);
    BOOST_REQUIRE(marker->tomb == tombstone(row_tomb));
}
