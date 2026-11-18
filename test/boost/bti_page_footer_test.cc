/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Tests of the page footers of the BTI index files of `mu`: the chunk table
// and the CRC32 at the end of each page, the payloads that refer to the chunk
// table, and the raw data (row index headers and the Partitions.db trailer)
// that is written around the footers.

#include <random>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/core/file.hh>
#include <seastar/util/closeable.hh>
#include <seastar/util/defer.hh>
#include "sstables/compress.hh"
#include "sstables/exceptions.hh"
#include "sstables/framed_chunk.hh"
#include "sstables/mx/types.hh"
#include "sstables/trie/bti_index.hh"
#include "sstables/trie/bti_index_internal.hh"
#include "sstables/trie/bti_node_reader.hh"
#include "sstables/trie/bti_page_footer.hh"
#include "schema/schema_builder.hh"
#include "test/lib/exception_utils.hh"
#include "test/lib/log.hh"
#include "test/lib/random_utils.hh"
#include "test/lib/reader_concurrency_semaphore.hh"
#include "utils/cached_file.hh"
#include "utils/i_filter.hh"
#include "utils/memory_data_sink.hh"

using namespace sstables::trie;
using sstables::sstable_position;
using sstables::sstable_version_types;
using u128 = unsigned __int128;

namespace {

// A read-only file in memory. A test can change the bytes after the file is
// created, e.g. to corrupt them.
class memory_file_impl : public seastar::file_impl {
    lw_shared_ptr<std::vector<char>> _data;
    [[noreturn]] static void unsupported() {
        throw std::logic_error("memory_file_impl: unsupported operation");
    }
    size_t available(uint64_t pos, size_t len) const {
        return pos < _data->size() ? std::min<size_t>(len, _data->size() - pos) : 0;
    }
public:
    explicit memory_file_impl(lw_shared_ptr<std::vector<char>> data) : _data(std::move(data)) {}
    future<size_t> write_dma(uint64_t, const void*, size_t, io_intent*) override { unsupported(); }
    future<size_t> write_dma(uint64_t, std::vector<iovec>, io_intent*) override { unsupported(); }
    future<size_t> read_dma(uint64_t pos, void* buffer, size_t len, io_intent*) override {
        auto n = available(pos, len);
        std::memcpy(buffer, _data->data() + pos, n);
        return make_ready_future<size_t>(n);
    }
    future<size_t> read_dma(uint64_t, std::vector<iovec>, io_intent*) override { unsupported(); }
    future<temporary_buffer<uint8_t>> dma_read_bulk(uint64_t offset, size_t range_size, io_intent*) override {
        auto n = available(offset, range_size);
        return make_ready_future<temporary_buffer<uint8_t>>(
            temporary_buffer<uint8_t>(reinterpret_cast<const uint8_t*>(_data->data() + offset), n));
    }
    future<> flush() override { return make_ready_future(); }
    future<struct stat> stat() override {
        struct stat st = {};
        st.st_size = _data->size();
        return make_ready_future<struct stat>(st);
    }
    future<> truncate(uint64_t) override { unsupported(); }
    future<> discard(uint64_t, uint64_t) override { unsupported(); }
    future<> allocate(uint64_t, uint64_t) override { unsupported(); }
    future<uint64_t> size() override { return make_ready_future<uint64_t>(_data->size()); }
    future<> close() override { return make_ready_future(); }
    subscription<directory_entry> list_directory(std::function<future<>(directory_entry)>) override { unsupported(); }
};

using file_bytes = lw_shared_ptr<std::vector<char>>;

// `n` bytes of value `v`.
bytes filled(size_t n, int v) {
    bytes b(bytes::initialized_later(), n);
    std::ranges::fill(b, int8_t(v));
    return b;
}

bytes to_bytes_of(std::initializer_list<int8_t> v) {
    bytes b(bytes::initialized_later(), v.size());
    std::ranges::copy(v, b.begin());
    return b;
}

seastar::file make_memory_file(file_bytes data) {
    return seastar::file(seastar::make_shared<memory_file_impl>(std::move(data)));
}

std::span<const std::byte> as_bytes(const file_bytes& f) {
    return std::as_bytes(std::span(*f));
}

schema_ptr make_schema() {
    return schema_builder(this_smp_shard_count(), "ks", "t")
        .with_column("pk", bytes_type, column_kind::partition_key)
        .with_column("ck", bytes_type, column_kind::clustering_key)
        .build();
}

struct test_block {
    bytes first_ck;
    bytes last_ck;
    sstable_position start;
    sstables::deletion_time range_tombstone;
};

struct test_partition {
    int64_t token;
    bytes key;
    sstable_position start;
    // The position of the END_OF_PARTITION byte.
    sstable_position end;
    sstables::deletion_time tombstone = sstables::deletion_time::make_live();
    // Partitions with no blocks have no row index.
    std::vector<test_block> blocks;

    dht::decorated_key dk(const schema& s) const {
        return dht::decorated_key(dht::token::from_int64(token), partition_key::from_single_value(s, key));
    }
    sstables::key sst_key(const schema& s) const {
        return sstables::key::from_partition_key(s, partition_key::from_single_value(s, key));
    }
};

struct index_files {
    file_bytes partitions = make_lw_shared<std::vector<char>>();
    file_bytes rows = make_lw_shared<std::vector<char>>();
    // The Rows.db position of the row index header of each partition, if it has one.
    std::vector<std::optional<uint64_t>> header_positions;
};

void linearize_into(const memory_data_sink_buffers& bufs, std::vector<char>& out) {
    for (const auto& frag : bufs.buffers()) {
        out.insert(out.end(), frag.get(), frag.get() + frag.size());
    }
}

sstables::clustering_info make_ck(const schema& s, const bytes& v) {
    return sstables::clustering_info{clustering_key::from_single_value(s, v), sstables::bound_kind_m::clustering};
}

// Writes Partitions.db and Rows.db for `partitions`, the way the sstable writer does.
// `trailer_keys` overrides the first and last key stored in the Partitions.db trailer.
index_files write_index(
    const schema& s,
    sstable_version_types sst_ver,
    const bti_position_encoding& encoding,
    const std::vector<test_partition>& partitions,
    std::optional<std::pair<bytes, bytes>> trailer_keys = {}
) {
    index_files result;
    memory_data_sink_buffers partitions_bufs;
    memory_data_sink_buffers rows_bufs;
    {
        sstables::file_writer partitions_db_writer(data_sink(std::make_unique<memory_data_sink>(partitions_bufs)));
        sstables::file_writer rows_db_writer(data_sink(std::make_unique<memory_data_sink>(rows_bufs)));
        auto close_partitions_db = defer([&] noexcept { partitions_db_writer.close(); });
        auto close_rows_db = defer([&] noexcept { rows_db_writer.close(); });
        auto partition_index_writer = bti_partition_index_writer(sst_ver, partitions_db_writer, encoding);
        auto row_index_writer = bti_row_index_writer(sst_ver, rows_db_writer, encoding);
        for (const auto& p : partitions) {
            for (const auto& b : p.blocks) {
                row_index_writer.add(s, make_ck(s, b.first_ck), make_ck(s, b.last_ck), p.start, b.start, b.range_tombstone);
            }
            auto key = p.sst_key(s);
            auto target = row_index_writer.finish(s, p.start, p.end, key, p.tombstone);
            auto rows_pos = target.rows_db();
            result.header_positions.push_back(rows_pos ? std::optional(rows_pos->value) : std::nullopt);
            partition_index_writer.add(s, p.dk(s), utils::make_hashed_key(bytes_view(key)), target);
        }
        row_index_writer.finish_file();
        auto first = trailer_keys ? sstables::key::from_bytes(trailer_keys->first) : partitions.front().sst_key(s);
        auto last = trailer_keys ? sstables::key::from_bytes(trailer_keys->second) : partitions.back().sst_key(s);
        std::move(partition_index_writer).finish(first, last);
    }
    linearize_into(partitions_bufs, *result.partitions);
    linearize_into(rows_bufs, *result.rows);
    return result;
}

// Opens index files for reading, the way an sstable does.
class opened_index {
    cached_file_stats _stats;
    lru _lru;
    logalloc::region _region;
    schema_ptr _schema;
    sstable_version_types _sst_ver;
    bti_position_encoding _encoding;
public:
    seastar::shared_ptr<cached_file> partitions;
    seastar::shared_ptr<cached_file> rows;
    std::optional<bti_partitions_db_footer> _footer;
    const bti_partitions_db_footer& footer() const {
        return *_footer;
    }

    opened_index(schema_ptr s, sstable_version_types sst_ver, const bti_position_encoding& encoding, const index_files& files)
        : _schema(std::move(s))
        , _sst_ver(sst_ver)
        , _encoding(encoding)
    {
        partitions = seastar::make_shared<cached_file>(make_memory_file(files.partitions), _stats, _lru, _region,
            files.partitions->size(), "Partitions.db", make_bti_page_verifier(sst_ver, "Partitions.db"));
        rows = seastar::make_shared<cached_file>(make_memory_file(files.rows), _stats, _lru, _region,
            files.rows->size(), "Rows.db", make_bti_page_verifier(sst_ver, "Rows.db"));
        // Like the sstable, read the trailer through the cache.
        _footer = read_bti_partitions_db_footer(*_schema, sst_ver, make_cached_seastar_file(*partitions), partitions->size()).get();
    }
    ~opened_index() {
        partitions = nullptr;
        rows = nullptr;
    }
    std::unique_ptr<sstables::abstract_index_reader> make_reader(reader_permit permit, sstable_position start, sstable_position end) {
        return make_bti_index_reader(partitions, rows, footer().trie_root_position, start, end, _sst_ver, _schema, _encoding, std::move(permit), {});
    }
};

// A Data.db with chunks of random extents, for generating physical positions.
class data_model {
    uint32_t _chunk_length;
    uint64_t _max_extent;
    std::mt19937_64& _rng;
    std::vector<uint64_t> _chunk_positions;
    std::vector<uint64_t> _extents;
public:
    data_model(uint32_t chunk_length, uint64_t first_chunk_position, std::mt19937_64& rng)
        : _chunk_length(chunk_length)
        , _max_extent(sstables::framed_chunk_codec::for_framed_chunks(chunk_length).extent(sstables::compressed_chunk_length_limit(chunk_length)))
        , _rng(rng)
        , _chunk_positions{first_chunk_position}
    {}
    // The physical position of the uncompressed position `u`.
    sstable_position at(uint64_t u) {
        auto i = u / _chunk_length;
        while (_extents.size() <= i) {
            _extents.push_back(1 + _rng() % _max_extent);
            _chunk_positions.push_back(_chunk_positions.back() + _extents.back());
        }
        return sstable_position::from_physical(_chunk_positions[i], _extents[i], u % _chunk_length);
    }
};

bytes random_bytes(std::mt19937_64& rng, size_t len) {
    bytes b(bytes::initialized_later(), len);
    for (auto& c : b) {
        c = int8_t(rng() % 4);
    }
    return b;
}

sstables::deletion_time random_tombstone(std::mt19937_64& rng) {
    return rng() % 4 ? sstables::deletion_time::make_live()
        : sstables::deletion_time{int32_t(rng() % 100000), int64_t(rng() % 100000)};
}

// Generates partitions laid out in Data.db by `to_position`, which maps
// uncompressed positions to positions of the index.
//
// Many partitions are small, so that they share chunks. Some have row indexes,
// and some have long keys, so that row index headers span pages.
std::vector<test_partition> generate_partitions(std::mt19937_64& rng, size_t n, std::function<sstable_position(uint64_t)> to_position) {
    std::vector<test_partition> result;
    int64_t token = -int64_t(rng() % 1000000);
    uint64_t u = rng() % 1000;
    for (size_t i = 0; i < n; ++i) {
        token += 1 + (rng() % 4 ? rng() % 100 : rng() % 100000000);
        test_partition p{
            .token = token,
            .key = random_bytes(rng, rng() % 32 ? 1 + rng() % 16 : 1 + rng() % 9000),
            .start = to_position(u),
            .end = to_position(u),
            .tombstone = random_tombstone(rng),
        };
        uint64_t size;
        switch (rng() % 8) {
        case 0:
        case 1: {
            // A partition with a row index.
            size_t n_blocks = 1 + rng() % 5;
            std::vector<bytes> cks;
            while (cks.size() < 2 * n_blocks) {
                cks.push_back(random_bytes(rng, 1 + rng() % (rng() % 16 ? 8 : 300)));
                std::ranges::sort(cks, [] (const bytes& a, const bytes& b) { return compare_unsigned(a, b) < 0; });
                auto [first, last] = std::ranges::unique(cks);
                cks.erase(first, last);
            }
            uint64_t block_u = u + 1 + rng() % 100;
            for (size_t b = 0; b < n_blocks; ++b) {
                p.blocks.push_back(test_block{
                    .first_ck = cks[2 * b],
                    .last_ck = cks[2 * b + 1],
                    .start = to_position(block_u),
                    .range_tombstone = random_tombstone(rng),
                });
                block_u += 1 + rng() % 70000;
            }
            size = block_u - u;
            break;
        }
        case 2:
            size = 1 + rng() % 30000;
            break;
        default:
            size = 1 + rng() % 200;
            break;
        }
        u += size;
        p.end = to_position(u - 1);
        result.push_back(std::move(p));
    }
    return result;
}

// A payload of the partition index, as found in the trie.
struct payload_ref {
    uint64_t pos;
    uint8_t bits;
    const_bytes bytes;
};

// Returns the payloads of the trie with the given root, in key order.
std::vector<payload_ref> trie_payloads(std::span<const std::byte> file, uint64_t root) {
    auto page_tail = [&] (uint64_t pos) {
        return file.subspan(pos, round_down(pos, BTI_PAGE_SIZE) + BTI_PAGE_SIZE - pos);
    };
    std::vector<payload_ref> result;
    struct frame {
        uint64_t pos;
        int next_child;
        int n_children;
    };
    std::vector<frame> stack;
    auto enter = [&] (uint64_t pos) {
        auto node = bti_read_node(pos, page_tail(pos));
        if (node.payload_bits) {
            result.push_back({pos, node.payload_bits, bti_get_payload(pos, page_tail(pos))});
        }
        stack.push_back({pos, 0, int(node.n_children)});
    };
    enter(root);
    while (!stack.empty()) {
        auto f = stack.back();
        if (f.next_child >= f.n_children) {
            stack.pop_back();
            continue;
        }
        auto child = bti_get_child(f.pos, page_tail(f.pos), f.next_child, true);
        stack.back().next_child = child.idx + 1;
        enter(f.pos - child.offset);
    }
    return result;
}

enum class payload_form { rows, inline_data, slot };

struct decoded_payload {
    payload_form form;
    unsigned __int128 value;
    unsigned value_bits;
};

decoded_payload decode_partition_payload(const payload_ref& p) {
    unsigned k = p.bits - 1;
    auto v = read_fixed_be_u128(p.bytes, 1, k);
    unsigned bits = 8 * k;
    if (!(v >> (bits - 1))) {
        return {payload_form::rows, v, bits};
    }
    return {(v >> (bits - 2)) & 1 ? payload_form::slot : payload_form::inline_data, v, bits};
}

// Checks every page of a file with page footers: its size, its CRC32, and that
// its chunk table holds no duplicates. Returns the chunk tables, by page.
std::vector<std::vector<uint64_t>> check_pages(std::span<const std::byte> file) {
    BOOST_REQUIRE_EQUAL(file.size() % BTI_PAGE_SIZE, 0);
    std::vector<std::vector<uint64_t>> tables;
    for (uint64_t page_pos = 0; page_pos < file.size(); page_pos += BTI_PAGE_SIZE) {
        auto page = file.subspan(page_pos, BTI_PAGE_SIZE);
        verify_bti_page(page, page_pos, BTI_PAGE_SIZE, "test");
        auto view = bti_page_view(page, page_pos);
        std::vector<uint64_t> table;
        for (unsigned slot = 0; slot < view.table_size(); ++slot) {
            table.push_back(view.table_entry(slot));
        }
        auto sorted = table;
        std::ranges::sort(sorted);
        BOOST_REQUIRE_MESSAGE(std::ranges::adjacent_find(sorted) == sorted.end(), fmt::format("duplicate chunk table entry in page {}", page_pos));
        tables.push_back(std::move(table));
    }
    return tables;
}

bool same_chunk(const test_partition& a, const test_partition& b) {
    return a.blocks.empty() && b.blocks.empty()
        && a.start.as_physical().chunk_position == b.start.as_physical().chunk_position;
}

// Whether each partition is expected to refer to its chunk through a slot:
// iff it has no row index, and starts in the same chunk as the previous or the
// next partition, and its chunk fits in a table entry, and two slot payloads
// together with a table entry are smaller than two inline payloads.
std::vector<bool> expected_slots(const std::vector<test_partition>& partitions, const bti_physical_layout& layout) {
    std::vector<bool> result(partitions.size());
    for (size_t i = 0; i < partitions.size(); ++i) {
        bool shared = (i > 0 && same_chunk(partitions[i - 1], partitions[i]))
            || (i + 1 < partitions.size() && same_chunk(partitions[i], partitions[i + 1]));
        if (!shared) {
            continue;
        }
        auto chunk_position = partitions[i].start.as_physical().chunk_position;
        if (chunk_position > bti_max_table_chunk_position(layout.block_count_bits())) {
            continue;
        }
        auto inline_bytes = div_ceil(std::bit_width(chunk_position) + 2 + layout.packed_bits(), 8u);
        auto slot_bytes = div_ceil(2 + layout.offset_within_chunk_bits() + bti_slot_bits, 8u);
        result[i] = 2 * inline_bytes > 2 * slot_bytes + bti_chunk_table_entry_size;
    }
    return result;
}

// Checks the Partitions.db payloads of physical positions against the partitions:
// - Each payload has the expected form: a Rows.db position, an inline Data.db
//   position, or a slot (see expected_slots()).
// - Each slot exists, and holds the partition's chunk.
// - Each chunk table entry is used, so the tables are deduplicated.
// Returns the number of slot payloads.
size_t check_partition_payloads(const index_files& files, const bti_partitions_db_footer& footer,
        const std::vector<test_partition>& partitions, const bti_physical_layout& layout) {
    auto file = as_bytes(files.partitions);
    auto tables = check_pages(file);
    auto payloads = trie_payloads(file, footer.trie_root_position);
    BOOST_REQUIRE_EQUAL(payloads.size(), partitions.size());
    std::vector<std::vector<bool>> used(tables.size());
    for (size_t i = 0; i < tables.size(); ++i) {
        used[i].resize(tables[i].size());
    }
    size_t n_slots = 0;
    auto slots = expected_slots(partitions, layout);
    for (size_t i = 0; i < partitions.size(); ++i) {
        const auto& p = partitions[i];
        auto d = decode_partition_payload(payloads[i]);
        if (!p.blocks.empty()) {
            BOOST_REQUIRE(d.form == payload_form::rows);
            BOOST_REQUIRE_EQUAL(uint64_t(d.value), *files.header_positions[i]);
            continue;
        }
        auto pc = p.start.as_physical();
        auto block_count = bti_extent_to_block_count(pc.chunk_position, pc.extent_hint);
        if (slots[i]) {
            BOOST_REQUIRE_MESSAGE(d.form == payload_form::slot, fmt::format("partition {} should use a slot", i));
            BOOST_REQUIRE_EQUAL(d.value_bits, 8 * div_ceil(2 + layout.offset_within_chunk_bits() + bti_slot_bits, 8u));
            auto slot = unsigned(d.value & 0x1ff);
            auto page_idx = payloads[i].pos / BTI_PAGE_SIZE;
            BOOST_REQUIRE_LT(slot, tables[page_idx].size());
            used[page_idx][slot] = true;
            BOOST_REQUIRE_EQUAL(tables[page_idx][slot], encode_bti_chunk_table_entry(pc.chunk_position, block_count, layout.block_count_bits()));
            BOOST_REQUIRE_EQUAL(uint64_t((d.value & ~(u128(3) << (d.value_bits - 2))) >> bti_slot_bits), pc.offset_within_chunk);
            ++n_slots;
        } else {
            BOOST_REQUIRE_MESSAGE(d.form == payload_form::inline_data, fmt::format("partition {} should be inline", i));
            // The writer uses the smallest size.
            BOOST_REQUIRE_EQUAL(d.value_bits, 8 * div_ceil(std::bit_width(pc.chunk_position) + 2 + layout.packed_bits(), 8u));
            auto rest = d.value & ~(u128(3) << (d.value_bits - 2));
            BOOST_REQUIRE_EQUAL(uint64_t(rest >> layout.packed_bits()), pc.chunk_position);
            BOOST_REQUIRE_EQUAL(uint64_t(rest & ((u128(1) << layout.packed_bits()) - 1)), layout.pack(block_count, pc.offset_within_chunk));
        }
    }
    for (size_t i = 0; i < tables.size(); ++i) {
        for (size_t slot = 0; slot < tables[i].size(); ++slot) {
            BOOST_REQUIRE_MESSAGE(used[i][slot], fmt::format("unused chunk table slot {} in page {}", slot, i));
        }
    }
    return n_slots;
}

// Position equality ignores extent_hint, so for physical positions this also
// checks it. The index stores the extent as a count of whole blocks, so the
// decoded hint is the expected extent rounded out to blocks.
void require_same_position(const sstable_position& actual, const sstable_position& expected) {
    BOOST_REQUIRE_MESSAGE(actual == expected, fmt::format("position {} != expected {}", actual, expected));
    if (expected.is_physical()) {
        auto e = expected.as_physical();
        auto expected_hint = bti_extent_hint_from_block_count(e.chunk_position, bti_extent_to_block_count(e.chunk_position, e.extent_hint));
        BOOST_REQUIRE_MESSAGE(actual.as_physical().extent_hint == expected_hint,
                fmt::format("position {}: extent hint != expected {}", actual, expected_hint));
    }
}

// Checks that the index reader finds every partition and every block,
// both by lookups and by a scan.
void check_reads(opened_index& idx, const schema& s, const std::vector<test_partition>& partitions,
        sstable_position start, sstable_position end) {
    tests::reader_concurrency_semaphore_wrapper semaphore;
    for (const auto& p : partitions) {
        auto r = idx.make_reader(semaphore.make_permit(), start, end);
        auto close_r = deferred_close(*r);
        auto dk = p.dk(s);
        auto key = p.sst_key(s);
        BOOST_REQUIRE(r->advance_lower_and_check_if_present(dht::ring_position_view(dk), utils::make_hashed_key(bytes_view(key))).get());
        require_same_position(r->sstable_positions().start, p.start);
        if (p.blocks.empty()) {
            BOOST_REQUIRE(!r->partition_tombstone());
            continue;
        }
        BOOST_REQUIRE(r->partition_tombstone() == p.tombstone);
        BOOST_REQUIRE(r->get_partition_key());
        BOOST_REQUIRE(bytes_view(sstables::key::from_partition_key(s, *r->get_partition_key())) == bytes_view(key));
        auto last = r->last_block_offset().get();
        BOOST_REQUIRE(last);
        require_same_position(p.start + *last, p.blocks.back().start);
        for (const auto& b : p.blocks) {
            r->advance_to(position_in_partition(partition_region::clustered, bound_weight::equal, clustering_key::from_single_value(s, b.first_ck))).get();
            require_same_position(r->sstable_positions().start, b.start);
            if (b.range_tombstone.live()) {
                BOOST_REQUIRE(!r->end_open_marker());
            } else {
                BOOST_REQUIRE(r->end_open_marker());
                BOOST_REQUIRE(r->end_open_marker()->tomb == tombstone(b.range_tombstone));
            }
        }
    }
    auto r = idx.make_reader(semaphore.make_permit(), start, end);
    auto close_r = deferred_close(*r);
    r->advance_to(dht::partition_range::make_open_ended_both_sides()).get();
    for (const auto& p : partitions) {
        BOOST_REQUIRE(!r->eof());
        require_same_position(r->sstable_positions().start, p.start);
        r->advance_to_next_partition().get();
    }
    BOOST_REQUIRE(r->eof());
}

const std::vector<uint32_t> all_chunk_lengths = {1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072};

bti_position_encoding physical_encoding(uint32_t chunk_length) {
    return bti_position_encoding::physical(sstables::framed_chunk_codec::for_framed_chunks(chunk_length));
}

sstable_position end_position(const std::vector<test_partition>& partitions, const bti_position_encoding& encoding) {
    auto end = partitions.back().end;
    if (encoding.is_physical()) {
        auto pc = end.as_physical();
        return sstable_position::from_physical(pc.chunk_position + pc.extent_hint, 1, 0);
    }
    return sstable_position::from_logical(end.to_logical() + 1);
}

} // anonymous namespace

// Writes and reads a random index for every allowed chunk length.
SEASTAR_THREAD_TEST_CASE(test_mu_index_roundtrip) {
    auto s = make_schema();
    auto seed = tests::random::get_int<uint64_t>();
    testlog.info("seed={}", seed);
    std::mt19937_64 rng(seed);
    for (auto chunk_length : all_chunk_lengths) {
        BOOST_TEST_CONTEXT("chunk_length=" << chunk_length) {
            auto encoding = physical_encoding(chunk_length);
            // The first chunk position. At 0, the chunk positions are small, so
            // slots pay off only later in the file, if at all. From 2^26, slots pay
            // off for every chunk length. Near the largest chunk position of a table
            // entry, the file crosses it, and above it, slots aren't used.
            // Large chunk positions also need wide inline payloads.
            const auto max_table_chunk = bti_max_table_chunk_position(encoding.layout().block_count_bits());
            uint64_t first_chunk = 0;
            bool expect_slots = false;
            switch (rng() % 4) {
            case 0:
                break;
            case 1:
                first_chunk = uint64_t(1) << (26 + rng() % (std::bit_width(max_table_chunk) - 27));
                expect_slots = true;
                break;
            case 2:
                first_chunk = max_table_chunk - 10'000'000;
                break;
            case 3:
                first_chunk = max_table_chunk + 1 + rng() % (uint64_t(1) << 52);
                break;
            }
            testlog.info("first chunk position: {}", first_chunk);
            auto model = data_model(chunk_length, first_chunk, rng);
            auto partitions = generate_partitions(rng, 1500, [&] (uint64_t u) { return model.at(u); });
            auto files = write_index(*s, sstable_version_types::mu, encoding, partitions);
            // check_partition_payloads() checks the pages of Partitions.db.
            for (const auto& t : check_pages(as_bytes(files.rows))) {
                BOOST_REQUIRE(t.empty());
            }
            opened_index idx(s, sstable_version_types::mu, encoding, files);
            BOOST_REQUIRE_EQUAL(idx.footer().partition_count, partitions.size());
            BOOST_REQUIRE(bytes_view(idx.footer().first_key) == bytes_view(partitions.front().sst_key(*s)));
            BOOST_REQUIRE(bytes_view(idx.footer().last_key) == bytes_view(partitions.back().sst_key(*s)));
            auto n_slots = check_partition_payloads(files, idx.footer(), partitions, encoding.layout());
            testlog.info("chunk_length={}: Partitions.db {} bytes, Rows.db {} bytes, {} of {} partition payloads use slots",
                chunk_length, files.partitions->size(), files.rows->size(), n_slots, partitions.size());
            if (expect_slots) {
                BOOST_REQUIRE_GT(n_slots, 0);
            }
            check_reads(idx, *s, partitions, partitions.front().start, end_position(partitions, encoding));
        }
    }
}

// An uncompressed `mu` sstable has logical positions, but its index files
// have page footers, with empty chunk tables.
SEASTAR_THREAD_TEST_CASE(test_uncompressed_mu_index_roundtrip) {
    auto s = make_schema();
    auto seed = tests::random::get_int<uint64_t>();
    testlog.info("seed={}", seed);
    std::mt19937_64 rng(seed);
    auto partitions = generate_partitions(rng, 1500, [] (uint64_t u) { return sstable_position::from_logical(u); });
    auto encoding = bti_position_encoding::logical();
    auto files = write_index(*s, sstable_version_types::mu, encoding, partitions);
    for (auto f : {files.partitions, files.rows}) {
        for (const auto& t : check_pages(as_bytes(f))) {
            BOOST_REQUIRE(t.empty());
        }
    }
    opened_index idx(s, sstable_version_types::mu, encoding, files);
    check_reads(idx, *s, partitions, partitions.front().start, end_position(partitions, encoding));
}

// The first and the last key refer to their chunk by a slot iff they share it
// with their only neighbour, and slots pay off for the chunk.
SEASTAR_THREAD_TEST_CASE(test_mu_payload_form_at_first_and_last_key) {
    auto s = make_schema();
    auto encoding = physical_encoding(4096);
    // With L = 4096, the slot payload has 3 bytes, a table entry has 5 bytes,
    // and a table entry holds chunk positions below 2^36.
    // - At 2^30, the inline payload has 7 bytes, so 2 keys in one chunk pay for
    //   a table entry.
    // - At 2^20, the inline payload has 5 bytes, so 2 keys in one chunk don't pay
    //   for a table entry. 3 keys would, but the writer doesn't look that far ahead.
    // - At 2^36, the inline payload has 7 bytes, but the chunk doesn't fit in a
    //   table entry.
    // - Below 2^6, the inline payload has 3 bytes, so slots never pay off.
    for (uint64_t base : {uint64_t(1) << 30, uint64_t(1) << 20, uint64_t(1) << 36, uint64_t(0)}) {
        const bool slots_pay_off = base == uint64_t(1) << 30;
        for (bool shared_ends : {false, true}) {
            BOOST_TEST_CONTEXT("base=" << base << " shared_ends=" << shared_ends) {
                std::vector<test_partition> partitions;
                auto add = [&] (uint64_t c, uint64_t o) {
                    partitions.push_back(test_partition{
                        .token = int64_t(partitions.size()),
                        .key = filled(1, partitions.size()),
                        .start = sstable_position::from_physical(base + c, 3000, o),
                        .end = sstable_position::from_physical(base + c, 3000, o + 10),
                    });
                };
                if (shared_ends) {
                    add(0, 0);
                    add(0, 100);
                } else {
                    add(0, 0);
                    add(10, 0);
                }
                add(20, 0);
                add(20, 100);
                add(20, 200);
                if (shared_ends) {
                    add(40, 0);
                    add(40, 100);
                } else {
                    add(30, 0);
                    add(40, 0);
                }
                auto files = write_index(*s, sstable_version_types::mu, encoding, partitions);
                opened_index idx(s, sstable_version_types::mu, encoding, files);
                check_partition_payloads(files, idx.footer(), partitions, encoding.layout());
                auto payloads = trie_payloads(as_bytes(files.partitions), idx.footer().trie_root_position);
                auto expected = shared_ends && slots_pay_off ? payload_form::slot : payload_form::inline_data;
                BOOST_REQUIRE(decode_partition_payload(payloads.front()).form == expected);
                BOOST_REQUIRE(decode_partition_payload(payloads.back()).form == expected);
                auto middle = slots_pay_off ? payload_form::slot : payload_form::inline_data;
                BOOST_REQUIRE(decode_partition_payload(payloads[3]).form == middle);
                check_reads(idx, *s, partitions, partitions.front().start, end_position(partitions, encoding));
            }
        }
    }
}

// Partitions which share chunks across the subtrees that the trie writer packs
// into one page share chunk table entries.
SEASTAR_THREAD_TEST_CASE(test_mu_chunk_table_deduplication) {
    auto s = make_schema();
    auto encoding = physical_encoding(4096);
    std::mt19937_64 rng(tests::random::get_int<uint64_t>());
    // Many partitions per chunk, and tokens with varied prefixes, so that the
    // trie has many subtrees per page, and adjacent subtrees share chunks.
    // The chunk positions are large enough for slots to pay off.
    const uint64_t base = uint64_t(1) << 30;
    std::vector<test_partition> partitions;
    int64_t token = std::numeric_limits<int64_t>::min() / 2;
    for (size_t i = 0; i < 20000; ++i) {
        token += 1 + rng() % (uint64_t(1) << 48);
        uint64_t u = i * 37;
        partitions.push_back(test_partition{
            .token = token,
            .key = filled(1, i),
            .start = sstable_position::from_physical(base + (u / 4096) * 3000, 3000, u % 4096),
            .end = sstable_position::from_physical(base + ((u + 36) / 4096) * 3000, 3000, (u + 36) % 4096),
        });
    }
    auto files = write_index(*s, sstable_version_types::mu, encoding, partitions);
    opened_index idx(s, sstable_version_types::mu, encoding, files);
    auto n_slots = check_partition_payloads(files, idx.footer(), partitions, encoding.layout());
    size_t n_entries = 0;
    size_t n_pages_with_tables = 0;
    for (const auto& t : check_pages(as_bytes(files.partitions))) {
        n_entries += t.size();
        n_pages_with_tables += !t.empty();
    }
    testlog.info("Partitions.db: {} bytes, {} slot payloads, {} chunks, {} table entries in {} pages",
        files.partitions->size(), n_slots, partitions.size() * 37 / 4096 + 1, n_entries, n_pages_with_tables);
    // Each chunk has an entry in at most a few pages.
    BOOST_REQUIRE_LT(n_entries, 2 * (partitions.size() * 37 / 4096 + 1) + 2 * n_pages_with_tables);
    check_reads(idx, *s, partitions, partitions.front().start, end_position(partitions, encoding));
}

// A row index header can span pages, and can end, or have any of its fields
// start, exactly at the start of a chunk table.
SEASTAR_THREAD_TEST_CASE(test_mu_row_index_header_across_pages) {
    auto s = make_schema();
    for (bool physical : {true, false}) {
        auto encoding = physical ? physical_encoding(4096) : bti_position_encoding::logical();
        auto pos = [&] (uint64_t u) {
            return physical ? sstable_position::from_physical((u / 4096) * 3000, 3000, u % 4096) : sstable_position::from_logical(u);
        };
        auto make_partitions = [&] (size_t key_length) {
            return std::vector<test_partition>{
                {.token = 1, .key = filled(1, 1), .start = pos(0), .end = pos(10)},
                {.token = 2, .key = filled(key_length, 2), .start = pos(11), .end = pos(200000),
                 .tombstone = sstables::deletion_time{7, 8}, .blocks = {
                    {.first_ck = filled(1, 1), .last_ck = filled(1, 2), .start = pos(50), .range_tombstone = sstables::deletion_time::make_live()},
                    {.first_ck = filled(1, 3), .last_ck = filled(1, 4), .start = pos(100000), .range_tombstone = sstables::deletion_time{1, 2}},
                }},
                {.token = 3, .key = filled(1, 3), .start = pos(200001), .end = pos(200010)},
            };
        };
        // The header starts after the row index trie.
        auto header_pos = *write_index(*s, sstable_version_types::mu, encoding, make_partitions(1)).header_positions[1];
        BOOST_REQUIRE_LT(header_pos, 100);
        auto usable = BTI_PAGE_SIZE - bti_page_footer_size;
        // The header's raw bytes start with the 2-byte key length.
        // Cover all ends of the header and of its fields near the end of the first and second page.
        size_t n_cases = 0;
        for (auto boundary : {usable, 2 * usable}) {
            for (size_t key_length = boundary - header_pos - 2 - 40; key_length <= boundary - header_pos - 2 + 4; ++key_length) {
                BOOST_TEST_CONTEXT("physical=" << physical << " key_length=" << key_length) {
                    auto partitions = make_partitions(key_length);
                    auto files = write_index(*s, sstable_version_types::mu, encoding, partitions);
                    BOOST_REQUIRE_EQUAL(*files.header_positions[1], header_pos);
                    check_pages(as_bytes(files.rows));
                    opened_index idx(s, sstable_version_types::mu, encoding, files);
                    check_reads(idx, *s, partitions, partitions.front().start, end_position(partitions, encoding));
                    ++n_cases;
                }
            }
        }
        BOOST_REQUIRE_GT(n_cases, 0);
    }
}

// Returns the fixed tail of the Partitions.db trailer: the key position,
// the partition count and the root position. It ends at the chunk table of the
// last page.
static std::array<uint64_t, 3> trailer_tail(std::span<const std::byte> file) {
    auto last_page_pos = file.size() - BTI_PAGE_SIZE;
    auto last_page = file.subspan(last_page_pos);
    auto usable = bti_page_view(last_page, last_page_pos).usable_size();
    BOOST_REQUIRE_GE(usable, 24);
    auto tail = reinterpret_cast<const char*>(last_page.data() + usable - 24);
    return {seastar::read_be<uint64_t>(tail), seastar::read_be<uint64_t>(tail + 8), seastar::read_be<uint64_t>(tail + 16)};
}

// The keys of the Partitions.db trailer can span pages, and the fixed tail of
// the trailer ends at the chunk table of the last page.
// Covers all ends of the keys near the ends of two pages: with room for the
// tail, with too little room, and exactly at the chunk table.
SEASTAR_THREAD_TEST_CASE(test_mu_partitions_db_trailer_across_pages) {
    auto s = make_schema();
    auto encoding = physical_encoding(4096);
    std::mt19937_64 rng(tests::random::get_int<uint64_t>());
    auto model = data_model(4096, 0, rng);
    auto partitions = generate_partitions(rng, 300, [&] (uint64_t u) { return model.at(u); });
    auto last_key = filled(3, 7);
    auto write = [&] (size_t first_key_length) {
        return write_index(*s, sstable_version_types::mu, encoding, partitions, std::pair(filled(first_key_length, 5), last_key));
    };
    // The keys start after the trie, at the same position for every key length.
    auto files0 = write(0);
    auto [keys_pos, count0, root0] = trailer_tail(as_bytes(files0.partitions));
    auto keys_page = round_down(keys_pos, BTI_PAGE_SIZE);
    auto table_start = keys_page + bti_page_view(as_bytes(files0.partitions).subspan(keys_page, BTI_PAGE_SIZE), keys_page).usable_size();
    // The keys are 2 + first_key_length + 2 + 3 bytes long. With this first key
    // length, they end exactly at the chunk table of their first page.
    int64_t boundary_length = table_start - keys_pos - 7;
    // The next page has no chunk table.
    int64_t page_capacity = BTI_PAGE_SIZE - bti_page_footer_size;
    size_t n_cases = 0;
    for (int64_t boundary : {boundary_length, boundary_length + page_capacity}) {
        for (int64_t first_key_length = boundary - 40; first_key_length <= boundary + 4; ++first_key_length) {
            if (first_key_length < 0) {
                continue;
            }
            BOOST_TEST_CONTEXT("first_key_length=" << first_key_length << " boundary=" << boundary) {
                auto files = write(first_key_length);
                check_pages(as_bytes(files.partitions));
                auto [keys_pos1, count, root] = trailer_tail(as_bytes(files.partitions));
                BOOST_REQUIRE_EQUAL(keys_pos1, keys_pos);
                BOOST_REQUIRE_EQUAL(count, partitions.size());
                BOOST_REQUIRE_EQUAL(root, root0);
                // Reads the trailer directly from the file, and checks the pages it reads.
                auto footer = read_bti_partitions_db_footer(*s, sstable_version_types::mu, make_memory_file(files.partitions), files.partitions->size()).get();
                BOOST_REQUIRE(bytes_view(footer.first_key) == bytes_view(filled(first_key_length, 5)));
                BOOST_REQUIRE(bytes_view(footer.last_key) == bytes_view(last_key));
                BOOST_REQUIRE_EQUAL(footer.partition_count, partitions.size());
                BOOST_REQUIRE_EQUAL(footer.trie_root_position, root0);
                // Reads the trailer through cached_file, and the trie.
                opened_index idx(s, sstable_version_types::mu, encoding, files);
                BOOST_REQUIRE(bytes_view(idx.footer().first_key) == bytes_view(filled(first_key_length, 5)));
                check_reads(idx, *s, partitions, partitions.front().start, end_position(partitions, encoding));
                ++n_cases;
            }
        }
    }
    BOOST_REQUIRE_GE(n_cases, 45);
}

// A trie node can end exactly at the chunk table of a page, without the page
// being ended yet. Then the raw bytes after it (here: the trailer keys) must
// start on the next page, and their recorded position must point there.
//
// This searches for partition sets whose Partitions.db trie ends exactly at
// the chunk table of a page. The payload sizes of the last few partitions
// grow with the width of their chunk position, which tunes the trie size.
SEASTAR_THREAD_TEST_CASE(test_mu_partitions_db_trie_ends_at_chunk_table) {
    auto s = make_schema();
    auto encoding = physical_encoding(4096);
    // Chunk positions with inline payloads of 5, 6, 7, 8 and 9 bytes.
    const std::vector<unsigned> widths = {21, 29, 37, 45, 53};
    for (bool with_slots : {false, true}) {
        bool found = false;
        for (size_t n = 200; n < 1000 && !found; ++n)
        for (size_t a = 0; a < widths.size() && !found; ++a)
        for (size_t b = a; b < widths.size() && !found; ++b)
        for (size_t c = b; c < widths.size() && !found; ++c) {
            std::vector<test_partition> partitions;
            for (size_t i = 0; i < n; ++i) {
                uint64_t chunk;
                uint64_t offset = 0;
                if (i + 3 >= n) {
                    // The tunable partitions, each in its own chunk.
                    auto w = widths[std::array{a, b, c}[i + 3 - n]];
                    chunk = (uint64_t(1) << (w - 1)) + i;
                } else if (with_slots) {
                    // Runs of 6 keys in one chunk, at chunk positions where
                    // slots pay off.
                    chunk = (uint64_t(1) << 30) + (i / 6 + 1) * 4096;
                    offset = (i % 6) * 100;
                } else {
                    chunk = (i + 1) * 4096;
                }
                partitions.push_back(test_partition{
                    .token = int64_t(i * 1000),
                    .key = filled(1, i),
                    .start = sstable_position::from_physical(chunk, 3000, offset),
                    .end = sstable_position::from_physical(chunk, 3000, offset + 10),
                });
            }
            auto files = write_index(*s, sstable_version_types::mu, encoding, partitions);
            auto [keys_pos, count, root] = trailer_tail(as_bytes(files.partitions));
            // The trie root is never written at a page start, so the keys start
            // at a page start only if the root ended exactly at a chunk table.
            if (keys_pos % BTI_PAGE_SIZE != 0) {
                continue;
            }
            found = true;
            testlog.info("with_slots={}: the trie ends at the chunk table with n={}, widths={},{},{}, keys at {}",
                with_slots, n, widths[a], widths[b], widths[c], keys_pos);
            auto prev_page = keys_pos - BTI_PAGE_SIZE;
            auto view = bti_page_view(as_bytes(files.partitions).subspan(prev_page, BTI_PAGE_SIZE), prev_page);
            BOOST_REQUIRE_EQUAL(root, prev_page + view.usable_size() - 2);
            BOOST_REQUIRE_EQUAL(with_slots, view.table_size() > 0);
            opened_index idx(s, sstable_version_types::mu, encoding, files);
            check_partition_payloads(files, idx.footer(), partitions, encoding.layout());
            check_reads(idx, *s, partitions, partitions.front().start, end_position(partitions, encoding));
        }
        BOOST_REQUIRE(found);
    }
}

// Like test_mu_partitions_db_trie_ends_at_chunk_table, but for a row index trie
// in Rows.db, followed by its row index header.
SEASTAR_THREAD_TEST_CASE(test_mu_rows_db_trie_ends_at_chunk_table) {
    auto s = make_schema();
    auto encoding = physical_encoding(4096);
    // Relative chunk positions with row payloads of 4, 5, 6, 7 and 8 bytes.
    const std::vector<unsigned> widths = {15, 23, 31, 39, 47};
    bool found = false;
    for (size_t n = 200; n < 1000 && !found; ++n)
    for (size_t a = 0; a < widths.size() && !found; ++a)
    for (size_t b = a; b < widths.size() && !found; ++b) {
        test_partition p{
            .token = 1,
            .key = filled(5, 1),
            .start = sstable_position::from_physical(0, 3000, 0),
            .end = sstable_position::from_physical((uint64_t(1) << (widths[b] - 1)) + 2 * n * 4096, 3000, 50),
        };
        for (size_t i = 0; i < n; ++i) {
            uint64_t chunk = i + 1 == n ? (uint64_t(1) << (widths[a] - 1)) + n * 4096 : i * 4096;
            p.blocks.push_back(test_block{
                .first_ck = to_bytes_of({int8_t(i >> 8), int8_t(i), 0}),
                .last_ck = to_bytes_of({int8_t(i >> 8), int8_t(i), 1}),
                .start = sstable_position::from_physical(chunk, 3000, i == 0 ? 0 : 100),
                .range_tombstone = sstables::deletion_time::make_live(),
            });
        }
        std::vector<test_partition> partitions = {std::move(p)};
        auto files = write_index(*s, sstable_version_types::mu, encoding, partitions);
        auto header_pos = *files.header_positions[0];
        // The row index header starts at a page start only if the trie ended
        // exactly at the chunk table.
        if (header_pos % BTI_PAGE_SIZE != 0) {
            continue;
        }
        found = true;
        testlog.info("The row index trie ends at the chunk table with n={}, widths={},{}, header at {}", n, widths[a], widths[b], header_pos);
        check_pages(as_bytes(files.rows));
        opened_index idx(s, sstable_version_types::mu, encoding, files);
        check_reads(idx, *s, partitions, partitions.front().start, end_position(partitions, encoding));
    }
    BOOST_REQUIRE(found);
}

namespace {

// Recomputes the CRC32 of the page at `page_pos`, after a test changed it.
void fix_page_crc(std::vector<char>& file, uint64_t page_pos) {
    auto page = std::as_bytes(std::span(file).subspan(page_pos, BTI_PAGE_SIZE));
    seastar::write_be<uint32_t>(file.data() + page_pos + BTI_PAGE_SIZE - 4, compute_bti_page_crc(page));
}

index_files copy_files(const index_files& f) {
    return index_files{
        .partitions = make_lw_shared<std::vector<char>>(*f.partitions),
        .rows = make_lw_shared<std::vector<char>>(*f.rows),
        .header_positions = f.header_positions,
    };
}

} // anonymous namespace

// A corrupt byte in any page of either index file is detected,
// on every path that reads the page.
SEASTAR_THREAD_TEST_CASE(test_mu_index_page_corruption_is_detected) {
    auto s = make_schema();
    auto encoding = physical_encoding(4096);
    auto seed = tests::random::get_int<uint64_t>();
    testlog.info("seed={}", seed);
    std::mt19937_64 rng(seed);
    auto model = data_model(4096, 0, rng);
    auto partitions = generate_partitions(rng, 300, [&] (uint64_t u) { return model.at(u); });
    // A long first key, so that the trailer spans pages.
    auto trailer_keys = std::pair(filled(6000, 1), filled(5, 2));
    auto files = write_index(*s, sstable_version_types::mu, encoding, partitions, trailer_keys);
    auto start = partitions.front().start;
    auto end = end_position(partitions, encoding);
    {
        opened_index idx(s, sstable_version_types::mu, encoding, files);
        check_reads(idx, *s, partitions, start, end);
    }
    auto last_page = files.partitions->size() - BTI_PAGE_SIZE;
    auto [keys_pos, count, root] = trailer_tail(as_bytes(files.partitions));
    auto keys_page = round_down(keys_pos, BTI_PAGE_SIZE);
    BOOST_REQUIRE_LT(keys_page, last_page);

    for (bool corrupt_partitions_db : {true, false}) {
        auto size = (corrupt_partitions_db ? files.partitions : files.rows)->size();
        for (uint64_t page_pos = 0; page_pos < size; page_pos += BTI_PAGE_SIZE) {
            // Either a random byte of the page, or its slot count.
            for (bool in_footer : {false, true}) {
                BOOST_TEST_CONTEXT("Partitions.db=" << corrupt_partitions_db << " page=" << page_pos << " in_footer=" << in_footer) {
                    auto corrupted = copy_files(files);
                    auto& file = *(corrupt_partitions_db ? corrupted.partitions : corrupted.rows);
                    auto offset = in_footer ? BTI_PAGE_SIZE - bti_page_footer_size + rng() % 2 : rng() % (BTI_PAGE_SIZE - bti_page_footer_size);
                    file[page_pos + offset] ^= char(1 + rng() % 255);
                    bool trailer_page = corrupt_partitions_db && (page_pos == last_page || page_pos >= keys_page);
                    if (trailer_page) {
                        // Read without cached_file.
                        BOOST_REQUIRE_THROW(
                            read_bti_partitions_db_footer(*s, sstable_version_types::mu, make_memory_file(corrupted.partitions), corrupted.partitions->size()).get(),
                            sstables::malformed_sstable_exception);
                        // Read through cached_file.
                        BOOST_REQUIRE_THROW(opened_index(s, sstable_version_types::mu, encoding, corrupted), sstables::malformed_sstable_exception);
                    } else {
                        // Every other page holds trie nodes or row index headers,
                        // which the reads use.
                        opened_index idx(s, sstable_version_types::mu, encoding, corrupted);
                        BOOST_REQUIRE_THROW(check_reads(idx, *s, partitions, start, end), sstables::malformed_sstable_exception);
                    }
                }
            }
        }
    }
}

// A slot that the page's chunk table doesn't have is rejected, even if the
// page's CRC32 is right.
SEASTAR_THREAD_TEST_CASE(test_mu_slot_outside_chunk_table_is_rejected) {
    auto s = make_schema();
    auto encoding = physical_encoding(4096);
    // A chunk position at which two keys use slots.
    auto pos = [] (uint64_t c, uint64_t o) { return sstable_position::from_physical((uint64_t(1) << 30) + c, 3000, o); };
    std::vector<test_partition> partitions = {
        {.token = 1, .key = filled(1, 1), .start = pos(0, 0), .end = pos(0, 10)},
        {.token = 2, .key = filled(1, 2), .start = pos(0, 11), .end = pos(0, 20)},
    };
    auto files = write_index(*s, sstable_version_types::mu, encoding, partitions);
    opened_index idx0(s, sstable_version_types::mu, encoding, files);
    auto payloads = trie_payloads(as_bytes(files.partitions), idx0.footer().trie_root_position);
    BOOST_REQUIRE(decode_partition_payload(payloads[1]).form == payload_form::slot);
    auto table_size = bti_page_view(as_bytes(files.partitions).subspan(0, BTI_PAGE_SIZE), 0).table_size();
    BOOST_REQUIRE_EQUAL(table_size, 1);

    auto corrupted = copy_files(files);
    // Set the slot of partition 2 to 1. The low 9 bits of the payload hold it.
    auto slot_offset = payloads[1].bytes.data() - as_bytes(files.partitions).data() + payloads[1].bits - 2;
    auto low = seastar::read_be<uint16_t>(corrupted.partitions->data() + slot_offset);
    seastar::write_be<uint16_t>(corrupted.partitions->data() + slot_offset, (low & ~0x1ff) | table_size);
    fix_page_crc(*corrupted.partitions, 0);
    opened_index idx(s, sstable_version_types::mu, encoding, corrupted);
    tests::reader_concurrency_semaphore_wrapper semaphore;
    auto r = idx.make_reader(semaphore.make_permit(), partitions.front().start, end_position(partitions, encoding));
    auto close_r = deferred_close(*r);
    auto dk = partitions[1].dk(*s);
    // The lookup decodes the payload, to check whether it points into Rows.db.
    BOOST_REQUIRE_EXCEPTION(
        r->advance_lower_and_check_if_present(dht::ring_position_view(dk), utils::make_hashed_key(bytes_view(partitions[1].sst_key(*s)))).get(),
        sstables::malformed_sstable_exception,
        exception_predicate::message_contains("chunk table slot 1, but the page has 1 slots"));
}

// An index writer whose file isn't finished, e.g. because the write failed,
// can be destroyed after abandon(). Without abandon(), destroying it is an
// internal error, because the unended page is missing from the file.
// (That would abort this test).
SEASTAR_THREAD_TEST_CASE(test_mu_abandoned_index_writers) {
    auto s = make_schema();
    auto encoding = physical_encoding(4096);
    std::mt19937_64 rng(tests::random::get_int<uint64_t>());
    auto model = data_model(4096, 0, rng);
    auto partitions = generate_partitions(rng, 3000, [&] (uint64_t u) { return model.at(u); });
    memory_data_sink_buffers partitions_bufs;
    memory_data_sink_buffers rows_bufs;
    sstables::file_writer partitions_db_writer(data_sink(std::make_unique<memory_data_sink>(partitions_bufs)));
    sstables::file_writer rows_db_writer(data_sink(std::make_unique<memory_data_sink>(rows_bufs)));
    auto close_partitions_db = defer([&] noexcept { partitions_db_writer.close(); });
    auto close_rows_db = defer([&] noexcept { rows_db_writer.close(); });
    auto partition_index_writer = bti_partition_index_writer(sstable_version_types::mu, partitions_db_writer, encoding);
    auto row_index_writer = bti_row_index_writer(sstable_version_types::mu, rows_db_writer, encoding);
    for (const auto& p : partitions) {
        for (const auto& b : p.blocks) {
            row_index_writer.add(*s, make_ck(*s, b.first_ck), make_ck(*s, b.last_ck), p.start, b.start, b.range_tombstone);
        }
        auto key = p.sst_key(*s);
        auto target = row_index_writer.finish(*s, p.start, p.end, key, p.tombstone);
        partition_index_writer.add(*s, p.dk(*s), utils::make_hashed_key(bytes_view(key)), target);
    }
    partition_index_writer.abandon();
    row_index_writer.abandon();
}

// cached_file checks each page that it reads, also pages read ahead,
// and caches none of them if one is corrupt.
SEASTAR_THREAD_TEST_CASE(test_cached_file_page_verifier) {
    auto data = make_lw_shared<std::vector<char>>(3 * cached_file::page_size, 'x');
    std::vector<uint64_t> verified;
    bool fail_page_2 = true;
    cached_file_stats stats;
    lru l;
    logalloc::region region;
    {
        cached_file cf(make_memory_file(data), stats, l, region, data->size(), "test", [&] (uint64_t pos, std::span<const char> page) {
            BOOST_REQUIRE_EQUAL(page.size(), cached_file::page_size);
            verified.push_back(pos);
            if (fail_page_2 && pos == 2 * cached_file::page_size) {
                throw sstables::malformed_sstable_exception("bad page");
            }
        });
        tests::reader_concurrency_semaphore_wrapper semaphore;
        // Reads all 3 pages at once.
        auto s1 = cf.read(0, semaphore.make_permit(), {}, 3 * cached_file::page_size);
        BOOST_REQUIRE_THROW(s1.next().get(), sstables::malformed_sstable_exception);
        BOOST_REQUIRE_EQUAL(cf.cached_bytes(), 0);
        BOOST_REQUIRE(verified == std::vector<uint64_t>({0, cached_file::page_size, 2 * cached_file::page_size}));
        fail_page_2 = false;
        verified.clear();
        auto s2 = cf.read(0, semaphore.make_permit(), {}, 3 * cached_file::page_size);
        BOOST_REQUIRE_EQUAL(s2.next().get().size(), cached_file::page_size);
        BOOST_REQUIRE_EQUAL(verified.size(), 3);
        // Cached pages aren't checked again.
        auto s3 = cf.read(0, semaphore.make_permit(), {}, 3 * cached_file::page_size);
        BOOST_REQUIRE_EQUAL(s3.next().get().size(), cached_file::page_size);
        BOOST_REQUIRE_EQUAL(verified.size(), 3);
    }
}
