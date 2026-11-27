/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Tests of the writer of sstables with a Parquet Data file (version `aa`,
// sstables/parquet/sstable_writer.hh), without the Parquet sstable reader.
//
// Each test writes sstables through sstable::write_components() or
// sstable_writer, and checks the written components:
//
// - The component set, and Digest.crc32 against the CRC-32 of the Data file.
// - The Data file, with Arrow C++ as the oracle: the rows, their kinds, the
//   partition starts, and the raw size computed from the values.
// - Scylla.db: the Parquet entry, against the bytes of the Data file.
// - Statistics.db and the statistics in Scylla.db, against an `mt` sstable
//   written from the same fragments.
// - The trie index, with the index reader, against a model of the rows
//   built from the fragments and the page set boundaries that Arrow reads
//   from the OffsetIndex.

#include <filesystem>
#include <fstream>
#include <set>
#include <string>
#include <vector>

#include <seastar/core/byteorder.hh>
#include <seastar/core/memory.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/closeable.hh>
#include <zlib.h>
#include <fmt/ranges.h>

// Arrow's headers use std::is_trivial, which C++26 deprecates.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#include <arrow/buffer.h>
#include <arrow/io/memory.h>
#include <parquet/column_reader.h>
#include <parquet/file_reader.h>
#include <parquet/metadata.h>
#include <parquet/page_index.h>
#include <parquet/schema.h>
#pragma GCC diagnostic pop

#include "db/config.hh"
#include "db/large_data_handler.hh"
#include "mutation/mutation_fragment_v2.hh"
#include "readers/from_mutations.hh"
#include "schema/schema_builder.hh"
#include "sstables/compressor.hh"
#include "sstables/parquet/sstable_writer.hh"
#include "sstables/parquet/writer.hh"
#include "sstables/sstable_writer.hh"
#include "sstables/sstables.hh"
#include "sstables/sstables_manager.hh"
#include "sstables/trie/bti_index.hh"
#include "test/lib/log.hh"
#include "test/lib/mutation_source_test.hh"
#include "test/lib/reader_concurrency_semaphore.hh"
#include "test/lib/simple_schema.hh"
#include "test/lib/sstable_test_env.hh"
#include "test/lib/sstable_utils.hh"
#include "utils/cached_file.hh"

using namespace sstables;
namespace pq = sstables::parquet;

namespace {

constexpr auto aa = sstable_version_types::aa;
constexpr auto mt = sstable_version_types::mt;

using tweak_config = std::function<void (sstable_writer_config&)>;

// Small page sets, so that partitions span several of them. A minimum
// page size of 1 byte makes every column take every page set cut, so the
// pages of `_scylla.kind` show the page set boundaries.
tweak_config small_page_sets(uint64_t page_set_size) {
    return [page_set_size] (sstable_writer_config& cfg) {
        cfg.parquet_page_set_size = page_set_size;
        cfg.parquet_min_page_size = 1;
        cfg.parquet_row_group_size = 64 << 10;
    };
}

utils::chunked_vector<mutation> sorted(utils::chunked_vector<mutation> muts) {
    std::ranges::sort(muts, mutation_decorated_key_less_comparator());
    return muts;
}

shared_sstable write(test_env& env, schema_ptr s, const utils::chunked_vector<mutation>& muts, sstable_version_types v, tweak_config tweak = {}) {
    auto sst = env.make_sstable(s, v);
    auto cfg = env.manager().configure_writer("test");
    if (tweak) {
        tweak(cfg);
    }
    sst->write_components(make_mutation_reader_from_mutations(s, env.make_reader_permit(), muts), muts.size(), s, cfg, encoding_stats{}).get();
    return sst;
}

std::string read_file(const sstring& path) {
    std::ifstream in(path, std::ios::binary);
    BOOST_REQUIRE_MESSAGE(in, path);
    return std::string(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
}

uint32_t crc32_of(std::string_view data) {
    return ::crc32(0, reinterpret_cast<const Bytef*>(data.data()), data.size());
}

// Statistics.db and Scylla.db, read from disk into a fresh sstable object.
struct metadata {
    shared_sstable sst;
    sstables::statistics& stats;
    scylla_metadata& scylla;

    const stats_metadata& stats_md() const {
        return static_cast<const stats_metadata&>(*stats.contents.at(metadata_type::Stats));
    }
    const scylla_metadata::parquet_data_file* parquet() const {
        return scylla.get_parquet_data_file();
    }
};

metadata read_metadata(test_env& env, schema_ptr s, const shared_sstable& written) {
    auto sst = env.make_sstable(s, written->generation(), written->get_version());
    sstables::test t(sst);
    t.read_toc().get();
    t.read_scylla_metadata().get();
    t.read_statistics().get();
    return metadata{sst, t._statistics(), *t._scylla_metadata()};
}

// A row of the Data file, as the fragments determine it.
struct model_row {
    // The value of `_scylla.kind`, from the fragment, without the marker bit.
    enum class kind { none, static_row, clustering_row, bound };
    kind k;
    // Engaged for clustering rows and bounds.
    std::optional<position_in_partition> pos;
    // The range tombstone open before the row.
    tombstone open_before;
};

struct model_partition {
    dht::decorated_key dk;
    // The ordinals of the first row and one past the last row.
    uint64_t first;
    uint64_t end;
};

struct model {
    std::vector<model_row> rows;
    std::vector<model_partition> partitions;
};

model build_model(test_env& env, schema_ptr s, const utils::chunked_vector<mutation>& muts) {
    model m;
    auto rd = make_mutation_reader_from_mutations(s, env.make_reader_permit(), muts);
    auto close = deferred_close(rd);
    tombstone open;
    while (auto mf = rd().get()) {
        if (mf->is_partition_start()) {
            m.partitions.push_back({mf->as_partition_start().key(), m.rows.size(), 0});
            open = {};
        } else if (mf->is_static_row()) {
            if (!mf->as_static_row().cells().empty()) {
                m.rows.push_back({model_row::kind::static_row, std::nullopt, {}});
            }
        } else if (mf->is_clustering_row()) {
            m.rows.push_back({model_row::kind::clustering_row, position_in_partition(mf->position()), open});
        } else if (mf->is_range_tombstone_change()) {
            const auto& rtc = mf->as_range_tombstone_change();
            if (open) {
                m.rows.push_back({model_row::kind::bound, position_in_partition(rtc.position()), open});
                open = {};
            }
            if (rtc.tombstone()) {
                m.rows.push_back({model_row::kind::bound, position_in_partition(rtc.position()), open});
                open = rtc.tombstone();
            }
        } else if (mf->is_end_of_partition()) {
            auto& p = m.partitions.back();
            if (m.rows.size() == p.first) {
                m.rows.push_back({model_row::kind::none, std::nullopt, {}});
            }
            p.end = m.rows.size();
        }
    }
    return m;
}

// The Data file as Arrow reads it.
class arrow_file {
    std::string _data;
    std::unique_ptr<::parquet::ParquetFileReader> _reader;

    // Calls `f(def, value_size)` for each entry of leaf `col`, where
    // `value_size` is the plain size of a value, or 0 for a null.
    template <typename DType, typename F>
    void scan(int col, F&& f) {
        const auto meta = _reader->metadata();
        const auto& d = *meta->schema()->Column(col);
        for (int rg = 0; rg < meta->num_row_groups(); ++rg) {
            auto base = _reader->RowGroup(rg)->Column(col);
            auto& r = static_cast<::parquet::TypedColumnReader<DType>&>(*base);
            constexpr int64_t batch = 1024;
            std::vector<int16_t> def(batch), rep(batch);
            auto vals = std::make_unique<typename DType::c_type[]>(batch);
            while (r.HasNext()) {
                int64_t n_values = 0;
                const int64_t n = r.ReadBatch(batch, def.data(), rep.data(), vals.get(), &n_values);
                int64_t vi = 0;
                for (int64_t i = 0; i < n; ++i) {
                    const int16_t dl = d.max_definition_level() ? def[i] : 0;
                    uint64_t size = 0;
                    if (dl == d.max_definition_level()) {
                        const auto& v = vals[vi++];
                        if constexpr (std::is_same_v<DType, ::parquet::ByteArrayType>) {
                            size = v.len;
                        } else if constexpr (std::is_same_v<DType, ::parquet::FLBAType>) {
                            size = d.type_length();
                        } else if constexpr (std::is_same_v<DType, ::parquet::BooleanType>) {
                            size = 1;
                        } else {
                            size = sizeof(v);
                        }
                    }
                    f(dl, size, vi > 0 && dl == d.max_definition_level() ? &vals[vi - 1] : nullptr);
                }
                BOOST_REQUIRE_EQUAL(vi, n_values);
            }
        }
    }
public:
    explicit arrow_file(std::string data) : _data(std::move(data)) {
        ::parquet::ReaderProperties props;
        props.set_page_checksum_verification(true);
        _reader = ::parquet::ParquetFileReader::Open(std::make_shared<::arrow::io::BufferReader>(::arrow::Buffer::FromString(_data)), props);
    }

    const ::parquet::FileMetaData& metadata() const { return *_reader->metadata(); }
    int64_t num_rows() const { return _reader->metadata()->num_rows(); }

    int column(const std::string& path) const {
        const auto meta = _reader->metadata();
        for (int i = 0; i < meta->num_columns(); ++i) {
            if (meta->schema()->Column(i)->path()->ToDotString() == path) {
                return i;
            }
        }
        BOOST_FAIL("no column " + path);
        return -1;
    }

    // The sum of the plain sizes of all values in the file.
    uint64_t raw_size() {
        uint64_t total = 0;
        auto add = [&] (int16_t, uint64_t size, const void*) { total += size; };
        const auto meta = _reader->metadata();
        for (int c = 0; c < meta->num_columns(); ++c) {
            switch (meta->schema()->Column(c)->physical_type()) {
            case ::parquet::Type::BOOLEAN: scan<::parquet::BooleanType>(c, add); break;
            case ::parquet::Type::INT32: scan<::parquet::Int32Type>(c, add); break;
            case ::parquet::Type::INT64: scan<::parquet::Int64Type>(c, add); break;
            case ::parquet::Type::FLOAT: scan<::parquet::FloatType>(c, add); break;
            case ::parquet::Type::DOUBLE: scan<::parquet::DoubleType>(c, add); break;
            case ::parquet::Type::BYTE_ARRAY: scan<::parquet::ByteArrayType>(c, add); break;
            case ::parquet::Type::FIXED_LEN_BYTE_ARRAY: scan<::parquet::FLBAType>(c, add); break;
            default: BOOST_FAIL("unexpected type");
            }
        }
        return total;
    }

    // The ordinals of the rows that start a partition: those where the
    // first leaf, the first partition key column, is not null.
    std::vector<uint64_t> partition_starts() {
        std::vector<uint64_t> r;
        uint64_t row = 0;
        auto on_entry = [&] (int16_t def, uint64_t, const void*) {
            if (def > 0) {
                r.push_back(row);
            }
            ++row;
        };
        switch (_reader->metadata()->schema()->Column(0)->physical_type()) {
        case ::parquet::Type::INT32: scan<::parquet::Int32Type>(0, on_entry); break;
        case ::parquet::Type::INT64: scan<::parquet::Int64Type>(0, on_entry); break;
        case ::parquet::Type::BYTE_ARRAY: scan<::parquet::ByteArrayType>(0, on_entry); break;
        default: BOOST_FAIL("unexpected partition key type");
        }
        return r;
    }

    std::vector<int32_t> kinds() {
        std::vector<int32_t> r;
        scan<::parquet::Int32Type>(column("_scylla.kind"), [&] (int16_t def, uint64_t, const void* v) {
            BOOST_REQUIRE(v);
            r.push_back(*static_cast<const int32_t*>(v));
        });
        return r;
    }

    // The first row of each page of `_scylla.kind`, and of each row group.
    std::set<uint64_t> page_starts() {
        std::set<uint64_t> r;
        const int col = column("_scylla.kind");
        auto page_index = _reader->GetPageIndexReader();
        BOOST_REQUIRE(page_index);
        uint64_t base = 0;
        for (int rg = 0; rg < metadata().num_row_groups(); ++rg) {
            auto oi = page_index->RowGroup(rg)->GetOffsetIndex(col);
            BOOST_REQUIRE(oi);
            for (const auto& loc : oi->page_locations()) {
                r.insert(base + loc.first_row_index);
            }
            base += metadata().RowGroup(rg)->num_rows();
        }
        return r;
    }

    // Reads every column, which verifies every page checksum.
    void read_all() {
        raw_size();
    }
};

// Checks the components on disk, Digest.crc32, and Scylla.db against the
// Data file. Returns the Data file.
std::string check_components(test_env& env, schema_ptr s, const shared_sstable& sst) {
    // The files of the sstable on disk. There is no CRC.db or CompressionInfo.db.
    const std::filesystem::path data_path(sst->get_filename(component_type::Data).format());
    const auto prefix = data_path.filename().native().substr(0, data_path.filename().native().size() - std::string_view("Data.db").size());
    std::set<std::string> files;
    for (const auto& e : std::filesystem::directory_iterator(data_path.parent_path())) {
        if (e.path().filename().native().starts_with(prefix)) {
            files.insert(e.path().filename().native().substr(prefix.size()));
        }
    }
    std::set<std::string> expected_files{"TOC.txt", "Statistics.db", "Digest.crc32", "Partitions.db", "Rows.db", "Data.db", "Scylla.db"};
    if (s->bloom_filter_fp_chance() != 1.0) {
        expected_files.insert("Filter.db");
    }
    BOOST_REQUIRE_MESSAGE(files == expected_files, fmt::format("files: {}", files));

    std::set<component_type> expected{component_type::TOC, component_type::Statistics, component_type::Digest,
            component_type::Partitions, component_type::Rows, component_type::Data, component_type::Scylla};
    if (s->bloom_filter_fp_chance() != 1.0) {
        expected.insert(component_type::Filter);
    }
    auto md = read_metadata(env, s, sst);
    std::set<component_type> toc;
    for (const auto& c : sstables::test(md.sst).get_components()) {
        toc.insert(c);
    }
    BOOST_REQUIRE(toc == expected);

    const auto data = read_file(sst->get_filename(component_type::Data).format());
    const auto crc = crc32_of(data);
    BOOST_REQUIRE_EQUAL(read_file(sst->get_filename(component_type::Digest).format()), fmt::to_string(crc));
    BOOST_REQUIRE_EQUAL(md.scylla.get_components_digests()->map.at(component_type::Data), crc);

    const auto* p = md.parquet();
    BOOST_REQUIRE(p);
    BOOST_REQUIRE_EQUAL(p->file_size, data.size());
    BOOST_REQUIRE_GE(data.size(), p->footer_size + 12);
    BOOST_REQUIRE_EQUAL(data.substr(data.size() - 4), "PAR1");
    BOOST_REQUIRE_EQUAL(read_le<uint32_t>(data.data() + data.size() - 8), p->footer_size);
    BOOST_REQUIRE_EQUAL(crc32_of(std::string_view(data).substr(data.size() - 8 - p->footer_size, p->footer_size)), p->footer_crc32);

    arrow_file f(data);
    BOOST_REQUIRE_EQUAL(uint64_t(f.num_rows()), p->rows);
    BOOST_REQUIRE_EQUAL(f.raw_size(), p->raw_size);
    return data;
}

// Opens the trie index of a written sstable, with the total row count in
// place of the Data file size.
class index_files {
    schema_ptr _s;
    file _partitions;
    file _rows;
    cached_file_stats _stats;
    lru _lru;
    logalloc::region _region;
    seastar::shared_ptr<cached_file> _partitions_cached;
    seastar::shared_ptr<cached_file> _rows_cached;
    uint64_t _root;
    uint64_t _total_rows;
    tests::reader_concurrency_semaphore_wrapper _semaphore;
public:
    index_files(schema_ptr s, const shared_sstable& sst, uint64_t total_rows)
        : _s(s)
        , _partitions(open_file_dma(sst->get_filename(component_type::Partitions).format(), open_flags::ro).get())
        , _rows(open_file_dma(sst->get_filename(component_type::Rows).format(), open_flags::ro).get())
        , _total_rows(total_rows)
    {
        const auto partitions_size = _partitions.size().get();
        _partitions_cached = seastar::make_shared<cached_file>(_partitions, _stats, _lru, _region, partitions_size, "Partitions.db");
        _rows_cached = seastar::make_shared<cached_file>(_rows, _stats, _lru, _region, _rows.size().get(), "Rows.db");
        _root = trie::read_bti_partitions_db_footer(*s, aa, _partitions, partitions_size).get().trie_root_position;
    }
    ~index_files() {
        _partitions_cached = nullptr;
        _rows_cached = nullptr;
        _partitions.close().get();
        _rows.close().get();
    }
    std::unique_ptr<abstract_index_reader> make_reader() {
        return trie::make_bti_index_reader(_partitions_cached, _rows_cached, _root, _total_rows, aa, _s, _semaphore.make_permit(), tracing::trace_state_ptr());
    }
};

int32_t kind_value(const model_row& r, int32_t actual) {
    switch (r.k) {
    case model_row::kind::none: return 0;
    case model_row::kind::static_row: return 1;
    // With or without a marker.
    case model_row::kind::clustering_row: return actual == 3 ? 3 : 2;
    // Any bound kind.
    case model_row::kind::bound: return actual >= 4 && actual <= 7 ? actual : 4;
    }
    std::unreachable();
}

// Checks the rows of the Data file and the trie index against the model.
// If `page_sets_known`, the pages of `_scylla.kind` start exactly at the
// page set boundaries (see small_page_sets()), and the row index blocks are
// checked exactly. Otherwise only the partition positions are.
void check_rows_and_index(schema_ptr s, const shared_sstable& sst, const std::string& data, const model& m, bool page_sets_known) {
    arrow_file f(data);
    BOOST_REQUIRE_EQUAL(uint64_t(f.num_rows()), m.rows.size());
    const auto kinds = f.kinds();
    BOOST_REQUIRE_EQUAL(kinds.size(), m.rows.size());
    for (size_t i = 0; i < kinds.size(); ++i) {
        BOOST_REQUIRE_EQUAL(kinds[i], kind_value(m.rows[i], kinds[i]));
    }
    std::vector<uint64_t> expected_starts;
    for (const auto& p : m.partitions) {
        expected_starts.push_back(p.first);
    }
    BOOST_REQUIRE(f.partition_starts() == expected_starts);

    const auto page_starts = f.page_starts();
    position_in_partition::equal_compare eq(*s);
    index_files index(s, sst, m.rows.size());
    uint64_t partitions_with_index = 0;
    for (size_t pi = 0; pi < m.partitions.size(); ++pi) {
        const auto& p = m.partitions[pi];
        // The expected blocks: the first clustering entry, and every page set
        // boundary after it in the partition that does not split two rows at
        // the same position.
        std::vector<uint64_t> blocks;
        for (uint64_t r = p.first; r < p.end; ++r) {
            if (m.rows[r].pos) {
                blocks.push_back(r);
                break;
            }
        }
        if (!blocks.empty()) {
            for (auto it = page_starts.upper_bound(blocks.front()); it != page_starts.end() && *it < p.end; ++it) {
                if (!eq(*m.rows[*it - 1].pos, *m.rows[*it].pos)) {
                    blocks.push_back(*it);
                }
            }
        }
        const bool has_index = blocks.size() >= 2;
        partitions_with_index += has_index;
        if (!page_sets_known) {
            auto ir = index.make_reader();
            auto close_ir = deferred_close(*ir);
            BOOST_REQUIRE(ir->advance_lower_and_check_if_present(dht::ring_position_view(p.dk)).get());
            BOOST_REQUIRE_EQUAL(ir->data_file_positions().start, p.first);
            ir->advance_upper_past(position_in_partition_view::after_all_clustered_rows()).get();
            BOOST_REQUIRE_EQUAL(ir->data_file_positions().end.value(), p.end);
            continue;
        }

        auto ir = index.make_reader();
        auto close_ir = deferred_close(*ir);
        BOOST_REQUIRE(ir->advance_lower_and_check_if_present(dht::ring_position_view(p.dk)).get());
        BOOST_REQUIRE_EQUAL(ir->data_file_positions().start, p.first);
        ir->read_partition_data().get();
        const auto last_block = ir->last_block_offset().get();
        if (has_index) {
            BOOST_REQUIRE(last_block);
            BOOST_REQUIRE_EQUAL(*last_block, blocks.back() - p.first);
            BOOST_REQUIRE(ir->get_partition_key()->equal(*s, p.dk.key()));
        } else {
            BOOST_REQUIRE(!last_block);
        }
        for (uint64_t r = p.first; r < p.end; ++r) {
            const auto& row = m.rows[r];
            if (!row.pos) {
                continue;
            }
            ir->advance_to(*row.pos).get();
            uint64_t expected = p.first;
            tombstone expected_tomb;
            if (has_index) {
                expected = *std::prev(std::ranges::upper_bound(blocks, r));
                expected_tomb = m.rows[expected].open_before;
            }
            BOOST_REQUIRE_EQUAL(ir->data_file_positions().start, expected);
            const auto marker = ir->end_open_marker();
            const auto tomb = marker ? marker->tomb : tombstone();
            BOOST_REQUIRE_MESSAGE(tomb == expected_tomb, fmt::format("row {}: open tombstone {}, expected {}", r, tomb, expected_tomb));
        }
        ir->advance_upper_past(position_in_partition_view::after_all_clustered_rows()).get();
        BOOST_REQUIRE_EQUAL(ir->data_file_positions().end.value(), p.end);
    }
    testlog.info("{} partitions, {} rows, {} page sets, {} partitions with a row index",
            m.partitions.size(), m.rows.size(), page_starts.size(), partitions_with_index);
}

// Checks that the statistics of `aa` equal those of `mt` for the same data,
// except the partition sizes and the compression ratio.
void check_statistics_like_mt(const metadata& got, const metadata& ref) {
    const auto& a = got.stats_md();
    const auto& b = ref.stats_md();
    BOOST_REQUIRE(a.estimated_cells_count.buckets == b.estimated_cells_count.buckets);
    BOOST_REQUIRE(a.estimated_cells_count.bucket_offsets == b.estimated_cells_count.bucket_offsets);
    BOOST_REQUIRE_EQUAL(a.estimated_partition_size.count(), b.estimated_partition_size.count());
    BOOST_REQUIRE(a.estimated_tombstone_drop_time.bin == b.estimated_tombstone_drop_time.bin);
    BOOST_REQUIRE(a.position == b.position);
    BOOST_REQUIRE_EQUAL(a.min_timestamp, b.min_timestamp);
    BOOST_REQUIRE_EQUAL(a.max_timestamp, b.max_timestamp);
    BOOST_REQUIRE_EQUAL(a.min_local_deletion_time, b.min_local_deletion_time);
    BOOST_REQUIRE_EQUAL(a.max_local_deletion_time, b.max_local_deletion_time);
    BOOST_REQUIRE_EQUAL(a.min_ttl, b.min_ttl);
    BOOST_REQUIRE_EQUAL(a.max_ttl, b.max_ttl);
    BOOST_REQUIRE_EQUAL(a.sstable_level, b.sstable_level);
    BOOST_REQUIRE_EQUAL(a.repaired_at, b.repaired_at);
    BOOST_REQUIRE_EQUAL(a.has_legacy_counter_shards, b.has_legacy_counter_shards);
    BOOST_REQUIRE_EQUAL(a.columns_count, b.columns_count);
    BOOST_REQUIRE_EQUAL(a.rows_count, b.rows_count);
    BOOST_REQUIRE(a.originating_host_id == b.originating_host_id);
    auto names = [] (const disk_array<uint32_t, disk_string<uint16_t>>& a) {
        std::vector<bytes> r;
        for (const auto& e : a.elements) {
            r.push_back(e.value);
        }
        return r;
    };
    BOOST_REQUIRE(names(a.min_column_names) == names(b.min_column_names));
    BOOST_REQUIRE(names(a.max_column_names) == names(b.max_column_names));

    const auto& va = static_cast<const validation_metadata&>(*got.stats.contents.at(metadata_type::Validation));
    const auto& vb = static_cast<const validation_metadata&>(*ref.stats.contents.at(metadata_type::Validation));
    BOOST_REQUIRE_EQUAL(va.partitioner.value, vb.partitioner.value);
    BOOST_REQUIRE_EQUAL(va.filter_chance, vb.filter_chance);
    const auto& ca = static_cast<const compaction_metadata&>(*got.stats.contents.at(metadata_type::Compaction));
    const auto& cb = static_cast<const compaction_metadata&>(*ref.stats.contents.at(metadata_type::Compaction));
    BOOST_REQUIRE(ca.cardinality.elements == cb.cardinality.elements);
    const auto& ha = static_cast<const serialization_header&>(*got.stats.contents.at(metadata_type::Serialization));
    const auto& hb = static_cast<const serialization_header&>(*ref.stats.contents.at(metadata_type::Serialization));
    BOOST_REQUIRE_EQUAL(ha.min_timestamp_base.value, hb.min_timestamp_base.value);
    BOOST_REQUIRE_EQUAL(ha.min_local_deletion_time_base.value, hb.min_local_deletion_time_base.value);
    BOOST_REQUIRE_EQUAL(ha.min_ttl_base.value, hb.min_ttl_base.value);
    BOOST_REQUIRE_EQUAL(ha.static_columns.elements.size(), hb.static_columns.elements.size());
    BOOST_REQUIRE_EQUAL(ha.regular_columns.elements.size(), hb.regular_columns.elements.size());

    BOOST_REQUIRE(got.scylla.get_ext_timestamp_stats()->map == ref.scylla.get_ext_timestamp_stats()->map);
    const auto* la = got.scylla.data.get<scylla_metadata_type::LargeDataStats, scylla_metadata::large_data_stats>();
    const auto* lb = ref.scylla.data.get<scylla_metadata_type::LargeDataStats, scylla_metadata::large_data_stats>();
    for (auto t : {large_data_type::rows_in_partition, large_data_type::elements_in_collection}) {
        BOOST_REQUIRE_EQUAL(la->map.at(t).max_value, lb->map.at(t).max_value);
        BOOST_REQUIRE_EQUAL(la->map.at(t).above_threshold, lb->map.at(t).above_threshold);
    }
    BOOST_REQUIRE(!ref.parquet());
}

// Writes `muts` as `aa` and as `mt`, and runs all the checks. `tweak` is
// small_page_sets() or nothing.
void check_all(test_env& env, schema_ptr s, utils::chunked_vector<mutation> muts, tweak_config tweak = {}) {
    muts = sorted(std::move(muts));
    const auto m = build_model(env, s, muts);
    auto sst = write(env, s, muts, aa, tweak);
    const auto data = check_components(env, s, sst);
    check_rows_and_index(s, sst, data, m, bool(tweak));
    auto ref = write(env, s, muts, mt);
    check_statistics_like_mt(read_metadata(env, s, sst), read_metadata(env, s, ref));
}

schema_ptr simple_table(compression_parameters cp = compression_parameters::no_compression()) {
    return schema_builder(this_smp_shard_count(), "ks", "t")
            .with_column("pk", int32_type, column_kind::partition_key)
            .with_column("ck", int32_type, column_kind::clustering_key)
            .with_column("v", utf8_type)
            .set_compressor_params(cp)
            .build();
}

// A value of `size` bytes, different for each partition and row.
sstring value_of(int32_t pk, int row, size_t size) {
    auto v = fmt::format("{}-{}-", pk, row);
    v.resize(size, 'x');
    return v;
}

mutation simple_partition(schema_ptr s, int32_t pk, int rows, size_t value_size = 10) {
    mutation m(s, partition_key::from_single_value(*s, int32_type->decompose(pk)));
    const auto& v = *s->get_column_definition("v");
    for (int i = 0; i < rows; ++i) {
        auto ck = clustering_key::from_single_value(*s, int32_type->decompose(i));
        m.set_clustered_cell(ck, v, atomic_cell::make_live(*utf8_type, 1000 + i, utf8_type->decompose(value_of(pk, i, value_size))));
    }
    return m;
}

clustering_key ck_of(const schema& s, int32_t v) {
    return clustering_key::from_single_value(s, int32_type->decompose(v));
}

} // anonymous namespace

SEASTAR_THREAD_TEST_CASE(test_simple_table) {
    test_env::do_with_async([] (test_env& env) {
        auto s = simple_table();
        utils::chunked_vector<mutation> muts;
        for (int pk = 0; pk < 20; ++pk) {
            muts.push_back(simple_partition(s, pk, pk));
        }
        check_all(env, s, muts, small_page_sets(512));
        // With the default sizes, the file has one page set.
        check_all(env, s, muts);
    }).get();
}

SEASTAR_THREAD_TEST_CASE(test_many_small_partitions) {
    test_env::do_with_async([] (test_env& env) {
        auto s = simple_table();
        utils::chunked_vector<mutation> muts;
        for (int pk = 0; pk < 2000; ++pk) {
            muts.push_back(simple_partition(s, pk, 1));
        }
        check_all(env, s, muts, small_page_sets(1024));
    }).get();
}

// Random schemas with static columns, collections, UDTs, row and range
// tombstones, TTLs, and counters.
SEASTAR_THREAD_TEST_CASE(test_random_mutations) {
    test_env::do_with_async([] (test_env& env) {
        for (uint32_t seed : {1, 2, 3}) {
            for (auto counters : {random_mutation_generator::generate_counters::no, random_mutation_generator::generate_counters::yes}) {
                random_mutation_generator gen(counters, local_shard_only::yes, random_mutation_generator::generate_uncompactable::no, seed);
                auto muts = gen(20);
                check_all(env, gen.schema(), muts, small_page_sets(256));
                check_all(env, gen.schema(), muts);
            }
        }
    }).get();
}

// Wide partitions with adjacent range tombstones. Every row ends a page
// set, so page sets also end between the two rows of every range
// tombstone boundary, where no block may start.
SEASTAR_THREAD_TEST_CASE(test_wide_partitions_with_range_tombstones) {
    test_env::do_with_async([] (test_env& env) {
        auto s = schema_builder(this_smp_shard_count(), "ks", "t")
                .with_column("pk", int32_type, column_kind::partition_key)
                .with_column("ck", int32_type, column_kind::clustering_key)
                .with_column("s", int32_type, column_kind::static_column)
                .with_column("v", utf8_type)
                .build();
        utils::chunked_vector<mutation> muts;
        for (int pk = 0; pk < 3; ++pk) {
            auto m = simple_partition(s, pk, 100);
            if (pk != 1) {
                m.set_static_cell(to_bytes("s"), data_value(int32_t(pk)), 500);
            }
            // Adjacent tombstones make boundaries, with both bound weights.
            for (int i = 0; i < 10; ++i) {
                const auto t = tombstone(2000 + i, gc_clock::time_point(gc_clock::duration(100 + i)));
                m.partition().apply_delete(*s, range_tombstone(ck_of(*s, 10 * i), i % 2 ? bound_kind::incl_start : bound_kind::excl_start,
                        ck_of(*s, 10 * i + 10), i % 2 ? bound_kind::incl_end : bound_kind::excl_end, t));
            }
            muts.push_back(std::move(m));
        }
        check_all(env, s, muts, small_page_sets(1));
        check_all(env, s, muts, small_page_sets(100));
    }).get();
}

// A partition with only a partition tombstone, and one with no fragment,
// get a row of kind "none".
SEASTAR_THREAD_TEST_CASE(test_partitions_without_rows) {
    test_env::do_with_async([] (test_env& env) {
        auto s = simple_table();
        utils::chunked_vector<mutation> muts;
        for (int pk = 0; pk < 6; ++pk) {
            mutation m(s, partition_key::from_single_value(*s, int32_type->decompose(pk)));
            if (pk % 3 == 0) {
                m.partition().apply(tombstone(100 + pk, gc_clock::time_point(gc_clock::duration(10))));
            } else if (pk % 3 == 1) {
                m = simple_partition(s, pk, 3);
            }
            muts.push_back(std::move(m));
        }
        check_all(env, s, muts, small_page_sets(1));
    }).get();
}

// Every table compression option maps to its Parquet compression codec,
// and Arrow reads the pages.
SEASTAR_THREAD_TEST_CASE(test_compression) {
    test_env::do_with_async([] (test_env& env) {
        using algorithm = compression_parameters::algorithm;
        const std::vector<std::pair<algorithm, ::parquet::Compression::type>> cases = {
            {algorithm::none, ::parquet::Compression::UNCOMPRESSED},
            {algorithm::lz4, ::parquet::Compression::LZ4},
            {algorithm::lz4_with_dicts, ::parquet::Compression::LZ4},
            {algorithm::snappy, ::parquet::Compression::SNAPPY},
            {algorithm::deflate, ::parquet::Compression::GZIP},
            {algorithm::zstd, ::parquet::Compression::ZSTD},
            {algorithm::zstd_with_dicts, ::parquet::Compression::ZSTD},
        };
        for (const auto& [alg, codec] : cases) {
            auto s = simple_table(compression_parameters(alg));
            utils::chunked_vector<mutation> muts;
            for (int pk = 0; pk < 20; ++pk) {
                muts.push_back(simple_partition(s, pk, 50, 100));
            }
            muts = sorted(std::move(muts));
            auto sst = write(env, s, muts, aa);
            const auto data = check_components(env, s, sst);
            arrow_file f(data);
            f.read_all();
            const auto& meta = f.metadata();
            for (int c = 0; c < meta.num_columns(); ++c) {
                // Arrow reports LZ4_RAW as LZ4.
                BOOST_REQUIRE_EQUAL(meta.RowGroup(0)->ColumnChunk(c)->compression(), codec);
            }
            const auto md = read_metadata(env, s, sst);
            if (alg == algorithm::none) {
                BOOST_REQUIRE_EQUAL(md.stats_md().compression_ratio, metadata_collector::NO_COMPRESSION_RATIO);
            } else {
                BOOST_REQUIRE_EQUAL(md.stats_md().compression_ratio, double(md.parquet()->file_size) / md.parquet()->raw_size);
                // The values repeat, so every codec compresses them.
                BOOST_REQUIRE_LT(md.stats_md().compression_ratio, 1.0);
            }
        }
    }).get();
}

SEASTAR_THREAD_TEST_CASE(test_writer_config) {
    test_env::do_with_async([] (test_env& env) {
        env.db_config().parquet_row_group_size_in_kb(1024);
        env.db_config().parquet_page_set_size_in_kb(16);
        env.db_config().parquet_min_page_size_in_kb(2);
        env.db_config().parquet_dictionary_page_size_limit_in_kb(1);
        const auto cfg = env.manager().configure_writer("test");
        BOOST_REQUIRE_EQUAL(cfg.parquet_row_group_size, 1u << 20);
        BOOST_REQUIRE_EQUAL(cfg.parquet_page_set_size, 16u << 10);
        BOOST_REQUIRE_EQUAL(cfg.parquet_min_page_size, 2u << 10);
        BOOST_REQUIRE_EQUAL(cfg.parquet_dictionary_page_limit, 1u << 10);

        const auto wc = pq::make_writer_config(cfg, compression_parameters({
            {compression_parameters::SSTABLE_COMPRESSION, "ZstdCompressor"},
            {"compression_level", "7"},
            {"chunk_length_in_kb", "4"},
        }));
        BOOST_REQUIRE_EQUAL(wc.row_group_size, cfg.parquet_row_group_size);
        BOOST_REQUIRE_EQUAL(wc.page_set_size, cfg.parquet_page_set_size);
        BOOST_REQUIRE_EQUAL(wc.min_page_size, cfg.parquet_min_page_size);
        BOOST_REQUIRE_EQUAL(wc.dictionary_page_limit, cfg.parquet_dictionary_page_limit);
        BOOST_REQUIRE(wc.codec == pq::compression_codec::zstd);
        BOOST_REQUIRE_EQUAL(wc.compression_level.value(), 7);
        BOOST_REQUIRE(!pq::make_writer_config(cfg, compression_parameters(compression_parameters::algorithm::lz4)).compression_level);
    }).get();
}

namespace {

struct large_data_handler_for_tests : public db::large_data_handler {
    large_data_handler_for_tests(uint64_t partition, uint64_t row, uint64_t cell)
        : large_data_handler(partition, row, cell, std::numeric_limits<uint64_t>::max(), std::numeric_limits<uint64_t>::max()) {
        start();
    }
protected:
    future<> record_large_cells(const sstables::sstable&, const sstables::key&,
            const clustering_key_prefix*, const column_definition&, uint64_t, uint64_t) const override { return make_ready_future<>(); }
    future<> record_large_rows(const sstables::sstable&, const sstables::key&,
            const clustering_key_prefix*, uint64_t) const override { return make_ready_future<>(); }
    future<> delete_large_data_entries(const schema&, sstring, std::string_view) const override { return make_ready_future<>(); }
    future<> update_large_data_entries_sstable_name(const schema&, sstring, sstring, std::string_view) const override { return make_ready_future<>(); }
    future<> record_large_partitions(const sstables::sstable&, const sstables::key&,
            uint64_t, uint64_t, uint64_t, uint64_t) const override { return make_ready_future<>(); }
};

} // anonymous namespace

// Large partitions and rows are measured in raw size, large cells by the
// size of their value.
SEASTAR_THREAD_TEST_CASE(test_large_data) {
    large_data_handler_for_tests handler(1000, 1000, 1000);
    test_env::do_with_async([] (test_env& env) {
        auto s = simple_table();
        // One partition with one row with a large value, and a small one.
        auto big = simple_partition(s, 1, 1, 5000);
        auto small = simple_partition(s, 2, 1, 10);
        utils::chunked_vector<mutation> muts;
        muts.push_back(big);
        muts.push_back(small);
        muts = sorted(std::move(muts));
        auto sst = write(env, s, muts, aa);
        const auto md = read_metadata(env, s, sst);
        const auto* records = md.scylla.data.get<scylla_metadata_type::LargeDataRecords, scylla_metadata::large_data_records>();
        BOOST_REQUIRE(records);
        const auto big_key = sstables::key::from_partition_key(*s, big.key());
        // The raw size of the big partition is the total raw size minus the
        // raw size of the small one, which is the same as the big one's
        // without the 4990 extra bytes of its value.
        const uint64_t small_raw = (md.parquet()->raw_size - 4990) / 2;
        const uint64_t big_raw = md.parquet()->raw_size - small_raw;
        std::set<large_data_type> found;
        for (const auto& rec : records->elements) {
            BOOST_REQUIRE(bytes_view(rec.partition_key.value) == bytes_view(big_key));
            found.insert(rec.type);
            switch (rec.type) {
            case large_data_type::partition_size:
                BOOST_REQUIRE_EQUAL(rec.value, big_raw);
                BOOST_REQUIRE_EQUAL(rec.elements_count, 1u);
                break;
            case large_data_type::row_size:
                // A single row without a static row is the whole partition.
                BOOST_REQUIRE_EQUAL(rec.value, big_raw);
                break;
            case large_data_type::cell_size:
                BOOST_REQUIRE_EQUAL(rec.value, 5000u);
                BOOST_REQUIRE_EQUAL(sstring(reinterpret_cast<const char*>(rec.column_name.value.data()), rec.column_name.value.size()), "v");
                break;
            default:
                BOOST_FAIL(fmt::format("unexpected record {}", int(rec.type)));
            }
        }
        BOOST_REQUIRE_EQUAL(found.size(), 3u);
        const auto* stats = md.scylla.data.get<scylla_metadata_type::LargeDataStats, scylla_metadata::large_data_stats>();
        BOOST_REQUIRE_EQUAL(stats->map.at(large_data_type::partition_size).max_value, big_raw);
        BOOST_REQUIRE_EQUAL(stats->map.at(large_data_type::cell_size).max_value, 5000u);
    }, {.large_data_handler = &handler}).get();
}

namespace {

// Writes partitions one by one with sstable_writer until it asks to stop.
// Returns the number of partitions written, or -1 if it never asked.
int write_until_stop(test_env& env, schema_ptr s, const utils::chunked_vector<mutation>& muts, sstable_writer_config cfg) {
    auto sst = env.make_sstable(s, aa);
    auto wr = sst->get_writer(*s, muts.size(), cfg, encoding_stats{});
    auto rd = make_mutation_reader_from_mutations(s, env.make_reader_permit(), muts);
    auto close = deferred_close(rd);
    int partitions = 0;
    int stopped = -1;
    while (auto mf = rd().get()) {
        if (mf->is_partition_start()) {
            wr.consume_new_partition(mf->as_partition_start().key());
            wr.consume(mf->as_partition_start().partition_tombstone());
        } else if (mf->is_clustering_row()) {
            wr.consume(std::move(*mf).as_clustering_row());
        } else if (mf->is_end_of_partition()) {
            ++partitions;
            if (wr.consume_end_of_partition() == stop_iteration::yes && stopped < 0) {
                stopped = partitions;
            }
        }
    }
    wr.consume_end_of_stream();
    return stopped;
}

} // anonymous namespace

// The writer asks to end the sstable when the estimated file size reaches
// the maximum sstable size.
SEASTAR_THREAD_TEST_CASE(test_max_sstable_size) {
    test_env::do_with_async([] (test_env& env) {
        auto s = simple_table();
        utils::chunked_vector<mutation> muts;
        // About 1 KiB of raw size per partition.
        for (int pk = 0; pk < 200; ++pk) {
            muts.push_back(simple_partition(s, pk, 10, 100));
        }
        muts = sorted(std::move(muts));
        auto cfg = env.manager().configure_writer("test");
        cfg.parquet_row_group_size = 16 << 10;
        cfg.parquet_page_set_size = 4 << 10;
        BOOST_REQUIRE_EQUAL(write_until_stop(env, s, muts, cfg), -1);
        cfg.max_sstable_size = 1;
        BOOST_REQUIRE_EQUAL(write_until_stop(env, s, muts, cfg), 1);
        cfg.max_sstable_size = 64 << 10;
        const int stopped = write_until_stop(env, s, muts, cfg);
        testlog.info("stopped after {} partitions", stopped);
        BOOST_REQUIRE_GE(stopped, 16);
        BOOST_REQUIRE_LE(stopped, 128);
    }).get();
}

// A large partition may be split at a clustering row once it has two row
// index blocks and the estimated file size is above the maximum.
SEASTAR_THREAD_TEST_CASE(test_partition_split) {
    test_env::do_with_async([] (test_env& env) {
        auto s = simple_table();
        auto m = simple_partition(s, 1, 1000, 100);
        auto cfg = env.manager().configure_writer("test");
        cfg.parquet_page_set_size = 1 << 10;
        cfg.max_sstable_size = 16 << 10;
        auto sst = env.make_sstable(s, aa);
        auto wr = sst->get_writer(*s, 1, cfg, encoding_stats{});
        wr.consume_new_partition(m.decorated_key());
        wr.consume(tombstone());
        int rows = 0;
        int split_at = -1;
        for (const auto& r : m.partition().clustered_rows()) {
            ++rows;
            if (wr.consume(clustering_row(*s, r)) == stop_iteration::yes) {
                split_at = rows;
                break;
            }
        }
        wr.consume_end_of_partition();
        wr.consume_end_of_stream();
        testlog.info("split after {} rows", split_at);
        // A row has about 110 bytes, and a page set holds about 10 rows.
        BOOST_REQUIRE_GE(split_at, 20);
        BOOST_REQUIRE_LE(split_at, 400);
    }).get();
}

namespace {

// The cut and the split are tested with values that compress more than
// tenfold, so that the raw size and the file size differ by far more than
// the error of the estimate, which is at most one row group.
constexpr uint64_t cut_test_row_group_size = 4 << 10;
constexpr uint64_t cut_test_max_sstable_size = 64 << 10;

sstable_writer_config cut_test_config(test_env& env) {
    auto cfg = env.manager().configure_writer("test");
    cfg.parquet_row_group_size = cut_test_row_group_size;
    cfg.parquet_page_set_size = 4 << 10;
    cfg.max_sstable_size = cut_test_max_sstable_size;
    return cfg;
}

// Checks that the sstable ended where its file size, without the footer,
// reached `limit`, within the error of the estimate. The raw size is far
// above the limit, so a cut by raw size would have ended it much earlier.
void check_cut_on_disk(test_env& env, schema_ptr s, const shared_sstable& sst, uint64_t limit) {
    const auto md = read_metadata(env, s, sst);
    const auto* p = md.parquet();
    BOOST_REQUIRE(p);
    BOOST_REQUIRE_EQUAL(p->file_size, read_file(sst->get_filename(component_type::Data).format()).size());
    const uint64_t data = p->file_size - p->footer_size;
    testlog.info("limit {}, file size {} without the footer, raw size {}", limit, data, p->raw_size);
    BOOST_REQUIRE_GE(p->raw_size, 4 * limit);
    BOOST_REQUIRE_GE(data + 2 * cut_test_row_group_size, limit);
    BOOST_REQUIRE_LE(data, limit + 2 * cut_test_row_group_size);
}

} // anonymous namespace

// The maximum sstable size is compared with the on-disk estimate of the file
// size, not with the raw size.
SEASTAR_THREAD_TEST_CASE(test_max_sstable_size_is_on_disk) {
    test_env::do_with_async([] (test_env& env) {
        auto s = simple_table(compression_parameters(compression_parameters::algorithm::zstd));
        utils::chunked_vector<mutation> muts;
        // About 10 KiB of raw size per partition, 12 MiB in total.
        for (int pk = 0; pk < 1200; ++pk) {
            muts.push_back(simple_partition(s, pk, 10, 1000));
        }
        muts = sorted(std::move(muts));
        auto sst = env.make_sstable(s, aa);
        auto wr = sst->get_writer(*s, muts.size(), cut_test_config(env), encoding_stats{});
        auto rd = make_mutation_reader_from_mutations(s, env.make_reader_permit(), muts);
        auto close = deferred_close(rd);
        bool stopped = false;
        while (auto mf = rd().get()) {
            if (mf->is_partition_start()) {
                wr.consume_new_partition(mf->as_partition_start().key());
                wr.consume(mf->as_partition_start().partition_tombstone());
            } else if (mf->is_clustering_row()) {
                wr.consume(std::move(*mf).as_clustering_row());
            } else if (mf->is_end_of_partition() && wr.consume_end_of_partition() == stop_iteration::yes) {
                stopped = true;
                break;
            }
        }
        BOOST_REQUIRE(stopped);
        wr.consume_end_of_stream();
        check_cut_on_disk(env, s, sst, cut_test_max_sstable_size);
    }).get();
}

// A large partition is split by the same estimate. The threshold is the
// maximum sstable size plus 10%.
SEASTAR_THREAD_TEST_CASE(test_partition_split_is_on_disk) {
    test_env::do_with_async([] (test_env& env) {
        auto s = simple_table(compression_parameters(compression_parameters::algorithm::zstd));
        auto m = simple_partition(s, 1, 12000, 1000);
        auto sst = env.make_sstable(s, aa);
        auto wr = sst->get_writer(*s, 1, cut_test_config(env), encoding_stats{});
        wr.consume_new_partition(m.decorated_key());
        wr.consume(tombstone());
        bool split = false;
        for (const auto& r : m.partition().clustered_rows()) {
            if (wr.consume(clustering_row(*s, r)) == stop_iteration::yes) {
                split = true;
                break;
            }
        }
        BOOST_REQUIRE(split);
        wr.consume_end_of_partition();
        wr.consume_end_of_stream();
        check_cut_on_disk(env, s, sst, partition_split_size_threshold(cut_test_max_sstable_size));
    }).get();
}

// The writers on a shard share a memory budget, so that each one's row
// group target shrinks as more writers are active.
SEASTAR_THREAD_TEST_CASE(test_memory_budget) {
    test_env::do_with_async([] (test_env& env) {
        auto& mgr = env.manager();
        const uint64_t budget = 1 << 20;
        env.db_config().parquet_writer_memory_fraction(double(budget) / memory::stats().total_memory());
        BOOST_REQUIRE_LE(mgr.parquet_writer_memory_budget(), budget);
        BOOST_REQUIRE_GE(mgr.parquet_writer_memory_budget(), budget - 1);
        const uint64_t exact = mgr.parquet_writer_memory_budget();

        const uint64_t page_set = 16 << 10;
        const uint64_t row_group = 8 << 20;
        BOOST_REQUIRE_EQUAL(mgr.active_parquet_writers(), 0u);
        BOOST_REQUIRE_EQUAL(mgr.parquet_row_group_target(page_set, row_group), exact);
        BOOST_REQUIRE_EQUAL(mgr.parquet_row_group_target(page_set, 256 << 10), 256u << 10);
        {
            auto h1 = mgr.register_parquet_writer(page_set, row_group);
            auto h2 = mgr.register_parquet_writer(page_set, row_group);
            BOOST_REQUIRE_EQUAL(mgr.active_parquet_writers(), 2u);
            BOOST_REQUIRE_EQUAL(h1->row_group_target(), exact / 2);
            std::vector<std::unique_ptr<pq::writer_memory_handle>> many;
            for (int i = 0; i < 1000; ++i) {
                many.push_back(mgr.register_parquet_writer(page_set, row_group));
            }
            // Capped below by the page set size.
            BOOST_REQUIRE_EQUAL(h2->row_group_target(), page_set);
            // The memory in use is the sum of the last reports.
            BOOST_REQUIRE_EQUAL(mgr.parquet_writer_memory_used(), 0u);
            h1->report_memory_usage(100);
            h2->report_memory_usage(50);
            BOOST_REQUIRE_EQUAL(mgr.parquet_writer_memory_used(), 150u);
            h1->report_memory_usage(30);
            BOOST_REQUIRE_EQUAL(mgr.parquet_writer_memory_used(), 80u);
            h1.reset();
            BOOST_REQUIRE_EQUAL(mgr.parquet_writer_memory_used(), 50u);
        }
        BOOST_REQUIRE_EQUAL(mgr.active_parquet_writers(), 0u);
        BOOST_REQUIRE_EQUAL(mgr.parquet_writer_memory_used(), 0u);

        // The row groups of a writer get smaller when other writers are open.
        auto s = simple_table();
        utils::chunked_vector<mutation> muts;
        for (int pk = 0; pk < 40; ++pk) {
            muts.push_back(simple_partition(s, pk, 100, 500));
        }
        muts = sorted(std::move(muts));
        auto tweak = [&] (sstable_writer_config& cfg) {
            cfg.parquet_page_set_size = page_set;
            cfg.parquet_row_group_size = row_group;
        };
        auto row_groups = [&] (const shared_sstable& sst) {
            return arrow_file(read_file(sst->get_filename(component_type::Data).format())).metadata().num_row_groups();
        };
        const auto alone = row_groups(write(env, s, muts, aa, tweak));

        std::vector<shared_sstable> others;
        std::vector<sstable_writer> open_writers;
        auto cfg = env.manager().configure_writer("test");
        tweak(cfg);
        for (int i = 0; i < 3; ++i) {
            others.push_back(env.make_sstable(s, aa));
            open_writers.push_back(others.back()->get_writer(*s, 1, cfg, encoding_stats{}));
        }
        BOOST_REQUIRE_EQUAL(mgr.active_parquet_writers(), 3u);
        const auto shared = row_groups(write(env, s, muts, aa, tweak));
        BOOST_REQUIRE_EQUAL(mgr.active_parquet_writers(), 3u);
        open_writers.clear();
        BOOST_REQUIRE_EQUAL(mgr.active_parquet_writers(), 0u);
        BOOST_REQUIRE_EQUAL(mgr.parquet_writer_memory_used(), 0u);
        testlog.info("row groups: {} alone, {} with 3 other writers", alone, shared);
        // About 2 MiB of data, with a target of 1 MiB alone, and of 256 KiB shared by 4 writers.
        BOOST_REQUIRE_GE(alone, 2);
        BOOST_REQUIRE_GE(shared, 2 * alone);
    }).get();
}
