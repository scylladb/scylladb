/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// End-to-end conformance of Parquet sstables: Arrow
// C++ opens the Data files that a node writes.
//
// A cql_test_env node with Parquet as the default writes tables of a broad
// schema through CQL, flushes them, and compacts them. Arrow then opens
// every `aa` Data file in the node's data directory, as a standard tool
// would, and reads every column of every row group. The test checks the
// row counts against the footer and Scylla.db, the OffsetIndex of every
// column chunk, the compression codec of every chunk, and the values of a
// few columns against what CQL reads. The mapping itself is tested
// elsewhere (parquet_mapping_test, parquet_sstable_writer_test).

#include <filesystem>
#include <fstream>
#include <map>
#include <set>
#include <string>
#include <vector>

#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>

#include <arrow/array.h>
#include <arrow/buffer.h>
#include <arrow/chunked_array.h>
#include <arrow/io/memory.h>
#include <arrow/scalar.h>
#include <arrow/table.h>
#include <parquet/arrow/reader.h>
#include <parquet/column_reader.h>
#include <parquet/file_reader.h>
#include <parquet/metadata.h>
#include <parquet/page_index.h>
#include <parquet/schema.h>
#include <parquet/statistics.h>

#include "cql3/untyped_result_set.hh"
#include "db/config.hh"
#include "replica/database.hh"
#include "sstables/sstables.hh"
#include "sstables/version.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/eventually.hh"
#include "test/lib/log.hh"

namespace {

const sstring ks = "e2e";

// A table of the broad schema, and the compression codec that its table
// compression option maps to.
struct codec_table {
    sstring name;
    sstring compression;
    ::arrow::Compression::type codec;
};

const std::vector<codec_table> codec_tables = {
    {"t_none", "{'sstable_compression': ''}", ::arrow::Compression::UNCOMPRESSED},
    // Parquet's LZ4_RAW is Arrow's LZ4.
    {"t_lz4", "{'sstable_compression': 'LZ4Compressor'}", ::arrow::Compression::LZ4},
    {"t_snappy", "{'sstable_compression': 'SnappyCompressor'}", ::arrow::Compression::SNAPPY},
    {"t_deflate", "{'sstable_compression': 'DeflateCompressor'}", ::arrow::Compression::GZIP},
    {"t_zstd", "{'sstable_compression': 'ZstdWithDictsCompressor', 'compression_level': 3}", ::arrow::Compression::ZSTD},
};

// The counter table keeps the default compression, LZ4 with dictionaries.
const sstring counter_table = "counters";
constexpr auto counter_codec = ::arrow::Compression::LZ4;

// Rows of the wide partition. With the small tunables of make_config(), it
// spans many row groups.
constexpr int wide_rows = 1500;
// The first row of the wide partition with a value in `late`. The rows
// before it fill several row groups, so `late` is a late column.
constexpr int first_late_row = 750;

cql_test_config make_config() {
    cql_test_config cfg;
    cfg.db_config->test_sstable_data_format_default.set("parquet");
    cfg.db_config->parquet_row_group_size_in_kb.set(2);
    cfg.db_config->parquet_page_set_size_in_kb.set(1);
    cfg.db_config->parquet_min_page_size_in_kb.set(1);
    cfg.db_config->parquet_dictionary_page_size_limit_in_kb.set(1);
    return cfg;
}

sstring ck2_of(int ck) {
    // An empty clustering key component on every third row.
    return ck % 3 == 0 ? "" : "x";
}

void create_tables(cql_test_env& e) {
    e.execute_cql(seastar::format("CREATE KEYSPACE {} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}}", ks)).get();
    e.execute_cql(seastar::format("CREATE TYPE {}.u (a int, b text)", ks)).get();
    for (const auto& t : codec_tables) {
        e.execute_cql(seastar::format(
                "CREATE TABLE {}.{} ("
                "pk int, pk2 text, ck int, ck2 text, "
                "s text static, s_set set<int> static, "
                "c_ascii ascii, c_bigint bigint, c_blob blob, c_boolean boolean, c_date date, c_decimal decimal, "
                "c_double double, c_duration duration, c_float float, c_inet inet, c_int int, c_smallint smallint, "
                "c_text text, c_time time, c_timestamp timestamp, c_timeuuid timeuuid, c_tinyint tinyint, "
                "c_uuid uuid, c_varint varint, c_vector vector<float, 3>, "
                "c_list list<int>, c_set set<text>, c_map map<int, text>, c_udt u, "
                "c_flist frozen<list<int>>, c_fmap frozen<map<text, int>>, c_fudt frozen<u>, c_tuple tuple<int, text>, "
                "PRIMARY KEY ((pk, pk2), ck, ck2)) "
                "WITH compression = {}", ks, t.name, t.compression)).get();
    }
    e.execute_cql(seastar::format("CREATE TABLE {}.{} (pk int, ck int, n counter, PRIMARY KEY (pk, ck))", ks, counter_table)).get();
}

// Runs the statements in unlogged batches.
void execute_batched(cql_test_env& e, const std::vector<sstring>& statements, std::string_view kind = "UNLOGGED") {
    constexpr size_t batch_size = 100;
    for (size_t i = 0; i < statements.size(); i += batch_size) {
        sstring batch = seastar::format("BEGIN {} BATCH\n", kind);
        for (size_t j = i; j < std::min(statements.size(), i + batch_size); ++j) {
            batch += statements[j] + ";\n";
        }
        batch += "APPLY BATCH";
        e.execute_cql(batch).get();
    }
}

// The first part of the data: inserts of every type, empty values, TTLs,
// and rows that later statements delete.
void write_first_part(cql_test_env& e, const sstring& t) {
    const auto table = seastar::format("{}.{}", ks, t);
    std::vector<sstring> statements;
    for (int ck = 0; ck < wide_rows; ++ck) {
        statements.push_back(seastar::format(
                "INSERT INTO {} (pk, pk2, ck, ck2, c_text, c_bigint, c_int, c_double) VALUES (0, 'wide', {}, '{}', 'text-{}', {}, {}, {})",
                table, ck, ck2_of(ck), ck, int64_t(ck) * 1000, ck, ck / 2.0));
    }
    execute_batched(e, statements);

    // Every type, with a static row.
    e.execute_cql(seastar::format(
            "INSERT INTO {} (pk, pk2, ck, ck2, s, s_set, c_ascii, c_bigint, c_blob, c_boolean, c_date, c_decimal, c_double, "
            "c_duration, c_float, c_inet, c_int, c_smallint, c_text, c_time, c_timestamp, c_timeuuid, c_tinyint, c_uuid, "
            "c_varint, c_vector, c_list, c_set, c_map, c_udt, c_flist, c_fmap, c_fudt, c_tuple) VALUES ("
            "1, 'types', 0, 'a', 'static', {{1, 2}}, 'ascii', -9000000000, 0xcafe, true, '2024-02-29', 3.14159, 2.5, "
            "1h2m3s, -1.5, '10.0.0.1', 42, -7, 'hello', '13:14:15.123456789', '2024-01-02 03:04:05.678+0000', "
            "50554d6e-29bb-11e5-b345-feff819cdc9f, 5, 6bddc89a-5644-11e4-97fc-56847afe9799, "
            "123456789012345678901234567890, [1.0, 2.0, 3.0], [1, 2, 3], {{'a', 'b'}}, {{1: 'one', 2: 'two'}}, "
            "{{a: 1, b: 'x'}}, [4, 5], {{'k': 1}}, {{a: 2, b: 'y'}}, (3, 'z'))", table)).get();
    // Empty values of the types that Parquet stores with a fixed width.
    e.execute_cql(seastar::format(
            "INSERT INTO {} (pk, pk2, ck, ck2, c_bigint, c_boolean, c_double, c_float, c_int, c_smallint, c_tinyint, "
            "c_uuid, c_timeuuid, c_date, c_time, c_timestamp, c_blob, c_inet, c_varint, c_decimal, c_ascii) VALUES ("
            "1, 'types', 1, '', blobAsBigint(0x), blobAsBoolean(0x), blobAsDouble(0x), blobAsFloat(0x), blobAsInt(0x), "
            "blobAsSmallint(0x), blobAsTinyint(0x), blobAsUuid(0x), blobAsTimeuuid(0x), blobAsDate(0x), blobAsTime(0x), "
            "blobAsTimestamp(0x), 0x, blobAsInet(0x), blobAsVarint(0x), blobAsDecimal(0x), '')", table)).get();
    // TTLs: a row with a TTL, and a cell with another TTL.
    e.execute_cql(seastar::format("INSERT INTO {} (pk, pk2, ck, ck2, c_text, c_int) VALUES (1, 'types', 2, 't', 'ttl', 1) USING TTL 86400", table)).get();
    e.execute_cql(seastar::format("UPDATE {} USING TTL 3600 SET c_bigint = 7 WHERE pk = 1 AND pk2 = 'types' AND ck = 2 AND ck2 = 't'", table)).get();
    // Rows that a partition tombstone and a range tombstone delete later.
    for (int ck = 0; ck < 30; ++ck) {
        e.execute_cql(seastar::format("INSERT INTO {} (pk, pk2, ck, ck2, c_int) VALUES (2, 'gone', {}, '', {})", table, ck, ck)).get();
        e.execute_cql(seastar::format("INSERT INTO {} (pk, pk2, ck, ck2, c_text) VALUES (3, 'rt', {}, 'r', 'rt-{}')", table, ck, ck)).get();
    }
    // A partition with an empty partition key component and a static row with a TTL.
    e.execute_cql(seastar::format("UPDATE {} USING TTL 7200 SET s = 'static-ttl' WHERE pk = 4 AND pk2 = ''", table)).get();
}

// The second part: deletions of every kind, collection updates, a row
// with only a marker, a row without a marker, and the late column.
void write_second_part(cql_test_env& e, const sstring& t) {
    const auto table = seastar::format("{}.{}", ks, t);
    const auto wide = "pk = 0 AND pk2 = 'wide'";
    std::vector<sstring> statements;
    for (int ck = first_late_row; ck < wide_rows; ++ck) {
        statements.push_back(seastar::format("UPDATE {} SET late = {} WHERE {} AND ck = {} AND ck2 = '{}'", table, ck, wide, ck, ck2_of(ck)));
    }
    execute_batched(e, statements);

    // A range tombstone with a clustering prefix, a row tombstone, and a cell tombstone.
    e.execute_cql(seastar::format("DELETE FROM {} WHERE {} AND ck = 5", table, wide)).get();
    e.execute_cql(seastar::format("DELETE FROM {} WHERE {} AND ck = 6 AND ck2 = '{}'", table, wide, ck2_of(6))).get();
    e.execute_cql(seastar::format("DELETE c_text FROM {} WHERE {} AND ck = 7 AND ck2 = '{}'", table, wide, ck2_of(7))).get();
    // Two overlapping range tombstones with different timestamps, which
    // makes boundaries.
    e.execute_cql(seastar::format("DELETE FROM {} WHERE {} AND ck > 100 AND ck <= 120", table, wide)).get();
    e.execute_cql(seastar::format("DELETE FROM {} WHERE {} AND ck >= 110 AND ck < 130", table, wide)).get();
    // A row with only a marker, and a row without a marker.
    e.execute_cql(seastar::format("INSERT INTO {} (pk, pk2, ck, ck2) VALUES (0, 'wide', -1, 'm')", table)).get();
    e.execute_cql(seastar::format("UPDATE {} SET c_text = 'no-marker' WHERE {} AND ck = -2 AND ck2 = 'u'", table, wide)).get();
    // Collections: an element tombstone, a collection tombstone, an append,
    // a UDT field, and an empty element.
    const auto types_row = "pk = 1 AND pk2 = 'types' AND ck = 0 AND ck2 = 'a'";
    e.execute_cql(seastar::format("UPDATE {} SET c_map = c_map - {{1}} WHERE {}", table, types_row)).get();
    e.execute_cql(seastar::format("UPDATE {} SET c_set = {{'q'}} WHERE {}", table, types_row)).get();
    e.execute_cql(seastar::format("UPDATE {} SET c_list = c_list + [9] WHERE {}", table, types_row)).get();
    e.execute_cql(seastar::format("UPDATE {} SET c_udt.a = 7 WHERE {}", table, types_row)).get();
    e.execute_cql(seastar::format("UPDATE {} SET c_list = c_list + [blobAsInt(0x)] WHERE {}", table, types_row)).get();
    e.execute_cql(seastar::format("UPDATE {} SET s_set = s_set - {{1}} WHERE pk = 1 AND pk2 = 'types'", table)).get();
    // A partition tombstone over earlier rows, then a newer row; a
    // partition with only a tombstone; a range tombstone over earlier rows.
    e.execute_cql(seastar::format("DELETE FROM {} WHERE pk = 2 AND pk2 = 'gone'", table)).get();
    e.execute_cql(seastar::format("INSERT INTO {} (pk, pk2, ck, ck2, c_text) VALUES (2, 'gone', 100, '', 'back')", table)).get();
    e.execute_cql(seastar::format("DELETE FROM {} WHERE pk = 5 AND pk2 = 'tombstone-only'", table)).get();
    e.execute_cql(seastar::format("DELETE FROM {} WHERE pk = 3 AND pk2 = 'rt' AND ck >= 10 AND ck < 20", table)).get();
}

void write_counters(cql_test_env& e, int round) {
    std::vector<sstring> statements;
    for (int pk = 0; pk < 10; ++pk) {
        for (int ck = 0; ck < 10; ++ck) {
            if (round == 0 || ck % 2 == 0) {
                statements.push_back(seastar::format("UPDATE {}.{} SET n = n + {} WHERE pk = {} AND ck = {}", ks, counter_table, pk * 10 + ck + 1, pk, ck));
            }
        }
    }
    execute_batched(e, statements, "COUNTER");
    if (round == 1) {
        e.execute_cql(seastar::format("DELETE n FROM {}.{} WHERE pk = 9 AND ck = 9", ks, counter_table)).get();
    }
}

std::vector<sstring> all_tables() {
    std::vector<sstring> r;
    for (const auto& t : codec_tables) {
        r.push_back(t.name);
    }
    r.push_back(counter_table);
    return r;
}

void flush_all(cql_test_env& e) {
    for (const auto& t : all_tables()) {
        e.db().invoke_on_all([t] (replica::database& db) {
            return db.flush(ks, t);
        }).get();
    }
}

void compact_all(cql_test_env& e) {
    for (const auto& t : all_tables()) {
        e.db().invoke_on_all([t] (replica::database& db) {
            return db.find_column_family(ks, t).compact_all_sstables(tasks::make_empty_task_info());
        }).get();
    }
}

// A live sstable of one of the test's tables.
struct live_sstable {
    sstring table;
    sstables::sstable_version_types version;
    // The Parquet entry of Scylla.db.
    uint64_t rows;
    uint64_t file_size;
};

// The live sstables of the test's tables on all shards, by the absolute
// path of their Data file.
std::map<std::string, live_sstable> live_sstables(cql_test_env& e) {
    using result = std::map<std::string, live_sstable>;
    return e.db().map_reduce0([] (replica::database& db) {
        result r;
        for (const auto& t : all_tables()) {
            for (const auto& sst : *db.find_column_family(ks, t).get_sstables()) {
                const auto* entry = sst->get_scylla_metadata() ? sst->get_scylla_metadata()->get_parquet_data_file() : nullptr;
                const auto path = std::filesystem::absolute(std::filesystem::path(sst->get_filename(sstables::component_type::Data).format())).lexically_normal().native();
                r.emplace(path, live_sstable{t, sst->get_version(), entry ? entry->rows : 0, entry ? entry->file_size : 0});
            }
        }
        return r;
    }, result{}, [] (result a, result b) {
        a.merge(std::move(b));
        return a;
    }).get();
}

// The Data files of `aa` sstables anywhere in the node's data directory,
// by absolute path.
std::set<std::string> parquet_data_files_on_disk(cql_test_env& e) {
    std::set<std::string> r;
    const std::filesystem::path data_dir(e.db_config().data_file_directories()[0]);
    for (const auto& entry : std::filesystem::recursive_directory_iterator(data_dir)) {
        const auto name = entry.path().filename().native();
        if (entry.is_regular_file() && name.ends_with("-pq-Data.db")) {
            r.insert(std::filesystem::absolute(entry.path()).lexically_normal().native());
        }
    }
    return r;
}

std::string read_file(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    BOOST_REQUIRE_MESSAGE(in, path);
    return std::string(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
}

// Reads every value of a column chunk, and returns the number of rows:
// the entries whose repetition level is zero.
template <typename DType>
int64_t count_rows(::parquet::ColumnReader& base, const ::parquet::ColumnDescriptor& d) {
    auto& r = static_cast<::parquet::TypedColumnReader<DType>&>(base);
    constexpr int64_t batch = 1024;
    std::vector<int16_t> def(batch), rep(batch);
    auto values = std::make_unique<typename DType::c_type[]>(batch);
    int64_t rows = 0;
    while (r.HasNext()) {
        int64_t n_values = 0;
        const int64_t n = r.ReadBatch(batch, def.data(), rep.data(), values.get(), &n_values);
        for (int64_t i = 0; i < n; ++i) {
            if (d.max_repetition_level() == 0 || rep[i] == 0) {
                ++rows;
            }
        }
    }
    return rows;
}

int64_t count_rows(::parquet::ColumnReader& r, const ::parquet::ColumnDescriptor& d) {
    switch (d.physical_type()) {
    case ::parquet::Type::BOOLEAN: return count_rows<::parquet::BooleanType>(r, d);
    case ::parquet::Type::INT32: return count_rows<::parquet::Int32Type>(r, d);
    case ::parquet::Type::INT64: return count_rows<::parquet::Int64Type>(r, d);
    case ::parquet::Type::FLOAT: return count_rows<::parquet::FloatType>(r, d);
    case ::parquet::Type::DOUBLE: return count_rows<::parquet::DoubleType>(r, d);
    case ::parquet::Type::BYTE_ARRAY: return count_rows<::parquet::ByteArrayType>(r, d);
    case ::parquet::Type::FIXED_LEN_BYTE_ARRAY: return count_rows<::parquet::FLBAType>(r, d);
    default: BOOST_FAIL(fmt::format("unexpected physical type of {}", d.path()->ToDotString()));
    }
    return 0;
}

// What one Data file showed.
struct file_report {
    int row_groups = 0;
    // Column chunks without an OffsetIndex: the chunks of late columns.
    int late_chunks = 0;
};

// Opens a Data file with the low-level Arrow reader, and checks its
// structure: row counts, OffsetIndexes, and compression codecs. Reads
// every column of every row group, with page checksum verification.
file_report check_structure(const std::string& path, const std::string& data, const live_sstable& sst, ::arrow::Compression::type codec) {
    ::parquet::ReaderProperties props;
    props.set_page_checksum_verification(true);
    auto reader = ::parquet::ParquetFileReader::Open(std::make_shared<::arrow::io::BufferReader>(::arrow::Buffer::FromString(data)), props);
    const auto meta = reader->metadata();
    const auto& schema = *meta->schema();

    // The row counts: Scylla.db, the footer, its row groups, and each column chunk.
    BOOST_REQUIRE_EQUAL(sst.file_size, data.size());
    BOOST_REQUIRE_EQUAL(uint64_t(meta->num_rows()), sst.rows);
    int64_t rows_in_row_groups = 0;
    file_report report;
    report.row_groups = meta->num_row_groups();
    auto page_index = reader->GetPageIndexReader();
    BOOST_REQUIRE(page_index);
    for (int rg = 0; rg < meta->num_row_groups(); ++rg) {
        const auto rg_meta = meta->RowGroup(rg);
        const int64_t rg_rows = rg_meta->num_rows();
        rows_in_row_groups += rg_rows;
        auto rg_reader = reader->RowGroup(rg);
        auto rg_page_index = page_index->RowGroup(rg);
        BOOST_REQUIRE(rg_page_index);
        for (int c = 0; c < meta->num_columns(); ++c) {
            const auto& d = *schema.Column(c);
            const auto what = fmt::format("{}: column {} in row group {}", path, d.path()->ToDotString(), rg);
            const auto chunk = rg_meta->ColumnChunk(c);
            BOOST_REQUIRE_MESSAGE(chunk->compression() == codec, what);
            BOOST_REQUIRE_MESSAGE(chunk->num_values() >= rg_rows, what);
            BOOST_REQUIRE_MESSAGE(count_rows(*rg_reader->Column(c), d) == rg_rows, what);

            auto offset_index = rg_page_index->GetOffsetIndex(c);
            if (!offset_index) {
                // A late chunk: entirely null, as its statistics say.
                const auto stats = chunk->statistics();
                BOOST_REQUIRE_MESSAGE(stats && stats->HasNullCount() && stats->null_count() == chunk->num_values(), what);
                ++report.late_chunks;
                continue;
            }
            const auto& pages = offset_index->page_locations();
            BOOST_REQUIRE_MESSAGE(!pages.empty(), what);
            BOOST_REQUIRE_MESSAGE(pages.front().first_row_index == 0, what);
            for (size_t i = 0; i < pages.size(); ++i) {
                BOOST_REQUIRE_MESSAGE(pages[i].first_row_index < rg_rows, what);
                BOOST_REQUIRE_MESSAGE(i == 0 || pages[i].first_row_index > pages[i - 1].first_row_index, what);
                BOOST_REQUIRE_MESSAGE(pages[i].offset > 0 && uint64_t(pages[i].offset) + pages[i].compressed_page_size <= data.size(), what);
            }
        }
    }
    BOOST_REQUIRE_EQUAL(rows_in_row_groups, meta->num_rows());

    // Key columns come first, under their CQL names, and every other
    // top-level field is a CQL column or the reserved group.
    const auto* root = schema.group_node();
    std::vector<std::string> fields;
    for (int i = 0; i < root->field_count(); ++i) {
        fields.push_back(root->field(i)->name());
    }
    const std::vector<std::string> keys = sst.table == counter_table
            ? std::vector<std::string>{"pk", "ck"}
            : std::vector<std::string>{"pk", "pk2", "ck", "ck2"};
    BOOST_REQUIRE_GE(fields.size(), keys.size() + 1);
    BOOST_REQUIRE(std::equal(keys.begin(), keys.end(), fields.begin()));
    BOOST_REQUIRE_EQUAL(fields.back(), "_scylla");
    return report;
}

// The value of a cell of an Arrow column, as text, or nullopt for a null.
// An absent column is null.
std::optional<std::string> text_of(const std::shared_ptr<::arrow::ChunkedArray>& column, int64_t row) {
    if (!column) {
        return std::nullopt;
    }
    auto scalar = column->GetScalar(row).ValueOrDie();
    if (!scalar->is_valid) {
        return std::nullopt;
    }
    if (auto binary = std::dynamic_pointer_cast<::arrow::BaseBinaryScalar>(scalar)) {
        return binary->value->ToString();
    }
    return scalar->ToString();
}

std::shared_ptr<::arrow::Table> read_table(const std::string& data) {
    ::parquet::arrow::FileReaderBuilder builder;
    PARQUET_THROW_NOT_OK(builder.Open(std::make_shared<::arrow::io::BufferReader>(::arrow::Buffer::FromString(data))));
    std::unique_ptr<::parquet::arrow::FileReader> reader;
    PARQUET_THROW_NOT_OK(builder.Build(&reader));
    std::shared_ptr<::arrow::Table> table;
    PARQUET_THROW_NOT_OK(reader->ReadTable(&table));
    return table;
}

// A key column is always in the file. Any other column is absent if it is
// null in the whole file.
std::shared_ptr<::arrow::ChunkedArray> key_column_of(const ::arrow::Table& table, const std::string& name) {
    auto c = table.GetColumnByName(name);
    BOOST_REQUIRE_MESSAGE(c, "no column " + name);
    return c;
}

// The key and some values of a clustering row, as text.
using row_key = std::tuple<std::string, std::string, std::string, std::string>;
using row_values = std::tuple<std::string, std::optional<std::string>>;

// What a standard tool sees of the CQL values in the Data files of one
// table, through Arrow's high-level reader.
struct table_values {
    // Clustering rows with a value in c_text: c_text and c_bigint.
    std::map<row_key, row_values> rows;
    // Partitions with a value in the static column s.
    std::map<std::pair<std::string, std::string>, std::string> statics;
    // Counter cells: the sum of the shard values.
    std::map<std::pair<std::string, std::string>, int64_t> counters;
};

// The partition key is on the first row of each partition only, so the
// reader carries it forward.
void collect_values(const std::string& data, bool counters, table_values& out) {
    const auto table = read_table(data);
    const auto pk = key_column_of(*table, "pk");
    const auto ck = key_column_of(*table, "ck");
    std::optional<std::string> current_pk, current_pk2;
    if (counters) {
        const auto n = table->GetColumnByName("n");
        for (int64_t row = 0; n && row < table->num_rows(); ++row) {
            if (auto v = text_of(pk, row)) {
                current_pk = v;
            }
            auto shards = n->GetScalar(row).ValueOrDie();
            if (!shards->is_valid) {
                continue;
            }
            const auto& values = static_cast<const ::arrow::Int64Array&>(*std::static_pointer_cast<::arrow::BaseListScalar>(shards)->value);
            int64_t sum = 0;
            for (int64_t i = 0; i < values.length(); ++i) {
                sum += values.Value(i);
            }
            BOOST_REQUIRE(current_pk);
            out.counters.emplace(std::pair(*current_pk, *text_of(ck, row)), sum);
        }
        return;
    }
    const auto pk2 = key_column_of(*table, "pk2");
    const auto ck2 = key_column_of(*table, "ck2");
    const auto s = table->GetColumnByName("s");
    const auto c_text = table->GetColumnByName("c_text");
    const auto c_bigint = table->GetColumnByName("c_bigint");
    for (int64_t row = 0; row < table->num_rows(); ++row) {
        if (auto v = text_of(pk, row)) {
            current_pk = v;
            current_pk2 = text_of(pk2, row);
            BOOST_REQUIRE(current_pk2);
        }
        BOOST_REQUIRE(current_pk);
        if (auto v = text_of(s, row)) {
            out.statics.emplace(std::pair(*current_pk, *current_pk2), *v);
        }
        if (auto text = text_of(c_text, row)) {
            row_key key{*current_pk, *current_pk2, *text_of(ck, row), *text_of(ck2, row)};
            out.rows.emplace(key, row_values{*text, text_of(c_bigint, row)});
        }
    }
}

// The same values, as CQL reads them.
table_values cql_values(cql_test_env& e, const sstring& t) {
    table_values r;
    if (t == counter_table) {
        cql3::untyped_result_set rs(e.execute_cql(seastar::format("SELECT pk, ck, n FROM {}.{}", ks, t)).get());
        for (const auto& row : rs) {
            if (auto n = row.get_opt<int64_t>("n")) {
                r.counters.emplace(std::pair(fmt::to_string(row.get_as<int32_t>("pk")), fmt::to_string(row.get_as<int32_t>("ck"))), *n);
            }
        }
        return r;
    }
    cql3::untyped_result_set rows(e.execute_cql(seastar::format("SELECT pk, pk2, ck, ck2, c_text, c_bigint FROM {}.{}", ks, t)).get());
    for (const auto& row : rows) {
        if (auto text = row.get_opt<sstring>("c_text")) {
            row_key key{fmt::to_string(row.get_as<int32_t>("pk")), row.get_as<sstring>("pk2"), fmt::to_string(row.get_as<int32_t>("ck")), row.get_as<sstring>("ck2")};
            auto bigint = row.get_opt<int64_t>("c_bigint");
            r.rows.emplace(key, row_values{*text, bigint ? std::optional(fmt::to_string(*bigint)) : std::nullopt});
        }
    }
    cql3::untyped_result_set statics(e.execute_cql(seastar::format("SELECT DISTINCT pk, pk2, s FROM {}.{}", ks, t)).get());
    for (const auto& row : statics) {
        if (auto s = row.get_opt<sstring>("s")) {
            r.statics.emplace(std::pair(fmt::to_string(row.get_as<int32_t>("pk")), row.get_as<sstring>("pk2")), *s);
        }
    }
    return r;
}

::arrow::Compression::type codec_of(const sstring& table) {
    for (const auto& t : codec_tables) {
        if (t.name == table) {
            return t.codec;
        }
    }
    return counter_codec;
}

// Arrow opens every Parquet Data file in the node's data directory. If
// `check_values` is set, every live row of the test's tables is in a single
// row of the files, so the values that Arrow reads must equal those that
// CQL reads.
void check_files(cql_test_env& e, bool check_values) {
    const auto live = live_sstables(e);
    std::set<std::string> live_paths;
    for (const auto& [path, sst] : live) {
        BOOST_REQUIRE_MESSAGE(sst.version == sstables::sstable_version_types::aa, path);
        live_paths.insert(path);
    }
    // No other table has Parquet sstables, and the files of the sstables
    // that a compaction replaced are gone.
    BOOST_REQUIRE(eventually_true([&] { return parquet_data_files_on_disk(e) == live_paths; }));

    std::map<sstring, table_values> arrow_values;
    std::map<sstring, int> max_row_groups;
    std::map<sstring, int> late_chunks;
    for (const auto& path : parquet_data_files_on_disk(e)) {
        const auto& sst = live.at(path);
        testlog.info("Checking {} of {}", path, sst.table);
        const auto data = read_file(path);
        const auto report = check_structure(path, data, sst, codec_of(sst.table));
        max_row_groups[sst.table] = std::max(max_row_groups[sst.table], report.row_groups);
        late_chunks[sst.table] += report.late_chunks;
        collect_values(data, sst.table == counter_table, arrow_values[sst.table]);
    }

    for (const auto& t : codec_tables) {
        // The wide partition spans several row groups.
        BOOST_REQUIRE_GE(max_row_groups[t.name], 3);
    }
    if (!check_values) {
        return;
    }
    for (const auto& t : codec_tables) {
        // In the file that holds the whole wide partition, `late` has
        // values only from row first_late_row on, so it has late chunks.
        BOOST_REQUIRE_GT(late_chunks[t.name], 0);
    }
    for (const auto& t : all_tables()) {
        const auto expected = cql_values(e, t);
        const auto& actual = arrow_values[t];
        BOOST_REQUIRE(actual.rows == expected.rows);
        BOOST_REQUIRE(actual.statics == expected.statics);
        BOOST_REQUIRE(actual.counters == expected.counters);
    }
    // A few literal values, to show that the comparison above compares something.
    const auto& wide = arrow_values.at("t_zstd").rows;
    BOOST_REQUIRE(wide.at(row_key{"0", "wide", "8", "x"}) == row_values("text-8", "8000"));
    BOOST_REQUIRE(wide.at(row_key{"0", "wide", "9", ""}) == row_values("text-9", "9000"));
    BOOST_REQUIRE(wide.at(row_key{"0", "wide", "-2", "u"}) == row_values("no-marker", std::nullopt));
    BOOST_REQUIRE(wide.at(row_key{"1", "types", "0", "a"}) == row_values("hello", "-9000000000"));
    BOOST_REQUIRE(!wide.contains(row_key{"0", "wide", "7", "x"}));
    BOOST_REQUIRE(!wide.contains(row_key{"0", "wide", "115", "x"}));
    BOOST_REQUIRE(!wide.contains(row_key{"3", "rt", "15", "r"}));
    BOOST_REQUIRE(arrow_values.at("t_zstd").statics.at({"4", ""}) == "static-ttl");
    BOOST_REQUIRE_EQUAL(arrow_values.at(counter_table).counters.at({"3", "4"}), 2 * (3 * 10 + 4 + 1));
    BOOST_REQUIRE(!arrow_values.at(counter_table).counters.contains({"9", "9"}));
}

} // anonymous namespace

SEASTAR_TEST_CASE(test_arrow_reads_the_parquet_files_of_a_node) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        create_tables(e);
        for (const auto& t : codec_tables) {
            write_first_part(e, t.name);
        }
        write_counters(e, 0);
        flush_all(e);

        for (const auto& t : codec_tables) {
            e.execute_cql(seastar::format("ALTER TABLE {}.{} ADD late int", ks, t.name)).get();
            write_second_part(e, t.name);
        }
        write_counters(e, 1);
        flush_all(e);
        // The files that flushes wrote. Some rows are in two of them.
        check_files(e, false);

        compact_all(e);
        // The files that a major compaction wrote. Each row is in one of them.
        check_files(e, true);
    }, make_config());
}
