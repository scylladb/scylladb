/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Tests of the write direction of the Parquet mapping
// (sstables/parquet/mapping.hh and mapping_writer.hh).
//
// The oracle is Arrow C++. The hand-written cases write a few fragments
// and compare what Arrow reads with literal values. These literals pin the
// format: an accidental format change breaks them. The random cases check
// that Arrow reads every column of files written from random mutations, and
// that the invariants of the mapping hold.
//
// In the literals, each string is one row of one leaf column: "null", a
// value, or for a repeated leaf a list such as "[1, null]" or "[]". Byte
// arrays are hexadecimal ("0x0102"), except STRING values, which are quoted.

#include <map>
#include <random>
#include <string>
#include <vector>

#include <seastar/core/memory.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/closeable.hh>

// Arrow's headers use std::is_trivial, which C++26 deprecates.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#include <arrow/buffer.h>
#include <arrow/io/memory.h>
#include <arrow/util/key_value_metadata.h>
#include <parquet/arrow/schema.h>
#include <parquet/column_reader.h>
#include <parquet/file_reader.h>
#include <parquet/metadata.h>
#include <parquet/schema.h>
#pragma GCC diagnostic pop

#include "encoding_stats.hh"
#include "mutation/collection_mutation.hh"
#include "mutation/counters.hh"
#include "mutation/mutation_fragment_v2.hh"
#include "readers/from_mutations.hh"
#include "schema/schema_builder.hh"
#include "sstables/file_writer.hh"
#include "sstables/parquet/mapping.hh"
#include "sstables/parquet/mapping_writer.hh"
#include "sstables/serialization_header_builder.hh"
// The serialization header is a Statistics.db component, whose virtual
// write() needs the serialization templates.
#include "sstables/writer.hh"
#include "test/lib/mutation_source_test.hh"
#include "test/lib/reader_concurrency_semaphore.hh"
#include "types/list.hh"
#include "types/map.hh"
#include "types/set.hh"
#include "types/user.hh"
#include "utils/memory_data_sink.hh"

namespace pq = sstables::parquet;

namespace {

using rows = std::vector<std::string>;

// Writes fragments with the mapping writer to memory.
class test_writer {
    schema_ptr _schema;
    sstables::serialization_header _header;
    memory_data_sink_buffers _bufs;
    sstables::file_writer _fw;
    pq::mapping_writer _w;
    tombstone _open_rt;
public:
    std::vector<pq::row_info> written;
    // The ordinal of the first row of each partition.
    std::vector<uint64_t> partition_starts;

    explicit test_writer(schema_ptr s, std::unique_ptr<pq::row_defaults_policy> policy = nullptr, pq::writer_config cfg = {})
        : _schema(s)
        , _header(sstables::make_serialization_header(*s, encoding_stats{}))
        , _fw(data_sink(std::make_unique<memory_data_sink>(_bufs)))
        , _w(*s, _header, _fw, std::move(cfg), nullptr, std::move(policy))
    {}

    pq::mapping_writer& writer() { return _w; }

    void partition(const partition_key& pk, tombstone t = {}) {
        partition_starts.push_back(_w.file().rows());
        _w.start_partition(pk);
        _w.partition_tombstone(t);
    }
    void static_row(const ::static_row& sr) {
        if (auto r = _w.write_static_row(sr)) {
            written.push_back(*r);
        }
    }
    void row(const clustering_row& cr) {
        written.push_back(_w.write_clustering_row(cr));
    }
    void rt(const range_tombstone_change& rtc) {
        for (const auto& b : pq::range_tombstone_bounds(_open_rt, rtc)) {
            written.push_back(_w.write_range_tombstone_bound(rtc.position().key(), b.kind, b.tomb));
        }
        _open_rt = rtc.tombstone();
    }
    void end_partition() {
        if (auto r = _w.end_partition()) {
            written.push_back(*r);
        }
    }
    // Ends the file, checks the reported rows, and returns the file.
    std::string finish() {
        const auto result = _w.finish();
        _fw.close();
        BOOST_REQUIRE_EQUAL(result.rows, written.size());
        uint64_t raw = 0;
        for (size_t i = 0; i < written.size(); ++i) {
            BOOST_REQUIRE_EQUAL(written[i].ordinal, i);
            raw += written[i].raw_size;
        }
        BOOST_REQUIRE_EQUAL(result.raw_size, raw);
        std::string data;
        for (const auto& b : _bufs.buffers()) {
            data.append(b.get(), b.size());
        }
        return data;
    }
};

std::string hex(const uint8_t* p, size_t n) {
    return "0x" + to_hex(bytes_view(reinterpret_cast<const int8_t*>(p), n));
}

// A file as Arrow reads it.
class arrow_file {
    std::unique_ptr<::parquet::ParquetFileReader> _reader;
    std::map<std::string, int> _columns;

    template <typename DType, typename Render>
    void read(int col, std::vector<int16_t>& defs, std::vector<int16_t>& reps, std::vector<std::optional<std::string>>& values, Render&& render) {
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
                    defs.push_back(dl);
                    reps.push_back(d.max_repetition_level() ? rep[i] : 0);
                    if (dl == d.max_definition_level()) {
                        values.push_back(render(vals[vi++], d));
                    } else {
                        values.push_back(std::nullopt);
                    }
                }
                BOOST_REQUIRE_EQUAL(vi, n_values);
            }
        }
    }
public:
    explicit arrow_file(const std::string& data) {
        ::parquet::ReaderProperties props;
        props.set_page_checksum_verification(true);
        _reader = ::parquet::ParquetFileReader::Open(std::make_shared<::arrow::io::BufferReader>(::arrow::Buffer::FromString(data)), props);
        const auto meta = _reader->metadata();
        for (int i = 0; i < meta->num_columns(); ++i) {
            _columns[meta->schema()->Column(i)->path()->ToDotString()] = i;
        }
        // Arrow accepts the whole schema, with its LIST and MAP shapes.
        std::shared_ptr<::arrow::Schema> arrow_schema;
        BOOST_REQUIRE(::parquet::arrow::FromParquetSchema(meta->schema(), &arrow_schema).ok());
    }

    const ::parquet::FileMetaData& metadata() const { return *_reader->metadata(); }
    int64_t num_rows() const { return _reader->metadata()->num_rows(); }

    std::vector<std::string> paths() const {
        std::vector<std::string> r;
        for (int i = 0; i < _reader->metadata()->num_columns(); ++i) {
            r.push_back(_reader->metadata()->schema()->Column(i)->path()->ToDotString());
        }
        return r;
    }
    bool has(const std::string& path) const { return _columns.contains(path); }

    // The physical type, the logical type, and the converted type of a leaf.
    std::string type_of(const std::string& path) const {
        BOOST_REQUIRE_MESSAGE(has(path), path);
        const auto& d = *_reader->metadata()->schema()->Column(_columns.at(path));
        return fmt::format("{} {} {}", ::parquet::TypeToString(d.physical_type(), d.type_length()), d.logical_type()->ToString(),
                ::parquet::ConvertedTypeToString(d.converted_type()));
    }

    // The entries of every row of a leaf: levels and rendered values.
    struct entries {
        std::vector<int16_t> defs, reps;
        std::vector<std::optional<std::string>> values;
        int16_t max_def, max_rep;
    };
    entries read(const std::string& path) {
        BOOST_REQUIRE_MESSAGE(has(path), path);
        const int col = _columns.at(path);
        const auto& d = *_reader->metadata()->schema()->Column(col);
        entries e{.max_def = d.max_definition_level(), .max_rep = d.max_repetition_level()};
        const bool is_string = d.logical_type()->is_string();
        switch (d.physical_type()) {
        case ::parquet::Type::BOOLEAN:
            read<::parquet::BooleanType>(col, e.defs, e.reps, e.values, [] (bool v, auto&) { return std::string(v ? "true" : "false"); });
            break;
        case ::parquet::Type::INT32:
            read<::parquet::Int32Type>(col, e.defs, e.reps, e.values, [] (int32_t v, auto&) { return fmt::to_string(v); });
            break;
        case ::parquet::Type::INT64:
            read<::parquet::Int64Type>(col, e.defs, e.reps, e.values, [] (int64_t v, auto&) { return fmt::to_string(v); });
            break;
        case ::parquet::Type::FLOAT:
            read<::parquet::FloatType>(col, e.defs, e.reps, e.values, [] (float v, auto&) { return fmt::to_string(v); });
            break;
        case ::parquet::Type::DOUBLE:
            read<::parquet::DoubleType>(col, e.defs, e.reps, e.values, [] (double v, auto&) { return fmt::to_string(v); });
            break;
        case ::parquet::Type::BYTE_ARRAY:
            read<::parquet::ByteArrayType>(col, e.defs, e.reps, e.values, [&] (const ::parquet::ByteArray& v, auto&) {
                return is_string ? fmt::format("'{}'", std::string_view(reinterpret_cast<const char*>(v.ptr), v.len)) : hex(v.ptr, v.len);
            });
            break;
        case ::parquet::Type::FIXED_LEN_BYTE_ARRAY:
            read<::parquet::FLBAType>(col, e.defs, e.reps, e.values, [] (const ::parquet::FixedLenByteArray& v, const ::parquet::ColumnDescriptor& d) {
                return hex(v.ptr, d.type_length());
            });
            break;
        default:
            BOOST_FAIL("unexpected type");
        }
        return e;
    }

    // One string per row. See the top of this file.
    rows column(const std::string& path) {
        const auto e = read(path);
        rows r;
        if (e.max_rep == 0) {
            for (const auto& v : e.values) {
                r.push_back(v.value_or("null"));
            }
            BOOST_REQUIRE_EQUAL(int64_t(r.size()), num_rows());
            return r;
        }
        std::vector<std::string> list;
        auto flush = [&] (int16_t first_def) {
            if (first_def == 0) {
                r.push_back("null");
            } else {
                r.push_back(fmt::format("[{}]", fmt::join(list, ", ")));
            }
            list.clear();
        };
        int16_t first_def = 0;
        for (size_t i = 0; i < e.values.size(); ++i) {
            if (e.reps[i] == 0) {
                if (i > 0) {
                    flush(first_def);
                }
                first_def = e.defs[i];
            }
            // Level 1 is an empty list, which has no element.
            if (e.defs[i] >= 2) {
                list.push_back(e.values[i].value_or("null"));
            }
        }
        if (!e.values.empty()) {
            flush(first_def);
        }
        BOOST_REQUIRE_EQUAL(int64_t(r.size()), num_rows());
        return r;
    }

    std::string key_value(const std::string& key) const {
        return _reader->metadata()->key_value_metadata()->Get(key).ValueOrDie();
    }
};

void require_column(arrow_file& f, const std::string& path, const rows& expected) {
    const auto got = f.column(path);
    BOOST_REQUIRE_MESSAGE(got == expected, fmt::format("{}: got [{}], expected [{}]", path, fmt::join(got, "; "), fmt::join(expected, "; ")));
}

template <typename T>
void require_same(const std::vector<T>& got, const std::vector<T>& expected) {
    BOOST_REQUIRE_MESSAGE(got == expected, fmt::format("got [{}], expected [{}]", fmt::join(got, ", "), fmt::join(expected, ", ")));
}

void require_absent(const arrow_file& f, const std::string& path) {
    BOOST_REQUIRE_MESSAGE(!f.has(path), fmt::format("{} is in the file", path));
}

bytes serialized(const data_type& t, std::string_view s) {
    return to_bytes(t->from_string(s));
}

partition_key pk_of(const schema& s, std::vector<bytes> components) {
    return partition_key::from_exploded(s, components);
}

clustering_key_prefix ck_of(const schema& s, std::vector<bytes> components) {
    return clustering_key_prefix::from_exploded(s, components);
}

bytes int_value(int32_t v) {
    return int32_type->decompose(v);
}

gc_clock::time_point tp(int64_t seconds) {
    return gc_clock::time_point(gc_clock::duration(seconds));
}

tombstone tomb(api::timestamp_type ts, int64_t deletion_time) {
    return tombstone(ts, tp(deletion_time));
}

atomic_cell live(const abstract_type& t, api::timestamp_type ts, bytes_view v, atomic_cell::collection_member cm = atomic_cell::collection_member::no) {
    return atomic_cell::make_live(t, ts, v, cm);
}

atomic_cell expiring(const abstract_type& t, api::timestamp_type ts, bytes_view v, int32_t ttl, int64_t expiry,
        atomic_cell::collection_member cm = atomic_cell::collection_member::no) {
    return atomic_cell::make_live(t, ts, v, tp(expiry), gc_clock::duration(ttl), cm);
}

atomic_cell dead(api::timestamp_type ts, int64_t deletion_time) {
    return atomic_cell::make_dead(ts, tp(deletion_time));
}

void set_cell(const schema& s, row& r, column_kind kind, std::string_view name, atomic_cell c) {
    const auto& cdef = *s.get_column_definition(to_bytes(name));
    BOOST_REQUIRE(cdef.kind == kind);
    r.apply(cdef, atomic_cell_or_collection(std::move(c)));
}

void set_cell(const schema& s, row& r, column_kind kind, std::string_view name, collection_mutation c) {
    const auto& cdef = *s.get_column_definition(to_bytes(name));
    BOOST_REQUIRE(cdef.kind == kind);
    r.apply(cdef, atomic_cell_or_collection(std::move(c)));
}

void set_cell(const schema& s, clustering_row& cr, std::string_view name, atomic_cell c) {
    set_cell(s, cr.cells(), column_kind::regular_column, name, std::move(c));
}

void set_cell(const schema& s, clustering_row& cr, std::string_view name, collection_mutation c) {
    set_cell(s, cr.cells(), column_kind::regular_column, name, std::move(c));
}

// A table with an int partition key, an int clustering key, and the given
// regular columns.
schema_ptr table_schema(std::vector<std::pair<std::string, data_type>> regular, std::vector<std::pair<std::string, data_type>> statics = {}) {
    schema_builder b(this_smp_shard_count(), "ks", "t");
    b.with_column("pk", int32_type, column_kind::partition_key);
    b.with_column("ck", int32_type, column_kind::clustering_key);
    for (const auto& [name, type] : statics) {
        b.with_column(to_bytes(name), type, column_kind::static_column);
    }
    for (const auto& [name, type] : regular) {
        b.with_column(to_bytes(name), type);
    }
    return b.build();
}

} // anonymous namespace

SEASTAR_THREAD_TEST_CASE(test_parquet_names) {
    BOOST_REQUIRE_EQUAL(pq::parquet_name(to_bytes("v")), "v");
    BOOST_REQUIRE_EQUAL(pq::parquet_name(to_bytes("zażółć")), "zażółć");
    BOOST_REQUIRE_EQUAL(pq::parquet_name(to_bytes("_scyll")), "_scyll");
    BOOST_REQUIRE_EQUAL(pq::parquet_name(to_bytes("x_scylla")), "x_scylla");
    BOOST_REQUIRE_EQUAL(pq::parquet_name(to_bytes("_scylla")), "_scylla_x5f7363796c6c61");
    BOOST_REQUIRE_EQUAL(pq::parquet_name(to_bytes("_scylla_x00")), "_scylla_x5f7363796c6c615f783030");
    BOOST_REQUIRE_EQUAL(pq::parquet_name(bytes()), "_scylla_x");
    const bytes not_utf8{int8_t(0x61), int8_t(0xff)};
    BOOST_REQUIRE_EQUAL(pq::parquet_name(not_utf8), "_scylla_x61ff");

    // The longest names whose Parquet names fit, plain and escaped, and
    // names one byte longer.
    auto fits = [] (std::string name, std::string field = "f", bool frozen = false) {
        auto ut = user_type_impl::get_instance("ks", to_bytes("ut"), {to_bytes(field)}, {int32_type}, !frozen);
        return pq::parquet_names_fit(*table_schema({{name, int32_type}, {"u", ut}}));
    };
    const size_t max = pq::max_name_size;
    const size_t max_escaped = (max - pq::escaped_name_prefix.size()) / 2;
    BOOST_REQUIRE(fits(std::string(max, 'a')));
    BOOST_REQUIRE(!fits(std::string(max + 1, 'a')));
    BOOST_REQUIRE(fits("_scylla" + std::string(max_escaped - 7, 'a')));
    BOOST_REQUIRE(!fits("_scylla" + std::string(max_escaped - 6, 'a')));
    BOOST_REQUIRE(!fits("v", std::string(max + 1, 'a')));
    // The fields of a frozen UDT have no Parquet names.
    BOOST_REQUIRE(fits("v", std::string(max + 1, 'a'), true));
}

// Every leaf of a table with every column shape, in schema order, and the
// leaves that a file with little data keeps.
SEASTAR_THREAD_TEST_CASE(test_schema_layout) {
    auto ut = user_type_impl::get_instance("ks", to_bytes("ut"), {to_bytes("a"), to_bytes("b")}, {int32_type, utf8_type}, true);
    auto s = schema_builder(this_smp_shard_count(), "ks", "t")
            .with_column("p1", int32_type, column_kind::partition_key)
            .with_column("p2", utf8_type, column_kind::partition_key)
            .with_column("c1", reversed_type_impl::get_instance(int32_type), column_kind::clustering_key)
            .with_column("c2", utf8_type, column_kind::clustering_key)
            .with_column("s", int32_type, column_kind::static_column)
            .with_column("v", int32_type)
            .with_column("m", map_type_impl::get_instance(int32_type, utf8_type, true))
            .with_column("st", set_type_impl::get_instance(int32_type, true))
            .with_column("l", list_type_impl::get_instance(utf8_type, true))
            .with_column("u", ut)
            .with_column("f", list_type_impl::get_instance(int32_type, false))
            .build();
    const auto header = sstables::make_serialization_header(*s, encoding_stats{});
    const auto names = pq::key_parquet_names(*s);
    const auto m = pq::table_mapping::from_header(header, names);
    std::vector<std::string> paths;
    for (auto c : m.get_schema().columns_in_schema_order()) {
        paths.push_back(fmt::format("{}", fmt::join(m.get_schema().path(c), ".")));
    }
    const std::vector<std::string> expected = {
        "p1", "p2", "c1", "c2",
        "s", "f", "l.list.element", "m.key_value.key", "m.key_value.value", "st.list.element", "u.a", "u.b", "v",
        "_scylla.kind", "_scylla.partition_tomb_ts", "_scylla.partition_tomb_deletion_time", "_scylla.ts", "_scylla.ttl",
        "_scylla.expiry", "_scylla.marker_deletion_time", "_scylla.tomb_ts", "_scylla.tomb_deletion_time",
        "_scylla.shadowable_tomb_ts", "_scylla.shadowable_tomb_deletion_time",
        "_scylla.cells.p1.empty", "_scylla.cells.c1.empty",
        "_scylla.cells.s.ts", "_scylla.cells.s.ttl", "_scylla.cells.s.expiry", "_scylla.cells.s.deletion_time", "_scylla.cells.s.empty",
        "_scylla.cells.f.ts", "_scylla.cells.f.ttl", "_scylla.cells.f.expiry", "_scylla.cells.f.deletion_time",
        "_scylla.cells.l.ts.list.element", "_scylla.cells.l.ttl.list.element", "_scylla.cells.l.expiry.list.element",
        "_scylla.cells.l.deletion_time.list.element", "_scylla.cells.l.key.list.element",
        "_scylla.cells.l.tomb_ts", "_scylla.cells.l.tomb_deletion_time",
        "_scylla.cells.m.ts.list.element", "_scylla.cells.m.ttl.list.element", "_scylla.cells.m.expiry.list.element",
        "_scylla.cells.m.deletion_time.list.element", "_scylla.cells.m.key_empty.list.element",
        "_scylla.cells.m.tomb_ts", "_scylla.cells.m.tomb_deletion_time",
        "_scylla.cells.st.ts.list.element", "_scylla.cells.st.ttl.list.element", "_scylla.cells.st.expiry.list.element",
        "_scylla.cells.st.deletion_time.list.element", "_scylla.cells.st.empty.list.element",
        "_scylla.cells.st.tomb_ts", "_scylla.cells.st.tomb_deletion_time",
        "_scylla.cells.u.fields.a.ts", "_scylla.cells.u.fields.a.ttl", "_scylla.cells.u.fields.a.expiry",
        "_scylla.cells.u.fields.a.deletion_time", "_scylla.cells.u.fields.a.empty",
        "_scylla.cells.u.fields.b.ts", "_scylla.cells.u.fields.b.ttl", "_scylla.cells.u.fields.b.expiry",
        "_scylla.cells.u.fields.b.deletion_time",
        "_scylla.cells.u.tomb_ts", "_scylla.cells.u.tomb_deletion_time",
        "_scylla.cells.v.ts", "_scylla.cells.v.ttl", "_scylla.cells.v.expiry", "_scylla.cells.v.deletion_time", "_scylla.cells.v.empty",
    };
    require_same(paths, expected);
    BOOST_REQUIRE(m.clustering_key()[0].type == int32_type);

    // A file with one tombstone-only partition keeps the key columns, null
    // or not, and the row-level columns that have values.
    test_writer w(s);
    w.partition(pk_of(*s, {int_value(1), to_bytes("a")}), tomb(10, 20));
    w.end_partition();
    arrow_file f(w.finish());
    require_same(f.paths(), (std::vector<std::string>{"p1", "p2", "c1", "c2",
            "_scylla.kind", "_scylla.partition_tomb_ts", "_scylla.partition_tomb_deletion_time"}));
    require_column(f, "c1", {"null"});
    require_column(f, "c2", {"null"});
    BOOST_REQUIRE_EQUAL(f.key_value("scylla.layout"), "1");
}

// The header is the authority: the reader computes the mapping from the
// header and the key names it finds in the file.
SEASTAR_THREAD_TEST_CASE(test_mapping_from_header) {
    auto s = schema_builder(this_smp_shard_count(), "ks", "t")
            .with_column("p1", int32_type, column_kind::partition_key)
            .with_column("p2", int32_type, column_kind::partition_key)
            .with_column("v", int32_type)
            .build();
    const auto header = sstables::make_serialization_header(*s, encoding_stats{});
    const auto types = pq::partition_key_types(header);
    BOOST_REQUIRE_EQUAL(types.size(), 2);
    BOOST_REQUIRE(types[0] == int32_type && types[1] == int32_type);
    const std::vector<sstring> renamed = {"x", "y"};
    const auto m = pq::table_mapping::from_header(header, renamed);
    BOOST_REQUIRE_EQUAL(m.partition_key()[1].parquet_name, "y");
    const std::vector<sstring> too_few = {"x"};
    BOOST_REQUIRE_THROW(pq::table_mapping::from_header(header, too_few), std::invalid_argument);

    auto single = schema_builder(this_smp_shard_count(), "ks", "t").with_column("p", utf8_type, column_kind::partition_key).build();
    const auto single_types = pq::partition_key_types(sstables::make_serialization_header(*single, encoding_stats{}));
    BOOST_REQUIRE_EQUAL(single_types.size(), 1);
    BOOST_REQUIRE(single_types[0] == utf8_type);
}

// Every native type: its Parquet type, a value, and an empty value.
SEASTAR_THREAD_TEST_CASE(test_native_types) {
    struct type_case {
        std::string name;
        data_type type;
        std::string input;
        std::string parquet_type;
        std::string stored;
        // The stored placeholder of an empty value, and whether there is an empty marker.
        std::string empty;
        bool has_marker;
        // The value, for types whose from_string() is not implemented.
        std::optional<bytes> raw = std::nullopt;

        bytes value() const {
            return raw ? *raw : serialized(type, input);
        }
    };
    auto frozen_list = list_type_impl::get_instance(int32_type, false);
    auto frozen_udt = user_type_impl::get_instance("ks", to_bytes("fu"), {to_bytes("a")}, {int32_type}, false);
    auto tuple = tuple_type_impl::get_instance({int32_type, utf8_type});
    const std::vector<type_case> cases = {
        {"c_boolean", boolean_type, "true", "BOOLEAN None NONE", "true", "false", true},
        {"c_tinyint", byte_type, "-5", "INT32 Int(bitWidth=8, isSigned=true) INT_8", "-5", "0", true},
        {"c_smallint", short_type, "-300", "INT32 Int(bitWidth=16, isSigned=true) INT_16", "-300", "0", true},
        {"c_int", int32_type, "-70000", "INT32 Int(bitWidth=32, isSigned=true) INT_32", "-70000", "0", true},
        {"c_date", simple_date_type, "1969-12-31", "INT32 Date DATE", "-1", "0", true},
        {"c_bigint", long_type, "-5000000000", "INT64 Int(bitWidth=64, isSigned=true) INT_64", "-5000000000", "0", true},
        {"c_timestamp", timestamp_type, "1609459200000",
                "INT64 Timestamp(isAdjustedToUTC=true, timeUnit=milliseconds, is_from_converted_type=false, force_set_converted_type=false) TIMESTAMP_MILLIS",
                "1609459200000", "0", true},
        {"c_time", time_type, "00:00:01.000000002", "INT64 Time(isAdjustedToUTC=false, timeUnit=nanoseconds) NONE", "1000000002", "0", true},
        {"c_float", float_type, "1.5", "FLOAT None NONE", "1.5", "0", true},
        {"c_double", double_type, "-2.25", "DOUBLE None NONE", "-2.25", "0", true},
        {"c_uuid", uuid_type, "00112233-4455-6677-8899-aabbccddeeff", "FIXED_LEN_BYTE_ARRAY(16) UUID NONE",
                "0x00112233445566778899aabbccddeeff", "0x00000000000000000000000000000000", true},
        {"c_timeuuid", timeuuid_type, "d2177dd0-eaa2-11de-a572-001b779c76e3", "FIXED_LEN_BYTE_ARRAY(16) UUID NONE",
                "0xd2177dd0eaa211dea572001b779c76e3", "0x00000000000000000000000000000000", true},
        {"c_ascii", ascii_type, "abc", "BYTE_ARRAY String UTF8", "'abc'", "''", false},
        {"c_text", utf8_type, "zażółć", "BYTE_ARRAY String UTF8", "'zażółć'", "''", false},
        {"c_blob", bytes_type, "0102", "BYTE_ARRAY None NONE", "0x0102", "0x", false},
        {"c_varint", varint_type, "-129", "BYTE_ARRAY None NONE", "0xff7f", "0x", false},
        {"c_decimal", decimal_type, "1.25", "BYTE_ARRAY None NONE", "0x000000027d", "0x", false},
        {"c_inet", inet_addr_type, "127.0.0.1", "BYTE_ARRAY None NONE", "0x7f000001", "0x", false},
        {"c_duration", duration_type, "1h", "BYTE_ARRAY None NONE", "0x0000fc068c61714000", "0x", false},
        {"c_frozen_list", frozen_list, "", "BYTE_ARRAY None NONE", "0x00000002000000040000000100000004" "00000002", "0x", false,
                frozen_list->decompose(make_list_value(frozen_list, {1, 2}))},
        {"c_frozen_udt", frozen_udt, "", "BYTE_ARRAY None NONE", "0x0000000400000007", "0x", false,
                frozen_udt->decompose(make_user_value(frozen_udt, {7}))},
        {"c_tuple", tuple, "", "BYTE_ARRAY None NONE", "0x00000004000000010000000178", "0x", false,
                tuple->decompose(make_tuple_value(tuple, {1, sstring("x")}))},
    };
    std::vector<std::pair<std::string, data_type>> columns;
    for (const auto& c : cases) {
        columns.emplace_back(c.name, c.type);
    }
    auto s = table_schema(columns);

    test_writer w(s);
    w.partition(pk_of(*s, {int_value(1)}));
    clustering_row values(ck_of(*s, {int_value(1)}));
    clustering_row empties(ck_of(*s, {int_value(2)}));
    for (const auto& c : cases) {
        set_cell(*s, values, c.name, live(*c.type, 100, c.value()));
        set_cell(*s, empties, c.name, live(*c.type, 100, bytes()));
    }
    w.row(values);
    w.row(empties);
    w.end_partition();
    arrow_file f(w.finish());

    for (const auto& c : cases) {
        BOOST_TEST_MESSAGE(c.name);
        // A byte array holds the CQL serialized bytes.
        if (!c.has_marker && c.stored[0] == '0') {
            BOOST_REQUIRE_EQUAL(c.stored, "0x" + to_hex(c.value()));
        }
        BOOST_REQUIRE_EQUAL(f.type_of(c.name), c.parquet_type);
        require_column(f, c.name, {c.stored, c.empty});
        if (c.has_marker) {
            require_column(f, "_scylla.cells." + c.name + ".empty", {"null", "true"});
        }
        require_absent(f, "_scylla.cells." + c.name + ".ts");
    }
    require_column(f, "_scylla.ts", {"100", "100"});
    require_column(f, "_scylla.kind", {"2", "2"});

    // The reader's conversion restores the CQL bytes.
    BOOST_REQUIRE_EQUAL(pq::to_cql_value(pq::value_type::boolean, true), serialized(boolean_type, "true"));
    BOOST_REQUIRE_EQUAL(pq::to_cql_value(pq::value_type::tinyint, int32_t(-5)), serialized(byte_type, "-5"));
    BOOST_REQUIRE_EQUAL(pq::to_cql_value(pq::value_type::smallint, int32_t(-300)), serialized(short_type, "-300"));
    BOOST_REQUIRE_EQUAL(pq::to_cql_value(pq::value_type::int32, int32_t(-70000)), serialized(int32_type, "-70000"));
    BOOST_REQUIRE_EQUAL(pq::to_cql_value(pq::value_type::date, int32_t(-1)), serialized(simple_date_type, "1969-12-31"));
    BOOST_REQUIRE_EQUAL(pq::to_cql_value(pq::value_type::bigint, int64_t(-5000000000)), serialized(long_type, "-5000000000"));
    BOOST_REQUIRE_EQUAL(pq::to_cql_value(pq::value_type::timestamp, int64_t(1609459200000)), serialized(timestamp_type, "1609459200000"));
    BOOST_REQUIRE_EQUAL(pq::to_cql_value(pq::value_type::time, int64_t(1000000002)), serialized(time_type, "00:00:01.000000002"));
    BOOST_REQUIRE_EQUAL(pq::to_cql_value(pq::value_type::float_, 1.5f), serialized(float_type, "1.5"));
    BOOST_REQUIRE_EQUAL(pq::to_cql_value(pq::value_type::double_, -2.25), serialized(double_type, "-2.25"));
}

// Value types of the types that a column can have, with reversal removed.
SEASTAR_THREAD_TEST_CASE(test_value_types) {
    BOOST_REQUIRE(pq::value_type_of(*reversed_type_impl::get_instance(timestamp_type)) == pq::value_type::timestamp);
    BOOST_REQUIRE(pq::value_type_of(*date_type) == pq::value_type::timestamp);
    BOOST_REQUIRE(pq::value_type_of(*empty_type) == pq::value_type::bytes);
    BOOST_REQUIRE(pq::value_type_of(*list_type_impl::get_instance(int32_type, false)) == pq::value_type::bytes);
    // A date at both ends of its range, and NaN payloads, survive.
    BOOST_REQUIRE_EQUAL(pq::to_cql_value(pq::value_type::date, std::numeric_limits<int32_t>::min()), bytes(4, int8_t(0)));
    BOOST_REQUIRE_EQUAL(pq::to_cql_value(pq::value_type::date, std::numeric_limits<int32_t>::max()), bytes(4, int8_t(0xff)));
    const bytes nan{int8_t(0x7f), int8_t(0xf0), 0, 0, 0, 0, 0, 1};
    BOOST_REQUIRE_EQUAL(pq::to_cql_value(pq::value_type::double_, std::bit_cast<double>(uint64_t(0x7ff0000000000001))), nan);
}

// Partition keys and tombstones on the first row of each partition, the
// kinds of rows, and partitions without fragments.
SEASTAR_THREAD_TEST_CASE(test_partitions) {
    auto s = table_schema({{"v", int32_type}}, {{"s", int32_type}});
    test_writer w(s);
    // Only a partition tombstone: one row of kind "none".
    w.partition(pk_of(*s, {int_value(1)}), tomb(10, 1000));
    w.end_partition();
    // Nothing at all: one row of kind "none" too.
    w.partition(pk_of(*s, {int_value(2)}));
    w.end_partition();
    // A static row and two clustering rows, with a partition tombstone.
    w.partition(pk_of(*s, {int_value(3)}), tomb(5, 1001));
    {
        ::static_row sr;
        set_cell(*s, sr.cells(), column_kind::static_column, "s", live(*int32_type, 20, int_value(30)));
        w.static_row(sr);
    }
    for (int i = 0; i < 2; ++i) {
        clustering_row cr(ck_of(*s, {int_value(i)}));
        set_cell(*s, cr, "v", live(*int32_type, 20, int_value(i)));
        w.row(cr);
    }
    w.end_partition();
    // An empty static row has no row, so the clustering row comes first.
    w.partition(pk_of(*s, {int_value(4)}));
    w.static_row(::static_row());
    {
        clustering_row cr(ck_of(*s, {int_value(7)}));
        cr.apply(row_marker(30));
        w.row(cr);
    }
    w.end_partition();
    arrow_file f(w.finish());

    require_column(f, "_scylla.kind", {"0", "0", "1", "2", "2", "3"});
    require_column(f, "pk", {"1", "2", "3", "null", "null", "4"});
    require_column(f, "ck", {"null", "null", "null", "0", "1", "7"});
    require_column(f, "_scylla.partition_tomb_ts", {"10", "null", "5", "null", "null", "null"});
    require_column(f, "_scylla.partition_tomb_deletion_time", {"1000", "null", "1001", "null", "null", "null"});
    require_column(f, "_scylla.ts", {"null", "null", "20", "20", "20", "30"});
    require_column(f, "s", {"null", "null", "30", "null", "null", "null"});
    require_column(f, "v", {"null", "null", "null", "0", "1", "null"});
    require_absent(f, "_scylla.ttl");
    require_absent(f, "_scylla.cells.v.ts");
}

// The row marker in its four states, and the defaults that it supplies.
SEASTAR_THREAD_TEST_CASE(test_markers) {
    auto s = table_schema({{"v", int32_type}});
    test_writer w(s);
    w.partition(pk_of(*s, {int_value(1)}));
    // Missing: the policy picks the defaults from the cells.
    {
        clustering_row cr(ck_of(*s, {int_value(0)}));
        set_cell(*s, cr, "v", expiring(*int32_type, 7, int_value(0), 100, 5000));
        w.row(cr);
    }
    // Live.
    {
        clustering_row cr(ck_of(*s, {int_value(1)}));
        cr.apply(row_marker(10));
        set_cell(*s, cr, "v", live(*int32_type, 10, int_value(1)));
        w.row(cr);
    }
    // Live with a TTL.
    {
        clustering_row cr(ck_of(*s, {int_value(2)}));
        cr.apply(row_marker(11, gc_clock::duration(3600), tp(9000)));
        set_cell(*s, cr, "v", expiring(*int32_type, 11, int_value(2), 3600, 9000));
        w.row(cr);
    }
    // Dead: the cells supply the TTL defaults.
    {
        clustering_row cr(ck_of(*s, {int_value(3)}));
        cr.apply(row_marker(tomb(12, 4000)));
        set_cell(*s, cr, "v", expiring(*int32_type, 13, int_value(3), 60, 4500));
        w.row(cr);
    }
    // Dead, without cells.
    {
        clustering_row cr(ck_of(*s, {int_value(4)}));
        cr.apply(row_marker(tomb(14, 4100)));
        w.row(cr);
    }
    w.end_partition();
    arrow_file f(w.finish());

    require_column(f, "_scylla.kind", {"2", "3", "3", "3", "3"});
    require_column(f, "_scylla.ts", {"7", "10", "11", "12", "14"});
    require_column(f, "_scylla.ttl", {"100", "null", "3600", "60", "null"});
    require_column(f, "_scylla.expiry", {"5000", "null", "9000", "4500", "null"});
    require_column(f, "_scylla.marker_deletion_time", {"null", "null", "null", "4000", "4100"});
    require_column(f, "v", {"0", "1", "2", "3", "null"});
    require_column(f, "_scylla.cells.v.ts", {"null", "null", "null", "1", "null"});
    require_absent(f, "_scylla.cells.v.ttl");
    require_absent(f, "_scylla.cells.v.expiry");
}

// Every row of the truth table of cell TTLs and expiry times, and
// timestamp differences that wrap around.
SEASTAR_THREAD_TEST_CASE(test_cell_metadata) {
    auto s = table_schema({{"a", int32_type}, {"b", int32_type}, {"c", int32_type}, {"d", int32_type}, {"e", int32_type}, {"f", int32_type}});
    test_writer w(s);
    w.partition(pk_of(*s, {int_value(1)}));
    // The row has the default TTL 100 and expiry 5000.
    {
        clustering_row cr(ck_of(*s, {int_value(0)}));
        cr.apply(row_marker(50, gc_clock::duration(100), tp(5000)));
        set_cell(*s, cr, "a", live(*int32_type, 50, int_value(1)));                  // does not expire
        set_cell(*s, cr, "b", expiring(*int32_type, 50, int_value(2), 100, 5000));   // the row's TTL and expiry
        set_cell(*s, cr, "c", expiring(*int32_type, 51, int_value(3), 200, 5000));   // another TTL
        set_cell(*s, cr, "d", expiring(*int32_type, 49, int_value(4), 100, 6000));   // another expiry
        set_cell(*s, cr, "e", expiring(*int32_type, 50, int_value(5), 300, 7000));   // both differ
        set_cell(*s, cr, "f", dead(52, 8000));
        w.row(cr);
    }
    // The row has no default TTL.
    {
        clustering_row cr(ck_of(*s, {int_value(1)}));
        cr.apply(row_marker(60));
        set_cell(*s, cr, "a", live(*int32_type, 60, int_value(1)));                  // does not expire
        set_cell(*s, cr, "b", expiring(*int32_type, 60, int_value(2), 100, 5000));   // expires
        set_cell(*s, cr, "f", dead(60, 8000));
        w.row(cr);
    }
    // Timestamp differences wrap around.
    {
        clustering_row cr(ck_of(*s, {int_value(2)}));
        cr.apply(row_marker(api::max_timestamp));
        set_cell(*s, cr, "a", live(*int32_type, api::min_timestamp, int_value(1)));
        set_cell(*s, cr, "b", live(*int32_type, -1, int_value(1)));
        w.row(cr);
    }
    w.end_partition();
    arrow_file f(w.finish());

    require_column(f, "_scylla.ts", {"50", "60", fmt::to_string(api::max_timestamp)});
    require_column(f, "_scylla.ttl", {"100", "null", "null"});
    require_column(f, "_scylla.expiry", {"5000", "null", "null"});
    require_column(f, "a", {"1", "1", "1"});
    // A column with only dead cells has no values.
    require_absent(f, "f");
    require_column(f, "_scylla.cells.a.ttl", {"0", "null", "null"});
    require_absent(f, "_scylla.cells.a.expiry");
    require_column(f, "_scylla.cells.b.ttl", {"null", "100", "null"});
    require_column(f, "_scylla.cells.b.expiry", {"null", "5000", "null"});
    require_column(f, "_scylla.cells.c.ttl", {"200", "null", "null"});
    require_absent(f, "_scylla.cells.c.expiry");
    require_absent(f, "_scylla.cells.d.ttl");
    require_column(f, "_scylla.cells.d.expiry", {"6000", "null", "null"});
    require_column(f, "_scylla.cells.e.ttl", {"300", "null", "null"});
    require_column(f, "_scylla.cells.e.expiry", {"7000", "null", "null"});
    require_absent(f, "_scylla.cells.f.ttl");
    require_column(f, "_scylla.cells.f.deletion_time", {"8000", "8000", "null"});
    require_column(f, "_scylla.cells.c.ts", {"1", "null", "null"});
    require_column(f, "_scylla.cells.d.ts", {"-1", "null", "null"});
    require_column(f, "_scylla.cells.f.ts", {"2", "null", "null"});
    // min - max wraps to 1 + 1 = 2 (api::min_timestamp is -max + 1), and -1 - max wraps to min.
    require_column(f, "_scylla.cells.a.ts", {"null", "null", "2"});
    require_column(f, "_scylla.cells.b.ts", {"null", "null", fmt::to_string(std::numeric_limits<int64_t>::min())});
    require_absent(f, "_scylla.cells.b.deletion_time");
}

// Row tombstones and shadowable tombstones, with timestamps equal to and
// different from the row's.
SEASTAR_THREAD_TEST_CASE(test_row_tombstones) {
    auto s = table_schema({{"v", int32_type}});
    test_writer w(s);
    w.partition(pk_of(*s, {int_value(1)}));
    {
        // Only a tombstone: the default policy takes its timestamp.
        clustering_row cr(ck_of(*s, {int_value(0)}));
        cr.apply(tomb(40, 900));
        w.row(cr);
    }
    {
        clustering_row cr(ck_of(*s, {int_value(1)}));
        cr.apply(row_marker(50));
        cr.apply(tomb(45, 901));
        w.row(cr);
    }
    {
        clustering_row cr(ck_of(*s, {int_value(2)}));
        cr.apply(row_marker(30));
        cr.apply(shadowable_tombstone(tomb(60, 902)));
        w.row(cr);
    }
    {
        clustering_row cr(ck_of(*s, {int_value(3)}));
        cr.apply(row_marker(70));
        cr.apply(tomb(65, 903));
        cr.apply(shadowable_tombstone(tomb(70, 904)));
        w.row(cr);
    }
    w.end_partition();
    arrow_file f(w.finish());

    require_column(f, "_scylla.ts", {"40", "50", "30", "70"});
    require_column(f, "_scylla.tomb_ts", {"null", "-5", "null", "-5"});
    require_column(f, "_scylla.tomb_deletion_time", {"900", "901", "null", "903"});
    require_column(f, "_scylla.shadowable_tomb_ts", {"null", "null", "30", "null"});
    require_column(f, "_scylla.shadowable_tomb_deletion_time", {"null", "null", "902", "904"});
}

// Range tombstone bounds, a boundary, prefixes, and empty clustering key components.
SEASTAR_THREAD_TEST_CASE(test_range_tombstones) {
    auto s = schema_builder(this_smp_shard_count(), "ks", "t")
            .with_column("pk", int32_type, column_kind::partition_key)
            .with_column("c1", int32_type, column_kind::clustering_key)
            .with_column("c2", utf8_type, column_kind::clustering_key)
            .with_column("v", int32_type)
            .build();
    test_writer w(s);
    w.partition(pk_of(*s, {int_value(1)}));
    // A tombstone from the start of the partition to before (1), a row with
    // empty key components under it, a boundary before (1) to another
    // tombstone, a boundary after (2, 'x') to a third one, which ends at the
    // end of the partition.
    w.rt(range_tombstone_change(position_in_partition::before_all_clustered_rows(), tomb(10, 100)));
    {
        clustering_row cr(ck_of(*s, {bytes(), to_bytes("")}));
        cr.apply(row_marker(30));
        w.row(cr);
    }
    w.rt(range_tombstone_change(position_in_partition::before_key(ck_of(*s, {int_value(1)})), tomb(20, 200)));
    w.rt(range_tombstone_change(position_in_partition::after_key(*s, ck_of(*s, {int_value(2), to_bytes("x")})), tomb(25, 250)));
    w.rt(range_tombstone_change(position_in_partition::after_all_clustered_rows(), tombstone()));
    w.end_partition();
    arrow_file f(w.finish());

    require_column(f, "_scylla.kind", {"4", "3", "7", "4", "6", "5", "6"});
    require_column(f, "c1", {"null", "0", "1", "1", "2", "2", "null"});
    require_column(f, "c2", {"null", "''", "null", "null", "'x'", "'x'", "null"});
    require_column(f, "_scylla.cells.c1.empty", {"null", "true", "null", "null", "null", "null", "null"});
    require_absent(f, "_scylla.cells.c2.empty");
    require_column(f, "_scylla.ts", {"10", "30", "10", "20", "20", "25", "25"});
    require_column(f, "_scylla.tomb_deletion_time", {"100", "null", "100", "200", "200", "250", "250"});
    require_absent(f, "_scylla.tomb_ts");
    require_absent(f, "_scylla.ttl");
    require_column(f, "pk", {"1", "null", "null", "null", "null", "null", "null"});
}

// Empty values in partition keys.
SEASTAR_THREAD_TEST_CASE(test_empty_partition_key_components) {
    auto composite = schema_builder(this_smp_shard_count(), "ks", "t")
            .with_column("p1", int32_type, column_kind::partition_key)
            .with_column("p2", uuid_type, column_kind::partition_key)
            .with_column("v", int32_type)
            .build();
    test_writer w(composite);
    w.partition(pk_of(*composite, {bytes(), bytes()}), tomb(1, 2));
    w.end_partition();
    w.partition(pk_of(*composite, {int_value(3), serialized(uuid_type, "00112233-4455-6677-8899-aabbccddeeff")}), tomb(1, 2));
    w.end_partition();
    arrow_file f(w.finish());
    require_column(f, "p1", {"0", "3"});
    require_column(f, "p2", {"0x00000000000000000000000000000000", "0x00112233445566778899aabbccddeeff"});
    require_column(f, "_scylla.cells.p1.empty", {"true", "null"});
    require_column(f, "_scylla.cells.p2.empty", {"true", "null"});

    // A single-column text key, as in a secondary index.
    auto single = schema_builder(this_smp_shard_count(), "ks", "t").with_column("p", utf8_type, column_kind::partition_key).with_column("v", int32_type).build();
    test_writer w2(single);
    w2.partition(pk_of(*single, {bytes()}), tomb(1, 2));
    w2.end_partition();
    arrow_file f2(w2.finish());
    require_column(f2, "p", {"''"});
    require_absent(f2, "_scylla.cells.p.empty");
}

// Non-frozen maps, sets, and lists.
SEASTAR_THREAD_TEST_CASE(test_collections) {
    auto map_type = map_type_impl::get_instance(int32_type, int32_type, true);
    auto set_type = set_type_impl::get_instance(int32_type, true);
    auto list_type = list_type_impl::get_instance(utf8_type, true);
    auto s = table_schema({{"m", map_type}, {"st", set_type}, {"l", list_type}});
    const auto yes = atomic_cell::collection_member::yes;
    const bytes key1 = timeuuid_type->decompose(utils::UUID("d2177dd0-eaa2-11de-a572-001b779c76e3"));
    const bytes key2 = timeuuid_type->decompose(utils::UUID("d2177dd1-eaa2-11de-a572-001b779c76e3"));
    test_writer w(s);
    w.partition(pk_of(*s, {int_value(1)}));
    // Every element live with the row's defaults: no metadata lists.
    {
        clustering_row cr(ck_of(*s, {int_value(0)}));
        cr.apply(row_marker(10));
        collection_mutation_writer m(tombstone{});
        m.push_back(managed_bytes(int_value(1)), live(*int32_type, 10, int_value(11), yes));
        m.push_back(managed_bytes(int_value(2)), live(*int32_type, 10, int_value(12), yes));
        set_cell(*s, cr, "m", std::move(m).finish());
        collection_mutation_writer st(tombstone{});
        st.push_back(managed_bytes(int_value(5)), live(*bytes_type, 10, bytes(), yes));
        set_cell(*s, cr, "st", std::move(st).finish());
        collection_mutation_writer l(tombstone{});
        l.push_back(managed_bytes(key1), live(*utf8_type, 10, to_bytes("a"), yes));
        set_cell(*s, cr, "l", std::move(l).finish());
        w.row(cr);
    }
    // Tombstones, dead elements, empty keys and values, and per-element metadata.
    {
        clustering_row cr(ck_of(*s, {int_value(1)}));
        cr.apply(row_marker(20));
        collection_mutation_writer m(tomb(19, 500));
        m.push_back(managed_bytes(bytes()), live(*int32_type, 20, int_value(13), yes));
        m.push_back(managed_bytes(int_value(1)), dead(21, 600));
        m.push_back(managed_bytes(int_value(2)), expiring(*int32_type, 20, bytes(), 50, 700, yes));
        set_cell(*s, cr, "m", std::move(m).finish());
        collection_mutation_writer st(tomb(19, 500));
        st.push_back(managed_bytes(bytes()), live(*bytes_type, 22, bytes(), yes));
        st.push_back(managed_bytes(int_value(6)), dead(20, 601));
        set_cell(*s, cr, "st", std::move(st).finish());
        collection_mutation_writer l(tombstone{});
        l.push_back(managed_bytes(key1), dead(20, 602));
        l.push_back(managed_bytes(key2), live(*utf8_type, 20, bytes(), yes));
        set_cell(*s, cr, "l", std::move(l).finish());
        w.row(cr);
    }
    // Only a collection tombstone.
    {
        clustering_row cr(ck_of(*s, {int_value(2)}));
        cr.apply(row_marker(30));
        set_cell(*s, cr, "l", collection_mutation_writer(tomb(29, 800)).finish());
        w.row(cr);
    }
    w.end_partition();
    arrow_file f(w.finish());

    BOOST_REQUIRE_EQUAL(f.type_of("m.key_value.key"), "INT32 Int(bitWidth=32, isSigned=true) INT_32");
    BOOST_REQUIRE_EQUAL(f.type_of("l.list.element"), "BYTE_ARRAY String UTF8");
    BOOST_REQUIRE_EQUAL(f.type_of("_scylla.cells.l.key.list.element"), "FIXED_LEN_BYTE_ARRAY(16) UUID NONE");

    require_column(f, "m.key_value.key", {"[1, 2]", "[0, 1, 2]", "null"});
    require_column(f, "m.key_value.value", {"[11, 12]", "[13, null, 0]", "null"});
    require_column(f, "_scylla.cells.m.key_empty.list.element", {"null", "[true, null, null]", "null"});
    require_column(f, "_scylla.cells.m.empty.list.element", {"null", "[null, null, true]", "null"});
    require_column(f, "_scylla.cells.m.ts.list.element", {"null", "[null, 1, null]", "null"});
    require_column(f, "_scylla.cells.m.ttl.list.element", {"null", "[null, null, 50]", "null"});
    require_column(f, "_scylla.cells.m.expiry.list.element", {"null", "[null, null, 700]", "null"});
    require_column(f, "_scylla.cells.m.deletion_time.list.element", {"null", "[null, 600, null]", "null"});
    require_column(f, "_scylla.cells.m.tomb_ts", {"null", "-1", "null"});
    require_column(f, "_scylla.cells.m.tomb_deletion_time", {"null", "500", "null"});

    require_column(f, "st.list.element", {"[5]", "[0, 6]", "null"});
    require_column(f, "_scylla.cells.st.empty.list.element", {"null", "[true, null]", "null"});
    require_column(f, "_scylla.cells.st.ts.list.element", {"null", "[2, null]", "null"});
    require_column(f, "_scylla.cells.st.deletion_time.list.element", {"null", "[null, 601]", "null"});
    require_column(f, "_scylla.cells.st.tomb_ts", {"null", "-1", "null"});

    require_column(f, "l.list.element", {"['a']", "[null, '']", "null"});
    require_column(f, "_scylla.cells.l.key.list.element", {"[0xd2177dd0eaa211dea572001b779c76e3]",
            "[0xd2177dd0eaa211dea572001b779c76e3, 0xd2177dd1eaa211dea572001b779c76e3]", "null"});
    require_column(f, "_scylla.cells.l.deletion_time.list.element", {"null", "[602, null]", "null"});
    require_column(f, "_scylla.cells.l.tomb_ts", {"null", "null", "-1"});
    require_column(f, "_scylla.cells.l.tomb_deletion_time", {"null", "null", "800"});
    require_absent(f, "_scylla.cells.l.ts.list.element");
    require_absent(f, "_scylla.cells.l.ttl.list.element");
    require_absent(f, "_scylla.cells.st.ttl.list.element");
}

// A non-frozen UDT: a group of fields with the metadata of atomic cells.
SEASTAR_THREAD_TEST_CASE(test_udt) {
    auto ut = user_type_impl::get_instance("ks", to_bytes("ut"), {to_bytes("a"), to_bytes("_scylla"), to_bytes("c")},
            {int32_type, utf8_type, set_type_impl::get_instance(int32_type, false)}, true);
    auto s = table_schema({{"u", ut}});
    const auto yes = atomic_cell::collection_member::yes;
    test_writer w(s);
    w.partition(pk_of(*s, {int_value(1)}));
    {
        clustering_row cr(ck_of(*s, {int_value(0)}));
        cr.apply(row_marker(10));
        collection_mutation_writer u(tomb(9, 100));
        u.push_back(managed_bytes(serialize_field_index(0)), live(*int32_type, 10, bytes(), yes));
        u.push_back(managed_bytes(serialize_field_index(1)), dead(11, 200));
        set_cell(*s, cr, "u", std::move(u).finish());
        w.row(cr);
    }
    {
        clustering_row cr(ck_of(*s, {int_value(1)}));
        cr.apply(row_marker(20));
        collection_mutation_writer u(tombstone{});
        u.push_back(managed_bytes(serialize_field_index(1)), expiring(*utf8_type, 20, to_bytes("x"), 5, 300, yes));
        set_cell(*s, cr, "u", std::move(u).finish());
        w.row(cr);
    }
    w.end_partition();
    arrow_file f(w.finish());

    BOOST_REQUIRE_EQUAL(f.type_of("u.a"), "INT32 Int(bitWidth=32, isSigned=true) INT_32");
    require_column(f, "u.a", {"0", "null"});
    require_column(f, "u._scylla_x5f7363796c6c61", {"null", "'x'"});
    require_absent(f, "u.c");
    require_column(f, "_scylla.cells.u.fields.a.empty", {"true", "null"});
    require_column(f, "_scylla.cells.u.fields._scylla_x5f7363796c6c61.deletion_time", {"200", "null"});
    require_column(f, "_scylla.cells.u.fields._scylla_x5f7363796c6c61.ts", {"1", "null"});
    require_column(f, "_scylla.cells.u.fields._scylla_x5f7363796c6c61.ttl", {"null", "5"});
    require_column(f, "_scylla.cells.u.fields._scylla_x5f7363796c6c61.expiry", {"null", "300"});
    require_column(f, "_scylla.cells.u.tomb_ts", {"-1", "null"});
    require_column(f, "_scylla.cells.u.tomb_deletion_time", {"100", "null"});
}

// Counters: shard lists, a dead counter, and a counter without shards.
SEASTAR_THREAD_TEST_CASE(test_counters) {
    auto s = schema_builder(this_smp_shard_count(), "ks", "t")
            .with_column("pk", int32_type, column_kind::partition_key)
            .with_column("ck", int32_type, column_kind::clustering_key)
            .with_column("c", counter_type)
            .build();
    const auto id1 = counter_id(utils::UUID("00000000-0000-0000-0000-000000000001"));
    const auto id2 = counter_id(utils::UUID("00000000-0000-0000-0000-000000000002"));
    test_writer w(s);
    w.partition(pk_of(*s, {int_value(1)}));
    {
        clustering_row cr(ck_of(*s, {int_value(0)}));
        counter_cell_builder b;
        b.add_shard(counter_shard(id1, 5, 1));
        b.add_shard(counter_shard(id2, -3, 7));
        set_cell(*s, cr, "c", b.build(10));
        w.row(cr);
    }
    {
        clustering_row cr(ck_of(*s, {int_value(1)}));
        set_cell(*s, cr, "c", dead(11, 400));
        w.row(cr);
    }
    {
        clustering_row cr(ck_of(*s, {int_value(2)}));
        set_cell(*s, cr, "c", counter_cell_builder().build(12));
        w.row(cr);
    }
    w.end_partition();
    arrow_file f(w.finish());

    BOOST_REQUIRE_EQUAL(f.type_of("c.list.element"), "INT64 None NONE");
    require_column(f, "c.list.element", {"[5, -3]", "null", "[]"});
    require_column(f, "_scylla.cells.c.shard_id.list.element", {
            "[0x00000000000000000000000000000001, 0x00000000000000000000000000000002]", "null", "[]"});
    require_column(f, "_scylla.cells.c.clock.list.element", {"[1, 7]", "null", "[]"});
    require_column(f, "_scylla.cells.c.deletion_time", {"null", "400", "null"});
    require_column(f, "_scylla.ts", {"10", "11", "12"});
    require_absent(f, "_scylla.cells.c.ts");
    require_absent(f, "_scylla.cells.c.ttl");
}

// Column names that need the escape rule.
// Column names that need the escape rule. A table cannot have a column
// name that is not valid UTF-8 (the schema rejects it), so test_parquet_names
// covers that case.
SEASTAR_THREAD_TEST_CASE(test_escaped_column_names) {
    auto s = schema_builder(this_smp_shard_count(), "ks", "t")
            .with_column(to_bytes("_scylla"), int32_type, column_kind::partition_key)
            .with_column(to_bytes("_scylla_x"), int32_type, column_kind::clustering_key)
            .with_column(bytes(), int32_type)
            .with_column(to_bytes("_scyllaz"), utf8_type)
            .with_column(to_bytes("ok"), int32_type)
            .build();
    test_writer w(s);
    w.partition(pk_of(*s, {int_value(1)}));
    {
        clustering_row cr(ck_of(*s, {int_value(2)}));
        set_cell(*s, cr, "", live(*int32_type, 1, int_value(3)));
        set_cell(*s, cr, "_scyllaz", live(*utf8_type, 2, to_bytes("v")));
        set_cell(*s, cr, "ok", live(*int32_type, 1, bytes()));
        w.row(cr);
    }
    w.end_partition();
    arrow_file f(w.finish());
    require_same(f.paths(), (std::vector<std::string>{
            "_scylla_x5f7363796c6c61", "_scylla_x5f7363796c6c615f78", "_scylla_x", "_scylla_x5f7363796c6c617a", "ok",
            "_scylla.kind", "_scylla.ts", "_scylla.cells._scylla_x5f7363796c6c617a.ts", "_scylla.cells.ok.empty"}));
    require_column(f, "_scylla_x5f7363796c6c61", {"1"});
    require_column(f, "_scylla_x", {"3"});
    require_column(f, "_scylla_x5f7363796c6c617a", {"'v'"});
    require_column(f, "_scylla.cells._scylla_x5f7363796c6c617a.ts", {"1"});
}

namespace {

// Converts mutations to fragments and writes them with `w`.
void write_mutations(test_writer& w, schema_ptr s, utils::chunked_vector<mutation> muts) {
    tests::reader_concurrency_semaphore_wrapper semaphore;
    auto reader = make_mutation_reader_from_mutations(s, semaphore.make_permit(), std::move(muts));
    auto close = deferred_close(reader);
    while (auto mf = reader().get()) {
        switch (mf->mutation_fragment_kind()) {
        case mutation_fragment_v2::kind::partition_start:
            w.partition(mf->as_partition_start().key().key(), mf->as_partition_start().partition_tombstone());
            break;
        case mutation_fragment_v2::kind::static_row:
            w.static_row(mf->as_static_row());
            break;
        case mutation_fragment_v2::kind::clustering_row:
            w.row(mf->as_clustering_row());
            break;
        case mutation_fragment_v2::kind::range_tombstone_change:
            w.rt(mf->as_range_tombstone_change());
            break;
        case mutation_fragment_v2::kind::partition_end:
            w.end_partition();
            break;
        }
    }
}

// Checks the invariants of the mapping on a file, through Arrow.
// `partition_starts` holds the ordinals of the first rows of the partitions.
void check_invariants(arrow_file& f, const schema& s, const std::vector<uint64_t>& partition_starts) {
    const auto n = f.num_rows();
    // The first partition key column is non-null exactly on the rows that
    // start a partition.
    const auto pk = f.column(pq::parquet_name(s.partition_key_columns().begin()->name()));
    std::vector<uint64_t> non_null;
    for (int64_t i = 0; i < n; ++i) {
        if (pk[i] != "null") {
            non_null.push_back(i);
        }
    }
    require_same(non_null, partition_starts);
    const auto kinds = f.column("_scylla.kind");
    const auto ts = f.column("_scylla.ts");
    std::vector<rows> ck;
    for (const auto& c : s.clustering_key_columns()) {
        ck.push_back(f.column(pq::parquet_name(c.name())));
    }
    for (int64_t i = 0; i < n; ++i) {
        // `kind` is non-null on every row, and `ts` on every row but kind "none".
        BOOST_REQUIRE_NE(kinds[i], "null");
        const auto kind = pq::fragment_kind(std::stoi(kinds[i]));
        BOOST_REQUIRE(kind >= pq::fragment_kind::none && kind <= pq::fragment_kind::excl_end);
        BOOST_REQUIRE_EQUAL(ts[i] == "null", kind == pq::fragment_kind::none);
        // A non-null clustering component never follows a null one.
        bool null_seen = false;
        for (const auto& c : ck) {
            if (c[i] == "null") {
                null_seen = true;
            } else {
                BOOST_REQUIRE(!null_seen);
            }
        }
        if (kind == pq::fragment_kind::none || kind == pq::fragment_kind::static_row) {
            for (const auto& c : ck) {
                BOOST_REQUIRE_EQUAL(c[i], "null");
            }
        }
        if (kind == pq::fragment_kind::clustering_row || kind == pq::fragment_kind::clustering_row_with_marker) {
            BOOST_REQUIRE(!null_seen);
        }
    }
    // Metadata lists are null or as long as their value list.
    for (const auto& c : s.regular_columns()) {
        if (!c.type->is_multi_cell() || c.type->is_user_type()) {
            continue;
        }
        const auto name = pq::parquet_name(c.name());
        const auto value_path = c.type->is_map() ? name + ".key_value.key" : name + ".list.element";
        if (!f.has(value_path)) {
            continue;
        }
        const auto values = f.read(value_path);
        auto lengths = [] (const arrow_file::entries& e) {
            std::vector<int64_t> r;
            for (size_t i = 0; i < e.defs.size(); ++i) {
                if (e.reps[i] == 0) {
                    r.push_back(e.defs[i] == 0 ? -1 : 0);
                }
                if (e.defs[i] >= 2) {
                    ++r.back();
                }
            }
            return r;
        };
        const auto value_lengths = lengths(values);
        for (const char* leaf : {"ts", "ttl", "expiry", "deletion_time", "empty", "key_empty", "key"}) {
            const auto path = fmt::format("_scylla.cells.{}.{}.list.element", name, leaf);
            if (!f.has(path)) {
                continue;
            }
            const auto meta_lengths = lengths(f.read(path));
            BOOST_REQUIRE_EQUAL(meta_lengths.size(), value_lengths.size());
            for (size_t i = 0; i < meta_lengths.size(); ++i) {
                BOOST_REQUIRE_MESSAGE(meta_lengths[i] == -1 || meta_lengths[i] == value_lengths[i], path);
                // The writer never writes an empty list.
                BOOST_REQUIRE_NE(meta_lengths[i], 0);
            }
        }
        for (auto l : value_lengths) {
            BOOST_REQUIRE_NE(l, 0);
        }
    }
}

void test_random_mutations(random_mutation_generator::generate_counters counters, uint32_t seed) {
    random_mutation_generator gen(counters, local_shard_only::yes, random_mutation_generator::generate_uncompactable::no, seed);
    auto s = gen.schema();
    auto muts = gen(10);
    std::mt19937 rng(seed);
    for (bool random_policy : {false, true}) {
        // Small page sets and row groups, so that the file has several of each.
        pq::writer_config cfg{.row_group_size = 64 << 10, .page_set_size = 4 << 10, .min_page_size = 1 << 10};
        test_writer w(s, random_policy ? pq::make_random_row_defaults_policy(rng()) : nullptr, cfg);
        write_mutations(w, s, muts);
        const auto data = w.finish();
        arrow_file f(data);
        // Arrow reads every column.
        for (const auto& path : f.paths()) {
            f.read(path);
        }
        BOOST_REQUIRE_EQUAL(w.partition_starts.size(), muts.size());
        check_invariants(f, *s, w.partition_starts);
    }
}

} // anonymous namespace

SEASTAR_THREAD_TEST_CASE(test_random_mutations_smoke) {
    for (uint32_t seed : {1, 2, 3}) {
        test_random_mutations(random_mutation_generator::generate_counters::no, seed);
    }
    test_random_mutations(random_mutation_generator::generate_counters::yes, 4);
}

// A row with many collection elements and no marker gives the policy a
// candidate per element. Neither the candidates nor the pages need a
// contiguous allocation above 128 KiB.
SEASTAR_THREAD_TEST_CASE(test_many_elements_without_large_allocations) {
    auto set_type = set_type_impl::get_instance(int32_type, true);
    auto s = table_schema({{"st", set_type}});
    const auto yes = atomic_cell::collection_member::yes;
    test_writer w(s);
    w.partition(pk_of(*s, {int_value(1)}));
    clustering_row cr(ck_of(*s, {int_value(0)}));
    collection_mutation_writer st(tombstone{});
    const int n = 40000;
    for (int i = 0; i < n; ++i) {
        st.push_back(managed_bytes(int_value(i)), live(*bytes_type, 10 + i % 2, bytes(), yes));
    }
    set_cell(*s, cr, "st", std::move(st).finish());
    {
        const auto before = seastar::memory::stats().large_allocations();
        const seastar::memory::scoped_large_allocation_warning_threshold guard((128 << 10) + 1);
        w.row(cr);
        BOOST_REQUIRE_EQUAL(seastar::memory::stats().large_allocations(), before);
    }
    w.end_partition();
    arrow_file f(w.finish());
    BOOST_REQUIRE_EQUAL(f.type_of("st.list.element"), "INT32 Int(bitWidth=32, isSigned=true) INT_32");
}

// The default policy picks the majority timestamp and expiration.
SEASTAR_THREAD_TEST_CASE(test_default_policy) {
    auto p = pq::make_default_row_defaults_policy();
    const utils::chunked_vector<api::timestamp_type> ts = {5, 7, 5, 9, 5};
    BOOST_REQUIRE_EQUAL(p->pick_timestamp(ts), 5);
    // Without candidates, the previous pick.
    BOOST_REQUIRE_EQUAL(p->pick_timestamp({}), 5);
    const utils::chunked_vector<std::optional<pq::expiration>> e = {std::nullopt, pq::expiration{10, 20}, pq::expiration{10, 20}};
    BOOST_REQUIRE(p->pick_expiration(e) == pq::expiration(10, 20));
    BOOST_REQUIRE(!p->pick_expiration({}));
}
