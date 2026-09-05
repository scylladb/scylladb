/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Drives the Parquet shredder with a real Scylla schema and real
// mutation_fragment types, then reads the resulting file back with our own
// footer parser.
//
// The standalone suite in sstables/parquet/ already proves the folding logic on
// a local row/cell model. What this test adds is the part that model cannot
// reach: that Scylla's own clustering_row, atomic_cell, decorated_key and
// column_definition are translated correctly.

#include <algorithm>
#include <fstream>
#include <zstd.h>
#include <map>
#include <set>
#include <boost/test/unit_test.hpp>

#include "test/lib/scylla_test_case.hh"
#include "test/lib/simple_schema.hh"

#include "sstables/parquet/writer_impl.hh"
#include "sstables/parquet/format/parquet_metadata.hh"
#include "sstables/parquet/format/encoders.hh"
#include "sstables/parquet/format/decoders.hh"
#include "sstables/parquet/tiering_context.hh"

#include "mutation/mutation.hh"
#include "schema/schema_builder.hh"
#include "exceptions/exceptions.hh"

namespace pq = sstables::parquet;

namespace {

schema_ptr make_test_schema() {
    return schema_builder(1u, "pqks", "pqcf")
        .with_column("pk", long_type, ::column_kind::partition_key)
        .with_column("ck", long_type, ::column_kind::clustering_key)
        .with_column("v_int", int32_type)
        .with_column("v_long", long_type)
        .with_column("v_dbl", double_type)
        .with_column("v_txt", utf8_type)
        .build();
}

} // namespace

// The type mapping must classify every column and keep them in
// partition / clustering / regular order, which is the order the shredder and
// the reader both rely on.
SEASTAR_THREAD_TEST_CASE(test_parquet_columns_of_schema) {
    auto s = make_test_schema();
    auto cols = pq::columns_of(*s);

    BOOST_REQUIRE_EQUAL(cols.size(), 6u);
    BOOST_REQUIRE_EQUAL(cols[0].name, "pk");
    BOOST_REQUIRE(cols[0].kind == pq::column_kind::partition_key);
    BOOST_REQUIRE(cols[1].kind == pq::column_kind::clustering_key);
    for (size_t i = 2; i < cols.size(); ++i) {
        BOOST_REQUIRE(cols[i].kind == pq::column_kind::regular);
    }
    // Regular columns come back in schema order, which is sorted by name and is
    // also the column_id order the shredder indexes cells by -- not declaration
    // order. Assert by name so the test states the real contract.
    std::map<std::string, pq::cql_type> by_name;
    for (const auto& c : cols) { by_name.emplace(c.name, c.type); }
    BOOST_REQUIRE(by_name.at("v_int")  == pq::cql_type::int32);
    BOOST_REQUIRE(by_name.at("v_long") == pq::cql_type::bigint);
    BOOST_REQUIRE(by_name.at("v_dbl")  == pq::cql_type::dbl);
    BOOST_REQUIRE(by_name.at("v_txt")  == pq::cql_type::text);

    // The regular block must be in ascending name order, because column_id
    // indexes it and the reader will rely on that to invert the mapping.
    std::vector<std::string> reg;
    for (size_t i = 2; i < cols.size(); ++i) { reg.push_back(cols[i].name); }
    BOOST_REQUIRE(std::is_sorted(reg.begin(), reg.end()));
}

// End to end: real fragments in, valid Parquet out.
SEASTAR_THREAD_TEST_CASE(test_parquet_shred_real_fragments) {
    auto s = make_test_schema();
    pq::fragment_shredder shredder(*s);

    constexpr int N = 500;
    const auto pk = partition_key::from_single_value(*s, long_type->decompose(int64_t(42)));
    shredder.new_partition(dht::decorate_key(*s, pk));

    for (int i = 0; i < N; ++i) {
        auto ck = clustering_key::from_single_value(*s, long_type->decompose(int64_t(i)));
        ::row cells;
        auto put = [&] (const char* name, bytes val, api::timestamp_type ts) {
            const column_definition& cdef = *s->get_column_definition(to_bytes(name));
            cells.apply(cdef, atomic_cell::make_live(*cdef.type, ts, std::move(val)));
        };
        // Every third row gets a divergent timestamp on one column, which is
        // what exercises the sparse exception channel.
        const api::timestamp_type base = 1700000000000000 + i;
        put("v_int",  int32_type->decompose(int32_t(i % 97)), base);
        put("v_long", long_type->decompose(int64_t(i) * 7), base);
        put("v_dbl",  double_type->decompose(double(i) / 4.0), base);
        put("v_txt",  utf8_type->decompose(sstring(i % 5 == 0 ? "alpha" : "beta")),
            (i % 3 == 0) ? base + 500 : base);

        clustering_row cr(std::move(ck), row_tombstone{}, row_marker(base), std::move(cells));
        shredder.add_clustering_row(cr);
    }

    BOOST_REQUIRE_EQUAL(shredder.size(), size_t(N));

    pq::pq_writer_config cfg;
    auto img = shredder.to_parquet(cfg);
    BOOST_REQUIRE_GT(img.size(), 0u);

    // Parse it back with our own reader and check the shape.
    auto md = pq::format::parse_footer(img);
    BOOST_REQUIRE_EQUAL(md.num_rows, int64_t(N));
    BOOST_REQUIRE_EQUAL(md.row_groups.size(), 1u);

    std::vector<std::string> names;
    for (size_t i = 1; i < md.schema.size(); ++i) {
        if (md.schema[i].is_leaf()) { names.push_back(md.schema[i].name); }
    }

    // The exact set, not just the count: a count catches a leaf appearing or
    // vanishing but not one being renamed or swapped, and it goes stale silently
    // when a feature adds a leaf -- which is how this assertion came to expect 9
    // after row markers introduced __rm.
    //
    // pk and ck, the four regular columns, the folded __ts, the two sparse
    // exception leaves that the divergent v_txt timestamps force into existence,
    // and __rm for the row markers every row here carries.
    const std::vector<std::string> want_leaves = {
        "pk", "ck", "v_dbl", "v_int", "v_long", "v_txt",
        "__ts", "__tsx_mask", "__tsx_vals", "__rm",
    };
    auto sorted_names = names;
    std::sort(sorted_names.begin(), sorted_names.end());
    auto sorted_want = want_leaves;
    std::sort(sorted_want.begin(), sorted_want.end());
    BOOST_REQUIRE_EQUAL(fmt::format("{}", fmt::join(sorted_names, ",")),
                        fmt::format("{}", fmt::join(sorted_want, ",")));
    BOOST_REQUIRE_EQUAL(md.leaf_count(), want_leaves.size());
    BOOST_REQUIRE_EQUAL(names[0], "pk");
    BOOST_REQUIRE_EQUAL(names[1], "ck");
    BOOST_REQUIRE(std::find(names.begin(), names.end(), "__ts") != names.end());
    BOOST_REQUIRE(std::find(names.begin(), names.end(), "__tsx_mask") != names.end());
    BOOST_REQUIRE(std::find(names.begin(), names.end(), "__tsx_vals") != names.end());

    // The folding level travels in the file's key/value metadata so a reader can
    // tell how to invert the mapping.
    const std::string* lvl = md.kv("scylla.folding_level");
    BOOST_REQUIRE(lvl != nullptr);
    BOOST_REQUIRE_EQUAL(*lvl, "L1");
}

// Without divergence the sparse channel must not be materialised at all --
// that is the whole point of deciding the schema from the data.
SEASTAR_THREAD_TEST_CASE(test_parquet_no_exception_leaves_when_uniform) {
    auto s = make_test_schema();
    pq::fragment_shredder shredder(*s);
    const auto pk = partition_key::from_single_value(*s, long_type->decompose(int64_t(1)));
    shredder.new_partition(dht::decorate_key(*s, pk));

    for (int i = 0; i < 100; ++i) {
        auto ck = clustering_key::from_single_value(*s, long_type->decompose(int64_t(i)));
        ::row cells;
        const api::timestamp_type ts = 1700000000000000;   // identical everywhere
        const column_definition& cdef = *s->get_column_definition(to_bytes("v_long"));
        cells.apply(cdef, atomic_cell::make_live(*cdef.type, ts, long_type->decompose(int64_t(i))));
        clustering_row cr(std::move(ck), row_tombstone{}, row_marker(ts), std::move(cells));
        shredder.add_clustering_row(cr);
    }

    auto img = shredder.to_parquet(pq::pq_writer_config{});
    auto md = pq::format::parse_footer(img);
    for (size_t i = 1; i < md.schema.size(); ++i) {
        BOOST_REQUIRE(md.schema[i].name != "__tsx_mask");
        BOOST_REQUIRE(md.schema[i].name != "__tsx_vals");
    }
}

// ---------------------------------------------------------------- tiering
// The policy itself is exhaustively tested standalone; what needs a real schema
// is the eligibility gate and the storage_format check in front of it.

// Encoding hints must survive the trip to the writer, and a `timestamp` column
// must be annotated in the unit it is actually written in.
//
// Both of these were broken and both were invisible to a round-trip suite, which is
// why they get an explicit test rather than being left to the size numbers:
//
//  - `write_rows()` builds the writer from the schema *tree*, while the per-leaf
//    encoding hints live in `mapped_schema::columns`. They were simply not passed,
//    so every column was written PLAIN -- including monotonic clustering keys the
//    mapping had explicitly asked to be DELTA_BINARY_PACKED. Nothing noticed,
//    because an encoding is self-describing: the reader honours whatever the page
//    header says, so the file still round-tripped, just much larger. On a
//    time-series table the clustering key alone was 912 882 bytes instead of
//    37 445.
//  - a CQL `timestamp` *column value* is milliseconds since epoch, while a cell's
//    *write* timestamp is microseconds. The mapping annotated columns MICROS while
//    writing millisecond values, so every external reader saw dates in 1970. Our
//    own reader inverts the mapping from `cql_type` and never reads the annotation,
//    so only a foreign decoder could catch it.
SEASTAR_THREAD_TEST_CASE(test_parquet_key_encoding_and_timestamp_unit) {
    auto s = schema_builder(1u, "pqks", "pqts")
        .with_column("pk", utf8_type, ::column_kind::partition_key)
        .with_column("ck", timestamp_type, ::column_kind::clustering_key)
        .with_column("v", int32_type)
        .build();

    pq::fragment_shredder shredder(*s);
    const auto pk = partition_key::from_single_value(*s, utf8_type->decompose(sstring("s1")));
    shredder.new_partition(dht::decorate_key(*s, pk));

    // A regular hourly stride, which is what delta encoding is for, and a value
    // whose wall-clock reading is unambiguous: 2023-01-01T00:00:00Z.
    constexpr int64_t base_ms = 1672531200000;
    constexpr int N = 2000;
    for (int i = 0; i < N; ++i) {
        auto ck = clustering_key::from_single_value(
                *s, timestamp_type->decompose(db_clock::time_point(
                        db_clock::duration(base_ms + int64_t(i) * 3600'000))));
        ::row cells;
        const column_definition& cdef = *s->get_column_definition(to_bytes("v"));
        cells.apply(cdef, atomic_cell::make_live(*cdef.type, 1700000000000000,
                                                 int32_type->decompose(int32_t(i))));
        clustering_row cr(std::move(ck), row_tombstone{}, row_marker(1700000000000000),
                          std::move(cells));
        shredder.add_clustering_row(cr);
    }

    pq::pq_writer_config cfg;
    auto img = shredder.to_parquet(cfg);
    auto md = pq::format::parse_footer(img);
    BOOST_REQUIRE_EQUAL(md.num_rows, int64_t(N));
    BOOST_REQUIRE_EQUAL(md.row_groups.size(), 1u);

    // The clustering key must actually be delta-encoded. Asserting the *encoding*
    // and not just the size is the point: a size assertion would drift with any
    // unrelated change, and this is the thing that was silently lost.
    bool saw_ck = false;
    for (const auto& cc : md.row_groups[0].columns) {
        if (!cc.meta || cc.meta->path_in_schema.empty()) { continue; }
        if (cc.meta->path_in_schema.back() != "ck") { continue; }
        saw_ck = true;
        const auto& enc = cc.meta->encodings;
        BOOST_REQUIRE_MESSAGE(
                std::find(enc.begin(), enc.end(),
                          pq::format::encoding::delta_binary_packed) != enc.end(),
                "clustering key was not DELTA_BINARY_PACKED -- the encoding hint was dropped");
    }
    BOOST_REQUIRE(saw_ck);

    // And the annotation must say MILLIS, matching the values written.
    bool saw_ts_col = false;
    for (size_t i = 1; i < md.schema.size(); ++i) {
        if (md.schema[i].name != "ck") { continue; }
        saw_ts_col = true;
        BOOST_REQUIRE(md.schema[i].converted_type.has_value());
        BOOST_REQUIRE_EQUAL(*md.schema[i].converted_type,
                            int32_t(pq::format::converted::timestamp_millis));
    }
    BOOST_REQUIRE(saw_ts_col);
}

// An operator's per-column encoding override (`parquet = {'encoding.<col>': ...}`) has to reach
// the file on *both* write paths.
//
// There are two, and they are chosen by size rather than by anything the operator can see:
// cut_row_group() drives the file writer directly once an sstable outgrows the row-group budget,
// and write_rows() emits it in one shot when the whole thing fits a single row group. Only the
// first passed the overrides on. The symptom was not "the option does nothing" but something far
// more misleading -- the option worked on big tables and silently did nothing on small ones, so
// raising rows_per_row_group appeared to *enable* it when all it did was force a cut.
//
// Both paths are exercised here against the same data for that reason. Asserting through only one
// of them is what let the gap exist in the first place.
SEASTAR_THREAD_TEST_CASE(test_parquet_per_column_encoding_override) {
    auto s = schema_builder(1u, "pqks", "pqenc")
        .with_column("pk", utf8_type, ::column_kind::partition_key)
        .with_column("ck", int32_type, ::column_kind::clustering_key)
        .with_column("v", utf8_type)
        .with_column("w", double_type)
        .build();

    // A helper so the two paths see byte-identical input.
    auto fill = [&] (pq::fragment_shredder& shredder) {
        const auto pk = partition_key::from_single_value(*s, utf8_type->decompose(sstring("p1")));
        shredder.new_partition(dht::decorate_key(*s, pk));
        for (int i = 0; i < 600; ++i) {
            auto ck = clustering_key::from_single_value(*s, int32_type->decompose(int32_t(i)));
            ::row cells;
            const column_definition& vdef = *s->get_column_definition(to_bytes("v"));
            const column_definition& wdef = *s->get_column_definition(to_bytes("w"));
            // Distinct and sorted, so the dictionary's cardinality test rejects it and the
            // column falls through to whatever the override asks for.
            cells.apply(vdef, atomic_cell::make_live(
                    *vdef.type, 1700000000000000,
                    utf8_type->decompose(format("value-{:08d}", i))));
            cells.apply(wdef, atomic_cell::make_live(
                    *wdef.type, 1700000000000000, double_type->decompose(double(i) * 1.5)));
            clustering_row cr(std::move(ck), row_tombstone{}, row_marker(1700000000000000),
                              std::move(cells));
            shredder.add_clustering_row(cr);
        }
    };

    auto encodings_of = [] (const std::vector<uint8_t>& img, const sstring& col) {
        auto md = pq::format::parse_footer(img);
        std::set<pq::format::encoding> out;
        for (const auto& rg : md.row_groups) {
            for (const auto& cc : rg.columns) {
                if (!cc.meta || cc.meta->path_in_schema.empty()) { continue; }
                if (cc.meta->path_in_schema.back() != col) { continue; }
                out.insert(cc.meta->encodings.begin(), cc.meta->encodings.end());
            }
        }
        return out;
    };

    // Without an override both columns are PLAIN -- the baseline the assertions below move away
    // from, so a test that passed for the wrong reason would show up here.
    {
        pq::fragment_shredder shredder(*s);
        fill(shredder);
        pq::pq_writer_config cfg;
        const auto img = shredder.to_parquet(cfg);
        BOOST_REQUIRE(encodings_of(img, "v").contains(pq::format::encoding::plain));
        BOOST_REQUIRE(!encodings_of(img, "v").contains(pq::format::encoding::delta_byte_array));
        BOOST_REQUIRE(!encodings_of(img, "w").contains(
                pq::format::encoding::byte_stream_split));
    }

    // The one-shot path: one row group, which is the case that was broken.
    {
        pq::fragment_shredder shredder(*s);
        fill(shredder);
        pq::pq_writer_config cfg;
        cfg.column_encodings["v"] = pq::format::encoding::delta_byte_array;
        cfg.column_encodings["w"] = pq::format::encoding::byte_stream_split;
        const auto img = shredder.to_parquet(cfg);
        const auto md = pq::format::parse_footer(img);
        BOOST_REQUIRE_EQUAL(md.row_groups.size(), 1u);
        BOOST_REQUIRE_MESSAGE(
                encodings_of(img, "v").contains(pq::format::encoding::delta_byte_array),
                "override dropped on the single-row-group path");
        BOOST_REQUIRE_MESSAGE(
                encodings_of(img, "w").contains(pq::format::encoding::byte_stream_split),
                "override dropped on the single-row-group path");
    }

    // A forced dictionary has to beat the repeat-ratio heuristic: the column is 600 distinct
    // values in 600 rows, which the heuristic rejects. Naming the encoding overrides it.
    {
        pq::fragment_shredder shredder(*s);
        fill(shredder);
        pq::pq_writer_config cfg;
        cfg.column_encodings["v"] = pq::format::encoding::rle_dictionary;
        const auto img = shredder.to_parquet(cfg);
        BOOST_REQUIRE_MESSAGE(
                encodings_of(img, "v").contains(pq::format::encoding::rle_dictionary),
                "an explicitly requested dictionary was refused by the cardinality heuristic");
    }
}

// The write-side memory budget's accounting (R-13, design doc 5.5a).
//
// The writer currently buffers every row of an sstable before encoding anything, which
// costs about 1.8 kB per row -- 17 GiB at ten million rows. Cutting row groups needs a
// number to cut on, and that number has to err *high*: under-counting is what OOMs a
// shard, over-counting only cuts a row group slightly early.
//
// Calibrated against measured RSS on a 10-column time-series table: the estimator says
// 1 887 B/row where the real slope is 1 814 B/row, i.e. 4 % conservative. This test does
// not re-derive that -- RSS is not available here -- it pins the properties the budget
// relies on, so the accounting cannot silently stop tracking the buffer.
SEASTAR_THREAD_TEST_CASE(test_parquet_buffered_bytes_accounting) {
    auto s = make_test_schema();
    pq::fragment_shredder shredder(*s);
    BOOST_REQUIRE_EQUAL(shredder.buffered_bytes(), 0u);

    const auto pk = partition_key::from_single_value(*s, long_type->decompose(int64_t(1)));
    shredder.new_partition(dht::decorate_key(*s, pk));

    auto add = [&] (int i) {
        auto ck = clustering_key::from_single_value(*s, long_type->decompose(int64_t(i)));
        ::row cells;
        for (const char* nm : {"v_int", "v_long", "v_dbl", "v_txt"}) {
            const column_definition& cdef = *s->get_column_definition(to_bytes(nm));
            bytes val = cdef.type == utf8_type
                      ? utf8_type->decompose(sstring(format("value-{}", i)))
                      : (cdef.type == int32_type ? int32_type->decompose(int32_t(i))
                      : (cdef.type == double_type ? double_type->decompose(double(i))
                                                  : long_type->decompose(int64_t(i))));
            cells.apply(cdef, atomic_cell::make_live(*cdef.type, 1700000000000000 + i,
                                                     std::move(val)));
        }
        clustering_row cr(std::move(ck), row_tombstone{}, row_marker(1700000000000000 + i),
                          std::move(cells));
        shredder.add_clustering_row(cr);
    };

    add(0);
    const size_t one = shredder.buffered_bytes();
    BOOST_REQUIRE_GT(one, sizeof(pq::row));       // it counts the heap, not just the struct

    for (int i = 1; i < 100; ++i) { add(i); }
    const size_t hundred = shredder.buffered_bytes();

    // Monotonic, and roughly proportional -- the rows are alike, so 100 of them should
    // cost far more than one and not wildly more than 100x.
    BOOST_REQUIRE_GT(hundred, one * 50);
    BOOST_REQUIRE_LT(hundred, one * 200);

    // Errs high: a row of six columns cannot really occupy less than its own struct plus
    // a cell per column, and must not be estimated at some implausible size either.
    const size_t per_row = hundred / 100;
    BOOST_REQUIRE_GT(per_row, sizeof(pq::row) + 4 * sizeof(pq::cell));
    BOOST_REQUIRE_LT(per_row, 8u * 1024u);

    // Every row path must feed the accounting, not just clustering rows: a range
    // tombstone change is buffered as a row too.
    const size_t before_rtc = shredder.buffered_bytes();
    auto bound = clustering_key_prefix::from_single_value(*s, long_type->decompose(int64_t(500)));
    shredder.add_range_tombstone_change(range_tombstone_change(
            position_in_partition::before_key(bound),
            tombstone(1700000000000001, gc_clock::time_point(gc_clock::duration(7)))));
    BOOST_REQUIRE_GT(shredder.buffered_bytes(), before_rtc);

    shredder.clear();
    BOOST_REQUIRE_EQUAL(shredder.buffered_bytes(), 0u);
}

// The `parquet = {...}` table property: parsing, validation, and round-tripping.
//
// Validation is the point. A storage-format option that silently ignores what it cannot
// honour is worse than one that refuses: a user who writes `compression: 'gzip'` and
// gets zstd has been told something untrue about their data. So every case below that
// *should* fail is asserted to fail, not just the ones that should succeed.
SEASTAR_THREAD_TEST_CASE(test_parquet_parameters) {
    using pp = pq::parquet_parameters;

    // Defaults when nothing is given.
    {
        pp p{};
        BOOST_REQUIRE(p.to_map().empty());          // DESCRIBE stays terse
        const pq::pq_writer_config def;
        BOOST_REQUIRE_EQUAL(p.config().rows_per_row_group, def.rows_per_row_group);
    }

    // Accepted values reach the config.
    {
        // 7500 rather than the default, so the round-trip below actually proves
        // something: to_map() records only what differs from the default, so a test
        // that sets the default value would assert against an absent key. It used to
        // set 5 000, which stopped being a non-default when 10.4c's sweep moved the
        // default there.
        pp p{{{pp::ROWS_PER_ROW_GROUP, "7500"},
              {pp::ROW_GROUP_BUFFER_BYTES, "32MiB"},
              {pp::PAGE_ROWS, "4096"},
              {pp::COMPRESSION, "none"},
              {pp::METADATA_FOLDING, "verbatim"}}};
        BOOST_REQUIRE_EQUAL(p.config().rows_per_row_group, 7500u);
        BOOST_REQUIRE_EQUAL(p.config().row_group_buffer_bytes, 32u * 1024 * 1024);
        BOOST_REQUIRE_EQUAL(p.config().wopt.page_values, 4096u);
        BOOST_REQUIRE(p.config().wopt.compression == pq::format::codec::uncompressed);
        BOOST_REQUIRE(p.config().level == pq::folding_level::verbatim);
        // Numeric dictionaries are off by default and reachable via 'all'.
        BOOST_REQUIRE(!p.config().wopt.numeric_dictionary);
        pp all{{{pp::DICTIONARY, "all"}}};
        BOOST_REQUIRE(all.config().wopt.numeric_dictionary);
        BOOST_REQUIRE_EQUAL(all.to_map().at(pp::DICTIONARY), "all");
        pp none{{{pp::DICTIONARY, "none"}}};
        BOOST_REQUIRE(!none.config().wopt.use_dictionary);
        BOOST_REQUIRE_EQUAL(none.to_map().at(pp::DICTIONARY), "none");

        // Every codec the option accepts, and the on-the-wire enum each maps to. "lz4" is
        // LZ4_RAW (codec 7), the bare block every current Parquet implementation reads, not the
        // deprecated Hadoop-framed codec 5 -- so the mapping is asserted rather than assumed.
        // It used to be in the `rejects` list below; see design doc 10.29 for why it is writable
        // now and why zstd is still the default.
        pp c_lz4{{{pp::COMPRESSION, "lz4"}}};
        BOOST_REQUIRE(c_lz4.config().wopt.compression == pq::format::codec::lz4_raw);
        BOOST_REQUIRE_EQUAL(c_lz4.to_map().at(pp::COMPRESSION), "lz4");
        pp c_none{{{pp::COMPRESSION, "none"}}};
        BOOST_REQUIRE(c_none.config().wopt.compression == pq::format::codec::uncompressed);
        BOOST_REQUIRE_EQUAL(c_none.to_map().at(pp::COMPRESSION), "none");
        pp c_zstd{{{pp::COMPRESSION, "zstd"}}};
        BOOST_REQUIRE(c_zstd.config().wopt.compression == pq::format::codec::zstd);
        // zstd is the default, so to_map() omits it -- which is the contract, not an oversight.
        BOOST_REQUIRE(!c_zstd.to_map().contains(pp::COMPRESSION));

        // Round-trips through the map form, which is how it is persisted.
        auto m = p.to_map();
        BOOST_REQUIRE_EQUAL(m[pp::ROWS_PER_ROW_GROUP], "7500");
        pp again{m};
        BOOST_REQUIRE_EQUAL(again.config().rows_per_row_group, 7500u);
        BOOST_REQUIRE_EQUAL(again.config().wopt.page_values, 4096u);
        BOOST_REQUIRE(again.config().level == pq::folding_level::verbatim);
    }

    auto rejects = [] (std::map<sstring, sstring> opts) {
        BOOST_CHECK_THROW(pp{opts}, exceptions::configuration_exception);
    };

    rejects({{"row_groop_rows", "5000"}});                  // typo, not silently ignored
    rejects({{pp::ROWS_PER_ROW_GROUP, "not-a-number"}});
    rejects({{pp::ROWS_PER_ROW_GROUP, "5000rows"}});            // trailing junk
    rejects({{pp::ROWS_PER_ROW_GROUP, "0"}});
    // Below the floor the fixed per-row-group metadata dominates: at 100 rows on a
    // 20-leaf table it is 45 B/row against a 5.2 B/row total (design doc 10.4c).
    rejects({{pp::ROWS_PER_ROW_GROUP, "100"}});
    rejects({{pp::ROW_GROUP_BUFFER_BYTES, "16"}});          // under the 1 MiB floor
    rejects({{pp::ROW_GROUP_BUFFER_BYTES, "8GiB"}});        // over the 1 GiB ceiling
    rejects({{pp::COMPRESSION, "gzip"}});                   // plausible, unsupported
    rejects({{pp::COMPRESSION, "snappy"}});                 // readable, but never written
    rejects({{pp::COMPRESSION_LEVEL, "99"}});
    // L3 discards write times and TTLs: it is export-only and must not be reachable
    // as a storage setting.
    rejects({{pp::DICTIONARY, "yes"}});
    rejects({{pp::METADATA_FOLDING, "logical"}});
    rejects({{pp::METADATA_FOLDING, "yes"}});

    // The `row_group_rows` alias. This is the compatibility half of the rename to
    // `rows_per_row_group`, and it matters more than the rename: the map is persisted in
    // schema, so tables created before the rename carry the old key and reconstruct a
    // parquet_parameters from it every time their schema is read. If this stopped parsing,
    // those tables would stop loading.
    {
        pp legacy{{{pp::ROW_GROUP_ROWS_LEGACY, "7500"}}};
        pp current{{{pp::ROWS_PER_ROW_GROUP, "7500"}}};
        BOOST_REQUIRE_EQUAL(legacy.config().rows_per_row_group, 7500u);
        // Same setting, not merely both accepted.
        BOOST_REQUIRE_EQUAL(legacy.config().rows_per_row_group,
                            current.config().rows_per_row_group);

        // to_map() emits the canonical name whichever spelling arrived, and its output
        // parses back to the same config -- which is what makes it safe to feed a
        // serialized map back through the parser.
        auto m = legacy.to_map();
        BOOST_REQUIRE_EQUAL(m.at(pp::ROWS_PER_ROW_GROUP), "7500");
        BOOST_REQUIRE(!m.contains(pp::ROW_GROUP_ROWS_LEGACY));
        BOOST_REQUIRE(m == current.to_map());
        BOOST_REQUIRE_EQUAL(pp{m}.config().rows_per_row_group, 7500u);

        // The alias is a second name, not a second code path: it gets the same bounds.
        rejects({{pp::ROW_GROUP_ROWS_LEGACY, "100"}});
        rejects({{pp::ROW_GROUP_ROWS_LEGACY, "not-a-number"}});

        // Both spellings at once is a user error, not a last-one-wins. Asserted in both
        // map orders even though std::map sorts them, so the check cannot be satisfied by
        // whichever key happens to come first.
        rejects({{pp::ROW_GROUP_ROWS_LEGACY, "7500"}, {pp::ROWS_PER_ROW_GROUP, "20000"}});
        rejects({{pp::ROWS_PER_ROW_GROUP, "20000"}, {pp::ROW_GROUP_ROWS_LEGACY, "7500"}});
        // Even when they agree: the operator still has to be told the map is ambiguous.
        rejects({{pp::ROW_GROUP_ROWS_LEGACY, "7500"}, {pp::ROWS_PER_ROW_GROUP, "7500"}});
    }

    // ---- per-column encryption keys
    //
    // The vocabulary half of the feature: `encryption_key.<column>` carries key *provider options*
    // overlaid on the table's own, because a key is identified by an option map (ent/encryption's
    // get_provider takes one) and which option names a key differs per provider.
    {
        pp p{{{pp::ENCRYPTION, "aes_gcm_v1"},
              {"key_provider", "LocalFileSystemKeyProviderFactory"},
              {"secret_key_file", "/keys/tbl.key"},
              {"secret_key_strength", "256"},
              {sstring(pp::ENCRYPTION_KEY_PREFIX) + "ssn", "secret_key_file=/keys/pii.key"},
              {sstring(pp::ENCRYPTION_KEY_PREFIX) + "dob", "secret_key_file=/keys/pii.key"}}};

        const auto& cko = p.column_key_opts();
        BOOST_REQUIRE_EQUAL(cko.size(), 2u);

        // The override is *overlaid*, so the column inherits the provider, the algorithm and the
        // key length and replaces only what it named. If it did not inherit, the column would
        // silently land on the default provider with the default AES/CBC algorithm, which the
        // format cannot honour at all.
        const auto& ssn = cko.at("ssn");
        BOOST_REQUIRE_EQUAL(ssn.at("secret_key_file"), "/keys/pii.key");
        BOOST_REQUIRE_EQUAL(ssn.at("key_provider"), "LocalFileSystemKeyProviderFactory");
        BOOST_REQUIRE_EQUAL(ssn.at("secret_key_strength"), "256");
        BOOST_REQUIRE_EQUAL(ssn.at("cipher_algorithm"), "AES/GCM/NoPadding");
        // And the table's own options are untouched by the column's.
        BOOST_REQUIRE_EQUAL(p.key_opts().at("secret_key_file"), "/keys/tbl.key");

        // Two columns naming the same key must produce *equal* option sets, because that equality
        // is what both the writer and the reader deduplicate on -- one provider round trip for
        // "encrypt the PII columns under the PII key" rather than one per column.
        BOOST_REQUIRE(cko.at("ssn") == cko.at("dob"));

        // Round-trips as the operator's own text, not as the overlay: DESCRIBE must not read as if
        // each column had been configured from scratch.
        auto m = p.to_map();
        BOOST_REQUIRE_EQUAL(m.at(sstring(pp::ENCRYPTION_KEY_PREFIX) + "ssn"),
                            "secret_key_file=/keys/pii.key");
        pp again{m};
        BOOST_REQUIRE(again.column_key_opts() == cko);

        // Several options at once, with the whitespace an operator will write.
        pp multi{{{pp::ENCRYPTION, "aes_gcm_v1"},
                  {sstring(pp::ENCRYPTION_KEY_PREFIX) + "ssn",
                   "key_provider=KmipKeyProviderFactory, kmip_host=h1, key_namespace=pii"}}};
        const auto& mk = multi.column_key_opts().at("ssn");
        BOOST_REQUIRE_EQUAL(mk.at("key_provider"), "KmipKeyProviderFactory");
        BOOST_REQUIRE_EQUAL(mk.at("kmip_host"), "h1");
        BOOST_REQUIRE_EQUAL(mk.at("key_namespace"), "pii");
        // A value containing '=' survives: only the first '=' splits. Base64 key material ids and
        // padded provider handles both look like this.
        pp eq{{{pp::ENCRYPTION, "aes_gcm_v1"},
               {sstring(pp::ENCRYPTION_KEY_PREFIX) + "ssn", "master_key=abc=="}}};
        BOOST_REQUIRE_EQUAL(eq.column_key_opts().at("ssn").at("master_key"), "abc==");
    }

    // A per-column key with no `encryption` is worse than inert: it reads as "this column has its
    // own key" when nothing is encrypted at all.
    rejects({{sstring(pp::ENCRYPTION_KEY_PREFIX) + "ssn", "secret_key_file=/k/p.key"}});
    // A typo'd provider option would be ignored by the provider and fall back to the *table's*
    // key, leaving the column looking separately encrypted when it is not.
    rejects({{pp::ENCRYPTION, "aes_gcm_v1"},
             {sstring(pp::ENCRYPTION_KEY_PREFIX) + "ssn", "secret_key_fil=/k/p.key"}});
    rejects({{pp::ENCRYPTION, "aes_gcm_v1"},
             {sstring(pp::ENCRYPTION_KEY_PREFIX) + "ssn", "/k/p.key"}});      // no '<opt>='
    rejects({{pp::ENCRYPTION, "aes_gcm_v1"},
             {sstring(pp::ENCRYPTION_KEY_PREFIX) + "ssn", "secret_key_file="}});  // empty value
    rejects({{pp::ENCRYPTION, "aes_gcm_v1"},
             {sstring(pp::ENCRYPTION_KEY_PREFIX) + "ssn", ""}});
    rejects({{pp::ENCRYPTION, "aes_gcm_v1"},
             {sstring(pp::ENCRYPTION_KEY_PREFIX), "secret_key_file=/k/p.key"}});  // no column
    // An override may not name an algorithm the format cannot honour, exactly as the table's own
    // options may not: the check is shared, so a per-column set cannot slip past it.
    rejects({{pp::ENCRYPTION, "aes_gcm_v1"},
             {sstring(pp::ENCRYPTION_KEY_PREFIX) + "ssn",
              "cipher_algorithm=AES/CBC/PKCS5Padding"}});
    rejects({{pp::ENCRYPTION, "aes_gcm_v1"},
             {sstring(pp::ENCRYPTION_KEY_PREFIX) + "ssn", "secret_key_strength=64"}});

    // Columns whose Parquet leaf name is not theirs alone cannot be keyed. `value` and `key` are
    // the names of leaves inside *every* non-frozen collection, so a key looked up by leaf name
    // would cover those too -- including a collection added by a later ALTER, which is why this is
    // rejected without consulting the current schema.
    rejects({{pp::ENCRYPTION, "aes_gcm_v1"},
             {sstring(pp::ENCRYPTION_KEY_PREFIX) + "value", "secret_key_file=/k/p.key"}});
    rejects({{pp::ENCRYPTION, "aes_gcm_v1"},
             {sstring(pp::ENCRYPTION_KEY_PREFIX) + "key", "secret_key_file=/k/p.key"}});
    rejects({{pp::ENCRYPTION, "aes_gcm_v1"},
             {sstring(pp::ENCRYPTION_KEY_PREFIX) + "__ts", "secret_key_file=/k/p.key"}});
    // And the rule has exactly one definition, which the DDL layer calls for the type-dependent
    // case it alone can see.
    BOOST_REQUIRE(!pp::keyable_column_error("ssn", false));
    BOOST_REQUIRE(pp::keyable_column_error("ssn", true));
    BOOST_REQUIRE(pp::keyable_column_error("value", false));
}

SEASTAR_THREAD_TEST_CASE(test_parquet_schema_eligibility) {
    BOOST_REQUIRE(pq::schema_is_parquet_eligible(*make_test_schema()));

    // Non-frozen collections became a repeated group and counters one element per
    // shard, so both are eligible now. They are still worth asserting: these two
    // were the gate's only rejections, and a regression in either encoding should
    // show up as a refusal here rather than as mangled data.
    auto with_set = schema_builder(1u, "pqks", "withset")
        .with_column("pk", long_type, ::column_kind::partition_key)
        .with_column("s", set_type_impl::get_instance(utf8_type, true))
        .build();
    BOOST_REQUIRE(pq::schema_is_parquet_eligible(*with_set));

    // A frozen collection is an opaque blob and is fine.
    auto with_frozen = schema_builder(1u, "pqks", "withfrozen")
        .with_column("pk", long_type, ::column_kind::partition_key)
        .with_column("s", set_type_impl::get_instance(utf8_type, false))
        .build();
    BOOST_REQUIRE(pq::schema_is_parquet_eligible(*with_frozen));

    auto with_counter = schema_builder(1u, "pqks", "withcounter")
        .with_column("pk", long_type, ::column_kind::partition_key)
        .with_column("c", counter_type)
        .build();
    BOOST_REQUIRE(pq::schema_is_parquet_eligible(*with_counter));

    // pk + ck + 4 regular. Exactly the schema's own count -- the Parquet leaf count is
    // data-dependent and deliberately not what C5 bounds (see tiering_policy.hh).
    BOOST_REQUIRE_EQUAL(pq::column_count(*make_test_schema()), 6u);
}

SEASTAR_THREAD_TEST_CASE(test_parquet_storage_format_gates_conversion) {
    pq::compaction_context ctx;
    ctx.bottom_tier = true;
    ctx.predicted_gain = 0.9;                  // everything else says yes

    // Default table: never converted, however good the numbers look.
    auto plain = make_test_schema();
    auto d = pq::decide_output_format({}, *plain, ctx);
    BOOST_REQUIRE(!d.parquet());
    BOOST_REQUIRE(d.reason.find("storage_format") != std::string::npos);

    // Opted into hybrid, and every criterion satisfied: converts.
    auto opted = schema_builder(schema_ptr(make_test_schema()))
        .set_storage_format(storage_format_type::hybrid)
        .build();
    auto d2 = pq::decide_output_format({}, *opted, ctx);
    BOOST_REQUIRE(d2.parquet());

    // Fail closed. This used to be asserted via the minimum-size criterion, which no longer
    // exists -- C6 subsumed it (see tiering_policy.hh). What guards an unmeasurable candidate
    // now is C6 itself: no gain means no conversion, which matters because the estimator
    // returns nullopt whenever it cannot sample, and "could not measure" must never read as
    // "go ahead".
    auto unmeasured = ctx;
    unmeasured.predicted_gain.reset();
    auto d3 = pq::decide_output_format({}, *opted, unmeasured);
    BOOST_REQUIRE(!d3.parquet());
    BOOST_REQUIRE(d3.reason.find("no measured gain") != std::string::npos);

    // And the width bound, which is the only thing standing in for C7.
    auto wide = ctx;
    auto d4 = pq::evaluate_tiering([&] {
        auto in = pq::make_tiering_inputs({}, *opted, wide);
        in.column_count = 500;
        return in;
    }());
    BOOST_REQUIRE(!d4.parquet());
    BOOST_REQUIRE(d4.reason.find("columns") != std::string::npos);
}

// DELTA_BYTE_ARRAY and DELTA_LENGTH_BYTE_ARRAY: round-trip, and the size claim that motivates them.
//
// Front coding stores each value as "share N bytes with the previous value, then this suffix", which
// is worth having because an SSTable delivers rows in clustering order -- so a text clustering key is
// *sorted* within a row group and adjacent values share long prefixes. That is the common case here,
// not a lucky one.
//
// The encoder is only useful if our own reader can read it back exactly, including the cases that
// break a careless implementation: an empty string, a value that is a strict prefix of its
// predecessor (so the shared run is the whole shorter value), a value sharing nothing, and a
// non-ASCII byte sequence.
SEASTAR_THREAD_TEST_CASE(test_delta_byte_array_round_trip) {
    using namespace sstables::parquet::format;

    const std::vector<std::vector<std::string>> cases = {
        // Sorted keys with long shared prefixes -- what a text clustering key looks like.
        {"st00000", "st00001", "st00002", "st00010", "st00100", "st01000"},
        // URLs sharing an authority, then diverging.
        {"https://example.com/a", "https://example.com/ab", "https://example.com/b",
         "https://example.org/", "https://zzz.example/"},
        // The awkward ones: empty values, a prefix of the previous value, no sharing at all.
        {"", "a", "ab", "ab", "a", "", "zzz", ""},
        // Non-ASCII bytes, to make sure nothing assumes char is signed or printable.
        {std::string("\xff\xfe\x01", 3), std::string("\xff\xfe\x02", 3), std::string("\x00\x01", 2)},
        // Single value, and empty input.
        {"only"},
        {},
    };

    for (const auto& vals : cases) {
        std::vector<uint8_t> dba, dlba, plain;
        encode_delta_byte_array(dba, vals);
        encode_delta_length_byte_array(dlba, vals);
        encode_plain_byte_array(plain, vals);

        auto back_dba = decode_delta_byte_array(dba, vals.size());
        auto back_dlba = decode_delta_length_byte_array(dlba, vals.size());
        BOOST_REQUIRE_EQUAL(back_dba.size(), vals.size());
        BOOST_REQUIRE_EQUAL(back_dlba.size(), vals.size());
        for (size_t i = 0; i < vals.size(); ++i) {
            BOOST_REQUIRE_EQUAL(back_dba[i], vals[i]);
            BOOST_REQUIRE_EQUAL(back_dlba[i], vals[i]);
        }
    }

    // And the point of it: on sorted keys with a shared prefix, front coding must actually be
    // smaller than PLAIN. Without this the round-trip above would pass on an encoder that simply
    // stored everything verbatim.
    std::vector<std::string> sorted_keys;
    for (int i = 0; i < 5000; ++i) {
        sorted_keys.push_back(seastar::format("station-{:08d}-eu-west", i));
    }
    std::vector<uint8_t> dba, plain;
    encode_delta_byte_array(dba, sorted_keys);
    encode_plain_byte_array(plain, sorted_keys);
    BOOST_TEST_MESSAGE(seastar::format("delta_byte_array {} bytes vs plain {} ({:.1f}%)",
                                       dba.size(), plain.size(),
                                       100.0 * dba.size() / plain.size()));
    BOOST_REQUIRE_LT(dba.size(), plain.size() / 2);

    // The bounded-downside claim: with no shared prefixes the prefix stream is all zeroes and
    // delta-packs away, so it must still not be *worse* than PLAIN, which spends a fixed 4 bytes
    // per value on lengths.
    std::vector<std::string> unrelated;
    for (int i = 0; i < 5000; ++i) {
        unrelated.push_back(seastar::format("{:x}-{}", (i * 2654435761u) & 0xffffff, i % 7));
    }
    std::vector<uint8_t> dba2, plain2;
    encode_delta_byte_array(dba2, unrelated);
    encode_plain_byte_array(plain2, unrelated);
    BOOST_TEST_MESSAGE(seastar::format("no shared prefix: delta {} vs plain {} ({:.1f}%)",
                                       dba2.size(), plain2.size(),
                                       100.0 * dba2.size() / plain2.size()));
    BOOST_REQUIRE_LE(dba2.size(), plain2.size());
}

// Does front coding survive the block compressor? The question §10.3f exists to ask.
//
// The encoder-level numbers (35 % of PLAIN on sorted keys) are measured *before* compression, and
// this project has already been caught by exactly that gap: BYTE_STREAM_SPLIT on doubles looked
// obviously right and cost 55 % once zstd ran, because transposing destroyed the whole-value
// repetition zstd was already exploiting. So the only figure that decides whether DELTA_BYTE_ARRAY
// earns its place is the compressed one.
//
// Driven by real data when it is available: /tmp/gh_event_ids.txt holds the GitHub Archive
// `event_id` column in clustering order, which is the *one* text clustering key in the whole
// seven-dataset corpus -- every other dataset clusters on bigint, timestamp or int. Its adjacent
// values share 6.25 of 11 characters on average. Without the file the test falls back to synthetic
// data of the same shape so it still runs in CI.
SEASTAR_THREAD_TEST_CASE(test_delta_byte_array_after_compression) {
    using namespace sstables::parquet::format;

    // zstd directly: the writer's own compress() lives in an anonymous namespace, and level 3 is
    // what writer_options defaults to, so this matches what a real page would get.
    auto zstd_size = [] (const std::vector<uint8_t>& body) {
        const size_t bound = ZSTD_compressBound(body.size());
        std::vector<uint8_t> out(bound);
        const size_t n = ZSTD_compress(out.data(), bound, body.data(), body.size(), 3);
        BOOST_REQUIRE(!ZSTD_isError(n));
        return n;
    };

    auto report = [&] (const char* label, const std::vector<std::string>& vals) {
        std::vector<uint8_t> plain, delta;
        encode_plain_byte_array(plain, vals);
        encode_delta_byte_array(delta, vals);
        const size_t pz = zstd_size(plain), dz = zstd_size(delta);
        BOOST_TEST_MESSAGE(seastar::format(
                "{:<22} n={} | raw plain {} delta {} ({:.1f}%) | zstd plain {} delta {} ({:.1f}%)",
                label, vals.size(), plain.size(), delta.size(),
                100.0 * delta.size() / plain.size(), pz, dz, 100.0 * dz / pz));
        return double(dz) / double(pz);
    };

    std::vector<std::string> real;
    {
        std::ifstream f("/tmp/gh_event_ids.txt");
        std::string line;
        while (std::getline(f, line)) {
            if (!line.empty()) { real.push_back(line); }
        }
    }
    if (!real.empty()) {
        const double ratio = report("github event_id", real);
        // No assertion on the exact figure -- it is data-dependent -- but a regression to *worse
        // than PLAIN after compression* would mean the hint is actively harmful and should be
        // reverted, so that is worth failing on.
        BOOST_REQUIRE_LT(ratio, 1.0);
    } else {
        BOOST_TEST_MESSAGE("/tmp/gh_event_ids.txt absent; synthetic shapes only");
    }

    // Monotonic numeric ids of the same shape as the real column, for CI.
    std::vector<std::string> ids;
    for (int i = 0; i < 180000; ++i) {
        ids.push_back(seastar::format("{}", 34502000000LL + i * 7));
    }
    report("synthetic ids", ids);

    // The two ends of the range, for context.
    std::vector<std::string> sorted_keys, unrelated;
    for (int i = 0; i < 100000; ++i) {
        sorted_keys.push_back(seastar::format("station-{:08d}-eu-west", i));
        unrelated.push_back(seastar::format("{:x}", (i * 2654435761u) & 0xffffffff));
    }
    report("sorted keys", sorted_keys);
    report("unrelated", unrelated);

    // The case that decides whether the hint is safe to apply by type. A clustering key is *sorted*,
    // but sorted is not the same as sharing a prefix: sorted UUIDs or hashes share one or two
    // characters out of 36, so they look like the "unrelated" row above -- where front coding *loses*
    // after compression, because splitting values into prefix/suffix streams destroys the byte
    // patterns zstd was exploiting. This is the BYTE_STREAM_SPLIT failure of §10.3f in another guise.
    std::vector<std::string> sorted_uuids;
    for (int i = 0; i < 100000; ++i) {
        const uint64_t h = 0x9e3779b97f4a7c15ull * uint64_t(i + 1);
        sorted_uuids.push_back(seastar::format("{:016x}-{:04x}-{:04x}", h, (h >> 13) & 0xffff,
                                               (h >> 29) & 0xffff));
    }
    std::sort(sorted_uuids.begin(), sorted_uuids.end());
    report("sorted uuid-like", sorted_uuids);

    // The regression risk the hint introduces, which is not about compression at all.
    //
    // In parquet_writer.cc an explicit encoding hint wins *outright*: `!hinted` guards both
    // dictionary paths, so hinting a column suppresses the dictionary for it entirely. That is
    // deliberate for a monotonic numeric key, where a dictionary would be chosen on cardinality and
    // lose the delta encoding. But a text clustering key can just as easily be low cardinality --
    // a weekday, a category, a status -- and there the dictionary stores each distinct value once
    // where front coding stores every occurrence. So the comparison that decides whether the hint
    // is safe for byte_array is delta against *dictionary*, not against PLAIN.
    std::vector<std::string> low_card;
    const char* days[] = {"friday", "monday", "saturday", "sunday", "thursday", "tuesday", "wednesday"};
    for (int i = 0; i < 100000; ++i) { low_card.push_back(days[i % 7]); }
    std::sort(low_card.begin(), low_card.end());          // clustering order
    {
        std::vector<uint8_t> delta, plain;
        encode_delta_byte_array(delta, low_card);
        encode_plain_byte_array(plain, low_card);
        auto d = encode_dictionary_byte_array(low_card);
        std::vector<uint8_t> dict_total = d.dictionary_page;
        dict_total.insert(dict_total.end(), d.index_page.begin(), d.index_page.end());
        const size_t dz = zstd_size(delta), pz = zstd_size(plain), kz = zstd_size(dict_total);
        BOOST_TEST_MESSAGE(seastar::format(
                "low-cardinality sorted text: zstd plain {} dict {} delta {} "
                "-- delta/dict = {:.1f}%", pz, kz, dz, 100.0 * dz / kz));
    }
}

// DELTA_BINARY_PACKED with wide deltas, which is the shape a *key* column has.
//
// This is a regression test for a data-loss bug, and the class matters more than the arithmetic.
// Both the packer (encoders.hh) and the unpacker (decoders.hh) shifted values through a `uint64_t`
// accumulator that carries 0..7 bits of the previous value between values -- so a w-bit value
// straddles up to w+6 bit positions, and everything past bit 63 was silently dropped. The packer
// lost the top bits of the value it was writing; the unpacker lost the top bits of the byte it
// over-read, which belong to the *next* value. Neither was symmetric with the other, so the two did
// not cancel: the round trip below actually comes back wrong, and a third-party reader (pyarrow)
// read the on-disk file as a third, different answer again. The file was wrong *and* the reader was
// wrong.
//
// It survived because of which data reaches this encoding and which data the tests used.
// schema_mapping.cc gives DELTA_BINARY_PACKED to `bigint` and `timestamp` *key* columns, on the
// argument that a clustering key ascends. A clustering key does, in small steps, and every
// fixed-schema test in the tree has one: residual widths stay far below 57 and nothing breaks. A
// **partition key** does not. Partitions arrive in token order, so the value is repeated for every
// row of a partition (delta 0) and then jumps by an arbitrary amount at the boundary -- which puts
// a near-full-width residual and a run of zeros in the same miniblock, the exact combination that
// needs more than 57 bits. That is why this only ever showed up under
// `make_random_schema_specification`, and there as a *misaligned partition sequence*: with the
// first key component decoded differently on every row, one partition read back as one partition
// per row, at different tokens (§9.6b).
//
// So the sweep below is the point of the test: for every residual width 1..64, one miniblock holding
// a near-full-width delta next to a run of zeros. It has to be built that way. Stepping the values
// by 2^(w-1) instead -- the obvious way to write a width sweep -- makes the deltas *uniform*, so
// min_delta absorbs the whole magnitude and every iteration exercises a residual width of about
// zero. That version of this sweep reported "all ok" against the broken codec.
SEASTAR_THREAD_TEST_CASE(test_delta_binary_packed_wide_residual_widths) {
    using namespace sstables::parquet::format;

    // A run of equal values, one jump of exactly `w` significant bits, then more equal values.
    // min_delta is 0 (the zeros), so the miniblock's residual width is exactly w -- and the values
    // after the jump are the ones the unpacker's over-read corrupts.
    for (int w = 1; w <= 64; ++w) {
        const uint64_t jump = (w == 64) ? ~0ull : ((1ull << w) - 1);
        std::vector<int64_t> vals;
        for (int i = 0; i < 4; ++i) { vals.push_back(7); }
        for (int i = 0; i < 40; ++i) { vals.push_back(int64_t(uint64_t(7) + jump)); }

        std::vector<uint8_t> buf;
        encode_delta_binary_packed(buf, vals);
        auto back = decode_delta_binary_packed(buf, vals.size());

        BOOST_TEST_CONTEXT("residual width " << w) {
            BOOST_REQUIRE_EQUAL(back.size(), vals.size());
            for (size_t i = 0; i < vals.size(); ++i) {
                BOOST_REQUIRE_EQUAL(back[i], vals[i]);
            }
        }
    }

    // And the shape as it actually arrives from a partition-key column: 20 distinct 64-bit values,
    // each repeated for the rows of its partition, in token order rather than value order. The
    // run *structure* is asserted as well as the values, because that is what the sstable reader
    // uses to decide where one partition ends and the next begins -- pre-fix this decoded as 401
    // runs instead of 20, and the reader duly produced 401 partitions from 20.
    const int64_t pk[] = {4740290627562976600LL, 5998643611329458451LL, 703819757810541764LL,
                          7173038666208164016LL, 8919275973775401134LL, 8055964333455730004LL,
                          5080213989483021976LL, 1421901242253940379LL, 3846349041273635051LL,
                          5061911113147656751LL, 7114177250487671933LL, 4021343193883838991LL,
                          7828961378139819765LL, 2717804964377853169LL, 95684721799680348LL,
                          1662694454617476799LL, 899588727426460270LL, 2047525607341836494LL,
                          2059496691679083758LL, 8235181314010893001LL};
    const int rows[] = {78, 66, 97, 50, 36, 20, 17, 22, 49, 69, 51, 49, 34, 10, 72, 46, 31, 35, 31, 23};
    static_assert(std::size(pk) == std::size(rows));

    std::vector<int64_t> col;
    for (size_t p = 0; p < std::size(pk); ++p) {
        for (int r = 0; r < rows[p]; ++r) { col.push_back(pk[p]); }
    }

    std::vector<uint8_t> buf;
    encode_delta_binary_packed(buf, col);
    auto back = decode_delta_binary_packed(buf, col.size());
    BOOST_REQUIRE_EQUAL(back.size(), col.size());

    size_t runs = 0;
    for (size_t i = 0; i < back.size(); ++i) {
        if (i == 0 || back[i] != back[i - 1]) { ++runs; }
        BOOST_REQUIRE_EQUAL(back[i], col[i]);
    }
    BOOST_REQUIRE_EQUAL(runs, std::size(pk));
}

// The same defect, in the other bit-packing loop in the same directory.
//
// rle_bitpack.hh packs and unpacks through the identical `uint64_t` accumulator, so it had the
// identical bug. This one is **latent rather than live**: in production the RLE/bit-packed hybrid
// carries definition and repetition levels and dictionary indices, and `bit_width_for()` never
// asks for more than 32 bits on any of them. But the code special-cases `bit_width == 64`, so it
// claims to support the full range, and the claim was false. Fixed with the encoding above and
// pinned here so the two do not drift apart.
SEASTAR_THREAD_TEST_CASE(test_rle_bit_packing_full_width_round_trip) {
    using namespace sstables::parquet::format;

    for (int w = 1; w <= 64; ++w) {
        const uint64_t top = (w == 64) ? ~0ull : ((1ull << w) - 1);
        std::vector<uint64_t> vals;
        // Derived from the widest representable value, so the high bits are set wherever the width
        // allows -- those are the bits a too-narrow accumulator drops. Masked back to w bits,
        // because a value that does not fit in its own bit width is not a legal input.
        for (int i = 0; i < 24; ++i) { vals.push_back((top ^ uint64_t(i)) & top); }

        rle_encoder enc{uint8_t(w)};
        enc.encode(vals);
        rle_decoder dec(enc.bytes(), uint8_t(w));
        auto back = dec.decode_all(vals.size());

        BOOST_TEST_CONTEXT("bit width " << w) {
            BOOST_REQUIRE_EQUAL(back.size(), vals.size());
            for (size_t i = 0; i < vals.size(); ++i) {
                BOOST_REQUIRE_EQUAL(back[i], vals[i]);
            }
        }
    }
}
