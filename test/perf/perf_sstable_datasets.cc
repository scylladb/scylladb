/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// scylla perf-sstable-datasets: compares sstable configurations on a real dataset.
//
// Input contract. --dataset names a directory with two files:
//
// - schema.cql: one `CREATE TABLE ks.name (...)` statement, with a PRIMARY KEY
//   and an optional CLUSTERING ORDER. A `CREATE KEYSPACE` statement is
//   optional. The table's own `WITH` options are kept, except `compression`,
//   which the tool replaces for each configuration. Use simple types only:
//   bigint, int, smallint, tinyint, double, float, text, ascii, boolean,
//   timestamp, uuid, timeuuid, blob, date, time, inet, decimal, varint.
// - data.jsonl: one row per line. A line is a JSON object in the syntax of
//   CQL `INSERT INTO t JSON '...'`: the keys are the column names exactly as
//   they are stored in the schema (unquoted CQL identifiers are lower case),
//   and each value has the JSON form that `INSERT ... JSON` accepts for its
//   type (for example a timestamp as an integer in milliseconds or as a
//   string, a uuid as a string, a blob as a "0x..." string, a date as
//   "YYYY-MM-DD"). Every primary key column must be present and not null.
//   A regular or static column that is absent or null gets no cell (as with
//   `INSERT ... JSON ... DEFAULT UNSET`), so the data has no tombstones.
//   Empty lines are skipped. The rows may come in any order. Two lines with
//   the same primary key merge as two INSERTs would.
//
// Every row gets a row marker and live cells, without TTL, with the write
// timestamp `--timestamp-base + line number` (`--timestamps row-order`, the
// default, which models rows inserted in file order) or `--timestamp-base`
// (`--timestamps constant`).
//
// The tool loads the rows into one memtable, and writes the same memtable to
// each configuration with the default chunk length, the default compression
// level and the default Parquet tunables:
//
//   mt_zstd       `mt`, ZstdCompressor (no dictionary). Also the source of the
//                 dictionary samples, as existing sstables are in production.
//   mt_zstd_dict  `mt`, ZstdWithDictsCompressor, with a dictionary trained as
//                 in production (sstables/dictionary_service.cc, replica::database::sample_data_files()).
//   aa_zstd       `aa` (Parquet), ZstdWithDictsCompressor, which gives plain
//                 ZSTD pages.
//
// Each read run opens every sstable anew, as a node opens it (without the
// first and last clustering positions, which test_env loads by default), so
// the per-sstable caches start empty.
//
// Each configuration ends with one sstable in <out>/<config>/. The results go
// to <out>/results.json and to stdout. Run it with -c1 and enough memory for
// the memtable, which takes 2 to 5 times the size of the JSON data (5 times
// for one small row per partition).

#include <algorithm>
#include <charconv>
#include <filesystem>
#include <fstream>
#include <random>
#include <time.h>

#include <fmt/ranges.h>
#include <seastar/core/app-template.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/closeable.hh>
#include <seastar/util/defer.hh>
#include <zstd.h>

#include "db/config.hh"
#include "message/dict_trainer.hh"
#include "partition_slice_builder.hh"
#include "readers/mutation_reader.hh"
#include "replica/memtable.hh"
#include "schema/schema_builder.hh"
#include "sstables/sstable_compressor_factory.hh"
#include "sstables/sstables.hh"
#include "sstables/sstables_manager.hh"
#include "test/lib/sstable_test_env.hh"
#include "test/perf/entry_point.hh"
#include "tools/schema_loader.hh"
#include "types/json_utils.hh"
#include "utils/alien_worker.hh"
#include "utils/chunked_vector.hh"
#include "utils/log.hh"
#include "utils/rjson.hh"
#include "utils/xx_hasher.hh"

#include <seastar/testing/linux_perf_event.hh>

using namespace sstables;
namespace fs = std::filesystem;

namespace perf {

namespace {

logging::logger pdlog("perf_sstable_datasets");

// ---------------------------------------------------------------------------
// Cost measurement.

struct cost {
    double wall = 0;          // seconds
    double thread_cpu = 0;    // seconds, CPU time of the reactor thread
    double process_cpu = 0;   // seconds, CPU time of all threads of the process
    uint64_t instructions = 0; // user mode, reactor thread
    uint64_t cycles = 0;       // user mode, reactor thread
    uint64_t read_ops = 0;
    uint64_t read_bytes = 0;
    uint64_t write_ops = 0;
    uint64_t write_bytes = 0;
    uint64_t rows = 0;
    uint64_t partitions = 0;
};

double cpu_seconds(clockid_t clk) {
    timespec ts;
    clock_gettime(clk, &ts);
    return ts.tv_sec + ts.tv_nsec * 1e-9;
}

class meter {
    linux_perf_event _instructions = linux_perf_event::user_instructions_retired();
    linux_perf_event _cycles = linux_perf_event::user_cpu_cycles_retired();
    struct snapshot {
        std::chrono::steady_clock::time_point wall;
        double thread_cpu;
        double process_cpu;
        uint64_t instructions;
        uint64_t cycles;
        reactor::io_stats io;
    };
    snapshot take() {
        return snapshot{
            .wall = std::chrono::steady_clock::now(),
            .thread_cpu = cpu_seconds(CLOCK_THREAD_CPUTIME_ID),
            .process_cpu = cpu_seconds(CLOCK_PROCESS_CPUTIME_ID),
            .instructions = _instructions.read(),
            .cycles = _cycles.read(),
            .io = engine().get_io_stats(),
        };
    }
public:
    bool perf_events_available = false;

    meter() {
        _instructions.enable();
        _cycles.enable();
        volatile uint64_t x = 0;
        auto before = _instructions.read();
        for (int i = 0; i < 1000; ++i) {
            x = x + i;
        }
        perf_events_available = _instructions.read() != before;
    }

    // Must run in a seastar thread.
    template <typename Func>
    cost measure(Func&& f) {
        auto a = take();
        f();
        auto b = take();
        return cost{
            .wall = std::chrono::duration<double>(b.wall - a.wall).count(),
            .thread_cpu = b.thread_cpu - a.thread_cpu,
            .process_cpu = b.process_cpu - a.process_cpu,
            .instructions = b.instructions - a.instructions,
            .cycles = b.cycles - a.cycles,
            .read_ops = b.io.aio_reads - a.io.aio_reads,
            .read_bytes = b.io.aio_read_bytes - a.io.aio_read_bytes,
            .write_ops = b.io.aio_writes - a.io.aio_writes,
            .write_bytes = b.io.aio_write_bytes - a.io.aio_write_bytes,
        };
    }
};

double median(std::vector<double> v) {
    if (v.empty()) {
        return 0;
    }
    std::ranges::sort(v);
    auto n = v.size();
    return n % 2 ? v[n / 2] : (v[n / 2 - 1] + v[n / 2]) / 2;
}

// ---------------------------------------------------------------------------
// JSON output.

rjson::value jstr(std::string_view s) {
    return rjson::from_string(s);
}

template <typename T>
rjson::value jnum(T v) {
    return rjson::value(v);
}

void jadd(rjson::value& o, std::string_view k, rjson::value&& v) {
    rjson::add_with_string_name(o, k, std::move(v));
}

rjson::value cost_to_json(const cost& c, bool perf_events) {
    auto o = rjson::empty_object();
    jadd(o, "wall_s", jnum(c.wall));
    jadd(o, "thread_cpu_s", jnum(c.thread_cpu));
    jadd(o, "process_cpu_s", jnum(c.process_cpu));
    jadd(o, "instructions", perf_events ? jnum(c.instructions) : rjson::null_value());
    jadd(o, "cycles", perf_events ? jnum(c.cycles) : rjson::null_value());
    jadd(o, "read_ops", jnum(c.read_ops));
    jadd(o, "read_bytes", jnum(c.read_bytes));
    jadd(o, "write_ops", jnum(c.write_ops));
    jadd(o, "write_bytes", jnum(c.write_bytes));
    jadd(o, "rows", jnum(c.rows));
    jadd(o, "partitions", jnum(c.partitions));
    return o;
}

// The runs, and the median of each field.
rjson::value runs_to_json(const std::vector<cost>& runs, bool perf_events) {
    auto o = rjson::empty_object();
    auto arr = rjson::empty_array();
    for (auto& c : runs) {
        rjson::push_back(arr, cost_to_json(c, perf_events));
    }
    jadd(o, "runs", std::move(arr));
    auto med = rjson::empty_object();
    auto field = [&] (std::string_view name, auto get, bool available = true) {
        std::vector<double> v;
        for (auto& c : runs) {
            v.push_back(double(get(c)));
        }
        jadd(med, name, available ? jnum(median(v)) : rjson::null_value());
    };
    field("wall_s", [] (const cost& c) { return c.wall; });
    field("thread_cpu_s", [] (const cost& c) { return c.thread_cpu; });
    field("process_cpu_s", [] (const cost& c) { return c.process_cpu; });
    field("instructions", [] (const cost& c) { return c.instructions; }, perf_events);
    field("cycles", [] (const cost& c) { return c.cycles; }, perf_events);
    field("read_ops", [] (const cost& c) { return c.read_ops; });
    field("read_bytes", [] (const cost& c) { return c.read_bytes; });
    field("write_ops", [] (const cost& c) { return c.write_ops; });
    field("write_bytes", [] (const cost& c) { return c.write_bytes; });
    field("rows", [] (const cost& c) { return c.rows; });
    jadd(o, "median", std::move(med));
    return o;
}

double median_of(const std::vector<cost>& runs, double (*get)(const cost&)) {
    std::vector<double> v;
    for (auto& c : runs) {
        v.push_back(get(c));
    }
    return median(v);
}

// ---------------------------------------------------------------------------
// Content digest of a fragment stream. It covers keys, tombstones, row
// markers and cells (timestamp, liveness, TTL, value), and not the way a
// reader splits the stream, so readers of different formats agree.

class content_digest {
    xx_hasher _h;
    void feed_u64(uint64_t v) {
        _h.update(reinterpret_cast<const char*>(&v), sizeof(v));
    }
    void feed_bytes(managed_bytes_view v) {
        feed_u64(v.size());
        v.with_linearized([&] (bytes_view bv) {
            _h.update(reinterpret_cast<const char*>(bv.data()), bv.size());
        });
    }
    void feed_tomb(const tombstone& t) {
        feed_u64(t.timestamp);
        feed_u64(t.deletion_time.time_since_epoch().count());
    }
    void feed_row(const schema& s, column_kind kind, const row& r) {
        r.for_each_cell([&] (column_id id, const atomic_cell_or_collection& c) {
            auto& cdef = s.column_at(kind, id);
            feed_u64(id);
            if (!cdef.is_atomic()) {
                // Not produced by the ingestion, which takes simple types only.
                throw std::runtime_error(fmt::format("unexpected non-atomic column {}", cdef.name_as_text()));
            }
            auto acv = c.as_atomic_cell(cdef);
            feed_u64(acv.timestamp());
            feed_u64(acv.is_live());
            if (acv.is_live_and_has_ttl()) {
                feed_u64(acv.ttl().count());
                feed_u64(acv.expiry().time_since_epoch().count());
            }
            if (acv.is_live()) {
                feed_bytes(acv.value());
            } else {
                feed_u64(acv.deletion_time().time_since_epoch().count());
            }
            ++cells;
        });
    }
public:
    uint64_t partitions = 0;
    uint64_t rows = 0;
    uint64_t static_rows = 0;
    uint64_t cells = 0;
    uint64_t range_tombstone_changes = 0;

    void feed(const schema& s, const mutation_fragment_v2& mf) {
        if (mf.is_partition_start()) {
            ++partitions;
            feed_u64(1);
            feed_bytes(mf.as_partition_start().key().key().representation());
            feed_tomb(mf.as_partition_start().partition_tombstone());
        } else if (mf.is_static_row()) {
            auto& sr = mf.as_static_row();
            if (!sr.cells().empty()) {
                ++static_rows;
                feed_u64(2);
                feed_row(s, column_kind::static_column, sr.cells());
            }
        } else if (mf.is_clustering_row()) {
            auto& cr = mf.as_clustering_row();
            ++rows;
            feed_u64(3);
            feed_bytes(cr.key().representation());
            auto& m = cr.marker();
            feed_u64(m.is_missing() ? 0 : m.is_live() ? 1 : 2);
            if (!m.is_missing()) {
                feed_u64(m.timestamp());
                if (m.is_expiring()) {
                    feed_u64(m.ttl().count());
                    feed_u64(m.expiry().time_since_epoch().count());
                }
            }
            feed_tomb(cr.tomb().tomb());
            feed_row(s, column_kind::regular_column, cr.cells());
        } else if (mf.is_range_tombstone_change()) {
            ++range_tombstone_changes;
            auto& rtc = mf.as_range_tombstone_change();
            feed_u64(4);
            auto pos = fmt::format("{}", rtc.position());
            _h.update(pos.data(), pos.size());
            feed_tomb(rtc.tombstone());
        } else if (mf.is_end_of_partition()) {
            feed_u64(5);
        }
    }

    uint64_t value() {
        return _h.finalize_uint64();
    }
};

// ---------------------------------------------------------------------------
// Ingestion.

struct ingest_stats {
    uint64_t lines = 0;
    uint64_t input_bytes = 0;
    double wall = 0;
};

class row_ingester {
    schema_ptr _s;
    replica::memtable& _mt;
    api::timestamp_type _ts_base;
    bool _row_order_ts;
    std::unordered_map<std::string, const column_definition*> _columns;
public:
    uint64_t lines = 0;

    row_ingester(schema_ptr s, replica::memtable& mt, api::timestamp_type ts_base, bool row_order_ts)
        : _s(std::move(s)), _mt(mt), _ts_base(ts_base), _row_order_ts(row_order_ts) {
        for (auto& cdef : _s->all_columns()) {
            if (!cdef.is_atomic()) {
                throw std::runtime_error(fmt::format("column {} has a non-simple type {}, which the tool does not support",
                        cdef.name_as_text(), cdef.type->cql3_type_name()));
            }
            _columns.emplace(cdef.name_as_text(), &cdef);
        }
    }

    void ingest_line(std::string_view line, uint64_t line_no) {
        if (line.find_first_not_of(" \t\r") == std::string_view::npos) {
            return;
        }
        rjson::value v;
        try {
            v = rjson::parse(line);
        } catch (...) {
            throw std::runtime_error(fmt::format("data.jsonl line {}: cannot parse JSON: {}", line_no, std::current_exception()));
        }
        if (!v.IsObject()) {
            throw std::runtime_error(fmt::format("data.jsonl line {}: not a JSON object", line_no));
        }
        const auto ts = _row_order_ts ? _ts_base + api::timestamp_type(line_no) : _ts_base;
        std::vector<bytes> pk(_s->partition_key_size());
        std::vector<bytes> ck(_s->clustering_key_size());
        std::vector<bool> pk_set(pk.size()), ck_set(ck.size());
        std::vector<std::pair<const column_definition*, bytes>> cells;
        for (auto it = v.MemberBegin(); it != v.MemberEnd(); ++it) {
            auto name = rjson::to_string_view(it->name);
            auto cit = _columns.find(std::string(name));
            if (cit == _columns.end()) {
                throw std::runtime_error(fmt::format("data.jsonl line {}: unknown column \"{}\"", line_no, name));
            }
            auto& cdef = *cit->second;
            if (it->value.IsNull()) {
                if (cdef.is_primary_key()) {
                    throw std::runtime_error(fmt::format("data.jsonl line {}: primary key column {} is null", line_no, name));
                }
                continue;
            }
            bytes value;
            try {
                value = from_json_object(*cdef.type, it->value);
            } catch (...) {
                throw std::runtime_error(fmt::format("data.jsonl line {}: bad value for column {}: {}", line_no, name, std::current_exception()));
            }
            if (cdef.is_partition_key()) {
                pk[cdef.id] = std::move(value);
                pk_set[cdef.id] = true;
            } else if (cdef.is_clustering_key()) {
                ck[cdef.id] = std::move(value);
                ck_set[cdef.id] = true;
            } else {
                cells.emplace_back(&cdef, std::move(value));
            }
        }
        for (size_t i = 0; i < pk.size(); ++i) {
            if (!pk_set[i]) {
                throw std::runtime_error(fmt::format("data.jsonl line {}: partition key column {} is missing", line_no,
                        _s->partition_key_columns()[i].name_as_text()));
            }
        }
        for (size_t i = 0; i < ck.size(); ++i) {
            if (!ck_set[i]) {
                throw std::runtime_error(fmt::format("data.jsonl line {}: clustering key column {} is missing", line_no,
                        _s->clustering_key_columns()[i].name_as_text()));
            }
        }
        mutation m(_s, partition_key::from_exploded(*_s, pk));
        auto& r = m.partition().clustered_row(*_s, clustering_key::from_exploded(*_s, ck));
        r.apply(row_marker(ts));
        for (auto& [cdef, value] : cells) {
            auto cell = atomic_cell::make_live(*cdef->type, ts, bytes_view(value));
            if (cdef->is_static()) {
                m.set_static_cell(*cdef, atomic_cell_or_collection(std::move(cell)));
            } else {
                r.cells().apply(*cdef, atomic_cell_or_collection(std::move(cell)));
            }
        }
        _mt.apply(m);
        ++lines;
    }
};

// Must run in a seastar thread.
ingest_stats ingest(const sstring& path, row_ingester& ing) {
    ingest_stats st;
    auto start = std::chrono::steady_clock::now();
    auto f = open_file_dma(path, open_flags::ro).get();
    st.input_bytes = f.size().get();
    auto in = make_file_input_stream(f, 0, file_input_stream_options{.buffer_size = 1 << 20, .read_ahead = 4});
    auto close_in = defer([&] noexcept { in.close().get(); });
    std::string partial;
    uint64_t line_no = 0;
    auto last_report = start;
    while (true) {
        auto buf = in.read().get();
        if (buf.empty()) {
            break;
        }
        std::string_view sv(buf.get(), buf.size());
        while (!sv.empty()) {
            auto nl = sv.find('\n');
            if (nl == std::string_view::npos) {
                partial.append(sv);
                break;
            }
            ++line_no;
            if (!partial.empty()) {
                partial.append(sv.substr(0, nl));
                ing.ingest_line(partial, line_no);
                partial.clear();
            } else {
                ing.ingest_line(sv.substr(0, nl), line_no);
            }
            sv.remove_prefix(nl + 1);
            thread::maybe_yield();
        }
        auto now = std::chrono::steady_clock::now();
        if (now - last_report > std::chrono::seconds(10)) {
            last_report = now;
            pdlog.info("ingested {} lines ({:.1f} s)", line_no, std::chrono::duration<double>(now - start).count());
        }
    }
    if (!partial.empty()) {
        ++line_no;
        ing.ingest_line(partial, line_no);
    }
    st.lines = line_no;
    st.wall = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
    return st;
}

// ---------------------------------------------------------------------------
// Configurations.

struct sstable_config {
    sstring name;
    sstable_version_types version;
    compression_parameters::algorithm algo;
    schema_ptr schema;
    sstring dir;
    generation_type generation;
    std::vector<cost> write_runs;
    // name of the read benchmark -> runs
    std::map<sstring, std::vector<cost>> read_runs;
    std::vector<cost> open_runs;
    uint64_t full_scan_digest = 0;
};

schema_ptr with_compression(schema_ptr base, compression_parameters::algorithm algo) {
    auto name = compression_parameters::algorithm_to_name(algo);
    // Only the class: the chunk length and the level stay at their defaults.
    auto params = compression_parameters(std::map<sstring, sstring>{
        {compression_parameters::SSTABLE_COMPRESSION, sstring(name)},
    });
    return schema_builder(base).set_compressor_params(params).build();
}

struct component_size {
    sstring component;
    uint64_t bytes;
};

// The sizes of the files of the one sstable in `dir`, by component.
std::vector<component_size> component_sizes(const sstring& dir) {
    std::vector<component_size> ret;
    for (auto& e : fs::directory_iterator(std::string(dir))) {
        if (!e.is_regular_file()) {
            continue;
        }
        auto fname = e.path().filename().native();
        auto dash = fname.rfind('-');
        ret.push_back({sstring(dash == std::string::npos ? fname : fname.substr(dash + 1)), uint64_t(e.file_size())});
    }
    std::ranges::sort(ret, std::less<>(), &component_size::component);
    return ret;
}

// The head of the CompressionInfo.db file in `dir`, parsed from the file
// itself: the sstable loader drops the dictionary options from memory
// (compression::discard_hidden_options()). The layout is the compressor name
// (be16 length + bytes), then the options (be32 count, then for each a be16
// length + key and a be16 length + value). The dictionary is stored as the
// options `.dictionary.NNNNNNNN` (dict_as_options() in sstables/compressor.cc).
struct compression_info_head {
    std::string compressor;
    std::map<std::string, std::string> options; // without the dictionary
    std::vector<std::byte> dict;
};

compression_info_head read_compression_info_head(const sstring& dir) {
    std::string content;
    for (auto& e : fs::directory_iterator(std::string(dir))) {
        if (e.path().native().ends_with("-CompressionInfo.db")) {
            std::ifstream in(e.path(), std::ios::binary);
            content.assign(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
        }
    }
    if (content.empty()) {
        throw std::runtime_error(fmt::format("no CompressionInfo.db in {}", dir));
    }
    size_t pos = 0;
    auto need = [&] (size_t n) {
        if (pos + n > content.size()) {
            throw std::runtime_error(fmt::format("truncated CompressionInfo.db in {}", dir));
        }
    };
    auto be = [&] (size_t n) {
        need(n);
        uint64_t v = 0;
        for (size_t i = 0; i < n; ++i) {
            v = (v << 8) | uint8_t(content[pos++]);
        }
        return v;
    };
    auto str16 = [&] {
        auto len = be(2);
        need(len);
        auto r = content.substr(pos, len);
        pos += len;
        return r;
    };
    compression_info_head h;
    h.compressor = str16();
    auto n = be(4);
    std::map<int, std::string> parts;
    const std::string_view prefix = ".dictionary.";
    for (uint64_t i = 0; i < n; ++i) {
        auto k = str16();
        auto v = str16();
        if (k.starts_with(prefix)) {
            parts.emplace(std::stoi(k.substr(prefix.size())), std::move(v));
        } else {
            h.options.emplace(std::move(k), std::move(v));
        }
    }
    int expect = 0;
    for (auto& [i, p] : parts) {
        if (i != expect++) {
            throw std::runtime_error(fmt::format("missing dictionary part {} in {}", expect - 1, dir));
        }
        auto b = std::as_bytes(std::span(p));
        h.dict.insert(h.dict.end(), b.begin(), b.end());
    }
    return h;
}

uint64_t peak_rss_bytes() {
    std::ifstream in("/proc/self/status");
    std::string line;
    while (std::getline(in, line)) {
        if (line.starts_with("VmHWM:")) {
            return std::stoull(line.substr(6)) * 1024;
        }
    }
    return 0;
}

// ---------------------------------------------------------------------------
// Read benchmarks.

struct read_query {
    dht::partition_range pr;
    query::partition_slice slice;
};

struct read_benchmark {
    sstring name;
    sstring description;
    bool full_scan = false;
    std::vector<read_query> queries;
};

// Reads `rd` to the end. Must run in a seastar thread.
template <typename Func>
void consume_all(mutation_reader& rd, Func&& f) {
    auto close = deferred_close(rd);
    rd.consume_pausable([&] (mutation_fragment_v2 mf) {
        f(mf);
        return stop_iteration::no;
    }).get();
}

mutation_reader make_reader_for(const shared_sstable& sst, schema_ptr s, reader_permit permit, const read_benchmark& b, const read_query* q) {
    if (b.full_scan) {
        return sst->make_full_scan_reader(s, std::move(permit));
    }
    return sst->make_reader(s, std::move(permit), q->pr, q->slice);
}

// Runs every query of `b` on `sst`, counting rows and partitions.
void run_benchmark(test_env& env, const shared_sstable& sst, schema_ptr s, const read_benchmark& b, cost& c, content_digest* digest) {
    auto run_one = [&] (const read_query* q) {
        auto rd = make_reader_for(sst, s, env.make_reader_permit(), b, q);
        consume_all(rd, [&] (const mutation_fragment_v2& mf) {
            if (mf.is_clustering_row()) {
                ++c.rows;
            } else if (mf.is_partition_start()) {
                ++c.partitions;
            }
            if (digest) {
                digest->feed(*s, mf);
            }
        });
    };
    if (b.full_scan) {
        run_one(nullptr);
    } else {
        for (auto& q : b.queries) {
            run_one(&q);
        }
    }
}

// The clustering keys at the given (sorted) indexes of a partition of the memtable.
std::vector<clustering_key> clustering_keys_at(test_env& env, replica::memtable& mt, schema_ptr s, const dht::decorated_key& dk, std::vector<uint64_t> idx) {
    std::vector<clustering_key> ret;
    auto pr = dht::partition_range::make_singular(dk);
    auto rd = mt.make_mutation_reader(s, env.make_reader_permit(), pr, s->full_slice());
    uint64_t i = 0;
    size_t next = 0;
    consume_all(rd, [&] (const mutation_fragment_v2& mf) {
        if (mf.is_clustering_row()) {
            while (next < idx.size() && idx[next] == i) {
                ret.push_back(mf.as_clustering_row().key());
                ++next;
            }
            ++i;
        }
    });
    if (ret.size() != idx.size()) {
        throw std::runtime_error("internal error: clustering key index out of range");
    }
    return ret;
}

void print_cost_line(const char* label, const std::vector<cost>& runs, bool perf_events) {
    auto ins = median_of(runs, [] (const cost& c) { return double(c.instructions); });
    fmt::print("    {:<16} wall {:8.3f} s  cpu {:8.3f} s  {}  read {:>12} B / {:>8} ops  write {:>12} B  rows {}\n",
            label,
            median_of(runs, [] (const cost& c) { return c.wall; }),
            median_of(runs, [] (const cost& c) { return c.thread_cpu; }),
            perf_events ? fmt::format("insns {:>14.0f}", ins) : std::string("insns n/a"),
            uint64_t(median_of(runs, [] (const cost& c) { return double(c.read_bytes); })),
            uint64_t(median_of(runs, [] (const cost& c) { return double(c.read_ops); })),
            uint64_t(median_of(runs, [] (const cost& c) { return double(c.write_bytes); })),
            uint64_t(median_of(runs, [] (const cost& c) { return double(c.rows); })));
}

} // anonymous namespace

int scylla_sstable_datasets_main(int argc, char** argv) {
    namespace bpo = boost::program_options;
    app_template::config app_cfg;
    app_cfg.name = "perf-sstable-datasets";
    app_template app(std::move(app_cfg));
    app.add_options()
        ("dataset", bpo::value<sstring>()->required(), "directory with schema.cql and data.jsonl")
        ("out", bpo::value<sstring>()->required(), "output directory (created; must not contain the config subdirectories)")
        ("runs", bpo::value<unsigned>()->default_value(3), "repetitions of every write and read benchmark")
        ("keys", bpo::value<unsigned>()->default_value(1000), "number of random queries per read benchmark")
        ("seed", bpo::value<uint64_t>()->default_value(1), "random seed for the queries and the dictionary samples")
        ("timestamps", bpo::value<sstring>()->default_value("row-order"), "row-order: timestamp-base + line number; constant: timestamp-base")
        ("timestamp-base", bpo::value<int64_t>()->default_value(1'700'000'000'000'000), "write timestamp base, in microseconds")
        ("dict-samples", bpo::value<uint64_t>()->default_value(4096), "number of dictionary training samples (production: 4096)")
        ("skip-reads", "only measure writes and sizes");

    return app.run(argc, argv, [&app] {
        return seastar::async([&app] {
            auto& opts = app.configuration();
            if (smp::count != 1) {
                throw std::runtime_error("run with -c1");
            }
            const auto dataset = opts["dataset"].as<sstring>();
            const auto out = opts["out"].as<sstring>();
            const auto runs = opts["runs"].as<unsigned>();
            const auto keys = opts["keys"].as<unsigned>();
            const auto seed = opts["seed"].as<uint64_t>();
            const auto ts_mode = opts["timestamps"].as<sstring>();
            if (ts_mode != "row-order" && ts_mode != "constant") {
                throw std::runtime_error("--timestamps must be row-order or constant");
            }
            const auto n_dict_samples = opts["dict-samples"].as<uint64_t>();
            const bool skip_reads = opts.contains("skip-reads");

            meter m;
            auto results = rjson::empty_object();
            jadd(results, "dataset", jstr(dataset));
            jadd(results, "perf_events_available", rjson::value(m.perf_events_available));
            jadd(results, "seed", jnum(seed));
            jadd(results, "runs", jnum(runs));
            jadd(results, "keys", jnum(keys));
            jadd(results, "timestamps", jstr(ts_mode));
            jadd(results, "shard_memory_bytes", jnum(uint64_t(memory::stats().total_memory())));

            db::config dbcfg;
            auto base = tools::load_one_schema_from_file(dbcfg, fs::path(std::string(dataset)) / "schema.cql").get();
            fmt::print("schema: {}.{}: {} partition key, {} clustering key, {} static, {} regular columns\n",
                    base->ks_name(), base->cf_name(), base->partition_key_size(), base->clustering_key_size(),
                    base->static_columns_count(), base->regular_columns_count());
            jadd(results, "table", jstr(fmt::format("{}.{}", base->ks_name(), base->cf_name())));

            auto scf = make_sstable_compressor_factory_for_tests_in_thread();
            test_env env({}, *scf);
            auto stop_env = defer([&env] noexcept { env.stop().get(); });

            // --- Ingestion.
            auto mt = make_lw_shared<replica::memtable>(base);
            row_ingester ing(base, *mt, opts["timestamp-base"].as<int64_t>(), ts_mode == "row-order");
            auto ist = ingest(sstring((fs::path(std::string(dataset)) / "data.jsonl").native()), ing);
            auto occ = mt->occupancy();
            fmt::print("ingested {} lines, {} bytes in {:.1f} s ({:.0f} rows/s); memtable: {} partitions, {} bytes used, {} bytes total\n",
                    ing.lines, ist.input_bytes, ist.wall, ing.lines / ist.wall, mt->partition_count(), occ.used_space(), occ.total_space());
            {
                auto o = rjson::empty_object();
                jadd(o, "input_lines", jnum(ing.lines));
                jadd(o, "input_bytes", jnum(ist.input_bytes));
                jadd(o, "wall_s", jnum(ist.wall));
                jadd(o, "rows_per_s", jnum(ing.lines / ist.wall));
                jadd(o, "memtable_used_bytes", jnum(uint64_t(occ.used_space())));
                jadd(o, "memtable_total_bytes", jnum(uint64_t(occ.total_space())));
                jadd(results, "ingestion", std::move(o));
            }

            // --- One pass over the memtable: keys, row counts, digest.
            utils::chunked_vector<dht::decorated_key> pkeys;
            utils::chunked_vector<uint64_t> row_prefix; // rows before partition i
            content_digest source_digest;
            {
                auto rd = mt->make_mutation_reader(base, env.make_reader_permit());
                uint64_t rows = 0;
                consume_all(rd, [&] (const mutation_fragment_v2& mf) {
                    if (mf.is_partition_start()) {
                        pkeys.push_back(mf.as_partition_start().key());
                        row_prefix.push_back(rows);
                    } else if (mf.is_clustering_row()) {
                        ++rows;
                    }
                    source_digest.feed(*base, mf);
                });
                row_prefix.push_back(rows);
            }
            const uint64_t n_partitions = pkeys.size();
            const uint64_t n_rows = row_prefix.back();
            const uint64_t expected_digest = source_digest.value();
            fmt::print("content: {} partitions, {} rows, {} static rows, {} cells, digest {:016x}\n",
                    n_partitions, n_rows, source_digest.static_rows, source_digest.cells, expected_digest);
            jadd(results, "partitions", jnum(n_partitions));
            jadd(results, "rows", jnum(n_rows));
            jadd(results, "cells", jnum(source_digest.cells));
            jadd(results, "content_digest", jstr(fmt::format("{:016x}", expected_digest)));
            if (n_partitions == 0) {
                throw std::runtime_error("the dataset is empty");
            }

            // --- The cost of the source alone: reading the memtable, as the writers do.
            std::vector<cost> source_runs;
            for (unsigned r = 0; r < runs; ++r) {
                uint64_t rows = 0, partitions = 0;
                auto c = m.measure([&] {
                    auto rd = mt->make_mutation_reader(base, env.make_reader_permit());
                    consume_all(rd, [&] (const mutation_fragment_v2& mf) {
                        rows += mf.is_clustering_row();
                        partitions += mf.is_partition_start();
                    });
                });
                c.rows = rows;
                c.partitions = partitions;
                source_runs.push_back(c);
            }
            jadd(results, "source_memtable_scan", runs_to_json(source_runs, m.perf_events_available));

            // --- Defaults.
            auto wcfg = env.manager().configure_writer("perf_sstable_datasets");
            const auto default_params = compression_parameters(compression_parameters::algorithm::zstd_with_dicts);
            {
                auto o = rjson::empty_object();
                jadd(o, "chunk_length", jnum(default_params.chunk_length()));
                jadd(o, "zstd_level", jnum(default_params.zstd_compression_level().value_or(ZSTD_defaultCLevel())));
                jadd(o, "parquet_row_group_size", jnum(wcfg.parquet_row_group_size));
                jadd(o, "parquet_page_set_size", jnum(wcfg.parquet_page_set_size));
                jadd(o, "parquet_min_page_size", jnum(wcfg.parquet_min_page_size));
                jadd(o, "parquet_dictionary_page_limit", jnum(wcfg.parquet_dictionary_page_limit));
                jadd(o, "parquet_writer_memory_fraction", jnum(env.db_config().parquet_writer_memory_fraction()));
                jadd(o, "dict_training_chunk_size", jnum(default_params.chunk_length()));
                jadd(o, "dict_training_samples", jnum(n_dict_samples));
                jadd(o, "dict_max_size", jnum(netw::zdict_train_config{}.max_dict_size));
                jadd(results, "defaults", std::move(o));
            }
            fmt::print("defaults: chunk_length {} B, zstd level {}, parquet row group {} B (budget fraction {} of {} B), page set {} B, min page {} B, dictionary page limit {} B\n",
                    default_params.chunk_length(), default_params.zstd_compression_level().value_or(ZSTD_defaultCLevel()),
                    wcfg.parquet_row_group_size, env.db_config().parquet_writer_memory_fraction(), memory::stats().total_memory(),
                    wcfg.parquet_page_set_size, wcfg.parquet_min_page_size, wcfg.parquet_dictionary_page_limit);

            using algo = compression_parameters::algorithm;
            std::vector<sstable_config> configs;
            configs.push_back({.name = "mt_zstd", .version = sstable_version_types::mt, .algo = algo::zstd});
            configs.push_back({.name = "mt_zstd_dict", .version = sstable_version_types::mt, .algo = algo::zstd_with_dicts});
            configs.push_back({.name = "aa_zstd", .version = sstable_version_types::aa, .algo = algo::zstd_with_dicts});
            fs::create_directories(std::string(out));
            for (auto& c : configs) {
                c.schema = with_compression(base, c.algo);
                c.dir = fmt::format("{}/{}", out, c.name);
                if (fs::exists(std::string(c.dir)) && !fs::is_empty(std::string(c.dir))) {
                    throw std::runtime_error(fmt::format("{} is not empty", c.dir));
                }
                fs::create_directories(std::string(c.dir));
            }

            // Opens the sstable of `c` as a node does: test_env::reusable_sst()
            // also loads the first and last clustering positions by default,
            // which reads the first and the last partition, and which the
            // node does not do (sstable_open_config).
            auto open_sst = [&] (const sstable_config& c) {
                return env.reusable_sst(c.schema, c.dir, c.generation, c.version,
                        sstable_open_config{.load_first_and_last_position_metadata = false}).get();
            };

            // Writes the memtable to `c`, `runs` times, and keeps the last sstable.
            auto write_config = [&] (sstable_config& c) {
                shared_sstable prev;
                for (unsigned r = 0; r < runs; ++r) {
                    if (prev) {
                        prev->unlink().get();
                        prev = {};
                    }
                    auto sst = env.make_sstable(c.schema, c.dir, env.new_generation(), c.version);
                    // The memtable's reader has the memtable's schema, which differs from
                    // c.schema only in the compression option, so no upgrade is needed.
                    auto rd = mt->make_mutation_reader(base, env.make_reader_permit());
                    auto cst = m.measure([&] {
                        sst->write_components(std::move(rd), n_partitions, c.schema, wcfg, mt->get_encoding_stats()).get();
                    });
                    cst.rows = n_rows;
                    cst.partitions = n_partitions;
                    fmt::print("write {} run {}: wall {:.3f} s, cpu {:.3f} s, wrote {} B\n", c.name, r, cst.wall, cst.thread_cpu, cst.write_bytes);
                    c.write_runs.push_back(cst);
                    c.generation = sst->generation();
                    prev = std::move(sst);
                }
            };

            // --- mt_zstd, then the dictionary, sampled from it.
            write_config(configs[0]);
            std::vector<std::byte> dict;
            {
                auto sst = open_sst(configs[0]);
                // As replica::database::sample_data_files(): `n` chunks of
                // `chunk_size` bytes of the uncompressed Data file, at aligned
                // offsets chosen uniformly with replacement, then sorted
                // (compute_random_sorted_ints()). The retrain_dict API uses
                // 4096 chunks of 4096 bytes; the autotrainer uses 4096 chunks
                // of chunk_length() bytes, 4096 by default.
                const uint64_t chunk_size = default_params.chunk_length();
                const uint64_t total_chunks = sst->data_size() / chunk_size;
                if (total_chunks == 0) {
                    throw std::runtime_error("the data is too small to sample a dictionary");
                }
                std::minstd_rand rng(seed);
                std::uniform_int_distribution<uint64_t> dist(0, total_chunks - 1);
                std::vector<uint64_t> chosen;
                for (uint64_t i = 0; i < n_dict_samples; ++i) {
                    chosen.push_back(dist(rng));
                }
                std::ranges::sort(chosen);
                std::vector<std::vector<std::byte>> samples;
                for (auto chunk : chosen) {
                    auto buf = sst->data_read(chunk * chunk_size, chunk_size, env.make_reader_permit()).get();
                    auto b = std::as_bytes(std::span(buf.get(), buf.size()));
                    samples.emplace_back(b.begin(), b.end());
                }
                // As dictionary_service::train_dict(): zdict_train() with the
                // default config, on a separate thread.
                utils::alien_worker trainer(pdlog, 19, "zdict");
                auto start = std::chrono::steady_clock::now();
                dict = trainer.submit<std::vector<std::byte>>([samples = std::move(samples)] {
                    return netw::zdict_train(samples, {});
                }).get();
                auto took = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
                fmt::print("dictionary: {} samples of {} B from {} chunks, trained {} B in {:.1f} s\n",
                        chosen.size(), chunk_size, total_chunks, dict.size(), took);
                if (dict.empty()) {
                    throw std::runtime_error("dictionary training returned an empty dictionary");
                }
                auto o = rjson::empty_object();
                jadd(o, "samples", jnum(uint64_t(chosen.size())));
                jadd(o, "sample_size", jnum(chunk_size));
                jadd(o, "population_chunks", jnum(total_chunks));
                jadd(o, "size", jnum(uint64_t(dict.size())));
                jadd(o, "train_wall_s", jnum(took));
                jadd(results, "dictionary", std::move(o));
                std::ofstream(std::string(out) + "/dict.bin", std::ios::binary).write(reinterpret_cast<const char*>(dict.data()), dict.size());
            }
            scf->set_recommended_dict(base->id(), dict).get();

            write_config(configs[1]);
            write_config(configs[2]);

            // --- Sizes, and the check that the dictionary was used.
            uint64_t mt_uncompressed = 0;
            auto sizes = rjson::empty_object();
            for (auto& c : configs) {
                auto sst = open_sst(c);
                auto o = rjson::empty_object();
                auto comps = rjson::empty_object();
                uint64_t total = 0;
                for (auto& cs : component_sizes(c.dir)) {
                    jadd(comps, cs.component, jnum(cs.bytes));
                    total += cs.bytes;
                }
                jadd(o, "components", std::move(comps));
                jadd(o, "total_bytes", jnum(total));
                jadd(o, "data_file_bytes", jnum(sst->ondisk_data_size()));
                // mt: the uncompressed Data length; aa: the raw size (Scylla.db).
                jadd(o, "data_size", jnum(sst->data_size()));
                jadd(o, "version", jstr(fmt::format("{}", c.version)));
                jadd(o, "compression", jstr(compression_parameters::algorithm_to_name(c.algo)));
                if (c.version == sstable_version_types::mt) {
                    mt_uncompressed = sst->data_size();
                    auto ci = read_compression_info_head(c.dir);
                    auto& sst_dict = ci.dict;
                    const bool want = c.algo == algo::zstd_with_dicts;
                    const bool same = sst_dict.size() == dict.size() && std::ranges::equal(sst_dict, dict);
                    fmt::print("{}: CompressionInfo.db: compressor {}, options {}, dictionary of {} B{}\n", c.name, ci.compressor, ci.options, sst_dict.size(),
                            sst_dict.empty() ? "" : same ? " (equal to the trained one)" : " (DIFFERENT from the trained one)");
                    jadd(o, "compression_info_compressor", jstr(ci.compressor));
                    if (want && !same) {
                        throw std::runtime_error(fmt::format("{}: the sstable does not carry the trained dictionary", c.name));
                    }
                    if (!want && !sst_dict.empty()) {
                        throw std::runtime_error(fmt::format("{}: the sstable unexpectedly carries a dictionary", c.name));
                    }
                    jadd(o, "dictionary_bytes_in_sstable", jnum(uint64_t(sst_dict.size())));
                    jadd(o, "dictionary_matches_trained", rjson::value(same));
                }
                jadd(sizes, c.name, std::move(o));
            }
            for (auto& c : configs) {
                auto& o = rjson::get(sizes, c.name);
                auto total = rjson::get(o, "total_bytes").GetUint64();
                auto data = rjson::get(o, "data_file_bytes").GetUint64();
                jadd(o, "ratio_total_to_mt_uncompressed", jnum(double(total) / mt_uncompressed));
                jadd(o, "ratio_data_to_mt_uncompressed", jnum(double(data) / mt_uncompressed));
                jadd(o, "ratio_total_to_input", jnum(double(total) / ist.input_bytes));
            }
            jadd(results, "mt_uncompressed_data_bytes", jnum(mt_uncompressed));
            jadd(results, "sizes", std::move(sizes));

            // --- Read benchmarks.
            std::vector<read_benchmark> benchmarks;
            if (!skip_reads) {
                std::mt19937_64 rng(seed);
                benchmarks.push_back({.name = "full_scan", .description = "make_full_scan_reader() over the whole sstable", .full_scan = true});
                {
                    read_benchmark b{.name = "partition", .description = fmt::format("{} random whole partitions", keys)};
                    std::uniform_int_distribution<uint64_t> d(0, n_partitions - 1);
                    for (unsigned i = 0; i < keys; ++i) {
                        b.queries.push_back({dht::partition_range::make_singular(pkeys[d(rng)]), base->full_slice()});
                    }
                    benchmarks.push_back(std::move(b));
                }
                if (base->clustering_key_size() > 0 && n_rows > 0) {
                    read_benchmark b{.name = "slice", .description = fmt::format("{} random clustering ranges of min(100, max(1, rows/10)) rows", keys)};
                    std::uniform_int_distribution<uint64_t> d(0, n_rows - 1);
                    for (unsigned i = 0; i < keys; ++i) {
                        // A partition with probability proportional to its rows.
                        auto row = d(rng);
                        size_t p = std::upper_bound(row_prefix.begin(), row_prefix.end(), row) - row_prefix.begin() - 1;
                        uint64_t n = row_prefix[p + 1] - row_prefix[p];
                        uint64_t len = std::min<uint64_t>(100, std::max<uint64_t>(1, n / 10));
                        uint64_t start = std::uniform_int_distribution<uint64_t>(0, n - len)(rng);
                        auto cks = clustering_keys_at(env, *mt, base, pkeys[p], {start, start + len - 1});
                        auto range = query::clustering_range::make({cks[0], true}, {cks[1], true});
                        b.queries.push_back({dht::partition_range::make_singular(pkeys[p]),
                                partition_slice_builder(*base).with_range(std::move(range)).build()});
                    }
                    benchmarks.push_back(std::move(b));
                }
                if (n_rows > 0) {
                    read_benchmark b{.name = "row", .description = fmt::format("{} random single rows by full primary key", keys)};
                    std::uniform_int_distribution<uint64_t> d(0, n_rows - 1);
                    for (unsigned i = 0; i < keys; ++i) {
                        auto row = d(rng);
                        size_t p = std::upper_bound(row_prefix.begin(), row_prefix.end(), row) - row_prefix.begin() - 1;
                        auto slice = base->full_slice();
                        if (base->clustering_key_size() > 0) {
                            auto cks = clustering_keys_at(env, *mt, base, pkeys[p], {row - row_prefix[p]});
                            slice = partition_slice_builder(*base).with_range(query::clustering_range::make_singular(cks[0])).build();
                        }
                        b.queries.push_back({dht::partition_range::make_singular(pkeys[p]), std::move(slice)});
                    }
                    benchmarks.push_back(std::move(b));
                }
            } else {
                benchmarks.push_back({.name = "full_scan", .description = "make_full_scan_reader() over the whole sstable", .full_scan = true});
            }

            // --- Correctness: the full scan of every configuration must give the
            // memtable's content, and every query must give the same result in
            // every configuration.
            {
                std::map<sstring, std::pair<uint64_t, sstring>> query_digests;
                for (auto& c : configs) {
                    auto sst = open_sst(c);
                    for (auto& b : benchmarks) {
                        content_digest dg;
                        cost unused;
                        run_benchmark(env, sst, c.schema, b, unused, &dg);
                        auto v = dg.value();
                        if (b.full_scan) {
                            c.full_scan_digest = v;
                            if (v != expected_digest || dg.rows != n_rows || dg.partitions != n_partitions) {
                                throw std::runtime_error(fmt::format("VALIDATION FAILED: full scan of {} gives digest {:016x}, {} partitions, {} rows; "
                                        "the source has {:016x}, {} partitions, {} rows",
                                        c.name, v, dg.partitions, dg.rows, expected_digest, n_partitions, n_rows));
                            }
                        }
                        auto [it, inserted] = query_digests.emplace(b.name, std::pair(v, c.name));
                        if (!inserted && it->second.first != v) {
                            throw std::runtime_error(fmt::format("VALIDATION FAILED: {} reads give digest {:016x} on {} and {:016x} on {}",
                                    b.name, it->second.first, it->second.second, v, c.name));
                        }
                    }
                }
                fmt::print("validation: full scans of all configurations match the source; query results match across configurations\n");
                auto o = rjson::empty_object();
                for (auto& [name, d] : query_digests) {
                    jadd(o, name, jstr(fmt::format("{:016x}", d.first)));
                }
                jadd(results, "validation_digests", std::move(o));
            }

            // The memtable is not needed any more.
            pkeys = {};
            row_prefix = {};
            mt = {};

            // --- Read runs. Every (run, benchmark, configuration) opens the
            // sstable anew, so per-sstable caches start empty.
            if (!skip_reads) {
                for (unsigned r = 0; r < runs; ++r) {
                    for (auto& b : benchmarks) {
                        for (auto& c : configs) {
                            shared_sstable sst;
                            auto open_cost = m.measure([&] {
                                sst = open_sst(c);
                            });
                            c.open_runs.push_back(open_cost);
                            cost rc;
                            auto measured = m.measure([&] {
                                run_benchmark(env, sst, c.schema, b, rc, nullptr);
                            });
                            measured.rows = rc.rows;
                            measured.partitions = rc.partitions;
                            c.read_runs[b.name].push_back(measured);
                            sst = {};
                        }
                    }
                }
            }

            // --- Output.
            auto jconfigs = rjson::empty_object();
            for (auto& c : configs) {
                auto o = rjson::empty_object();
                jadd(o, "dir", jstr(c.dir));
                jadd(o, "write", runs_to_json(c.write_runs, m.perf_events_available));
                if (!c.open_runs.empty()) {
                    jadd(o, "open", runs_to_json(c.open_runs, m.perf_events_available));
                }
                auto reads = rjson::empty_object();
                for (auto& [name, rr] : c.read_runs) {
                    jadd(reads, name, runs_to_json(rr, m.perf_events_available));
                }
                jadd(o, "read", std::move(reads));
                jadd(jconfigs, c.name, std::move(o));
            }
            jadd(results, "configs", std::move(jconfigs));
            auto jbench = rjson::empty_object();
            for (auto& b : benchmarks) {
                jadd(jbench, b.name, jstr(b.description));
            }
            jadd(results, "read_benchmarks", std::move(jbench));
            jadd(results, "peak_rss_bytes", jnum(peak_rss_bytes()));
            {
                std::ofstream f(std::string(out) + "/results.json");
                f << rjson::print(results) << "\n";
            }

            fmt::print("\n=== summary: {} ({} rows, {} partitions, input {} B, mt uncompressed Data {} B) ===\n",
                    dataset, n_rows, n_partitions, ist.input_bytes, mt_uncompressed);
            fmt::print("source (memtable scan): wall {:.3f} s, cpu {:.3f} s (median)\n",
                    median_of(source_runs, [] (const cost& c) { return c.wall; }),
                    median_of(source_runs, [] (const cost& c) { return c.thread_cpu; }));
            auto& jsizes = rjson::get(results, "sizes");
            for (auto& c : configs) {
                auto& o = rjson::get(jsizes, c.name);
                fmt::print("{}:\n    size: total {} B, Data {} B, total/mt-uncompressed {:.4f}, Data/mt-uncompressed {:.4f}\n",
                        c.name, rjson::get(o, "total_bytes").GetUint64(), rjson::get(o, "data_file_bytes").GetUint64(),
                        rjson::get(o, "ratio_total_to_mt_uncompressed").GetDouble(), rjson::get(o, "ratio_data_to_mt_uncompressed").GetDouble());
                print_cost_line("write", c.write_runs, m.perf_events_available);
                if (!c.open_runs.empty()) {
                    print_cost_line("open", c.open_runs, m.perf_events_available);
                }
                for (auto& [name, rr] : c.read_runs) {
                    print_cost_line(name.c_str(), rr, m.perf_events_available);
                }
            }
            fmt::print("peak RSS {} B; results in {}/results.json\n", peak_rss_bytes(), out);
        });
    });
}

} // namespace perf
