/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// A CPU oriented micro benchmark of the logstor hot path. It drives a logstor instance directly -
// no CQL, no coordinator, no replica, no database - and measures the steps of a read and of a write
// both together and one at a time, so that the cost of a change can be attributed to the step it
// changed. Use it to iterate on a change to the hot path: it starts in seconds, and the steps that
// touch neither the disk nor the caches have almost no variance.
//
// For the cost of an operation as a workload sees it, and for the two write paths of the two
// storage engines side by side, use `scylla perf-simple-query --logstor` instead. What each of the
// two tools answers, what the per operation IO and cache counters of a run say about which path it
// measured, and how to compare a logstor run against the sstable baseline are in
// docs/dev/logstor.md.
//
//
// Running it
// ==========
//
// A `release` build, one shard pinned to a core, the reactor's idle polling off, and the data on
// the kind of disk the numbers are meant to describe:
//
//   TMPDIR=/nvme taskset -c 2 build/release/test/perf/perf_logstor --smp 1 \
//       --idle-poll-time-us 0 --concurrency 100 --test all
//
// One step, with a profile's worth of duration:
//
//   perf_logstor --smp 1 --idle-poll-time-us 0 --test segment-read --duration 30
//
// What logstor adds to a read of a record, and what a write costs beyond the sum of its steps:
//
//   perf_logstor --test raw-read,segment-read
//   perf_logstor --test build-mutation,freeze,record-sizes,append,index-insert,index-lookup,write
//
// The cost of a row shape, which is what a change to the format of a record is measured against,
// and what a buffer amortizes over the writes that share it:
//
//   perf_logstor --test serialize,write --columns 30 --value-size 20 --compaction 0 \
//       --disk-size-in-mb 4096
//   perf_logstor --test write --concurrency 1     # ... and 16, and 64
//
// What a record takes on disk, for a matrix of row shapes, with no logstor and no disk in it:
//
//   perf_logstor --record-size-report --report-columns 1,5,30 --report-value-sizes 20,300
//
// Options:
//
//   --test                       comma separated test names, or `all`
//   --partitions                 partitions written per shard
//   --columns                    value columns of the table. The description of the schema a record
//                                carries and the framing of its cells both scale with this
//   --value-size                 bytes of the value of one column
//   --select-columns             how many of the value columns a read asks for, the first ones of
//                                the table; 0, the default, asks for all of them. The slice is
//                                given to the whole reads - `read-cached`, `query-cached` and
//                                `read-disk` - and to none of the single steps
//   --concurrency                operations in flight per shard, for the tests that wait for the disk
//   --duration                   one second iterations per test
//   --operations-per-shard       a fixed number of operations per shard instead, for comparing builds
//   --segment-size-in-kb,
//   --file-size-in-mb,
//   --disk-size-in-mb            the geometry of the segment pool of a shard
//   --compaction                 let compaction run. On by default, see below
//   --dir                        where to put the logstor files. Defaults to a temporary directory
//   --json-result                write one file per test, suffixed with the name of the test
//   --record-size-report         print what a record takes on disk for a matrix of row shapes and
//                                exit, without building a logstor
//   --report-key-sizes,
//   --report-columns,
//   --report-value-sizes         the partition key sizes, the column counts and the column value
//                                sizes of that matrix
//
// The dataset is written once and serves every test. Its index has the cache enabled, and
// `read-disk` takes the segment by bypassing the cache per read rather than by disabling it, so
// that both read paths are measured against the same data. The run prints what one record of the
// dataset takes in a segment against the bytes of the row it carries, and warns when the dataset
// does not leave the pool room for the dead records the overwrites of the write test will leave
// behind. The files are preallocated rather than sparse, as a node's are, so `--disk-size-in-mb`
// costs that much disk and that much formatting before the run starts measuring.
//
//
// The record size report
// ======================
//
// `--record-size-report` answers what a record takes on disk and where those bytes go, for as many
// row shapes as one run is given, and then exits. It builds no logstor and touches no disk, so it
// runs in a second and has no variance at all: it serializes one record per shape through the code
// the write path uses and prints the parts of it against the bytes of the row a write was handed.
// The difference between the two is what the format of a record costs, which is paid on every
// write, on every byte of the segment pool and on every read that goes to a segment, and it is what
// a change to the format is measured by. The `record:` line a measured run prints is the same
// number for the one shape that run used.
//
//
// The tests
// =========
//
// What one operation of each test does:
//
//   index-lookup     look one key up in the primary index
//   index-insert     point the index of one key at a record, which is what a write does once its
//                    record is in a segment. Includes the lookup
//   deserialize      deserialize one record from a buffer, which copies out the bytes of its
//                    `canonical_mutation`
//   materialize      turn one `canonical_mutation` into the mutation a read returns
//   build-mutation   build the mutation one write is given. Not a step of a write: a node is handed
//                    it by the layer above logstor, and only the `write` test builds one per
//                    operation, so this is what has to come off `write` before its steps add up
//   freeze           freeze one mutation into the `canonical_mutation` the record of a write carries
//   record-header    build the header of one record, which copies the decorated key of the
//                    partition into it
//   record-sizes     `record-header`, and then what `log_record_writer::compute_sizes()` does:
//                    serialize the record into a stream that only counts its bytes, so that the
//                    writer knows how much room to ask the buffer for
//   append           copy one record whose sizes are already known into the buffer of the writer
//   serialize        what a write pays before its record reaches the buffer: `freeze`,
//                    `record-header`, `record-sizes` and `append`
//   cache-lookup     look one cached mutation up and copy its partition out of the cache region.
//                    Includes the lookup of the index entry the cached partition hangs off. Draws
//                    only from the keys the cache still holds after the warming, and says how
//                    many of the dataset those are: a dataset larger than the memory of the shard
//                    leaves the LRU holding the part of it that fit
//   cache-populate   evict the cached partition of one entry and admit one in its place. Includes
//                    the lookup of that entry
//   raw-read         one DMA read of the size of a record, straight to the data file, with no
//                    logstor in it
//   read-cached      a whole read the cache serves, up to the mutation it returns. Draws from the
//                    same keys as `cache-lookup`
//   query-cached     a whole read the cache serves, as a query pays for it: the reader for the key
//                    is made, its fragments are drained and it is closed. Draws from the same keys
//                    as `cache-lookup`. What it costs beyond `read-cached` is the reader that turns
//                    the mutation into fragments, which a query pays and `read-cached` does not
//   read-disk        a whole read that goes to a segment: index lookup, DMA read, deserialization,
//                    materialization
//   segment-read     the read of the record from its segment and its deserialization, without
//                    materializing the mutation. Includes the lookup of the location, since a read
//                    starts from a key
//   write            a whole write, up to and including the flush of the buffer its record went into
//
// Everything above `raw-read` touches no disk - the two cache tests touch the cache, the rest touch
// neither it nor the disk. They run `cpu_test_batch` operations per invocation of the measurement
// loop, since one of them costs of the order of what the loop itself costs, and they run without
// concurrency, since they never wait for anything. `read-cached` and `query-cached` touch no disk
// either, but go through the read path's futures, so they run as the IO tests do.
//
//
// Reading the numbers
// ===================
//
// The steps are meant to add up, and that is the first thing to check. A whole read that goes to a
// segment costs about what the read of its record costs plus the materialization of its mutation,
// and a read the cache serves costs a fraction of either, since it copies a partition that is
// already in memory rather than deserializing one:
//
//   read-disk      ~  segment-read + materialize
//   read-cached    ~  cache-lookup
//   query-cached   ~  read-cached + the reader that fragments the mutation
//   segment-read   ~  index-lookup + raw-read + deserialize + what the segment manager puts between
//                     them
//   serialize      ~  freeze + record-sizes + append
//   write          ~  build-mutation + serialize + (index-insert - index-lookup)
//                     + the writer machinery and the separator
//
// What is left over on either side of one of these is a number in its own right: `serialize` beyond
// `freeze`, `record-sizes` and `append` is nil, and `write` beyond the rest is what the machinery
// around the record costs. Two subtractions in that last relation are easy to get wrong.
// `build-mutation` is the test's own work and has to come off `write` first - at thirty columns it
// is a third of what the write test reads. And the write path inserts into the index without
// looking the key up first, so what it pays is `index-insert - index-lookup`, not `index-insert`.
//
// The read relations do not add `index-lookup` for the opposite reason: a read starts from a key,
// so `segment-read` and `cache-lookup` each begin by looking the entry up, and it is already inside
// them. `index-lookup` is there to be subtracted from a step that carries it, not added to one.
//
// If a change moves `read-disk` without moving any of the steps under it, either the change is in
// the part of the read that none of the steps cover, or the measurement is not measuring what it
// looks like. Check the per operation counters first.
//
//
// What a run has to get right
// ===========================
//
// - Turn the reactor's idle polling off with `--idle-poll-time-us 0`. A reactor with nothing to do
//   polls, and the instructions it retires while a test waits for the disk are charged to the
//   operations of that test: the write test reads 47k instructions per operation with the default
//   idle polling and 14.6k without it, at the same throughput. Without this, two thirds of the cost
//   of a write is the reactor waiting for the disk, and a change that only moved the throughput
//   would read as a change in the cost of a write.
//
// - Give the tests that wait for the disk enough concurrency, at least 32, and read `polls/op` to
//   confirm they had it. What is left of the poll loop after the point above is a cost per turn of
//   that loop rather than per operation, so it is charged to an operation in proportion to how few
//   operations each turn served: `instructions/op = work + cost_of_a_poll * polls/op` fits to within
//   half a percent over a concurrency sweep, and for `segment-read` it gives 7150 instructions of
//   work and about 1900 per poll. At a concurrency of one that poll term is 3 polls per operation
//   and most of what the test appears to cost; by 100 it is 0.03 and nothing at all. `polls/op` well
//   under 0.1 means the number being read is the operation.
//
// - Measure a write at more than one concurrency. Sealing a buffer, reserving room for it in a
//   segment, submitting its IO and handing its records to the separator are per buffer, and the
//   records in the buffer divide them. The same write costs 31.5k instructions and writes eight
//   times its record's bytes at a concurrency of one, 15.1k at sixteen, and 14.4k from sixty-four
//   up, where it is flat. A number taken at one concurrency describes that concurrency only, and
//   the low end is the latency-bound case a workload actually cares about.
//
// - Give a write test of a wide row a pool it cannot fill, and no compaction. The write test
//   overwrites the dataset, and the wider the row the sooner the dead records fill the segment pool;
//   once they do, the test measures compaction rather than the write path, and says so with a `mad`
//   of several percent and a throughput a factor lower than the same test one shape narrower.
//   Thirty columns of 20 bytes needs `--compaction 0` and a pool of a few GB before its number
//   holds still.
//
// - Know which of the two write regimes a run is in. Whether compaction is in the number is decided
//   by whether the pool fills, not by `--compaction`: with a pool the test cannot fill, turning
//   compaction off moves the write by less than the run to run spread, because there is nothing for
//   it to reclaim. Overwriting into a pool a quarter the size of what the test writes puts it in the
//   other regime, and the per second lines show the moment it crosses over - the write went from
//   14.5k instructions and no reads to 16.3k and 651 read bytes per operation once the pool ran out.
//   Both are real; a comparison just has to be between two runs in the same one.
//
// - On a machine whose disk is shared, take the IO tests more than once and keep the clean ones. The
//   same binary and configuration returned 190k operations per second at `polls/op` 0.03 on one run
//   of `read-disk` and 47k at 1.5 on the next, which puts about 1800 instructions per poll onto the
//   number. Run the two builds alternately at each row shape rather than one side after the other,
//   and quote the median of the iterations whose `polls/op` was under 0.1.
//
// - Take the noise floor from separate processes, not from one. The `mad` a run reports is the
//   spread between its own iterations, which is the smaller of the two. Three processes of the same
//   configuration, worst deviation from their median, is what a change has to beat: 0.05% on the
//   pure CPU tests and 0.1% to 0.4% on `write` depending on how quiet the machine is.
//
//
// Profiling a step
// ================
//
// For a profile rather than a number, run the pinned single shard case under `perf`, with the perf
// test binaries keeping their symbols (`./configure.py --mode=release --perf-tests-debuginfo 1`):
//
//   perf record -g --call-graph dwarf -F 499 -e instructions:u -D 12000 \
//       -- build/release/test/perf/perf_logstor --smp 1 --test segment-read --duration 30
//
// `-e instructions:u` samples what the counters count; the default samples cycles, and a sleeping
// syscall or a memory stall is most of a cycle profile and almost none of an instruction one. `-D`
// skips the population of the dataset, which takes far longer than the measurement that follows and
// is what makes the poll loop look like the hot path when it is not. Do not read the call tree too
// literally: everything past the first `co_await` of a coroutine runs as its own task from the
// reactor's loop, so the work of a step does not appear under whatever called it.
//
// And one thing a profile of this test cannot be used for at all: the shares in it do not convert
// into instructions per operation. Recording keeps the reactor from sleeping, so the run being
// profiled is not the run that was measured - under `perf record` the write test reads 56.8k
// instructions and 31.9 polls per operation against 14.5k and 0.045 without it, and about seventy
// percent of the samples are a poll loop the measured run does not have. Renormalizing what is left
// onto the measured cost is not safe either: doing that put the sizing pass of a record at ~1900
// instructions, and a test written to measure it directly said 291. Use the profile to find which
// functions are on the path, then add a test here for the ones that look expensive - a method on
// `logstor_bench` and a name in `test_kinds`.

#include <charconv>
#include <filesystem>
#include <ranges>
#include <vector>

#include <fmt/ranges.h>
#include <json/json.h>

#include <seastar/core/align.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/simple-stream.hh>
#include <seastar/core/thread.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/testing/test_runner.hh>
#include <seastar/util/closeable.hh>

#include "dht/i_partitioner.hh"
#include "keys/keys.hh"
#include "mutation/canonical_mutation.hh"
#include "mutation/mutation.hh"
#include "mutation/mutation_partition_serializer.hh"
#include "partition_slice_builder.hh"
#include "reader_concurrency_semaphore.hh"
#include "replica/logstor/index.hh"
#include "replica/logstor/logstor.hh"
#include "replica/logstor/segment_io.hh"
#include "replica/logstor/segment_manager.hh"
#include "replica/logstor/write_buffer.hh"
#include "schema/schema_builder.hh"
#include "test/lib/logstor_test_utils.hh"
#include "test/lib/random_utils.hh"
#include "test/lib/tmpdir.hh"
#include "test/perf/perf.hh"
#include "types/types.hh"

// The record header is serialized through the same generated code the write path uses, so that what
// the size of a record costs to compute here is what it costs there.
#include "serializer_impl.hh"
#include "idl/logstor.dist.hh"
#include "idl/logstor.dist.impl.hh"

using namespace replica::logstor;

namespace {

// What one operation of a test does. The first tests are the steps of a read and of a write with no
// IO in them at all, which is where a CPU regression shows up with the least noise; the ones after
// them add the index, the disk and the caches back, up to a whole read and a whole write.
enum class test_kind {
    index_lookup,
    index_insert,
    deserialize,
    materialize,
    build_mutation,
    freeze,
    record_header,
    record_sizes,
    append,
    serialize,
    cache_lookup,
    cache_populate,
    raw_read,
    read_cached,
    query_cached,
    read_disk,
    segment_read,
    write,
};

const std::vector<std::pair<std::string_view, test_kind>> test_kinds = {
    {"index-lookup", test_kind::index_lookup},
    {"index-insert", test_kind::index_insert},
    {"deserialize", test_kind::deserialize},
    {"materialize", test_kind::materialize},
    {"build-mutation", test_kind::build_mutation},
    {"freeze", test_kind::freeze},
    {"record-header", test_kind::record_header},
    {"record-sizes", test_kind::record_sizes},
    {"append", test_kind::append},
    {"serialize", test_kind::serialize},
    {"cache-lookup", test_kind::cache_lookup},
    {"cache-populate", test_kind::cache_populate},
    {"raw-read", test_kind::raw_read},
    {"read-cached", test_kind::read_cached},
    {"query-cached", test_kind::query_cached},
    {"read-disk", test_kind::read_disk},
    {"segment-read", test_kind::segment_read},
    {"write", test_kind::write},
};

std::string_view name_of(test_kind kind) {
    for (const auto& [name, k] : test_kinds) {
        if (k == kind) {
            return name;
        }
    }
    on_internal_error(logstor_logger, "unknown test kind");
}

// A pure CPU operation costs a few thousand instructions, of the order of what the measurement loop
// itself costs per invocation, so those tests do a batch of them per invocation.
constexpr unsigned cpu_test_batch = 32;

struct test_config {
    unsigned partitions;
    unsigned columns;
    size_t value_size;
    // How many of the value columns a read asks for, the first ones of the table. 0 asks for all.
    unsigned select_columns;
    unsigned concurrency;
    unsigned duration_in_seconds;
    unsigned operations_per_shard;
    size_t segment_size;
    size_t file_size;
    size_t disk_size;
    bool compaction;
    bool stop_on_error;
};

std::ostream& operator<<(std::ostream& os, const test_config& cfg) {
    return os << "{partitions=" << cfg.partitions
           << ", columns=" << cfg.columns
           << ", value_size=" << cfg.value_size
           << ", select_columns=" << (cfg.select_columns ? std::to_string(cfg.select_columns) : std::string("all"))
           << ", concurrency=" << cfg.concurrency
           << ", segment_size=" << cfg.segment_size
           << ", disk_size=" << cfg.disk_size
           << ", compaction=" << (cfg.compaction ? "yes" : "no")
           << "}";
}

// A logstor table: a partition key and the value columns of the row it holds. The number of them is
// what the description of the schema a record carries scales with, and what the framing of the
// cells scales with, so it is a parameter of the run rather than one column.
schema_ptr make_kv_schema(unsigned columns) {
    auto sb = schema_builder(this_smp_shard_count(), "ks", "cf")
            .with_column("pk", bytes_type, column_kind::partition_key);
    for (unsigned i = 0; i < columns; ++i) {
        sb.with_column(to_bytes(fmt::format("v{}", i)), bytes_type);
    }
    return sb.set_logstor().build();
}

// The row one record of the size report holds: the partition key, the columns of the table, and
// the value of one of them. A logstor partition is at most one row, so this is the whole shape of
// a record.
struct row_shape {
    size_t key_size;
    size_t columns;
    size_t value_size;

    // The bytes of the row a write is handed. What a record takes beyond this is what the format
    // of a record costs.
    size_t payload_size() const noexcept { return key_size + columns * value_size; }
};

// The partition of one record of the report: the row with the empty clustering key, with a live
// marker and a live cell in every column of the schema.
mutation make_report_mutation(schema_ptr s, const row_shape& shape) {
    bytes key(bytes::initialized_later(), shape.key_size);
    std::ranges::fill(key, int8_t('k'));
    mutation m(s, dht::decorate_key(*s, partition_key::from_single_value(*s, key)));
    const auto ts = api::new_timestamp();
    auto& row = m.partition().clustered_row(*s, clustering_key::make_empty());
    row.apply(row_marker(ts));
    bytes value(bytes::initialized_later(), shape.value_size);
    std::ranges::fill(value, int8_t('v'));
    for (const auto& value_def : s->regular_columns()) {
        row.cells().apply(value_def, atomic_cell::make_live(*value_def.type, ts, value));
    }
    return m;
}

template <typename T>
size_t serialized_size_of(const T& v) {
    seastar::measuring_output_stream ms;
    ser::serialize(ms, v);
    return ms.size();
}

// What one record is made of, all of it measured through the serializers the write path uses.
struct record_sizes {
    size_t header{};    // the serialized log_record_header
    size_t value{};     // the serialized canonical_mutation the record carries
    size_t mapping{};   // of the value, the column mapping of the schema
    size_t partition{}; // of the value, the partition itself
    size_t record{};    // the record header, the log record header and the value
    size_t padding{};   // what aligning the next record after this one costs

    // What the value spends on neither the mapping nor the partition: the table id, the schema
    // version, the copy of the partition key that the log record header already carries, and the
    // framing of all of them.
    size_t value_rest() const noexcept { return value - mapping - partition; }
};

record_sizes measure_record(const schema& s, const mutation& m) {
    const log_record_header header {
        .key = primary_index_key{m.decorated_key()},
        .timestamp = api::new_timestamp(),
        .table = s.id(),
    };
    // The partition as the canonical_mutation of the record writes it, which is the only part of a
    // record that holds anything the write was given.
    bytes_ostream partition;
    mutation_partition_serializer(s, m.partition()).write(partition);

    record_sizes sizes {
        .header = serialized_size_of(header),
        .value = serialized_size_of(canonical_mutation(m)),
        .mapping = serialized_size_of(s.get_column_mapping()),
        .partition = partition.size(),
    };
    sizes.record = ondisk::record_header_size + sizes.header + sizes.value;
    sizes.padding = align_up(sizes.record, ondisk::record_alignment) - sizes.record;
    return sizes;
}

// Prints what a record takes on disk for every combination of the three lists, and where those
// bytes go. See the record size report in the comment at the top of this file.
void print_record_size_report(const std::vector<size_t>& key_sizes, const std::vector<size_t>& column_counts,
        const std::vector<size_t>& value_sizes) {
    fmt::print("What a record takes on disk, by row shape. All sizes are bytes.\n"
            "\n"
            "payload is the row a write is handed: the partition key and the cells. What a record\n"
            "takes beyond it is the overhead of the format, which is paid on every write, on every\n"
            "byte of the segment pool and on every read that goes to a segment. pad is what\n"
            "aligning the next record after this one costs, which logstor counts against the\n"
            "buffer rather than against the record, so it is not part of the overhead here.\n"
            "\n"
            "value is the canonical_mutation the record carries, split into the column mapping of\n"
            "the schema, which every record written under a version repeats, the partition itself,\n"
            "and what is left of it: the table id, the schema version, the second copy of the\n"
            "partition key and the framing of all of them.\n"
            "\n");
    fmt::print("{:>4} {:>5} {:>6} {:>8} | {:>7} {:>7} {:>7} {:>4} | {:>9} {:>6} | {:>8} {:>10} {:>8}\n",
            "key", "cols", "value", "payload",
            "header", "value", "record", "pad",
            "overhead", "ratio",
            "mapping", "partition", "ids+key");
    for (auto columns : column_counts) {
        auto s = make_kv_schema(static_cast<unsigned>(columns));
        for (auto value_size : value_sizes) {
            for (auto key_size : key_sizes) {
                const row_shape shape{.key_size = key_size, .columns = columns, .value_size = value_size};
                const auto sizes = measure_record(*s, make_report_mutation(s, shape));
                const auto payload = shape.payload_size();
                fmt::print("{:>4} {:>5} {:>6} {:>8} | {:>7} {:>7} {:>7} {:>4} | {:>9} {:>6.2f} | {:>8} {:>10} {:>8}\n",
                        key_size, columns, value_size, payload,
                        sizes.header, sizes.value, sizes.record, sizes.padding,
                        sizes.record - payload,
                        payload ? double(sizes.record) / payload : 0.0,
                        sizes.mapping, sizes.partition, sizes.value_rest());
            }
        }
    }
}

// The slice a read of the dataset is given: the first select_columns value columns of the table,
// or all of them when that is 0. Only a whole read consults it; the single steps copy or decode the
// whole partition regardless.
query::partition_slice make_read_slice(const schema& s, unsigned select_columns, bool bypass_cache) {
    partition_slice_builder builder(s);
    if (select_columns) {
        for (unsigned i = 0; i < select_columns; ++i) {
            builder.with_regular_column(to_bytes(fmt::format("v{}", i)));
        }
    }
    if (bypass_cache) {
        builder.with_option<query::partition_slice::option::bypass_cache>();
    }
    return builder.build();
}

// One logstor of one shard, with a dataset written to it, which is what a shard of a node has: the
// segments of a shard are its own, and so is its index.
class logstor_bench {
    test_config _cfg;
    schema_ptr _schema;
    // Outlives _logstor, which holds a reference to it.
    tests::logstor::shared_logstor_cache _cache;
    std::unique_ptr<logstor> _logstor;
    std::unique_ptr<tests::logstor::test_logstor_group> _group;
    std::vector<dht::decorated_key> _keys;
    // The keys of the dataset whose mutations are in the cache, which is what the cache-lookup and
    // read-cached tests draw from: a dataset larger than the cache of the shard leaves only part of
    // itself in it, and a key the LRU dropped is not one a test of what a hit costs can use. Points
    // into _keys, which does not change once the dataset is written.
    std::vector<const dht::decorated_key*> _cached_keys;
    // How many keys of the dataset the cache held when the test was prepared, which is not the size
    // of _cached_keys: that falls back to the whole dataset when the cache held none of it.
    size_t _resident_keys = 0;
    bytes _value;
    query::partition_slice _slice;
    query::partition_slice _slice_bypassing_cache;
    // The permit the query-cached test makes its readers with. A query is handed one by the layer
    // above logstor, so one is made up front and every reader of the test shares it. Released, and
    // its semaphore stopped, in stop().
    reader_concurrency_semaphore _semaphore;
    std::optional<reader_permit> _permit;
    // One record of the dataset in the forms the steps of the read and the write path work on, for
    // the tests that measure a single step.
    std::unique_ptr<raw_write_buffer> _serialization_buffer;
    temporary_buffer<char> _serialized_record;
    canonical_mutation _canonical_mutation;
    std::optional<mutation> _mutation;
    // A record whose sizes have already been computed, for the append test, which is about what the
    // copy into the buffer costs and not about what computing the sizes of a record costs.
    std::optional<log_record_writer> _record_writer;
    // Takes the result of a step whose result is otherwise unused, so that the step is not optimized
    // away and cannot be hoisted out of the loop of the test that repeats it. Volatile because that
    // is what makes the store to it something the compiler has to keep; it costs the same load and
    // store in every test that uses it, so it cancels out of a difference between two of them.
    volatile uint64_t _sink = 0;
    // The first data file of the shard, opened for the raw read test.
    seastar::file _data_file;
    uint64_t _data_file_size = 0;
    std::filesystem::path _dir;

public:
    logstor_bench(test_config cfg, std::filesystem::path dir)
        : _cfg(cfg)
        , _schema(make_kv_schema(cfg.columns))
        , _value(bytes::initialized_later(), cfg.value_size)
        , _slice(make_read_slice(*_schema, cfg.select_columns, false))
        , _slice_bypassing_cache(make_read_slice(*_schema, cfg.select_columns, true))
        , _semaphore(reader_concurrency_semaphore::no_limits{}, "perf_logstor", reader_concurrency_semaphore::register_metrics::no)
        , _permit(_semaphore.make_tracking_only_permit(nullptr, "perf_logstor", db::no_timeout, {}))
        , _serialization_buffer(std::make_unique<raw_write_buffer>(cfg.segment_size, segment_kind::mixed))
        , _dir(dir) {
        std::ranges::fill(_value, int8_t('v'));
        auto logstor_cfg = tests::logstor::make_test_logstor_config(dir, {
            .segment_size = cfg.segment_size,
            .file_size = cfg.file_size,
            .disk_size = cfg.disk_size,
            // Preallocated rather than sparse, which is what a node writes into: a read of a hole
            // is not a read of a disk, and a write into one pays for the extent it allocates.
            .sparse_files = false,
            .compaction_enabled = cfg.compaction,
        });
        // The same share of memory a node gives the writes it has taken but not yet flushed, which
        // is what bounds how far ahead of the disk the write path may run.
        logstor_cfg.max_queued_write_bytes = memory::stats().total_memory() / 100;
        _logstor = std::make_unique<logstor>(std::move(logstor_cfg), _cache.shared_tracker);
    }

    future<> start() {
        co_await _logstor->do_recovery_for_test();
        co_await _logstor->start();
        // The cache is enabled, and the tests that are not about it bypass it per read instead, so
        // that one dataset serves them all.
        _group = std::make_unique<tests::logstor::test_logstor_group>(_schema, *_logstor, true /* cache_enabled */);
        co_await populate();
        prepare_single_step_inputs();
        co_await open_data_file();
    }

    future<> stop() {
        if (_data_file) {
            co_await _data_file.close();
            _data_file = seastar::file();
        }
        if (_group) {
            // The index evicts what it has in the cache before it goes, since a cached mutation
            // lives in the shared cache region and the index entry only points at it. This is what
            // a table does when it stops.
            co_await index().drain_cache();
            // The group deregisters itself from the compaction manager in its destructor, which
            // waits for an ongoing compaction of the group and therefore needs a seastar thread. It
            // has to be gone before the logstor it belongs to is stopped.
            co_await seastar::async([this] {
                _group.reset();
            });
        }
        co_await _logstor->stop();
        _permit.reset();
        co_await _semaphore.stop();
    }

    future<> do_write() {
        auto m = make_mutation(random_key());
        co_await _logstor->write(m, write_target{.cg = _group.get()}, db::no_timeout);
    }

    future<> do_read(const dht::decorated_key& key, const query::partition_slice& slice) {
        auto mut = co_await _logstor->read(*_schema, index(), key, slice);
        if (!mut) [[unlikely]] {
            on_internal_error(logstor_logger, "key of the dataset is missing from the index");
        }
    }

    future<> do_read_cached() {
        return do_read(random_cached_key(), _slice);
    }

    future<> do_read_bypassing_cache() {
        return do_read(random_key(), _slice_bypassing_cache);
    }

    // A whole read as a query pays for it: the reader a table hands the querier for one key is
    // made, drained to the end of its stream and closed. The fragments are counted and dropped,
    // which is the least a consumer of the reader does with them.
    future<> do_query_cached() {
        auto reader = _logstor->make_reader(_schema, index(), *_permit,
                dht::partition_range::make_singular(random_cached_key()), _slice);
        auto close_reader = deferred_close(reader);
        while (!reader.is_end_of_stream()) {
            co_await reader.fill_buffer();
            _sink += reader.buffer().size();
            reader.detach_buffer();
        }
    }

    // The read of the record from its segment and its deserialization, without materializing the
    // mutation the read returns. The lookup of the location is in it: a read starts from a key, and
    // holding a location from before the measurement would not survive compaction moving its record.
    future<> do_segment_read() {
        const auto& key = random_key();
        auto entry = index().get(primary_index_key{key});
        if (!entry) [[unlikely]] {
            on_internal_error(logstor_logger, "key of the dataset is missing from the index");
        }
        co_await _logstor->get_segment_manager().read(entry->location);
    }

    // One DMA read of the size of a record, issued straight to the data file: what a read of a
    // record costs before logstor puts anything of its own on top of it. What segment-read costs
    // beyond this is the index lookup of the location, the file handle lookup, the segment object
    // built per read, and the deserialization of the record.
    future<> do_raw_read() {
        const auto size = _serialized_record.size();
        auto offset = seastar::align_down<uint64_t>(tests::random::get_int<uint64_t>(uint64_t(_data_file_size - size)), 512);
        auto buf = co_await _data_file.dma_read_exactly<char>(offset, size);
        if (buf.size() < size) [[unlikely]] {
            on_internal_error(logstor_logger, "short read of the data file");
        }
    }

    void do_index_lookup(unsigned count) {
        for (unsigned i = 0; i < count; ++i) {
            if (!index().get(primary_index_key{random_key()})) [[unlikely]] {
                on_internal_error(logstor_logger, "key of the dataset is missing from the index");
            }
        }
    }

    // What a write pays once its record is in a segment: the index is pointed at the new record.
    // The record is left where it is - the entry is replaced by one with the same location and a
    // newer timestamp - so that the space accounting of the segment manager sees a record freed and
    // the same record added, and the dataset the other tests read stays as it was. It includes the
    // lookup of the location to reinsert, which is what index-lookup measures on its own.
    void do_index_insert(unsigned count) {
        for (unsigned i = 0; i < count; ++i) {
            primary_index_key key{random_key()};
            auto entry = index().get(key);
            if (!entry) [[unlikely]] {
                on_internal_error(logstor_logger, "key of the dataset is missing from the index");
            }
            index().insert(key, index_entry{
                .location = entry->location,
                .timestamp = api::new_timestamp(),
            });
        }
    }

    // What a write pays before its record reaches a buffer of the writer: the mutation is frozen
    // into a canonical_mutation and the record is serialized into the buffer.
    void do_serialize(unsigned count) {
        for (unsigned i = 0; i < count; ++i) {
            _serialization_buffer->reset();
            _serialization_buffer->append(log_record_writer(log_record{
                .header = make_record_header(_mutation->decorated_key()),
                .mut = canonical_mutation(*_mutation),
            }));
        }
    }

    // Not a step of a write: the mutation a write is given, which the test has to build itself and
    // a node is handed by the layer above it. Measured so that it can be taken off what the write
    // test costs, which is the only test that builds one per operation.
    void do_build_mutation(unsigned count) {
        for (unsigned i = 0; i < count; ++i) {
            auto m = make_mutation(random_key());
            _sink += m.partition().row_count();
        }
    }

    // The first half of what do_serialize() measures: the mutation is frozen into the
    // canonical_mutation that the record of a write carries.
    void do_freeze(unsigned count) {
        for (unsigned i = 0; i < count; ++i) {
            auto frozen = canonical_mutation(*_mutation);
            (void)frozen;
        }
    }

    // The header of the record a write builds around its frozen mutation. It carries a copy of the
    // decorated key of the partition, which is what makes building it cost anything at all.
    void do_record_header(unsigned count) {
        for (unsigned i = 0; i < count; ++i) {
            auto header = make_record_header(random_key());
            _sink += header.timestamp;
        }
    }

    // What log_record_writer::compute_sizes() does: the record is serialized once into a stream that
    // only counts the bytes, so that the writer knows how much room to ask the buffer for. Measured
    // over the header building of do_record_header(), since the size of a header can only be
    // measured on a header, and against a key that changes per operation, so that the measuring
    // cannot be hoisted out of the loop.
    void do_record_sizes(unsigned count) {
        for (unsigned i = 0; i < count; ++i) {
            auto header = make_record_header(random_key());
            seastar::measuring_output_stream header_size;
            ser::serialize(header_size, header);
            seastar::measuring_output_stream data_size;
            ser::serialize(data_size, _canonical_mutation);
            _sink += header.timestamp + header_size.size() + data_size.size();
        }
    }

    // And the last: the record, whose sizes are already known, is copied into the buffer of the
    // writer. What do_serialize() costs beyond these two is the computation of those sizes, which
    // serializes the record once more only to measure it.
    void do_append(unsigned count) {
        for (unsigned i = 0; i < count; ++i) {
            _serialization_buffer->reset();
            _serialization_buffer->append(*_record_writer);
        }
    }

    // What a read that the cache serves pays: the entry of the key is looked up and the partition
    // of the cached mutation is copied out of the cache region. Draws from the keys the cache holds,
    // which warm_cache_and_find_cached_keys() collected, rather than from the whole dataset. A
    // lookup that finds nothing is counted by the cache tracker, see cache_misses().
    void do_cache_lookup(unsigned count) {
        auto* cache = index().cache_tracker();
        for (unsigned i = 0; i < count; ++i) {
            auto it = index().find(random_cached_key());
            if (it == index().end()) [[unlikely]] {
                on_internal_error(logstor_logger, "key of the dataset is missing from the index");
            }
            _sink += cache->lookup(*it, *_schema).has_value();
        }
    }

    // What a read that went to a segment pays to leave its mutation in the cache. The entry is
    // evicted first, since a populate of an entry that is already cached does nothing, and what is
    // measured is therefore the eviction of a cached partition together with the admission of one.
    // What it admits is the partition prepared for the single step tests, so that the cost of
    // building a mutation is not part of the measurement; the entry is left holding a partition of
    // the same shape as its own, which is all a test that runs after this one reads out of it.
    void do_cache_populate(unsigned count) {
        auto* cache = index().cache_tracker();
        for (unsigned i = 0; i < count; ++i) {
            const auto& key = random_key();
            auto it = index().find(key);
            if (it == index().end()) [[unlikely]] {
                on_internal_error(logstor_logger, "key of the dataset is missing from the index");
            }
            cache->evict(*it);
            cache->populate(*it, *_mutation);
        }
    }

    // What a read pays once the record is in memory: the record is deserialized, which copies the
    // bytes of its canonical_mutation out of the buffer read from the segment.
    void do_deserialize(unsigned count) {
        for (unsigned i = 0; i < count; ++i) {
            deserialize_log_record(simple_memory_input_stream(_serialized_record.begin(), _serialized_record.size()));
        }
    }

    // And what it pays after that: the canonical_mutation is turned into the mutation the read
    // returns.
    void do_materialize(unsigned count) {
        for (unsigned i = 0; i < count; ++i) {
            _canonical_mutation.to_mutation(_schema);
        }
    }

    // Reads every key once, so that the reads that follow find them in the cache, and then keeps the
    // keys the cache actually holds, for the cache-lookup and read-cached tests to draw from. peek()
    // is what asks without counting a lookup or touching the LRU, so the probing does not change
    // what the test that follows finds. Whether every key survived the warming is a property of the
    // dataset against the memory of the shard, which nothing here bounds.
    future<> warm_cache_and_find_cached_keys() {
        co_await max_concurrent_for_each(_keys, _cfg.concurrency, [this] (const dht::decorated_key& key) {
            return _logstor->read(*_schema, index(), key, _slice).discard_result();
        });
        auto* cache = index().cache_tracker();
        _cached_keys.clear();
        _cached_keys.reserve(_keys.size());
        for (const auto& key : _keys) {
            auto it = index().find(key);
            if (it == index().end()) [[unlikely]] {
                on_internal_error(logstor_logger, "key of the dataset is missing from the index");
            }
            if (cache->peek(*it, _schema)) {
                _cached_keys.push_back(&key);
            }
            co_await coroutine::maybe_yield();
        }
        _resident_keys = _cached_keys.size();
        if (_cached_keys.empty()) {
            // Nothing of the dataset is cached, so there is no hit to draw. Draw from all of it
            // instead and let the miss count say what the number is, rather than leaving the test
            // with nothing to pick and the run with the tests after it unmeasured.
            for (const auto& key : _keys) {
                _cached_keys.push_back(&key);
            }
        }
    }

    size_t cached_key_count() const noexcept { return _resident_keys; }

    // Lookups of the cache of the shard that found no cached mutation, whether a test looked the
    // cache up itself or a read did. Once the cache was warmed, a miss of a key it held then is the
    // LSA reclaiming under memory pressure, or a read-cached miss evicting another key to admit its
    // own. Counted rather than treated as an error: a run that says its number is not a cache hit is
    // more use than one that ends here.
    uint64_t cache_misses() const noexcept { return _cache.shared_tracker.get_stats().partition_misses; }

    // The partition key of the dataset is an int64 - see populate() - which is what the payload of
    // one of its records counts as its key.
    static constexpr size_t dataset_key_size = sizeof(int64_t);

    // What one record of the dataset takes in a segment, against the bytes of the row it carries.
    // The difference between the two is what the format of a record costs, which is paid on every
    // write, on every disk byte and on every read that goes to a segment. The record size report
    // prints the same two numbers, and the parts they are made of, for shapes the run did not use.
    size_t record_size() const noexcept { return _serialized_record.size(); }
    size_t payload_size() const noexcept {
        return row_shape{
            .key_size = dataset_key_size,
            .columns = _cfg.columns,
            .value_size = _cfg.value_size,
        }.payload_size();
    }

private:
    primary_index& index() noexcept {
        return _group->logstor_index();
    }

    const dht::decorated_key& random_key() const noexcept {
        return _keys[tests::random::get_int<size_t>(_keys.size() - 1)];
    }

    const dht::decorated_key& random_cached_key() const noexcept {
        return *_cached_keys[tests::random::get_int<size_t>(_cached_keys.size() - 1)];
    }

    log_record_header make_record_header(const dht::decorated_key& key) const {
        return log_record_header{
            .key = primary_index_key{key},
            .timestamp = api::new_timestamp(),
            .table = _schema->id(),
        };
    }

    mutation make_mutation(const dht::decorated_key& key) const {
        const auto ts = api::new_timestamp();
        mutation m(_schema, key);
        auto& row = m.partition().clustered_row(*_schema, clustering_key::make_empty());
        row.apply(row_marker(ts));
        for (const auto& value_def : _schema->regular_columns()) {
            row.cells().apply(value_def, atomic_cell::make_live(*value_def.type, ts, _value));
        }
        return m;
    }

    // The data files of a shard are named ls_{shard}-{file}-Data.db, and the first one is as good
    // as any for a read that is only about what the read costs.
    future<> open_data_file() {
        auto path = _dir / fmt::format("ls_{}-0-Data.db", this_shard_id());
        _data_file = co_await seastar::open_file_dma(path.string(), seastar::open_flags::ro);
        _data_file_size = co_await _data_file.size();
    }

    future<> populate() {
        _keys.reserve(_cfg.partitions);
        for (unsigned i = 0; i < _cfg.partitions; ++i) {
            auto pk = partition_key::from_single_value(*_schema, serialized(int64_t(i)));
            _keys.push_back(dht::decorate_key(*_schema, std::move(pk)));
        }
        // A write completes only once its record has been flushed to a segment, so writing the
        // dataset one record at a time would wait for the disk for the whole populate phase.
        co_await max_concurrent_for_each(_keys, _cfg.concurrency, [this] (const dht::decorated_key& key) -> future<> {
            auto m = make_mutation(key);
            co_await _logstor->write(m, write_target{.cg = _group.get()}, db::no_timeout);
        });
        // Seal the records into segments of the group, which is where the steady state of a node
        // has them and where a read finds them.
        co_await _logstor->flush_to_separator();
        co_await _group->flush_separator();
    }

    void prepare_single_step_inputs() {
        _mutation = make_mutation(_keys[0]);
        _canonical_mutation = canonical_mutation(*_mutation);
        _record_writer.emplace(log_record{
            .header = make_record_header(_mutation->decorated_key()),
            .mut = _canonical_mutation,
        });
        _serialization_buffer->reset();
        auto appended = _serialization_buffer->append(*_record_writer);
        _serialized_record = temporary_buffer<char>(_serialization_buffer->data() + appended.record_header_offset,
                appended.total_size);
    }
};

std::vector<perf_result_with_io> run_test(sharded<logstor_bench>& bench, test_kind kind, const test_config& cfg) {
    // An operation of the tests that only use the CPU does not wait for anything, so running more
    // than one of them at a time only adds the cost of switching between them.
    const auto cpu_test = [&bench, &cfg] (void (logstor_bench::*op)(unsigned)) {
        return time_parallel_ex<perf_result_with_io>([&bench, op] {
            (bench.local().*op)(cpu_test_batch);
            return make_ready_future<>();
        }, 1, cfg.duration_in_seconds, cfg.operations_per_shard, cfg.stop_on_error, io_counters_updater(), cpu_test_batch);
    };
    const auto io_test = [&bench, &cfg] (future<> (logstor_bench::*op)()) {
        return time_parallel_ex<perf_result_with_io>([&bench, op] {
            return (bench.local().*op)();
        }, cfg.concurrency, cfg.duration_in_seconds, cfg.operations_per_shard, cfg.stop_on_error, io_counters_updater());
    };
    // What the cache tests measure is what a hit costs, so they draw only from the keys the cache
    // holds after the warming. How many of the dataset those are is the first thing to read: a
    // shard whose cache did not take all of it measures the part that fit.
    const auto cache_hit_test = [&bench, &cfg] (auto run) {
        bench.invoke_on_all(&logstor_bench::warm_cache_and_find_cached_keys).get();
        const auto cached = bench.map_reduce0([] (const logstor_bench& b) { return b.cached_key_count(); },
                size_t(0), std::plus<size_t>()).get();
        const auto total = size_t(cfg.partitions) * this_smp_shard_count();
        fmt::print("{} of the {} keys of the dataset are in the cache\n", cached, total);
        const auto count_misses = [&bench] {
            return bench.map_reduce0([] (const logstor_bench& b) { return b.cache_misses(); },
                    uint64_t(0), std::plus<uint64_t>()).get();
        };
        const auto misses_before = count_misses();
        auto results = run();
        const auto misses = count_misses() - misses_before;
        if (misses) {
            fmt::print("WARNING: {} lookups found no cached mutation, so the number above is not what a hit"
                    " costs. Either nothing of the dataset fit the cache, or the LSA reclaimed while the"
                    " test ran; give the run more memory or fewer partitions\n", misses);
        }
        return results;
    };

    switch (kind) {
    case test_kind::index_lookup:
        return cpu_test(&logstor_bench::do_index_lookup);
    case test_kind::index_insert:
        return cpu_test(&logstor_bench::do_index_insert);
    case test_kind::deserialize:
        return cpu_test(&logstor_bench::do_deserialize);
    case test_kind::materialize:
        return cpu_test(&logstor_bench::do_materialize);
    case test_kind::build_mutation:
        return cpu_test(&logstor_bench::do_build_mutation);
    case test_kind::freeze:
        return cpu_test(&logstor_bench::do_freeze);
    case test_kind::record_header:
        return cpu_test(&logstor_bench::do_record_header);
    case test_kind::record_sizes:
        return cpu_test(&logstor_bench::do_record_sizes);
    case test_kind::append:
        return cpu_test(&logstor_bench::do_append);
    case test_kind::serialize:
        return cpu_test(&logstor_bench::do_serialize);
    case test_kind::cache_lookup:
        return cache_hit_test([&] { return cpu_test(&logstor_bench::do_cache_lookup); });
    case test_kind::cache_populate:
        return cpu_test(&logstor_bench::do_cache_populate);
    case test_kind::raw_read:
        return io_test(&logstor_bench::do_raw_read);
    case test_kind::read_cached:
        return cache_hit_test([&] { return io_test(&logstor_bench::do_read_cached); });
    case test_kind::query_cached:
        return cache_hit_test([&] { return io_test(&logstor_bench::do_query_cached); });
    case test_kind::read_disk:
        return io_test(&logstor_bench::do_read_bypassing_cache);
    case test_kind::segment_read:
        return io_test(&logstor_bench::do_segment_read);
    case test_kind::write:
        return io_test(&logstor_bench::do_write);
    }
    on_internal_error(logstor_logger, "unknown test kind");
}

void write_json_result(const std::string& file, const test_config& cfg, test_kind kind, const aggregated_perf_results& agg,
        const perf_result_with_io& median, size_t record_bytes, size_t payload_bytes) {
    Json::Value params;
    params["partitions"] = cfg.partitions;
    params["columns"] = cfg.columns;
    params["value_size"] = cfg.value_size;
    params["select_columns"] = cfg.select_columns;
    params["concurrency"] = cfg.concurrency;
    params["cpus"] = this_smp_shard_count();
    params["duration"] = cfg.duration_in_seconds;
    params["segment_size"] = Json::UInt64(cfg.segment_size);
    params["disk_size"] = Json::UInt64(cfg.disk_size);
    params["compaction"] = cfg.compaction;

    Json::Value extra_stats;
    extra_stats["reads_per_op"] = median.reads;
    extra_stats["read_bytes_per_op"] = median.read_bytes;
    extra_stats["writes_per_op"] = median.writes;
    extra_stats["write_bytes_per_op"] = median.write_bytes;
    extra_stats["record_bytes"] = Json::UInt64(record_bytes);
    extra_stats["payload_bytes"] = Json::UInt64(payload_bytes);

    perf::write_json_result(file, agg, params, fmt::format("logstor_{}", name_of(kind)), extra_stats);
}

// Everything a run takes from the command line. The body of the run is a coroutine, and the lambda
// that holds the reference to the application does not outlive it, so the configuration is read out
// once, up front, rather than looked up as the run goes.
struct run_config {
    test_config test;
    std::vector<test_kind> tests;
    std::string dir;
    std::string json_result;
    unsigned seed;
};

std::vector<test_kind> parse_tests(const std::string& names) {
    if (names == "all") {
        return test_kinds | std::views::values | std::ranges::to<std::vector<test_kind>>();
    }
    std::vector<test_kind> kinds;
    for (const auto& name : std::views::split(std::string_view(names), std::string_view(","))) {
        auto sv = std::string_view(name.begin(), name.end());
        auto found = std::ranges::find(test_kinds, sv, &std::pair<std::string_view, test_kind>::first);
        if (found == test_kinds.end()) {
            throw std::invalid_argument(fmt::format("unknown test '{}', expected one of {} or 'all'",
                    sv, fmt::join(test_kinds | std::views::keys, ", ")));
        }
        kinds.push_back(found->second);
    }
    return kinds;
}

// A comma separated list of sizes, which is how the row shapes of the record size report are given.
std::vector<size_t> parse_size_list(std::string_view option, std::string_view values) {
    std::vector<size_t> sizes;
    for (const auto& part : std::views::split(values, std::string_view(","))) {
        const auto text = std::string_view(part.begin(), part.end());
        size_t size = 0;
        const auto [end, ec] = std::from_chars(text.data(), text.data() + text.size(), size);
        if (ec != std::errc() || end != text.data() + text.size()) {
            throw std::invalid_argument(fmt::format("--{} takes comma separated numbers, got '{}'", option, text));
        }
        sizes.push_back(size);
    }
    if (sizes.empty()) {
        throw std::invalid_argument(fmt::format("--{} takes at least one number", option));
    }
    return sizes;
}

run_config make_run_config(const boost::program_options::variables_map& config) {
    auto seed = config["random-seed"];
    run_config run{
        .test = test_config{
            .partitions = config["partitions"].as<unsigned>(),
            .columns = config["columns"].as<unsigned>(),
            .value_size = config["value-size"].as<unsigned>(),
            .select_columns = config["select-columns"].as<unsigned>(),
            .concurrency = config["concurrency"].as<unsigned>(),
            .duration_in_seconds = config["duration"].as<unsigned>(),
            .operations_per_shard = 0,
            .segment_size = config["segment-size-in-kb"].as<unsigned>() * 1024ull,
            .file_size = config["file-size-in-mb"].as<unsigned>() * 1024ull * 1024ull,
            .disk_size = config["disk-size-in-mb"].as<unsigned>() * 1024ull * 1024ull,
            .compaction = config["compaction"].as<bool>(),
            .stop_on_error = config["stop-on-error"].as<bool>(),
        },
        .tests = parse_tests(config["test"].as<std::string>()),
        .dir = config.contains("dir") ? config["dir"].as<std::string>() : std::string(),
        .json_result = config.contains("json-result") ? config["json-result"].as<std::string>() : std::string(),
        .seed = seed.empty() ? std::random_device()() : seed.as<unsigned>(),
    };
    if (config.contains("operations-per-shard")) {
        run.test.operations_per_shard = config["operations-per-shard"].as<unsigned>();
        if (run.test.operations_per_shard == 0) {
            throw std::invalid_argument("--operations-per-shard must be at least one operation; leave it out to run for --duration instead");
        }
    } else if (run.test.duration_in_seconds == 0) {
        throw std::invalid_argument("--duration must be at least one iteration, or --operations-per-shard has to say what the run is to do instead");
    }
    if (run.test.partitions == 0) {
        throw std::invalid_argument("--partitions must be at least one: every test reads or overwrites the dataset");
    }
    if (run.test.concurrency == 0) {
        throw std::invalid_argument("--concurrency must be at least one: it is how many operations the tests that wait for the disk keep in flight");
    }
    if (run.test.select_columns > run.test.columns) {
        throw std::invalid_argument(fmt::format("--select-columns {} asks for more value columns than the {} the table has",
                run.test.select_columns, run.test.columns));
    }
    // The test builds a buffer of one segment before logstor is started, so a zero segment size
    // would fail there rather than at the geometry checks. The rest of the geometry is left to the
    // segment manager, which has to check it anyway for the configuration a node is started with.
    if (run.test.segment_size == 0) {
        throw std::invalid_argument("--segment-size-in-kb must be at least one kilobyte");
    }
    return run;
}

} // namespace

int main(int argc, char** argv) {
    namespace bpo = boost::program_options;
    app_template app;
    app.add_options()
        ("random-seed", bpo::value<unsigned>(), "random number generator seed")
        ("test", bpo::value<std::string>()->default_value("all"), "comma separated tests to run, or 'all'")
        ("partitions", bpo::value<unsigned>()->default_value(100000), "number of partitions written per shard")
        ("columns", bpo::value<unsigned>()->default_value(1), "number of value columns of the table, each holding a value of --value-size bytes")
        ("value-size", bpo::value<unsigned>()->default_value(200), "size of the value of a column in bytes")
        ("select-columns", bpo::value<unsigned>()->default_value(0), "how many of the value columns a read asks for, the first ones of the table; 0 asks for all")
        ("concurrency", bpo::value<unsigned>()->default_value(50), "operations in flight per shard, for the tests that wait for the disk")
        ("duration", bpo::value<unsigned>()->default_value(5), "number of one second iterations per test")
        ("operations-per-shard", bpo::value<unsigned>(), "run this many operations per shard (overrides duration)")
        ("segment-size-in-kb", bpo::value<unsigned>()->default_value(128), "size of a logstor segment")
        ("file-size-in-mb", bpo::value<unsigned>()->default_value(32), "size of a logstor data file")
        ("disk-size-in-mb", bpo::value<unsigned>()->default_value(512), "size of the logstor segment pool of a shard")
        ("compaction", bpo::value<bool>()->default_value(true), "let compaction run, which is what gives free segments back")
        ("dir", bpo::value<std::string>(), "directory for the logstor files (default: a temporary directory)")
        ("stop-on-error", bpo::value<bool>()->default_value(true), "stop after encountering the first error")
        ("json-result", bpo::value<std::string>(), "name of the json result file, suffixed with the name of the test")
        ("record-size-report", bpo::bool_switch(), "print what a record takes on disk for a matrix of row shapes and exit, without building a logstor")
        ("report-key-sizes", bpo::value<std::string>()->default_value("8,16,64"), "comma separated partition key sizes of that matrix")
        ("report-columns", bpo::value<std::string>()->default_value("1,5,30"), "comma separated value column counts of that matrix")
        ("report-value-sizes", bpo::value<std::string>()->default_value("20,100,300"), "comma separated column value sizes of that matrix")
        ;

    set_abort_on_internal_error(true);

    return app.run(argc, argv, [&app] () -> future<> {
        const auto& options = app.configuration();
        // The report needs neither a dataset nor a disk, so it runs before anything is built and
        // leaves the rest of the configuration, which describes a measured run, unread.
        if (options["record-size-report"].as<bool>()) {
            print_record_size_report(
                    parse_size_list("report-key-sizes", options["report-key-sizes"].as<std::string>()),
                    parse_size_list("report-columns", options["report-columns"].as<std::string>()),
                    parse_size_list("report-value-sizes", options["report-value-sizes"].as<std::string>()));
            co_return;
        }
        auto run = make_run_config(app.configuration());
        const auto& cfg = run.test;
        fmt::print("random-seed={}\n", run.seed);
        co_await smp::invoke_on_all([seed = run.seed] {
            seastar::testing::local_random_engine.seed(seed + this_shard_id());
        });

        // Overwriting the dataset needs room for the dead records the overwrites leave behind, which
        // compaction only reclaims once the disk is short of free segments.
        const auto dataset_size = size_t(cfg.partitions) * (cfg.columns * cfg.value_size + 100);
        if (dataset_size * 2 > cfg.disk_size) {
            fmt::print("WARNING: the dataset of about {} MB does not leave room in a segment pool of {} MB;"
                    " writes will stall on compaction\n", dataset_size / 1024 / 1024, cfg.disk_size / 1024 / 1024);
        }

        tmpdir tmp;
        auto dir = run.dir.empty() ? tmp.path() : std::filesystem::path(run.dir);
        co_await recursive_touch_directory(dir.string());
        std::cout << "Running logstor tests with config: " << cfg << ", dir=" << dir << std::endl;

        sharded<logstor_bench> bench;
        co_await bench.start(cfg, dir);
        std::exception_ptr ex;
        try {
            co_await bench.invoke_on_all(&logstor_bench::start);
            co_await seastar::async([&] {
                const auto record_bytes = bench.local().record_size();
                const auto payload_bytes = bench.local().payload_size();
                fmt::print("record: {} bytes in a segment for {} bytes of row ({:.2f}x)\n",
                        record_bytes, payload_bytes, payload_bytes ? double(record_bytes) / payload_bytes : 0.0);
                for (auto kind : run.tests) {
                    fmt::print("\n{}:\n", name_of(kind));
                    auto results = run_test(bench, kind, cfg);
                    std::vector<perf_result> throughput_results(results.begin(), results.end());
                    aggregated_perf_results agg(throughput_results);
                    std::cout << agg << std::endl;
                    // The same median as the aggregation reports, with the IO counters of that
                    // iteration, which are not part of it.
                    std::ranges::sort(results, std::less<>{}, &perf_result::throughput);
                    const auto& median = results[results.size() / 2];
                    fmt::print("median: {}\n", median);
                    if (!run.json_result.empty()) {
                        write_json_result(fmt::format("{}.{}", run.json_result, name_of(kind)), cfg, kind, agg, median,
                                record_bytes, payload_bytes);
                    }
                }
            });
        } catch (...) {
            ex = std::current_exception();
        }

        // Stops every instance before destroying it.
        co_await bench.stop();
        if (ex) {
            std::rethrow_exception(ex);
        }
    });
}
