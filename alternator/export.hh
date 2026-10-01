/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "seastarx.hh"
#include <cstddef>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <variant>
#include <vector>
#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <seastar/util/noncopyable_function.hh>
#include "alternator/executor.hh"
#include "schema/schema_fwd.hh"
#include "service_permit.hh"
#include "utils/rjson.hh"

namespace s3 { class client; }
namespace service { class storage_proxy; }
namespace db { struct alternator_export_row; }

namespace alternator {

// A row of the exports table, as stored (see db/system_distributed_keyspace.hh).
using export_row = db::alternator_export_row;

// Compression type selection for export/import pipelines.
// Pass one of these structs to create_sink_pipeline / create_source_pipeline to select compression mode.
struct no_compression {};
struct gzip_compression {};
using compression_type = std::variant<no_compression, gzip_compression>;

// An interface encapsulating write (sink) pipeline for exporting data. Is used to implement DynamoDB export api (ExportTableToPointInTime call).
// The pipeline is a multistage processing unit, which takes `rjson::value` item (of any content), serializes it as-is and writes it depending on the configuration.
// Currently supporting test-only in-memory pipeline and writing to S3, in both cases serializing to raw text JSON lines,
// written either uncompressed (`no_compression`) or gzip compressed (`gzip_compression`) - see `compression_type` above.
// In the future we will add support for different formats (e.g. Ion, CSV).
// Call respective factory method below (`create_sink_pipeline`) to construct.
// Call `process()` method for each item (they might come in random order) - they will be serialized and written to the appropriate sink.
// After all items are processed, call `flush_and_close()` to flush and finalize the pipeline - the call is mandatory, otherwise part of the data might not be written.
// Calls to `process()` and `flush_and_close()` must be serialized, i.e. each call is allowed only after previous call's future is completed.
struct export_pipeline_interface {
    // Invokes whole pipeline for a single item. The future will complete once item is processed.
    // This doesn't mean the item hit external storage, but you're free to process another item.
    // Call to `process()` is allowed only after previous call to `process()` or `flush_and_close()` future is completed.
    // Caller is responsible for ensuring `item` is kept alive until the future is completed.
    virtual future<> process(const rjson::value &item) = 0;

    struct result {
        // as returned from AWS, without quotes
        sstring etag;
        // md5 of the content, base64 encoded, with filled padding if necessary
        sstring md5;
        // size of the content in bytes
        size_t size_in_bytes = 0;
    };
    // Flushes and closes the pipeline. The future will complete once all items are flushed and pipeline is finalized.
    // Do not call process() after calling flush_and_close(). Do not call `flush_and_close()` more than once.
    // The outcome of the export is carried solely by the exceptions from the factory function and from this call.
    // A failure of either can still leave an object behind under the target name, and such an object is
    // indistinguishable from the result of a successful export of no items - a source pipeline reads it back as
    // zero items, without reporting an error. The caller must therefore not record, publish or otherwise treat
    // the target object as an export unless both calls succeeded.
    virtual future<result> flush_and_close() = 0;

    virtual ~export_pipeline_interface() = default;
};

// An interface encapsulating read (source) pipeline. This mirrors write (sink) pipeline - what sink pipeline can produce, source pipeline will consume.
// This will be used in future for DynamoDB import api (ImportTable call).
// Added currently for testing purposes - so we have a consistent way to read exported data without relying on connection to S3 / DynamoDB.
// Call respective factory method below (`create_source_pipeline`) to construct.
// Call `read_all()` (only once!) method to start reading the data - it will read all data, pass it through the decompressor and parser
// and call the callback provided to the factory function for each parsed item. The pipeline will wait
// for each callback's future to complete before processing the next item.
// After all data is read (the future from `read_all()` call completes), call `close()` to flush and finalize the pipeline.
// `close()` might call the item callback any number of times and might fail.
// Calling `close()` is required and needs to be done manually. `close()` can be called more than once.
struct import_pipeline_interface {
    // Reads all available data from the source, feeds it through the decompression and parsing pipeline,
    // and invokes the on_item callback (passed to the pipeline constructor function) for each parsed item.
    // The future completes after source is exhausted.
    // Note: you still need to call `close()` to finalize the pipeline and release all resources,
    // even if `read_all()` fails, is aborted via abort_source or is not called.
    // If an abort_source was supplied in the target config and the abort fires, this future
    // resolves with an exception rather than returning normally.
    // Completing without an exception does not mean the whole export was read - only a successful
    // `close()` does, see below.
    virtual future<> read_all() = 0;

    // Closes the pipeline, releasing all resources. The call doesn't read additional data.
    // Do not call read_all() after calling close().
    // With gzip compression this is also where the completeness of the object is verified: zlib checks the
    // CRC32 and ISIZE trailer only at the end of a member, so an object that was cut short decodes (and reports
    // items) as far as it goes and fails only here, with a `truncated gzip stream` validation error. An object
    // with no bytes at all is accepted as an empty stream and read back as zero items instead, because AWS
    // produces zero-byte .gz objects for empty partitions.
    virtual future<> close() = 0;

    virtual ~import_pipeline_interface() = default;
};

// Simple in-memory byte buffer used for testing the export pipeline without actual S3 or compression.
// Represents content of single file. Allows both exporting and importing data.
class in_memory_test_storage {
    std::vector<std::byte> _data;
    bool _read_flushed = false;
    bool _write_flushed = false;
public:
    void append(std::span<const std::byte> bytes) {
        _data.insert(_data.end(), bytes.begin(), bytes.end());
    }
    std::span<const std::byte> data() const { return _data; }

    // for testing calling `flush` methods - pipeline will call those methods when flush / flush_and_close is called, and we want to verify that.
    void flush_read() { _read_flushed = true; }
    void flush_write() { _write_flushed = true; }
    bool is_read_flushed() const { return _read_flushed; }
    bool is_write_flushed() const { return _write_flushed; }
};

struct in_memory_target_config {
    std::shared_ptr<in_memory_test_storage> storage;
};

// Configuration for the S3 target of either pipeline. The sink writes the object with
// `s3::client::make_upload_jumbo_sink_and_get_etag`, the source reads it with
// `s3::client::make_chunked_download_source`; see utils/s3/client.hh for the object size limits.
// `as`, if set, is handed to the S3 client by both pipelines:
//  - source: an abort makes `read_all()` fail with an exception; `close()` is still required,
//  - sink: an abort makes `process()` fail with an exception; `flush_and_close()` is still required.
// If set, it must stay valid until the future returned by `flush_and_close()` (sink) or `close()`
// (source) resolves.
struct s3_target_config {
    shared_ptr<s3::client> client;
    sstring object_name;
    abort_source *as = nullptr;
};

// Create sink pipeline for a single file. Depending on the configuration:
// - in_memory_target_config - creates in-memory sink pipeline for testing.
//   You should not use the same in_memory_test_storage object for sink and source pipeline simultaneously -
//   you need to complete sink pipeline first, then create and run source pipeline.
// - s3_target_config - creates sink pipeline that will write to S3 object. The object will be created if it doesn't exist, or overwritten if it does.
// The function is a coroutine, because a pipeline which fails to build half way has to close the stages
// it already created - closing a storage sink is asynchronous. Closing them finalizes the target object,
// so a failed call can leave an empty object behind, overwriting whatever was stored under that name before -
// see `export_pipeline_interface::flush_and_close()` above for what that means for the caller, and the
// definition of this function for why that object appears and why it is not worth avoiding.
future<std::unique_ptr<export_pipeline_interface>> create_sink_pipeline(std::variant<in_memory_target_config, s3_target_config> target_config, compression_type compression);

// Create source pipeline for a single file. Depending on the configuration:
// - in_memory_target_config - creates in-memory source pipeline for testing.
//   You should not use the same in_memory_test_storage object for sink and source pipeline simultaneously -
//   you need to complete sink pipeline first, then create and run source pipeline.
// - s3_target_config - creates source pipeline that will read from S3 object.
// A caller that wants to know whether it read a whole export must let `close()` succeed - with gzip
// compression that is where the object's completeness is checked, see `import_pipeline_interface::close()`.
future<std::unique_ptr<import_pipeline_interface>> create_source_pipeline(std::variant<in_memory_target_config, s3_target_config> target_config, std::function<future<>(rjson::value)> on_item, compression_type compression);


/// Full table scan of an Alternator table, calling `cb` with every item (DynamoDB-style JSON).
///
/// Reads page by page at (LOCAL_)QUORUM. Concurrent writes never make it fail or restart:
///  - every item present for the whole scan is visited exactly once; items added, deleted or
///    expired during the scan may or may not be visited, but never twice,
///  - it's not a snapshot - an updated item is visited with either its old or new value,
///  - `cb` calls are sequential, in any order, in the export/import scheduling group.
///
/// Transient read errors are retried indefinitely with backoff and smaller pages, except a
/// read failure persisting at a single-row page, which fails the scan. A NetworkTopologyStrategy
/// keyspace with RF=0 in the local DC fails with `api_error::validation`.
///
/// An abort of `as` fails the scan with `as.abort_requested_exception_ptr()`, after at most one
/// in-flight read (`alternator_timeout_in_ms`) or `cb` call.
///
/// Replica reads and read-repair writes started by the scan may hold a copy of `permit` after
/// the future resolves, until their deadlines: at most the longer of `alternator_timeout_in_ms`
/// and `write_request_timeout_in_ms`. Keep its semaphore alive that long.
///
/// IMPORTANT: this is an *internal* read - no authorization check (the caller must do it)
/// and no user's service level applies.

seastar::future<> export_scan_table(
    service::storage_proxy& proxy,
    schema_ptr schema,
    seastar::abort_source& as,
    service_permit permit,
    seastar::noncopyable_function<seastar::future<>(rjson::value)> cb);

// Owns the background fibers of S3 export (the export workers and the
// garbage collector) and their state. The member functions are in export.cc.
class executor::s3_exports {
    executor& _e;
    // Holds the background fibers. stop() closes it, so that all fibers
    // finish before the executor is destroyed.
    seastar::gate _gate;
    // stop() aborts it before it closes _gate, to stop the exports early.
    seastar::abort_source _as;
    // The number of export fibers running on this shard, see try_reserve_export_slot().
    // A fiber of finish_export_in_background() also counts. It holds the slot
    // of its export until it writes the terminal state. Until then
    // DescribeExport still reports that export as IN_PROGRESS.
    size_t _running_exports = 0;
    // The time of the next garbage collection round. The first value is in the
    // past, so that the first round does not wait. request_gc_round() moves the
    // time earlier, run_garbage_collector() sets it after each round.
    seastar::lowres_clock::time_point _gc_round_at = seastar::lowres_clock::time_point::min();
    // Waits for `_gc_round_at`, see run_garbage_collector().
    future<bool> wait_for_next_gc_round();
    // Asks for a garbage collection round in `delay`, if no earlier round is
    // due. Only gc_node_down_subscriber calls it.
    void request_gc_round(seastar::lowres_clock::duration delay);
    // The gossip subscriber which asks for a round after a node-down event.
    // Defined in export.cc.
    class gc_node_down_subscriber;
public:
    explicit s3_exports(executor& e) : _e(e) {}
    // True after stop() aborted the exports, i.e. during a clean shutdown.
    bool abort_requested() const noexcept { return _as.abort_requested(); }
    // A reserved slot of the counter above. The fiber holds it while it
    // runs. The destructor releases the slot.
    class slot {
        size_t* _counter;
        friend class s3_exports;
        explicit slot(size_t& counter) noexcept : _counter(&counter) { ++*_counter; }
    public:
        slot(slot&& other) noexcept : _counter(std::exchange(other._counter, nullptr)) {}
        ~slot() { if (_counter) { --*_counter; } }
    };
    // Reserves an export slot on this shard. Returns nullopt if
    // alternator_max_concurrent_exports_per_shard exports already run.
    std::optional<slot> try_reserve_export_slot();
    // Marks as FAILED the S3 exports that no node runs. It does not resume
    // exports and does not delete data from S3. An export is collected if its
    // node:
    //  - is not a member of the cluster any more (it was removed or
    //    replaced), or
    //  - has a gossip generation newer than the export (this includes
    //    previous incarnations of this node), or
    //  - is unreachable for a long time (DOWN alone is not sufficient).
    // A member the gossiper does not know yet counts as running, so its
    // exports are not collected.
    // Runs one round at startup, then periodically, until stop(). A node-down
    // event from the gossiper also starts a round.
    // executor::start() starts it on shard 0 of each node.
    future<> run_garbage_collector();
    // Starts a background fiber that does the export of `row`. The fiber
    // writes the result to the export row and holds `export_slot` until it
    // finishes. If that write fails, the fiber passes the row and the slot
    // to finish_export_in_background().
    void start_export(slot export_slot, schema_ptr schema, export_row row, std::string table_arn, std::string s3_bucket,
            std::string s3_prefix);
    // Starts a background fiber that writes the terminal state of `row`.
    // It retries until the write succeeds or the node stops. Use it when
    // that write fails and the caller cannot wait. The garbage collector
    // does not collect IN_PROGRESS rows of the current incarnation, so
    // this node must finish them.
    // The fiber never gives up the write. It keeps `export_slot`, which the
    // caller must hold for the export of `row`. Thus the number of these
    // fibers, and of the rows they hold, has the same bound as the number
    // of exports. A write which keeps failing stops new exports.
    void finish_export_in_background(slot export_slot, export_row row);
    // Aborts the fibers above and waits for them to finish.
    future<> stop();
};

} // namespace alternator
