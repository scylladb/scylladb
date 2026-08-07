/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <cstddef>
#include <functional>
#include <memory>
#include <span>
#include <vector>
#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/util/noncopyable_function.hh>
#include "schema/schema_fwd.hh"
#include "service_permit.hh"
#include "utils/rjson.hh"

namespace service {
class storage_proxy;
}

namespace alternator {

// An interface encapsulating write (sink) pipeline for exporting data. Is used to implement DynamoDB export api (ExportTableToPointInTime call).
// The pipeline is a multistage processing unit, which takes `rjson::value` item (of any content), serializes it as-is and writes it depending on the configuration.
// Currently supporting only test-only in-memory pipeline, serializing to raw text JSON lines. In the future we will add support for S3, compression and different formats (e.g. Ion, CSV).
// Call respective factory method below (`create_in_memory_sink_pipeline`) to construct.
// Call `process()` method for each item (they might come in random order) - they will be serialized and written to the appropriate sink.
// After all items are processed, call `flush_and_close()` to flush and finalize the pipeline - the call is mandatory, otherwise part of the data might not be written.
// Calling `flush_and_close()` is required and needs to be done manually.
// Calls to `process()` and `flush_and_close()` must be serialized, i.e. each call is allowed only after previous call's future is completed.
struct export_pipeline_interface {
    // Invokes whole pipeline for a single item. The future will complete once item is processed.
    // This doesn't mean the item hit external storage, but you're free to process another item.
    // Call to `process()` is allowed only after previous call to `process()` or `flush_and_close()` future is completed.
    // Caller is responsible for ensuring `item` is kept alive until the future is completed.
    virtual seastar::future<> process(const rjson::value &item) = 0;

    // Flushes and closes the pipeline. The future will complete once all items are flushed and pipeline is finalized.
    // Do not call process() after calling flush_and_close().
    virtual seastar::future<> flush_and_close() = 0;

    virtual ~export_pipeline_interface() = default;
};

// An interface encapsulating read (source) pipeline. This mirrors write (sink) pipeline - what sink pipeline can produce, source pipeline will consume.
// This will be used in future for DynamoDB import api (ImportTable call).
// Added currently for testing purposes - so we have a consistent way to read exported data without relying on connection to S3 / DynamoDB.
// Call respective factory method below (`create_in_memory_source_pipeline`) to construct.
// Call `read()` (only once!) method to start reading the data - it will read all data, pass it through the decompressor and parser
// and call the callback provided to the factory function for each parsed item. The pipeline will wait
// for each callback's future to complete before processing the next item.
// After all data is read (the future from `read()` call completes), call `flush_and_close()` to flush and finalize the pipeline.
// Calling `flush_and_close()` is required and needs to be done manually.
struct import_pipeline_interface {
    // Reads all available data from the source, feeds it through the decompression and parsing pipeline,
    // and invokes the on_item callback (passed to the pipeline constructor function) for each parsed item.
    // The future completes after source is exhausted.
    // Note: you still need to call `flush_and_close()` to finalize the pipeline - there might be some remaining data to process.
    virtual seastar::future<> read() = 0;

    // Flushes and closes the pipeline. The future will complete once all remaining, already read data is processed, flushed and pipeline is finalized.
    // The call doesn't read additional data.
    // Do not call read() after calling flush_and_close().
    virtual seastar::future<> flush_and_close() = 0;

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

// Create in-memory sink pipeline for a single file
// You should not use the same in_memory_test_storage object for sink and source pipeline simultaneously -
// you need to complete sink pipeline first, then create and run source pipeline.
std::unique_ptr<export_pipeline_interface> create_in_memory_sink_pipeline(in_memory_test_storage&);

// Create in-memory source pipeline for a single file
// You should not use the same in_memory_test_storage object for sink and source pipeline simultaneously -
// you need to complete sink pipeline first, then create and run source pipeline.
std::unique_ptr<import_pipeline_interface> create_in_memory_source_pipeline(in_memory_test_storage &, std::function<seastar::future<>(rjson::value)> on_item);

/// Perform a full table scan over an Alternator table, calling `cb` for
/// every item found. The callback receives an `rjson::value` representing
/// one DynamoDB-style item (JSON object with typed attribute values).
///
/// Guarantees:
///  - Every item in the table is visited exactly once as long as table is not modified during the scan -
///    it's unspecified if newly added items during the scan are visited or not.
///    Similarly it's unspecified if deleted items during the scan are visited or not.
///  - Calls to `cb` are sequential (never parallel).
///  - Items may be visited in any order (not necessarily sort-key order).
///  - Aborting `as` stops the scan by throwing `abort_requested_exception`.
///
/// The function underneath performs global scan over whole table, using single-threaded query_pager.
/// The scan uses LOCAL_QUORUM consistency and bypasses the cache to avoid polluting it.
/// The scan takes ownership of `permit` and holds it for the duration of the scan.

seastar::future<> scan_table(
    service::storage_proxy& proxy,
    schema_ptr schema,
    seastar::abort_source& as,
    service_permit permit,
    seastar::noncopyable_function<seastar::future<>(rjson::value)> cb);

/// Hardcoded page size for scan_table - nothing special about the number itself, but it's hardcoded.
/// The number is public so tests could use it to verify pagination works.
static constexpr uint32_t scan_table_page_size = 4096u;

} // namespace alternator
