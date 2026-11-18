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
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sstring.hh>
#include "alternator/executor.hh"
#include "alternator/export.hh"
#include "alternator/node_incarnation.hh"
#include "utils/rjson.hh"

namespace db { struct alternator_import_row; }

namespace alternator {

// A row of the imports table, as stored (see db/system_distributed_keyspace.hh).
using import_row = db::alternator_import_row;

// The tag which marks a table created by an import while the import fills it, see executor.cc.
extern const sstring IMPORT_IN_PROGRESS_TAG_KEY;

// The InputFormat values ImportTable supports so far.
enum class import_input_format {
    dynamodb_json,
};

// The InputCompressionType values ImportTable supports so far.
enum class import_input_compression_type {
    none,
    gzip,
};

// A validated ImportTable request.
struct import_table_request {
    std::string client_token;
    std::string s3_bucket;
    std::string s3_key_prefix;
    import_input_format input_format;
    import_input_compression_type input_compression_type;
    // Only the TableCreationParameters members ImportTable accepts, kept
    // here so there's no need to re-serialize for the response.
    rjson::value table_creation_parameters;
    create_table_params validated_table_params;
    // Set, to the error to refuse it with, for a request DynamoDB would accept but Alternator cannot
    // run yet (CSV, ION, ZSTD, InputFormatOptions). ImportTable refuses it only after the
    // ClientToken lookup, because a replay returns the import of its token whatever its other
    // parameters. When it is set, input_format and input_compression_type may not be the request's.
    std::optional<std::string> unsupported;
};

// Validate an ImportTable request, throwing api_error::validation on the
// first problem - also for a string which is not valid UTF-8 - except for the
// input formats and options which Alternator does not support yet (CSV, ION,
// ZSTD, InputFormatOptions), which are recorded in
// import_table_request::unsupported. Only parses: looks up neither imports
// nor tables.
import_table_request parse_import_table_request(const rjson::value& request, const gms::feature_service& feat, db::tablets_mode_t::mode tablets_mode);

// The ImportTableDescription of an import, as ImportTable (and, in the future, DescribeImport)
// reports it. Throws std::runtime_error if the stored request of the row cannot be parsed - a
// corrupt row.
rjson::value make_import_table_description(const import_row& row);

// One object of the source of an import.
struct import_source_object {
    sstring key;
    // The size of the object in bytes. An object of size 0 holds no items, so it is not read
    // (DynamoDB exports write such objects, also with GZIP, for empty partitions).
    uint64_t size = 0;
};

// The objects which an import reads: those under the S3 bucket and key prefix of the import.
// The import calls next_object() until it returns nullopt, and then close() - also after a
// failure.
class import_source {
public:
    virtual ~import_source() = default;
    // Returns the next object, in the order in which the import reads them, or nullopt after
    // the last one. The import calls it once before it creates the target table, so that a source
    // which cannot be listed - e.g. a missing bucket - fails the import without leaving a table
    // behind. To fail the import with a specific FailureCode, throw import_failure.
    virtual future<std::optional<import_source_object>> next_object() = 0;
    // Creates the pipeline which reads `object`. See create_source_pipeline() for `on_item`.
    virtual future<std::unique_ptr<import_pipeline_interface>> open(const import_source_object& object,
            std::function<future<>(rjson::value)> on_item, compression_type compression) = 0;
    virtual future<> close() = 0;
};

// Makes the source of an import of the objects under `s3_key_prefix` in `s3_bucket`. `as` is
// aborted when the node shuts down, and must abort the reads of the source.
using import_source_factory = std::function<future<std::unique_ptr<import_source>>(
        sstring s3_bucket, sstring s3_key_prefix, abort_source& as)>;

// A source over in-memory objects, for tests. Returns the objects in the order given.
std::unique_ptr<import_source> make_in_memory_import_source(
        std::vector<std::pair<sstring, std::shared_ptr<in_memory_test_storage>>> objects);

// Fails an import with the FailureCode `code` and the FailureMessage what().
class import_failure : public std::runtime_error {
    std::string _code;
public:
    import_failure(std::string code, const std::string& message)
        : std::runtime_error(message), _code(std::move(code)) {}
    const std::string& code() const noexcept { return _code; }
};

// Owns the background fibers of ImportTable - the imports themselves, and the garbage collector
// which fails the imports of nodes which stopped - and their state.
// An import is a state machine of its row in the imports table, the pair (import_status,
// node_id), changed only with conditional writes:
//  - ImportTable inserts the row as IN_PROGRESS, owned by the node which accepted the request,
//    and that node runs the import, on the shard of the request;
//  - the import removes the import-in-progress tag of the table it created, and ends as
//    COMPLETED, or as FAILED (ItemValidationError if only items, lines or objects were skipped,
//    see count_error()). For any other failure it first writes FAILING with the failure code, so
//    that the garbage collector can finish the cleanup if this node stops;
//  - if ImportTable fails after it inserted the client token row, it writes FAILED itself;
//  - the garbage collector takes the IN_PROGRESS and FAILING rows of node incarnations which can
//    no longer run them, and makes them FAILED the same way.
class executor::s3_imports {
    executor& _e;
    // Holds the background fibers. stop() closes it.
    seastar::gate _gate;
    // stop() aborts it before it closes _gate, to stop the imports early.
    seastar::abort_source _as;
    // The number of reserved import slots on this shard: of ImportTable requests being accepted, of
    // running imports and of final writes being retried. See try_reserve_import_slot().
    size_t _reserved_import_slots = 0;
    import_source_factory _source_factory;
    // The scheduler of the rounds of the garbage collector. Engaged by run_garbage_collector(), so on
    // shard 0 only.
    std::optional<gc_round_scheduler> _gc;

    // Runs one import. Defined in import.cc.
    class import_task;
    // Removes the import-in-progress tag of the table which the import `import_arn` created, so
    // that DescribeTable reports the table as ACTIVE. Does nothing if the table is gone, or if the
    // tag is not this import's. Runs on shard 0, where the executor makes its schema changes; then
    // waits for schema agreement.
    future<> remove_import_in_progress_tag(::table_id table_id, sstring import_arn);
    // One round of the garbage collector. Runs on shard 0.
    future<> garbage_collect();
    // Takes `row` and makes it FAILED, if its node can no longer run it.
    future<> collect_import_row(const running_node_ids& running_nodes, const live_node_identifier& self_node_id, const import_row& row);
public:
    explicit s3_imports(executor& e);
    // True once stop() aborted the imports: late in a clean shutdown, after the server stopped
    // serving requests.
    bool abort_requested() const noexcept { return _as.abort_requested(); }
    // A reserved slot of _reserved_import_slots. ImportTable holds it until it starts the import, then the
    // import fiber holds it, then finish_import_in_background() if needed. The destructor releases it.
    class slot {
        size_t* _counter;
        friend class s3_imports;
        explicit slot(size_t& counter) noexcept : _counter(&counter) { ++*_counter; }
    public:
        slot(slot&& other) noexcept : _counter(std::exchange(other._counter, nullptr)) {}
        slot& operator=(slot&&) = delete;
        ~slot() { if (_counter) { --*_counter; } }
    };
    // Reserves an import slot on this shard. Returns nullopt if
    // alternator_max_concurrent_imports_per_shard slots are already reserved.
    std::optional<slot> try_reserve_import_slot();
    // Starts the background fiber which runs the import of `row`, which ImportTable has just
    // inserted as IN_PROGRESS and owned by this node.
    void start_import(slot&& import_slot, import_row row);
    // Starts a background fiber which writes the terminal state of `row`, retrying until the write
    // succeeds or the node stops, and keeps `import_slot` until then. For a caller whose write of
    // that state failed and which cannot wait: the garbage collector does not collect the rows of
    // the current incarnation of this node, so this node must finish them. The import fiber and
    // ImportTable pass their slot; the garbage collector, whose rows hold none, passes nullopt.
    void finish_import_in_background(std::optional<slot> import_slot, import_row row);
    // Fails the imports which no node runs any more, as described above. Runs one round at start,
    // then periodically and after node-down events, until stop(). executor::start() starts it on
    // shard 0 of each node.
    future<> run_garbage_collector();
    // Replaces the source of the imports which start after the call. For tests: the default
    // source does not read S3 yet.
    void set_source_factory(import_source_factory factory) { _source_factory = std::move(factory); }
    // Aborts the fibers above and waits for them to finish.
    future<> stop();
private:
    // The fiber which start_import() starts.
    future<> run_import(std::unique_ptr<import_task> task, slot import_slot);
};

} // namespace alternator
