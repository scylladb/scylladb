/*
 * Copyright (C) 2015-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "version.hh"
#include "shared_sstable.hh"
#include "open_info.hh"
#include "sstables_registry.hh"
#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/enum.hh>
#include <seastar/core/shared_ptr.hh>
#include <unordered_set>
#include <unordered_map>
#include <variant>
#include "schema/schema_fwd.hh"
#include <seastar/core/stream.hh>
#include "encoding_stats.hh"
#include "filter.hh"
#include "utils/disk-error-handler.hh"
#include "sstables/progress_monitor.hh"
#include "db/commitlog/replay_position.hh"
#include "component_type.hh"
#include "column_translation.hh"
#include "stats.hh"
#include "utils/observable.hh"
#include "sstables/shareable_components.hh"
#include "sstables/storage.hh"
#include "sstables/generation_type.hh"
#include "mutation/mutation_fragment_stream_validator.hh"
#include "readers/mutation_reader_fwd.hh"
#include "readers/mutation_reader.hh"
#include "tracing/trace_state.hh"
#include "utils/updateable_value.hh"
#include "dht/decorated_key.hh"

#include <seastar/util/optimized_optional.hh>

class sstable_assertions;
class cached_file;

namespace data_dictionary {
class storage_options;
}

namespace db {
class large_data_handler;
}

namespace sstables {

class random_access_reader;

class sstable_directory;
extern thread_local utils::updateable_value<bool> global_cache_index_pages;

namespace mc {
class writer;
}

namespace fs = std::filesystem;

extern logging::logger sstlog;
class key;
class sstable_writer;
class sstable_writer_v2;
class sstables_manager;
class metadata_collector;

struct foreign_sstable_open_info;

template<typename T>
concept ConsumeRowsContext =
    requires(T c, indexable_element el, size_t s) {
        { c.consume_input() } -> std::same_as<future<>>;
        { c.reset(el) } -> std::same_as<void>;
        { c.fast_forward_to(s, s) } -> std::same_as<future<>>;
        { c.position() } -> std::same_as<uint64_t>;
        { c.skip_to(s) } -> std::same_as<future<>>;
        { c.reader_position() } -> std::same_as<const sstables::reader_position_tracker&>;
        { c.eof() } -> std::same_as<bool>;
        { c.close() } -> std::same_as<future<>>;
    };

template <typename DataConsumeRowsContext>
requires ConsumeRowsContext<DataConsumeRowsContext>
class data_consume_context;

class index_reader;
class partition_index_cache;
class sstables_manager;

extern size_t summary_byte_cost(double summary_ratio);

struct sstable_writer_config {
    size_t promoted_index_block_size;
    size_t promoted_index_auto_scale_threshold;
    uint64_t max_sstable_size = std::numeric_limits<uint64_t>::max();
    bool backup = false;
    mutation_fragment_stream_validation_level validation_level;
    std::optional<db::replay_position> replay_position;
    std::optional<int> sstable_level;
    write_monitor* monitor = &default_write_monitor();
    run_id run_identifier = run_id::create_random_id();
    size_t summary_byte_cost;
    sstring origin;

private:
    explicit sstable_writer_config() {}
    friend class sstables_manager;
};

constexpr const char* normal_dir = "";
constexpr const char* staging_dir = "staging";
constexpr const char* upload_dir = "upload";
constexpr const char* snapshots_dir = "snapshots";
constexpr const char* quarantine_dir = "quarantine";
constexpr const char* pending_delete_dir = "pending_delete";
constexpr const char* tempdir_extension = ".sstable";

constexpr auto table_subdirectories = std::to_array({
    staging_dir,
    upload_dir,
    snapshots_dir,
    quarantine_dir,
    pending_delete_dir,
});

inline std::string_view state_to_dir(sstable_state state) {
    switch (state) {
    case sstable_state::normal:
        return normal_dir;
    case sstable_state::staging:
        return staging_dir;
    case sstable_state::quarantine:
        return quarantine_dir;
    case sstable_state::upload:
        return upload_dir;
    }
}

inline sstable_state state_from_dir(std::string_view dir) {
    if (dir == "") {
        return sstable_state::normal;
    }
    if (dir == staging_dir) {
        return sstable_state::staging;
    }
    if (dir == quarantine_dir) {
        return sstable_state::quarantine;
    }
    if (dir == upload_dir) {
        return sstable_state::upload;
    }

    throw std::runtime_error(format("Unknown sstable state dir {}", dir));
}

// FIXME -- temporary, move to fs storage after patching the rest
inline fs::path make_path(std::string_view table_dir, sstable_state state) {
    fs::path ret(table_dir);
    if (state != sstable_state::normal) {
        ret /= state_to_dir(state);
    }
    return ret;
}

constexpr const char* repair_origin = "repair";

class delayed_commit_changes {
    std::unordered_set<sstring> _dirs;
    friend class filesystem_storage;
public:
    future<> commit();
};

class sstable : public enable_lw_shared_from_this<sstable> {
    friend ::sstable_assertions;
public:
    using version_types = sstable_version_types;
    using format_types = sstable_format_types;
    using manager_list_link_type = bi::list_member_hook<bi::link_mode<bi::auto_unlink>>;
    using manager_set_link_type = bi::set_member_hook<bi::link_mode<bi::auto_unlink>>;
public:
    sstable(schema_ptr schema,
            sstring table_dir,
            const data_dictionary::storage_options& storage,
            generation_type generation,
            sstable_state state,
            version_types v,
            format_types f,
            db::large_data_handler& large_data_handler,
            sstables_manager& manager,
            gc_clock::time_point now,
            io_error_handler_gen error_handler_gen,
            size_t buffer_size);
    sstable& operator=(const sstable&) = delete;
    sstable(const sstable&) = delete;
    sstable(sstable&&) = delete;

    // disk_read_range describes a byte ranges covering part of an sstable
    // row that we need to read from disk. Usually this is the whole byte
    // range covering a single sstable row, but in very large rows we might
    // want to only read a subset of the atoms which we know contains the
    // columns we are looking for.
    struct disk_read_range {
        // TODO: this should become a vector of ranges
        uint64_t start;
        uint64_t end;

        disk_read_range() : start(0), end(0) {}
        disk_read_range(uint64_t start, uint64_t end) :
            start(start), end(end) { }
        explicit operator bool() const {
            return start != end;
        }
    };

    static component_type component_from_sstring(version_types version, const sstring& s);
    static sstring component_basename(const sstring& ks, const sstring& cf, version_types version, generation_type generation,
                                      format_types format, component_type component);
    static sstring component_basename(const sstring& ks, const sstring& cf, version_types version, generation_type generation,
                                      format_types format, sstring component);
    static sstring filename(const sstring& dir, const sstring& ks, const sstring& cf, version_types version, generation_type generation,
                            format_types format, component_type component);
    static sstring filename(const sstring& dir, const sstring& ks, const sstring& cf, version_types version, generation_type generation,
                            format_types format, sstring component);

    // load sstable using components shared by a shard
    future<> load(foreign_sstable_open_info info) noexcept;
    // Load metadata components from disk
    future<> load_metadata(sstable_open_config cfg = {}, bool validate = true) noexcept;
    // load all components from disk
    // this variant will be useful for testing purposes and also when loading
    // a new sstable from scratch for sharing its components.
    future<> load(const dht::sharder& sharder, sstable_open_config cfg = {}) noexcept;
    future<> open_data(sstable_open_config cfg = {}) noexcept;
    future<> update_info_for_opened_data(sstable_open_config cfg = {});

    // Load set of shards that own the SSTable, while reading the minimum
    // from disk to achieve that.
    future<> load_owner_shards(const dht::sharder& sharder);

    // Call as the last method before the object is destroyed.
    // No other uses of the object can happen at this point.
    future<> destroy();

    // Move the sstable between states
    //
    // Known states are normal, staging, upload and quarantine.
    // It's up to the storage driver how to implement this.
    future<> change_state(sstable_state to, delayed_commit_changes* delay = nullptr);

    // Filesystem-specific call to grab an sstable from upload dir and
    // put it into the desired destination assigning the given generation
    future<> pick_up_from_upload(sstable_state to, generation_type new_generation);

    generation_type generation() const {
        return _generation;
    }

    // Returns a mutation_reader for given range of partitions.
    //
    // Precondition: if the slice is reversed, the schema must be reversed as well.
    mutation_reader make_reader(
            schema_ptr query_schema,
            reader_permit permit,
            const dht::partition_range& range,
            const query::partition_slice& slice,
            tracing::trace_state_ptr trace_state = {},
            streamed_mutation::forwarding fwd = streamed_mutation::forwarding::no,
            mutation_reader::forwarding fwd_mr = mutation_reader::forwarding::yes,
            read_monitor& monitor = default_read_monitor());

    // A reader which doesn't use the index at all. It reads everything from the
    // sstable and it doesn't support skipping.
    mutation_reader make_crawling_reader(
            schema_ptr schema,
            reader_permit permit,
            tracing::trace_state_ptr trace_state = {},
            read_monitor& monitor = default_read_monitor());

    // Returns mutation_source containing all writes contained in this sstable.
    // The mutation_source shares ownership of this sstable.
    mutation_source as_mutation_source();

    future<> write_components(mutation_reader mr,
            uint64_t estimated_partitions,
            schema_ptr schema,
            const sstable_writer_config&,
            encoding_stats stats);

    sstable_writer get_writer(const schema& s,
        uint64_t estimated_partitions,
        const sstable_writer_config&,
        encoding_stats enc_stats,
        shard_id shard = this_shard_id());

    // Validates the content of the sstable.
    // Reports all errors via the provided error handler.
    // Returns the count of all validation errors found.
    // Can be aborted via the abort-source parameter.
    // If aborted, either via the abort-source or via unrecoverable errors
    // (e.g. parse error), it will return with validation error count seen up to
    // the abort. In the latter case it will call the error-handler before doing so.
    future<uint64_t> validate(reader_permit permit, abort_source& abort,
            std::function<void(sstring)> error_handler, sstables::read_monitor& monitor = default_read_monitor());

    encoding_stats get_encoding_stats_for_compaction() const;

    future<> seal_sstable(bool backup);

    static uint64_t get_estimated_key_count(const uint32_t size_at_full_sampling, const uint32_t min_index_interval) {
        return ((uint64_t)size_at_full_sampling + 1) * min_index_interval;
    }
    // Size at full sampling is calculated as if sampling were static, using minimum index as a strict sampling interval.
    static uint64_t get_size_at_full_sampling(const uint64_t key_count, const uint32_t min_index_interval) {
        return std::ceil(float(key_count) / min_index_interval) - 1;
    }

    uint64_t get_estimated_key_count() const {
        return get_estimated_key_count(_components->summary.header.size_at_full_sampling, _components->summary.header.min_index_interval);
    }

    uint64_t estimated_keys_for_range(const dht::token_range& range);

    std::vector<dht::decorated_key> get_key_samples(const schema& s, const dht::token_range& range);

    // mark_for_deletion() specifies that a sstable isn't relevant to the
    // current shard, and thus can be deleted by the deletion manager, if
    // all shards sharing it agree. In case the sstable is unshared, it's
    // guaranteed that all of its on-disk files will be deleted as soon as
    // the in-memory object is destroyed.
    void mark_for_deletion() {
        _marked_for_deletion = mark_for_deletion::marked;
    }

    bool marked_for_deletion() const {
        return _marked_for_deletion == mark_for_deletion::marked;
    }

    const std::set<generation_type>& compaction_ancestors() const {
        return _compaction_ancestors;
    }

    void add_ancestor(generation_type generation) {
        _compaction_ancestors.insert(generation);
    }

    // Returns true iff this sstable contains data which belongs to many shards.
    bool is_shared() const;

    // Returns uncompressed size of data component.
    uint64_t data_size() const;
    // Returns on-disk size of data component.
    uint64_t ondisk_data_size() const;

    uint64_t index_size() const {
        return _index_file_size;
    }
    file& index_file() {
        return _index_file;
    }
    file uncached_index_file();
    // Returns size of bloom filter data.
    uint64_t filter_size() const;

    db_clock::time_point data_file_write_time() const {
        return _data_file_write_time;
    }

    uint64_t filter_memory_size() const {
        return _components->filter->memory_size();
    }

    version_types get_version() const {
        return _version;
    }

    // Returns the total bytes of all components.
    uint64_t bytes_on_disk() const;

    const partition_key& get_first_partition_key() const;
    const partition_key& get_last_partition_key() const;

    const dht::decorated_key& get_first_decorated_key() const;
    const dht::decorated_key& get_last_decorated_key() const;

    // SSTable comparator using the first key (decorated key).
    std::strong_ordering compare_by_first_key(const sstable& other) const;

    // SSTable comparator using the max timestamp.
    // Return values are those of a trichotomic comparison.
    int compare_by_max_timestamp(const sstable& other) const;

    sstring component_basename(component_type f) const {
        return component_basename(_schema->ks_name(), _schema->cf_name(), _version, _generation, _format, f);
    }

    sstring get_filename() const {
        return filename(component_type::Data);
    }

    sstring toc_filename() const {
        return filename(component_type::TOC);
    }

    sstring index_filename() const {
        return filename(component_type::Index);
    }

    bool requires_view_building() const noexcept { return _state == sstable_state::staging; }

    bool is_quarantined() const noexcept { return _state == sstable_state::quarantine; }

    bool is_uploaded() const noexcept { return _state == sstable_state::upload; }

    std::vector<std::pair<component_type, sstring>> all_components() const;

    future<> snapshot(const sstring& dir) const;

    // Delete the sstable by unlinking all sstable files
    // Ignores all errors.
    // Caller may pass sync_dir::no for batching multiple deletes in the same directory,
    // and make sure the directory is sync'ed on or after the last call.
    future<> unlink(storage::sync_dir sync = storage::sync_dir::yes) noexcept;

    db::large_data_handler& get_large_data_handler() {
        return _large_data_handler;
    }

    void assert_large_data_handler_is_running();

    /**
     * Note. This is using the Origin definition of
     * max_data_age, which is load time. This could maybe
     * be improved upon.
     */
    gc_clock::time_point max_data_age() const {
        return _now;
    }
    std::vector<sstring> component_filenames() const;

    utils::observer<sstable&> add_on_closed_handler(std::function<void (sstable&)> on_closed_handler) noexcept {
        return _on_closed.observe(on_closed_handler);
    }

    utils::observer<sstable&> add_on_delete_handler(std::function<void (sstable&)> on_delete_handler) noexcept {
        return _on_delete.observe(on_delete_handler);
    }

    template<typename Func, typename... Args>
    requires std::is_nothrow_move_constructible_v<Func>
    auto sstable_write_io_check(Func&& func, Args&&... args) const noexcept {
        return do_io_check(_write_error_handler, std::forward<Func>(func), std::forward<Args>(args)...);
    }

    // required since touch_directory has an optional parameter
    auto sstable_touch_directory_io_check(std::filesystem::path name) const noexcept {
        return do_io_check(_write_error_handler, [name = std::move(name)] () mutable {
            return touch_directory(name.native());
        });
    }
    future<> close_files();

    /* Returns a lower-bound for the set of `position_in_partition`s appearing in the sstable across all partitions.
     *
     * Might be `before_all_keys` if, for example, the sstable comes from outside Scylla and lacks sufficient metadata,
     * or the sstable's schema does not have clustering columns.
     *
     * But if the schema has clustering columns and the sstable is sufficiently ``modern'',
     * the returned value should be equal to the smallest clustering key occurring in the sstable (across all partitions).
     *
     * The lower bound is inclusive: there might be a clustering row with position equal to min_position.
     */
    const position_in_partition& min_position() const {
        return _min_max_position_range.start();
    }

    /* Similar to min_position, but returns an upper-bound.
     * However, the upper-bound is exclusive: all positions are smaller than max_position.
     *
     * If certain conditions are satisfied (the same as for `min_position`, see above),
     * the returned value should be equal to after_key(ck), where ck is the greatest clustering key
     * occurring in the sstable (across all partitions).
     */
    const position_in_partition& max_position() const {
        return _min_max_position_range.end();
    }

    const position_in_partition& first_partition_first_position() const noexcept {
        return _first_partition_first_position;
    }

    const position_in_partition& last_partition_last_position() const noexcept {
        return _last_partition_last_position;
    }

    const storage& get_storage() const {
        return *_storage;
    }

private:
    sstring filename(component_type f) const {
        auto dir = _storage->prefix();
        return filename(dir, _schema->ks_name(), _schema->cf_name(), _version, _generation, _format, f);
    }

    friend class sstable_directory;
    friend class filesystem_storage;
    friend class s3_storage;
    friend class tiered_storage;

    size_t sstable_buffer_size;

    std::unordered_set<component_type, enum_hash<component_type>> _recognized_components;
    std::vector<sstring> _unrecognized_components;

    foreign_ptr<lw_shared_ptr<shareable_components>> _components = make_foreign(make_lw_shared<shareable_components>());
    column_translation _column_translation;
    std::optional<open_flags> _open_mode;
    // _compaction_ancestors track which sstable generations were used to generate this sstable.
    // it is then used to generate the ancestors metadata in the statistics or scylla components.
    std::set<generation_type> _compaction_ancestors;
    file _index_file;
    seastar::shared_ptr<cached_file> _cached_index_file;
    file _data_file;
    uint64_t _data_file_size;
    uint64_t _index_file_size;
    // on-disk size of components but data and index.
    uint64_t _metadata_size_on_disk = 0;
    db_clock::time_point _data_file_write_time;
    position_range _min_max_position_range = position_range::all_clustered_rows();
    position_in_partition _first_partition_first_position = position_in_partition::before_all_clustered_rows();
    position_in_partition _last_partition_last_position = position_in_partition::after_all_clustered_rows();
    std::vector<unsigned> _shards;
    std::optional<dht::decorated_key> _first;
    std::optional<dht::decorated_key> _last;
    run_id _run_identifier;
    utils::observable<sstable&> _on_closed;
    utils::observable<sstable&> _on_delete;

    lw_shared_ptr<file_input_stream_history> _single_partition_history = make_lw_shared<file_input_stream_history>();
    lw_shared_ptr<file_input_stream_history> _partition_range_history = make_lw_shared<file_input_stream_history>();
    lw_shared_ptr<file_input_stream_history> _index_history = make_lw_shared<file_input_stream_history>();

    schema_ptr _schema;
    generation_type _generation{0};
    sstable_state _state;

    std::unique_ptr<storage> _storage;

    const version_types _version;
    const format_types _format;

    filter_tracker _filter_tracker;
    std::unique_ptr<partition_index_cache> _index_cache;

    enum class mark_for_deletion {
        implicit = -1,
        none = 0,
        marked = 1
    } _marked_for_deletion = mark_for_deletion::none;
    bool _active = true;

    gc_clock::time_point _now;

    io_error_handler _read_error_handler;
    io_error_handler _write_error_handler;

    db::large_data_handler& _large_data_handler;
    sstables_manager& _manager;

    sstables_stats _stats;
    // link used by the _active list of sstables manager
    manager_list_link_type _manager_list_link;
    // link used by the _reclaimed set of sstables manager
    manager_set_link_type _manager_set_link;


    // The _large_data_stats map stores e.g. largest partitions, rows, cells sizes,
    // and max number of rows in a partition.
    //
    // It can be disengaged normally when loading legacy sstables that do not have this
    // information in their scylla metadata.
    std::optional<scylla_metadata::large_data_stats> _large_data_stats;
    sstring _origin;

    // Total reclaimable memory from all the components of the SSTable.
    // It is initialized to 0 to prevent the sstables manager from reclaiming memory
    // from the components before the SSTable has been fully loaded.
    mutable std::optional<size_t> _total_reclaimable_memory{0};
    // Total memory reclaimed so far from this sstable
    size_t _total_memory_reclaimed{0};
public:
    bool has_component(component_type f) const;
    sstables_manager& manager() { return _manager; }
    const sstables_manager& manager() const { return _manager; }

    static future<std::vector<sstring>> read_and_parse_toc(file f);
private:
    void unused(); // Called when reference count drops to zero
    future<file> open_file(component_type, open_flags, file_open_options = {}) const noexcept;

    template <component_type Type, typename T>
    future<> read_simple(T& comp);
    future<> do_read_simple(component_type type,
                            noncopyable_function<future<> (version_types, file&&, uint64_t sz)> read_component);
    // this variant closes the file on parse completion
    future<> do_read_simple(component_type type,
                            noncopyable_function<future<> (version_types, file)> read_component);

    template <component_type Type, typename T>
    void write_simple(const T& comp);
    void do_write_simple(file_writer&& writer,
                         noncopyable_function<void (version_types, file_writer&)> write_component);
    void do_write_simple(component_type type,
            noncopyable_function<void (version_types version, file_writer& writer)> write_component,
            unsigned buffer_size);

    void write_crc(const checksum& c);
    void write_digest(uint32_t full_checksum);

    future<file> new_sstable_component_file(const io_error_handler& error_handler, component_type f, open_flags flags, file_open_options options = {}) const noexcept;

    future<file_writer> make_component_file_writer(component_type c, file_output_stream_options options,
            open_flags oflags = open_flags::wo | open_flags::create | open_flags::exclusive) noexcept;

    void generate_toc();
    void open_sstable(const sstring& origin);

    future<> read_compression();
    void write_compression();

    future<> read_scylla_metadata() noexcept;

    void write_scylla_metadata(shard_id shard,
                               sstable_enabled_features features,
                               run_identifier identifier,
                               std::optional<scylla_metadata::large_data_stats> ld_stats);

    future<> read_filter(sstable_open_config cfg = {});

    void write_filter();
    // Rebuild a bloom filter from the index with the given number of
    // partitions, if the partition estimate provided during bloom
    // filter initialisation was not good.
    // This should be called only before an sstable is sealed.
    void maybe_rebuild_filter_from_index(uint64_t num_partitions);

    future<> read_summary() noexcept;

    void write_summary() {
        write_simple<component_type::Summary>(_components->summary);
    }

    // To be called when we try to load an SSTable that lacks a Summary. Could
    // happen if old tools are being used.
    future<> generate_summary();

    future<> read_statistics();
    void write_statistics();
    // Rewrite statistics component by creating a temporary Statistics and
    // renaming it into place of existing one.
    void rewrite_statistics();
    // Validate metadata that's used to optimize reads when user specifies
    // a clustering key range. If this specific metadata is incorrect, then
    // it should be cleared. Otherwise, it could lead to bad decisions.
    // Metadata is probably incorrect if generated by previous Scylla versions.
    void validate_min_max_metadata();
    // Validate metadata that's used to determine if sstable is fully expired
    // sstable that doesn't contain scylla component may contain wrong metadata,
    // and so max_local_deletion_time should be discarded for those.
    void validate_max_local_deletion_time();
    void validate_partitioner();

    void set_first_and_last_keys();

    // Create a position range based on the min/max_column_names metadata of this sstable.
    // It does nothing if schema defines no clustering key, and it's supposed
    // to be called when loading an existing sstable or after writing a new one.
    void set_min_max_position_range();

    // Loads first position of the first partition, and last position of the last
    // partition. Does nothing if schema defines no clustering key.
    future<> load_first_and_last_position_in_partition();

    future<> create_data() noexcept;

    // Note that only bloom filters are reclaimable by the following methods.
    // Return the total reclaimable memory in this SSTable
    size_t total_reclaimable_memory_size() const;
    // Reclaim memory from the components back to the system.
    size_t reclaim_memory_from_components();
    // Return memory reclaimed so far from this sstable
    size_t total_memory_reclaimed() const;
    // Reload components from which memory was previously reclaimed
    future<> reload_reclaimed_components();

public:
    // Finds first position_in_partition in a given partition.
    // If reversed is false, then the first position is actually the first row (can be the static one).
    // If reversed is true, then the first position is the last row (can be static if partition has a single static row).
    future<std::optional<position_in_partition>>
    find_first_position_in_partition(reader_permit permit, const dht::decorated_key& key, bool reversed);

    // Return an input_stream which reads exactly the specified byte range
    // from the data file (after uncompression, if the file is compressed).
    // Unlike data_read() below, this method does not read the entire byte
    // range into memory all at once. Rather, this method allows reading the
    // data incrementally as a stream. Knowing in advance the exact amount
    // of bytes to be read using this stream, we can make better choices
    // about the buffer size to read, and where exactly to stop reading
    // (even when a large buffer size is used).
    //
    // When created with `raw_stream::yes`, the sstable data file will be
    // streamed as-is, without decompressing (if compressed).
    using raw_stream = bool_class<class raw_stream_tag>;
    input_stream<char> data_stream(uint64_t pos, size_t len,
            reader_permit permit, tracing::trace_state_ptr trace_state, lw_shared_ptr<file_input_stream_history> history, raw_stream raw = raw_stream::no);

    // Read exactly the specific byte range from the data file (after
    // uncompression, if the file is compressed). This can be used to read
    // a specific row from the data file (its position and length can be
    // determined using the index file).
    // This function is intended (and optimized for) random access, not
    // for iteration through all the rows.
    future<temporary_buffer<char>> data_read(uint64_t pos, size_t len, reader_permit permit);

private:
    future<summary_entry&> read_summary_entry(size_t i);

    // FIXME: pending on Bloom filter implementation
    bool filter_has_key(const schema& s, const dht::decorated_key& dk) { return filter_has_key(key::from_partition_key(s, dk._key)); }

    std::optional<std::pair<uint64_t, uint64_t>> get_sample_indexes_for_range(const dht::token_range& range);
    std::optional<std::pair<uint64_t, uint64_t>> get_index_pages_for_range(const dht::token_range& range);

    std::vector<unsigned> compute_shards_for_this_sstable(const dht::sharder&) const;
    template <typename Components>
    static auto& get_mutable_serialization_header(Components& components) {
        auto entry = components.statistics.contents.find(metadata_type::Serialization);
        if (entry == components.statistics.contents.end()) {
            throw std::runtime_error("Serialization header metadata not available");
        }
        auto& p = entry->second;
        if (!p) {
            throw std::runtime_error("Statistics is malformed");
        }
        serialization_header& s = *static_cast<serialization_header *>(p.get());
        return s;
    }

    future<> open_or_create_data(open_flags oflags, file_open_options options = {}) noexcept;
    // runs in async context (called from storage::open)
    void write_toc(file_writer w);
public:
    future<> read_toc() noexcept;

    schema_ptr get_schema() const {
        return _schema;
    }

    bool has_scylla_component() const {
        return has_component(component_type::Scylla);
    }

    // Returns an optional boolean value set to true iff the
    // sstable's `originating_host_id` in stats metadata equals
    // this node's host_id.
    //
    // The returned value may be nullopt if:
    // - The sstable format is older than version_types::me, or
    // - The local host_id is unknown yet (may happen early in the start-up process)
    std::optional<bool> originated_on_this_node() const;

    void validate_originating_host_id() const;

    bool has_correct_promoted_index_entries() const {
        return _schema->is_compound() || !has_scylla_component() || _components->scylla_metadata->has_feature(sstable_feature::NonCompoundPIEntries);
    }

    bool has_correct_non_compound_range_tombstones() const {
        return _schema->is_compound() || !has_scylla_component() || _components->scylla_metadata->has_feature(sstable_feature::NonCompoundRangeTombstones);
    }

    bool has_shadowable_tombstones() const {
        return has_scylla_component() && _components->scylla_metadata->has_feature(sstable_feature::ShadowableTombstones);
    }

    sstable_enabled_features features() const {
        if (!has_scylla_component()) {
            return {};
        }
        return _components->scylla_metadata->get_features();
    }

    const scylla_metadata* get_scylla_metadata() const {
        return _components->scylla_metadata ? &*_components->scylla_metadata : nullptr;
    }

    run_id run_identifier() const {
        return _run_identifier;
    }

    bool has_correct_max_deletion_time() const {
        return (_version >= sstable_version_types::mc) || has_scylla_component();
    }

    bool filter_has_key(const key& key) const {
        return _components->filter->is_present(bytes_view(key));
    }

    /*!
     * \brief check if the sstable contains the given key.
     * The method would search that the key is actually
     * found in the sstable not just in the filter.
     *
     */
    future<bool> has_partition_key(const utils::hashed_key& hk, const dht::decorated_key& dk);

    bool filter_has_key(utils::hashed_key key) const {
        return _components->filter->is_present(key);
    }

    bool filter_has_key(const schema& s, partition_key_view key) const {
        return filter_has_key(key::from_partition_key(s, key));
    }

    static utils::hashed_key make_hashed_key(const schema& s, const partition_key& key);

    filter_tracker& get_filter_tracker() { return _filter_tracker; }

    uint64_t filter_get_false_positive() const {
        return _filter_tracker.false_positive;
    }
    uint64_t filter_get_true_positive() const {
        return _filter_tracker.true_positive;
    }
    uint64_t filter_get_recent_false_positive() {
        auto t = _filter_tracker.false_positive - _filter_tracker.last_false_positive;
        _filter_tracker.last_false_positive = _filter_tracker.false_positive;
        return t;
    }
    uint64_t filter_get_recent_true_positive() {
        auto t = _filter_tracker.true_positive - _filter_tracker.last_true_positive;
        _filter_tracker.last_true_positive = _filter_tracker.true_positive;
        return t;
    }

    const statistics& get_statistics() const {
        return _components->statistics;
    }
    const stats_metadata& get_stats_metadata() const {
        auto entry = _components->statistics.contents.find(metadata_type::Stats);
        if (entry == _components->statistics.contents.end()) {
            throw std::runtime_error("Stats metadata not available");
        }
        auto& p = entry->second;
        if (!p) {
            throw std::runtime_error("Statistics is malformed");
        }
        const stats_metadata& s = *static_cast<stats_metadata *>(p.get());
        return s;
    }
    const compaction_metadata& get_compaction_metadata() const {
        auto entry = _components->statistics.contents.find(metadata_type::Compaction);
        if (entry == _components->statistics.contents.end()) {
            throw std::runtime_error("Compaction metadata not available");
        }
        auto& p = entry->second;
        if (!p) {
            throw std::runtime_error("Statistics is malformed");
        }
        const compaction_metadata& s = *static_cast<compaction_metadata *>(p.get());
        return s;
    }
    const serialization_header& get_serialization_header() const {
        return get_mutable_serialization_header(*_components);
    }
    column_translation get_column_translation(
            const schema& s, const serialization_header& h, const sstable_enabled_features& f) {
        return _column_translation.get_for_schema(s, h, f);
    }
    const std::vector<unsigned>& get_shards_for_this_sstable() const {
        return _shards;
    }

    gc_clock::time_point get_max_local_deletion_time() const {
        return gc_clock::time_point(gc_clock::duration(get_stats_metadata().max_local_deletion_time));
    }

    uint32_t get_sstable_level() const {
        return get_stats_metadata().sstable_level;
    }

    // This will change sstable level only in memory.
    void set_sstable_level(uint32_t);

    void generate_new_run_identifier() {
        _run_identifier = run_id::create_random_id();
    }

    double get_compression_ratio() const;

    const sstables::compression& get_compression() const {
        return _components->compression;
    }

    future<> mutate_sstable_level(uint32_t);

    const summary& get_summary() const {
        return _components->summary;
    }

    // Gets ratio of droppable tombstone. A tombstone is considered droppable here
    // for cells and tombstones expired before the time point "GC before", which
    // is the point before which expiring data can be purged.
    double estimate_droppable_tombstone_ratio(const gc_clock::time_point& compaction_time, const tombstone_gc_state& gc_state, const schema_ptr& s) const;

    // get sstable open info from a loaded sstable, which can be used to quickly open a sstable
    // at another shard.
    future<foreign_sstable_open_info> get_open_info() &;
    entry_descriptor get_descriptor(component_type c) const;

    sstables_stats& get_stats() {
        return _stats;
    }

    bool has_correct_min_max_column_names() const noexcept {
        return _version >= sstable_version_types::md;
    }

    // Return true if this sstable possibly stores clustering row(s) specified by ranges.
    bool may_contain_rows(const query::clustering_row_ranges& ranges) const;

    // false => there are no partition tombstones, true => we don't know
    bool may_have_partition_tombstones() const {
        return !has_correct_min_max_column_names()
            || _min_max_position_range.is_all_clustered_rows(*_schema);
    }

    // Return the large_data_stats_entry identified by large_data_type
    // iff _large_data_stats is available and the requested entry is in
    // the map.  Otherwise, return a disengaged optional.
    std::optional<large_data_stats_entry> get_large_data_stat(large_data_type t) const noexcept;

    const sstring& get_origin() const noexcept {
        return _origin;
    }

    // Drops all evictable in-memory caches of on-disk content.
    future<> drop_caches();

    // Returns a read-only file for all existing components of the sstable
    future<std::unordered_map<component_type, file>> readable_file_for_all_components() const;

    // Clones this sstable with a new generation, under the same location as the original one.
    // Implementation is underlying storage specific.
    future<entry_descriptor> clone(generation_type new_generation) const;

    struct lesser_reclaimed_memory {
        // comparator class to be used by the _reclaimed set in sstables manager
        bool operator()(const sstable& sst1, const sstable& sst2) const {
            return sst1.total_memory_reclaimed() < sst2.total_memory_reclaimed();
        }
    };

    // Allow the test cases from sstable_test.cc to test private methods. We use
    // a placeholder to avoid cluttering this class too much. The sstable_test class
    // will then re-export as public every method it needs.
    friend class test;

    friend class mc::writer;
    friend class index_reader;
    friend class promoted_index;
    friend class sstables_manager;
    template <typename DataConsumeRowsContext>
    friend std::unique_ptr<DataConsumeRowsContext>
    data_consume_rows(const schema&, shared_sstable, typename DataConsumeRowsContext::consumer&, disk_read_range, uint64_t);
    template <typename DataConsumeRowsContext>
    friend std::unique_ptr<DataConsumeRowsContext>
    data_consume_single_partition(const schema&, shared_sstable, typename DataConsumeRowsContext::consumer&, disk_read_range);
    template <typename DataConsumeRowsContext>
    friend std::unique_ptr<DataConsumeRowsContext>
    data_consume_rows(const schema&, shared_sstable, typename DataConsumeRowsContext::consumer&);
    friend void lw_shared_ptr_deleter<sstables::sstable>::dispose(sstable* s);
    gc_clock::time_point get_gc_before_for_drop_estimation(const gc_clock::time_point& compaction_time, const tombstone_gc_state& gc_state, const schema_ptr& s) const;
    gc_clock::time_point get_gc_before_for_fully_expire(const gc_clock::time_point& compaction_time, const tombstone_gc_state& gc_state, const schema_ptr& s) const;

    future<uint32_t> read_digest();
    future<checksum> read_checksum();
};

// Validate checksums
//
// Sstables have two kind of checksums: per-chunk checksums and a
// full-checksum (digest) calculated over the entire content of Data.db.
//
// The full-checksum (digest) is stored in Digest.crc (component_type::Digest).
//
// When compression is used, the per-chunk checksum is stored directly inside
// Data.db, after each compressed chunk. These are validated on read, when
// decompressing the respective chunks.
// When no compression is used, the per-chunk checksum is stored separately
// in CRC.db (component_type::CRC). Chunk size is defined and stored in said
// component as well.
//
// In both compressed and uncompressed sstables, checksums are calculated
// on the data that is actually written to disk, so in case of compressed
// data, on the compressed data.
//
// This method validates both the full checksum and the per-chunk checksum
// for the entire Data.db.
//
// Returns true if all checksums are valid.
// Validation errors are logged individually.
future<bool> validate_checksums(shared_sstable sst, reader_permit permit);

struct index_sampling_state {
    static constexpr size_t default_summary_byte_cost = 2000;

    uint64_t next_data_offset_to_write_summary = 0;
    uint64_t partition_count = 0;
    // Enforces ratio of summary to data of 1 to N.
    size_t summary_byte_cost = default_summary_byte_cost;
};

future<> init_metrics();

class file_io_extension {
public:
    virtual ~file_io_extension() {}
    virtual future<file> wrap_file(sstable&, component_type, file, open_flags flags) = 0;
    // optionally return a map of attributes for a given sstable,
    // suitable for "describe".
    // This would preferably be interesting info on what/why the extension did
    // to this table.
    using attr_value_type = std::variant<sstring, std::map<sstring, sstring>>;
    using attr_value_map = std::map<sstring, attr_value_type>;
    virtual attr_value_map get_attributes(const sstable&) const {
        return {};
    }
};

// safely removes the table directory.
// swallows all errors and just reports them to the log.
future<> remove_table_directory_if_has_no_snapshots(fs::path table_dir);

// makes sure the TOC file is temporary by moving existing TOC file or otherwise
// checking the temporary-TOC already exists
// resolves into temporary-TOC file name or empty string if neither TOC nor temp.
// TOC is there
future<sstring> make_toc_temporary(sstring sstable_toc_name, storage::sync_dir sync = storage::sync_dir::yes);

// This snapshot allows the sstable files to be read even if they were removed from the directory
struct sstable_files_snapshot {
    shared_sstable sst;
    std::unordered_map<component_type, file> files;
};

} // namespace sstables

template <> struct fmt::formatter<sstables::sstable_state> : fmt::formatter<string_view> {
    auto format(sstables::sstable_state state, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "{}", state_to_dir(state));
    }
};
