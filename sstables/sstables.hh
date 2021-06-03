/*
 * Copyright (C) 2015-present ScyllaDB
 *
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "version.hh"
#include "shared_sstable.hh"
#include "shared_index_lists.hh"
#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/enum.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/distributed.hh>
#include <unordered_set>
#include <unordered_map>
#include <variant>
#include "types.hh"
#include "clustering_key_filter.hh"
#include <seastar/core/enum.hh>
#include "compress.hh"
#include "dht/i_partitioner.hh"
#include "schema_fwd.hh"
#include "utils/i_filter.hh"
#include <seastar/core/stream.hh>
#include "metadata_collector.hh"
#include "encoding_stats.hh"
#include "filter.hh"
#include "exceptions.hh"
#include "mutation_reader.hh"
#include "query-request.hh"
#include "compound_compat.hh"
#include "utils/disk-error-handler.hh"
#include "sstables/progress_monitor.hh"
#include "db/commitlog/replay_position.hh"
#include "component_type.hh"
#include "sstable_version.hh"
#include "db/large_data_handler.hh"
#include "column_translation.hh"
#include "stats.hh"
#include "utils/observable.hh"
#include "sstables/shareable_components.hh"
#include "sstables/open_info.hh"
#include "query-request.hh"
#include "mutation_fragment_stream_validator.hh"

#include <seastar/util/optimized_optional.hh>
#include <boost/intrusive/list.hpp>

class sstable_assertions;
class flat_mutation_reader;

namespace sstables {

namespace mc {
class writer;
}

namespace fs = std::filesystem;

extern logging::logger sstlog;
class key;
class sstable_writer;
class sstable_writer_k_l;
class sstables_manager;

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
class sstables_manager;

extern bool use_binary_search_in_promoted_index;

extern size_t summary_byte_cost(double summary_ratio);

struct sstable_writer_config {
    size_t promoted_index_block_size;
    uint64_t max_sstable_size = std::numeric_limits<uint64_t>::max();
    bool backup = false;
    bool leave_unsealed = false;
    mutation_fragment_stream_validation_level validation_level;
    std::optional<db::replay_position> replay_position;
    std::optional<int> sstable_level;
    write_monitor* monitor = &default_write_monitor();
    utils::UUID run_identifier = utils::make_random_uuid();
    size_t summary_byte_cost;
    sstring origin;

private:
    explicit sstable_writer_config() {}
    friend class sstables_manager;
};

class sstable_tracker;

class sstable : public enable_lw_shared_from_this<sstable> {
    friend ::sstable_assertions;
    friend sstable_tracker;
public:
    using version_types = sstable_version_types;
    using format_types = sstable_format_types;
    using tracker_link_type = bi::list_member_hook<bi::link_mode<bi::auto_unlink>>;
    using manager_link_type = bi::list_member_hook<bi::link_mode<bi::auto_unlink>>;
public:
    sstable(schema_ptr schema,
            sstring dir,
            int64_t generation,
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

    static component_type component_from_sstring(version_types version, sstring& s);
    static version_types version_from_sstring(sstring& s);
    static format_types format_from_sstring(sstring& s);
    static sstring component_basename(const sstring& ks, const sstring& cf, version_types version, int64_t generation,
                                      format_types format, component_type component);
    static sstring component_basename(const sstring& ks, const sstring& cf, version_types version, int64_t generation,
                                      format_types format, sstring component);
    static sstring filename(const sstring& dir, const sstring& ks, const sstring& cf, version_types version, int64_t generation,
                            format_types format, component_type component);
    static sstring filename(const sstring& dir, const sstring& ks, const sstring& cf, version_types version, int64_t generation,
                            format_types format, sstring component);
    // WARNING: it should only be called to remove components of a sstable with
    // a temporary TOC file.
    static future<> remove_sstable_with_temp_toc(sstring ks, sstring cf, sstring dir, int64_t generation,
                                                 version_types v, format_types f);

    // load sstable using components shared by a shard
    future<> load(foreign_sstable_open_info info) noexcept;
    // load all components from disk
    // this variant will be useful for testing purposes and also when loading
    // a new sstable from scratch for sharing its components.
    future<> load(const io_priority_class& pc = default_priority_class()) noexcept;
    future<> open_data() noexcept;
    future<> update_info_for_opened_data();

    future<> set_generation(int64_t generation);
    future<> move_to_new_dir(sstring new_dir, int64_t generation, bool do_sync_dirs = true);

    int64_t generation() const {
        return _generation;
    }

    // Returns a mutation_reader for given range of partitions
    flat_mutation_reader make_reader(
            schema_ptr schema,
            reader_permit permit,
            const dht::partition_range& range,
            const query::partition_slice& slice,
            const io_priority_class& pc = default_priority_class(),
            tracing::trace_state_ptr trace_state = {},
            streamed_mutation::forwarding fwd = streamed_mutation::forwarding::no,
            mutation_reader::forwarding fwd_mr = mutation_reader::forwarding::yes,
            read_monitor& monitor = default_read_monitor());

    // Returns mutation_source containing all writes contained in this sstable.
    // The mutation_source shares ownership of this sstable.
    mutation_source as_mutation_source();

    future<> write_components(flat_mutation_reader mr,
            uint64_t estimated_partitions,
            schema_ptr schema,
            const sstable_writer_config&,
            encoding_stats stats,
            const io_priority_class& pc = default_priority_class());

    sstable_writer get_writer(const schema& s,
        uint64_t estimated_partitions,
        const sstable_writer_config&,
        encoding_stats enc_stats,
        const io_priority_class& pc = default_priority_class(),
        shard_id shard = this_shard_id());

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

    const std::set<int>& compaction_ancestors() const {
        return _compaction_ancestors;
    }

    void add_ancestor(int64_t generation) {
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
    uint64_t filter_size() const {
        return _filter_file_size;
    }

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

    sstring filename(const sstring& dir, component_type f) const {
        return filename(dir, _schema->ks_name(), _schema->cf_name(), _version, _generation, _format, f);
    }

    sstring filename(component_type f) const {
        return filename(get_dir(), f);
    }

    sstring temp_filename(component_type f) const {
        return filename(get_temp_dir(), f);
    }

    sstring get_filename() const {
        return filename(component_type::Data);
    }

    sstring toc_filename() const {
        return filename(component_type::TOC);
    }

    static sstring sst_dir_basename(unsigned long gen) {
        return fmt::format("{:016d}.sstable", gen);
    }

    static sstring temp_sst_dir(const sstring& dir, unsigned long gen) {
        return dir + "/" + sst_dir_basename(gen);
    }

    static bool is_temp_dir(const fs::path& dirpath)
    {
        return dirpath.extension().string() == ".sstable";
    }

    static sstring pending_delete_dir_basename() {
        return "pending_delete";
    }

    static bool is_pending_delete_dir(const fs::path& dirpath)
    {
        return dirpath.filename().string() == pending_delete_dir_basename().c_str();
    }

    const sstring& get_dir() const {
        return _dir;
    }

    const sstring get_temp_dir() const {
        return temp_sst_dir(_dir, _generation);
    }

    bool requires_view_building() const;

    std::vector<std::pair<component_type, sstring>> all_components() const;

    future<> create_links(const sstring& dir, int64_t generation) const;

    future<> create_links(const sstring& dir) const {
        return create_links(dir, _generation);
    }

    // Delete the sstable by unlinking all sstable files
    // Ignores all errors.
    future<> unlink() noexcept;

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

    template<typename Func, typename... Args>
    requires std::is_nothrow_move_constructible_v<Func>
    auto sstable_write_io_check(Func&& func, Args&&... args) const noexcept {
        return do_io_check(_write_error_handler, std::forward<Func>(func), std::forward<Args>(args)...);
    }

    // required since touch_directory has an optional parameter
    auto sstable_touch_directory_io_check(sstring name) const noexcept {
        return do_io_check(_write_error_handler, [name = std::move(name)] () mutable {
            return touch_directory(std::move(name));
        });
    }
    future<> close_files();

    /* Returns a lower-bound for the set of `position_in_partition`s appearing in the sstable across all partitions.
     *
     * Might be `before_all_keys` if, for example, the sstable comes from outside Scylla and lacks sufficient metadata,
     * or the sstable's schema does not have clustering columns.
     *
     * But if the schema has clustering columns and the sstable is sufficiently ``modern'',
     * the returned value should be equal to the smallest clustering key occuring in the sstable (across all partitions).
     *
     * The lower bound is inclusive: there might be a clustering row with position equal to min_position.
     */
    const position_in_partition& min_position() const {
        return _position_range.start();
    }

    /* Similar to min_position, but returns an upper-bound.
     * However, the upper-bound is exclusive: all positions are smaller than max_position.
     *
     * If certain conditions are satisfied (the same as for `min_position`, see above),
     * the returned value should be equal to after_key(ck), where ck is the greatest clustering key
     * occuring in the sstable (across all partitions).
     */
    const position_in_partition& max_position() const {
        return _position_range.end();
    }

private:
    size_t sstable_buffer_size;

    static std::unordered_map<version_types, sstring, enum_hash<version_types>> _version_string;
    static std::unordered_map<format_types, sstring, enum_hash<format_types>> _format_string;

    std::unordered_set<component_type, enum_hash<component_type>> _recognized_components;
    std::vector<sstring> _unrecognized_components;

    foreign_ptr<lw_shared_ptr<shareable_components>> _components = make_foreign(make_lw_shared<shareable_components>());
    column_translation _column_translation;
    std::optional<open_flags> _open_mode;
    // _compaction_ancestors track which sstable generations were used to generate this sstable.
    // it is then used to generate the ancestors metadata in the statistics or scylla components.
    std::set<int> _compaction_ancestors;
    file _index_file;
    file _data_file;
    uint64_t _data_file_size;
    uint64_t _index_file_size;
    uint64_t _filter_file_size = 0;
    uint64_t _bytes_on_disk = 0;
    db_clock::time_point _data_file_write_time;
    position_range _position_range = position_range::all_clustered_rows();
    std::vector<unsigned> _shards;
    std::optional<dht::decorated_key> _first;
    std::optional<dht::decorated_key> _last;
    utils::UUID _run_identifier;
    utils::observable<sstable&> _on_closed;

    lw_shared_ptr<file_input_stream_history> _single_partition_history = make_lw_shared<file_input_stream_history>();
    lw_shared_ptr<file_input_stream_history> _partition_range_history = make_lw_shared<file_input_stream_history>();
    lw_shared_ptr<file_input_stream_history> _index_history = make_lw_shared<file_input_stream_history>();

    schema_ptr _schema;
    sstring _dir;
    std::optional<sstring> _temp_dir; // Valid while the sstable is being created, until sealed
    unsigned long _generation = 0;
    version_types _version;
    format_types _format;

    filter_tracker _filter_tracker;
    shared_index_lists _index_lists;

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
    tracker_link_type _tracker_link;
    manager_link_type _manager_link;

    // The _large_data_stats map stores e.g. largest partitions, rows, cells sizes,
    // and max number of rows in a partition.
    //
    // It can be disengaged normally when loading legacy sstables that do not have this
    // information in their scylla metadata.
    std::optional<scylla_metadata::large_data_stats> _large_data_stats;
    sstring _origin;
public:
    const bool has_component(component_type f) const;
    sstables_manager& manager() { return _manager; }
    const sstables_manager& manager() const { return _manager; }
private:
    void unused(); // Called when reference count drops to zero
    future<file> open_file(component_type, open_flags, file_open_options = {}) noexcept;

    template <component_type Type, typename T>
    future<> read_simple(T& comp, const io_priority_class& pc);

    template <component_type Type, typename T>
    void write_simple(const T& comp, const io_priority_class& pc);
    void do_write_simple(component_type type, const io_priority_class& pc,
            noncopyable_function<void (version_types version, file_writer& writer)> write_component);

    void write_crc(const checksum& c);
    void write_digest(uint32_t full_checksum);

    future<> rename_new_sstable_component_file(sstring from_file, sstring to_file);
    future<file> new_sstable_component_file(const io_error_handler& error_handler, component_type f, open_flags flags, file_open_options options = {}) noexcept;

    future<file_writer> make_component_file_writer(component_type c, file_output_stream_options options,
            open_flags oflags = open_flags::wo | open_flags::create | open_flags::exclusive) noexcept;

    future<> touch_temp_dir();
    future<> remove_temp_dir();

    void generate_toc(compressor_ptr c, double filter_fp_chance);
    void write_toc(const io_priority_class& pc);
    future<> seal_sstable();

    future<> read_compression(const io_priority_class& pc);
    void write_compression(const io_priority_class& pc);

    future<> read_scylla_metadata(const io_priority_class& pc) noexcept;
    void write_scylla_metadata(const io_priority_class& pc, shard_id shard, sstable_enabled_features features, run_identifier identifier,
            std::optional<scylla_metadata::large_data_stats> ld_stats, sstring origin);

    future<> read_filter(const io_priority_class& pc);

    void write_filter(const io_priority_class& pc);

    future<> read_summary(const io_priority_class& pc) noexcept;

    void write_summary(const io_priority_class& pc) {
        write_simple<component_type::Summary>(_components->summary, pc);
    }

    // To be called when we try to load an SSTable that lacks a Summary. Could
    // happen if old tools are being used.
    future<> generate_summary(const io_priority_class& pc);

    future<> read_statistics(const io_priority_class& pc);
    void write_statistics(const io_priority_class& pc);
    // Rewrite statistics component by creating a temporary Statistics and
    // renaming it into place of existing one.
    void rewrite_statistics(const io_priority_class& pc);
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
    void set_position_range();

    future<> create_data() noexcept;

    // Return an input_stream which reads exactly the specified byte range
    // from the data file (after uncompression, if the file is compressed).
    // Unlike data_read() below, this method does not read the entire byte
    // range into memory all at once. Rather, this method allows reading the
    // data incrementally as a stream. Knowing in advance the exact amount
    // of bytes to be read using this stream, we can make better choices
    // about the buffer size to read, and where exactly to stop reading
    // (even when a large buffer size is used).
    input_stream<char> data_stream(uint64_t pos, size_t len, const io_priority_class& pc,
            reader_permit permit, tracing::trace_state_ptr trace_state, lw_shared_ptr<file_input_stream_history> history);

    // Read exactly the specific byte range from the data file (after
    // uncompression, if the file is compressed). This can be used to read
    // a specific row from the data file (its position and length can be
    // determined using the index file).
    // This function is intended (and optimized for) random access, not
    // for iteration through all the rows.
    future<temporary_buffer<char>> data_read(uint64_t pos, size_t len, const io_priority_class& pc, reader_permit permit);

    future<summary_entry&> read_summary_entry(size_t i);

    // FIXME: pending on Bloom filter implementation
    bool filter_has_key(const schema& s, const dht::decorated_key& dk) { return filter_has_key(key::from_partition_key(s, dk._key)); }

    std::optional<std::pair<uint64_t, uint64_t>> get_sample_indexes_for_range(const dht::token_range& range);
    std::optional<std::pair<uint64_t, uint64_t>> get_index_pages_for_range(const dht::token_range& range);

    std::vector<unsigned> compute_shards_for_this_sstable() const;
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

    future<> check_create_links_replay(const sstring& dst_dir, int64_t dst_gen, const std::vector<std::pair<sstables::component_type, sstring>>& comps) const;
    future<> create_links_common(const sstring& dst_dir, int64_t dst_gen, bool mark_for_removal) const;
    future<> create_links_and_mark_for_removal(const sstring& dst_dir, int64_t dst_gen) const;
public:
    future<> read_toc() noexcept;

    schema_ptr get_schema() const {
        return _schema;
    }

    bool has_scylla_component() const {
        return has_component(component_type::Scylla);
    }

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

    utils::UUID run_identifier() const {
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

    double get_compression_ratio() const;

    const sstables::compression& get_compression() const {
        return _components->compression;
    }

    future<> mutate_sstable_level(uint32_t);

    const summary& get_summary() const {
        return _components->summary;
    }

    // Gets ratio of droppable tombstone. A tombstone is considered droppable here
    // for cells expired before gc_before and regular tombstones older than gc_before.
    double estimate_droppable_tombstone_ratio(gc_clock::time_point gc_before) const;

    // get sstable open info from a loaded sstable, which can be used to quickly open a sstable
    // at another shard.
    future<foreign_sstable_open_info> get_open_info() &;

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
            || _position_range.is_all_clustered_rows(*_schema);
    }

    // Return the large_data_stats_entry identified by large_data_type
    // iff _large_data_stats is available and the requested entry is in
    // the map.  Otherwise, return a disengaged optional.
    std::optional<large_data_stats_entry> get_large_data_stat(large_data_type t) const noexcept;

    const sstring& get_origin() const noexcept {
        return _origin;
    }

    // Allow the test cases from sstable_test.cc to test private methods. We use
    // a placeholder to avoid cluttering this class too much. The sstable_test class
    // will then re-export as public every method it needs.
    friend class test;

    friend class components_writer;
    friend class sstable_writer;
    friend class sstable_writer_k_l;
    friend class mc::writer;
    friend class index_reader;
    friend class sstable_writer;
    friend class compaction;
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
};

// When we compact sstables, we have to atomically instantiate the new
// sstable and delete the old ones.  Otherwise, if we compact A+B into C,
// and if A contained some data that was tombstoned by B, and if B was
// deleted but A survived, then data from A will be resurrected.
//
// There are two violators of the requirement to atomically delete
// sstables: first sstable instantiation and deletion on disk is atomic
// only wrt. itself, not other sstables, and second when an sstable is
// shared among shard, so actual on-disk deletion of an sstable is deferred
// until all shards agree it can be deleted.
//
// This function only solves the second problem for now.
future<> delete_atomically(std::vector<shared_sstable> ssts);
future<> replay_pending_delete_log(sstring log_file);

struct index_sampling_state {
    static constexpr size_t default_summary_byte_cost = 2000;

    uint64_t next_data_offset_to_write_summary = 0;
    uint64_t partition_count = 0;
    // Enforces ratio of summary to data of 1 to N.
    size_t summary_byte_cost = default_summary_byte_cost;
};

class sstable_writer {
public:
    class writer_impl;
private:
    std::unique_ptr<writer_impl> _impl;
public:
    sstable_writer(sstable& sst, const schema& s, uint64_t estimated_partitions,
            const sstable_writer_config&, encoding_stats enc_stats,
            const io_priority_class& pc, shard_id shard = this_shard_id());

    sstable_writer(sstable_writer&& o);
    sstable_writer& operator=(sstable_writer&& o);

    ~sstable_writer();

    void consume_new_partition(const dht::decorated_key& dk);
    void consume(tombstone t);
    stop_iteration consume(static_row&& sr);
    stop_iteration consume(clustering_row&& cr);
    stop_iteration consume(range_tombstone&& rt);
    stop_iteration consume_end_of_partition();
    void consume_end_of_stream();

    metadata_collector& get_metadata_collector();
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

}
