/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */
#include "replica/logstor/segment_manager.hh"
#include "replica/logstor/index.hh"
#include "replica/logstor/logstor.hh"
#include "replica/logstor/types.hh"
#include <linux/if_link.h>
#include <seastar/core/file.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/simple-stream.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/with_scheduling_group.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/when_all.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/core/abort_source.hh>
#include <seastar/core/circular_buffer.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/semaphore.hh>
#include "replica/logstor/write_buffer.hh"
#include "serializer_impl.hh"
#include "idl/logstor.dist.hh"
#include "idl/logstor.dist.impl.hh"
#include "utils/dynamic_bitset.hh"
#include "utils/log_heap.hh"
#include "utils/serialized_action.hh"

namespace replica::logstor {

class segment {
protected:
    log_segment_id _id;
    seastar::file _file;
    uint64_t _file_offset; // Offset within the shared file where this segment starts
    size_t _max_size;

public:
    segment(log_segment_id id, seastar::file file, uint64_t file_offset, size_t max_size);

    virtual ~segment() = default;

    future<> close();

    future<log_record> read(log_location);

    log_segment_id id() const noexcept { return _id; }
    seastar::file& get_file() noexcept { return _file; }

protected:
    uint64_t absolute_offset(uint64_t relative_offset) const noexcept {
        return _file_offset + relative_offset;
    }
};

class writeable_segment : public segment {
    seastar::gate _write_gate;

    uint32_t _current_offset = 0; // next offset for write

public:
    using segment::segment;

    struct allocation {
        log_location location;
        seastar::gate::holder holder;
    };

    future<> stop();

    // allocate and write a serialized sequence of records
    allocation allocate(size_t data_size);
    future<> write(allocation&& , bytes_view data);

    bool can_fit(size_t data_size) const noexcept {
        return _current_offset + data_size <= _max_size;
    }

    size_t bytes_remaining() const noexcept {
        return _max_size - _current_offset;
    }

};

segment::segment(log_segment_id id, seastar::file file, uint64_t file_offset, size_t max_size)
    : _id(id)
    , _file(std::move(file))
    , _file_offset(file_offset)
    , _max_size(max_size) {
}

future<> segment::close() {
    if (_file) {
        co_await _file.close();
    }
}

future<log_record> segment::read(log_location loc) {
    if (loc.offset + loc.size > _max_size) [[unlikely]] {
        throw std::runtime_error(fmt::format("Read beyond end of segment {}: offset {} + size {} > max_size {}",
                                             _id, loc.offset, loc.size, _max_size));
    }

    // Read the serialized record
    return _file.dma_read_exactly<char>(absolute_offset(loc.offset), loc.size).then([] (seastar::temporary_buffer<char> buf) {
        seastar::simple_input_stream in(buf.begin(), buf.size());
        return ser::deserialize(in, std::type_identity<log_record>{});
    });
}

future<> writeable_segment::stop() {
    if (_write_gate.is_closed()) {
        co_return;
    }
    co_await _write_gate.close();
}

writeable_segment::allocation writeable_segment::allocate(size_t data_size) {
    if (!can_fit(data_size)) {
        throw std::runtime_error("Entry too large for remaining segment space");
    }

    auto current_pos = _current_offset;
    _current_offset += data_size;

    return allocation{
        .location = log_location{
            .segment = _id,
            .offset = current_pos,
            .size = static_cast<uint32_t>(data_size)
        },
        .holder = _write_gate.hold()
    };
}

future<> writeable_segment::write(writeable_segment::allocation&& alloc, bytes_view data) {
    const auto alignment = _file.disk_write_dma_alignment();
    const auto total = data.size();
    auto offset = absolute_offset(alloc.location.offset);
    size_t written = 0;

    while (written < total) {
        auto new_written = co_await _file.dma_write(
                offset, data.data() + written, total - written);

        written += new_written;
        if (written == total) {
            break;
        }

        written = align_down(written, alignment);
        offset += written;
    }
}

using seg_ptr = lw_shared_ptr<writeable_segment>;

constexpr log_heap_options segment_descriptor_hist_options(4 * 1024, 3, 128 * 1024);

struct segment_descriptor : public log_heap_hook<segment_descriptor_hist_options> {
    // free_space = segment_size - net_data_size
    // initially set to segment_size
    // when writing records, decrease by total net data size
    // when freeing a record, increase by the record's net data size
    size_t free_space{0};
    size_t record_count{0};

    void reset(size_t segment_size) noexcept {
        free_space = segment_size;
        record_count = 0;
    }

    size_t net_data_size(size_t segment_size) const noexcept {
        return segment_size - free_space;
    }

    void on_write(size_t net_data_size, size_t cnt = 1) noexcept {
        free_space -= net_data_size;
        record_count += cnt;
    }

    void on_write(log_location loc) noexcept {
        on_write(loc.size);
    }

    void on_free(size_t net_data_size, size_t cnt = 1) noexcept {
        free_space += net_data_size;
        record_count -= cnt;
    }

    void on_free(log_location loc) noexcept {
        on_free(loc.size);
    }
};

using segment_descriptor_hist = log_heap<segment_descriptor, segment_descriptor_hist_options>;

class file_manager {
    size_t _segments_per_file;
    size_t _max_files;
    size_t _file_size;
    std::filesystem::path _base_dir;
    seastar::scheduling_group _sched_group;

    size_t _next_file_id{0};

    seastar::gate _async_gate;
    shared_future<> _next_file_formatter{make_ready_future<>()};

public:
    file_manager(segment_manager_config cfg)
        : _segments_per_file(cfg.file_size / cfg.segment_size)
        , _max_files(cfg.disk_size / cfg.file_size)
        , _file_size(cfg.file_size)
        , _base_dir(cfg.base_dir)
        , _sched_group(cfg.compaction_sg)
    {}

    future<> start();
    future<> stop();

    future<seastar::file> get_file_for_write(size_t file_id);
    future<seastar::file> get_file_for_read(size_t file_id);

    future<> format_file_region(seastar::file file, uint64_t offset, size_t size);
    future<> format_file(size_t file_id);

    size_t allocated_file_count() const noexcept { return _next_file_id; }

    size_t segments_per_file() const noexcept { return _segments_per_file; }
    size_t max_files() const noexcept { return _max_files; }

    // the file names are ls_{shard_id}-{file_id}-Data.db
    static const sstring get_file_name_prefix() {
        return fmt::format("ls_{}-", this_shard_id());
    }

    std::filesystem::path get_file_path(size_t file_id) const {
        auto fname = fmt::format("{}{}-Data.db", get_file_name_prefix(), file_id);
        return _base_dir / fname;
    }

    std::optional<size_t> file_name_to_file_id(const std::string& fname) const;
};

future<> file_manager::start() {
    co_await seastar::recursive_touch_directory(_base_dir.string());

    _next_file_formatter = with_gate(_async_gate, [this] {
        return with_scheduling_group(_sched_group, [this] {
            return format_file(_next_file_id);
        });
    });
}

future<> file_manager::stop() {
    if (_async_gate.is_closed()) {
        co_return;
    }
    co_await _async_gate.close();
}

future<> file_manager::format_file(size_t file_id) {
    auto file_path = get_file_path(file_id).string();
    bool file_exists = co_await seastar::file_exists(file_path);
    if (!file_exists) {
        // Create and format a temporary file, then move it to the final location
        auto tmp_path = file_path + ".tmp";
        auto tmp_file = co_await seastar::open_file_dma(tmp_path,
                seastar::open_flags::rw | seastar::open_flags::create | seastar::open_flags::truncate | seastar::open_flags::dsync);
        co_await tmp_file.allocate(0, _file_size);
        co_await format_file_region(tmp_file, 0, _file_size);
        co_await tmp_file.close();

        // move the temp file to the final location
        co_await seastar::rename_file(tmp_path, file_path);
    }
}

future<seastar::file> file_manager::get_file_for_write(size_t file_id) {
    if (file_id == _next_file_id && file_id < _max_files) {
        // allocate file_id and wait for it to be formatted, and start formatting
        // the next file in background
        co_await _next_file_formatter.get_future();

        if (file_id == _next_file_id) {
            _next_file_id++;

            if (_next_file_id < _max_files) {
                _next_file_formatter = with_gate(_async_gate, [this] {
                    return with_scheduling_group(_sched_group, [this] {
                        return format_file(_next_file_id);
                    });
                });
            }
        }
    } else if (file_id >= _max_files) {
        on_internal_error(logstor_logger, "Disk size limit reached, cannot allocate more files");
    } else if (file_id > _next_file_id) {
        on_internal_error(logstor_logger, "files must be allocated in sequential order");
    }

    auto file_path = get_file_path(file_id).string();
    auto file = co_await seastar::open_file_dma(file_path,
            seastar::open_flags::rw | seastar::open_flags::create | seastar::open_flags::dsync);

    co_return file;
}

future<seastar::file> file_manager::get_file_for_read(size_t file_id) {
    auto file = co_await seastar::open_file_dma(
        get_file_path(file_id).string(),
        seastar::open_flags::ro
    );

    co_return std::move(file);
}

future<> file_manager::format_file_region(seastar::file file, uint64_t offset, size_t size) {
    // Allocate aligned buffer for zeroing
    const auto write_alignment = file.disk_write_dma_alignment();
    size_t buf_size = align_up<size_t>(128 * 1024, size_t(write_alignment));
    auto zero_buf = allocate_aligned_buffer<char>(buf_size, write_alignment);
    std::memset(zero_buf.get(), 0, buf_size);

    // Write zeros to entire region
    size_t remaining = size;
    uint64_t current_offset = offset;

    while (remaining > 0) {
        auto write_size = std::min(remaining, buf_size);
        auto written = co_await file.dma_write(current_offset, zero_buf.get(), write_size);

        current_offset += written;
        remaining -= written;
    }
}

std::optional<size_t> file_manager::file_name_to_file_id(const std::string& fname) const {
    std::string prefix = get_file_name_prefix();
    std::string suffix = "-Data.db";
    if (fname.starts_with(prefix) && fname.ends_with(suffix)) {
        // Extract file_id between prefix and suffix
        size_t start = prefix.size();
        size_t end = fname.size() - suffix.size();
        std::string file_id_str = fname.substr(start, end - start);
        try {
            return std::stoull(file_id_str);
        } catch (...) {
            return std::nullopt;
        }
    }
    return std::nullopt;
}

class compaction_manager {
public:

    struct compaction_config {
        bool compaction_enabled;
        size_t max_segments_per_compaction;
        seastar::scheduling_group compaction_sg;
    };

private:
    segment_manager_impl& _sm;
    log_index& _index;
    compaction_config _cfg;

    serialized_action _compaction_action;

    segment_descriptor_hist _segment_hist;

    seastar::gate _async_gate;

    struct stats {
        uint64_t segments_compacted{0};
        uint64_t compaction_segments_freed{0};
        uint64_t compaction_records_skipped{0};
        uint64_t compaction_records_rewritten{0};
    } _stats;

public:
    compaction_manager(segment_manager_impl& sm, log_index& index, compaction_config cfg)
        : _sm(sm)
        , _index(index)
        , _cfg(std::move(cfg))
        , _compaction_action([this] {
            return start_compaction();
        })
    {}

    future<> start();
    future<> stop();

    void enable_auto_compaction() {
        _cfg.compaction_enabled = true;
    }

    future<> disable_auto_compaction() {
        _cfg.compaction_enabled = false;
        return _compaction_action.join();
    }

    future<> trigger_compaction(bool major) {
        if (major) {
            return trigger_major_compaction();
        }
        return _compaction_action.trigger();
    }

    future<> trigger_major_compaction() {
        logstor_logger.info("Starting major compaction");
        auto prev_segmented_freed = _stats.compaction_segments_freed;
        while (true) {
            co_await _compaction_action.trigger();
            if (_stats.compaction_segments_freed == prev_segmented_freed) {
                break;
            }
            prev_segmented_freed = _stats.compaction_segments_freed;
        }
        logstor_logger.info("Major compaction completed");
    }

    const stats& get_stats() const noexcept { return _stats; }

    void add_segment(segment_descriptor& desc) {
        _segment_hist.push(desc);
    }

    // call when space is freed in a segment.
    void update_segment(segment_descriptor& desc) {
        if (desc.is_linked()) {
            _segment_hist.adjust_up(desc);
        }
    }

private:

    bool is_record_alive(const index_key&, log_location);
    bool update_record_location(const index_key&, log_location old_loc, log_location new_loc);

    std::vector<log_segment_id> select_segments_for_compaction();
    future<> start_compaction();
    future<> compact_segments(std::vector<log_segment_id> segments);

    void remove_segment(segment_descriptor& desc) {
        _segment_hist.erase(desc);
    }
};

future<> compaction_manager::start() {
    co_return;
}

future<> compaction_manager::stop() {
    if (_async_gate.is_closed()) {
        co_return;
    }
    co_await _async_gate.close();

    co_await _compaction_action.join();

    while (!_segment_hist.empty()) {
        _segment_hist.pop_one_of_largest();
        co_await coroutine::maybe_yield();
    }
}

class segment_manager_impl {

    struct stats {
        uint64_t segments_in_use{0};
        uint64_t bytes_written{0};
        uint64_t data_bytes_written{0};
        uint64_t bytes_read{0};
        uint64_t bytes_freed{0};
        uint64_t segments_allocated{0};
        uint64_t segments_freed{0};
        uint64_t compaction_bytes_written{0};
        uint64_t compaction_data_bytes_written{0};
    };

    file_manager _file_mgr;
    compaction_manager _compaction_mgr;

    segment_manager_config _cfg;
    size_t _segments_per_file;
    size_t _max_segments;
    size_t _next_new_segment_id{0};

    stats _stats;
    seastar::metrics::metric_groups _metrics;

    static constexpr size_t trigger_compaction_threshold = 10; // percentage of max segments
    static constexpr size_t segment_pool_size = 128;

    seg_ptr _active_segment;
    seastar::queue<seg_ptr> _segment_pool{segment_pool_size};
    std::optional<shared_future<>> _switch_segment_fut;

    seastar::gate _async_gate;
    future<> _reserve_replenisher{make_ready_future<>()};
    seastar::condition_variable _segment_freed_cv;

    std::vector<segment_descriptor> _segment_descs;
    seastar::circular_buffer<log_segment_id> _free_segments;

public:
    static constexpr size_t block_alignment = segment_manager::block_alignment;

    explicit segment_manager_impl(segment_manager_config, log_index&);

    segment_manager_impl(const segment_manager_impl&) = delete;
    segment_manager_impl& operator=(const segment_manager_impl&) = delete;

    future<> start();
    future<> stop();

    future<log_location> write(write_buffer&, bool from_compaction = false);

    future<log_record> read(log_location);

    void free_record(log_location);

    future<> for_each_record(log_segment_id,
                            std::function<future<>(log_location, log_record)>);

    future<> for_each_record(const std::vector<log_segment_id>&,
                            std::function<future<>(log_location, log_record)>);

    void enable_auto_compaction() {
        logstor_logger.info("Enabling automatic compaction");
        _cfg.compaction_enabled = true;
        _compaction_mgr.enable_auto_compaction();
    }

    future<> disable_auto_compaction() {
        logstor_logger.info("Disabling automatic compaction");
        _cfg.compaction_enabled = false;
        co_await _compaction_mgr.disable_auto_compaction();
    }

    future<> trigger_compaction(bool major = false) {
        co_await _compaction_mgr.trigger_compaction(major);
    }

    size_t get_segment_size() const noexcept {
        return _cfg.segment_size;
    }

private:

    struct segment_allocation_guard {
        segment_manager_impl& sm;
        std::optional<log_segment_id> segment_id;

        segment_allocation_guard(segment_manager_impl& sm, log_segment_id seg_id) noexcept
            : sm(sm)
            , segment_id(seg_id)
        {}

        segment_allocation_guard(segment_allocation_guard&& other) noexcept
            : sm(other.sm)
            , segment_id(std::exchange(other.segment_id, std::nullopt))
        {}

        segment_allocation_guard& operator=(segment_allocation_guard&& other) = delete;
        segment_allocation_guard(const segment_allocation_guard&) = delete;
        segment_allocation_guard& operator=(const segment_allocation_guard&) = delete;

        ~segment_allocation_guard() {
            if (segment_id) {
                sm._free_segments.push_back(*segment_id);
                sm._segment_freed_cv.signal();
            }
        }

        void release() noexcept {
            segment_id.reset();
        }
    };

    future<> replenish_reserve();

    future<> request_segment_switch();
    future<> switch_active_segment();
    future<> close_segment(seg_ptr seg);

    future<std::pair<segment_allocation_guard, seg_ptr>> allocate_and_create_new_segment();
    future<segment_allocation_guard> allocate_segment();

    future<> free_segment(log_segment_id);

    segment_descriptor& get_segment_descriptor(log_segment_id segment_id) {
        return _segment_descs[segment_id.value];
    }

    segment_descriptor& get_segment_descriptor(log_location loc) {
        return _segment_descs[loc.segment.value];
    }

    log_segment_id desc_to_segment_id(const segment_descriptor& desc) const noexcept {
        size_t index = &desc - &_segment_descs[0];
        return log_segment_id(index);
    }

    struct segment_location {
        size_t file_id;
        size_t file_offset;
    };

    size_t file_offset_to_segment_index(size_t file_offset) const noexcept {
        return file_offset / _cfg.segment_size;
    }

    size_t segment_index_to_file_offset(size_t segment_index) const noexcept {
        return segment_index * _cfg.segment_size;
    }

    segment_location segment_id_to_file_location(log_segment_id segment_id) const noexcept {
        size_t file_id = segment_id.value / _segments_per_file;
        size_t file_offset = (segment_id.value % _segments_per_file) * _cfg.segment_size;
        return segment_location{file_id, file_offset};
    }

    auto segments_in_file(size_t file_id) const noexcept {
        return std::views::iota(file_id * _segments_per_file, (file_id + 1) * _segments_per_file)
            | std::views::transform([] (size_t i) {
                return log_segment_id(i);
            });
    }

    friend class compaction_manager;
};

segment_manager_impl::segment_manager_impl(segment_manager_config config, log_index& index)
    : _file_mgr(config)
    , _compaction_mgr(*this, index, compaction_manager::compaction_config{
            .compaction_enabled = config.compaction_enabled,
            .max_segments_per_compaction = config.max_segments_per_compaction,
            .compaction_sg = config.compaction_sg
        })
    , _cfg(config)
    , _segments_per_file(config.file_size / config.segment_size)
    , _max_segments((config.disk_size / config.file_size) * _segments_per_file)
    , _segment_descs(_max_segments)
    {

    _free_segments.reserve(_max_segments);

    namespace sm = seastar::metrics;

    _metrics.add_group("logstor_sm", {
        sm::make_gauge("segments_in_use", _stats.segments_in_use,
                       sm::description("Counts number of segments currently in use.")),
        sm::make_gauge("free_segments", [this] { return _free_segments.size(); },
                       sm::description("Counts number of free segments currently available.")),
        sm::make_gauge("segment_pool_size", [this] { return _segment_pool.size(); },
                       sm::description("Counts number of segments in the segment pool.")),
        sm::make_counter("bytes_written", _stats.bytes_written,
                       sm::description("Counts number of bytes written to the disk.")),
        sm::make_counter("data_bytes_written", _stats.data_bytes_written,
                       sm::description("Counts number of data bytes written to the disk.")),
        sm::make_counter("bytes_read", _stats.bytes_read,
                       sm::description("Counts number of bytes read from the disk.")),
        sm::make_counter("bytes_freed", _stats.bytes_freed,
                       sm::description("Counts number of data bytes freed.")),
        sm::make_counter("segments_allocated", _stats.segments_allocated,
                       sm::description("Counts number of segments allocated.")),
        sm::make_counter("segments_freed", _stats.segments_freed,
                       sm::description("Counts number of segments freed.")),
        sm::make_gauge("disk_usage", [this] { return _file_mgr.allocated_file_count() * _cfg.file_size; },
                       sm::description("Total disk usage.")),
        sm::make_counter("compaction_bytes_written", _stats.compaction_bytes_written,
                       sm::description("Counts number of bytes written to the disk by compaction.")),
        sm::make_counter("compaction_data_bytes_written", _stats.compaction_data_bytes_written,
                       sm::description("Counts number of data bytes written to the disk by compaction.")),
        sm::make_counter("segments_compacted", _compaction_mgr.get_stats().segments_compacted,
                       sm::description("Counts number of segments compacted.")),
        sm::make_counter("compaction_segments_freed", _compaction_mgr.get_stats().compaction_segments_freed,
                       sm::description("Counts number of segments freed by compaction.")),
        sm::make_counter("compaction_records_skipped", _compaction_mgr.get_stats().compaction_records_skipped,
                       sm::description("Counts number of records skipped during compaction.")),
        sm::make_counter("compaction_records_rewritten", _compaction_mgr.get_stats().compaction_records_rewritten,
                       sm::description("Counts number of records rewritten during compaction.")),
    });
}

future<> segment_manager_impl::start() {
    co_await _file_mgr.start();

    // Start background replenisher before creating initial segment
    _reserve_replenisher = with_scheduling_group(_cfg.compaction_sg, [this] {
        return replenish_reserve();
    });

    co_await _compaction_mgr.start();

    co_await switch_active_segment();

    logstor_logger.info("Segment manager started with base directory {}", _cfg.base_dir.string());
}

future<> segment_manager_impl::stop() {
    if (_async_gate.is_closed()) {
        co_return;
    }
    logstor_logger.info("Stopping segment manager");

    co_await _async_gate.close();

    if (_active_segment) {
        co_await _active_segment->stop();
    }

    if (_switch_segment_fut) {
        try {
            co_await _switch_segment_fut->get_future();
        } catch (...) {}
    }

    co_await _compaction_mgr.stop();

    _segment_pool.abort(std::make_exception_ptr(abort_requested_exception()));

    _segment_freed_cv.broken();

    co_await std::move(_reserve_replenisher);

    co_await _file_mgr.stop();

    logstor_logger.info("Segment manager stopped");
}

future<log_location> segment_manager_impl::write(write_buffer& wb, bool from_compaction) {
    auto holder = _async_gate.hold();

    wb.finalize(block_alignment);

    bytes_view data(reinterpret_cast<const int8_t*>(wb.data()), wb.offset_in_buffer());

    if (data.size() > _cfg.segment_size) {
        throw std::runtime_error(fmt::format( "Write size {} exceeds segment size {}", data.size(), _cfg.segment_size));
    }

    while (!_active_segment || !_active_segment->can_fit(data.size())) {
        co_await request_segment_switch();
    }

    seg_ptr seg = _active_segment;

    auto alloc = seg->allocate(data.size());
    auto loc = alloc.location;

    wb.write_header();

    co_await seg->write(std::move(alloc), data);

    auto& desc = get_segment_descriptor(loc);
    desc.on_write(wb.get_net_data_size(), wb.get_record_count());

    if (from_compaction) {
        _stats.compaction_bytes_written += data.size();
        _stats.compaction_data_bytes_written += wb.get_net_data_size();
    } else {
        _stats.bytes_written += data.size();
        _stats.data_bytes_written += wb.get_net_data_size();
    }

    // complete all buffered writes with their individual locations
    wb.complete_writes(loc);

    co_return loc;
}

void segment_manager_impl::free_record(log_location location) {
    auto& desc = get_segment_descriptor(location);
    desc.on_free(location);
    _compaction_mgr.update_segment(desc);
    _stats.bytes_freed += location.size;
}

future<log_record> segment_manager_impl::read(log_location location) {
    auto holder = _async_gate.hold();
    auto [file_id, file_offset] = segment_id_to_file_location(location.segment);
    auto file = co_await _file_mgr.get_file_for_read(file_id);
    segment seg(location.segment, file, file_offset, _cfg.segment_size);
    auto record = co_await seg.read(location);
    _stats.bytes_read += location.size;
    co_return std::move(record);
}

future<> segment_manager_impl::request_segment_switch() {
    if (!_switch_segment_fut) {
        auto f = switch_active_segment();
        if (f.available()) {
            f.get();
            co_return;
        }
        _switch_segment_fut.emplace(f.discard_result().finally([this] {
            _switch_segment_fut.reset();
        }));
    }
    co_await _switch_segment_fut->get_future();
}

future<> segment_manager_impl::switch_active_segment() {
    auto holder = _async_gate.hold();

    auto new_seg = co_await _segment_pool.pop_eventually();

    auto old_seg = std::exchange(_active_segment, std::move(new_seg));
    _stats.segments_in_use++;

    if (old_seg) {
        // close old segment in background
        (void)with_gate(_async_gate, [this, old_seg] mutable {
            return close_segment(std::move(old_seg));
        });
    }

    logstor_logger.trace("Switched active segment to {}", _active_segment->id());
}

future<> segment_manager_impl::close_segment(seg_ptr seg) {
    co_await seg->stop();

    auto& desc = get_segment_descriptor(seg->id());
    _compaction_mgr.add_segment(desc);
}

future<> segment_manager_impl::replenish_reserve() {
    while (true) {
        bool retry = false;
        try {
            auto [seg_guard, seg] = co_await allocate_and_create_new_segment();
            co_await _segment_pool.push_eventually(std::move(seg));
            seg_guard.release();
        } catch (abort_requested_exception&) {
            logstor_logger.debug("Reserve replenisher stopping due to abort");
            break;
        } catch (...) {
            if (_async_gate.is_closed()) {
                logstor_logger.debug("Reserve replenisher stopping due to gate close");
                break;
            }
            retry = true;
            logstor_logger.warn("Exception in reserve replenisher: {}, will retry", std::current_exception());
        }

        if (retry) {
            co_await seastar::sleep(std::chrono::seconds(1));
        }
    }

    logstor_logger.debug("Reserve replenisher stopped");
}

future<std::pair<segment_manager_impl::segment_allocation_guard, seg_ptr>>
segment_manager_impl::allocate_and_create_new_segment() {
    auto seg_guard = co_await allocate_segment();

    auto seg_id = seg_guard.segment_id.value();
    auto seg_loc = segment_id_to_file_location(seg_id);
    auto file = co_await _file_mgr.get_file_for_write(seg_loc.file_id);
    auto seg = make_lw_shared<writeable_segment>(seg_id, std::move(file), seg_loc.file_offset, _cfg.segment_size);
    get_segment_descriptor(seg_id).reset(_cfg.segment_size);
    _stats.segments_allocated++;

    co_return std::make_pair(std::move(seg_guard), std::move(seg));
}

future<segment_manager_impl::segment_allocation_guard>
segment_manager_impl::allocate_segment() {
    while (true) {
        // first, allocate all new segments sequentially
        if (_next_new_segment_id < _max_segments) {
            auto seg_id = log_segment_id(_next_new_segment_id++);
            co_return segment_allocation_guard(*this, seg_id);
        }

        if (_free_segments.size() < _max_segments * trigger_compaction_threshold / 100) {
            (void)trigger_compaction().handle_exception([] (std::exception_ptr e) {
                logstor_logger.warn("Failed to trigger compaction: {}", e);
            });
        }

        // reuse freed segments
        if (!_free_segments.empty()) {
            auto seg_id = _free_segments.front();
            _free_segments.pop_front();
            co_return segment_allocation_guard(*this, seg_id);
        }

        // no free segments - wait for a segment to be freed.
        // compaction might fail to free segments now, but can succeed later as data is freed.
        // for now let's solve it by waiting with a timeout to re-trigger compaction periodically.
        co_await _segment_freed_cv.wait(std::chrono::seconds(5));
    }
}

future<> segment_manager_impl::free_segment(log_segment_id segment_id) {
    // TODO don't free segment while someone reads from it?

    _free_segments.push_back(segment_id);
    _segment_freed_cv.signal();

    _stats.segments_freed++;
    _stats.segments_in_use--;

    co_return;
}

future<> segment_manager_impl::for_each_record(log_segment_id segment_id,
                                std::function<future<>(log_location, log_record)> callback) {
    auto holder = _async_gate.hold();

    auto [file_id, file_offset] = segment_id_to_file_location(segment_id);
    auto file = co_await _file_mgr.get_file_for_read(file_id);
    auto fin = make_file_input_stream(std::move(file), file_offset, _cfg.segment_size,
        file_input_stream_options {
            .buffer_size = std::min<size_t>(_cfg.segment_size, 128 * 1024),
            .read_ahead = 1,
    });
    size_t current_position = 0;

    logstor_logger.trace("Reading records from segment {} at file {} offset {}",
                        segment_id, file_id, file_offset);

    while (current_position < _cfg.segment_size) {
        // Align to block boundary
        auto skip_bytes = align_up(current_position, block_alignment) - current_position;
        if (skip_bytes > 0) {
            co_await fin.skip(skip_bytes);
            current_position += skip_bytes;
        }

        if (current_position >= _cfg.segment_size) {
            break;
        }

        // read buffer header
        auto buffer_header_buf = co_await fin.read_exactly(write_buffer::buffer_header_size);
        current_position += write_buffer::buffer_header_size;
        if (buffer_header_buf.size() < write_buffer::buffer_header_size) {
            break;
        }
        seastar::simple_memory_input_stream buffer_header_stream(buffer_header_buf.get(), buffer_header_buf.size());
        auto bh = ser::deserialize(buffer_header_stream, std::type_identity<write_buffer::buffer_header>{});

        // if buffer header is not valid, skip to next block
        if (bh.magic != write_buffer::buffer_header_magic) {
            continue;
        }

        const auto buffer_data_end_position = current_position + bh.data_size;
        while (current_position < buffer_data_end_position) {
            // Read record header
            auto size_buf = co_await fin.read_exactly(write_buffer::record_header_size);
            current_position += write_buffer::record_header_size;
            if (size_buf.size() < write_buffer::record_header_size) {
                break;
            }
            seastar::simple_memory_input_stream size_stream(size_buf.get(), size_buf.size());
            auto rh = ser::deserialize(size_stream, std::type_identity<write_buffer::record_header>{});
            if (rh.data_size == 0) {
                // End of records in this block
                break;
            }

            logstor_logger.trace("Found record of size {} bytes in segment {}",
                                rh.data_size, segment_id);

            auto record_offset = current_position;
            auto record_buf = co_await fin.read_exactly(rh.data_size);
            current_position += rh.data_size;
            if (record_buf.size() < rh.data_size) {
                break;
            }

            seastar::simple_memory_input_stream record_stream(record_buf.get(), record_buf.size());
            auto record = ser::deserialize(record_stream, std::type_identity<log_record>{});

            log_location loc {
                .segment = segment_id,
                .offset = record_offset,
                .size = rh.data_size
            };

            co_await callback(loc, std::move(record));

            // align up to next record
            auto padding = align_up(current_position, write_buffer::record_alignment) - current_position;
            if (padding > 0) {
                co_await fin.skip(padding);
                current_position += padding;
            }
        }

        if (current_position < buffer_data_end_position) {
            // skip remaining buffer data
            auto bytes_to_skip = buffer_data_end_position - current_position;
            co_await fin.skip(bytes_to_skip);
            current_position += bytes_to_skip;
        }
    }

    co_await fin.close();
}

future<> segment_manager_impl::for_each_record(const std::vector<log_segment_id>& segments,
                                        std::function<future<>(log_location, log_record)> callback) {
    for (auto segment_id : segments) {
        co_await for_each_record(segment_id, callback);
    }
}

bool compaction_manager::is_record_alive(const index_key& key, log_location loc) {
    return _index.get(key)
        .transform([loc] (const index_entry& e) { return e.location == loc; })
        .value_or(false);
}

bool compaction_manager::update_record_location(const index_key& key, log_location old_loc, log_location new_loc) {
    return _index.update_record_location(key, old_loc, new_loc);
}

std::vector<log_segment_id> compaction_manager::select_segments_for_compaction() {
    size_t accum_net_data_size = 0;
    size_t accum_record_count = 0;
    ssize_t max_gain = 0;
    size_t best_count = 0;
    std::vector<log_segment_id> candidates;
    const auto segment_size = _sm.get_segment_size();

    for (auto& desc : _segment_hist) {
        if (candidates.size() >= _cfg.max_segments_per_compaction) {
            break;
        }

        auto seg_id = _sm.desc_to_segment_id(desc);
        candidates.push_back(seg_id);

        accum_net_data_size += desc.net_data_size(segment_size);
        accum_record_count += desc.record_count;

        auto required_segments = write_buffer::estimate_required_segments(
            accum_net_data_size, accum_record_count, segment_size);

        logstor_logger.trace("Evaluating compaction candidate {} with net data size {} accumulated {} required segments {}",
                           seg_id, desc.net_data_size(segment_size), accum_net_data_size, required_segments);

        auto gain = ssize_t(candidates.size()) - ssize_t(required_segments);
        if (gain > max_gain) {
            max_gain = gain;
            best_count = candidates.size();
        }
    }

    logstor_logger.debug("Selected {} segments for compaction with net data {} for gain of {} segments",
                       best_count, accum_net_data_size, max_gain);

    return candidates
            | std::views::take(best_count)
            | std::ranges::to<std::vector<log_segment_id>>();
}

future<> compaction_manager::start_compaction() {
    if (!_cfg.compaction_enabled) {
        logstor_logger.debug("Compaction is disabled, skipping");
        co_return;
    }

    auto candidates = select_segments_for_compaction();
    if (candidates.size() == 0) {
        logstor_logger.debug("No segments for compaction");
        co_return;
    }

    auto holder = _async_gate.hold();

    logstor_logger.debug("Starting compaction of {} segments", candidates.size());
    co_await with_scheduling_group(_cfg.compaction_sg, [this, candidates = std::move(candidates)] mutable {
        return compact_segments(std::move(candidates));
    });
}

future<> compaction_manager::compact_segments(std::vector<log_segment_id> segments) {
    struct compaction_buffer {
        segment_manager_impl& sm;
        write_buffer buf;
        std::vector<future<>> pending_updates;
        size_t flush_count{0};

        explicit compaction_buffer(segment_manager_impl& sm, size_t buffer_size)
            : sm(sm), buf(buffer_size) {}

        future<> flush() {
            if (buf.has_data()) {
                flush_count++;
                auto base_location = co_await sm.write(buf, true);
                co_await when_all_succeed(pending_updates.begin(), pending_updates.end());

                logstor_logger.trace("Compaction buffer flushed to {} with {} bytes", base_location, buf.get_net_data_size());

                buf.reset();
                pending_updates.clear();
            }
        }
    };

    compaction_buffer cb(_sm, _sm.get_segment_size());

    size_t records_rewritten = 0;
    size_t records_skipped = 0;

    co_await _sm.for_each_record(segments,
            [this, &records_rewritten, &records_skipped, &cb]
            (log_location read_location, log_record record) -> future<> {

        if (!is_record_alive(record.key, read_location)) {
            records_skipped++;
            _stats.compaction_records_skipped++;
            co_return;
        }

        auto key = record.key;
        log_record_writer writer(std::move(record));

        if (!cb.buf.can_fit(writer)) {
            co_await cb.flush();
        }

        // write the record and then update the index with the new location
        auto write_and_update_index = cb.buf.write(std::move(writer)).then(
                [this, key = std::move(key), read_location, &records_rewritten, &records_skipped]
                (log_location new_location) {

            if (update_record_location(key, read_location, new_location)) {
                records_rewritten++;
            } else {
                // another write updated this key
                _sm.free_record(new_location);
                records_skipped++;
            }
        });

        cb.pending_updates.push_back(std::move(write_and_update_index));
    });

    co_await cb.flush();

    logstor_logger.debug("Compaction complete: {} records rewritten, {} skipped from {} segments, flushed {} times",
                       records_rewritten, records_skipped, segments.size(), cb.flush_count);

    // Free the compacted segments
    for (auto seg_id : segments) {
        remove_segment(_sm.get_segment_descriptor(seg_id));
        co_await _sm.free_segment(seg_id);
    }

    size_t new_segments = segments.size() > cb.flush_count ? segments.size() - cb.flush_count : 0;
    _stats.segments_compacted += segments.size();
    _stats.compaction_segments_freed += new_segments;
    _stats.compaction_records_rewritten += records_rewritten;
    _stats.compaction_records_skipped += records_skipped;
}

// segment_manager wrapper

segment_manager::segment_manager(segment_manager_config config, log_index& index)
    : _impl(std::make_unique<segment_manager_impl>(std::move(config), index))
{ }

segment_manager::~segment_manager() = default;

segment_manager_impl& segment_manager::get_impl() noexcept {
    return *_impl;
}

const segment_manager_impl& segment_manager::get_impl() const noexcept {
    return *_impl;
}

future<> segment_manager::start() {
    return _impl->start();
}

future<> segment_manager::stop() {
    return _impl->stop();
}

future<log_location> segment_manager::write(write_buffer& wb) {
    return _impl->write(wb);
}

future<log_record> segment_manager::read(log_location location) {
    return _impl->read(location);
}

void segment_manager::free_record(log_location location) {
    _impl->free_record(location);
}

future<> segment_manager::for_each_record(const std::vector<log_segment_id>& segments,
                                        std::function<future<>(log_location, log_record)> callback) {
    return _impl->for_each_record(std::move(segments), std::move(callback));
}

void segment_manager::enable_auto_compaction() {
    _impl->enable_auto_compaction();
}
future<> segment_manager::disable_auto_compaction() {
    return _impl->disable_auto_compaction();
}
future<> segment_manager::trigger_compaction(bool major) {
    return _impl->trigger_compaction(major);
}

size_t segment_manager::get_segment_size() const noexcept {
    return _impl->get_segment_size();
}

}

template<>
size_t hist_key<replica::logstor::segment_descriptor>(const replica::logstor::segment_descriptor& desc) {
    return desc.free_space;
}
