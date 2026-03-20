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
#include "replica/logstor/compaction.hh"
#include <absl/container/flat_hash_map.h>
#include <chrono>
#include <linux/if_link.h>
#include <seastar/core/file.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/simple-stream.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/with_scheduling_group.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/when_all.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/core/abort_source.hh>
#include <seastar/core/circular_buffer.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/condition-variable.hh>
#include "replica/logstor/write_buffer.hh"
#include "serializer_impl.hh"
#include "idl/logstor.dist.hh"
#include "idl/logstor.dist.impl.hh"
#include "utils/dynamic_bitset.hh"
#include "utils/serialized_action.hh"
#include "utils/lister.hh"
#include "replica/database.hh"

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
    segment_ref _seg_ref;

    uint32_t _current_offset = 0; // next offset for write

public:
    using segment::segment;

    void start(segment_ref seg_ref);

    future<> stop();

    // allocate and write a serialized sequence of records
    log_location allocate(size_t data_size);
    future<> write(log_location , bytes_view data);

    bool can_fit(size_t data_size) const noexcept {
        return _current_offset + data_size <= _max_size;
    }

    size_t bytes_remaining() const noexcept {
        return _max_size - _current_offset;
    }

    seastar::gate::holder hold() {
        return _write_gate.hold();
    }

    segment_ref ref() {
        return _seg_ref;
    }
};

segment::segment(log_segment_id id, seastar::file file, uint64_t file_offset, size_t max_size)
    : _id(id)
    , _file(std::move(file))
    , _file_offset(file_offset)
    , _max_size(max_size) {
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

void writeable_segment::start(segment_ref seg_ref) {
    _seg_ref = std::move(seg_ref);
}

future<> writeable_segment::stop() {
    if (_write_gate.is_closed()) {
        co_return;
    }
    co_await _write_gate.close();
    _seg_ref = {};
}

log_location writeable_segment::allocate(size_t data_size) {
    if (!can_fit(data_size)) {
        throw std::runtime_error("Entry too large for remaining segment space");
    }

    auto current_pos = _current_offset;
    _current_offset += data_size;

    return log_location{
        .segment = _id,
        .offset = current_pos,
        .size = static_cast<uint32_t>(data_size)
    };
}

future<> writeable_segment::write(log_location loc, bytes_view data) {
    const auto alignment = _file.disk_write_dma_alignment();
    const auto total = data.size();
    auto offset = absolute_offset(loc.offset);
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

class file_manager {
    size_t _segments_per_file;
    size_t _max_files;
    size_t _file_size;
    std::filesystem::path _base_dir;
    seastar::scheduling_group _sched_group;

    size_t _next_file_id{0};

    seastar::gate _async_gate;
    shared_future<> _next_file_formatter{make_ready_future<>()};

    std::vector<seastar::file> _open_read_files;

public:
    file_manager(segment_manager_config cfg)
        : _segments_per_file(cfg.file_size / cfg.segment_size)
        , _max_files(cfg.disk_size / cfg.file_size)
        , _file_size(cfg.file_size)
        , _base_dir(cfg.base_dir)
        , _sched_group(cfg.compaction_sg)
        , _open_read_files(_max_files)
    {}

    future<> start();
    future<> stop();

    future<seastar::file> get_file_for_write(size_t file_id);
    future<seastar::file> get_file_for_read(size_t file_id);

    future<> format_file_region(seastar::file file, uint64_t offset, size_t size);
    future<> format_file(size_t file_id);
    void recover_next_file_id(size_t next_file_id);

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

void file_manager::recover_next_file_id(size_t next_file_id) {
    _next_file_id = next_file_id;

    if (_next_file_id < _max_files) {
        _next_file_formatter = with_gate(_async_gate, [this] {
            return with_scheduling_group(_sched_group, [this] {
                return format_file(_next_file_id);
            });
        });
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

    if (!_open_read_files[file_id]) {
        _open_read_files[file_id] = file;
    }

    co_return file;
}

future<seastar::file> file_manager::get_file_for_read(size_t file_id) {
    auto& cached_file = _open_read_files[file_id];
    if (cached_file) {
        co_return cached_file;
    }

    auto file = co_await seastar::open_file_dma(
        get_file_path(file_id).string(),
        seastar::open_flags::ro
    );

    _open_read_files[file_id] = file;

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

class compaction_manager_impl : public compaction_manager {
public:

    struct compaction_config {
        bool compaction_enabled;
        size_t max_segments_per_compaction;
        seastar::scheduling_group compaction_sg;
        utils::updateable_value<float> compaction_static_shares;
        seastar::scheduling_group separator_sg;
    };

private:
    segment_manager_impl& _sm;
    compaction_config _cfg;

    seastar::gate _async_gate;

    struct stats {
        uint64_t segments_compacted{0};
        uint64_t compaction_segments_freed{0};
        uint64_t compaction_records_skipped{0};
        uint64_t compaction_records_rewritten{0};
        uint64_t separator_buffer_flushed{0};
        uint64_t separator_segments_freed{0};
    } _stats;

    struct controller {
        float _compaction_overhead{1.0}; // running average ratio

        void update(size_t segment_write_count, size_t new_segments);
    };
    controller _controller;
    timer<lowres_clock> _adjust_shares_timer;

    seastar::semaphore _separator_flush_sem{0};
    seastar::semaphore _compaction_sem{1};

    struct group_compaction_state {
        bool running{false};
        shared_future<> completion{make_ready_future<>()};
        abort_source as;
    };
    absl::flat_hash_map<compaction_group*, std::unique_ptr<group_compaction_state>> _groups;

public:
    compaction_manager_impl(segment_manager_impl& sm, compaction_config cfg)
        : _sm(sm)
        , _cfg(std::move(cfg))
        , _adjust_shares_timer(default_scheduling_group(), [this] { adjust_shares(); })
    {}

    future<> start();
    future<> stop();

    void enable_separator_flush(size_t max_concurrency) {
        _separator_flush_sem.signal(max_concurrency);
    }

    const stats& get_stats() const noexcept { return _stats; }

    future<> write_to_separator(write_buffer&, segment_ref, size_t segment_seq_num);

    separator_buffer allocate_separator_buffer() override;
    future<> flush_separator_buffer(separator_buffer buf, compaction_group&) override;

private:

    bool is_record_alive(primary_index&, const primary_index_key&, log_location);
    bool update_record_location(primary_index&, const primary_index_key&, log_location old_loc, log_location new_loc);

    void submit(compaction_group&) override;
    future<> stop_ongoing_compactions(compaction_group&) override;

    std::vector<log_segment_id> select_segments_for_compaction(const segment_descriptor_hist&);
    future<> do_compact(compaction_group&, abort_source&);
    future<> compact_segments(compaction_group&, std::vector<log_segment_id>);

    void adjust_shares() {
        if (auto static_shares = _cfg.compaction_static_shares.get(); static_shares != 0) {
            _cfg.compaction_sg.set_shares(static_shares);
        } else {
            auto shares = std::max<float>(1000 * _controller._compaction_overhead, 1000);
            _cfg.compaction_sg.set_shares(shares);
        }
    }
};

future<> compaction_manager_impl::start() {
    if (_cfg.compaction_sg != default_scheduling_group()) {
        _adjust_shares_timer.arm_periodic(std::chrono::milliseconds(50));
    }
    co_return;
}

future<> compaction_manager_impl::stop() {
    if (_async_gate.is_closed()) {
        co_return;
    }
    _adjust_shares_timer.cancel();
    for (auto& [cg, state] : _groups) {
        state->as.request_abort();
    }
    _separator_flush_sem.broken();
    _compaction_sem.broken();
    co_await _async_gate.close();
    _groups.clear();
}

enum class write_source {
    normal_write,
    compaction,
    separator,
};

static sstring write_source_to_string(write_source src) {
    switch (src) {
    case write_source::normal_write:
        return "normal_write";
    case write_source::compaction:
        return "compaction";
    case write_source::separator:
        return "separator";
    }
    return "unknown";
}

class segment_pool {

    seastar::queue<seg_ptr> _segments;
    size_t _reserved_for_compaction;
    seastar::condition_variable _segment_available;

public:

    struct stats {
        uint64_t segments_put{0};
        uint64_t segments_get{0};
        uint64_t compaction_segments_get{0};
        uint64_t normal_segments_get{0};
        uint64_t normal_segments_wait{0};
        uint64_t separator_segments_get{0};
    } _stats;

    segment_pool(size_t pool_size, size_t reserved_for_compaction)
        : _segments(pool_size)
        , _reserved_for_compaction(reserved_for_compaction)
    {}

    future<> start() {
        co_return;
    }

    future<> stop() {
        _segments.abort(std::make_exception_ptr(abort_requested_exception()));
        _segment_available.broken();
        co_return;
    }

    future<> put(seg_ptr seg) {
        co_await _segments.push_eventually(std::move(seg));
        _segment_available.broadcast();
        _stats.segments_put++;
    }

    future<seg_ptr> get_segment(write_source src) {
        seg_ptr seg;
        switch (src) {
        case write_source::compaction:
            _stats.compaction_segments_get++;
            co_return co_await _segments.pop_eventually();
        case write_source::normal_write:
            if (_segments.size() <= _reserved_for_compaction) {
                _stats.normal_segments_wait++;
            }
            while (_segments.size() <= _reserved_for_compaction) {
                co_await _segment_available.wait([this] {
                    return _segments.size() > _reserved_for_compaction;
                });
            }
            _stats.normal_segments_get++;
            co_return _segments.pop();
        case write_source::separator:
            while (_segments.size() <= _reserved_for_compaction) {
                co_await _segment_available.wait([this] {
                    return _segments.size() > _reserved_for_compaction;
                });
            }
            _stats.separator_segments_get++;
            co_return _segments.pop();
        }
    }

    size_t size() const noexcept {
        return _segments.size();
    }

    const stats& get_stats() const noexcept {
        return _stats;
    }
};

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
        uint64_t separator_bytes_written{0};
        uint64_t separator_data_bytes_written{0};
    };

    file_manager _file_mgr;
    compaction_manager_impl _compaction_mgr;

    segment_manager_config _cfg;
    size_t _segments_per_file;
    size_t _max_segments;
    size_t _next_new_segment_id{0};

    stats _stats;
    seastar::metrics::metric_groups _metrics;

    static constexpr size_t trigger_compaction_threshold = 10; // percentage of max segments
    static constexpr size_t segment_pool_size = 128;

    seg_ptr _active_segment;
    segment_pool _segment_pool;
    std::optional<shared_future<>> _switch_segment_fut;
    size_t _segment_seq_num{0};

    seastar::gate _async_gate;
    future<> _reserve_replenisher{make_ready_future<>()};
    seastar::condition_variable _segment_freed_cv;

    std::vector<segment_descriptor> _segment_descs;
    seastar::circular_buffer<log_segment_id> _free_segments;

    static constexpr size_t separator_flush_max_concurrency = 4;

    std::vector<write_buffer> _compaction_buffer_pool;
    std::vector<write_buffer*> _available_compaction_buffers;

    std::vector<write_buffer> _separator_buffer_pool;
    std::vector<write_buffer*> _available_separator_buffers;

    std::function<void()> _trigger_compaction_fn;
    std::function<void(size_t)> _trigger_separator_flush_fn;

    utils::phased_barrier _writes_phaser{"logstor_sm_writes"};

public:
    static constexpr size_t block_alignment = segment_manager::block_alignment;

    explicit segment_manager_impl(segment_manager_config);

    segment_manager_impl(const segment_manager_impl&) = delete;
    segment_manager_impl& operator=(const segment_manager_impl&) = delete;

    future<> do_recovery(replica::database&);

    future<> start();
    future<> stop();

    future<log_location> write(write_buffer&);
    future<log_location> write_full_segment(write_buffer&, compaction_group&, write_source);

    future<log_record> read(log_location);

    void free_record(log_location);

    future<> for_each_record(log_segment_id,
                            std::function<future<>(log_location, log_record)>);

    future<> for_each_record(const std::vector<log_segment_id>&,
                            std::function<future<>(log_location, log_record)>);

    future<> recover_segment(replica::database&, log_segment_id);
    future<std::optional<segment_generation>> recover_segment_generation(log_segment_id);
    future<> add_segment_to_compaction_group(replica::database&, segment_descriptor&);

    void trigger_compaction() {
        if (_cfg.compaction_enabled && _trigger_compaction_fn) {
            _trigger_compaction_fn();
        }
    }

    void trigger_separator_flush(size_t seq) {
        if (_trigger_separator_flush_fn) {
            _trigger_separator_flush_fn(seq);
        }
    }

    compaction_manager& get_compaction_manager() noexcept {
        return _compaction_mgr;
    }

    const compaction_manager& get_compaction_manager() const noexcept {
        return _compaction_mgr;
    }

    void set_trigger_compaction_hook(std::function<void()> fn) {
        _trigger_compaction_fn = std::move(fn);
    }

    void set_trigger_separator_flush_hook(std::function<void(size_t)> fn) {
        _trigger_separator_flush_fn = std::move(fn);
    }

    size_t get_segment_size() const noexcept {
        return _cfg.segment_size;
    }

    future<> discard_segments(segment_set&);

    size_t get_memory_usage() const {
        return _cfg.max_separator_memory;
    }

    future<> await_pending_writes() {
        return _writes_phaser.advance_and_await();
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
    std::chrono::microseconds calculate_separator_delay() const;

    future<std::pair<segment_allocation_guard, seg_ptr>> allocate_and_create_new_segment();
    future<segment_allocation_guard> allocate_segment();

    segment_ref make_segment_ref(log_segment_id seg_id) {
        return segment_ref(seg_id,
            [this, seg_id] {
                return free_segment(seg_id);
            },
            [seg_id] {
                logstor_logger.warn("Segment {} has no more references but it can't be freed", seg_id);
            }
        );
    }

    void free_segment(log_segment_id) noexcept;

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

    friend class compaction_manager_impl;
};

segment_manager_impl::segment_manager_impl(segment_manager_config config)
    : _file_mgr(config)
    , _compaction_mgr(*this, compaction_manager_impl::compaction_config{
            .compaction_enabled = config.compaction_enabled,
            .max_segments_per_compaction = config.max_segments_per_compaction,
            .compaction_sg = config.compaction_sg,
            .compaction_static_shares = config.compaction_static_shares,
            .separator_sg = config.separator_sg
        })
    , _cfg(config)
    , _segments_per_file(config.file_size / config.segment_size)
    , _max_segments((config.disk_size / config.file_size) * _segments_per_file)
    , _segment_pool(segment_pool_size, config.max_segments_per_compaction)
    , _segment_descs(_max_segments)
    {

    _free_segments.reserve(_max_segments);

    // pre-allocate write buffers for compaction
    // currently there is only a single compaction running at a time
    size_t compaction_buffer_count = 1;
    _available_compaction_buffers.reserve(compaction_buffer_count);
    _compaction_buffer_pool.reserve(compaction_buffer_count);
    for (size_t i = 0; i < compaction_buffer_count; ++i) {
        _compaction_buffer_pool.emplace_back(config.segment_size, false);
        _available_compaction_buffers.push_back(&_compaction_buffer_pool.back());
    }

    // pre-allocate write buffers for separator
    size_t separator_buffer_count = _cfg.max_separator_memory / _cfg.segment_size;
    _available_separator_buffers.reserve(separator_buffer_count);
    _separator_buffer_pool.reserve(separator_buffer_count);
    for (size_t i = 0; i < separator_buffer_count; ++i) {
        _separator_buffer_pool.emplace_back(config.segment_size, false);
        _available_separator_buffers.push_back(&_separator_buffer_pool.back());
    }

    namespace sm = seastar::metrics;

    _metrics.add_group("logstor_sm", {
        sm::make_gauge("segments_in_use", _stats.segments_in_use,
                       sm::description("Counts number of segments currently in use.")),
        sm::make_gauge("free_segments", [this] { return _free_segments.size(); },
                       sm::description("Counts number of free segments currently available.")),
        sm::make_gauge("segment_pool_size", [this] { return _segment_pool.size(); },
                       sm::description("Counts number of segments in the segment pool.")),
        sm::make_counter("segment_pool_segments_put", _segment_pool.get_stats().segments_put,
                       sm::description("Counts number of segments returned to the segment pool.")),
        sm::make_counter("segment_pool_normal_segments_get", _segment_pool.get_stats().normal_segments_get,
                       sm::description("Counts number of segments taken from the segment pool for normal writes.")),
        sm::make_counter("segment_pool_compaction_segments_get", _segment_pool.get_stats().compaction_segments_get,
                       sm::description("Counts number of segments taken from the segment pool for compaction.")),
        sm::make_counter("segment_pool_separator_segments_get", _segment_pool.get_stats().separator_segments_get,
                       sm::description("Counts number of segments taken from the segment pool for separator writes.")),
        sm::make_counter("segment_pool_normal_segments_wait", _segment_pool.get_stats().normal_segments_wait,
                       sm::description("Counts number of times normal writes had to wait for a segment to become available in the segment pool.")),
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
        sm::make_counter("separator_bytes_written", _stats.separator_bytes_written,
                       sm::description("Counts number of bytes written to the separator.")),
        sm::make_counter("separator_data_bytes_written", _stats.separator_data_bytes_written,
                       sm::description("Counts number of data bytes written to the separator.")),
        sm::make_counter("separator_buffer_flushed", _compaction_mgr.get_stats().separator_buffer_flushed,
                       sm::description("Counts number of times the separator buffer has been flushed.")),
        sm::make_counter("separator_segments_freed", _compaction_mgr.get_stats().separator_segments_freed,
                       sm::description("Counts number of segments freed by the separator.")),
        sm::make_gauge("separator_flow_control_delay", [this]() { return calculate_separator_delay().count(); },
                       sm::description("Current delay applied to writes to control separator debt in microseconds.")),
    });
}

future<> segment_manager_impl::start() {
    // Start background replenisher before creating initial segment
    _reserve_replenisher = with_scheduling_group(_cfg.compaction_sg, [this] {
        return replenish_reserve();
    });

    co_await _compaction_mgr.start();

    co_await switch_active_segment();

    _compaction_mgr.enable_separator_flush(separator_flush_max_concurrency);

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

    co_await _segment_pool.stop();

    _segment_freed_cv.broken();
    co_await std::move(_reserve_replenisher);

    co_await _compaction_mgr.stop();

    co_await _file_mgr.stop();

    logstor_logger.info("Segment manager stopped");
}

future<log_location> segment_manager_impl::write(write_buffer& wb) {
    auto holder = _async_gate.hold();
    auto write_op = _writes_phaser.start();

    wb.finalize(block_alignment);

    bytes_view data(reinterpret_cast<const int8_t*>(wb.data()), wb.offset_in_buffer());

    if (data.size() > _cfg.segment_size) {
        throw std::runtime_error(fmt::format( "Write size {} exceeds segment size {}", data.size(), _cfg.segment_size));
    }

    while (!_active_segment || !_active_segment->can_fit(data.size())) {
        co_await request_segment_switch();
    }

    log_location loc;

    {
        seg_ptr seg = _active_segment;
        auto seg_holder = seg->hold();
        auto seg_ref = seg->ref();
        auto segment_seq_num = _segment_seq_num;

        auto loc = seg->allocate(data.size());
        auto& desc = get_segment_descriptor(loc);

        wb.write_header(desc.seg_gen);

        co_await seg->write(loc, data);

        desc.on_write(wb.get_net_data_size(), wb.get_record_count());

        _stats.bytes_written += data.size();
        _stats.data_bytes_written += wb.get_net_data_size();

        // complete all buffered writes with their individual locations and wait
        // for them to be updated in the index.
        co_await wb.complete_writes(loc);

        co_await with_scheduling_group(_cfg.separator_sg, [&] {
            return _compaction_mgr.write_to_separator(wb, std::move(seg_ref), segment_seq_num);
        });
    }

    // flow control for separator debt
    if (auto separator_delay = calculate_separator_delay(); separator_delay.count() > 0) {
        co_await seastar::sleep(separator_delay);
    }

    co_return loc;
}

future<log_location> segment_manager_impl::write_full_segment(write_buffer& wb, compaction_group& cg, write_source source) {
    auto holder = _async_gate.hold();

    wb.finalize(block_alignment);

    bytes_view data(reinterpret_cast<const int8_t*>(wb.data()), wb.offset_in_buffer());

    if (data.size() > _cfg.segment_size) {
        throw std::runtime_error(fmt::format( "Write size {} exceeds segment size {}", data.size(), _cfg.segment_size));
    }

    seg_ptr seg = co_await _segment_pool.get_segment(source);
    _stats.segments_in_use++;
    logstor_logger.trace("Write full segment {} from {}", seg->id(), write_source_to_string(source));

    auto loc = seg->allocate(data.size());
    auto& desc = get_segment_descriptor(loc);

    wb.write_header(desc.seg_gen);

    co_await seg->write(loc, data);

    desc.on_write(wb.get_net_data_size(), wb.get_record_count());

    switch (source) {
        case write_source::separator:
            _stats.separator_bytes_written += data.size();
            _stats.separator_data_bytes_written += wb.get_net_data_size();
            break;
        case write_source::compaction:
            _stats.compaction_bytes_written += data.size();
            _stats.compaction_data_bytes_written += wb.get_net_data_size();
            break;
        default:
            _stats.bytes_written += data.size();
            _stats.data_bytes_written += wb.get_net_data_size();
            break;
    }

    co_await wb.complete_writes(loc);
    co_await seg->stop();
    cg.add_logstor_segment(desc);

    co_return loc;
}

void segment_manager_impl::free_record(log_location location) {
    auto& desc = get_segment_descriptor(location);
    desc.on_free(location);
    if (desc.owner) {
        desc.owner->update_segment(desc);
    }
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

    auto new_seg = co_await _segment_pool.get_segment(write_source::normal_write);

    auto old_seg = std::exchange(_active_segment, std::move(new_seg));
    _stats.segments_in_use++;
    _segment_seq_num++;

    if (old_seg) {
        // close old segment in background
        (void)with_gate(_async_gate, [old_seg] {
            return old_seg->stop();
        }).then([old_seg] {});
    }

    // trigger separator flush for separator buffers that hold old segments
    auto u = std::max<size_t>(1, _max_segments / 100);
    if (_segment_seq_num % u == 0 && _segment_seq_num > 5*u) {
        trigger_separator_flush(_segment_seq_num - 5*u);
    }

    _active_segment->start(make_segment_ref(_active_segment->id()));
    logstor_logger.trace("Switched active segment to {}", _active_segment->id());
}

future<> segment_manager_impl::replenish_reserve() {
    while (true) {
        bool retry = false;
        try {
            auto [seg_guard, seg] = co_await allocate_and_create_new_segment();
            co_await _segment_pool.put(std::move(seg));
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

    auto seg_id = *seg_guard.segment_id;
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
            trigger_compaction();
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

void segment_manager_impl::free_segment(log_segment_id segment_id) noexcept {
    // Before freeing a segment, ensure there are no ongoing operations that use
    // locations in this segment. See for example `await_pending_reads`.
    logstor_logger.trace("Free segment {}", segment_id);

    auto& desc = get_segment_descriptor(segment_id);
    if (desc.net_data_size(_cfg.segment_size) != 0) {
        logstor_logger.error("Freeing segment {} that has data", segment_id);
    }
    desc.on_free_segment();

    // TODO write new generation?

    _free_segments.push_back(segment_id);
    _segment_freed_cv.signal();

    _stats.segments_freed++;
    _stats.segments_in_use--;
}

future<> segment_manager_impl::discard_segments(segment_set& ss) {
    auto holder = _async_gate.hold();

    while (!ss._segments.empty()) {
        co_await coroutine::maybe_yield();
        auto& desc = ss._segments.one_of_largest();
        auto seg_id = desc_to_segment_id(desc);

        // the index should be cleared before discarding segments, so no data should be reachable
        desc.reset(_cfg.segment_size);

        ss.remove_segment(desc);
        free_segment(seg_id);
    }
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
    auto seg_gen = get_segment_descriptor(segment_id).seg_gen;

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

        if (bh.seg_gen != seg_gen) {
            continue;
        }

        // TODO crc, torn writes

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

bool compaction_manager_impl::is_record_alive(primary_index& index, const primary_index_key& key, log_location loc) {
    return index.get(key)
        .transform([loc] (const index_entry& e) { return e.location == loc; })
        .value_or(false);
}

bool compaction_manager_impl::update_record_location(primary_index& index, const primary_index_key& key, log_location old_loc, log_location new_loc) {
    return index.update_record_location(key, old_loc, new_loc);
}

void compaction_manager_impl::submit(compaction_group& cg) {
    if (_async_gate.is_closed() || !_cfg.compaction_enabled) {
        return;
    }
    auto& state_ptr = _groups[&cg];
    if (!state_ptr) {
        state_ptr = std::make_unique<group_compaction_state>();
    }
    auto& state = *state_ptr;
    if (state.running) {
        return;
    }
    state.running = true;
    state.as = {};
    state.completion = shared_future(with_gate(_async_gate,
        [this, &cg, &state] -> future<> {
            return do_compact(cg, state.as).then([&state] {
                state.running = false;
            });
        }
    ));
}

future<> compaction_manager_impl::stop_ongoing_compactions(compaction_group& cg) {
    auto it = _groups.find(&cg);
    if (it == _groups.end()) {
        co_return;
    }
    auto& state = *it->second;
    state.as.request_abort();
    co_await state.completion.get_future();
    _groups.erase(it);
}

std::vector<log_segment_id> compaction_manager_impl::select_segments_for_compaction(const segment_descriptor_hist& segments) {
    size_t accum_net_data_size = 0;
    size_t accum_record_count = 0;
    ssize_t max_gain = 0;
    size_t best_count = 0;
    std::vector<log_segment_id> candidates;
    const auto segment_size = _sm.get_segment_size();

    for (const auto& desc : segments) {
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

    logstor_logger.debug("Selected {} segments for compaction for estimated gain of {} segments", best_count, max_gain);

    return candidates
            | std::views::take(best_count)
            | std::ranges::to<std::vector<log_segment_id>>();
}

future<> compaction_manager_impl::do_compact(compaction_group& cg, abort_source& as) {
    auto sem_units = co_await get_units(_compaction_sem, 1, as);

    if (as.abort_requested() || !_cfg.compaction_enabled) {
        co_return;
    }

    auto candidates = select_segments_for_compaction(cg.logstor_segments()._segments);
    if (candidates.size() == 0) {
        co_return;
    }

    auto holder = _async_gate.hold();

    co_await with_scheduling_group(_cfg.compaction_sg, [this, &cg, candidates = std::move(candidates)] mutable {
        return compact_segments(cg, std::move(candidates));
    });
}

future<> compaction_manager_impl::compact_segments(compaction_group& cg, std::vector<log_segment_id> segments) {
    logstor_logger.trace("Starting compaction of segments {} in compaction group {}:{}", segments, cg.schema()->id(), cg.group_id());

    struct compaction_buffer {
        segment_manager_impl& sm;
        write_buffer* buf = nullptr;
        compaction_group& cg;
        std::vector<future<>> pending_updates;
        size_t flush_count{0};

        explicit compaction_buffer(segment_manager_impl& sm, compaction_group& cg)
            : sm(sm), cg(cg)
        {
            if (sm._available_compaction_buffers.empty()) {
                throw std::runtime_error("No available compaction buffers");
            }
            buf = sm._available_compaction_buffers.back();
            sm._available_compaction_buffers.pop_back();
        }

        ~compaction_buffer() {
            if (buf) {
                (void)buf->close().then([sm = &this->sm, buf = this->buf] {
                    buf->reset();
                    sm->_available_compaction_buffers.push_back(buf);
                });
            }
        }

        future<> flush() {
            if (buf->has_data()) {
                flush_count++;
                auto base_location = co_await sm.write_full_segment(*buf, cg, write_source::compaction);
                co_await when_all_succeed(pending_updates.begin(), pending_updates.end());
                logstor_logger.trace("Compaction buffer flushed to {} with {} bytes", base_location, buf->get_net_data_size());
            }
            co_await buf->close();
            buf->reset();
            pending_updates.clear();
        }

        future<> close() {
            co_await flush();
            sm._available_compaction_buffers.push_back(buf);
            buf = nullptr;
        }
    };

    compaction_buffer cb(_sm, cg);

    size_t records_rewritten = 0;
    size_t records_skipped = 0;

    co_await _sm.for_each_record(segments,
            [this, &cg, &records_rewritten, &records_skipped, &cb]
            (log_location read_location, log_record record) -> future<> {

        if (!is_record_alive(cg.get_logstor_index(), record.key, read_location)) {
            records_skipped++;
            _stats.compaction_records_skipped++;
            co_return;
        }

        auto key = record.key;
        log_record_writer writer(std::move(record));

        if (!cb.buf->can_fit(writer)) {
            co_await cb.flush();
        }

        // write the record and then update the index with the new location
        auto write_and_update_index = cb.buf->write(std::move(writer)).then_unpack(
                [this, &cg, key = std::move(key), read_location, &records_rewritten, &records_skipped]
                (log_location new_location, seastar::gate::holder op) {

            if (update_record_location(cg.get_logstor_index(), key, read_location, new_location)) {
                _sm.free_record(read_location);
                records_rewritten++;
            } else {
                // another write updated this key
                _sm.free_record(new_location);
                records_skipped++;
            }
        });

        cb.pending_updates.push_back(std::move(write_and_update_index));
    });

    co_await cb.close();

    logstor_logger.debug("Compaction complete: {} records rewritten, {} skipped from {} segments, flushed {} times",
                       records_rewritten, records_skipped, segments.size(), cb.flush_count);

    // wait for read operations that use the old locations
    co_await cg.get_logstor_index().await_pending_reads();

    // Free the compacted segments
    auto& ss = cg.logstor_segments();
    for (auto seg_id : segments) {
        logstor_logger.trace("Free segment {} by compaction", seg_id);
        ss.remove_segment(_sm.get_segment_descriptor(seg_id));
        _sm.free_segment(seg_id);
    }

    size_t new_segments = segments.size() > cb.flush_count ? segments.size() - cb.flush_count : 0;
    _stats.segments_compacted += segments.size();
    _stats.compaction_segments_freed += new_segments;
    _stats.compaction_records_rewritten += records_rewritten;
    _stats.compaction_records_skipped += records_skipped;

    _controller.update(cb.flush_count, new_segments);
}

void compaction_manager_impl::controller::update(size_t segment_write_count, size_t new_segments) {
    float new_overhead = static_cast<float>(segment_write_count) / std::max<size_t>(1, new_segments);
    _compaction_overhead = 0.8 * _compaction_overhead + 0.2 * new_overhead;
}

separator_buffer compaction_manager_impl::allocate_separator_buffer() {
    // TODO really ensure we have enough buffers
    if (_sm._available_separator_buffers.empty()) {
        throw std::runtime_error("No available separator buffers");
    }
    write_buffer* wb = _sm._available_separator_buffers.back();
    _sm._available_separator_buffers.pop_back();

    return separator_buffer(wb);
}

future<> compaction_manager_impl::write_to_separator(write_buffer& wb, segment_ref seg_ref, size_t segment_seq_num) {
    for (auto&& w : wb.records()) {
        co_await coroutine::maybe_yield();

        auto key = w.writer.record().key;
        log_location prev_loc = co_await std::move(w.loc);

        auto& index = w.cg->get_logstor_index();
        auto& buf = w.cg->get_separator_buffer(w.writer.size());

        // the separator buffer holds a reference to the segment.
        // the segment is freed after all separator buffers that reference it are flushed.
        if (buf.held_segments.empty() || buf.held_segments.back().id() != seg_ref.id()) {
            buf.held_segments.push_back(seg_ref);
        }

        if (!buf.min_seq_num || segment_seq_num < *buf.min_seq_num) {
            buf.min_seq_num = segment_seq_num;
        }

        buf.pending_updates.push_back(
            buf.write(std::move(w.writer)).then_unpack([this, &index, key = std::move(key), prev_loc] (log_location new_loc, seastar::gate::holder op) {
                if (update_record_location(index, key, prev_loc, new_loc)) {
                    _sm.free_record(prev_loc);
                } else {
                    _sm.free_record(new_loc);
                }
                return make_ready_future<>();
            }
        ));
    }
}

future<> compaction_manager_impl::flush_separator_buffer(separator_buffer buf, compaction_group& cg) {
    logstor_logger.trace("Flushing separator buffer with {} bytes", buf.buf->offset_in_buffer());

    if (buf.buf->has_data()) {
        auto sem_units = co_await get_units(_separator_flush_sem, 1);
        co_await with_scheduling_group(_cfg.separator_sg, [&] {
            return _sm.write_full_segment(*buf.buf, cg, write_source::separator);
        });
        _stats.separator_buffer_flushed++;
    }
    co_await when_all_succeed(buf.pending_updates.begin(), buf.pending_updates.end());
    co_await buf.buf->close();

    auto wb = std::move(buf.buf);
    wb->reset();
    _sm._available_separator_buffers.push_back(std::move(wb));

    // wait for read operations that use the old locations before freeing the old segments
    co_await cg.get_logstor_index().await_pending_reads();

    // the separator buffer is destroyed and frees the segment if it's the last holder
    buf.flushed = true;
}

std::chrono::microseconds segment_manager_impl::calculate_separator_delay() const {
    auto soft_limit = _separator_buffer_pool.size() / 2;
    auto hard_limit = _separator_buffer_pool.size();
    auto used_buffers = _separator_buffer_pool.size() - _available_separator_buffers.size();
    if (used_buffers < soft_limit) {
        return std::chrono::microseconds(0);
    }
    float debt_ratio = float(used_buffers - soft_limit) / (hard_limit - soft_limit);
    auto adjust = [] (float x) { return x * x * x; };
    return std::chrono::microseconds(size_t(adjust(debt_ratio) * _cfg.separator_delay_limit_ms * 1000));
}

future<> segment_manager_impl::do_recovery(replica::database& db) {
    logstor_logger.info("Starting recovery for shard {} in directory {}", this_shard_id(), _cfg.base_dir.string());

    co_await _file_mgr.start();

    // Scan the base directory for all files belonging to this shard.
    std::set<size_t> found_file_ids;
    std::vector<sstring> files_for_removal;
    std::string file_prefix = _file_mgr.get_file_name_prefix();
    co_await lister::scan_dir(_cfg.base_dir, lister::dir_entry_types::of<directory_entry_type::regular>(),
            [this, &found_file_ids, &files_for_removal, &file_prefix] (fs::path dir, directory_entry de) {
        if (!de.name.starts_with(file_prefix)) {
            // not our file
            return make_ready_future<>();
        }
        auto file_id_opt = _file_mgr.file_name_to_file_id(de.name);
        if (file_id_opt) {
            found_file_ids.insert(*file_id_opt);
        } else if (de.name.ends_with(".tmp")) {
            files_for_removal.push_back((dir / de.name).string());
        }
        return make_ready_future<>();
    });
    logstor_logger.info("Recovery: found {} files for shard {} in {}", found_file_ids.size(), this_shard_id(), _cfg.base_dir.string());

    // Remove any leftover temp files
    co_await coroutine::parallel_for_each(files_for_removal.begin(), files_for_removal.end(),
        [] (const sstring& file_path) {
            logstor_logger.info("Recovery: removing leftover temp file {}", file_path);
            return seastar::remove_file(file_path);
        }
    );

    // Verify all files are present
    size_t next_file_id = 0;
    for (auto file_id : found_file_ids) {
        if (file_id != next_file_id) {
            throw std::runtime_error(fmt::format("Missing log segment file(s) detected during recovery: file {} missing", _file_mgr.get_file_path(next_file_id).string()));
        }
        next_file_id++;
    }

    // populate index from all segments. keep the latest record for each key.
    for (auto file_id : found_file_ids) {
        logstor_logger.info("Recovering segments from file {}: {}%", _file_mgr.get_file_path(file_id).string(), (file_id + 1) * 100 / found_file_ids.size());
        co_await max_concurrent_for_each(segments_in_file(file_id), 32,
            [this, &db] (log_segment_id seg_id) {
                return recover_segment(db, seg_id);
            }
        );
    }

    // go over the index and mark all segments that have live data as used.
    size_t allocated_segment_count = next_file_id * _segments_per_file;
    utils::dynamic_bitset used_segments(allocated_segment_count);

    co_await db.get_tables_metadata().for_each_table_gently([&] (table_id tid, lw_shared_ptr<table> tp) -> future<> {
        if (!tp->uses_logstor()) {
            co_return;
        }
        for (const auto& entry : tp->logstor_index()) {
            used_segments.set(entry.entry().location.segment.value);
            co_await coroutine::maybe_yield();
        }
    });

    // put used segments in compaction groups, and put the rest in the free list.
    size_t free_segment_count = 0;
    size_t used_segment_count = 0;
    for (size_t seg_idx = 0; seg_idx < allocated_segment_count; ++seg_idx) {
        co_await coroutine::maybe_yield();
        log_segment_id seg_id(seg_idx);
        auto& desc = get_segment_descriptor(seg_id);
        if (!used_segments.test(seg_idx)) {
            desc.on_free_segment();
            _free_segments.push_back(seg_id);
            free_segment_count++;
        } else {
            used_segment_count++;
        }
    }
    logstor_logger.info("Found {} used segments and {} free segments", used_segment_count, free_segment_count);

    size_t recovered_used_segment_count = 0;
    for (size_t seg_idx = 0; seg_idx < allocated_segment_count; ++seg_idx) {
        co_await coroutine::maybe_yield();
        log_segment_id seg_id(seg_idx);
        auto& desc = get_segment_descriptor(seg_id);
        if (used_segments.test(seg_idx)) {
            logstor_logger.trace("Recovering used segment {}", seg_id);
            if (recovered_used_segment_count % 1000 == 0) {
                logstor_logger.info("Recovering used segments: {}%", 100 * recovered_used_segment_count / used_segment_count);
            }
            co_await add_segment_to_compaction_group(db, desc);
            _stats.segments_in_use++;
            recovered_used_segment_count++;
        }
    }

    _next_new_segment_id = allocated_segment_count;

    _file_mgr.recover_next_file_id(next_file_id);

    logstor_logger.info("Recovery complete");
}

future<> segment_manager_impl::recover_segment(replica::database& db, log_segment_id segment_id) {
    auto& desc = get_segment_descriptor(segment_id);
    desc.reset(_cfg.segment_size);

    auto seg_gen_opt = co_await recover_segment_generation(segment_id);
    if (!seg_gen_opt) {
        co_return;
    }
    desc.seg_gen = *seg_gen_opt;

    co_await for_each_record(segment_id, [this, &desc, &db] (log_location loc, log_record record) -> future<> {
        logstor_logger.trace("Recovery: read record at {} gen {}", loc, record.generation);

        index_entry new_entry {
            .location = loc,
            .generation = record.generation
        };

        try {
            auto& t = db.find_column_family(record.table);
            if (!t.uses_logstor()) {
                co_return;
            }
            auto [inserted, prev_entry] = t.logstor_index().insert_if_newer(record.key, new_entry);
            if (inserted) {
                desc.on_write(loc);
                if (prev_entry) {
                    get_segment_descriptor(prev_entry->location).on_free(prev_entry->location);
                }
            }
        } catch (const replica::no_such_column_family&) {
            // ignore record
        }

        co_return;
    });
}

future<std::optional<segment_generation>> segment_manager_impl::recover_segment_generation(log_segment_id segment_id) {
    auto [file_id, file_offset] = segment_id_to_file_location(segment_id);
    auto file = co_await _file_mgr.get_file_for_read(file_id);

    std::optional<segment_generation> max_gen;

    for (size_t current_position = 0; current_position < _cfg.segment_size; current_position += block_alignment) {
        auto buffer_header_buf = co_await file.dma_read_exactly<char>(
            file_offset + current_position, write_buffer::buffer_header_size);

        if (buffer_header_buf.size() < write_buffer::buffer_header_size) {
            continue;
        }

        seastar::simple_memory_input_stream buffer_header_stream(
            buffer_header_buf.begin(), buffer_header_buf.size());

        auto bh = ser::deserialize(buffer_header_stream,
            std::type_identity<write_buffer::buffer_header>{});

        if (bh.magic != write_buffer::buffer_header_magic) {
            continue;
        }

        if (!max_gen || bh.seg_gen > *max_gen) {
            max_gen = bh.seg_gen;
        }
    }

    co_return max_gen;
}

future<> segment_manager_impl::add_segment_to_compaction_group(replica::database& db, segment_descriptor& desc) {
    auto seg_id = desc_to_segment_id(desc);

    auto for_each_live_record = [this, &db, seg_id] (std::function<future<>(log_location, log_record, table&)> callback) {
        return for_each_record(seg_id, [&db, callback = std::move(callback)] (log_location loc, log_record record) -> future<> {
            try {
                auto& t = db.find_column_family(record.table);
                if (!t.uses_logstor()) {
                    co_return;
                }
                if (t.logstor_index().get(record.key).transform([](auto&& entry) { return entry.location; }) == loc) {
                    co_await callback(loc, std::move(record), t);
                }
            } catch(const replica::no_such_column_family&) {
                co_return;
            }
        });
    };

    // find the segment's table and token range
    struct mixed_tables{};
    std::optional<std::variant<table_id, mixed_tables>> segment_table;
    std::optional<dht::token> first_token;
    std::optional<dht::token> last_token;
    size_t live_record_count = 0;
    co_await for_each_live_record([&segment_table, &first_token, &last_token, &live_record_count] (log_location loc, log_record record, table&) -> future<> {
        ++live_record_count;

        if (!segment_table) {
            segment_table = record.table;
        } else if (std::holds_alternative<table_id>(*segment_table) && std::get<table_id>(*segment_table) != record.table) {
            segment_table = mixed_tables{};
        }

        auto record_token = record.key.dk.token();
        if (!first_token || record_token < *first_token) {
            first_token = record_token;
        }
        if (!last_token || record_token > *last_token) {
            last_token = record_token;
        }
        co_return;
    });

    bool need_separator = false;
    if (!segment_table) {
        logstor_logger.warn("Segment {} has no live records, but was not freed. Freeing now.", seg_id);
        desc.on_free_segment();
        _free_segments.push_back(seg_id);
    } else if (std::holds_alternative<mixed_tables>(*segment_table)) {
        logstor_logger.debug("Segment {} has {} live records from multiple tables", seg_id, live_record_count);
        need_separator = true;
    } else {
        auto tid = std::get<table_id>(*segment_table);
        auto& t = db.find_column_family(tid);
        if (t.add_logstor_segment(desc, *first_token, *last_token)) {
            // all record belong to a single compaction group and the segment was added to the compaction group
            logstor_logger.debug("Add segment {} with {} record with tokens [{},{}] to table", seg_id, live_record_count, *first_token, *last_token);
        } else {
            // the record belong to different compaction groups - write to separator
            logstor_logger.debug("Add segment {} with {} record with tokens [{},{}] to separator", seg_id, live_record_count, *first_token, *last_token);
            need_separator = true;
        }
    }

    if (need_separator) {
        auto seg_ref = make_segment_ref(seg_id);
        co_await for_each_live_record([this, seg_ref] (log_location prev_loc, log_record record, table& t) -> future<> {
            auto key = record.key;

            auto& index = t.logstor_index();

            log_record_writer writer(std::move(record));
            auto& buf = t.get_logstor_separator_buffer(key.dk.token(), writer.size());

            if (buf.held_segments.empty() || buf.held_segments.back().id() != seg_ref.id()) {
                buf.held_segments.push_back(seg_ref);
            }

            buf.pending_updates.push_back(
                buf.write(std::move(writer)).then_unpack([this, &index, key = std::move(key), prev_loc] (log_location new_loc, seastar::gate::holder op) {
                    if (index.update_record_location(key, prev_loc, new_loc)) {
                        free_record(prev_loc);
                    } else {
                        free_record(new_loc);
                    }
                    return make_ready_future<>();
                }
            ));
            return make_ready_future<>();
        });
    }
}

// segment_manager wrapper

segment_manager::segment_manager(segment_manager_config config)
    : _impl(std::make_unique<segment_manager_impl>(std::move(config)))
{ }

segment_manager::~segment_manager() = default;

segment_manager_impl& segment_manager::get_impl() noexcept {
    return *_impl;
}

const segment_manager_impl& segment_manager::get_impl() const noexcept {
    return *_impl;
}

future<> segment_manager::do_recovery(replica::database& db) {
    return _impl->do_recovery(db);
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

void segment_manager::set_trigger_compaction_hook(std::function<void()> fn) {
    _impl->set_trigger_compaction_hook(std::move(fn));
}

void segment_manager::set_trigger_separator_flush_hook(std::function<void(size_t)> fn) {
    _impl->set_trigger_separator_flush_hook(std::move(fn));
}

compaction_manager& segment_manager::get_compaction_manager() noexcept {
    return _impl->get_compaction_manager();
}

const compaction_manager& segment_manager::get_compaction_manager() const noexcept {
    return _impl->get_compaction_manager();
}

size_t segment_manager::get_segment_size() const noexcept {
    return _impl->get_segment_size();
}

future<> segment_manager::discard_segments(segment_set& ss) {
    return _impl->discard_segments(ss);
}

size_t segment_manager::get_memory_usage() const {
    return _impl->get_memory_usage();
}

future<> segment_manager::await_pending_writes() {
    return _impl->await_pending_writes();
}

}

template<>
size_t hist_key<replica::logstor::segment_descriptor>(const replica::logstor::segment_descriptor& desc) {
    return desc.free_space;
}
