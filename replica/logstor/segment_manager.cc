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
#include "utils/log_heap.hh"
#include "utils/phased_barrier.hh"
#include "utils/serialized_action.hh"
#include "utils/lister.hh"

namespace replica::logstor {

class segment {
protected:
    log_segment_id _id;
    seastar::file _file;
    uint64_t _file_offset; // Offset within the shared file where this segment starts
    size_t _max_size;
    std::optional<group_id> _gid;

public:
    segment(log_segment_id id, seastar::file file, uint64_t file_offset, size_t max_size);

    virtual ~segment() = default;

    future<> close();

    future<log_record> read(log_location);

    log_segment_id id() const noexcept { return _id; }
    seastar::file& get_file() noexcept { return _file; }

    void set_group_id(group_id gid) {
        _gid = gid;
    }

    std::optional<group_id> get_group_id() const noexcept {
        return _gid;
    }

protected:
    uint64_t absolute_offset(uint64_t relative_offset) const noexcept {
        return _file_offset + relative_offset;
    }
};

class writeable_segment : public segment {
    seastar::gate _write_gate;
    utils::phased_barrier::operation _barrier_op;
    seastar::gate::holder _separator_holder;

    uint32_t _current_offset = 0; // next offset for write

public:
    using segment::segment;

    struct allocation {
        log_location location;
        seastar::gate::holder holder;
    };

    void start(utils::phased_barrier::operation barrier_op, seastar::gate::holder separator_holder);

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

    seastar::gate::holder hold() {
        return _write_gate.hold();
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

void writeable_segment::start(utils::phased_barrier::operation barrier_op, seastar::gate::holder separator_holder) {
    _barrier_op = std::move(barrier_op);
    _separator_holder = std::move(separator_holder);
}

future<> writeable_segment::stop() {
    if (_write_gate.is_closed()) {
        co_return;
    }
    co_await _write_gate.close();
    _separator_holder.release();
    _barrier_op = {};
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
    segment_generation seg_gen{1};
    std::optional<group_id> gid;

    void reset(size_t segment_size) noexcept {
        free_space = segment_size;
        record_count = 0;
        gid = std::nullopt;
    }

    size_t net_data_size(size_t segment_size) const noexcept {
        return segment_size - free_space;
    }

    void on_free_segment() noexcept {
        ++seg_gen;
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

struct debt_counter {

    struct units {
        debt_counter& counter;
        size_t count;

        units(debt_counter& counter, size_t count)
            : counter(counter)
            , count(count) {}

        ~units() {
            release();
        }

        units(units&& other) noexcept
            : counter(other.counter)
            , count(std::exchange(other.count, 0)) {}

        units& operator=(units&& other) noexcept {
            if (this != &other) {
                release();
                counter = other.counter;
                count = std::exchange(other.count, 0);
            }
            return *this;

        }

        void release() {
            if (count > 0) {
                counter -= count;
                count = 0;
            }
        }

        units(const units&) = delete;
        units& operator=(const units&) = delete;

        units& operator+=(size_t n) {
            counter += n;
            count += n;
            return *this;
        }
    };

    friend class units;

    units consume(size_t amount) {
        _cnt += amount;
        return units{*this, amount};
    }

    size_t count() const noexcept {
        return _cnt;
    }

private:
    size_t _cnt{0};

    debt_counter& operator+=(size_t n) {
        _cnt += n;
        return *this;
    }

    debt_counter& operator-=(size_t n) {
        _cnt -= n;
        return *this;
    }
};

struct separator {
    struct buffer {
        write_buffer* buf;
        utils::chunked_vector<future<>> pending_updates;
        group_id gid;
        debt_counter::units debt_units;

        buffer(write_buffer* wb, group_id gid, debt_counter& counter)
            : buf(wb)
            , gid(gid)
            , debt_units(counter.consume(0))
        {}

        buffer(const buffer&) = delete;
        buffer& operator=(const buffer&) = delete;

        buffer(buffer&&) noexcept = default;
        buffer& operator=(buffer&&) noexcept = default;

        future<log_location_with_holder> write(log_record_writer writer) {
            debt_units += writer.size();
            return buf->write_with_holder(std::move(writer));
        }
    };
    absl::flat_hash_map<group_id, std::deque<buffer>> _group_buffers;
    seastar::circular_buffer<log_segment_id> _segments;
    seastar::gate _async_gate;
    utils::phased_barrier::operation _barrier_op;
    size_t _buffer_count{0};
    bool _switch_requested{false};

    separator(size_t max_segments) {
        _segments.reserve(max_segments);
    }

    separator(separator&&) = default;
    separator& operator=(separator&&) = default;

    separator(const separator&) = delete;
    separator& operator=(const separator&) = delete;

    void start(utils::phased_barrier::operation barrier_op) {
        _barrier_op = std::move(barrier_op);
    }

    future<> close() {
        if (_async_gate.is_closed()) {
            co_return;
        }
        co_await _async_gate.close();
    }

    seastar::gate::holder hold() {
        return _async_gate.hold();
    }

    void add_segment(log_segment_id seg_id) {
        _segments.push_back(seg_id);
    }

    size_t segment_count() const noexcept { return _segments.size(); }
    size_t group_count() const noexcept { return _group_buffers.size(); }
    size_t buffer_count() const noexcept { return _buffer_count; }

    void request_switch() noexcept { _switch_requested = true; }
    bool switch_requested() const noexcept { return _switch_requested; }
};

using sep_ptr = lw_shared_ptr<separator>;

class compaction_manager {
public:

    struct compaction_config {
        bool compaction_enabled;
        size_t max_segments_per_compaction;
        seastar::scheduling_group compaction_sg;
        seastar::scheduling_group separator_sg;
    };

private:
    segment_manager_impl& _sm;
    log_index& _index;
    compaction_config _cfg;

    serialized_action _compaction_action;

    struct compaction_group {
        segment_descriptor_hist segment_hist;
        bool compaction_enabled{true};
    };

    using compaction_group_map = std::map<group_id, compaction_group>;
    using compaction_group_it = compaction_group_map::iterator;

    compaction_group_map _compaction_groups;
    compaction_group_it _next_group_for_compaction;

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

public:
    compaction_manager(segment_manager_impl& sm, log_index& index, compaction_config cfg)
        : _sm(sm)
        , _index(index)
        , _cfg(std::move(cfg))
        , _compaction_action([this] {
            return start_compaction();
        })
        , _next_group_for_compaction(_compaction_groups.end())
        , _adjust_shares_timer(default_scheduling_group(), [this] { adjust_shares(); })
    {}

    future<> start();
    future<> stop();

    void enable_auto_compaction() {
        _cfg.compaction_enabled = true;
    }

    void enable_auto_compaction(table_id table) {
        auto lower = _compaction_groups.lower_bound(group_id{table, 0});
        for (auto it = lower; it != _compaction_groups.end() && it->first.table == table; ++it) {
            it->second.compaction_enabled = true;
        }
    }

    future<> disable_auto_compaction() {
        _cfg.compaction_enabled = false;
        return _compaction_action.join();
    }

    future<> disable_auto_compaction(group_id gid) {
        auto it = _compaction_groups.find(gid);
        if (it != _compaction_groups.end()) {
            it->second.compaction_enabled = false;
            co_await _compaction_action.join();
        }
    }

    future<> disable_auto_compaction(table_id table) {
        auto lower = _compaction_groups.lower_bound(group_id{table, 0});
        for (auto it = lower; it != _compaction_groups.end() && it->first.table == table; ++it) {
            it->second.compaction_enabled = false;
        }
        co_await _compaction_action.join();
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

    // compaction group must be set.
    void add_segment(segment_descriptor& desc) {
        _compaction_groups[*desc.gid].segment_hist.push(desc);
    }

    // call when space is freed in a segment.
    void update_segment(segment_descriptor& desc) {
        if (desc.is_linked()) {
            _compaction_groups[*desc.gid].segment_hist.adjust_up(desc);
        }
    }

    future<std::vector<log_segment_id>> free_compaction_groups(table_id);

    future<> write_to_separator(separator&, write_buffer&, log_location base_location);
    future<> write_to_separator(separator&, log_segment_id);
    future<> flush_separator(separator&);
    future<> abort_separator(separator&);

private:

    bool is_record_alive(const index_key&, log_location);
    bool update_record_location(const index_key&, log_location old_loc, log_location new_loc);

    std::vector<log_segment_id> select_segments_for_compaction(const compaction_group&);
    future<std::optional<std::pair<group_id, std::vector<log_segment_id>>>> select_segments_for_compaction();
    future<> start_compaction();
    future<> compact_segments(group_id gid, std::vector<log_segment_id> segments);

    void remove_segment(segment_descriptor& desc);
    future<std::pair<compaction_group_it, std::vector<log_segment_id>>> remove_compaction_group(compaction_group_it);

    separator::buffer& get_separator_buffer(separator& sep, const log_record_writer& writer);

    void adjust_shares() {
        auto shares = std::max<float>(1000 * _controller._compaction_overhead, 1000);
        _cfg.compaction_sg.set_shares(shares);
    }
};

future<> compaction_manager::start() {
    if (_cfg.compaction_sg != default_scheduling_group()) {
        _adjust_shares_timer.arm_periodic(std::chrono::milliseconds(50));
    }
    co_return;
}

future<> compaction_manager::stop() {
    if (_async_gate.is_closed()) {
        co_return;
    }
    co_await _async_gate.close();

    co_await _compaction_action.join();

    for (auto& [gid, cg] : _compaction_groups) {
        co_await coroutine::maybe_yield();
        while (!cg.segment_hist.empty()) {
            cg.segment_hist.pop_one_of_largest();
            co_await coroutine::maybe_yield();
        }
    }
}

enum class write_source {
    normal_write,
    compaction,
    separator,
};

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

    log_index& _index;
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
    segment_pool _segment_pool;
    std::optional<shared_future<>> _switch_segment_fut;
    utils::phased_barrier _segment_phaser{"logstor_segment_phaser"};

    seastar::gate _async_gate;
    future<> _reserve_replenisher{make_ready_future<>()};
    seastar::condition_variable _segment_freed_cv;

    std::vector<segment_descriptor> _segment_descs;
    seastar::circular_buffer<log_segment_id> _free_segments;

    static constexpr size_t separator_debt_target = 2;

    sep_ptr _active_separator;
    size_t _separator_flush_threshold;
    debt_counter _separator_debt;
    std::vector<write_buffer> _separator_buffer_pool;
    std::vector<write_buffer*> _available_separator_buffers;

public:
    static constexpr size_t block_alignment = segment_manager::block_alignment;

    explicit segment_manager_impl(segment_manager_config, log_index&);

    segment_manager_impl(const segment_manager_impl&) = delete;
    segment_manager_impl& operator=(const segment_manager_impl&) = delete;

    future<> start();
    future<> stop();

    future<log_location> write(write_buffer&);
    future<log_location> write_full_segment(write_buffer&, group_id, write_source);

    future<log_record> read(log_location);

    void free_record(log_location);

    future<> for_each_record(log_segment_id,
                            std::function<future<>(log_location, log_record)>);

    future<> for_each_record(const std::vector<log_segment_id>&,
                            std::function<future<>(log_location, log_record)>);

    future<> do_recovery();
    future<> recover_segment(log_segment_id);
    future<std::optional<segment_generation>> recover_segment_generation(log_segment_id);

    void enable_auto_compaction() {
        logstor_logger.info("Enabling automatic compaction");
        _cfg.compaction_enabled = true;
        _compaction_mgr.enable_auto_compaction();
    }

    void enable_auto_compaction(table_id tid) {
        logstor_logger.info("Enabling automatic compaction for table {}", tid);
        _compaction_mgr.enable_auto_compaction(tid);
    }

    future<> disable_auto_compaction() {
        logstor_logger.info("Disabling automatic compaction");
        _cfg.compaction_enabled = false;
        co_await _compaction_mgr.disable_auto_compaction();
    }

    future<> disable_auto_compaction(table_id tid) {
        logstor_logger.info("Disabling automatic compaction for table {}", tid);
        co_await _compaction_mgr.disable_auto_compaction(tid);
    }

    future<> trigger_compaction(bool major = false) {
        co_await _compaction_mgr.trigger_compaction(major);
    }

    size_t get_segment_size() const noexcept {
        return _cfg.segment_size;
    }

    future<> do_barrier();

    future<> truncate_table(table_id table);

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
    future<> flush_separator(sep_ptr sep);
    std::chrono::microseconds calculate_separator_delay() const;

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

static size_t calculate_separator_flush_threshold(const segment_manager_config& cfg) {
    size_t estimated_tablet_count = std::max<size_t>(1, cfg.disk_size / (5 * 1024 * 1024));

    // we want at least N segments per tablet to reduce waste
    size_t threshold = 10 * estimated_tablet_count;

    // use at most X% of available segments
    threshold = std::min(threshold, cfg.disk_size / cfg.segment_size / 20);

    // don't hold too much segments because we need to keep them all in memory
    threshold = std::min(threshold, cfg.max_separator_memory / cfg.segment_size / 3);

    threshold = std::max<size_t>(threshold, 1);

    logstor_logger.debug("Separator flush threshold set to {} segments", threshold);

    return threshold;
}

segment_manager_impl::segment_manager_impl(segment_manager_config config, log_index& index)
    : _index(index)
    , _file_mgr(config)
    , _compaction_mgr(*this, index, compaction_manager::compaction_config{
            .compaction_enabled = config.compaction_enabled,
            .max_segments_per_compaction = config.max_segments_per_compaction,
            .compaction_sg = config.compaction_sg,
            .separator_sg = config.separator_sg
        })
    , _cfg(config)
    , _segments_per_file(config.file_size / config.segment_size)
    , _max_segments((config.disk_size / config.file_size) * _segments_per_file)
    , _segment_pool(segment_pool_size, config.max_segments_per_compaction)
    , _segment_descs(_max_segments)
    , _separator_flush_threshold(calculate_separator_flush_threshold(config))
    {

    _free_segments.reserve(_max_segments);

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
        sm::make_gauge("separator_debt", [this]() { return _separator_debt.count(); },
                       sm::description("Counts the current separator debt in bytes.")),
        sm::make_gauge("separator_flow_control_delay", [this]() { return calculate_separator_delay().count(); },
                       sm::description("Current delay applied to writes to control separator debt in microseconds.")),
    });
}

future<> segment_manager_impl::start() {
    co_await _file_mgr.start();

    co_await do_recovery();

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

    if (_active_separator) {
        co_await _active_separator->close();
        co_await _compaction_mgr.abort_separator(*_active_separator);
    }

    co_await _compaction_mgr.stop();

    co_await _segment_pool.stop();

    _segment_freed_cv.broken();

    co_await std::move(_reserve_replenisher);

    co_await _file_mgr.stop();

    logstor_logger.info("Segment manager stopped");
}

future<log_location> segment_manager_impl::write(write_buffer& wb) {
    auto holder = _async_gate.hold();

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
        sep_ptr sep = _active_separator;

        // hold the segment for the duration of the write to the segment and separator.
        // the segment holds also the separator.
        auto seg_holder = seg->hold();

        auto alloc = seg->allocate(data.size());
        loc = alloc.location;
        auto& desc = get_segment_descriptor(loc);

        wb.write_header(desc.seg_gen);

        co_await seg->write(std::move(alloc), data);

        desc.on_write(wb.get_net_data_size(), wb.get_record_count());

        _stats.bytes_written += data.size();
        _stats.data_bytes_written += wb.get_net_data_size();

        // complete all buffered writes with their individual locations and wait
        // for them to be updated in the index.
        co_await wb.complete_writes(loc);

        co_await with_scheduling_group(_cfg.separator_sg, [&] {
            return _compaction_mgr.write_to_separator(*sep, wb, loc);
        });
    }

    // flow control for separator debt
    if (auto separator_delay = calculate_separator_delay(); separator_delay.count() > 0) {
        co_await seastar::sleep(separator_delay);
    }

    co_return loc;
}

future<log_location> segment_manager_impl::write_full_segment(write_buffer& wb, group_id gid, write_source source) {
    auto holder = _async_gate.hold();

    wb.finalize(block_alignment);

    bytes_view data(reinterpret_cast<const int8_t*>(wb.data()), wb.offset_in_buffer());

    if (data.size() > _cfg.segment_size) {
        throw std::runtime_error(fmt::format( "Write size {} exceeds segment size {}", data.size(), _cfg.segment_size));
    }

    seg_ptr seg = co_await _segment_pool.get_segment(source);
    seg->set_group_id(gid);
    _stats.segments_in_use++;

    auto alloc = seg->allocate(data.size());
    auto loc = alloc.location;
    auto& desc = get_segment_descriptor(loc);

    wb.write_header(desc.seg_gen);

    co_await seg->write(std::move(alloc), data);

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

    // complete all buffered writes with their individual locations
    co_await wb.complete_writes(loc);

    (void)with_gate(_async_gate, [this, seg] () {
        return close_segment(std::move(seg));
    });

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

    // get new segment and separator, then switch both atomically
    std::optional<sep_ptr> new_sep;
    if (!_active_separator
            || _active_separator->switch_requested()
            || _active_separator->segment_count() >= _separator_flush_threshold
            || _available_separator_buffers.size() < _active_separator->group_count() * 2) {
        new_sep = make_lw_shared<separator>(_separator_flush_threshold);
    }

    auto new_seg = co_await _segment_pool.get_segment(write_source::normal_write);

    auto old_seg = std::exchange(_active_segment, std::move(new_seg));
    _stats.segments_in_use++;

    if (new_sep) {
        auto old_sep = std::exchange(_active_separator, std::move(*new_sep));

        _active_separator->start(_segment_phaser.start());

        // flush old separator in background
        if (old_sep) {
            (void)with_gate(_async_gate, [this, sep = std::move(old_sep)] mutable {
                return with_scheduling_group(_cfg.separator_sg, [this, sep = std::move(sep)] mutable {
                    return flush_separator(std::move(sep));
                });
            });
        }
    }

    if (old_seg) {
        // close old segment in background
        (void)with_gate(_async_gate, [this, old_seg] mutable {
            return close_segment(std::move(old_seg));
        });
    }

    _active_segment->start(_segment_phaser.start(), _active_separator->hold());
    _active_separator->add_segment(_active_segment->id());

    logstor_logger.trace("Switched active segment to {}, separator has {} segments",
            _active_segment->id(), _active_separator->segment_count());
}

future<> segment_manager_impl::do_barrier() {
    // The barrier forces switch of the active segment and separator and waits for
    // them and all previous segments and separators to be closed and flushed.
    logstor_logger.debug("Starting barrier operation");
    auto phaser_fut =  _segment_phaser.advance_and_await();

    if (_active_separator) {
        _active_separator->request_switch();
    }

    if (_switch_segment_fut) {
        co_await _switch_segment_fut->get_future();
    }
    co_await request_segment_switch();

    co_await std::move(phaser_fut);
    logstor_logger.debug("Barrier operation completed");
}

future<> segment_manager_impl::close_segment(seg_ptr seg) {
    co_await seg->stop();

    auto& desc = get_segment_descriptor(seg->id());
    if (auto gid = seg->get_group_id()) {
        desc.gid = *gid;
        _compaction_mgr.add_segment(desc);
    }
}

future<> segment_manager_impl::flush_separator(sep_ptr sep) {
    // wait for all segments to be closed and no writes to the separator.
    co_await sep->close();

    bool flushed = false;

    while (true) {
        try {
            _async_gate.check();
            co_await _compaction_mgr.flush_separator(*sep);
            flushed = true;
            break;
        } catch (abort_requested_exception&) {
            logstor_logger.debug("Separator flush aborted due to shutdown");
            break;
        } catch (gate_closed_exception&) {
            logstor_logger.debug("Separator flush aborted due to gate close");
            break;
        } catch (...) {
            if (_async_gate.is_closed()) {
                logstor_logger.debug("Separator flush aborted due to gate close");
                break;
            }
            logstor_logger.warn("Exception during separator flush: {}, retrying", std::current_exception());
        }
        co_await seastar::sleep(std::chrono::seconds(1));
    }

    if (!flushed) {
        co_await _compaction_mgr.abort_separator(*sep);
    }
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
    // Before freeing a segment, ensure there are no ongoing operations that use
    // locations in this segment. See for example `await_pending_reads`.

    get_segment_descriptor(segment_id).on_free_segment();

    // TODO write new generation?

    _free_segments.push_back(segment_id);
    _segment_freed_cv.signal();

    _stats.segments_freed++;
    _stats.segments_in_use--;

    co_return;
}

future<> segment_manager_impl::truncate_table(table_id table) {
    logstor_logger.info("Truncating table {}", table);

    auto holder = _async_gate.hold();

    // do a barrier to ensure all the table's data is flushed to segments
    // in the table's compaction groups. we assume there are no new writes
    // to the table by this point.
    co_await do_barrier();

    // stop and free the compaction groups of the table.
    auto segments = co_await _compaction_mgr.free_compaction_groups(table);

    // free all segments and remove their records from the index.
    for (auto seg_id : segments) {
        logstor_logger.debug("Freeing segment {} of truncated table {}", seg_id, table);
        co_await for_each_record(seg_id, [this] (log_location loc, log_record record) {
            _index.erase(record.key, loc);
            return make_ready_future<>();
        });

        co_await free_segment(seg_id);
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

bool compaction_manager::is_record_alive(const index_key& key, log_location loc) {
    return _index.get(key)
        .transform([loc] (const index_entry& e) { return e.location == loc; })
        .value_or(false);
}

bool compaction_manager::update_record_location(const index_key& key, log_location old_loc, log_location new_loc) {
    return _index.update_record_location(key, old_loc, new_loc);
}

std::vector<log_segment_id> compaction_manager::select_segments_for_compaction(const compaction_group& cg) {
    size_t accum_net_data_size = 0;
    size_t accum_record_count = 0;
    ssize_t max_gain = 0;
    size_t best_count = 0;
    std::vector<log_segment_id> candidates;
    const auto segment_size = _sm.get_segment_size();

    for (const auto& desc : cg.segment_hist) {
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

future<std::optional<std::pair<group_id, std::vector<log_segment_id>>>>
compaction_manager::select_segments_for_compaction() {
    if (_compaction_groups.empty()) {
        co_return std::nullopt;
    }

    if (_next_group_for_compaction == _compaction_groups.end()) {
        _next_group_for_compaction = _compaction_groups.begin();
    }

    auto start = _next_group_for_compaction;
    do {
        co_await coroutine::maybe_yield();

        auto& [gid, cg] = *_next_group_for_compaction;
        ++_next_group_for_compaction;

        if (cg.compaction_enabled) {
            auto candidates = select_segments_for_compaction(cg);
            if (candidates.size() > 0) {
                co_return std::make_pair(gid, std::move(candidates));
            }
        }

        if (_next_group_for_compaction == _compaction_groups.end()) {
            _next_group_for_compaction = _compaction_groups.begin();
        }
    } while (_next_group_for_compaction != start);

    co_return std::nullopt;
}

future<> compaction_manager::start_compaction() {
    if (!_cfg.compaction_enabled) {
        logstor_logger.debug("Compaction is disabled, skipping");
        co_return;
    }

    if (auto group_and_candidates = co_await select_segments_for_compaction()) {
        auto& [gid, candidates] = *group_and_candidates;
        logstor_logger.debug("Starting compaction for group {}", gid);

        auto holder = _async_gate.hold();
        co_await with_scheduling_group(_cfg.compaction_sg, [this, gid, candidates = std::move(candidates)] mutable {
            return compact_segments(gid, std::move(candidates));
        });
    }
}

future<> compaction_manager::compact_segments(group_id gid, std::vector<log_segment_id> segments) {
    struct compaction_buffer {
        segment_manager_impl& sm;
        write_buffer buf;
        group_id gid;
        std::vector<future<>> pending_updates;
        size_t flush_count{0};

        explicit compaction_buffer(segment_manager_impl& sm, size_t buffer_size, group_id gid)
            : sm(sm), buf(buffer_size, false), gid(gid) {}

        future<> flush() {
            if (buf.has_data()) {
                flush_count++;
                auto base_location = co_await sm.write_full_segment(buf, gid, write_source::compaction);
                co_await when_all_succeed(pending_updates.begin(), pending_updates.end());
                logstor_logger.trace("Compaction buffer flushed to {} with {} bytes", base_location, buf.get_net_data_size());
            }
            co_await buf.close();
            buf.reset();
            pending_updates.clear();
        }
    };

    compaction_buffer cb(_sm, _sm.get_segment_size(), gid);

    size_t records_rewritten = 0;
    size_t records_skipped = 0;

    co_await _sm.for_each_record(segments,
            [this, &records_rewritten, &records_skipped, &cb, gid]
            (log_location read_location, log_record record) -> future<> {

        if (!is_record_alive(record.key, read_location)) {
            records_skipped++;
            _stats.compaction_records_skipped++;
            co_return;
        }

        if (record.group != gid) {
            throw std::runtime_error(fmt::format("Record group {} doesn't match expected group {} during compaction",
                    record.group, gid));
        }

        auto key = record.key;
        log_record_writer writer(std::move(record));

        if (!cb.buf.can_fit(writer)) {
            co_await cb.flush();
        }

        // write the record and then update the index with the new location
        auto write_and_update_index = cb.buf.write_with_holder(std::move(writer)).then_unpack(
                [this, key = std::move(key), read_location, &records_rewritten, &records_skipped]
                (log_location new_location, seastar::gate::holder op) {

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

    // wait for read operations that use the old locations
    co_await _index.await_pending_reads();

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

    _controller.update(cb.flush_count, new_segments);
}

void compaction_manager::controller::update(size_t segment_write_count, size_t new_segments) {
    float new_overhead = static_cast<float>(segment_write_count) / std::max<size_t>(1, new_segments);
    _compaction_overhead = 0.8 * _compaction_overhead + 0.2 * new_overhead;
}

void compaction_manager::remove_segment(segment_descriptor& desc) {
    auto it = _compaction_groups.find(*desc.gid);
    if (it == _compaction_groups.end()) {
        on_internal_error(logstor_logger, fmt::format("Compaction group {} not found for segment {}", *desc.gid, desc.seg_gen));
    }

    auto& cg = it->second;
    cg.segment_hist.erase(desc);

    if (cg.segment_hist.empty()) {
        if (_next_group_for_compaction == it) {
            ++_next_group_for_compaction;
        }
        _compaction_groups.erase(it);
    }
}

future<std::pair<compaction_manager::compaction_group_it, std::vector<log_segment_id>>>
compaction_manager::remove_compaction_group(compaction_group_it it) {
    std::vector<log_segment_id> segments;
    auto& cg = it->second;
    auto& hist = cg.segment_hist;
    while (!hist.empty()) {
        co_await coroutine::maybe_yield();
        auto& desc = hist.one_of_largest();
        auto seg_id = _sm.desc_to_segment_id(desc);
        segments.push_back(seg_id);
        hist.erase(desc);
    }

    if (_next_group_for_compaction == it) {
        ++_next_group_for_compaction;
    }
    it = _compaction_groups.erase(it);
    co_return std::make_pair(it, std::move(segments));
}

future<std::vector<log_segment_id>> compaction_manager::free_compaction_groups(table_id table) {
    co_await disable_auto_compaction(table);

    std::vector<log_segment_id> segments_to_free;

    auto it = _compaction_groups.lower_bound(group_id{table, 0});
    while (it != _compaction_groups.end() && it->first.table == table) {
        co_await coroutine::maybe_yield();
        auto [next_it, segments] = co_await remove_compaction_group(it);
        segments_to_free.insert(segments_to_free.end(), segments.begin(), segments.end());
        it = next_it;
    }

    co_return std::move(segments_to_free);
}

separator::buffer& compaction_manager::get_separator_buffer(separator& sep, const log_record_writer& writer) {
    auto gid = writer.record().group;

    auto it = sep._group_buffers.find(gid);
    if (it == sep._group_buffers.end()) {
        it = sep._group_buffers.emplace(gid, std::deque<separator::buffer>{}).first;
    }

    auto& group_bufs = it->second;
    if (group_bufs.empty() || !group_bufs.back().buf->can_fit(writer)) {
        // TODO really ensure we have enough buffers
        if (_sm._available_separator_buffers.empty()) {
            throw std::runtime_error("No available separator buffers");
        }
        write_buffer* wb = _sm._available_separator_buffers.back();
        _sm._available_separator_buffers.pop_back();
        group_bufs.emplace_back(std::move(wb), gid, _sm._separator_debt);
        sep._buffer_count++;
    }

    auto& buf = group_bufs.back();
    if (!buf.buf->can_fit(writer)) {
        throw std::runtime_error(fmt::format("Record size {} exceeds compaction buffer size {}", writer.size(), buf.buf->get_max_write_size()));
    }

    return buf;
}

future<> compaction_manager::write_to_separator(separator& sep, write_buffer& wb, log_location base_location) {
    for (auto& w : wb.records()) {
        co_await coroutine::maybe_yield();

        auto key = w.writer.record().key;
        log_location prev_loc = wb.get_record_location(base_location, w);

        auto& buf = get_separator_buffer(sep, w.writer);
        auto f = buf.write(std::move(w.writer)).then_unpack(
            [this, key = std::move(key), prev_loc] (log_location new_loc, seastar::gate::holder op) {
                if (!update_record_location(key, prev_loc, new_loc)) {
                    _sm.free_record(new_loc);
                }
                return make_ready_future<>();
            }
        );
        buf.pending_updates.push_back(std::move(f));
    }
}

future<> compaction_manager::write_to_separator(separator& sep, log_segment_id seg_id) {
    co_await _sm.for_each_record(seg_id, [this, &sep] (log_location read_location, log_record record) -> future<> {
        if (!is_record_alive(record.key, read_location)) {
            co_return;
        }

        auto key = record.key;
        auto writer = log_record_writer(std::move(record));

        auto& buf = get_separator_buffer(sep, writer);
        auto f = buf.write(std::move(writer)).then_unpack(
            [this, key = std::move(key), prev_loc = read_location] (log_location new_loc, seastar::gate::holder op) {
                if (!update_record_location(key, prev_loc, new_loc)) {
                    _sm.free_record(new_loc);
                }
                return make_ready_future<>();
            }
        );
        buf.pending_updates.push_back(std::move(f));
    });
}

future<> compaction_manager::flush_separator(separator& sep) {
    logstor_logger.debug("Flushing separator with {} segments and {} buffers from {} groups",
            sep._segments.size(), sep._buffer_count, sep.group_count());

    for (auto&& [gid, bufs] : sep._group_buffers) {
        logstor_logger.trace("Flushing separator buffers for group {} with {} buffers from {} segments", gid, bufs.size(), sep._segments.size());
        while (!bufs.empty()) {
            if (auto& buf = bufs.front(); buf.buf->has_data()) {
                co_await _sm.write_full_segment(*buf.buf, buf.gid, write_source::separator);
                co_await when_all_succeed(buf.pending_updates.begin(), buf.pending_updates.end());
                _stats.separator_buffer_flushed++;
            }
            co_await bufs.front().buf->close();
            auto wb = std::move(bufs.front().buf);
            bufs.pop_front();

            wb->reset();
            _sm._available_separator_buffers.push_back(std::move(wb));
        }
    }

    // wait for read operations that use the old locations
    co_await _index.await_pending_reads();

    while (!sep._segments.empty()) {
        auto seg_id = sep._segments.front();
        sep._segments.pop_front();
        co_await _sm.free_segment(seg_id);
        _stats.separator_segments_freed++;
    }
}

future<> compaction_manager::abort_separator(separator& sep) {
    for (auto&& [gid, bufs] : sep._group_buffers) {
        while (!bufs.empty()) {
            auto& buf = bufs.front();
            co_await buf.buf->abort_writes(std::make_exception_ptr(abort_requested_exception()));
            for (auto&& f : buf.pending_updates) {
                try {
                    co_await std::move(f);
                } catch (...) {}
            }
            bufs.pop_front();
        }
    }
}

std::chrono::microseconds segment_manager_impl::calculate_separator_delay() const {
    size_t min_debt = _separator_flush_threshold * _cfg.segment_size;
    size_t debt_target = separator_debt_target * _separator_flush_threshold * _cfg.segment_size;
    size_t current_debt = (_separator_debt.count() > min_debt) ? (_separator_debt.count() - min_debt) : 0;
    float debt_ratio = float(current_debt) / debt_target;
    auto adjust = [] (float x) { return x * x * x; };
    return std::chrono::microseconds(size_t(adjust(debt_ratio) * _cfg.separator_delay_limit_ms * 1000));
}

future<> segment_manager_impl::do_recovery() {
    logstor_logger.info("Starting recovery for shard {} in directory {}", this_shard_id(), _cfg.base_dir.string());

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
            throw std::runtime_error(fmt::format("Missing log segment file(s) detected during recovery: file {} missing", _file_mgr.get_file_path(next_file_id)));
        }
        next_file_id++;
    }
    _file_mgr.recover_next_file_id(next_file_id);

    // populate index from all segments. keep the latest record for each key.
    for (auto file_id : found_file_ids) {
        co_await max_concurrent_for_each(segments_in_file(file_id), 32,
            [this] (log_segment_id seg_id) {
                return recover_segment(seg_id);
            }
        );
    }

    // go over the index and mark all segments that have live data as used.
    size_t allocated_segment_count = next_file_id * _segments_per_file;
    utils::dynamic_bitset used_segments(allocated_segment_count);
    for (const auto& entry : _index) {
        used_segments.set(entry.location.segment.value);
        co_await coroutine::maybe_yield();
    }

    // put used segments in the histogram, and put the rest in the free list.
    size_t free_segment_count = 0;
    size_t used_segment_count = 0;
    for (size_t seg_idx = 0; seg_idx < allocated_segment_count; ++seg_idx) {
        log_segment_id seg_id(seg_idx);
        auto& desc = get_segment_descriptor(seg_id);
        if (!used_segments.test(seg_idx)) {
            desc.on_free_segment();
            _free_segments.push_back(seg_id);
            free_segment_count++;
        } else {
            used_segment_count++;
            _stats.segments_in_use++;

            if (desc.gid) {
                logstor_logger.trace("Recovered segment {} with group id {}", seg_id, *desc.gid);
                _compaction_mgr.add_segment(desc);
            } else {
                logstor_logger.trace("Recovered segment {} with mixed groups", seg_id);
                if (!_active_separator) {
                    _active_separator = make_lw_shared<separator>(_separator_flush_threshold);
                    _active_separator->start(_segment_phaser.start());
                }
                _active_separator->add_segment(seg_id);
                co_await with_scheduling_group(_cfg.separator_sg, [this, seg_id] {
                     return _compaction_mgr.write_to_separator(*_active_separator, seg_id);
                });
            }
        }
        co_await coroutine::maybe_yield();
    }

    if (_active_separator) {
        _active_separator->request_switch();
    }

    _next_new_segment_id = allocated_segment_count;

    logstor_logger.trace("Recovery: found {} used segments and {} free segments",
                        used_segment_count, free_segment_count);

    logstor_logger.info("Recovery complete");
}

future<> segment_manager_impl::recover_segment(log_segment_id segment_id) {
    auto& desc = get_segment_descriptor(segment_id);
    desc.reset(_cfg.segment_size);

    auto seg_gen_opt = co_await recover_segment_generation(segment_id);
    if (!seg_gen_opt) {
        co_return;
    }
    desc.seg_gen = *seg_gen_opt;

    struct mixed_groups {};
    std::optional<std::variant<group_id, mixed_groups>> seg_group;

    co_await for_each_record(segment_id, [this, &desc, &seg_group] (log_location loc, log_record record) -> future<> {
        logstor_logger.trace("Recovery: read record at {} gen {}", loc, record.generation);

        if (!seg_group) {
            seg_group = record.group;
        } else if (std::holds_alternative<group_id>(*seg_group) && std::get<group_id>(*seg_group) != record.group) {
            seg_group = mixed_groups{};
        }

        index_entry new_entry {
            .location = loc,
            .generation = record.generation
        };

        if (auto [inserted, prev_entry] = _index.insert_if_newer(record.key, new_entry); inserted) {
            desc.on_write(loc);
            if (prev_entry) {
                get_segment_descriptor(prev_entry->location).on_free(prev_entry->location);
            }
        } else {
            desc.on_free(loc);
        }

        co_return;
    });

    if (std::holds_alternative<group_id>(*seg_group)) {
        desc.gid = std::get<group_id>(*seg_group);
    }
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

void segment_manager::enable_auto_compaction(table_id tid) {
    _impl->enable_auto_compaction(tid);
}

future<> segment_manager::disable_auto_compaction() {
    return _impl->disable_auto_compaction();
}

future<> segment_manager::disable_auto_compaction(table_id tid) {
    return _impl->disable_auto_compaction(tid);
}

future<> segment_manager::trigger_compaction(bool major) {
    return _impl->trigger_compaction(major);
}

size_t segment_manager::get_segment_size() const noexcept {
    return _impl->get_segment_size();
}

future<> segment_manager::do_barrier() {
    return _impl->do_barrier();
}

future<> segment_manager::truncate_table(table_id table) {
    return _impl->truncate_table(table);
}

}

template<>
size_t hist_key<replica::logstor::segment_descriptor>(const replica::logstor::segment_descriptor& desc) {
    return desc.free_space;
}
