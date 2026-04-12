/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
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

    future<> do_write(log_location , bytes_view data);

public:
    using segment::segment;

    void start(segment_ref seg_ref);

    future<> stop();

    // write a serialized sequence of records.
    // must not be called concurrently.
    future<log_location> append(bytes_view data);

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
        return ser::deserialize_from_buffer(buf, std::type_identity<log_record>{});
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
}

future<log_location> writeable_segment::append(bytes_view data) {
    auto data_size = data.size();

    if (!can_fit(data_size)) {
        throw std::runtime_error("Entry too large for remaining segment space");
    }

    log_location loc {
        .segment = _id,
        .offset = _current_offset,
        .size = static_cast<uint32_t>(data_size)
    };

    co_await do_write(loc, data);
    _current_offset += data_size;
    co_return loc;
}

future<> writeable_segment::do_write(log_location loc, bytes_view data) {
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
        int compaction_disabled_counter{0};
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

    separator_buffer allocate_separator_buffer() override;
    future<> flush_separator_buffer(separator_buffer buf, compaction_group&) override;

    void submit(compaction_group&) override;
    future<> stop_ongoing_compactions(compaction_group&) override;
    future<compaction_reenabler> disable_compaction(replica::compaction_group&) override;
    compaction_reenabler disable_compaction_no_wait(replica::compaction_group&) override;
    future<> split_compaction(replica::table&, compaction_group&, mutation_writer::classify_by_token_group) override;

private:

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
    streaming,
};

static constexpr size_t write_source_count = 4;

static sstring write_source_to_string(write_source src) {
    switch (src) {
        case write_source::normal_write: return "normal_write";
        case write_source::compaction: return "compaction";
        case write_source::separator: return "separator";
        case write_source::streaming: return "streaming";
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
        std::array<uint64_t, write_source_count> segments_get{0};
        uint64_t normal_segments_wait{0};
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
        if (src == write_source::compaction) {
            _stats.segments_get[static_cast<size_t>(src)]++;
            co_return co_await _segments.pop_eventually();
        }
        if (_segments.size() <= _reserved_for_compaction) {
            if (src == write_source::normal_write) {
                _stats.normal_segments_wait++;
            }
            while (_segments.size() <= _reserved_for_compaction) {
                co_await _segment_available.wait([this] {
                    return _segments.size() > _reserved_for_compaction;
                });
            }
        }
        _stats.segments_get[static_cast<size_t>(src)]++;
        co_return _segments.pop();
    }

    size_t size() const noexcept {
        return _segments.size();
    }

    const stats& get_stats() const noexcept {
        return _stats;
    }
};

struct segment_header {
    segment_kind kind;
    segment_generation seg_gen;

    struct mixed {};
    struct full {
        table_id table;
        dht::token first_token;
        dht::token last_token;
    };

    std::variant<mixed, full> v;
};

static segment_header make_segment_header(const write_buffer::buffer_header& bh, std::optional<write_buffer::segment_header> sh) {
    segment_header seg_hdr {
        .kind = bh.kind,
        .seg_gen = bh.seg_gen,
    };

    switch (bh.kind) {
    case segment_kind::full:
        seg_hdr.v = segment_header::full {
            .table = sh->table,
            .first_token = sh->first_token,
            .last_token = sh->last_token,
        };
        break;
    case segment_kind::mixed:
        seg_hdr.v = segment_header::mixed{};
        break;
    }
    return seg_hdr;
}

class segment_manager_impl {

    struct stats {
        uint64_t segments_in_use{0};
        std::array<uint64_t, write_source_count> bytes_written{0};
        std::array<uint64_t, write_source_count> data_bytes_written{0};
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
    seastar::semaphore _active_segment_write_sem{1};
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

    future<> write(write_buffer&);
    future<> write_full_segment(write_buffer&, compaction_group&, write_source);

    future<log_record> read(log_location);

    void free_record(log_location);

    future<> for_each_record(log_segment_id,
                            std::function<future<>(const segment_header&)>,
                            std::function<future<>(log_location, log_record)>);

    future<> for_each_record(log_segment_id,
                            std::function<future<>(log_location, log_record)>);

    future<> for_each_record(const std::vector<log_segment_id>&,
                            std::function<future<>(log_location, log_record)>);

    future<> load_segment(replica::database&, log_segment_id);
    future<> recover_segment(replica::database&, log_segment_id);
    future<std::optional<segment_header>> read_segment_header(log_segment_id);
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
        return _cfg.max_separator_memory + sizeof(_segment_descs);
    }

    future<> await_pending_writes() {
        return _writes_phaser.advance_and_await();
    }

    future<utils::chunked_vector<segment_snapshot>> make_snapshot(compaction_group&);

    future<seastar::input_stream<char>> create_segment_input_stream(log_segment_id segment_id, const seastar::file_input_stream_options& opts);
    future<std::unique_ptr<segment_stream_sink>> create_segment_output_stream(replica::database&);

private:

    future<> replenish_reserve();
    future<seg_ptr> allocate_segment();

    future<> request_segment_switch();
    future<> switch_active_segment();
    std::chrono::microseconds calculate_separator_delay() const;

    future<> write_to_separator(write_buffer&, segment_ref, size_t segment_seq_num);
    void write_to_separator(table&, log_location prev_loc, log_record, segment_ref);

    segment_ref make_segment_ref(log_segment_id seg_id) {
        auto& desc = get_segment_descriptor(seg_id);
        ++desc.ref_count;

        return segment_ref(seg_id,
            [this, seg_id] {
                auto& desc = get_segment_descriptor(seg_id);
                if (--desc.ref_count == 0) {
                    return free_segment(seg_id);
                }
            },
            [seg_id] {
                logstor_logger.warn("Segment {} can't be freed", seg_id);
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
    friend struct compaction_buffer;
    friend class segment_stream_sink_impl;
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
    // at most a single compaction/split running at a time
    // and at most two buffers used at a time by split.
    size_t compaction_buffer_count = 2;
    _available_compaction_buffers.reserve(compaction_buffer_count);
    _compaction_buffer_pool.reserve(compaction_buffer_count);
    for (size_t i = 0; i < compaction_buffer_count; ++i) {
        _compaction_buffer_pool.emplace_back(config.segment_size, segment_kind::full);
        _available_compaction_buffers.push_back(&_compaction_buffer_pool.back());
    }

    // pre-allocate write buffers for separator
    size_t separator_buffer_count = _cfg.max_separator_memory / _cfg.segment_size;
    _available_separator_buffers.reserve(separator_buffer_count);
    _separator_buffer_pool.reserve(separator_buffer_count);
    for (size_t i = 0; i < separator_buffer_count; ++i) {
        _separator_buffer_pool.emplace_back(config.segment_size, segment_kind::full);
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
        sm::make_counter("segment_pool_normal_segments_get", _segment_pool.get_stats().segments_get[static_cast<size_t>(write_source::normal_write)],
                       sm::description("Counts number of segments taken from the segment pool for normal writes.")),
        sm::make_counter("segment_pool_compaction_segments_get", _segment_pool.get_stats().segments_get[static_cast<size_t>(write_source::compaction)],
                       sm::description("Counts number of segments taken from the segment pool for compaction.")),
        sm::make_counter("segment_pool_separator_segments_get", _segment_pool.get_stats().segments_get[static_cast<size_t>(write_source::separator)],
                       sm::description("Counts number of segments taken from the segment pool for separator writes.")),
        sm::make_counter("segment_pool_normal_segments_wait", _segment_pool.get_stats().normal_segments_wait,
                       sm::description("Counts number of times normal writes had to wait for a segment to become available in the segment pool.")),
        sm::make_counter("bytes_written", _stats.bytes_written[static_cast<size_t>(write_source::normal_write)],
                       sm::description("Counts number of bytes written to the disk.")),
        sm::make_counter("data_bytes_written", _stats.data_bytes_written[static_cast<size_t>(write_source::normal_write)],
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
        sm::make_counter("compaction_bytes_written", _stats.bytes_written[static_cast<size_t>(write_source::compaction)],
                       sm::description("Counts number of bytes written to the disk by compaction.")),
        sm::make_counter("compaction_data_bytes_written", _stats.data_bytes_written[static_cast<size_t>(write_source::compaction)],
                       sm::description("Counts number of data bytes written to the disk by compaction.")),
        sm::make_counter("segments_compacted", _compaction_mgr.get_stats().segments_compacted,
                       sm::description("Counts number of segments compacted.")),
        sm::make_counter("compaction_segments_freed", _compaction_mgr.get_stats().compaction_segments_freed,
                       sm::description("Counts number of segments freed by compaction.")),
        sm::make_counter("compaction_records_skipped", _compaction_mgr.get_stats().compaction_records_skipped,
                       sm::description("Counts number of records skipped during compaction.")),
        sm::make_counter("compaction_records_rewritten", _compaction_mgr.get_stats().compaction_records_rewritten,
                       sm::description("Counts number of records rewritten during compaction.")),
        sm::make_counter("separator_bytes_written", _stats.bytes_written[static_cast<size_t>(write_source::separator)],
                       sm::description("Counts number of bytes written to the separator.")),
        sm::make_counter("separator_data_bytes_written", _stats.data_bytes_written[static_cast<size_t>(write_source::separator)],
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

future<> segment_manager_impl::write(write_buffer& wb) {
    write_source source = write_source::normal_write;
    auto holder = _async_gate.hold();

    wb.finalize(block_alignment);

    bytes_view data(reinterpret_cast<const int8_t*>(wb.data()), wb.offset_in_buffer());

    if (data.size() > _cfg.segment_size) {
        throw std::runtime_error(fmt::format( "Write size {} exceeds segment size {}", data.size(), _cfg.segment_size));
    }

    {
        auto sem_units = co_await get_units(_active_segment_write_sem, 1);

        while (!_active_segment || !_active_segment->can_fit(data.size())) {
            co_await request_segment_switch();
        }

        seg_ptr seg = _active_segment;
        auto seg_holder = seg->hold();
        auto seg_ref = seg->ref();
        auto segment_seq_num = _segment_seq_num;
        auto write_op = _writes_phaser.start();
        auto& desc = get_segment_descriptor(seg->id());

        // if we wrote a record to the segment but failed to write it to the separator, the segment should not be freed.
        auto write_to_separator_failed = defer([seg_ref] mutable {
            seg_ref.set_flush_failure();
        });

        wb.write_header(desc.seg_gen, std::nullopt);

        auto loc = co_await seg->append(data);
        sem_units.return_all();

        desc.on_write(wb.get_net_data_size(), wb.get_record_count());

        _stats.bytes_written[static_cast<size_t>(source)] += data.size();
        _stats.data_bytes_written[static_cast<size_t>(source)] += wb.get_net_data_size();

        // complete all buffered writes with their individual locations and wait
        // for them to be updated in the index.
        co_await wb.complete_writes(loc);

        co_await with_scheduling_group(_cfg.separator_sg, [&] {
            return write_to_separator(wb, std::move(seg_ref), segment_seq_num);
        });
        write_to_separator_failed.cancel();
    }

    // flow control for separator debt
    if (auto separator_delay = calculate_separator_delay(); separator_delay.count() > 0) {
        co_await seastar::sleep(separator_delay);
    }
}

future<> segment_manager_impl::write_full_segment(write_buffer& wb, compaction_group& cg, write_source source) {
    auto holder = _async_gate.hold();

    wb.finalize(block_alignment);

    bytes_view data(reinterpret_cast<const int8_t*>(wb.data()), wb.offset_in_buffer());

    if (data.size() > _cfg.segment_size) {
        throw std::runtime_error(fmt::format("Write size {} exceeds segment size {}", data.size(), _cfg.segment_size));
    }

    seg_ptr seg = co_await _segment_pool.get_segment(source);
    auto& desc = get_segment_descriptor(seg->id());

    _stats.segments_in_use++;
    logstor_logger.trace("Write full segment {} from {}", seg->id(), write_source_to_string(source));

    wb.write_header(desc.seg_gen, cg.schema()->id());

    auto loc = co_await seg->append(data);

    desc.on_write(wb.get_net_data_size(), wb.get_record_count());

    _stats.bytes_written[static_cast<size_t>(source)] += data.size();
    _stats.data_bytes_written[static_cast<size_t>(source)] += wb.get_net_data_size();

    co_await wb.complete_writes(loc);
    co_await seg->stop();

    cg.add_logstor_segment(desc);
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
            auto seg = co_await allocate_segment();
            co_await _segment_pool.put(std::move(seg));
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

future<seg_ptr> segment_manager_impl::allocate_segment() {
    auto make_segment = [this] (log_segment_id seg_id) -> future<seg_ptr> {
        try {
            auto seg_loc = segment_id_to_file_location(seg_id);
            auto file = co_await _file_mgr.get_file_for_write(seg_loc.file_id);
            auto seg = make_lw_shared<writeable_segment>(seg_id, std::move(file), seg_loc.file_offset, _cfg.segment_size);
            get_segment_descriptor(seg_id).reset(_cfg.segment_size);
            _stats.segments_allocated++;
            co_return std::move(seg);
        } catch (...) {
            _free_segments.push_back(seg_id);
            _segment_freed_cv.signal();
            throw;
        }
    };

    while (true) {
        // first, allocate all new segments sequentially
        if (_next_new_segment_id < _max_segments) {
            auto seg_id = log_segment_id(_next_new_segment_id++);
            co_return co_await make_segment(seg_id);
        }

        if (_free_segments.size() < _max_segments * trigger_compaction_threshold / 100) {
            trigger_compaction();
        }

        // reuse freed segments
        if (!_free_segments.empty()) {
            auto seg_id = _free_segments.front();
            _free_segments.pop_front();
            co_return co_await make_segment(seg_id);
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
        on_internal_error(logstor_logger, format("Freeing segment {} that has data", segment_id));
    }
    if (desc.ref_count != 0) {
        on_internal_error(logstor_logger, format("Freeing segment {} with non-zero reference count", segment_id));
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

    std::vector<log_segment_id> segments;
    segments.reserve(ss.segment_count());
    while (!ss._segments.empty()) {
        co_await coroutine::maybe_yield();
        auto& desc = ss._segments.one_of_largest();
        auto seg_id = desc_to_segment_id(desc);
        ss.remove_segment(desc);
        if (desc.ref_count != 0) {
            on_internal_error(logstor_logger, format("Discarding segment {} with non-zero reference count", seg_id));
        }

        // the index should be cleared before discarding segments, so no data should be reachable
        desc.reset(_cfg.segment_size);

        segments.push_back(seg_id);
    }

    co_await max_concurrent_for_each(segments, 32, [this] (log_segment_id seg_id) -> future<> {
        // Write a valid empty segment header with the next generation.
        // This marks the segment as discarded while preserving the generation counter.
        auto next_gen = get_segment_descriptor(seg_id).seg_gen;
        ++next_gen;
        auto buf = allocate_aligned_buffer<char>(block_alignment, 4096);
        std::memset(buf.get(), 0, block_alignment);
        simple_memory_output_stream out(buf.get(), write_buffer::buffer_header_size);
        write_buffer::write_empty_header(out, next_gen);

        auto [file_id, file_offset] = segment_id_to_file_location(seg_id);
        auto file = co_await _file_mgr.get_file_for_write(file_id);
        co_await file.dma_write(file_offset, buf.get(), block_alignment);

        logstor_logger.trace("Discard segment {} next gen {}", seg_id, next_gen);

        free_segment(seg_id);
    });
}

future<> segment_manager_impl::for_each_record(log_segment_id segment_id,
                                std::function<future<>(const segment_header&)> header_callback,
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
        auto bh = ser::deserialize_from_buffer(buffer_header_buf, std::type_identity<write_buffer::buffer_header>{});

        // if the buffer is invalid then skip the rest of the segment - buffer writes are sequential and serialized.
        if (!write_buffer::validate_header(bh)) {
            break;
        }

        if (bh.seg_gen != seg_gen) {
            break;
        }

        std::optional<write_buffer::segment_header> sh;
        if (bh.kind == segment_kind::full) {
            // read segment header
            auto segment_header_buf = co_await fin.read_exactly(write_buffer::segment_header_size);
            current_position += write_buffer::segment_header_size;
            if (segment_header_buf.size() < write_buffer::segment_header_size) {
                break;
            }
            sh = ser::deserialize_from_buffer(segment_header_buf, std::type_identity<write_buffer::segment_header>{});
        }

        auto seg_hdr = make_segment_header(bh, sh);
        co_await header_callback(seg_hdr);

        // TODO crc, torn writes

        const auto buffer_data_end_position = current_position + bh.data_size;
        while (current_position < buffer_data_end_position) {
            // Read record header
            auto size_buf = co_await fin.read_exactly(write_buffer::record_header_size);
            current_position += write_buffer::record_header_size;
            if (size_buf.size() < write_buffer::record_header_size) {
                break;
            }
            auto rh = ser::deserialize_from_buffer(size_buf, std::type_identity<write_buffer::record_header>{});
            if (rh.data_size == 0 || rh.data_size > _cfg.segment_size) {
                // invalid record size
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

            auto record = ser::deserialize_from_buffer(record_buf, std::type_identity<log_record>{});

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

        if (seg_hdr.kind == segment_kind::full) {
            // A segment of this kind has only a single buffer
            break;
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

future<> segment_manager_impl::for_each_record(log_segment_id segment_id,
                                std::function<future<>(log_location, log_record)> callback) {
    return for_each_record(segment_id,
        [] (const segment_header&) { return make_ready_future<>(); },
        std::move(callback));
}

future<> segment_manager_impl::for_each_record(const std::vector<log_segment_id>& segments,
                                        std::function<future<>(log_location, log_record)> callback) {
    for (auto segment_id : segments) {
        co_await for_each_record(segment_id, callback);
    }
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
    if (state.running || state.compaction_disabled_counter > 0) {
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

future<compaction_reenabler> compaction_manager_impl::disable_compaction(compaction_group& cg) {
    auto& state_ptr = _groups[&cg];
    if (!state_ptr) {
        state_ptr = std::make_unique<group_compaction_state>();
    }
    auto& state = *state_ptr;

    ++state.compaction_disabled_counter;

    // Wait for any ongoing compaction to finish before disabling
    co_await state.completion.get_future();

    co_return compaction_reenabler([this, &cg] {
        auto it = _groups.find(&cg);
        if (it != _groups.end()) {
            --it->second->compaction_disabled_counter;
        }
    });
}

compaction_reenabler compaction_manager_impl::disable_compaction_no_wait(compaction_group& cg) {
    auto& state_ptr = _groups[&cg];
    if (!state_ptr) {
        state_ptr = std::make_unique<group_compaction_state>();
    }
    auto& state = *state_ptr;

    ++state.compaction_disabled_counter;

    return compaction_reenabler([this, &cg] {
        auto it = _groups.find(&cg);
        if (it != _groups.end()) {
            --it->second->compaction_disabled_counter;
        }
    });
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

// A single buffer used by compaction for rewriting records into new segments in a single compaction group.
// `rewrite_record` append a record to the buffer and given it's current location, and
// when the buffer is flushed it updates the index with the new location.
// the buffer is flushed when the next record doesn't fit and on close().
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

    compaction_buffer(compaction_buffer&& o) noexcept
        : sm(o.sm), buf(std::exchange(o.buf, nullptr)), cg(o.cg)
        , pending_updates(std::move(o.pending_updates)), flush_count(o.flush_count) {}

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
            co_await sm.write_full_segment(*buf, cg, write_source::compaction);
            logstor_logger.trace("Compaction buffer flushed with {} bytes", buf->get_net_data_size());
        }
        co_await when_all_succeed(pending_updates.begin(), pending_updates.end());
        co_await buf->close();
        buf->reset();
        pending_updates.clear();
    }

    future<> close() {
        co_await flush();
        sm._available_compaction_buffers.push_back(buf);
        buf = nullptr;
    }

    // Rewrite a single live record into this buffer, updating the index atomically.
    // Returns immediately after queuing the write; caller must co_await close()/flush()
    // to ensure all pending updates complete.
    future<> rewrite_record(primary_index& index, log_location read_location, log_record record,
                             size_t& records_rewritten, size_t& records_skipped) {
        if (!index.is_record_alive(record.key, read_location)) {
            records_skipped++;
            co_return;
        }

        auto key = record.key;
        log_record_writer writer(std::move(record));

        if (!buf->can_fit(writer)) {
            co_await flush();
        }

        auto write_and_update_index = buf->write(std::move(writer)).then_unpack(
                [this, &index, key = std::move(key), read_location, &records_rewritten, &records_skipped]
                (log_location new_location, seastar::gate::holder op) {

            if (index.update_record_location(key, read_location, new_location)) {
                sm.free_record(read_location);
                records_rewritten++;
            } else {
                // another write updated this key
                sm.free_record(new_location);
                records_skipped++;
            }
        });

        pending_updates.push_back(std::move(write_and_update_index));
    }
};

future<> compaction_manager_impl::compact_segments(compaction_group& cg, std::vector<log_segment_id> segments) {
    logstor_logger.trace("Starting compaction of segments {} in compaction group {}:{}", segments, cg.schema()->id(), cg.group_id());

    compaction_buffer cb(_sm, cg);

    size_t records_rewritten = 0;
    size_t records_skipped = 0;

    auto& index = cg.get_logstor_index();

    co_await _sm.for_each_record(segments,
            [&index, &records_rewritten, &records_skipped, &cb] (log_location read_location, log_record record) -> future<> {
        co_await cb.rewrite_record(index, read_location, std::move(record), records_rewritten, records_skipped);
    });

    co_await cb.close();

    logstor_logger.debug("Compaction complete: {} records rewritten, {} skipped from {} segments, flushed {} times",
                       records_rewritten, records_skipped, segments.size(), cb.flush_count);

    // wait for read operations that use the old locations
    co_await index.await_pending_reads();

    // Free the compacted segments
    auto& ss = cg.logstor_segments();
    for (auto seg_id : segments) {
        logstor_logger.trace("Free segment {} by compaction", seg_id);
        auto& desc = _sm.get_segment_descriptor(seg_id);
        ss.remove_segment(desc);
        if (desc.ref_count == 0) {
            _sm.free_segment(seg_id);
        }
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

future<> compaction_manager_impl::split_compaction(replica::table& t, compaction_group& src, mutation_writer::classify_by_token_group classifier) {
    static constexpr size_t batch_size = 32;

    // Disable compaction on src for the duration of the split.
    // This waits for any running compaction on src to finish first.
    auto compaction_disable_guard = co_await disable_compaction(src);

    auto& src_segments = src.logstor_segments();
    auto& index = t.logstor_index();

    while (!src_segments._segments.empty()) {
        // Collect candidate IDs without yielding to avoid iterator invalidation.
        std::vector<log_segment_id> candidates;
        candidates.reserve(batch_size);
        for (const auto& cand_desc : src_segments._segments) {
            candidates.push_back(_sm.desc_to_segment_id(cand_desc));
            if (candidates.size() == batch_size) {
                break;
            }
        }

        // For each candidate, check whether it already belongs to a single group (fast path)
        // or straddles the split boundary (slow path).
        // Fast-path segments are moved to the correct child group immediately.
        // Slow-path segments are collected into a batch for rewriting.
        std::vector<log_segment_id> batch;
        batch.reserve(candidates.size());
        for (auto cand_seg_id : candidates) {
            auto cand_hdr = co_await _sm.read_segment_header(cand_seg_id);
            if (!cand_hdr || !std::holds_alternative<segment_header::full>(cand_hdr->v)) {
                on_internal_error(logstor_logger, format("Invalid segment header for segment {} during split compaction", cand_seg_id));
            }
            auto& cand_seg_hdr = std::get<segment_header::full>(cand_hdr->v);
            if (classifier(cand_seg_hdr.first_token) == classifier(cand_seg_hdr.last_token)) {
                // Fast path: segment already belongs to a single group.
                // Remove from src and add to the correct child group.
                logstor_logger.trace("Fast path split segment {} with token range [{}, {}]", cand_seg_id, cand_seg_hdr.first_token, cand_seg_hdr.last_token);
                auto& cand_desc = _sm.get_segment_descriptor(cand_seg_id);
                src_segments.remove_segment(cand_desc);
                if (!t.add_logstor_segment(cand_seg_id, cand_desc, cand_seg_hdr.first_token, cand_seg_hdr.last_token) || cand_desc.owner == &src_segments) {
                    on_internal_error(logstor_logger, format("Failed to add segment {} to table {} during split", cand_seg_id, t.schema()->id()));
                }
            } else {
                batch.push_back(cand_seg_id);
            }
        }

        if (batch.empty()) {
            continue;
        }

        // Slow path: rewrite live records from straddling segments into two compaction_buffers
        // (one per target group). Both buffers write back into src; the next outer loop
        // iteration will fast-path the resulting single-group segments to the correct child group.

        // Acquire _compaction_sem to be mutually exclusive with background compaction.
        auto sem_units = co_await get_units(_compaction_sem, 1);

        std::array<compaction_buffer, 2> bufs{compaction_buffer{_sm, src}, compaction_buffer{_sm, src}};

        size_t records_rewritten = 0;
        size_t records_skipped = 0;

        co_await _sm.for_each_record(batch,
                [&index, &classifier, &bufs, &records_rewritten, &records_skipped] (log_location read_location, log_record record) -> future<> {
            auto& cb = bufs[classifier(record.key.dk.token())];
            co_await cb.rewrite_record(index, read_location, std::move(record), records_rewritten, records_skipped);
        });

        for (auto& cb : bufs) {
            co_await cb.close();
        }

        logstor_logger.debug("Split compaction: {} records rewritten, {} skipped from {} segments",
                             records_rewritten, records_skipped, batch.size());

        // All records are safely written to new segments in src.
        // Await pending reads before freeing the source segments.
        co_await index.await_pending_reads();

        // Remove and free the source segments of this batch.
        for (auto seg_id : batch) {
            logstor_logger.trace("Free segment {} by split", seg_id);
            auto& desc = _sm.get_segment_descriptor(seg_id);
            src_segments.remove_segment(desc);
            if (desc.ref_count == 0) {
                _sm.free_segment(seg_id);
            }
        }
    }
}

separator_buffer compaction_manager_impl::allocate_separator_buffer() {
    if (_sm._available_separator_buffers.empty()) {
        throw std::runtime_error("No available separator buffers");
    }
    write_buffer* wb = _sm._available_separator_buffers.back();
    _sm._available_separator_buffers.pop_back();

    return separator_buffer(wb);
}

future<> segment_manager_impl::write_to_separator(write_buffer& wb, segment_ref seg_ref, size_t segment_seq_num) {
    for (auto&& w : wb.records()) {
        co_await coroutine::maybe_yield();

        auto key = w.writer.record().key;
        log_location prev_loc = co_await std::move(w.loc);

        auto* index_ptr = &w.cg->get_logstor_index();
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
            buf.write(std::move(w.writer)).then_unpack([this, index_ptr, key = std::move(key), prev_loc] (log_location new_loc, seastar::gate::holder op) {
                if (index_ptr->update_record_location(key, prev_loc, new_loc)) {
                    free_record(prev_loc);
                } else {
                    free_record(new_loc);
                }
            }
        ));
    }
}

void segment_manager_impl::write_to_separator(table& t, log_location prev_loc, log_record record, segment_ref seg_ref) {
    auto key = record.key;
    auto* index_ptr = &t.logstor_index();
    log_record_writer writer(std::move(record));

    auto& buf = t.get_logstor_separator_buffer(key.dk.token(), writer.size());

    if (buf.held_segments.empty() || buf.held_segments.back().id() != seg_ref.id()) {
        buf.held_segments.push_back(seg_ref);
    }

    buf.pending_updates.push_back(
        buf.write(std::move(writer)).then_unpack([this, index_ptr, key = std::move(key), prev_loc] (log_location new_loc, seastar::gate::holder op) {
            if (index_ptr->update_record_location(key, prev_loc, new_loc)) {
                free_record(prev_loc);
            } else {
                free_record(new_loc);
            }
        })
    );
}

future<> compaction_manager_impl::flush_separator_buffer(separator_buffer buf, compaction_group& cg) {
    logstor_logger.trace("Flushing separator buffer with {} bytes", buf.buf->offset_in_buffer());

    utils::get_local_injector().inject("fail_flush_separator_buffer", []() {
        throw std::runtime_error("flush_separator_buffer failed by injection");
    });

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
        logstor_logger.info("Table {}.{} has {} entries in logstor index", tp->schema()->ks_name(), tp->schema()->cf_name(), tp->logstor_index().get_key_count());
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

    auto seg_hdr = co_await read_segment_header(segment_id);
    if (!seg_hdr) {
        logstor_logger.trace("Segment {} has invalid header, skipping", segment_id);
        co_return;
    }
    desc.seg_gen = seg_hdr->seg_gen;
    bool is_full_segment = seg_hdr->kind == segment_kind::full;
    logstor_logger.trace("Recovering segment {} with generation {}", segment_id, desc.seg_gen);

    co_await for_each_record(segment_id, [this, &desc, &db, is_full_segment] (log_location loc, log_record record) -> future<> {
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
            auto [inserted, prev_entry] = t.logstor_index().insert_if_newer(record.key, new_entry, is_full_segment);
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

future<std::optional<segment_header>> segment_manager_impl::read_segment_header(log_segment_id segment_id) {
    auto [file_id, file_offset] = segment_id_to_file_location(segment_id);
    auto file = co_await _file_mgr.get_file_for_read(file_id);

    auto max_header_size = write_buffer::buffer_header_size + write_buffer::segment_header_size;
    auto header_buf = co_await file.dma_read_exactly<char>(file_offset, max_header_size);

    if (header_buf.size() < max_header_size) {
        co_return std::nullopt;
    }

    simple_memory_input_stream bh_stream(header_buf.begin(), write_buffer::buffer_header_size);
    auto bh = ser::deserialize(bh_stream, std::type_identity<write_buffer::buffer_header>{});

    if (!write_buffer::validate_header(bh)) {
        co_return std::nullopt;
    }

    std::optional<write_buffer::segment_header> sh;
    if (bh.kind == segment_kind::full) {
        simple_memory_input_stream sh_stream(header_buf.begin() + write_buffer::buffer_header_size, write_buffer::segment_header_size);
        sh = ser::deserialize(sh_stream, std::type_identity<write_buffer::segment_header>{});
    }

    co_return make_segment_header(bh, sh);
}

future<> segment_manager_impl::add_segment_to_compaction_group(replica::database& db, segment_descriptor& desc) {
    auto seg_id = desc_to_segment_id(desc);
    auto maybe_header = co_await read_segment_header(seg_id);
    if (!maybe_header) {
        co_return;
    }
    auto& header = *maybe_header;

    bool need_separator = false;

    if (header.kind == segment_kind::mixed) {
        logstor_logger.debug("Recovering mixed segment {} using separator", seg_id);
        need_separator = true;
    } else {
        auto& seg_header = std::get<segment_header::full>(header.v);
        try {
            auto& t = db.find_column_family(seg_header.table);
            if (t.add_logstor_segment(seg_id, desc, seg_header.first_token, seg_header.last_token)) {
                // all record belong to a single compaction group and the segment was added to the compaction group
                logstor_logger.debug("Add segment {} with tokens [{},{}] to table", seg_id, seg_header.first_token, seg_header.last_token);
            } else {
                // the record belong to different compaction groups - write to separator
                logstor_logger.debug("Add segment {} with tokens [{},{}] to separator", seg_id, seg_header.first_token, seg_header.last_token);
                need_separator = true;
            }
        } catch(const replica::no_such_column_family&) {
            co_return;
        }
    }

    if (need_separator) {
        auto seg_ref = make_segment_ref(seg_id);
        auto write_to_separator_failed = defer([seg_ref] mutable {
            seg_ref.set_flush_failure();
        });
        co_await for_each_record(seg_id, [this, seg_ref, &db] (log_location prev_loc, log_record record) -> future<> {
            try {
                auto& t = db.find_column_family(record.table);
                if (t.uses_logstor() && t.logstor_index().is_record_alive(record.key, prev_loc)) {
                    write_to_separator(t, prev_loc, std::move(record), seg_ref);
                }
            } catch(const replica::no_such_column_family&) {
                // ignore record
            }
            return make_ready_future<>();
        });
        write_to_separator_failed.cancel();
    }
}

future<utils::chunked_vector<segment_snapshot>> segment_manager_impl::make_snapshot(compaction_group& cg) {
    auto& segments = cg.logstor_segments();

    utils::chunked_vector<segment_snapshot> snp;
    snp.reserve(segments.segment_count());
    for (auto& desc : segments._segments) {
        auto seg_id = desc_to_segment_id(desc);
        snp.push_back(segment_snapshot{
            .segment_id = seg_id,
            .seg_ref = make_segment_ref(seg_id),
            .source = [this, seg_id] (const file_input_stream_options& opts) {
                return create_segment_input_stream(seg_id, opts);
            }
        });
        co_await coroutine::maybe_yield();
    }

    co_return std::move(snp);
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

future<> segment_manager::write(write_buffer& wb) {
    return _impl->write(wb);
}

future<log_record> segment_manager::read(log_location location) {
    return _impl->read(location);
}

void segment_manager::free_record(log_location location) {
    _impl->free_record(location);
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

class segment_data_sink_impl : public data_sink_impl {
    seg_ptr _segment;
public:
    segment_data_sink_impl(seg_ptr segment)
        : _segment(std::move(segment))
    {}

    virtual future<> put(std::span<temporary_buffer<char>> data) override {
        for (auto& buf : data) {
            co_await _segment->append(bytes_view((const int8_t*)buf.get(), buf.size()));
        }
    }

    virtual future<> close() override {
        co_return;
    }

    virtual size_t buffer_size() const noexcept override {
        return 128 * 1024;
    }
};

future<seastar::input_stream<char>> segment_manager_impl::create_segment_input_stream(log_segment_id segment_id, const seastar::file_input_stream_options& opts) {
    auto [file_id, file_offset] = segment_id_to_file_location(segment_id);
    auto file = co_await _file_mgr.get_file_for_read(file_id);
    auto stream = make_file_input_stream(std::move(file), file_offset, _cfg.segment_size, opts);
    co_return std::move(stream);
}

class segment_stream_sink_impl : public segment_stream_sink {
    segment_manager_impl& _sm;
    replica::database& _db;
    seg_ptr _seg;
public:
    segment_stream_sink_impl(segment_manager_impl& sm, replica::database& db, seg_ptr seg)
        : _sm(sm), _db(db), _seg(std::move(seg))
    {}
public:
    log_segment_id segment_id() const noexcept override {
        return _seg->id();
    }
    future<output_stream<char>> output() override {
        auto sink = std::make_unique<segment_data_sink_impl>(_seg);
        auto stream = output_stream<char>(data_sink(std::move(sink)));
        co_return std::move(stream);
    }
    future<> close() override {
        co_await _seg->stop();
        co_await _sm.load_segment(_db, _seg->id());
    }
    future<> abort() override {
        _sm.free_segment(_seg->id());
        co_return;
    }
};

future<> segment_manager_impl::load_segment(replica::database& db, log_segment_id seg_id) {
    // read the segment and populate the index
    co_await recover_segment(db, seg_id);

    auto& desc = get_segment_descriptor(seg_id);
    co_await add_segment_to_compaction_group(db, desc);
}

future<std::unique_ptr<segment_stream_sink>> segment_manager_impl::create_segment_output_stream(replica::database& db) {
    auto seg = co_await _segment_pool.get_segment(write_source::streaming);
    _stats.segments_in_use++;
    co_return std::make_unique<segment_stream_sink_impl>(*this, db, std::move(seg));
}

future<utils::chunked_vector<segment_snapshot>> segment_manager::make_snapshot(compaction_group& cg) {
    return _impl->make_snapshot(cg);
}

future<std::unique_ptr<segment_stream_sink>> segment_manager::create_segment_output_stream(replica::database& db) {
    return _impl->create_segment_output_stream(db);
}

}

template<>
size_t hist_key<replica::logstor::segment_descriptor>(const replica::logstor::segment_descriptor& desc) {
    return desc.free_space;
}
