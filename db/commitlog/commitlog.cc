/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Modified by ScyllaDB
 * Copyright (C) 2015 ScyllaDB
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

#include <stdexcept>
#include <string>
#include <sys/stat.h>
#include <malloc.h>
#include <regex>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/adaptor/reversed.hpp>
#include <unordered_map>
#include <unordered_set>
#include <exception>
#include <filesystem>

#include <seastar/core/align.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/file.hh>
#include <seastar/core/rwlock.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/queue.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/net/byteorder.hh>

#include "seastarx.hh"

#include "commitlog.hh"
#include "rp_set.hh"
#include "db/config.hh"
#include "db/extensions.hh"
#include "utils/data_input.hh"
#include "utils/crc.hh"
#include "utils/runtime.hh"
#include "utils/flush_queue.hh"
#include "log.hh"
#include "commitlog_entry.hh"
#include "commitlog_extensions.hh"
#include "service/priority_manager.hh"
#include "serializer.hh"

#include <boost/range/numeric.hpp>
#include <boost/range/adaptor/transformed.hpp>

#include "checked-file-impl.hh"
#include "utils/disk-error-handler.hh"

static logging::logger clogger("commitlog");

using namespace std::chrono_literals;

class crc32_nbo {
    utils::crc32 _c;
public:
    template <typename T>
    void process(T t) {
        _c.process_be(t);
    }
    uint32_t checksum() const {
        return _c.get();
    }
    void process_bytes(const uint8_t* data, size_t size) {
        return _c.process(data, size);
    }
    void process_bytes(const int8_t* data, size_t size) {
        return _c.process(reinterpret_cast<const uint8_t*>(data), size);
    }
    void process_bytes(const char* data, size_t size) {
        return _c.process(reinterpret_cast<const uint8_t*>(data), size);
    }
    template<typename FragmentedBuffer>
    requires FragmentRange<FragmentedBuffer>
    void process_fragmented(const FragmentedBuffer& buffer) {
        return _c.process_fragmented(buffer);
    }
};

class db::cf_holder {
public:
    virtual ~cf_holder() {};
    virtual void release_cf_count(const cf_id_type&) = 0;
};

db::commitlog::config db::commitlog::config::from_db_config(const db::config& cfg, size_t shard_available_memory) {
    config c;

    c.commit_log_location = cfg.commitlog_directory();
    c.metrics_category_name = "commitlog";
    c.commitlog_total_space_in_mb = cfg.commitlog_total_space_in_mb() >= 0 ? cfg.commitlog_total_space_in_mb() : (shard_available_memory * smp::count) >> 20;
    c.commitlog_segment_size_in_mb = cfg.commitlog_segment_size_in_mb();
    c.commitlog_sync_period_in_ms = cfg.commitlog_sync_period_in_ms();
    c.mode = cfg.commitlog_sync() == "batch" ? sync_mode::BATCH : sync_mode::PERIODIC;
    c.extensions = &cfg.extensions();
    c.reuse_segments = cfg.commitlog_reuse_segments();
    c.use_o_dsync = cfg.commitlog_use_o_dsync();

    return c;
}

db::commitlog::descriptor::descriptor(segment_id_type i, const std::string& fname_prefix, uint32_t v, sstring fname)
        : _filename(std::move(fname)), id(i), ver(v), filename_prefix(fname_prefix) {
}

db::commitlog::descriptor::descriptor(replay_position p, const std::string& fname_prefix)
        : descriptor(p.id, fname_prefix) {
}


db::commitlog::descriptor::descriptor(const sstring& filename, const std::string& fname_prefix)
        : descriptor([&filename, &fname_prefix]() {
            std::smatch m;
            // match both legacy and new version of commitlogs Ex: CommitLog-12345.log and CommitLog-4-12345.log.
                std::regex rx("(?:Recycled-)?" + fname_prefix + "((\\d+)(" + SEPARATOR + "\\d+)?)" + FILENAME_EXTENSION);
                std::string sfilename = filename;
                auto cbegin = sfilename.cbegin();
                // skip the leading path
                // Note: we're using rfind rather than the regex above
                // since it may run out of stack in debug builds.
                // See https://github.com/scylladb/scylla/issues/4464
                auto pos = std::string(filename).rfind('/');
                if (pos != std::string::npos) {
                    cbegin += pos + 1;
                }
                if (!std::regex_match(cbegin, sfilename.cend(), m, rx)) {
                    throw std::domain_error("Cannot parse the version of the file: " + filename);
                }
                if (m[3].length() == 0) {
                    // CMH. Can most likely ignore this
                    throw std::domain_error("Commitlog segment is too old to open; upgrade to 1.2.5+ first");
                }

                segment_id_type id = std::stoull(m[3].str().substr(1));
                uint32_t ver = std::stoul(m[2].str());

                return descriptor(id, fname_prefix, ver, filename);
            }()) {
}

sstring db::commitlog::descriptor::filename() const {
    if (!_filename.empty()) {
        return _filename;
    }
    return filename_prefix + std::to_string(ver) + SEPARATOR
            + std::to_string(id) + FILENAME_EXTENSION;
}

db::commitlog::descriptor::operator db::replay_position() const {
    return replay_position(id);
}

/**
 * virtual dispatch for actually inputting data.
 * purposely de/un-templated
 *
 * Writes N entries to a single segment,
 * where each entry has its own header+crc,
 * i.e. will be deserialized separately.
 */
struct db::commitlog::entry_writer {
    force_sync sync;
    size_t num_entries;

    explicit entry_writer(force_sync fs, size_t ne = 1)
        : sync(fs)
        , num_entries(ne)
    {}
    virtual ~entry_writer() = default;

    /** return the CF id for n:th entry */
    virtual const cf_id_type& id(size_t) const = 0;

    /**
     * Returns segment-independent size of all entries combined. Must be >= than segment-dependant total size.
     * This is always called first, and should return "worst-case"
     * for the complete set of entries
     */
    virtual size_t size() const = 0;
    /**
     * Return the total size of all entries in this given segment
     * Called after size(void), once a segment has been chosen.
     * Should return the total, exact, size for all entries + overhead (i.e. schema)
     * for this segment.
     *
     * Can be called more than once, if segment switch is neccesary (because race)
     */
    virtual size_t size(segment&) = 0;
    /**
     * return the size of the n:th entry in this given segment
     * Only called IFF num_entries > 1, and if so, after size(void)/size(segment&)
     * and before write(...)
     */
    virtual size_t size(segment&, size_t) = 0;

    /* write nth entry */
    virtual void write(segment&, output&, size_t) const = 0;

    /** the resulting rp_handle for writing a given entry */
    virtual void result(size_t, rp_handle) = 0;
};

const std::string db::commitlog::descriptor::SEPARATOR("-");
const std::string db::commitlog::descriptor::FILENAME_PREFIX(
        "CommitLog" + SEPARATOR);
const std::string db::commitlog::descriptor::FILENAME_EXTENSION(".log");

class db::commitlog::segment_manager : public ::enable_shared_from_this<segment_manager> {
public:
    config cfg;
    std::vector<sstring> _segments_to_replay;
    const uint64_t max_size;
    const uint64_t max_mutation_size;
    // Divide the size-on-disk threshold by #cpus used, since we assume
    // we distribute stuff more or less equally across shards.
    const uint64_t max_disk_size; // per-shard
    const uint64_t disk_usage_threshold;

    bool _shutdown = false;
    std::optional<shared_promise<>> _shutdown_promise = {};

    struct request_controller_timeout_exception_factory {
        class request_controller_timed_out_error : public timed_out_error {
        public:
            virtual const char* what() const noexcept override {
                return "commitlog: timed out";
            }
        };
        static auto timeout() {
            return request_controller_timed_out_error();
        }
    };
    // Allocation must throw timed_out_error by contract.
    using timeout_exception_factory = request_controller_timeout_exception_factory;

    basic_semaphore<timeout_exception_factory> _flush_semaphore;

    seastar::metrics::metric_groups _metrics;

    // TODO: verify that we're ok with not-so-great granularity
    using clock_type = lowres_clock;
    using time_point = clock_type::time_point;
    using sseg_ptr = ::shared_ptr<segment>;

    using request_controller_type = basic_semaphore<timeout_exception_factory, db::timeout_clock>;
    using request_controller_units = semaphore_units<timeout_exception_factory, db::timeout_clock>;
    request_controller_type _request_controller;
    shared_promise<> _disk_deletions;

    std::optional<shared_future<with_clock<db::timeout_clock>>> _segment_allocating;
    std::unordered_map<sstring, descriptor> _files_to_delete;
    std::vector<file> _files_to_close;

    void account_memory_usage(size_t size) {
        _request_controller.consume(size);
    }

    void notify_memory_written(size_t size) {
        _request_controller.signal(size);
    }

    future<>
    allocate_when_possible(shared_ptr<entry_writer> writer, db::timeout_clock::time_point timeout);

    struct stats {
        uint64_t cycle_count = 0;
        uint64_t flush_count = 0;
        uint64_t allocation_count = 0;
        uint64_t bytes_written = 0;
        uint64_t bytes_slack = 0;
        uint64_t segments_created = 0;
        uint64_t segments_destroyed = 0;
        uint64_t pending_flushes = 0;
        uint64_t flush_limit_exceeded = 0;
        uint64_t buffer_list_bytes = 0;
        // size on disk, actually used - i.e. containing data (allocate+cycle)
        uint64_t active_size_on_disk = 0;
        // size allocated on disk - i.e. files created (new, reserve, recycled)
        uint64_t total_size_on_disk = 0;
        uint64_t requests_blocked_memory = 0;
    };

    stats totals;

    size_t pending_allocations() const {
        return _request_controller.waiters();
    }

    future<> begin_flush() {
        ++totals.pending_flushes;
        if (totals.pending_flushes >= cfg.max_active_flushes) {
            ++totals.flush_limit_exceeded;
            clogger.trace("Flush ops overflow: {}. Will block.", totals.pending_flushes);
        }
        return _flush_semaphore.wait();
    }
    void end_flush() {
        _flush_semaphore.signal();
        --totals.pending_flushes;
    }
    segment_manager(config c);
    ~segment_manager() {
        clogger.trace("Commitlog {} disposed", cfg.commit_log_location);
    }

    uint64_t next_id() {
        return ++_ids;
    }

    std::exception_ptr sanity_check_size(size_t size) {
        if (size > max_mutation_size) {
            return make_exception_ptr(std::invalid_argument(
                            "Mutation of " + std::to_string(size)
                                    + " bytes is too large for the maximum size of "
                                    + std::to_string(max_mutation_size)));
        }
        return nullptr;
    }

    future<> init();
    future<sseg_ptr> new_segment();
    future<sseg_ptr> active_segment(db::timeout_clock::time_point timeout);
    future<sseg_ptr> allocate_segment();
    future<sseg_ptr> allocate_segment_ex(descriptor, sstring filename, open_flags);

    sstring filename(const descriptor& d) const {
        return cfg.commit_log_location + "/" + d.filename();
    }

    future<> clear();
    future<> sync_all_segments();
    future<> shutdown_all_segments();
    future<> shutdown();

    void create_counters(const sstring& metrics_category_name);

    future<> orphan_all();

    void add_file_to_delete(sstring, descriptor);
    void add_file_to_close(file);

    future<> do_pending_deletes();

    future<> delete_segments(std::vector<sstring>);
    future<> delete_file(const sstring&);

    void discard_unused_segments();
    void discard_completed_segments(const cf_id_type&);
    void discard_completed_segments(const cf_id_type&, const rp_set&);
    void on_timer();
    void sync();
    void arm(uint32_t extra = 0) {
        if (!_shutdown) {
            _timer.arm(std::chrono::milliseconds(cfg.commitlog_sync_period_in_ms + extra));
        }
    }

    std::vector<sstring> get_active_names() const;
    uint64_t get_num_dirty_segments() const;
    uint64_t get_num_active_segments() const;

    using buffer_type = fragmented_temporary_buffer;

    buffer_type acquire_buffer(size_t s);
    temporary_buffer<char> allocate_single_buffer(size_t);

    future<std::vector<descriptor>> list_descriptors(sstring dir);

    flush_handler_id add_flush_handler(flush_handler h) {
        auto id = ++_flush_ids;
        _flush_handlers[id] = std::move(h);
        return id;
    }
    void remove_flush_handler(flush_handler_id id) {
        _flush_handlers.erase(id);
    }

    void flush_segments(uint64_t size_to_remove);

private:
    future<> clear_reserve_segments();

    future<> rename_file(sstring, sstring) const;
    size_t max_request_controller_units() const;
    segment_id_type _ids = 0;
    std::vector<sseg_ptr> _segments;
    queue<sseg_ptr> _reserve_segments;
    queue<sstring> _recycled_segments;
    std::unordered_map<flush_handler_id, flush_handler> _flush_handlers;
    flush_handler_id _flush_ids = 0;
    replay_position _flush_position;
    timer<clock_type> _timer;
    future<> replenish_reserve();
    future<> _reserve_replenisher;
    seastar::gate _gate;
    uint64_t _new_counter = 0;
};

template<typename T>
static void write(fragmented_temporary_buffer::ostream& out, T value) {
    auto v = net::hton(value);
    out.write(reinterpret_cast<const char*>(&v), sizeof(v));
}

template<typename T, typename Input>
std::enable_if_t<std::is_fundamental<T>::value, T> read(Input& in) {
    return net::ntoh(in.template read<T>());
}

/*
 * A single commit log file on disk. Manages creation of the file and writing mutations to disk,
 * as well as tracking the last mutation position of any "dirty" CFs covered by the segment file. Segment
 * files are initially allocated to a fixed size and can grow to accomidate a larger value if necessary.
 *
 * The IO flow is somewhat convoluted and goes something like this:
 *
 * Mutation path:
 *  - Adding data to the segment usually writes into the internal buffer
 *  - On EOB or overflow we issue a write to disk ("cycle").
 *      - A cycle call will acquire the segment read lock and send the
 *        buffer to the corresponding position in the file
 *  - If we are periodic and crossed a timing threshold, or running "batch" mode
 *    we might be forced to issue a flush ("sync") after adding data
 *      - A sync call acquires the write lock, thus locking out writes
 *        and waiting for pending writes to finish. It then checks the
 *        high data mark, and issues the actual file flush.
 *        Note that the write lock is released prior to issuing the
 *        actual file flush, thus we are allowed to write data to
 *        after a flush point concurrently with a pending flush.
 *
 * Sync timer:
 *  - In periodic mode, we try to primarily issue sync calls in
 *    a timer task issued every N seconds. The timer does the same
 *    operation as the above described sync, and resets the timeout
 *    so that mutation path will not trigger syncs and delay.
 *
 * Note that we do not care which order segment chunks finish writing
 * to disk, other than all below a flush point must finish before flushing.
 *
 * We currently do not wait for flushes to finish before issueing the next
 * cycle call ("after" flush point in the file). This might not be optimal.
 *
 * To close and finish a segment, we first close the gate object that guards
 * writing data to it, then flush it fully (including waiting for futures create
 * by the timer to run their course), and finally wait for it to
 * become "clean", i.e. get notified that all mutations it holds have been
 * persisted to sstables elsewhere. Once this is done, we can delete the
 * segment. If a segment (object) is deleted without being fully clean, we
 * do not remove the file on disk.
 *
 */

class db::commitlog::segment : public enable_shared_from_this<segment>, public cf_holder {
    friend class rp_handle;

    ::shared_ptr<segment_manager> _segment_manager;

    descriptor _desc;
    file _file;
    sstring _file_name;

    uint64_t _file_pos = 0;
    uint64_t _flush_pos = 0;
    uint64_t _size_on_disk = 0;

    bool _closed = false;
    // Not the same as _closed since files can be reused
    bool _closed_file = false;

    bool _terminated = false;

    using buffer_type = segment_manager::buffer_type;
    using sseg_ptr = segment_manager::sseg_ptr;
    using clock_type = segment_manager::clock_type;
    using time_point = segment_manager::time_point;

    buffer_type _buffer;
    fragmented_temporary_buffer::ostream _buffer_ostream;
    std::unordered_map<cf_id_type, uint64_t> _cf_dirty;
    time_point _sync_time;
    utils::flush_queue<replay_position, std::less<replay_position>, clock_type> _pending_ops;

    uint64_t _num_allocs = 0;

    std::unordered_set<table_schema_version> _known_schema_versions;

    friend std::ostream& operator<<(std::ostream&, const segment&);
    friend class segment_manager;

    size_t buffer_position() const {
        return _buffer.size_bytes() - _buffer_ostream.size();
    }

    future<> begin_flush() {
        // This is maintaining the semantica of only using the write-lock
        // as a gate for flushing, i.e. once we've begun a flush for position X
        // we are ok with writes to positions > X
        return _segment_manager->begin_flush();
    }

    void end_flush() {
        _segment_manager->end_flush();
    }

public:
    struct cf_mark {
        const segment& s;
    };
    friend std::ostream& operator<<(std::ostream&, const cf_mark&);

    // The commit log entry overhead in bytes (int: length + int: head checksum + int: tail checksum)
    static constexpr size_t entry_overhead_size = 3 * sizeof(uint32_t);
    static constexpr size_t multi_entry_overhead_size = entry_overhead_size + sizeof(uint32_t);
    static constexpr size_t segment_overhead_size = 2 * sizeof(uint32_t);
    static constexpr size_t descriptor_header_size = 5 * sizeof(uint32_t);
    static constexpr uint32_t segment_magic = ('S'<<24) |('C'<< 16) | ('L' << 8) | 'C';
    static constexpr uint32_t multi_entry_size_magic = 0xffffffff;

    // The commit log (chained) sync marker/header size in bytes (int: length + int: checksum [segmentId, position])
    static constexpr size_t sync_marker_size = 2 * sizeof(uint32_t);

    static constexpr size_t alignment = 4096;
    // TODO : tune initial / default size
    static constexpr size_t default_size = align_up<size_t>(128 * 1024, alignment);

    segment(::shared_ptr<segment_manager> m, descriptor&& d, file&& f, uint64_t initial_disk_size)
            : _segment_manager(std::move(m)), _desc(std::move(d)), _file(std::move(f)),
        _file_name(_segment_manager->cfg.commit_log_location + "/" + _desc.filename()), 
        _size_on_disk(initial_disk_size),
        _sync_time(clock_type::now()), _pending_ops(true) // want exception propagation
    {
        ++_segment_manager->totals.segments_created;
        clogger.debug("Created new segment {}", *this);
    }
    ~segment() {
        if (!_closed_file) {
            _segment_manager->add_file_to_close(std::move(_file));
        }
        
        _segment_manager->totals.buffer_list_bytes -= _buffer.size_bytes();
        
        if (is_clean()) {
            clogger.debug("Segment {} is no longer active and will submitted for delete now", *this);
            ++_segment_manager->totals.segments_destroyed;
            _segment_manager->totals.active_size_on_disk -= file_position();
            _segment_manager->add_file_to_delete(_file_name, _desc);
        } else if (_segment_manager->cfg.warn_about_segments_left_on_disk_after_shutdown) {
            clogger.warn("Segment {} is dirty and is left on disk.", *this);
        }
    }

    bool is_schema_version_known(schema_ptr s) {
        return _known_schema_versions.contains(s->version());
    }
    void add_schema_version(schema_ptr s) {
        _known_schema_versions.emplace(s->version());
    }
    void forget_schema_versions() {
        _known_schema_versions.clear();
    }

    void release_cf_count(const cf_id_type& cf) override {
        mark_clean(cf, 1);
        if (can_delete()) {
            _segment_manager->discard_unused_segments();
        }
    }

    bool must_sync() {
        if (_segment_manager->cfg.mode == sync_mode::BATCH) {
            return false;
        }
        auto now = clock_type::now();
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                now - _sync_time).count();
        if ((_segment_manager->cfg.commitlog_sync_period_in_ms * 2) < uint64_t(ms)) {
            clogger.debug("{} needs sync. {} ms elapsed", *this, ms);
            return true;
        }
        return false;
    }
    /**
     * Finalize this segment and get a new one
     */
    future<sseg_ptr> finish_and_get_new(db::timeout_clock::time_point timeout) {
        //FIXME: discarded future.
        (void)close();
        return _segment_manager->active_segment(timeout);
    }
    void reset_sync_time() {
        _sync_time = clock_type::now();
    }
    future<sseg_ptr> shutdown() {
        /**
         * When we are shutting down, we first
         * close the segment, thus no new
         * data can be appended. Then we just issue a
         * flush, which will wait for any queued ops
         * to complete as well. Then we close the ops
         * queue, just to be sure.
         */
        auto me = shared_from_this();
        return close().finally([me] {
            // When we get here, nothing should add ops,
            // and we should have waited out all pending.
            return me->_pending_ops.close().finally([me] {
                return me->_file.truncate(me->_flush_pos).then([me] {
                    return me->_file.close().finally([me] { me->_closed_file = true; });
                });
            });
        });
    }
    // See class comment for info
    future<sseg_ptr> sync() {
        // Note: this is not a marker for when sync was finished.
        // It is when it was initiated
        reset_sync_time();
        return cycle(true);
    }
    // See class comment for info
    future<sseg_ptr> flush() {
        auto me = shared_from_this();
        assert(me.use_count() > 1);
        uint64_t pos = _file_pos;

        clogger.trace("Syncing {} {} -> {}", *this, _flush_pos, pos);

        // Only run the flush when all write ops at lower rp:s
        // have completed.
        replay_position rp(_desc.id, position_type(pos));

        // Run like this to ensure flush ordering, and making flushes "waitable"
        return _pending_ops.run_with_ordered_post_op(rp, [] { return make_ready_future<>(); }, [this, pos, me, rp] {
            assert(_pending_ops.has_operation(rp));
            return do_flush(pos);
        });
    }
    future<sseg_ptr> terminate() {
        assert(_closed);
        if (!std::exchange(_terminated, true)) {
            // write a terminating zero block iff we are ending (a reused)
            // block before actual file end.
            // we should only get here when all actual data is 
            // already flushed (see below, close()).
            if (file_position() < _segment_manager->max_size) {
                clogger.trace("{} is closed but not terminated.", *this);
                if (_buffer.empty()) {
                    new_buffer(0);
                }
                return cycle(true, true);
            }
        }
        return make_ready_future<sseg_ptr>(shared_from_this());
    }
    future<sseg_ptr> close() {
        _closed = true;
        return sync().then([] (sseg_ptr s) { return s->flush(); }).then([] (sseg_ptr s) { return s->terminate(); });
    }
    future<sseg_ptr> do_flush(uint64_t pos) {
        auto me = shared_from_this();
        return begin_flush().then([this, pos]() {
            if (pos <= _flush_pos) {
                clogger.trace("{} already synced! ({} < {})", *this, pos, _flush_pos);
                return make_ready_future<>();
            }
            return _file.flush().then_wrapped([this, pos](future<> f) {
                try {
                    f.get();
                    // TODO: retry/ignore/fail/stop - optional behaviour in origin.
                    // we fast-fail the whole commit.
                    _flush_pos = std::max(pos, _flush_pos);
                    ++_segment_manager->totals.flush_count;
                    clogger.trace("{} synced to {}", *this, _flush_pos);
                } catch (...) {
                    clogger.error("Failed to flush commits to disk: {}", std::current_exception());
                    throw;
                }
            });
        }).finally([this] {
            end_flush();
        }).then([me] {
            return make_ready_future<sseg_ptr>(me);
        });
    }

    /**
     * Allocate a new buffer
     */
    void new_buffer(size_t s) {
        assert(_buffer.empty());

        auto overhead = segment_overhead_size;
        if (_file_pos == 0) {
            overhead += descriptor_header_size;
        }

        auto a = align_up(s + overhead, alignment);
        auto k = std::max(a, default_size);

        _buffer = _segment_manager->acquire_buffer(k);
        _buffer_ostream = _buffer.get_ostream();
        auto out = _buffer_ostream.write_substream(overhead);
        out.fill('\0', overhead);
        _segment_manager->totals.buffer_list_bytes += _buffer.size_bytes();
    }

    bool buffer_is_empty() const {
        return buffer_position() <= segment_overhead_size
                        || (_file_pos == 0 && buffer_position() <= (segment_overhead_size + descriptor_header_size));
    }
    /**
     * Send any buffer contents to disk and get a new tmp buffer
     */
    // See class comment for info
    future<sseg_ptr> cycle(bool flush_after = false, bool termination = false) {
        if (_buffer.empty() && !termination) {
            return flush_after ? flush() : make_ready_future<sseg_ptr>(shared_from_this());
        }

        auto size = clear_buffer_slack();
        auto buf = std::exchange(_buffer, { });
        auto off = _file_pos;
        auto top = off + size;
        auto num = _num_allocs;

        _file_pos = top;
        _buffer_ostream = { };
        _num_allocs = 0;

        auto me = shared_from_this();
        assert(me.use_count() > 1);

        auto out = buf.get_ostream();

        auto header_size = 0;

        if (off == 0) {
            // first block. write file header.
            write(out, segment_magic);
            write(out, _desc.ver);
            write(out, _desc.id);
            crc32_nbo crc;
            crc.process(_desc.ver);
            crc.process<int32_t>(_desc.id & 0xffffffff);
            crc.process<int32_t>(_desc.id >> 32);
            write(out, crc.checksum());
            header_size = descriptor_header_size;
        }

        if (!termination) {
            // write chunk header
            crc32_nbo crc;
            crc.process<int32_t>(_desc.id & 0xffffffff);
            crc.process<int32_t>(_desc.id >> 32);
            crc.process(uint32_t(off + header_size));

            write(out, uint32_t(_file_pos));
            write(out, crc.checksum());

            forget_schema_versions();

            clogger.trace("Writing {} entries, {} k in {} -> {}", num, size, off, off + size);
        } else {
            assert(num == 0);
            assert(_closed);
            clogger.trace("Terminating {} at pos {}", *this, _file_pos);
            write(out, uint64_t(0));
        }

        replay_position rp(_desc.id, position_type(off));

        // The write will be allowed to start now, but flush (below) must wait for not only this,
        // but all previous write/flush pairs.
        return _pending_ops.run_with_ordered_post_op(rp, [this, size, off, buf = std::move(buf)]() mutable {
            auto view = fragmented_temporary_buffer::view(buf);
            view.remove_suffix(buf.size_bytes() - size);
            assert(size == view.size_bytes());
            return do_with(off, view, [&] (uint64_t& off, fragmented_temporary_buffer::view& view) {
                if (view.empty()) {
                    return make_ready_future<>();
                }
                return repeat([this, size, &off, &view] {
                    auto&& priority_class = service::get_local_commitlog_priority();
                    auto current = *view.begin();
                    return _file.dma_write(off, current.data(), current.size(), priority_class).then_wrapped([this, size, &off, &view](future<size_t>&& f) {
                        try {
                            auto bytes = f.get0();
                            _segment_manager->totals.bytes_written += bytes;
                            _segment_manager->totals.active_size_on_disk += bytes;
                            ++_segment_manager->totals.cycle_count;
                            if (bytes == view.size_bytes()) {
                                clogger.trace("Final write of {} to {}: {}/{} bytes at {}", bytes, *this, size, size, off);
                                return make_ready_future<stop_iteration>(stop_iteration::yes);
                            }
                            // gah, partial write. should always get here with dma chunk sized
                            // "bytes", but lets make sure...
                            bytes = align_down(bytes, alignment);
                            off += bytes;
                            view.remove_prefix(bytes);
                            clogger.trace("Partial write of {} to {}: {}/{} bytes at at {}", bytes, *this, size - view.size_bytes(), size, off - bytes);
                            return make_ready_future<stop_iteration>(stop_iteration::no);
                            // TODO: retry/ignore/fail/stop - optional behaviour in origin.
                            // we fast-fail the whole commit.
                        } catch (...) {
                            clogger.error("Failed to persist commits to disk for {}: {}", *this, std::current_exception());
                            throw;
                        }
                    });
                });
            }).finally([this, buf = std::move(buf), size] {
                _segment_manager->notify_memory_written(size);
                _segment_manager->totals.buffer_list_bytes -= buf.size_bytes();
                if (_size_on_disk < _file_pos) {
                    _segment_manager->totals.total_size_on_disk += (_file_pos - _size_on_disk);
                    _size_on_disk = _file_pos;
                }
            });
        }, [me, flush_after, top, rp] { // lambda instead of bind, so we keep "me" alive.
            assert(me->_pending_ops.has_operation(rp));
            return flush_after ? me->do_flush(top) : make_ready_future<sseg_ptr>(me);
        });
    }

    future<sseg_ptr> batch_cycle(timeout_clock::time_point timeout) {
        /**
         * For batch mode we force a write "immediately".
         * However, we first wait for all previous writes/flushes
         * to complete.
         *
         * This has the benefit of allowing several allocations to
         * queue up in a single buffer.
         */
        auto me = shared_from_this();
        auto fp = _file_pos;
        return _pending_ops.wait_for_pending(timeout).then([me, fp, timeout] {
            if (fp != me->_file_pos) {
                // some other request already wrote this buffer.
                // If so, wait for the operation at our intended file offset
                // to finish, then we know the flush is complete and we
                // are in accord.
                // (Note: wait_for_pending(pos) waits for operation _at_ pos (and before),
                replay_position rp(me->_desc.id, position_type(fp));
                return me->_pending_ops.wait_for_pending(rp, timeout).then([me, fp] {
                    assert(me->_segment_manager->cfg.mode != sync_mode::BATCH || me->_flush_pos > fp);
                    if (me->_flush_pos <= fp) {
                        // previous op we were waiting for was not sync one, so it did not flush
                        // force flush here
                        return me->do_flush(fp);
                    }
                    return make_ready_future<sseg_ptr>(me);
                });
            }
            // It is ok to leave the sync behind on timeout because there will be at most one
            // such sync, all later allocations will block on _pending_ops until it is done.
            return with_timeout(timeout, me->sync());
        }).handle_exception([me, fp](auto p) {
            // If we get an IO exception (which we assume this is)
            // we should close the segment.
            // TODO: should we also trunctate away any partial write
            // we did?
            me->_closed = true; // just mark segment as closed, no writes will be done.
            return make_exception_future<sseg_ptr>(p);
        });
    }

    /**
     * Add a "mutation" to the segment.
     */
    future<> allocate(shared_ptr<entry_writer> writer, segment_manager::request_controller_units permit, db::timeout_clock::time_point timeout) {
        if (must_sync()) {
            return with_timeout(timeout, sync()).then([this, writer = std::move(writer), permit = std::move(permit), timeout] (auto s) mutable {
                return s->allocate(std::move(writer), std::move(permit), timeout);
            });
        }

        const auto size = writer->size(*this);
        const auto s = size + writer->num_entries * entry_overhead_size + (writer->num_entries > 1 ? multi_entry_overhead_size : 0u); // total size
        auto ep = _segment_manager->sanity_check_size(s);
        if (ep) {
            return make_exception_future<>(std::move(ep));
        }

        if (!is_still_allocating() || position() + s > _segment_manager->max_size) { // would we make the file too big?
            return finish_and_get_new(timeout).then([writer = std::move(writer), permit = std::move(permit), timeout] (auto new_seg) mutable {
                return new_seg->allocate(std::move(writer), std::move(permit), timeout);
            });
        } else if (!_buffer.empty() && (s > _buffer_ostream.size())) {  // enough data?
            if (_segment_manager->cfg.mode == sync_mode::BATCH || writer->sync) {
                // TODO: this could cause starvation if we're really unlucky.
                // If we run batch mode and find ourselves not fit in a non-empty
                // buffer, we must force a cycle and wait for it (to keep flush order)
                // This will most likely cause parallel writes, and consecutive flushes.
                return with_timeout(timeout, sync()).then([this, writer = std::move(writer), permit = std::move(permit), timeout] (auto new_seg) mutable {
                    return new_seg->allocate(std::move(writer), std::move(permit), timeout);
                });
            } else {
                //FIXME: discarded future
                (void)cycle().discard_result().handle_exception([] (auto ex) {
                    clogger.error("Failed to flush commits to disk: {}", ex);
                });
            }
        }

        size_t buf_memory = s;
        if (_buffer.empty()) {
            new_buffer(s);
            buf_memory += buffer_position();
        }

        if (_closed) {
            return make_exception_future<>(std::runtime_error("commitlog: Cannot add data to a closed segment"));
        }

        buf_memory -= permit.release();
        _segment_manager->account_memory_usage(buf_memory);

        auto& out = _buffer_ostream;

        std::optional<crc32_nbo> mecrc;

        // if this is multi-entry write, we need to add an extra header + crc
        // the header and crc formula is:
        // header:
        //      magic : uint32_t
        //      size  : uint32_t
        //      crc1  : uint32_t - crc of magic, size
        // -> entries[]
        // post:
        //      crc2  : uint32_t - crc1 + each entry crc.
        if (writer->num_entries > 1) {
            mecrc.emplace();
            write<uint32_t>(out, multi_entry_size_magic);
            write<uint32_t>(out, s);
            mecrc->process(multi_entry_size_magic);
            mecrc->process(uint32_t(s));
            write<uint32_t>(out, mecrc->checksum());
        }

        for (size_t entry = 0; entry < writer->num_entries; ++entry) {
            replay_position rp(_desc.id, position());
            auto id = writer->id(entry);
            auto entry_size = writer->num_entries == 1 ? size : writer->size(*this, entry);
            auto es = entry_size + entry_overhead_size;

            _cf_dirty[id]++; // increase use count for cf.

            rp_handle h(static_pointer_cast<cf_holder>(shared_from_this()), std::move(id), rp);

            crc32_nbo crc;

            write<uint32_t>(out, es);
            crc.process(uint32_t(es));
            write<uint32_t>(out, crc.checksum());

            // actual data
            auto entry_out = out.write_substream(entry_size);
            auto entry_data = entry_out.to_input_stream();
            writer->write(*this, entry_out, entry);
            entry_data.with_stream([&] (auto data_str) {
                crc.process_fragmented(ser::buffer_view<typename std::vector<temporary_buffer<char>>::iterator>(data_str));
            });

            auto checksum = crc.checksum();
            write<uint32_t>(out, checksum);
            if (mecrc) {
                mecrc->process(checksum);
            }

            writer->result(entry, std::move(h));
        }

        if (mecrc) {
            // write the crc of header + all sub-entry crc
            write<uint32_t>(out, mecrc->checksum());
        }

        ++_segment_manager->totals.allocation_count;
        ++_num_allocs;

        if (_segment_manager->cfg.mode == sync_mode::BATCH || writer->sync) {
            return batch_cycle(timeout).discard_result();
        } else {
            // If this buffer alone is too big, potentially bigger than the maximum allowed size,
            // then no other request will be allowed in to force the cycle()ing of this buffer. We
            // have to do it ourselves.
            if ((buffer_position() >= (db::commitlog::segment::default_size))) {
                //FIXME: discarded future.
                (void)cycle().discard_result().handle_exception([] (auto ex) {
                    clogger.error("Failed to flush commits to disk: {}", ex);
                });
            }
            return make_ready_future<>();
        }
    }

    position_type position() const {
        return position_type(_file_pos + buffer_position());
    }

    size_t file_position() const {
        return _file_pos;
    }

    // ensures no more of this segment is writeable, by allocating any unused section at the end and marking it discarded
    // a.k.a. zero the tail.
    size_t clear_buffer_slack() {
        auto buf_pos = buffer_position();
        auto size = align_up(buf_pos, alignment);
        auto fill_size = size - buf_pos;
        _buffer_ostream.fill('\0', fill_size);
        _segment_manager->totals.bytes_slack += fill_size;
        _segment_manager->account_memory_usage(fill_size);
        return size;
    }
    void mark_clean(const cf_id_type& id, uint64_t count) {
        auto i = _cf_dirty.find(id);
        if (i != _cf_dirty.end()) {
            assert(i->second >= count);
            i->second -= count;
            if (i->second == 0) {
                _cf_dirty.erase(i);
            }
        }
    }
    void mark_clean(const cf_id_type& id) {
        _cf_dirty.erase(id);
    }
    void mark_clean() {
        _cf_dirty.clear();
    }
    bool is_still_allocating() const {
        return !_closed && position() < _segment_manager->max_size;
    }
    bool is_clean() const {
        return _cf_dirty.empty();
    }
    bool is_unused() const {
        return !is_still_allocating() && is_clean();
    }
    bool is_flushed() const {
        return position() <= _flush_pos;
    }
    bool can_delete() const {
        return is_unused() && is_flushed();
    }
    bool contains(const replay_position& pos) const {
        return pos.id == _desc.id;
    }
    sstring get_segment_name() const {
        return _desc.filename();
    }
};

future<>
db::commitlog::segment_manager::allocate_when_possible(shared_ptr<entry_writer> writer, db::timeout_clock::time_point timeout) {
    auto size = writer->size();
    // If this is already too big now, we should throw early. It's also a correctness issue, since
    // if we are too big at this moment we'll never reach allocate() to actually throw at that
    // point.
    auto ep = sanity_check_size(size);
    if (ep) {
        return make_exception_future<>(std::move(ep));
    }

    auto fut = get_units(_request_controller, size, timeout);
    if (_request_controller.waiters()) {
        totals.requests_blocked_memory++;
    }
    return fut.then([this, writer = std::move(writer), timeout] (auto permit) mutable {
        return active_segment(timeout).then([this, timeout, writer = std::move(writer), permit = std::move(permit)] (auto s) mutable {
            return s->allocate(std::move(writer), std::move(permit), timeout);
        });
    });
}

const size_t db::commitlog::segment::default_size;

db::commitlog::segment_manager::segment_manager(config c)
    : cfg([&c] {
        config cfg(c);

        if (cfg.commit_log_location.empty()) {
            cfg.commit_log_location = "/var/lib/scylla/commitlog";
        }

        if (cfg.max_active_writes == 0) {
            cfg.max_active_writes = // TODO: call someone to get an idea...
                            25 * smp::count;
        }
        cfg.max_active_writes = std::max(uint64_t(1), cfg.max_active_writes / smp::count);
        if (cfg.max_active_flushes == 0) {
            cfg.max_active_flushes = // TODO: call someone to get an idea...
                            5 * smp::count;
        }
        cfg.max_active_flushes = std::max(uint64_t(1), cfg.max_active_flushes / smp::count);

        return cfg;
    }())
    , max_size(std::min<size_t>(std::numeric_limits<position_type>::max() / (1024 * 1024), std::max<size_t>(cfg.commitlog_segment_size_in_mb, 1)) * 1024 * 1024)
    , max_mutation_size(max_size >> 1)
    , max_disk_size(size_t(std::ceil(cfg.commitlog_total_space_in_mb / double(smp::count))) * 1024 * 1024)
    // our threshold for trying to force a flush. needs heristics, for now max - segment_size/2.
    , disk_usage_threshold(cfg.commitlog_flush_threshold_in_mb.has_value() 
        ? size_t(std::ceil(*cfg.commitlog_flush_threshold_in_mb / double(smp::count))) * 1024 * 1024 
        : (max_disk_size - (max_disk_size > (max_size/2) ? (max_size/2) : 0)))
    , _flush_semaphore(cfg.max_active_flushes)
    // That is enough concurrency to allow for our largest mutation (max_mutation_size), plus
    // an existing in-flight buffer. Since we'll force the cycling() of any buffer that is bigger
    // than default_size at the end of the allocation, that allows for every valid mutation to
    // always be admitted for processing.
    , _request_controller(max_request_controller_units(), request_controller_timeout_exception_factory{})
    , _reserve_segments(1)
    , _recycled_segments(std::numeric_limits<size_t>::max())
    , _reserve_replenisher(make_ready_future<>())
{
    assert(max_size > 0);
    assert(max_mutation_size < segment::multi_entry_size_magic);

    clogger.trace("Commitlog {} maximum disk size: {} MB / cpu ({} cpus)",
            cfg.commit_log_location, max_disk_size / (1024 * 1024),
            smp::count);

    if (!cfg.metrics_category_name.empty()) {
        create_counters(cfg.metrics_category_name);
    }
}

size_t db::commitlog::segment_manager::max_request_controller_units() const {
    return max_mutation_size + db::commitlog::segment::default_size;
}

future<> db::commitlog::segment_manager::replenish_reserve() {
    return do_until([this] { return _shutdown; }, [this] {
        return _reserve_segments.not_full().then([this] {
            if (_shutdown) {
                return make_ready_future<>();
            }
            return with_gate(_gate, [this] {
                // note: if we were strict with disk size, we would refuse to do this 
                // unless disk footprint is lower than threshold. but we cannot (yet?)
                // trust that flush logic will absolutely free up an existing 
                // segment (because colocation stuff etc), so always allow a new
                // file if needed. That and performance stuff...
                return allocate_segment().then([this](sseg_ptr s) {
                    auto ret = _reserve_segments.push(std::move(s));
                    if (!ret) {
                        clogger.error("Segment reserve is full! Ignoring and trying to continue, but shouldn't happen");
                    }
                    return make_ready_future<>();
                });
            }).handle_exception([](std::exception_ptr ep) {
                clogger.warn("Exception in segment reservation: {}", ep);
                return sleep(100ms);
            });
        });
    });
}

future<std::vector<db::commitlog::descriptor>>
db::commitlog::segment_manager::list_descriptors(sstring dirname) {
    struct helper {
        sstring _dirname;
        file _file;
        sstring _fname_prefix;
        future<> _list_done;
        std::vector<db::commitlog::descriptor> _result;

        helper(helper&&) = default;
        helper(sstring n, sstring fname_prefix, file && f)
                : _dirname(std::move(n)), _file(std::move(f)), _fname_prefix(std::move(fname_prefix)), _list_done(
                        _file.list_directory(
                                std::bind(&helper::process, this,
                                        std::placeholders::_1)).done()) {
        }

        future<> process(directory_entry de) {
            auto entry_type = [this](const directory_entry & de) {
                if (!de.type && !de.name.empty()) {
                    return file_type(_dirname + "/" + de.name);
                }
                return make_ready_future<std::optional<directory_entry_type>>(de.type);
            };
            return entry_type(de).then([this, de](std::optional<directory_entry_type> type) {
                if (type == directory_entry_type::regular && de.name[0] != '.' && !is_cassandra_segment(de.name)) {
                    try {
                        _result.emplace_back(de.name, _fname_prefix);
                    } catch (std::domain_error& e) {
                        clogger.warn(e.what());
                    }
                }
                return make_ready_future<>();
            });
        }

        future<> done() {
            return std::move(_list_done);
        }

        static bool is_cassandra_segment(sstring name) {
            // We want to ignore commitlog segments generated by Cassandra-derived tools (#1112)
            auto c = sstring("Cassandra");
            if (name.size() < c.size()) {
                return false;
            }
            return name.substr(0, c.size()) == c;
        }
    };

    return open_checked_directory(commit_error_handler, dirname).then([this, dirname](file dir) {
        auto h = make_lw_shared<helper>(std::move(dirname), cfg.fname_prefix, std::move(dir));
        return h->done().then([h]() {
            return make_ready_future<std::vector<db::commitlog::descriptor>>(std::move(h->_result));
        }).finally([h] {});
    });
}

future<> db::commitlog::segment_manager::init() {
    return list_descriptors(cfg.commit_log_location).then([this](std::vector<descriptor> descs) {
        assert(_reserve_segments.empty()); // _segments_to_replay must not pick them up
        segment_id_type id = std::chrono::duration_cast<std::chrono::milliseconds>(runtime::get_boot_time().time_since_epoch()).count() + 1;
        for (auto& d : descs) {
            id = std::max(id, replay_position(d.id).base_id());
            _segments_to_replay.push_back(cfg.commit_log_location + "/" + d.filename());
        }

        // base id counter is [ <shard> | <base> ]
        _ids = replay_position(this_shard_id(), id).id;
        // always run the timer now, since we need to handle segment pre-alloc etc as well.
        _timer.set_callback(std::bind(&segment_manager::on_timer, this));
        auto delay = this_shard_id() * std::ceil(double(cfg.commitlog_sync_period_in_ms) / smp::count);
        clogger.trace("Delaying timer loop {} ms", delay);
        // We need to wait until we have scanned all other segments to actually start serving new
        // segments. We are ready now
        _reserve_replenisher = replenish_reserve();
        arm(delay);
    });
}

void db::commitlog::segment_manager::create_counters(const sstring& metrics_category_name) {
    namespace sm = seastar::metrics;

    _metrics.add_group(metrics_category_name, {
        sm::make_gauge("segments", [this] { return _segments.size(); },
                       sm::description("Holds the current number of segments.")),

        sm::make_gauge("allocating_segments", [this] { return std::count_if(_segments.begin(), _segments.end(), [] (const sseg_ptr & s) { return s->is_still_allocating(); }); },
                       sm::description("Holds the number of not closed segments that still have some free space. "
                                       "This value should not get too high.")),

        sm::make_gauge("unused_segments", [this] { return std::count_if(_segments.begin(), _segments.end(), [] (const sseg_ptr & s) { return s->is_unused(); }); },
                       sm::description("Holds the current number of unused segments. "
                                       "A non-zero value indicates that the disk write path became temporary slow.")),

        sm::make_derive("alloc", totals.allocation_count,
                       sm::description("Counts a number of times a new mutation has been added to a segment. "
                                       "Divide bytes_written by this value to get the average number of bytes per mutation written to the disk.")),

        sm::make_derive("cycle", totals.cycle_count,
                       sm::description("Counts a number of commitlog write cycles - when the data is written from the internal memory buffer to the disk.")),

        sm::make_derive("flush", totals.flush_count,
                       sm::description("Counts a number of times the flush() method was called for a file.")),

        sm::make_derive("bytes_written", totals.bytes_written,
                       sm::description("Counts a number of bytes written to the disk. "
                                       "Divide this value by \"alloc\" to get the average number of bytes per mutation written to the disk.")),

        sm::make_derive("slack", totals.bytes_slack,
                       sm::description("Counts a number of unused bytes written to the disk due to disk segment alignment.")),

        sm::make_gauge("pending_flushes", totals.pending_flushes,
                       sm::description("Holds a number of currently pending flushes. See the related flush_limit_exceeded metric.")),

        sm::make_gauge("pending_allocations", [this] { return pending_allocations(); },
                       sm::description("Holds a number of currently pending allocations. "
                                       "A non-zero value indicates that we have a bottleneck in the disk write flow.")),

        sm::make_derive("requests_blocked_memory", totals.requests_blocked_memory,
                       sm::description("Counts a number of requests blocked due to memory pressure. "
                                       "A non-zero value indicates that the commitlog memory quota is not enough to serve the required amount of requests.")),

        sm::make_derive("flush_limit_exceeded", totals.flush_limit_exceeded,
                       sm::description(
                           seastar::format("Counts a number of times a flush limit was exceeded. "
                                           "A non-zero value indicates that there are too many pending flush operations (see pending_flushes) and some of "
                                           "them will be blocked till the total amount of pending flush operations drops below {}.", cfg.max_active_flushes))),

        sm::make_gauge("disk_total_bytes", totals.total_size_on_disk,
                       sm::description("Holds a size of disk space in bytes reserved for data so far. "
                                       "A too high value indicates that we have some bottleneck in the writing to sstables path.")),

        sm::make_gauge("disk_active_bytes", totals.active_size_on_disk,
                       sm::description("Holds a size of disk space in bytes used for data so far. "
                                       "A too high value indicates that we have some bottleneck in the writing to sstables path.")),

        sm::make_gauge("memory_buffer_bytes", totals.buffer_list_bytes,
                       sm::description("Holds the total number of bytes in internal memory buffers.")),
    });
}

void db::commitlog::segment_manager::flush_segments(uint64_t size_to_remove) {
    if (_segments.empty()) {
        return;
    }
    // defensive copy.
    auto callbacks = boost::copy_range<std::vector<flush_handler>>(_flush_handlers | boost::adaptors::map_values);
    auto& active = _segments.back();

    // RP at "start" of segment we leave untouched.
    replay_position high(active->_desc.id, 0);

    // But if all segments are closed or we force-flush,
    // include all.
    if (!active->is_still_allocating()) {
        high = replay_position(high.id + 1, 0);
    }

    auto n = size_to_remove;

    if (size_to_remove != 0) {
        for (auto& s : _segments) {
            if (n <= s->_size_on_disk) {
                high = replay_position(s->_desc.id, db::position_type(s->_size_on_disk));
                break;
            }
            n -= s->_size_on_disk;
        }
    }

    // Now get a set of used CF ids:
    std::unordered_set<cf_id_type> ids;
    std::for_each(_segments.begin(), _segments.end() - 1, [&ids](sseg_ptr& s) {
        for (auto& id : s->_cf_dirty | boost::adaptors::map_keys) {
            ids.insert(id);
        }
    });

    clogger.debug("Flushing ({} MB) to {}", size_to_remove/(1024*1024), high);

    // For each CF id: for each callback c: call c(id, high)
    for (auto& f : callbacks) {
        for (auto& id : ids) {
            try {
                f(id, high);
            } catch (...) {
                clogger.error("Exception during flush request {}/{}: {}", id, high, std::current_exception());
            }
        }
    }
}

future<db::commitlog::segment_manager::sseg_ptr> db::commitlog::segment_manager::allocate_segment_ex(descriptor d, sstring filename, open_flags flags) {
    file_open_options opt;
    opt.extent_allocation_size_hint = max_size;
    opt.append_is_unlikely = true;

    file f;
    std::exception_ptr ep;

    try {
        f = co_await open_file_dma(filename, flags, opt);

        if ((flags & open_flags::dsync) != open_flags{}) {
            auto existing_size = (flags & open_flags::create) == open_flags{}
                ? co_await f.size()
                : 0
                ;

            // would be super nice if we just could mmap(/dev/zero) and do sendto
            // instead of this, but for now we must do explicit buffer writes.

            // if recycled (or from last run), we might have either truncated smaller or written it 
            // (slightly) larger due to final zeroing of file
            if (existing_size > max_size) {
                co_await f.truncate(max_size);
            } else if (existing_size < max_size) {
                totals.total_size_on_disk += (max_size - existing_size);

                clogger.trace("Pre-writing {} of {} KB to segment {}", (max_size - existing_size)/1024, max_size/1024, filename);

                // re-open without o_dsync for pre-alloc. The reason/rationale
                // being that we want automatic (meta)data sync from O_DSYNC for when
                // we do actual CL flushes, but here it would just result in
                // data being committed before we've reached eof/finished writing.
                // Again an argument for sendfile-like constructs I guess...
                co_await f.close();
                f = co_await open_file_dma(filename, flags & open_flags(~int(open_flags::dsync)), opt);
                co_await f.allocate(existing_size, max_size - existing_size);

                static constexpr size_t buf_size = 4 * segment::alignment;
                size_t zerofill_size = max_size - align_down(existing_size, segment::alignment);
                auto rem = zerofill_size;

                auto buf = allocate_single_buffer(buf_size);
                while (rem != 0) {
                    static constexpr size_t max_write = 128 * 1024;
                    auto n = std::min(max_write / buf_size, 1 + rem / buf_size);

                    std::vector<iovec> v;
                    v.reserve(n);
                    size_t m = 0;
                    while (m < rem && m < max_write) {
                        auto s = std::min(rem - m, buf_size);
                        v.emplace_back(iovec{ buf.get_write(), s});
                        m += s;
                    }
                    auto s = co_await f.dma_write(max_size - rem, std::move(v), service::get_local_commitlog_priority());
                    if (!s) [[unlikely]] {
                        on_internal_error(clogger, format("dma_write returned 0: max_size={} rem={} iovec.n={}", max_size, rem, n));
                    }
                    rem -= s;
                }

                // sync metadata (size/written)
                co_await f.flush();
                co_await f.close();
                f = co_await open_file_dma(filename, flags, opt);
            }
        } else {
            co_await f.truncate(max_size);
        }

        if (cfg.extensions && !cfg.extensions->commitlog_file_extensions().empty()) {
            for (auto * ext : cfg.extensions->commitlog_file_extensions()) {
                auto nf = co_await ext->wrap_file(std::move(filename), f, flags);
                if (nf) {
                    f = std::move(nf);
                }
            }
        }

        f = make_checked_file(commit_error_handler, std::move(f));
    } catch (...) {
        ep = std::current_exception();
        commit_error_handler(ep);
    }
    if (ep && f) {
        co_await f.close();
    }
    if (ep) {
        std::rethrow_exception(ep);
    }

    co_return make_shared<segment>(shared_from_this(), std::move(d), std::move(f), max_size);
}

future<> db::commitlog::segment_manager::rename_file(sstring from, sstring to) const {
    return do_io_check(commit_error_handler, [this, from = std::move(from), to = std::move(to)]() mutable {
        return seastar::rename_file(std::move(from), std::move(to)).then([this] {
            return seastar::sync_directory(cfg.commit_log_location);
        });
    });
}

future<db::commitlog::segment_manager::sseg_ptr> db::commitlog::segment_manager::allocate_segment() {
    descriptor d(next_id(), cfg.fname_prefix);
    auto dst = filename(d);
    auto flags = open_flags::wo;
    if (cfg.use_o_dsync) {
        flags |= open_flags::dsync;
    }

    if (!_recycled_segments.empty()) {
        auto src = _recycled_segments.pop();
        // Note: we have to do the rename here to ensure
        // proper descriptor id order. If we renamed in the delete call
        // that recycled the file we could potentially have
        // out-of-order files. (Sort does not help).
        clogger.debug("Using recycled segment file {} -> {}", src, dst);
        return rename_file(std::move(src), dst).then([this, d = std::move(d), dst = std::move(dst), flags] () mutable {
            return allocate_segment_ex(std::move(d), std::move(dst), flags);
        });
    }

    if (!cfg.allow_going_over_size_limit && max_disk_size != 0 && totals.total_size_on_disk >= max_disk_size) {
        clogger.debug("Disk usage ({} MB) exceeds maximum ({} MB) - allocation will wait...", totals.total_size_on_disk/(1024*1024), max_disk_size/(1024*1024));
        auto f = cfg.reuse_segments ? _recycled_segments.not_empty() :  _disk_deletions.get_shared_future();
        return f.handle_exception([this](auto ep) {
            clogger.warn("Exception while waiting for segments {}. Will retry allocation...", ep);
        }).then([this] {
            return allocate_segment();
        });
    }

    return allocate_segment_ex(std::move(d), std::move(dst), flags|open_flags::create);
}

future<db::commitlog::segment_manager::sseg_ptr> db::commitlog::segment_manager::new_segment() {
    if (_shutdown) {
        throw std::runtime_error("Commitlog has been shut down. Cannot add data");
    }

    ++_new_counter;

    if (_reserve_segments.empty()) {        
        // don't increase reserve count if we are at max, or we would go over disk limit. 
        if (_reserve_segments.max_size() < cfg.max_reserve_segments && (totals.total_size_on_disk + max_size) <= max_disk_size) {
            _reserve_segments.set_max_size(_reserve_segments.max_size() + 1);
            clogger.debug("Increased segment reserve count to {}", _reserve_segments.max_size());
        }
        // if we have no reserve and we're above/at limits, make background task a little more eager.
        if (!_shutdown && totals.total_size_on_disk >= disk_usage_threshold) {
            _timer.cancel();
            _timer.arm(std::chrono::milliseconds(0));
        }
    }

    return _reserve_segments.pop_eventually().then([this] (auto s) {
        _segments.push_back(std::move(s));
        _segments.back()->reset_sync_time();
        return make_ready_future<sseg_ptr>(_segments.back());
    });
}

future<db::commitlog::segment_manager::sseg_ptr> db::commitlog::segment_manager::active_segment(db::timeout_clock::time_point timeout) {
    // If there is no active segment, try to allocate one using new_segment(). If we time out,
    // make sure later invocations can still pick that segment up once it's ready.
    return repeat_until_value([this, timeout] () -> future<std::optional<sseg_ptr>> {
        if (!_segments.empty() && _segments.back()->is_still_allocating()) {
            return make_ready_future<std::optional<sseg_ptr>>(_segments.back());
        }
        return [this, timeout] {
            if (!_segment_allocating) {
                promise<> p;
                _segment_allocating.emplace(p.get_future());
                auto f = _segment_allocating->get_future(timeout);
                try_with_gate(_gate, [this] {
                    return new_segment().discard_result().finally([this]() {
                        _segment_allocating = std::nullopt;
                    });
                }).forward_to(std::move(p));
                return f;
            } else {
                return _segment_allocating->get_future(timeout);
            }
        }().then([] () -> std::optional<sseg_ptr> {
            return std::nullopt;
        });
    });
}

/**
 * go through all segments, clear id up to pos. if segment becomes clean and unused by this,
 * it is discarded.
 */
void db::commitlog::segment_manager::discard_completed_segments(const cf_id_type& id, const rp_set& used) {
    auto& usage = used.usage();

    clogger.debug("Discarding {}: {}", id, usage);

    for (auto&s : _segments) {
        auto i = usage.find(s->_desc.id);
        if (i != usage.end()) {
            s->mark_clean(id, i->second);
        }
    }
    discard_unused_segments();
}

void db::commitlog::segment_manager::discard_completed_segments(const cf_id_type& id) {
    clogger.debug("Discard all data for {}", id);
    for (auto&s : _segments) {
        s->mark_clean(id);
    }
    discard_unused_segments();
}

namespace db {

std::ostream& operator<<(std::ostream& out, const db::commitlog::segment& s) {
    return out << s._desc.filename();
}

std::ostream& operator<<(std::ostream& out, const db::commitlog::segment::cf_mark& m) {
    return out << (m.s._cf_dirty | boost::adaptors::map_keys);
}

std::ostream& operator<<(std::ostream& out, const db::replay_position& p) {
    return out << "{" << p.shard_id() << ", " << p.base_id() << ", " << p.pos << "}";
}

}

void db::commitlog::segment_manager::discard_unused_segments() {
    clogger.trace("Checking for unused segments ({} active)", _segments.size());

    std::erase_if(_segments, [=](sseg_ptr s) {
        if (s->can_delete()) {
            clogger.debug("Segment {} is unused", *s);
            return true;
        }
        if (s->is_still_allocating()) {
            clogger.debug("Not safe to delete segment {}; still allocating.", s);
        } else if (!s->is_clean()) {
            clogger.debug("Not safe to delete segment {}; dirty is {}", s, segment::cf_mark {*s});
        } else {
            clogger.debug("Not safe to delete segment {}; disk ops pending", s);
        }
        return false;
    });

    // launch in background, but guard with gate so this deletion is
    // sure to finish in shutdown, because at least through this path,
    // segments on deletion queue could be non-empty, and we don't want
    // those accidentally left around for replay.
    if (!_shutdown) {
        (void)with_gate(_gate, [this] {
            return do_pending_deletes();
        });
    }
}

future<> db::commitlog::segment_manager::clear_reserve_segments() {
    while (!_reserve_segments.empty()) {
        _reserve_segments.pop();
    }

    std::vector<sstring> tmp;
    tmp.reserve(_recycled_segments.size());

    _recycled_segments.consume([&](sstring s) {
        tmp.emplace_back(std::move(s));
        return true;
    });

    auto i = tmp.begin();
    auto e = tmp.end();

    return parallel_for_each(i, e, [this](const sstring& filename) {
        clogger.debug("Deleting recycled segment file {}", filename);
        return delete_file(filename);
    }).finally([this, tmp = std::move(tmp)] {
        return do_pending_deletes();
    });
}

future<> db::commitlog::segment_manager::sync_all_segments() {
    clogger.debug("Issuing sync for all segments");
    return parallel_for_each(_segments, [] (sseg_ptr s) {
        return s->sync().then([](sseg_ptr s) {
            clogger.debug("Synced segment {}", *s);
        });
    });
}

future<> db::commitlog::segment_manager::shutdown_all_segments() {
    clogger.debug("Issuing shutdown for all segments");
    return parallel_for_each(_segments, [] (sseg_ptr s) {
        return s->shutdown().then([](sseg_ptr s) {
            clogger.debug("Shutdown segment {}", *s);
        });
    });
}

future<> db::commitlog::segment_manager::shutdown() {
    if (!_shutdown_promise) {
        _shutdown_promise = shared_promise<>();

        // Wait for all pending requests to finish. Need to sync first because segments that are
        // alive may be holding semaphore permits.
        auto block_new_requests = get_units(_request_controller, max_request_controller_units());
        return sync_all_segments().then_wrapped([this, block_new_requests = std::move(block_new_requests)] (future<> f) mutable {
            if (f.failed()) {
                clogger.error("Syncing all segments failed during shutdown: {}. Aborting.", f.get_exception());
                abort();
            }
            return std::move(block_new_requests).then([this] (auto permits) {
                _timer.cancel(); // no more timer calls
                _shutdown = true; // no re-arm, no create new segments.
                // Now first wait for periodic task to finish, then sync and close all
                // segments, flushing out any remaining data.
                return _gate.close().finally([this, permits = std::move(permits)] () mutable {
                    return shutdown_all_segments().handle_exception([permits = std::move(permits)] (std::exception_ptr ex) {
                        clogger.error("Shutting down all segments failed during shutdown: {}. Aborting.", ex);
                        abort();
                    });
                });
            });
        }).finally([this] {
            discard_unused_segments();
            // Now that the gate is closed and requests completed we are sure nobody else will pop()
            return clear_reserve_segments().finally([this] {
                return std::move(_reserve_replenisher).then_wrapped([this] (auto f) {
                    // Could be cleaner with proper seastar support
                    if (f.failed()) {
                        _shutdown_promise->set_exception(f.get_exception());
                    } else {
                        _shutdown_promise->set_value();
                    }
                });
            });
        });
    }
    return _shutdown_promise->get_shared_future();
}

void db::commitlog::segment_manager::add_file_to_delete(sstring filename, descriptor d) {
    assert(!_files_to_delete.contains(filename));
    _files_to_delete.emplace(std::move(filename), std::move(d));
}

void db::commitlog::segment_manager::add_file_to_close(file f) {
    _files_to_close.emplace_back(std::move(f));
}

future<> db::commitlog::segment_manager::delete_file(const sstring& filename) {
    return seastar::file_size(filename).then([this, filename](uint64_t size) {
        clogger.debug("Deleting segment file {}", filename);
        return commit_io_check(&seastar::remove_file, filename).then([this, size] {
            clogger.trace("Reclaimed {} MB", size/(1024*1024));
            totals.total_size_on_disk -= size;
            auto p = std::exchange(_disk_deletions, {});
            p.set_value();
        });
    });
}

future<> db::commitlog::segment_manager::delete_segments(std::vector<sstring> files) {
    if (files.empty()) {
        co_return;
    }

    clogger.debug("Delete segments {}", files);

    std::exception_ptr recycle_error;

    while (!files.empty()) {
        auto filename = std::move(files.back());
        files.pop_back();

        try {
            auto exts = cfg.extensions;
            if (exts && !exts->commitlog_file_extensions().empty()) {
                for (auto& ext : exts->commitlog_file_extensions()) {
                    co_await ext->before_delete(filename);
                }
            }

            // We allow reuse of the segment if the current disk size is less than shard max.
            if (!_shutdown && cfg.reuse_segments) {
                auto usage = totals.total_size_on_disk;
                auto recycle = usage <= max_disk_size;

                // if total size is not a multiple of segment size, we need
                // to check if we are the overlap segment, and noone else
                // can be recycled. If so, let this one live so allocation
                // can proceed. We assume/hope a future delete will kill
                // files down to under the threshold, but we should expect
                // to stomp around nearest multiple of segment size, not 
                // the actual limit.
                if (!recycle && _recycled_segments.empty() && files.empty()) {
                    auto size = co_await seastar::file_size(filename);
                    recycle = (usage - size) <= max_disk_size;
                }

                if (recycle) {
                    descriptor d(next_id(), "Recycled-" + cfg.fname_prefix);
                    auto dst = this->filename(d);

                    clogger.debug("Recycling segment file {}", filename);
                    // must rename the file since we must ensure the
                    // data is not replayed. Changing the name will
                    // cause header ID to be invalid in the file -> ignored
                    try {
                        co_await rename_file(filename, dst);
                        auto b = _recycled_segments.push(std::move(dst));
                        assert(b); // we set this to max_size_t so...
                        continue;
                    } catch (...) {
                        recycle_error = std::current_exception();
                        // fallthrough
                    }
                }
            }
            co_await delete_file(filename);
        } catch (...) {
            clogger.error("Could not delete segment {}: {}", filename, std::current_exception());
        }
    }

    // #8376 - if we had an error in recycling (disk rename?), and no elements
    // are available, we could have waiters hoping they will get segements.
    // abort the queue (wakes up any existing waiters - futures), and let them
    // retry. Since we did deletions instead, disk footprint should allow
    // for new allocs at least. Or more likely, everything is broken, but
    // we will at least make more noise.
    if (recycle_error && _recycled_segments.empty()) {
        _recycled_segments.abort(recycle_error);
        // and ensure next lap(s) still has a queue
        _recycled_segments = queue<sstring>(std::numeric_limits<size_t>::max());
    }
}

future<> db::commitlog::segment_manager::do_pending_deletes() {
    auto ftc = std::exchange(_files_to_close, {});
    auto ftd = std::exchange(_files_to_delete, {});
    auto i = ftc.begin();
    auto e = ftc.end();
    return parallel_for_each(i, e, [](file & f) {
        return f.close();
    }).then([this, ftc = std::move(ftc), ftd = std::move(ftd)] {
        return delete_segments(boost::copy_range<std::vector<sstring>>(ftd | boost::adaptors::map_keys));
    });
}

future<> db::commitlog::segment_manager::orphan_all() {
    _segments.clear();
    return clear_reserve_segments();
}

/*
 * Sync all segments, then clear them out. To ensure all ops are done.
 * (Assumes you have barriered adding ops!)
 * Only use from tests.
 */
future<> db::commitlog::segment_manager::clear() {
    clogger.debug("Clearing commitlog");
    return shutdown().then([this] {
        clogger.debug("Clearing all segments");
        for (auto& s : _segments) {
            s->mark_clean();
        }
        return orphan_all();
    });
}
/**
 * Called by timer in periodic mode.
 */
void db::commitlog::segment_manager::sync() {
    for (auto s : _segments) {
        (void)s->sync(); // we do not care about waiting...
    }
}

void db::commitlog::segment_manager::on_timer() {
    // Gate, because we are starting potentially blocking ops
    // without waiting for them, so segement_manager could be shut down
    // while they are running.
    (void)seastar::with_gate(_gate, [this] {
        if (cfg.mode != sync_mode::BATCH) {
            sync();
        }
        // IFF a new segment was put in use since last we checked, and we're
        // above threshold, request flush.
        if (_new_counter > 0) {
            auto max = disk_usage_threshold;
            auto cur = totals.active_size_on_disk;
            if (max != 0 && cur >= max) {
                _new_counter = 0;
                clogger.debug("Used size on disk {} MB exceeds local threshold {} MB", cur / (1024 * 1024), max / (1024 * 1024));
                flush_segments(cur - max);
            }
        }
        return do_pending_deletes();
    });
    arm();
}

std::vector<sstring> db::commitlog::segment_manager::get_active_names() const {
    std::vector<sstring> res;
    for (auto i: _segments) {
        if (!i->is_unused()) {
            // Each shared is located in its own directory
            res.push_back(cfg.commit_log_location + "/" + i->get_segment_name());
        }
    }
    return res;
}

uint64_t db::commitlog::segment_manager::get_num_dirty_segments() const {
    return std::count_if(_segments.begin(), _segments.end(), [](sseg_ptr s) {
        return !s->is_still_allocating() && !s->is_clean();
    });
}

uint64_t db::commitlog::segment_manager::get_num_active_segments() const {
    return std::count_if(_segments.begin(), _segments.end(), [](sseg_ptr s) {
        return s->is_still_allocating();
    });
}

temporary_buffer<char> db::commitlog::segment_manager::allocate_single_buffer(size_t s) {
    return temporary_buffer<char>::aligned(segment::alignment, s);
}

db::commitlog::segment_manager::buffer_type db::commitlog::segment_manager::acquire_buffer(size_t s) {
    s = align_up(s, segment::default_size);
    auto fragment_count = s / segment::default_size;

    std::vector<temporary_buffer<char>> buffers;
    buffers.reserve(fragment_count);
    while (buffers.size() < fragment_count) {
        buffers.emplace_back(allocate_single_buffer(segment::default_size));
    }
    clogger.trace("Allocated {} k buffer", s / 1024);
    return fragmented_temporary_buffer(std::move(buffers), s);
}

/**
 * Add mutation.
 */
future<db::rp_handle> db::commitlog::add(const cf_id_type& id,
        size_t size, db::timeout_clock::time_point timeout, db::commitlog::force_sync sync, serializer_func func) {
    class serializer_func_entry_writer final : public entry_writer {
        cf_id_type _id;
        serializer_func _func;
        size_t _size;
    public:
        db::rp_handle res;

        serializer_func_entry_writer(const cf_id_type& id, size_t sz, serializer_func func, db::commitlog::force_sync sync)
            : entry_writer(sync), _id(id), _func(std::move(func)), _size(sz)
        {}
        const cf_id_type& id(size_t) const { return _id; }
        size_t size(segment&, size_t) override { return _size; }
        size_t size(segment&) override { return _size; }
        size_t size() const override { return _size; }
        void write(segment&, output& out, size_t) const override {
            _func(out);
        }
        void result(size_t, rp_handle h) override {
            res = std::move(h);
        }
    };
    auto writer = ::make_shared<serializer_func_entry_writer>(id, size, std::move(func), sync);
    return _segment_manager->allocate_when_possible(writer, timeout).then([writer] {
        return std::move(writer->res);
    });
}

future<db::rp_handle> db::commitlog::add_entry(const cf_id_type& id, const commitlog_entry_writer& cew, timeout_clock::time_point timeout)
{
    assert(id == cew.schema()->id());

    class cl_entry_writer final : public entry_writer {
        commitlog_entry_writer _writer;
    public:
        rp_handle res;
        cl_entry_writer(const commitlog_entry_writer& wr) 
            : entry_writer(wr.sync()), _writer(wr) 
        {}
        const cf_id_type& id(size_t) const override {
            return _writer.schema()->id();
        }
        size_t size(segment& seg) override {
            _writer.set_with_schema(!seg.is_schema_version_known(_writer.schema()));
            return _writer.size();
        }
        size_t size(segment& seg, size_t) override {
            return size(seg);
        }
        size_t size() const override {
            return _writer.mutation_size();
        }
        void write(segment& seg, output& out, size_t) const override {
            if (_writer.with_schema()) {
                seg.add_schema_version(_writer.schema());
            }
            _writer.write(out);
        }
        void result(size_t, rp_handle h) override {
            res = std::move(h);
        }
    };
    auto writer = ::make_shared<cl_entry_writer>(cew);
    return _segment_manager->allocate_when_possible(writer, timeout).then([writer] {
        return std::move(writer->res);
    });
}

future<std::vector<db::rp_handle>> 
db::commitlog::add_entries(std::vector<commitlog_entry_writer> entry_writers, db::timeout_clock::time_point timeout) {
    class cl_entries_writer final : public entry_writer {
        std::vector<commitlog_entry_writer> _writers;
        std::unordered_set<table_schema_version> _known;
    public:
        std::vector<rp_handle> res;

        cl_entries_writer(force_sync sync, std::vector<commitlog_entry_writer> entry_writers)
            : entry_writer(sync, entry_writers.size()), _writers(std::move(entry_writers))
        {
            res.reserve(_writers.size());
        }
        const cf_id_type& id(size_t i) const override {
            return _writers.at(i).schema()->id();
        }
        size_t size(segment& seg) override {
            size_t res = 0;
            for (auto i = _writers.begin(), e = _writers.end(); i != e; ++i) {
                auto known = seg.is_schema_version_known(i->schema());
                if (!known) {
                    known = _known.contains(i->schema()->version());
                }
                if (!known) {
                    _known.emplace(i->schema()->version());
                }
                i->set_with_schema(!known);
                res += i->size();
            }
            return res;
        }
        size_t size(segment& seg, size_t i) override {
            return _writers.at(i).size(); // we have already set schema known/unknown
        }
        size_t size() const override {
            return std::accumulate(_writers.begin(), _writers.end(), size_t(0), [](size_t acc, const commitlog_entry_writer& w) {
                return w.mutation_size() + acc;
            });
        }
        void write(segment& seg, output& out, size_t i) const override {
            auto& w = _writers.at(i);
            if (w.with_schema()) {
                seg.add_schema_version(w.schema());
            }
            w.write(out);
        }
        void result(size_t i, rp_handle h) override {
            assert(i == res.size());
            res.emplace_back(std::move(h));
        }
    };

    force_sync sync(std::any_of(entry_writers.begin(), entry_writers.end(), [](auto& w) { return bool(w.sync()); }));
    auto writer = ::make_shared<cl_entries_writer>(sync, std::move(entry_writers));
    return _segment_manager->allocate_when_possible(writer, timeout).then([writer] {
        return std::move(writer->res);
    });
}

db::commitlog::commitlog(config cfg)
        : _segment_manager(::make_shared<segment_manager>(std::move(cfg))) {
}

db::commitlog::commitlog(commitlog&& v) noexcept
        : _segment_manager(std::move(v._segment_manager)) {
}

db::commitlog::~commitlog()
{}

future<db::commitlog> db::commitlog::create_commitlog(config cfg) {
    commitlog c(std::move(cfg));
    auto f = c._segment_manager->init();
    return f.then([c = std::move(c)]() mutable {
        return make_ready_future<commitlog>(std::move(c));
    });
}

db::commitlog::flush_handler_anchor::flush_handler_anchor(flush_handler_anchor&& f)
    : _cl(f._cl), _id(f._id)
{
    f._id = 0;
}

db::commitlog::flush_handler_anchor::flush_handler_anchor(commitlog& cl, flush_handler_id id)
    : _cl(cl), _id(id)
{}

db::commitlog::flush_handler_anchor::~flush_handler_anchor() {
    unregister();
}

db::commitlog::flush_handler_id db::commitlog::flush_handler_anchor::release() {
    flush_handler_id id = 0;
    std::swap(_id, id);
    return id;
}

void db::commitlog::flush_handler_anchor::unregister() {
    auto id = release();
    if (id != 0) {
        _cl.remove_flush_handler(id);
    }
}

db::commitlog::flush_handler_anchor db::commitlog::add_flush_handler(flush_handler h) {
    return flush_handler_anchor(*this, _segment_manager->add_flush_handler(std::move(h)));
}

void db::commitlog::remove_flush_handler(flush_handler_id id) {
    _segment_manager->remove_flush_handler(id);
}

void db::commitlog::discard_completed_segments(const cf_id_type& id, const rp_set& used) {
    _segment_manager->discard_completed_segments(id, used);
}

void db::commitlog::discard_completed_segments(const cf_id_type& id) {
    _segment_manager->discard_completed_segments(id);
}

future<> db::commitlog::sync_all_segments() {
    return _segment_manager->sync_all_segments();
}

future<> db::commitlog::shutdown() {
    return _segment_manager->shutdown();
}

future<> db::commitlog::release() {
    return _segment_manager->orphan_all();
}

size_t db::commitlog::max_record_size() const {
    return _segment_manager->max_mutation_size - segment::entry_overhead_size;
}

uint64_t db::commitlog::max_active_writes() const {
    return _segment_manager->cfg.max_active_writes;
}

uint64_t db::commitlog::max_active_flushes() const {
    return _segment_manager->cfg.max_active_flushes;
}

future<> db::commitlog::clear() {
    return _segment_manager->clear();
}

const db::commitlog::config& db::commitlog::active_config() const {
    return _segment_manager->cfg;
}

// No commit_io_check needed in the log reader since the database will fail
// on error at startup if required
future<>
db::commitlog::read_log_file(const sstring& filename, const sstring& pfx, seastar::io_priority_class read_io_prio_class, commit_load_reader_func next, position_type off, const db::extensions* exts) {
    struct work {
    private:
        file_input_stream_options make_file_input_stream_options(seastar::io_priority_class read_io_prio_class) {
            file_input_stream_options fo;
            fo.buffer_size = db::commitlog::segment::default_size;
            fo.read_ahead = 10;
            fo.io_priority_class = read_io_prio_class;
            return fo;
        }
    public:
        file f;
        descriptor d;
        stream<buffer_and_replay_position> s;
        input_stream<char> fin;
        input_stream<char> r;
        uint64_t id = 0;
        size_t pos = 0;
        size_t next = 0;
        size_t start_off = 0;
        size_t file_size = 0;
        size_t corrupt_size = 0;
        bool eof = false;
        bool header = true;
        bool failed = false;
        fragmented_temporary_buffer::reader frag_reader;

        work(file f, descriptor din, seastar::io_priority_class read_io_prio_class, position_type o = 0)
                : f(f), d(din), fin(make_file_input_stream(f, 0, make_file_input_stream_options(read_io_prio_class))), start_off(o) {
        }
        work(work&&) = default;

        bool advance(const fragmented_temporary_buffer& buf) {
            pos += buf.size_bytes();
            if (buf.size_bytes() == 0) {
                eof = true;
            }
            return !eof;
        }
        bool end_of_file() const {
            return eof;
        }
        bool end_of_chunk() const {
            return eof || next == pos;
        }
        future<> skip(size_t bytes) {
            pos += bytes;
            if (pos > file_size) {
                eof = true;
                pos = file_size;
            }
            return fin.skip(bytes);
        }
        future<> stop() {
            eof = true;
            return make_ready_future<>();
        }
        future<> fail() {
            failed = true;
            return stop();
        }
        future<> read_header() {
            return frag_reader.read_exactly(fin, segment::descriptor_header_size).then([this](fragmented_temporary_buffer buf) {
                if (!advance(buf)) {
                    // zero length file. accept it just to be nice.
                    return make_ready_future<>();
                }
                // Will throw if we got eof
                auto in = buf.get_istream();
                auto magic = read<uint32_t>(in);
                auto ver = read<uint32_t>(in);
                auto id = read<uint64_t>(in);
                auto checksum = read<uint32_t>(in);

                if (magic == 0 && ver == 0 && id == 0 && checksum == 0) {
                    // let's assume this was an empty (pre-allocated)
                    // file. just skip it.
                    return stop();
                }
                if (id != d.id) {
                    // filename and id in file does not match.
                    // assume not valid/recycled.
                    return stop();
                }

                if (magic != segment::segment_magic) {
                    throw invalid_segment_format();
                }
                crc32_nbo crc;
                crc.process(ver);
                crc.process<int32_t>(id & 0xffffffff);
                crc.process<int32_t>(id >> 32);

                auto cs = crc.checksum();
                if (cs != checksum) {
                    throw header_checksum_error();
                }

                this->id = id;
                this->next = 0;

                return make_ready_future<>();
            });
        }
        future<> read_chunk() {
            return frag_reader.read_exactly(fin, segment::segment_overhead_size).then([this](fragmented_temporary_buffer buf) {
                auto start = pos;

                if (!advance(buf)) {
                    return make_ready_future<>();
                }

                auto in = buf.get_istream();
                auto next = read<uint32_t>(in);
                auto checksum = read<uint32_t>(in);

                if (next == 0 && checksum == 0) {
                    // in a pre-allocating world, this means eof
                    return stop();
                }

                crc32_nbo crc;
                crc.process<int32_t>(id & 0xffffffff);
                crc.process<int32_t>(id >> 32);
                crc.process<uint32_t>(start);

                auto cs = crc.checksum();
                if (cs != checksum) {
                    // if a chunk header checksum is broken, we shall just assume that all
                    // remaining is as well. We cannot trust the "next" pointer, so...
                    clogger.debug("Checksum error in segment chunk at {}.", start);
                    corrupt_size += (file_size - pos);
                    return stop();
                }

                this->next = next;

                if (start_off >= next) {
                    return skip(next - pos);
                }

                return do_until(std::bind(&work::end_of_chunk, this), std::bind(&work::read_entry, this));
            });
        }

        using produce_func = std::function<future<>(buffer_and_replay_position, uint32_t)>;

        future<> produce(buffer_and_replay_position bar) {
            return s.produce(std::move(bar)).handle_exception([this](auto ep) {
                return fail();
            });
        }

        future<> read_entry() {
            return do_read_entry(std::bind(&work::produce, this, std::placeholders::_1));
        }

        future<> do_read_entry(produce_func pf) {
            static constexpr size_t entry_header_size = segment::entry_overhead_size - sizeof(uint32_t);

            /**
             * #598 - Must check that data left in chunk is enough to even read an entry.
             * If not, this is small slack space in the chunk end, and we should just go
             * to the next.
             */
            assert(pos <= next);
            if ((pos + entry_header_size) >= next) {
                co_await skip(next - pos);
                co_return;
            }

            auto buf = co_await frag_reader.read_exactly(fin, entry_header_size);

            replay_position rp(id, position_type(pos));

            if (!advance(buf)) {
                co_return;
            }

            auto in = buf.get_istream();
            auto size = read<uint32_t>(in);
            auto checksum = read<uint32_t>(in);

            crc32_nbo crc;
            crc.process(size);

            // check for multi-entry
            if (d.ver >= descriptor::segment_version_2 && size == segment::multi_entry_size_magic) {
                auto actual_size = checksum;
                auto end = pos + actual_size - entry_header_size - sizeof(uint32_t);

                assert(end <= next);
                // really small read...
                buf = co_await frag_reader.read_exactly(fin, sizeof(uint32_t));
                in = buf.get_istream();
                checksum = read<uint32_t>(in);

                advance(buf);
                crc.process(actual_size);
                // verify header crc.
                if (actual_size < 2 * segment::entry_overhead_size || crc.checksum() != checksum) {
                    auto slack = next - pos;
                    if (size != 0) {
                        clogger.debug("Segment entry at {} has broken header. Skipping to next chunk ({} bytes)", rp, slack);
                        corrupt_size += slack;
                    }
                    co_await skip(slack);
                    co_return;
                }

                std::vector<buffer_and_replay_position> tmp;
                tmp.reserve(10);
                // now read all sub-entries into buffers, and collect crc.
                while (pos < end) {
                    co_await do_read_entry([&](buffer_and_replay_position br, uint32_t checksum) -> future<> {
                        tmp.emplace_back(std::move(br));
                        crc.process(checksum);
                        co_return;
                    });
                }
                // and verify crc.
                buf = co_await frag_reader.read_exactly(fin, sizeof(uint32_t)); 
                in = buf.get_istream();
                checksum = read<uint32_t>(in);

                advance(buf);

                if (checksum != crc.checksum()) {
                    auto slack = next - pos;
                    clogger.debug("Segment entry at {} has broken header. Skipping to next chunk ({} bytes)", rp, actual_size);
                    corrupt_size += actual_size;
                    co_await skip(slack);
                    co_return;
                }
                // all is ok. send data to subscriber.
                for (auto&& br : tmp) {
                    co_await produce(std::move(br));
                    if (failed) {
                        break;
                    }
                }
                co_return;
            }

            if (size < 3 * sizeof(uint32_t) || checksum != crc.checksum()) {
                auto slack = next - pos;
                if (size != 0) {
                    clogger.debug("Segment entry at {} has broken header. Skipping to next chunk ({} bytes)", rp, slack);
                    corrupt_size += slack;
                }
                // size == 0 -> special scylla case: zero padding due to dma blocks
                co_await skip(slack);
                co_return;
            }

            buf = co_await frag_reader.read_exactly(fin, size - entry_header_size);

            advance(buf);

            in = buf.get_istream();
            auto data_size = size - segment::entry_overhead_size;
            in.skip(data_size);
            checksum = read<uint32_t>(in);

            buf.remove_suffix(buf.size_bytes() - data_size);
            crc.process_fragmented(fragmented_temporary_buffer::view(buf));

            if (crc.checksum() != checksum) {
                // If we're getting a checksum error here, most likely the rest of
                // the file will be corrupt as well. But it does not hurt to retry.
                // Just go to the next entry (since "size" in header seemed ok).
                clogger.debug("Segment entry at {} checksum error. Skipping {} bytes", rp, size);
                corrupt_size += size;
                co_return;
            }

            co_await pf({std::move(buf), rp}, checksum);
        }

        future<> read_file() {
            return f.size().then([this](uint64_t size) {
                file_size = size;
            }).then([this] {
                return read_header().then(
                        [this] {
                            return do_until(std::bind(&work::end_of_file, this), std::bind(&work::read_chunk, this));
                }).then([this] {
                  if (corrupt_size > 0) {
                      throw segment_data_corruption_error("Data corruption", corrupt_size);
                  }
                });
            }).finally([this] {
                return fin.close();
            });
        }
    };

    auto bare_filename = std::filesystem::path(filename).filename().string();
    if (bare_filename.rfind(pfx, 0) != 0) {
        return make_ready_future<>();
    }

    auto fut = do_io_check(commit_error_handler, [&] {
        auto fut = open_file_dma(filename, open_flags::ro);
        if (exts && !exts->commitlog_file_extensions().empty()) {
            for (auto * ext : exts->commitlog_file_extensions()) {
                fut = fut.then([ext, filename](file f) {
                   return ext->wrap_file(filename, f, open_flags::ro).then([f](file nf) mutable {
                       return nf ? nf : std::move(f);
                   });
                });
            }
        }
        return fut;
    });

    return fut.then([off, next, read_io_prio_class, pfx, filename] (file f) {
        f = make_checked_file(commit_error_handler, std::move(f));
        descriptor d(filename, pfx);
        auto w = make_lw_shared<work>(std::move(f), d, read_io_prio_class, off);
        auto ret = w->s.listen(next).done();

        return w->s.started().then(std::bind(&work::read_file, w.get())).then([w] {
            if (!w->failed) {
                w->s.close();
            }
        }).handle_exception([w](auto ep) {
            w->s.set_exception(ep);
        }).finally([ret = std::move(ret)] () mutable {
            return std::move(ret);
        });
    });
}

std::vector<sstring> db::commitlog::get_active_segment_names() const {
    return _segment_manager->get_active_names();
}

uint64_t db::commitlog::disk_limit() const {
    return _segment_manager->max_disk_size;
}

uint64_t db::commitlog::disk_footprint() const {
    return _segment_manager->totals.total_size_on_disk;
}

uint64_t db::commitlog::get_total_size() const {
    return _segment_manager->totals.active_size_on_disk + _segment_manager->totals.buffer_list_bytes;
}

uint64_t db::commitlog::get_completed_tasks() const {
    return _segment_manager->totals.allocation_count;
}

uint64_t db::commitlog::get_flush_count() const {
    return _segment_manager->totals.flush_count;
}

uint64_t db::commitlog::get_pending_tasks() const {
    return _segment_manager->totals.pending_flushes;
}

uint64_t db::commitlog::get_pending_flushes() const {
    return _segment_manager->totals.pending_flushes;
}

uint64_t db::commitlog::get_pending_allocations() const {
    return _segment_manager->pending_allocations();
}

uint64_t db::commitlog::get_flush_limit_exceeded_count() const {
    return _segment_manager->totals.flush_limit_exceeded;
}

uint64_t db::commitlog::get_num_segments_created() const {
    return _segment_manager->totals.segments_created;
}

uint64_t db::commitlog::get_num_segments_destroyed() const {
    return _segment_manager->totals.segments_destroyed;
}

uint64_t db::commitlog::get_num_dirty_segments() const {
    return _segment_manager->get_num_dirty_segments();
}

uint64_t db::commitlog::get_num_active_segments() const {
    return _segment_manager->get_num_active_segments();
}

future<std::vector<db::commitlog::descriptor>> db::commitlog::list_existing_descriptors() const {
    return list_existing_descriptors(active_config().commit_log_location);
}

future<std::vector<db::commitlog::descriptor>> db::commitlog::list_existing_descriptors(const sstring& dir) const {
    return _segment_manager->list_descriptors(dir);
}

future<std::vector<sstring>> db::commitlog::list_existing_segments() const {
    return list_existing_segments(active_config().commit_log_location);
}

future<std::vector<sstring>> db::commitlog::list_existing_segments(const sstring& dir) const {
    return list_existing_descriptors(dir).then([dir](auto descs) {
        std::vector<sstring> paths;
        std::transform(descs.begin(), descs.end(), std::back_inserter(paths), [&](auto& d) {
           return dir + "/" + d.filename();
        });
        return make_ready_future<std::vector<sstring>>(std::move(paths));
    });
}

std::vector<sstring> db::commitlog::get_segments_to_replay() const {
    return std::move(_segment_manager->_segments_to_replay);
}

future<> db::commitlog::delete_segments(std::vector<sstring> files) const {
    return _segment_manager->delete_segments(std::move(files));
}

db::rp_handle::rp_handle() noexcept
{}

db::rp_handle::rp_handle(shared_ptr<cf_holder> h, cf_id_type cf, replay_position rp) noexcept
    : _h(std::move(h)), _cf(cf), _rp(rp)
{}

db::rp_handle::rp_handle(rp_handle&& v) noexcept
    : _h(std::move(v._h)), _cf(v._cf), _rp(std::exchange(v._rp, {}))
{}

db::rp_handle& db::rp_handle::operator=(rp_handle&& v) noexcept {
    if (this != &v) {
        this->~rp_handle();
        new (this) rp_handle(std::move(v));
    }
    return *this;
}

db::rp_handle::~rp_handle() {
    if (_rp != replay_position() && _h) {
        _h->release_cf_count(_cf);
    }
}

db::replay_position db::rp_handle::release() {
    return std::exchange(_rp, {});
}
