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
 * Modified by Cloudius Systems
 * Copyright 2015 Cloudius Systems
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
#include <unordered_map>
#include <unordered_set>

#include <core/align.hh>
#include <core/reactor.hh>
#include <core/scollectd.hh>
#include <core/future-util.hh>
#include <core/file.hh>
#include <core/rwlock.hh>
#include <core/gate.hh>
#include <core/fstream.hh>
#include <seastar/core/memory.hh>
#include <net/byteorder.hh>

#include "commitlog.hh"
#include "db/config.hh"
#include "utils/data_input.hh"
#include "utils/crc.hh"
#include "utils/runtime.hh"
#include "log.hh"
#include "commitlog_entry.hh"
#include "service/priority_manager.hh"

static logging::logger logger("commitlog");

class crc32_nbo {
    crc32 _c;
public:
    template <typename T>
    void process(T t) {
        _c.process(net::hton(t));
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
};

db::commitlog::config::config(const db::config& cfg)
    : commit_log_location(cfg.commitlog_directory())
    , commitlog_total_space_in_mb(cfg.commitlog_total_space_in_mb() >= 0 ? cfg.commitlog_total_space_in_mb() : memory::stats().total_memory() >> 20)
    , commitlog_segment_size_in_mb(cfg.commitlog_segment_size_in_mb())
    , commitlog_sync_period_in_ms(cfg.commitlog_sync_batch_window_in_ms())
    , mode(cfg.commitlog_sync() == "batch" ? sync_mode::BATCH : sync_mode::PERIODIC)
{}

db::commitlog::descriptor::descriptor(segment_id_type i, uint32_t v)
        : id(i), ver(v) {
}

db::commitlog::descriptor::descriptor(replay_position p)
        : descriptor(p.id) {
}

db::commitlog::descriptor::descriptor(std::pair<uint64_t, uint32_t> p)
        : descriptor(p.first, p.second) {
}

db::commitlog::descriptor::descriptor(sstring filename)
        : descriptor([filename]() {
            std::smatch m;
            // match both legacy and new version of commitlogs Ex: CommitLog-12345.log and CommitLog-4-12345.log.
                std::regex rx("(?:.*/)?" + FILENAME_PREFIX + "((\\d+)(" + SEPARATOR + "\\d+)?)" + FILENAME_EXTENSION);
                std::string sfilename = filename;
                if (!std::regex_match(sfilename, m, rx)) {
                    throw std::domain_error("Cannot parse the version of the file: " + filename);
                }
                if (m[3].length() == 0) {
                    // CMH. Can most likely ignore this
                    throw std::domain_error("Commitlog segment is too old to open; upgrade to 1.2.5+ first");
                }

                segment_id_type id = std::stoull(m[3].str().substr(1));
                uint32_t ver = std::stoul(m[2].str());

                return std::make_pair(id, ver);
            }()) {
}

sstring db::commitlog::descriptor::filename() const {
    return FILENAME_PREFIX + std::to_string(ver) + SEPARATOR
            + std::to_string(id) + FILENAME_EXTENSION;
}

db::commitlog::descriptor::operator db::replay_position() const {
    return replay_position(id);
}

const std::string db::commitlog::descriptor::SEPARATOR("-");
const std::string db::commitlog::descriptor::FILENAME_PREFIX(
        "CommitLog" + SEPARATOR);
const std::string db::commitlog::descriptor::FILENAME_EXTENSION(".log");

class db::commitlog::segment_manager {
public:
    config cfg;
    const uint64_t max_size;
    const uint64_t max_mutation_size;
    // Divide the size-on-disk threshold by #cpus used, since we assume
    // we distribute stuff more or less equally across shards.
    const uint64_t max_disk_size; // per-shard

    bool _shutdown = false;

    semaphore _new_segment_semaphore;
    semaphore _write_semaphore;
    semaphore _flush_semaphore;

    scollectd::registrations _regs;

    // TODO: verify that we're ok with not-so-great granularity
    using clock_type = lowres_clock;
    using time_point = clock_type::time_point;
    using sseg_ptr = lw_shared_ptr<segment>;

    struct stats {
        uint64_t cycle_count = 0;
        uint64_t flush_count = 0;
        uint64_t allocation_count = 0;
        uint64_t bytes_written = 0;
        uint64_t bytes_slack = 0;
        uint64_t segments_created = 0;
        uint64_t segments_destroyed = 0;
        uint64_t pending_writes = 0;
        uint64_t pending_flushes = 0;
        uint64_t pending_allocations = 0;
        uint64_t write_limit_exceeded = 0;
        uint64_t flush_limit_exceeded = 0;
        uint64_t total_size = 0;
        uint64_t buffer_list_bytes = 0;
        uint64_t total_size_on_disk = 0;
    };

    stats totals;

    future<> begin_write() {
        _gate.enter();
        ++totals.pending_writes; // redundant, given semaphore. but easier to read
        if (totals.pending_writes >= cfg.max_active_writes) {
            ++totals.write_limit_exceeded;
            logger.trace("Write ops overflow: {}. Will block.", totals.pending_writes);
        }
        return _write_semaphore.wait();
    }
    void end_write() {
        _write_semaphore.signal();
        --totals.pending_writes;
        _gate.leave();
    }

    future<> begin_flush() {
        _gate.enter();
        ++totals.pending_flushes;
        if (totals.pending_flushes >= cfg.max_active_flushes) {
            ++totals.flush_limit_exceeded;
            logger.trace("Flush ops overflow: {}. Will block.", totals.pending_flushes);
        }
        return _flush_semaphore.wait();
    }
    void end_flush() {
        _flush_semaphore.signal();
        --totals.pending_flushes;
        _gate.leave();
    }

    bool should_wait_for_write() const {
        return _write_semaphore.waiters() > 0 || _flush_semaphore.waiters() > 0;
    }

    segment_manager(config c)
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
        , max_size(std::min<size_t>(std::numeric_limits<position_type>::max(), std::max<size_t>(cfg.commitlog_segment_size_in_mb, 1) * 1024 * 1024))
        , max_mutation_size(max_size >> 1)
        , max_disk_size(size_t(std::ceil(cfg.commitlog_total_space_in_mb / double(smp::count))) * 1024 * 1024)
        , _write_semaphore(cfg.max_active_writes)
        , _flush_semaphore(cfg.max_active_flushes)
    {
        assert(max_size > 0);

        logger.trace("Commitlog {} maximum disk size: {} MB / cpu ({} cpus)",
                cfg.commit_log_location, max_disk_size / (1024 * 1024),
                smp::count);

        _regs = create_counters();
    }
    ~segment_manager() {
        logger.trace("Commitlog {} disposed", cfg.commit_log_location);
    }

    uint64_t next_id() {
        return ++_ids;
    }

    future<> init();
    future<sseg_ptr> new_segment();
    future<sseg_ptr> active_segment();
    future<sseg_ptr> allocate_segment(bool active);

    future<> clear();
    future<> sync_all_segments();
    future<> shutdown();

    scollectd::registrations create_counters();

    void discard_unused_segments();
    void discard_completed_segments(const cf_id_type& id,
            const replay_position& pos);
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

    using buffer_type = temporary_buffer<char>;

    buffer_type acquire_buffer(size_t s);
    void release_buffer(buffer_type&&);

    future<std::vector<descriptor>> list_descriptors(sstring dir);

    flush_handler_id add_flush_handler(flush_handler h) {
        auto id = ++_flush_ids;
        _flush_handlers[id] = std::move(h);
        return id;
    }
    void remove_flush_handler(flush_handler_id id) {
        _flush_handlers.erase(id);
    }

    void flush_segments(bool = false);

private:
    segment_id_type _ids = 0;
    std::vector<sseg_ptr> _segments;
    std::deque<sseg_ptr> _reserve_segments;
    std::vector<buffer_type> _temp_buffers;
    std::unordered_map<flush_handler_id, flush_handler> _flush_handlers;
    flush_handler_id _flush_ids = 0;
    replay_position _flush_position;
    timer<clock_type> _timer;
    size_t _reserve_allocating = 0;
    // # segments to try to keep available in reserve
    // i.e. the amount of segments we expect to consume inbetween timer
    // callbacks.
    // The idea is that since the files are 0 len at start, and thus cost little,
    // it is easier to adapt this value compared to timer freq.
    size_t _num_reserve_segments = 0;
    seastar::gate _gate;
    uint64_t _new_counter = 0;
};

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

class db::commitlog::segment: public enable_lw_shared_from_this<segment> {
    segment_manager* _segment_manager;

    descriptor _desc;
    file _file;

    uint64_t _file_pos = 0;
    uint64_t _flush_pos = 0;
    uint64_t _buf_pos = 0;
    bool _closed = false;

    using buffer_type = segment_manager::buffer_type;
    using sseg_ptr = segment_manager::sseg_ptr;
    using clock_type = segment_manager::clock_type;
    using time_point = segment_manager::time_point;

    buffer_type _buffer;
    rwlock _dwrite; // used as a barrier between write & flush
    std::unordered_map<cf_id_type, position_type> _cf_dirty;
    time_point _sync_time;
    seastar::gate _gate;
    uint64_t _write_waiters = 0;
    semaphore _queue;

    std::unordered_set<table_schema_version> _known_schema_versions;

    friend std::ostream& operator<<(std::ostream&, const segment&);
    friend class segment_manager;

    future<> begin_flush() {
        // This is maintaining the semantica of only using the write-lock
        // as a gate for flushing, i.e. once we've begun a flush for position X
        // we are ok with writes to positions > X
        return _dwrite.write_lock().then(std::bind(&segment_manager::begin_flush, _segment_manager)).finally([this] {
            _dwrite.write_unlock();
        });
    }

    void end_flush() {
        _segment_manager->end_flush();
    }

    future<> begin_write() {
        // This is maintaining the semantica of only using the write-lock
        // as a gate for flushing, i.e. once we've begun a flush for position X
        // we are ok with writes to positions > X
        return _dwrite.read_lock().then(std::bind(&segment_manager::begin_write, _segment_manager));
    }

    void end_write() {
        _segment_manager->end_write();
        _dwrite.read_unlock();
    }

public:
    struct cf_mark {
        const segment& s;
    };
    friend std::ostream& operator<<(std::ostream&, const cf_mark&);

    // The commit log entry overhead in bytes (int: length + int: head checksum + int: tail checksum)
    static constexpr size_t entry_overhead_size = 3 * sizeof(uint32_t);
    static constexpr size_t segment_overhead_size = 2 * sizeof(uint32_t);
    static constexpr size_t descriptor_header_size = 5 * sizeof(uint32_t);
    static constexpr uint32_t segment_magic = ('S'<<24) |('C'<< 16) | ('L' << 8) | 'C';

    // The commit log (chained) sync marker/header size in bytes (int: length + int: checksum [segmentId, position])
    static constexpr size_t sync_marker_size = 2 * sizeof(uint32_t);

    static constexpr size_t alignment = 4096;
    // TODO : tune initial / default size
    static constexpr size_t default_size = align_up<size_t>(128 * 1024, alignment);

    segment(segment_manager* m, const descriptor& d, file && f, bool active)
            : _segment_manager(m), _desc(std::move(d)), _file(std::move(f)), _sync_time(
                    clock_type::now()), _queue(0)
    {
        ++_segment_manager->totals.segments_created;
        logger.debug("Created new {} segment {}", active ? "active" : "reserve", *this);
    }
    ~segment() {
        if (is_clean()) {
            logger.debug("Segment {} is no longer active and will be deleted now", *this);
            ++_segment_manager->totals.segments_destroyed;
            _segment_manager->totals.total_size_on_disk -= size_on_disk();
            _segment_manager->totals.total_size -= (size_on_disk() + _buffer.size());
            ::unlink(
                    (_segment_manager->cfg.commit_log_location + "/" + _desc.filename()).c_str());
        } else {
            logger.warn("Segment {} is dirty and is left on disk.", *this);
        }
    }

    bool is_schema_version_known(schema_ptr s) {
        return _known_schema_versions.count(s->version());
    }
    void add_schema_version(schema_ptr s) {
        _known_schema_versions.emplace(s->version());
    }
    void forget_schema_versions() {
        _known_schema_versions.clear();
    }

    bool must_sync() {
        if (_segment_manager->cfg.mode == sync_mode::BATCH) {
            return true;
        }
        auto now = clock_type::now();
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                now - _sync_time).count();
        if ((_segment_manager->cfg.commitlog_sync_period_in_ms * 2) < uint64_t(ms)) {
            logger.debug("{} needs sync. {} ms elapsed", *this, ms);
            return true;
        }
        return false;
    }
    /**
     * Finalize this segment and get a new one
     */
    future<sseg_ptr> finish_and_get_new() {
        _closed = true;
        return maybe_wait_for_write(sync()).then([](sseg_ptr s) {
            return s->_segment_manager->active_segment();
        });
    }
    void reset_sync_time() {
        _sync_time = clock_type::now();
    }
    // See class comment for info
    future<sseg_ptr> sync() {
        // Note: this is not a marker for when sync was finished.
        // It is when it was initiated
        reset_sync_time();

        if (position() <= _flush_pos) {
            logger.trace("Sync not needed {}: ({} / {})", *this, position(), _flush_pos);
            return make_ready_future<sseg_ptr>(shared_from_this());
        }
        return cycle().then([](sseg_ptr seg) {
            return seg->flush();
        });
    }
    future<> shutdown() {
        return _gate.close();
    }
    // See class comment for info
    future<sseg_ptr> flush(uint64_t pos = 0) {
        auto me = shared_from_this();
        assert(!me.owned());
        if (pos == 0) {
            pos = _file_pos;
        }
        if (pos != 0 && pos <= _flush_pos) {
            logger.trace("{} already synced! ({} < {})", *this, pos, _flush_pos);
            return make_ready_future<sseg_ptr>(std::move(me));
        }
        logger.trace("Syncing {} {} -> {}", *this, _flush_pos, pos);
        // Make sure all disk writes are done.
        // This is not 100% neccesary, we really only need the ones below our flush pos,
        // but since we pretty much assume that task ordering will make this the case anyway...

        return begin_flush().then(
                [this, me, pos]() mutable {
                    pos = std::max(pos, _file_pos);
                    if (pos <= _flush_pos) {
                        logger.trace("{} already synced! ({} < {})", *this, pos, _flush_pos);
                        return make_ready_future<sseg_ptr>(std::move(me));
                    }
                    return _file.flush().then_wrapped([this, pos, me](future<> f) {
                                try {
                                    f.get();
                                    // TODO: retry/ignore/fail/stop - optional behaviour in origin.
                                    // we fast-fail the whole commit.
                                    _flush_pos = std::max(pos, _flush_pos);
                                    ++_segment_manager->totals.flush_count;
                                    logger.trace("{} synced to {}", *this, _flush_pos);
                                    return make_ready_future<sseg_ptr>(std::move(me));
                                } catch (...) {
                                    logger.error("Failed to flush commits to disk: {}", std::current_exception());
                                    throw;
                                }
                            });
        }).finally([this] {
            end_flush();
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

        for (;;) {
            try {
                _buffer = _segment_manager->acquire_buffer(k);
                break;
            } catch (std::bad_alloc&) {
                logger.warn("Could not allocate {} k bytes output buffer ({} k required)", k / 1024, a / 1024);
                if (k > a) {
                    k = std::max(a, k / 2);
                    logger.debug("Trying reduced size: {} k", k / 1024);
                    continue;
                }
                throw;
            }
        }
        _buf_pos = overhead;
        auto * p = reinterpret_cast<uint32_t *>(_buffer.get_write());
        std::fill(p, p + overhead, 0);
        _segment_manager->totals.total_size += k;
    }

    /**
     * Send any buffer contents to disk and get a new tmp buffer
     */
    // See class comment for info
    future<sseg_ptr> cycle() {
        auto size = clear_buffer_slack();
        auto buf = std::move(_buffer);
        auto off = _file_pos;

        _file_pos += size;
        _buf_pos = 0;

        auto me = shared_from_this();
        assert(!me.owned());

        if (size == 0) {
            return make_ready_future<sseg_ptr>(std::move(me));
        }

        auto * p = buf.get_write();
        assert(std::count(p, p + 2 * sizeof(uint32_t), 0) == 2 * sizeof(uint32_t));

        data_output out(p, p + buf.size());

        auto header_size = 0;

        if (off == 0) {
            // first block. write file header.
            out.write(segment_magic);
            out.write(_desc.ver);
            out.write(_desc.id);
            crc32_nbo crc;
            crc.process(_desc.ver);
            crc.process<int32_t>(_desc.id & 0xffffffff);
            crc.process<int32_t>(_desc.id >> 32);
            out.write(crc.checksum());
            header_size = descriptor_header_size;
        }

        // write chunk header
        crc32_nbo crc;
        crc.process<int32_t>(_desc.id & 0xffffffff);
        crc.process<int32_t>(_desc.id >> 32);
        crc.process(uint32_t(off + header_size));

        out.write(uint32_t(_file_pos));
        out.write(crc.checksum());

        forget_schema_versions();

        // acquire read lock
        return begin_write().then([this, size, off, buf = std::move(buf), me]() mutable {
            auto written = make_lw_shared<size_t>(0);
            auto p = buf.get();
            return repeat([this, size, off, written, p]() mutable {
                auto& priority_class = service::get_local_commitlog_priority();
                return _file.dma_write(off + *written, p + *written, size - *written, priority_class).then_wrapped([this, size, written](future<size_t>&& f) {
                    try {
                        auto bytes = std::get<0>(f.get());
                        *written += bytes;
                        _segment_manager->totals.bytes_written += bytes;
                        _segment_manager->totals.total_size_on_disk += bytes;
                        ++_segment_manager->totals.cycle_count;
                        if (*written == size) {
                            return make_ready_future<stop_iteration>(stop_iteration::yes);
                        }
                        // gah, partial write. should always get here with dma chunk sized
                        // "bytes", but lets make sure...
                        logger.debug("Partial write {}: {}/{} bytes", *this, *written, size);
                        *written = align_down(*written, alignment);
                        return make_ready_future<stop_iteration>(stop_iteration::no);
                        // TODO: retry/ignore/fail/stop - optional behaviour in origin.
                        // we fast-fail the whole commit.
                    } catch (...) {
                        logger.error("Failed to persist commits to disk for {}: {}", *this, std::current_exception());
                        throw;
                    }
                });
            }).finally([this, buf = std::move(buf)]() mutable {
                _segment_manager->release_buffer(std::move(buf));
            });
        }).then([me] {
            return make_ready_future<sseg_ptr>(std::move(me));
        }).finally([me, this]() {
            end_write(); // release
        });
    }

    future<sseg_ptr> maybe_wait_for_write(future<sseg_ptr> f) {
        if (_segment_manager->should_wait_for_write()) {
            ++_write_waiters;
            logger.trace("Too many pending writes. Must wait.");
            return f.finally([this] {
                if (--_write_waiters == 0) {
                    _queue.signal(_queue.waiters());
                }
            });
        }
        return make_ready_future<sseg_ptr>(shared_from_this());
    }

    /**
     * If an allocation causes a write, and the write causes a block,
     * any allocations post that need to wait for this to finish,
     * other wise we will just continue building up more write queue
     * eventually (+ loose more ordering)
     *
     * Some caution here, since maybe_wait_for_write actually
     * releases _all_ queued up ops when finishing, we could get
     * "bursts" of alloc->write, causing build-ups anyway.
     * This should be measured properly. For now I am hoping this
     * will work out as these should "block as a group". However,
     * buffer memory usage might grow...
     */
    bool must_wait_for_alloc() {
        return _write_waiters > 0;
    }

    future<sseg_ptr> wait_for_alloc() {
        auto me = shared_from_this();
        ++_segment_manager->totals.pending_allocations;
        logger.trace("Previous allocation is blocking. Must wait.");
        return _queue.wait().then([me] { // TODO: do we need a finally?
            --me->_segment_manager->totals.pending_allocations;
            return make_ready_future<sseg_ptr>(me);
        });
    }

    /**
     * Add a "mutation" to the segment.
     */
    future<replay_position> allocate(const cf_id_type& id, shared_ptr<entry_writer> writer) {
        const auto size = writer->size(*this);
        const auto s = size + entry_overhead_size; // total size
        if (s > _segment_manager->max_mutation_size) {
            return make_exception_future<replay_position>(
                    std::invalid_argument(
                            "Mutation of " + std::to_string(s)
                                    + " bytes is too large for the maxiumum size of "
                                    + std::to_string(_segment_manager->max_mutation_size)));
        }

        std::experimental::optional<future<sseg_ptr>> op;

        if (must_sync()) {
            op = sync();
        } else if (must_wait_for_alloc()) {
            op = wait_for_alloc();
        } else if (!is_still_allocating() || position() + s > _segment_manager->max_size) { // would we make the file too big?
            // do this in next segment instead.
            op = finish_and_get_new();
        } else if (_buffer.empty()) {
            new_buffer(s);
        } else if (s > (_buffer.size() - _buf_pos)) { // enough data?
            op = maybe_wait_for_write(cycle());
        }

        if (op) {
            return op->then([id, writer = std::move(writer)] (sseg_ptr new_seg) mutable {
                return new_seg->allocate(id, std::move(writer));
            });
        }

        _gate.enter(); // this might throw. I guess we accept this?

        replay_position rp(_desc.id, position());
        auto pos = _buf_pos;
        _buf_pos += s;
        _cf_dirty[id] = rp.pos;

        auto * p = _buffer.get_write() + pos;
        auto * e = _buffer.get_write() + pos + s - sizeof(uint32_t);

        data_output out(p, e);
        crc32_nbo crc;

        out.write(uint32_t(s));
        crc.process(uint32_t(s));
        out.write(crc.checksum());

        // actual data
        writer->write(*this, out);

        crc.process_bytes(p + 2 * sizeof(uint32_t), size);

        out = data_output(e, sizeof(uint32_t));
        out.write(crc.checksum());

        ++_segment_manager->totals.allocation_count;

        _gate.leave();

        return make_ready_future<replay_position>(rp);
    }

    position_type position() const {
        return position_type(_file_pos + _buf_pos);
    }

    size_t size_on_disk() const {
        return _file_pos;
    }

    // ensures no more of this segment is writeable, by allocating any unused section at the end and marking it discarded
    // a.k.a. zero the tail.
    size_t clear_buffer_slack() {
        auto size = align_up(_buf_pos, alignment);
        std::fill(_buffer.get_write() + _buf_pos, _buffer.get_write() + size,
                0);
        _segment_manager->totals.bytes_slack += (size - _buf_pos);
        return size;
    }
    void mark_clean(const cf_id_type& id, position_type pos) {
        auto i = _cf_dirty.find(id);
        if (i != _cf_dirty.end() && i->second <= pos) {
            _cf_dirty.erase(i);
        }
    }
    void mark_clean(const cf_id_type& id, const replay_position& pos) {
        if (pos.id == _desc.id) {
            mark_clean(id, pos.pos);
        } else if (pos.id > _desc.id) {
            mark_clean(id, std::numeric_limits<position_type>::max());
        }
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

const size_t db::commitlog::segment::default_size;

future<std::vector<db::commitlog::descriptor>>
db::commitlog::segment_manager::list_descriptors(sstring dirname) {
    struct helper {
        sstring _dirname;
        file _file;
        subscription<directory_entry> _list;
        std::vector<db::commitlog::descriptor> _result;

        helper(helper&&) = default;
        helper(sstring n, file && f)
                : _dirname(std::move(n)), _file(std::move(f)), _list(
                        _file.list_directory(
                                std::bind(&helper::process, this,
                                        std::placeholders::_1))) {
        }

        future<> process(directory_entry de) {
            auto entry_type = [this](const directory_entry & de) {
                if (!de.type && !de.name.empty()) {
                    return engine().file_type(_dirname + "/" + de.name);
                }
                return make_ready_future<std::experimental::optional<directory_entry_type>>(de.type);
            };
            return entry_type(de).then([this, de](std::experimental::optional<directory_entry_type> type) {
                if (type == directory_entry_type::regular && de.name[0] != '.') {
                    try {
                        _result.emplace_back(de.name);
                    } catch (std::domain_error& e) {
                        logger.warn(e.what());
                    }
                }
                return make_ready_future<>();
            });
        }

        future<> done() {
            return _list.done();
        }
    };

    return engine().open_directory(dirname).then([this, dirname](file dir) {
        auto h = make_lw_shared<helper>(std::move(dirname), std::move(dir));
        return h->done().then([h]() {
            return make_ready_future<std::vector<db::commitlog::descriptor>>(std::move(h->_result));
        }).finally([h] {});
    });
}

future<> db::commitlog::segment_manager::init() {
    return list_descriptors(cfg.commit_log_location).then([this](std::vector<descriptor> descs) {
        segment_id_type id = std::chrono::duration_cast<std::chrono::milliseconds>(runtime::get_boot_time().time_since_epoch()).count() + 1;
        for (auto& d : descs) {
            id = std::max(id, replay_position(d.id).base_id());
        }

        // base id counter is [ <shard> | <base> ]
        _ids = replay_position(engine().cpu_id(), id).id;
        // always run the timer now, since we need to handle segment pre-alloc etc as well.
        _timer.set_callback(std::bind(&segment_manager::on_timer, this));
        auto delay = engine().cpu_id() * std::ceil(double(cfg.commitlog_sync_period_in_ms) / smp::count);
        logger.trace("Delaying timer loop {} ms", delay);
        this->arm(delay);
    });
}

scollectd::registrations db::commitlog::segment_manager::create_counters() {
    using scollectd::add_polled_metric;
    using scollectd::make_typed;
    using scollectd::type_instance_id;
    using scollectd::per_cpu_plugin_instance;
    using scollectd::data_type;

    return {
        add_polled_metric(type_instance_id("commitlog"
                        , per_cpu_plugin_instance, "queue_length", "segments")
                , make_typed(data_type::GAUGE
                        , std::bind(&decltype(_segments)::size, &_segments))
        ),
        add_polled_metric(type_instance_id("commitlog"
                        , per_cpu_plugin_instance, "queue_length", "allocating_segments")
                , make_typed(data_type::GAUGE
                        , [this]() {
                            return std::count_if(_segments.begin(), _segments.end(), [](const sseg_ptr & s) {
                                        return s->is_still_allocating();
                                    });
                        })
        ),
        add_polled_metric(type_instance_id("commitlog"
                        , per_cpu_plugin_instance, "queue_length", "unused_segments")
                , make_typed(data_type::GAUGE
                        , [this]() {
                            return std::count_if(_segments.begin(), _segments.end(), [](const sseg_ptr & s) {
                                        return s->is_unused();
                                    });
                        })
        ),
        add_polled_metric(type_instance_id("commitlog"
                        , per_cpu_plugin_instance, "total_operations", "alloc")
                , make_typed(data_type::DERIVE, totals.allocation_count)
        ),
        add_polled_metric(type_instance_id("commitlog"
                        , per_cpu_plugin_instance, "total_operations", "cycle")
                , make_typed(data_type::DERIVE, totals.cycle_count)
        ),
        add_polled_metric(type_instance_id("commitlog"
                        , per_cpu_plugin_instance, "total_operations", "flush")
                , make_typed(data_type::DERIVE, totals.flush_count)
        ),

        add_polled_metric(type_instance_id("commitlog"
                        , per_cpu_plugin_instance, "total_bytes", "written")
                , make_typed(data_type::DERIVE, totals.bytes_written)
        ),
        add_polled_metric(type_instance_id("commitlog"
                        , per_cpu_plugin_instance, "total_bytes", "slack")
                , make_typed(data_type::DERIVE, totals.bytes_slack)
        ),

        add_polled_metric(type_instance_id("commitlog"
                        , per_cpu_plugin_instance, "queue_length", "pending_writes")
                , make_typed(data_type::GAUGE, totals.pending_writes)
        ),
        add_polled_metric(type_instance_id("commitlog"
                        , per_cpu_plugin_instance, "queue_length", "pending_flushes")
                , make_typed(data_type::GAUGE, totals.pending_flushes)
        ),

        add_polled_metric(type_instance_id("commitlog"
                        , per_cpu_plugin_instance, "total_operations", "write_limit_exceeded")
                , make_typed(data_type::DERIVE, totals.write_limit_exceeded)
        ),
        add_polled_metric(type_instance_id("commitlog"
                        , per_cpu_plugin_instance, "total_operations", "flush_limit_exceeded")
                , make_typed(data_type::DERIVE, totals.flush_limit_exceeded)
        ),

        add_polled_metric(type_instance_id("commitlog"
                        , per_cpu_plugin_instance, "memory", "total_size")
                , make_typed(data_type::GAUGE, totals.total_size)
        ),
        add_polled_metric(type_instance_id("commitlog"
                        , per_cpu_plugin_instance, "memory", "buffer_list_bytes")
                , make_typed(data_type::GAUGE, totals.buffer_list_bytes)
        ),
    };
}

void db::commitlog::segment_manager::flush_segments(bool force) {
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
    if (force || !active->is_still_allocating()) {
        high = replay_position(high.id + 1, 0);
    }

    // Now get a set of used CF ids:
    std::unordered_set<cf_id_type> ids;
    std::for_each(_segments.begin(), _segments.end() - 1, [&ids](sseg_ptr& s) {
        for (auto& id : s->_cf_dirty | boost::adaptors::map_keys) {
            ids.insert(id);
        }
    });

    logger.debug("Flushing ({}) to {}", force, high);

    // For each CF id: for each callback c: call c(id, high)
    for (auto& f : callbacks) {
        for (auto& id : ids) {
            try {
                f(id, high);
            } catch (...) {
                logger.error("Exception during flush request {}/{}: {}", id, high, std::current_exception());
            }
        }
    }
}

future<db::commitlog::segment_manager::sseg_ptr> db::commitlog::segment_manager::allocate_segment(bool active) {
    descriptor d(next_id());
    return open_file_dma(cfg.commit_log_location + "/" + d.filename(), open_flags::wo | open_flags::create).then([this, d, active](file f) {
        // xfs doesn't like files extended betond eof, so enlarge the file
        return f.truncate(max_size).then([this, d, active, f] () mutable {
            auto s = make_lw_shared<segment>(this, d, std::move(f), active);
            return make_ready_future<sseg_ptr>(s);
        });
    });
}

future<db::commitlog::segment_manager::sseg_ptr> db::commitlog::segment_manager::new_segment() {
    if (_shutdown) {
        throw std::runtime_error("Commitlog has been shut down. Cannot add data");
    }

    ++_new_counter;

    if (_reserve_segments.empty()) {
        if (_num_reserve_segments < cfg.max_reserve_segments) {
            ++_num_reserve_segments;
            logger.trace("Increased segment reserve count to {}", _num_reserve_segments);
        }
        return allocate_segment(true).then([this](sseg_ptr s) {
            _segments.push_back(s);
            return make_ready_future<sseg_ptr>(s);
        });
    }

    _segments.push_back(_reserve_segments.front());
    _reserve_segments.pop_front();
    _segments.back()->reset_sync_time();
    logger.trace("Acquired segment {} from reserve", _segments.back());
    return make_ready_future<sseg_ptr>(_segments.back());
}

future<db::commitlog::segment_manager::sseg_ptr> db::commitlog::segment_manager::active_segment() {
    if (_segments.empty() || !_segments.back()->is_still_allocating()) {
        return _new_segment_semaphore.wait().then([this]() {
            if (_segments.empty() || !_segments.back()->is_still_allocating()) {
                return new_segment();
            }
            return make_ready_future<sseg_ptr>(_segments.back());
        }).finally([this]() {
            _new_segment_semaphore.signal();
        });
    }
    return make_ready_future<sseg_ptr>(_segments.back());
}

/**
 * go through all segments, clear id up to pos. if segment becomes clean and unused by this,
 * it is discarded.
 */
void db::commitlog::segment_manager::discard_completed_segments(
        const cf_id_type& id, const replay_position& pos) {
    logger.debug("Discard completed segments for {}, table {}", pos, id);
    for (auto&s : _segments) {
        s->mark_clean(id, pos);
    }
    discard_unused_segments();
}

std::ostream& db::operator<<(std::ostream& out, const db::commitlog::segment& s) {
    return out << s._desc.filename();
}

std::ostream& db::operator<<(std::ostream& out, const db::commitlog::segment::cf_mark& m) {
    return out << (m.s._cf_dirty | boost::adaptors::map_keys);
}

std::ostream& db::operator<<(std::ostream& out, const db::replay_position& p) {
    return out << "{" << p.shard_id() << ", " << p.base_id() << ", " << p.pos << "}";
}

void db::commitlog::segment_manager::discard_unused_segments() {
    logger.trace("Checking for unused segments ({} active)", _segments.size());

    auto i = std::remove_if(_segments.begin(), _segments.end(), [=](sseg_ptr s) {
        if (s->can_delete()) {
            logger.debug("Segment {} is unused", *s);
            return true;
        }
        if (s->is_still_allocating()) {
            logger.debug("Not safe to delete segment {}; still allocating.", s);
        } else if (!s->is_clean()) {
            logger.debug("Not safe to delete segment {}; dirty is {}", s, segment::cf_mark {*s});
        } else {
            logger.debug("Not safe to delete segment {}; disk ops pending", s);
        }
        return false;
    });
    if (i != _segments.end()) {
        _segments.erase(i, _segments.end());
    }
}

future<> db::commitlog::segment_manager::sync_all_segments() {
    logger.debug("Issuing sync for all segments");
    return parallel_for_each(_segments, [this](sseg_ptr s) {
        return s->sync().then([](sseg_ptr s) {
            logger.debug("Synced segment {}", *s);
        });
    });
}

future<> db::commitlog::segment_manager::shutdown() {
    if (!_shutdown) {
        _shutdown = true; // no re-arm, no create new segments.
        _timer.cancel(); // no more timer calls
        return parallel_for_each(_segments, [this](sseg_ptr s) {
            return s->shutdown(); // close each segment (no more alloc)
        }).then(std::bind(&segment_manager::sync_all_segments, this)).then([this] { // flush all
            return _gate.close(); // wait for any pending ops
        });
    }
    return make_ready_future<>();
}


/*
 * Sync all segments, then clear them out. To ensure all ops are done.
 * (Assumes you have barriered adding ops!)
 * Only use from tests.
 */
future<> db::commitlog::segment_manager::clear() {
    logger.debug("Clearing commitlog");
    return shutdown().then([this] {
        logger.debug("Clearing all segments");
        for (auto& s : _segments) {
            s->mark_clean();
        }
       _segments.clear();
    });
}
/**
 * Called by timer in periodic mode.
 */
void db::commitlog::segment_manager::sync() {
    for (auto s : _segments) {
        s->sync(); // we do not care about waiting...
    }
}

void db::commitlog::segment_manager::on_timer() {
    // Gate, because we are starting potentially blocking ops
    // without waiting for them, so segement_manager could be shut down
    // while they are running.
    seastar::with_gate(_gate, [this] {
        if (cfg.mode != sync_mode::BATCH) {
            sync();
        }
        // IFF a new segment was put in use since last we checked, and we're
        // above threshold, request flush.
        if (_new_counter > 0) {
            auto max = max_disk_size;
            auto cur = totals.total_size_on_disk;
            if (max != 0 && cur >= max) {
                _new_counter = 0;
                logger.debug("Size on disk {} MB exceeds local maximum {} MB", cur / (1024 * 1024), max / (1024 * 1024));
                flush_segments();
            }
        }
        // take outstanding allocations into regard. This is paranoid,
        // but if for some reason the file::open takes longer than timer period,
        // we could flood the reserve list with new segments
        auto n = _reserve_segments.size() + _reserve_allocating;
        return parallel_for_each(boost::irange(n, _num_reserve_segments), [this, n](auto i) {
            ++_reserve_allocating;
            return this->allocate_segment(false).then([this](sseg_ptr s) {
                if (!_shutdown) {
                    // insertion sort.
                    auto i = std::upper_bound(_reserve_segments.begin(), _reserve_segments.end(), s, [](sseg_ptr s1, sseg_ptr s2) {
                        const descriptor& d1 = s1->_desc;
                        const descriptor& d2 = s2->_desc;
                        return d1.id < d2.id;
                    });
                    i = _reserve_segments.emplace(i, std::move(s));
                    logger.trace("Added reserve segment {}", *i);
                }
            }).finally([this] {
                --_reserve_allocating;
            });
        });
    }).handle_exception([](std::exception_ptr ep) {
        logger.warn("Exception in segment reservation: {}", ep);
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


db::commitlog::segment_manager::buffer_type db::commitlog::segment_manager::acquire_buffer(size_t s) {
    auto i = _temp_buffers.begin();
    auto e = _temp_buffers.end();

    while (i != e) {
        if (i->size() >= s) {
            auto r = std::move(*i);
            _temp_buffers.erase(i);
            totals.buffer_list_bytes -= r.size();
            return r;
        }
        ++i;
    }
    auto a = ::memalign(segment::alignment, s);
    if (a == nullptr) {
        throw std::bad_alloc();
    }
    logger.trace("Allocated {} k buffer", s / 1024);
    return buffer_type(reinterpret_cast<char *>(a), s, make_free_deleter(a));
}

void db::commitlog::segment_manager::release_buffer(buffer_type&& b) {
    _temp_buffers.emplace_back(std::move(b));
    std::sort(_temp_buffers.begin(), _temp_buffers.end(), [](const buffer_type& b1, const buffer_type& b2) {
        return b1.size() < b2.size();
    });

    constexpr const size_t max_temp_buffers = 4;

    if (_temp_buffers.size() > max_temp_buffers) {
        logger.trace("Deleting {} buffers", _temp_buffers.size() - max_temp_buffers);
        _temp_buffers.erase(_temp_buffers.begin() + max_temp_buffers, _temp_buffers.end());
    }
    totals.buffer_list_bytes = std::accumulate(_temp_buffers.begin(),
            _temp_buffers.end(), size_t(0), std::plus<size_t>());
}

/**
 * Add mutation.
 */
future<db::replay_position> db::commitlog::add(const cf_id_type& id,
        size_t size, serializer_func func) {
    class serializer_func_entry_writer final : public entry_writer {
        serializer_func _func;
        size_t _size;
    public:
        serializer_func_entry_writer(size_t sz, serializer_func func)
            : _func(std::move(func)), _size(sz)
        { }
        virtual size_t size(segment&) override { return _size; }
        virtual void write(segment&, output& out) override {
            _func(out);
        }
    };
    auto writer = ::make_shared<serializer_func_entry_writer>(size, std::move(func));
    return _segment_manager->active_segment().then([id, writer] (auto s) {
        return s->allocate(id, writer);
    });
}

future<db::replay_position> db::commitlog::add_entry(const cf_id_type& id, const commitlog_entry_writer& cew)
{
    class cl_entry_writer final : public entry_writer {
        commitlog_entry_writer _writer;
    public:
        cl_entry_writer(const commitlog_entry_writer& wr) : _writer(wr) { }
        virtual size_t size(segment& seg) override {
            _writer.set_with_schema(!seg.is_schema_version_known(_writer.schema()));
            return _writer.size();
        }
        virtual void write(segment& seg, output& out) override {
            if (_writer.with_schema()) {
                seg.add_schema_version(_writer.schema());
            }
            _writer.write(out);
        }
    };
    auto writer = ::make_shared<cl_entry_writer>(cew);
    return _segment_manager->active_segment().then([id, writer] (auto s) {
        return s->allocate(id, writer);
    });
}

db::commitlog::commitlog(config cfg)
        : _segment_manager(new segment_manager(std::move(cfg))) {
}

db::commitlog::commitlog(commitlog&& v) noexcept
        : _segment_manager(std::move(v._segment_manager)) {
}

db::commitlog::~commitlog() {
}

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

void db::commitlog::discard_completed_segments(const cf_id_type& id,
        const replay_position& pos) {
    _segment_manager->discard_completed_segments(id, pos);
}

future<> db::commitlog::sync_all_segments() {
    return _segment_manager->sync_all_segments();
}

future<> db::commitlog::shutdown() {
    return _segment_manager->shutdown();
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

future<std::unique_ptr<subscription<temporary_buffer<char>, db::replay_position>>>
db::commitlog::read_log_file(const sstring& filename, commit_load_reader_func next, position_type off) {
    return open_file_dma(filename, open_flags::ro).then([next = std::move(next), off](file f) {
       return std::make_unique<subscription<temporary_buffer<char>, replay_position>>(
           read_log_file(std::move(f), std::move(next), off));
    });
}

subscription<temporary_buffer<char>, db::replay_position>
db::commitlog::read_log_file(file f, commit_load_reader_func next, position_type off) {
    struct work {
        file f;
        stream<temporary_buffer<char>, replay_position> s;
        input_stream<char> fin;
        input_stream<char> r;
        uint64_t id = 0;
        size_t pos = 0;
        size_t next = 0;
        size_t start_off = 0;
        size_t skip_to = 0;
        size_t file_size = 0;
        size_t corrupt_size = 0;
        bool eof = false;
        bool header = true;

        work(file f, position_type o = 0)
                : f(f), fin(make_file_input_stream(f)), start_off(o) {
        }
        work(work&&) = default;

        bool advance(const temporary_buffer<char>& buf) {
            pos += buf.size();
            if (buf.size() == 0) {
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
            skip_to = pos + bytes;
            return do_until([this] { return pos == skip_to || eof; }, [this, bytes] {
                auto s = std::min<size_t>(4096, skip_to - pos);
                // should eof be an error here?
                return fin.read_exactly(s).then([this](auto buf) {
                    this->advance(buf);
                });
            });
        }
        future<> stop() {
            eof = true;
            return make_ready_future<>();
        }
        future<> read_header() {
            return fin.read_exactly(segment::descriptor_header_size).then([this](temporary_buffer<char> buf) {
                if (!advance(buf)) {
                    // zero length file. accept it just to be nice.
                    return make_ready_future<>();
                }
                // Will throw if we got eof
                data_input in(buf);
                auto magic = in.read<uint32_t>();
                auto ver = in.read<uint32_t>();
                auto id = in.read<uint64_t>();
                auto checksum = in.read<uint32_t>();

                if (magic == 0 && ver == 0 && id == 0 && checksum == 0) {
                    // let's assume this was an empty (pre-allocated)
                    // file. just skip it.
                    return stop();
                }

                if (magic != segment::segment_magic) {
                    throw std::invalid_argument("Not a scylla format commitlog file");
                }
                crc32_nbo crc;
                crc.process(ver);
                crc.process<int32_t>(id & 0xffffffff);
                crc.process<int32_t>(id >> 32);

                auto cs = crc.checksum();
                if (cs != checksum) {
                    throw std::runtime_error("Checksum error in file header");
                }

                this->id = id;
                this->next = 0;

                return make_ready_future<>();
            });
        }
        future<> read_chunk() {
            return fin.read_exactly(segment::segment_overhead_size).then([this](temporary_buffer<char> buf) {
                auto start = pos;

                if (!advance(buf)) {
                    return make_ready_future<>();
                }

                data_input in(buf);
                auto next = in.read<uint32_t>();
                auto checksum = in.read<uint32_t>();

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
                    logger.debug("Checksum error in segment chunk at {}.", pos);
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
        future<> read_entry() {
            static constexpr size_t entry_header_size = segment::entry_overhead_size - sizeof(uint32_t);

            /**
             * #598 - Must check that data left in chunk is enough to even read an entry.
             * If not, this is small slack space in the chunk end, and we should just go
             * to the next.
             */
            assert(pos <= next);
            if ((pos + entry_header_size) >= next) {
                return skip(next - pos);
            }

            return fin.read_exactly(entry_header_size).then([this](temporary_buffer<char> buf) {
                replay_position rp(id, position_type(pos));

                if (!advance(buf)) {
                    return make_ready_future<>();
                }

                data_input in(buf);

                auto size = in.read<uint32_t>();
                auto checksum = in.read<uint32_t>();

                crc32_nbo crc;
                crc.process(size);

                if (size < 3 * sizeof(uint32_t) || checksum != crc.checksum()) {
                    auto slack = next - pos;
                    if (size != 0) {
                        logger.debug("Segment entry at {} has broken header. Skipping to next chunk ({} bytes)", rp, slack);
                        corrupt_size += slack;
                    }
                    // size == 0 -> special scylla case: zero padding due to dma blocks
                    return skip(slack);
                }

                return fin.read_exactly(size - entry_header_size).then([this, size, crc = std::move(crc), rp](temporary_buffer<char> buf) mutable {
                    advance(buf);

                    data_input in(buf);

                    auto data_size = size - segment::entry_overhead_size;
                    in.skip(data_size);
                    auto checksum = in.read<uint32_t>();

                    crc.process_bytes(buf.get(), data_size);

                    if (crc.checksum() != checksum) {
                        // If we're getting a checksum error here, most likely the rest of
                        // the file will be corrupt as well. But it does not hurt to retry.
                        // Just go to the next entry (since "size" in header seemed ok).
                        logger.debug("Segment entry at {} checksum error. Skipping {} bytes", rp, size);
                        corrupt_size += size;
                        return make_ready_future<>();
                    }

                    return s.produce(buf.share(0, data_size), rp);
                });
            });
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
            });
        }
    };

    auto w = make_lw_shared<work>(std::move(f), off);
    auto ret = w->s.listen(std::move(next));

    w->s.started().then(std::bind(&work::read_file, w.get())).then([w] {
        w->s.close();
    }).handle_exception([w](auto ep) {
        w->s.set_exception(ep);
    });

    return ret;
}

std::vector<sstring> db::commitlog::get_active_segment_names() const {
    return _segment_manager->get_active_names();
}

uint64_t db::commitlog::get_total_size() const {
    return _segment_manager->totals.total_size;
}

uint64_t db::commitlog::get_completed_tasks() const {
    return _segment_manager->totals.allocation_count;
}

uint64_t db::commitlog::get_flush_count() const {
    return _segment_manager->totals.flush_count;
}

uint64_t db::commitlog::get_pending_tasks() const {
    return _segment_manager->totals.pending_writes
                    + _segment_manager->totals.pending_flushes;
}

uint64_t db::commitlog::get_pending_writes() const {
    return _segment_manager->totals.pending_writes;
}

uint64_t db::commitlog::get_pending_flushes() const {
    return _segment_manager->totals.pending_flushes;
}

uint64_t db::commitlog::get_pending_allocations() const {
    return _segment_manager->totals.pending_allocations;
}

uint64_t db::commitlog::get_write_limit_exceeded_count() const {
    return _segment_manager->totals.write_limit_exceeded;
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

