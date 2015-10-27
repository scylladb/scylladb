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
#include <net/byteorder.hh>

#include "commitlog.hh"
#include "db/config.hh"
#include "utils/data_input.hh"
#include "utils/crc.hh"
#include "utils/runtime.hh"
#include "log.hh"

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
    , commitlog_total_space_in_mb(cfg.commitlog_total_space_in_mb())
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
        uint64_t pending_operations = 0;
        uint64_t total_size = 0;
        uint64_t buffer_list_bytes = 0;
        uint64_t total_size_on_disk = 0;
    };

    stats totals;

    void begin_op() {
        _gate.enter();
        ++totals.pending_operations;
    }
    void end_op() {
        --totals.pending_operations;
        _gate.leave();
    }

    segment_manager(config c)
            : cfg(c), max_size(
                    std::min<size_t>(std::numeric_limits<position_type>::max(),
                            std::max<size_t>(cfg.commitlog_segment_size_in_mb,
                                    1) * 1024 * 1024)), max_mutation_size(
                    max_size >> 1), max_disk_size(
                    size_t(
                            std::ceil(
                                    cfg.commitlog_total_space_in_mb
                                            / double(smp::count))) * 1024 * 1024)
    {
        assert(max_size > 0);
        if (cfg.commit_log_location.empty()) {
            cfg.commit_log_location = "/var/lib/scylla/commitlog";
        }
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

    friend std::ostream& operator<<(std::ostream&, const segment&);
    friend class segment_manager;
public:
    struct cf_mark {
        const segment& s;
    };
    friend std::ostream& operator<<(std::ostream&, const cf_mark&);

    // The commit log entry overhead in bytes (int: length + int: head checksum + int: tail checksum)
    static constexpr size_t entry_overhead_size = 3 * sizeof(uint32_t);
    static constexpr size_t segment_overhead_size = 2 * sizeof(uint32_t);
    static constexpr size_t descriptor_header_size = 4 * sizeof(uint32_t);

    // The commit log (chained) sync marker/header size in bytes (int: length + int: checksum [segmentId, position])
    static constexpr size_t sync_marker_size = 2 * sizeof(uint32_t);

    static constexpr size_t alignment = 4096;
    // TODO : tune initial / default size
    static constexpr size_t default_size = align_up<size_t>(128 * 1024, alignment);

    segment(segment_manager* m, const descriptor& d, file && f, bool active)
            : _segment_manager(m), _desc(std::move(d)), _file(std::move(f)), _sync_time(
                    clock_type::now())
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
        sync();
        return _segment_manager->active_segment();
    }
    void reset_sync_time() {
        _sync_time = clock_type::now();
    }
    future<sseg_ptr> sync() {
        // Note: this is not a marker for when sync was finished.
        // It is when it was initiated
        reset_sync_time();

        if (position() <= _flush_pos) {
            logger.trace("Sync not needed {}: ({} / {})", *this, position(), _flush_pos);
            return make_ready_future<sseg_ptr>(shared_from_this());
        }
        return cycle().then([](auto seg) {
            return seg->flush();
        });
    }
    future<> shutdown() {
        return _gate.close();
    }
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

        return _dwrite.write_lock().then(
                [this, me, pos]() mutable {
                    _dwrite.write_unlock(); // release it already.
                    pos = std::max(pos, _file_pos);
                    if (pos <= _flush_pos) {
                        logger.trace("{} already synced! ({} < {})", *this, pos, _flush_pos);
                        return make_ready_future<sseg_ptr>(std::move(me));
                    }
                    _segment_manager->begin_op();
                    return _file.flush().then_wrapped([this, pos, me](auto f) {
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
                            }).finally([this, me] {
                                _segment_manager->end_op();
                            });
                });
    }
    /**
     * Send any buffer contents to disk and get a new tmp buffer
     */
    future<sseg_ptr> cycle(size_t s = 0) {
        auto size = clear_buffer_slack();
        auto buf = std::move(_buffer);
        auto off = _file_pos;

        _file_pos += size;
        _buf_pos = 0;

        // if we need new buffer, get one.
        // TODO: keep a queue of available buffers?
        if (s > 0) {
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

        // acquire read lock
        return _dwrite.read_lock().then([this, size, off, buf = std::move(buf), me]() mutable {
            auto written = make_lw_shared<size_t>(0);
            auto p = buf.get();
            _segment_manager->begin_op();
            return repeat([this, size, off, written, p]() mutable {
                return _file.dma_write(off + *written, p + *written, size - *written).then_wrapped([this, size, written](auto&& f) {
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
                _segment_manager->end_op();
            });
        }).then([me] {
            return make_ready_future<sseg_ptr>(std::move(me));
        }).finally([me, this]() {
            _dwrite.read_unlock(); // release
        });
    }

    /**
     * Add a "mutation" to the segment.
     */
    future<replay_position> allocate(const cf_id_type& id, size_t size,
            serializer_func func) {
        const auto s = size + entry_overhead_size; // total size
        if (s > _segment_manager->max_mutation_size) {
            return make_exception_future<replay_position>(
                    std::invalid_argument(
                            "Mutation of " + std::to_string(s)
                                    + " bytes is too large for the maxiumum size of "
                                    + std::to_string(_segment_manager->max_mutation_size)));
        }
        // would we make the file too big?
        for (;;) {
            if (position() + s > _segment_manager->max_size) {
                // do this in next segment instead.
                return finish_and_get_new().then(
                        [id, size, func = std::move(func)](auto new_seg) {
                            return new_seg->allocate(id, size, func);
                        });
            }
            // enough data?
            if (s > (_buffer.size() - _buf_pos)) {
                // TODO: iff we have to many writes running, maybe we should
                // wait for this?
                cycle(s);
                continue; // re-check file size overflow
            }
            break;
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
        func(out);

        crc.process_bytes(p + 2 * sizeof(uint32_t), size);

        out = data_output(e, sizeof(uint32_t));
        out.write(crc.checksum());

        ++_segment_manager->totals.allocation_count;

        _gate.leave();

        // finally, check if we're required to sync.
        if (must_sync()) {
            return sync().then([rp](auto seg) {
                return make_ready_future<replay_position>(rp);
            });
        }

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
            return entry_type(de).then([this, de](auto type) {
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

    return engine().open_directory(dirname).then([this, dirname](auto dir) {
        auto h = make_lw_shared<helper>(std::move(dirname), std::move(dir));
        return h->done().then([h]() {
            return make_ready_future<std::vector<db::commitlog::descriptor>>(std::move(h->_result));
        }).finally([h] {});
    });
}

future<> db::commitlog::segment_manager::init() {
    return list_descriptors(cfg.commit_log_location).then([this](auto descs) {
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
                        , per_cpu_plugin_instance, "queue_length", "pending_operations")
                , make_typed(data_type::GAUGE, totals.pending_operations)
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
    return engine().open_file_dma(cfg.commit_log_location + "/" + d.filename(), open_flags::wo | open_flags::create).then([this, d, active](file f) {
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

    auto i = std::remove_if(_segments.begin(), _segments.end(), [=](auto s) {
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
                    auto i = std::upper_bound(_reserve_segments.begin(), _reserve_segments.end(), s, [](auto s1, auto s2) {
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
    }).handle_exception([](auto ep) {
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
    return _segment_manager->active_segment().then([=](auto s) {
        return s->allocate(id, size, std::move(func));
    });
}

db::commitlog::commitlog(config cfg)
        : _segment_manager(new segment_manager(std::move(cfg))) {
}

db::commitlog::commitlog(commitlog&& v)
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

future<> db::commitlog::clear() {
    return _segment_manager->clear();
}

const db::commitlog::config& db::commitlog::active_config() const {
    return _segment_manager->cfg;
}

future<subscription<temporary_buffer<char>, db::replay_position>>
db::commitlog::read_log_file(const sstring& filename, commit_load_reader_func next, position_type off) {
    return engine().open_file_dma(filename, open_flags::ro).then([next = std::move(next), off](file f) {
       return read_log_file(std::move(f), std::move(next), off);
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
                auto ver = in.read<uint32_t>();
                auto id = in.read<uint64_t>();
                auto checksum = in.read<uint32_t>();

                if (ver == 0 && id == 0 && checksum == 0) {
                    // let's assume this was an empty (pre-allocated)
                    // file. just skip it.
                    return stop();
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
                    throw std::runtime_error("Checksum error in chunk header");
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
            return fin.read_exactly(entry_header_size).then([this](temporary_buffer<char> buf) {
                replay_position rp(id, position_type(pos));

                if (!advance(buf)) {
                    return make_ready_future<>();
                }

                data_input in(buf);

                auto size = in.read<uint32_t>();
                auto checksum = in.read<uint32_t>();

                if (size == 0) {
                    // special scylla case: zero padding due to dma blocks
                    auto slack = next - pos;
                    return skip(slack);
                }

                if (size < 3 * sizeof(uint32_t)) {
                    throw std::runtime_error("Invalid entry size");
                }

                if (start_off > pos) {
                    return skip(size - entry_header_size);
                }

                return fin.read_exactly(size - entry_header_size).then([this, size, checksum, rp](temporary_buffer<char> buf) {
                    advance(buf);

                    data_input in(buf);

                    auto data_size = size - segment::entry_overhead_size;
                    in.skip(data_size);
                    auto checksum = in.read<uint32_t>();

                    crc32_nbo crc;
                    crc.process(size);
                    crc.process_bytes(buf.get(), data_size);

                    if (crc.checksum() != checksum) {
                        throw std::runtime_error("Checksum error in data entry");
                    }

                    return s.produce(buf.share(0, data_size), rp);
                });
            });
        }
        future<> read_file() {
            return read_header().then(
                    [this] {
                        return do_until(std::bind(&work::end_of_file, this), std::bind(&work::read_chunk, this));
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

uint64_t db::commitlog::get_pending_tasks() const {
    return _segment_manager->totals.pending_operations;
}

uint64_t db::commitlog::get_num_segments_created() const {
    return _segment_manager->totals.segments_created;
}

uint64_t db::commitlog::get_num_segments_destroyed() const {
    return _segment_manager->totals.segments_destroyed;
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

