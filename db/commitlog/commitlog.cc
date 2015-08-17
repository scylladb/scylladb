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

#include <stdexcept>
#include <string>
#include <sys/stat.h>
#include <malloc.h>
#include <regex>
#include <boost/range/adaptor/map.hpp>

#include "core/align.hh"
#include "core/reactor.hh"
#include "core/scollectd.hh"
#include "core/future-util.hh"
#include "core/file.hh"
#include "core/rwlock.hh"
#include "core/fstream.hh"
#include "net/byteorder.hh"
#include "commitlog.hh"
#include "db/config.hh"
#include "utils/data_input.hh"
#include "utils/crc.hh"
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

class db::commitlog::descriptor {
public:
    static const std::string SEPARATOR;
    static const std::string FILENAME_PREFIX;
    static const std::string FILENAME_EXTENSION;

    descriptor(descriptor&&) = default;
    descriptor(const descriptor&) = default;
    // TODO : version management
    descriptor(uint64_t i, uint32_t v = 1)
            : id(i), ver(v) {
    }
    descriptor(std::pair<uint64_t, uint32_t> p)
            : descriptor(p.first, p.second) {
    }
    descriptor(sstring filename)
            : descriptor([filename]() {
                std::smatch m;
                // match both legacy and new version of commitlogs Ex: CommitLog-12345.log and CommitLog-4-12345.log.
                    std::regex rx(FILENAME_PREFIX + "((\\d+)(" + SEPARATOR + "\\d+)?)" + FILENAME_EXTENSION);
                    std::string sfilename = filename;
                    if (!std::regex_match(sfilename, m, rx)) {
                        throw std::runtime_error("Cannot parse the version of the file: " + filename);
                    }
                    if (m[3].length() == 0) {
                        // CMH. Can most likely ignore this
                        throw std::domain_error("Commitlog segment is too old to open; upgrade to 1.2.5+ first");
                    }

                    uint64_t id = std::stoull(m[3].str().substr(1));
                    uint32_t ver = std::stoul(m[2].str());

                    return std::make_pair(id, ver);
                }()) {
    }

    sstring filename() const {
        return FILENAME_PREFIX + std::to_string(ver) + SEPARATOR
                + std::to_string(id) + FILENAME_EXTENSION;
    }

    const uint64_t id;
    const uint32_t ver;
};

const std::string db::commitlog::descriptor::SEPARATOR("-");
const std::string db::commitlog::descriptor::FILENAME_PREFIX(
        "CommitLog" + SEPARATOR);
const std::string db::commitlog::descriptor::FILENAME_EXTENSION(".log");

class db::commitlog::segment_manager {
public:
    config cfg;
    const uint64_t max_size;
    const uint64_t max_mutation_size;

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
    };

    stats totals;

    segment_manager(config cfg)
            : cfg(cfg), max_size(
                    std::max<size_t>(cfg.commitlog_segment_size_in_mb, 1) * 1024
                            * 1024), max_mutation_size(max_size >> 1)
    {
        assert(max_size > 0);
        if (cfg.commit_log_location.empty()) {
            cfg.commit_log_location = "/tmp/urchin/commitlog/"
                    + std::to_string(engine().cpu_id());
        }
        _regs = create_counters();
    }

    uint64_t next_id() {
        return ++_ids;
    }

    future<> process(directory_entry de) {
        auto entry_type = [this](const directory_entry & de) {
            if (!de.type && !de.name.empty()) {
                return engine().file_type(cfg.commit_log_location + "/" + de.name);
            }
            return make_ready_future<std::experimental::optional<directory_entry_type>>(de.type);
        };
        return entry_type(de).then([de, this](auto type) {
            if (type == directory_entry_type::regular) {
                descriptor d(de.name);
                _ids = std::max(_ids, d.id);
            }
            return make_ready_future<>();
        });
    }

    future<> init();
    future<sseg_ptr> new_segment();
    future<sseg_ptr> active_segment();
    future<> clear();

    scollectd::registrations create_counters();

    void discard_unused_segments();
    void discard_completed_segments(const cf_id_type& id,
            const replay_position& pos);
    void sync();
    void arm() {
        _timer.arm(std::chrono::milliseconds(cfg.commitlog_sync_period_in_ms));
    }

    std::vector<sstring> get_active_names() const;

    using buffer_type = temporary_buffer<char>;

    buffer_type acquire_buffer(size_t s);
    void release_buffer(buffer_type&&);

private:
    uint64_t _ids = 0;
    std::vector<sseg_ptr> _segments;
    std::vector<buffer_type> _temp_buffers;
    timer<clock_type> _timer;
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

    friend std::ostream& operator<<(std::ostream&, const segment&);
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

    segment(segment_manager* m, const descriptor& d, file && f)
            : _segment_manager(m), _desc(std::move(d)), _file(std::move(f))
    {
        ++_segment_manager->totals.segments_created;
    }
    ~segment() {
        logger.debug("Segment {} is no longer active and will be deleted now", *this);
        ++_segment_manager->totals.segments_destroyed;
        ::unlink(
                (_segment_manager->cfg.commit_log_location + "/" + _desc.filename()).c_str());
    }

    bool must_sync() {
        if (_segment_manager->cfg.mode == sync_mode::BATCH) {
            return true;
        }
        auto now = clock_type::now();
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                now - _sync_time).count();
        return _segment_manager->cfg.commitlog_sync_period_in_ms < uint64_t(ms);
    }
    /**
     * Finalize this segment and get a new one
     */
    future<sseg_ptr> finish_and_get_new() {
        _closed = true;
        return sync().then([](auto seg) {
            return seg->_segment_manager->active_segment();
        });
    }
    future<sseg_ptr> sync() {
        if (position() <= _flush_pos) {
            return make_ready_future<sseg_ptr>(shared_from_this());
        }
        return cycle().then([](auto seg) {
            return seg->flush();
        });
    }
    future<sseg_ptr> flush(uint64_t pos = 0) {
        auto me = shared_from_this();
        if (pos == 0) {
            pos = _file_pos;
        }
        if (pos != 0 && pos <= _flush_pos) {
            return make_ready_future<sseg_ptr>(std::move(me));
        }
        // Make sure all disk writes are done.
        // This is not 100% neccesary, we really only need the ones below our flush pos,
        // but since we pretty much assume that task ordering will make this the case anyway...
        return _dwrite.write_lock().then(
                [this, me = std::move(me), pos]() mutable {
                    _dwrite.write_unlock(); // release it already.
                    pos = std::max(pos, _file_pos);
                    if (pos <= _flush_pos) {
                        return make_ready_future<sseg_ptr>(std::move(me));
                    }
                    _sync_time = clock_type::now();
                    return _file.flush().handle_exception([](auto ex) {
                                try {
                                    std::rethrow_exception(ex);
                                    // TODO: retry/ignore/fail/stop - optional behaviour in origin.
                                    // we fast-fail the whole commit.
                                } catch (std::exception& e) {
                                    logger.error("Failed to flush commits to disk: {}", e.what());
                                    throw;
                                } catch (...) {
                                    logger.error("Failed to flush commits to disk.");
                                    throw;
                                }
                            }).then([this, pos, me = std::move(me)]() {
                                _flush_pos = std::max(pos, _flush_pos);
                                ++_segment_manager->totals.flush_count;
                                return make_ready_future<sseg_ptr>(std::move(me));
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
                    logger.error("Could not allocate {} k bytes output buffer ({} k required)", k / 1024, a / 1024);
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
        }
        auto me = shared_from_this();
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
        return _dwrite.read_lock().then([this, size, off, buf = std::move(buf), me = std::move(me)]() mutable {
            auto p = buf.get();
            return _file.dma_write(off, p, size).then_wrapped([this, size, me, buf = std::move(buf)](auto&& f) mutable {
                try {
                    _segment_manager->release_buffer(std::move(buf));
                    size_t written = std::get<0>(f.get());
                    assert(written == size); // we are not equipped to deal with partial writes.
                    _segment_manager->totals.bytes_written += written;
                    ++_segment_manager->totals.cycle_count;
                    return make_ready_future<sseg_ptr>(std::move(me));
                    // TODO: retry/ignore/fail/stop - optional behaviour in origin.
                    // we fast-fail the whole commit.
                } catch (std::exception& e) {
                    logger.error("Failed to persist commits to disk: {}", e.what());
                    throw;
                } catch (...) {
                    logger.error("Failed to persist commits to disk.");
                    throw;
                }
            });
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
        }

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
        // finally, check if we're required to sync.
        if (must_sync()) {
            return sync().then([rp](auto seg) {
                return make_ready_future<replay_position>(rp);
            });
        }

        return make_ready_future<replay_position>(rp);
    }

    position_type position() const {
        return _file_pos + _buf_pos;
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
    bool is_clean() {
        return _cf_dirty.empty();
    }
    bool is_unused() {
        return !is_still_allocating() && is_clean();
    }
    bool contains(const replay_position& pos) {
        return pos.id == _desc.id;
    }
    sstring get_segment_name() const {
        return _desc.filename();
    }
};

const size_t db::commitlog::segment::default_size;

future<> db::commitlog::segment_manager::init() {
    struct helper {
        file _file;
        subscription<directory_entry> _list;

        helper(segment_manager * m, file && f)
                : _file(std::move(f)), _list(
                        _file.list_directory(
                                std::bind(&segment_manager::process, m,
                                        std::placeholders::_1))) {
        }

        future<> done() {
            return _list.done();
        }
    };

    return engine().open_directory(cfg.commit_log_location).then([this](auto dir) {
        // keep sub alive...
            auto h = make_lw_shared<helper>(this, std::move(dir));
            return h->done().then([this, h]() {
                // nothing really. just keeping sub alive
                if (cfg.mode != sync_mode::BATCH) {
                    _timer.set_callback(std::bind(&segment_manager::sync, this));
                    this->arm();
                }
            });
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
                , make_typed(data_type::GAUGE, totals.bytes_written)
        ),
        add_polled_metric(type_instance_id("commitlog"
                        , per_cpu_plugin_instance, "total_bytes", "slack")
                , make_typed(data_type::GAUGE, totals.bytes_slack)
        ),
    };
}

future<db::commitlog::segment_manager::sseg_ptr> db::commitlog::segment_manager::new_segment() {
    descriptor d(next_id());
    return engine().open_file_dma(cfg.commit_log_location + "/" + d.filename(), open_flags::wo|open_flags::create).then(
            [this, d](file f) {
                if (cfg.commitlog_total_space_in_mb != 0) {
                    auto i = _segments.rbegin();
                    auto e = _segments.rend();
                    size_t s = 0, n = 0;

                    while (i != e) {
                        auto& seg = *i;
                        s += seg->size_on_disk();
                        if (!seg->is_still_allocating() && s >= cfg.commitlog_total_space_in_mb) {
                            seg->mark_clean();
                            ++n;
                        }
                        ++i;
                    }

                    if (n > 0) {
                        discard_unused_segments();
                    }
                }

                _segments.emplace_back(make_lw_shared<segment>(this, d, std::move(f)));
                return make_ready_future<sseg_ptr>(_segments.back());
            });

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
    logger.debug("discard completed log segments for {}, table {}", pos, id);
    for (auto&s : _segments) {
        s->mark_clean(id, pos);
    }
    discard_unused_segments();
}

std::ostream& db::operator<<(std::ostream& out, const db::commitlog::segment& s) {
    return out << "commit log segment (" << s._desc.filename() << ")";
}

std::ostream& db::operator<<(std::ostream& out, const db::commitlog::segment::cf_mark& m) {
    return out << (m.s._cf_dirty | boost::adaptors::map_keys);
}

std::ostream& db::operator<<(std::ostream& out, const db::replay_position& p) {
    return out << "{" << p.id << ", " << p.pos << "}";
}

void db::commitlog::segment_manager::discard_unused_segments() {
    auto i = std::remove_if(_segments.begin(), _segments.end(), [=](auto& s) {
        if (s->is_unused()) {
            logger.debug("{} is unused", *s);
            return true;
        }
        logger.debug("Not safe to delete {}; dirty is {}", s, segment::cf_mark {*s});
        return false;
    });
    if (i != _segments.end()) {
        _segments.erase(i, _segments.end());
    }
}

/*
 * Sync all segments, then clear them out. To ensure all ops are done.
 * (Assumes you have barriered adding ops!)
 */
future<> db::commitlog::segment_manager::clear() {
    return do_until([this]() {return _segments.empty();}, [this]() {
        auto s = _segments.front();
        _segments.erase(_segments.begin());
        return s->sync().then([](sseg_ptr) {
                });
    });
}
/**
 * Called by timer in periodic mode.
 */
void db::commitlog::segment_manager::sync() {
    for (auto& s : _segments) {
        if (s->must_sync()) {
            s->sync(); // we do not care about waiting...
        }
    }
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
            return r;
        }
        ++i;
    }
    auto a = ::memalign(segment::alignment, s);
    if (a == nullptr) {
        throw std::bad_alloc();
    }
    return buffer_type(reinterpret_cast<char *>(a), s, make_free_deleter(a));
}

void db::commitlog::segment_manager::release_buffer(buffer_type&& b) {
    _temp_buffers.emplace_back(std::move(b));
    std::sort(_temp_buffers.begin(), _temp_buffers.end(), [](const buffer_type& b1, const buffer_type& b2) {
        return b1.size() < b2.size();
    });

    constexpr const size_t max_temp_buffers = 4;

    if (_temp_buffers.size() > max_temp_buffers) {
        _temp_buffers.erase(_temp_buffers.begin() + max_temp_buffers, _temp_buffers.end());
    }
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

void db::commitlog::discard_completed_segments(const cf_id_type& id,
        const replay_position& pos) {
    _segment_manager->discard_completed_segments(id, pos);
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

future<subscription<temporary_buffer<char>>> db::commitlog::read_log_file(const sstring& filename, commit_load_reader_func next) {
    return engine().open_file_dma(filename, open_flags::ro).then([next = std::move(next)](file f) {
       return read_log_file(std::move(f), std::move(next));
    });
}

subscription<temporary_buffer<char>> db::commitlog::read_log_file(file f, commit_load_reader_func next) {
    struct work {
        file f;
        stream<temporary_buffer<char>> s;
        input_stream<char> fin;
        input_stream<char> r;
        uint64_t id = 0;
        size_t pos = 0;
        size_t next = 0;
        bool eof = false;
        bool header = true;
        work(file f) : f(f), fin(make_file_input_stream(f)) {}
    };

    auto w = make_lw_shared<work>(std::move(f));

    auto ret = w->s.listen(std::move(next));

    w->s.started().then([w] {
        return w->fin.read_exactly(segment::descriptor_header_size).then([w](temporary_buffer<char> buf) {
           // Will throw if we got eof
           data_input in(buf);
           auto ver = in.read<uint32_t>();
           auto id = in.read<uint64_t>();
           auto checksum = in.read<uint32_t>();

           crc32_nbo crc;
           crc.process(ver);
           crc.process<int32_t>(id & 0xffffffff);
           crc.process<int32_t>(id >> 32);

           auto cs = crc.checksum();
           if (cs != checksum) {
               throw std::runtime_error("Checksum error in file header");
           }

           w->id = id;
           w->next = 0;
           w->pos = buf.size();

           auto eofcond = [w] { return w->eof; };
           return do_until(eofcond, [w] {
               assert(w->pos == w->next || w->next == 0);
               return w->fin.read_exactly(segment::segment_overhead_size).then([w](temporary_buffer<char> buf) {
                   if (buf.size() == 0) {
                       w->eof = true;
                       return make_ready_future<>();
                   }

                   data_input in(buf);
                   auto next = in.read<uint32_t>();
                   auto checksum = in.read<uint32_t>();

                   crc32_nbo crc;
                   crc.process<int32_t>(w->id & 0xffffffff);
                   crc.process<int32_t>(w->id >> 32);
                   crc.process<uint32_t>(w->pos);

                   auto cs = crc.checksum();
                   if (cs != checksum) {
                       throw std::runtime_error("Checksum error in chunk header");
                   }

                   w->pos += 8;
                   w->next = next;

                   auto eoccond = [w] { return w->pos == w->next; };
                   return do_until(eoccond, [w] {
                       static constexpr size_t entry_header_size = segment::entry_overhead_size - sizeof(uint32_t);
                       return w->fin.read_exactly(entry_header_size).then([w](temporary_buffer<char> buf) {
                           data_input in(buf);

                           auto size = in.read<uint32_t>();
                           auto checksum = in.read<uint32_t>();

                           if (size == 0) {
                               // special urchin case: zero padding due to dma blocks
                               auto slack = w->next - entry_header_size - w->pos;
                               return w->fin.read_exactly(slack).then([w, slack](temporary_buffer<char> buf) {
                                   // should eof be an error here?
                                   w->pos += slack + entry_header_size;
                               });
                           }
                           if (size < 3 * sizeof(uint32_t)) {
                               throw std::runtime_error("Invalid entry size");
                           }
                           return w->fin.read_exactly(size - entry_header_size).then([w, size, checksum](temporary_buffer<char> buf) {
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
                               w->pos += size;
                               return w->s.produce(buf.share(0, data_size));
                           });
                       });
                   });
               });
           });
        });
    }).then([w] {
        return w->s.close();
    }).finally([w] {}); // keep w alive for last close
    return ret;
}

std::vector<sstring> db::commitlog::get_active_segment_names() const {
    return _segment_manager->get_active_names();
}
