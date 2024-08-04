/*
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include <stdexcept>
#include <string>
#include <sys/stat.h>
#include <malloc.h>
#include <boost/regex.hpp>
#include <filesystem>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/adaptor/reversed.hpp>
#include <unordered_map>
#include <unordered_set>
#include <exception>
#include <filesystem>

#include <fmt/ranges.h>

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
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/coroutine/switch_to.hh>
#include <seastar/net/byteorder.hh>
#include <seastar/util/defer.hh>

#include "seastarx.hh"

#include "commitlog.hh"
#include "rp_set.hh"
#include "db/config.hh"
#include "db/extensions.hh"
#include "utils/assert.hh"
#include "utils/crc.hh"
#include "utils/runtime.hh"
#include "utils/flush_queue.hh"
#include "log.hh"
#include "commitlog_entry.hh"
#include "commitlog_extensions.hh"

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

db::commitlog::config db::commitlog::config::from_db_config(const db::config& cfg, seastar::scheduling_group sg, size_t shard_available_memory) {
    config c;

    c.sched_group = std::move(sg);
    c.commit_log_location = cfg.commitlog_directory();
    c.metrics_category_name = "commitlog";
    c.commitlog_total_space_in_mb = cfg.commitlog_total_space_in_mb() >= 0 ? cfg.commitlog_total_space_in_mb() : (shard_available_memory * smp::count) >> 20;
    c.commitlog_segment_size_in_mb = cfg.commitlog_segment_size_in_mb();
    c.commitlog_sync_period_in_ms = cfg.commitlog_sync_period_in_ms();
    c.mode = cfg.commitlog_sync() == "batch" ? sync_mode::BATCH : sync_mode::PERIODIC;
    c.extensions = &cfg.extensions();
    c.use_o_dsync = cfg.commitlog_use_o_dsync();
    c.allow_going_over_size_limit = !cfg.commitlog_use_hard_size_limit();

    if (cfg.commitlog_flush_threshold_in_mb() >= 0) {
        c.commitlog_flush_threshold_in_mb = cfg.commitlog_flush_threshold_in_mb();
    }
    if (cfg.commitlog_max_data_lifetime_in_seconds() > 0) {
        c.commitlog_data_max_lifetime_in_seconds = cfg.commitlog_max_data_lifetime_in_seconds();
    }

    return c;
}

db::commitlog::descriptor::descriptor(segment_id_type i, const std::string& fname_prefix, uint32_t v, sstring fname)
        : _filename(std::move(fname)), id(i), ver(v), filename_prefix(fname_prefix) {
}

db::commitlog::descriptor::descriptor(replay_position p, const std::string& fname_prefix)
        : descriptor(p.id, fname_prefix) {
}

const std::string db::commitlog::descriptor::SEPARATOR("-");
const std::string db::commitlog::descriptor::FILENAME_PREFIX("CommitLog" + SEPARATOR);
const std::string db::commitlog::descriptor::FILENAME_EXTENSION(".log");

static const boost::regex allowed_prefix("[a-zA-Z]+" + db::commitlog::descriptor::SEPARATOR);
static const boost::regex filename_match("(?:Recycled-)?([a-zA-Z]+" + db::commitlog::descriptor::SEPARATOR + ")(\\d+)(?:" + db::commitlog::descriptor::SEPARATOR + "(\\d+))?\\" + db::commitlog::descriptor::FILENAME_EXTENSION);

db::commitlog::descriptor::descriptor(const std::string& filename, const std::string& fname_prefix)
    : descriptor([&filename, &fname_prefix]() {
        boost::smatch m;
        // match both legacy and new version of commitlogs Ex: CommitLog-12345.log and CommitLog-4-12345.log.
        auto cbegin = filename.cbegin();
        auto pos = filename.rfind('/');
        if (pos != std::string::npos) {
            cbegin += pos + 1;
        }
        if (!boost::regex_match(cbegin, filename.cend(), m, filename_match)) {
            throw std::domain_error("Cannot parse the version of the file: " + filename);
        }
        if (m[1].str() != fname_prefix) {
            throw std::domain_error("File does not match prefix pattern: " + filename + " / " + fname_prefix);
        }
        if (m[3].length() == 0) {
            // CMH. Can most likely ignore this
            throw std::domain_error("Commitlog segment is too old to open; upgrade to 1.2.5+ first");
        }

        segment_id_type id = std::stoull(m[3].str());
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
     * Can be called more than once, if segment switch is necessary (because race)
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

class db::commitlog::segment_manager : public ::enable_shared_from_this<segment_manager> {
public:
    config cfg;
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
        static auto timeout() noexcept {
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

    class named_file : public seastar::file {
        sstring _name;
        uint64_t _known_size = 0;

        template<typename Func, typename... Args>
        auto make_awaiter(future<Args...>, Func func);
        template<typename Func, typename... Args>
        struct myawait;
    public:
        named_file(std::string_view name)
            : _name(name)
        {}
        named_file(named_file&&) = default;

        future<> open(open_flags, file_open_options, std::optional<uint64_t> size_in = std::nullopt) noexcept;
        future<> rename(std::string_view to);

        // NOT overrides. Nothing virtual. Will rely on exact type
        template <typename CharType>
        auto dma_write(uint64_t pos, const CharType* buffer, size_t len, io_intent* intent = nullptr) noexcept;
        auto dma_write(uint64_t pos, std::vector<iovec> iov, io_intent* intent = nullptr) noexcept;

        auto truncate(uint64_t length) noexcept;
        auto allocate(uint64_t position, uint64_t length) noexcept;
        auto remove_file() noexcept;

        void assign(file&& f, uint64_t size) {
            this->file::operator=(std::move(f));
            this->_known_size = size;
        }
        uint64_t known_size() const {
            return _known_size;
        }
        const sstring& name() const {
            return _name;
        }
        void maybe_update_size(uint64_t pos) {
            _known_size = std::max(pos, _known_size);
        }
    };

    // Segments dropped while not clean may not be
    // deleted. Marker enum to keep track of this.
    enum class dispose_mode : char {
        Delete, ForceDelete, Keep,
    };

    std::optional<shared_future<with_clock<db::timeout_clock>>> _segment_allocating;
    std::vector<std::pair<named_file, dispose_mode>> _files_to_dispose;

    void account_memory_usage(size_t size) noexcept {
        _request_controller.consume(size);
    }

    void notify_memory_written(size_t size) noexcept {
        _request_controller.signal(size);
    }

    template<typename T, typename R = typename T::result_type>
    requires std::derived_from<T, db::commitlog::entry_writer> && std::same_as<R, decltype(std::declval<T>().result())>
    future<R> allocate_when_possible(T writer, db::timeout_clock::time_point timeout);

    replay_position min_position();

    template<typename T>
    struct byte_flow {
        T bytes_written = 0;
        T bytes_released = 0;
        T bytes_flush_requested = 0;

        byte_flow operator+(const byte_flow& rhs) const {
            return byte_flow{
                .bytes_written = bytes_written + rhs.bytes_written,
                .bytes_released = bytes_released + rhs.bytes_released,
                .bytes_flush_requested = bytes_flush_requested + rhs.bytes_flush_requested,
            };
        }
        byte_flow operator-(const byte_flow& rhs) const {
            return byte_flow{
                .bytes_written = bytes_written - rhs.bytes_written,
                .bytes_released = bytes_released - rhs.bytes_released,
                .bytes_flush_requested = bytes_flush_requested - rhs.bytes_flush_requested,
            };
        }
        byte_flow<double> operator/(double d) const {
            return byte_flow<double>{
                .bytes_written = bytes_written / d,
                .bytes_released = bytes_released / d,
                .bytes_flush_requested = bytes_flush_requested / d,
            };
        }
    };

    struct stats : public byte_flow<uint64_t> {
        uint64_t cycle_count = 0;
        uint64_t flush_count = 0;
        uint64_t allocation_count = 0;
        uint64_t bytes_slack = 0;
        uint64_t segments_created = 0;
        uint64_t segments_destroyed = 0;
        uint64_t pending_flushes = 0;
        uint64_t flush_limit_exceeded = 0;
        uint64_t buffer_list_bytes = 0;
        // size on disk, actually used - i.e. containing data (allocate+cycle)
        uint64_t active_size_on_disk = 0;
        uint64_t wasted_size_on_disk = 0;
        // size allocated on disk - i.e. files created (new, reserve, recycled)
        uint64_t total_size_on_disk = 0;
        uint64_t requests_blocked_memory = 0;
        uint64_t blocked_on_new_segment = 0;
        uint64_t active_allocations = 0;
    };

    class scope_increment_counter {
        uint64_t& _dst;
    public:
        scope_increment_counter(uint64_t& dst)
            : _dst(dst)
        {
            ++_dst;
        }
        ~scope_increment_counter() {
            --_dst;
        }
    };

    stats totals;
    byte_flow<uint64_t> last_bytes;
    byte_flow<double> bytes_rate;

    typename std::chrono::high_resolution_clock::time_point last_time;

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
    void end_flush() noexcept {
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

    void sanity_check_size(size_t size) {
        if (size > max_mutation_size) {
            throw std::invalid_argument(
                            "Mutation of " + std::to_string(size)
                                    + " bytes is too large for the maximum size of "
                                    + std::to_string(max_mutation_size));
        }
    }

    future<> init();
    future<sseg_ptr> new_segment();
    future<sseg_ptr> active_segment(db::timeout_clock::time_point timeout);
    future<sseg_ptr> allocate_segment();
    future<sseg_ptr> allocate_segment_ex(descriptor, named_file, open_flags);

    sstring filename(const descriptor& d) const {
        return cfg.commit_log_location + "/" + d.filename();
    }

    future<> clear();
    future<> sync_all_segments();
    future<> shutdown_all_segments();
    future<> shutdown();

    void create_counters(const sstring& metrics_category_name);

    future<> orphan_all();

    void add_file_to_dispose(named_file, dispose_mode);

    future<> do_pending_deletes();
    future<> delete_segments(std::vector<sstring>);

    void discard_unused_segments() noexcept;
    void discard_completed_segments(const cf_id_type&) noexcept;
    void discard_completed_segments(const cf_id_type&, const rp_set&) noexcept;
    
    future<> force_new_active_segment() noexcept;
    future<> wait_for_pending_deletes() noexcept;

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

    buffer_type acquire_buffer(size_t s, size_t align);
    temporary_buffer<char> allocate_single_buffer(size_t, size_t);

    future<std::vector<descriptor>> list_descriptors(sstring dir) const;
    future<std::vector<sstring>> get_segments_to_replay() const;

    gc_clock::time_point min_gc_time(const cf_id_type&) const;

    flush_handler_id add_flush_handler(flush_handler h) {
        auto id = ++_flush_ids;
        _flush_handlers[id] = std::move(h);
        return id;
    }
    void remove_flush_handler(flush_handler_id id) {
        _flush_handlers.erase(id);
    }

    void flush_segments(uint64_t size_to_remove);
    void check_no_data_older_than_allowed();

private:
    class shutdown_marker{};

    future<> clear_reserve_segments();
    void abort_recycled_list(std::exception_ptr);

    size_t max_request_controller_units() const;
    segment_id_type _ids = 0, _low_id = 0;
    std::vector<sseg_ptr> _segments;
    queue<sseg_ptr> _reserve_segments;
    queue<named_file> _recycled_segments;
    std::unordered_map<flush_handler_id, flush_handler> _flush_handlers;
    flush_handler_id _flush_ids = 0;
    replay_position _flush_position;
    timer<clock_type> _timer;
    future<> replenish_reserve();
    future<> _reserve_replenisher;
    future<> _background_sync;
    seastar::gate _gate;
    uint64_t _new_counter = 0;
    std::optional<size_t> _disk_write_alignment;
    future<> _pending_deletes = make_ready_future<>();
};

future<> db::commitlog::segment_manager::named_file::open(open_flags flags, file_open_options opt, std::optional<uint64_t> size_in) noexcept {
    SCYLLA_ASSERT(!*this);
    auto f = co_await open_file_dma(_name, flags, opt);
    // bypass roundtrip to disk if caller knows size, or open flags truncated file
    auto existing_size = size_in 
        ? *size_in
        : (flags & open_flags::truncate) == open_flags{}
            ? co_await f.size()
            : 0
        ;
    assign(std::move(f), existing_size);
}

future<> db::commitlog::segment_manager::named_file::rename(std::string_view to) {
    SCYLLA_ASSERT(!*this);
    try {
        auto s = sstring(to);
        auto dir = std::filesystem::path(to).parent_path();
        co_await seastar::rename_file(_name, s);
        _name = std::move(s);
        co_await seastar::sync_directory(dir.string());
    } catch (...) {
        commit_error_handler(std::current_exception());
        throw;
    }
}

/**
 * Special awaiter for chaining a callee-continuation (essentially "then") into calling
 * co-routine frame, by storing the continuation not as a chained task, but a func 
 * in the returned awaiter. The function is only executed iff we don't except
 * the base call.
 * This is a very explicit way to optimize away a then or coroutine frame, the latter
 * which the compiler really should be able to coalesque, but...
 */
template<typename Func, typename... Args>
struct db::commitlog::segment_manager::named_file::myawait : public seastar::internal::awaiter<true, Args...> {
    using mybase = seastar::internal::awaiter<true, Args...>;
    using resume_type = decltype(std::declval<mybase>().await_resume());

    Func _func;

    myawait(future<Args...> f, Func func)
        : mybase(std::move(f))
        , _func(std::move(func))
    {}
    resume_type await_resume() {
        if constexpr (std::is_same_v<resume_type, void>) {
            mybase::await_resume();
            _func();
        } else {
            return _func(mybase::await_resume());
        }
    }
};

template<typename Func, typename... Args>
auto db::commitlog::segment_manager::named_file::make_awaiter(future<Args...> f, Func func) {
    return myawait<Func, Args...>(std::move(f), std::move(func));
}

template <typename CharType>
auto db::commitlog::segment_manager::named_file::dma_write(uint64_t pos, const CharType* buffer, size_t len, io_intent* intent) noexcept {
    return make_awaiter(file::dma_write(pos, buffer, len, intent), [this, pos](size_t res) {
        maybe_update_size(pos + res);
        return res;
    });
}

auto db::commitlog::segment_manager::named_file::dma_write(uint64_t pos, std::vector<iovec> iov, io_intent* intent) noexcept {
    return make_awaiter(file::dma_write(pos, std::move(iov), intent), [this, pos](size_t res) {
        maybe_update_size(pos + res);
        return res;
    });
}

auto db::commitlog::segment_manager::named_file::truncate(uint64_t length) noexcept {
    return make_awaiter(file::truncate(length), [this, length] {
        _known_size = length;
    });
}

auto db::commitlog::segment_manager::named_file::allocate(uint64_t position, uint64_t length) noexcept {
    return make_awaiter(file::allocate(position, length), [this, position, length] {
        _known_size = position + length;
    });
}

auto db::commitlog::segment_manager::named_file::remove_file() noexcept {
    return make_awaiter(seastar::remove_file(name()), [this] {
        _known_size = 0;
    });
}

template<typename T>
static void write(db::commitlog::output& out, T value) {
    auto v = net::hton(value);
    out.write(reinterpret_cast<const char*>(&v), sizeof(v));
}

template<typename T>
static void write(fragmented_temporary_buffer::ostream& out, T value) {
    auto v = net::hton(value);
    out.write(reinterpret_cast<const char*>(&v), sizeof(v));
}

template<typename T, typename Input>
std::enable_if_t<std::is_fundamental<T>::value, T> read(Input& in) {
    return net::ntoh(in.template read<T>());
}

detail::sector_split_iterator::sector_split_iterator(const sector_split_iterator&) noexcept = default;

detail::sector_split_iterator::sector_split_iterator()
    : _ptr(nullptr)
    , _size(0)
    , _sector_size(0)
{}

detail::sector_split_iterator::sector_split_iterator(base_iterator i, base_iterator e, size_t sector_size)
    : _iter(i)
    , _end(e)
    , _ptr(i != e ? const_cast<char*>(i->get()) : nullptr)
    , _size(i != e ? sector_size - sector_overhead_size : 0)
    , _sector_size(sector_size)
{}

detail::sector_split_iterator& detail::sector_split_iterator::operator++() {
    SCYLLA_ASSERT(_iter != _end);
    _ptr += _sector_size;
    // check if we have more pages in this temp-buffer (in out case they are always aligned + sized in page units)
    auto rem = _iter->size() - std::distance(_iter->get(), const_cast<const char*>(_ptr));
    if (rem == 0) {
        if (++_iter == _end) {
            _ptr = nullptr;
            _size = 0; 
            return *this;
        }
        rem = _iter->size();
        SCYLLA_ASSERT(rem >= _sector_size);
        // booh. ugly.
        _ptr = const_cast<char*>(_iter->get());
    }
    return *this;
}

detail::sector_split_iterator detail::sector_split_iterator::operator++(int) {
    auto res = *this;
    ++(*this);
    return res;
}

/*
 * A single commit log file on disk. Manages creation of the file and writing mutations to disk,
 * as well as tracking the last mutation position of any "dirty" CFs covered by the segment file. Segment
 * files are initially allocated to a fixed size and can grow to accommodate a larger value if necessary.
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
 * We currently do not wait for flushes to finish before issuing the next
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
    using named_file = segment_manager::named_file;
    using dispose_mode = segment_manager::dispose_mode;

    ::shared_ptr<segment_manager> _segment_manager;

    descriptor _desc;
    named_file _file;

    uint64_t _file_pos = 0;
    uint64_t _flush_pos = 0;
    uint64_t _waste = 0;

    size_t _alignment;

    bool _closed = false;
    bool _terminated = false;

    using buffer_type = segment_manager::buffer_type;
    using sseg_ptr = segment_manager::sseg_ptr;
    using clock_type = segment_manager::clock_type;
    using time_point = segment_manager::time_point;

    using base_ostream_type = memory_output_stream<detail::sector_split_iterator>;
    using frag_ostream_type = typename base_ostream_type::fragmented;

    buffer_type _buffer;
    base_ostream_type _buffer_ostream;
    size_t _buffer_ostream_size = 0;
    std::unordered_map<cf_id_type, uint64_t> _cf_dirty;
    std::unordered_map<cf_id_type, gc_clock::time_point> _cf_min_time;
    time_point _sync_time;
    utils::flush_queue<replay_position, std::less<replay_position>, clock_type> _pending_ops;

    uint64_t _num_allocs = 0;

    std::unordered_set<table_schema_version> _known_schema_versions;

    friend sstring format_as(const segment& s) {
        return s._desc.filename();
    }

    friend std::ostream& operator<<(std::ostream&, const segment&);
    friend class segment_manager;

    size_t sector_overhead(size_t size) const {
        return (size / (_alignment - detail::sector_overhead_size)) * detail::sector_overhead_size;
    }

    size_t buffer_position() const {
        // need some arithmetic to figure out what out actual position is, including
        // page checksums etc. The ostream does not include this, as it is subdivided and
        // skips sector_overhead parts of the memory buffer. So to get actual position 
        // in the buffer, we need to add it back.

        // #16298 - this was based on _buffer_size vs. remaining in ostream.
        // ostream type has no "position" type, need to keep track of 
        // what the original size was, i.e. _buffer.size() adjusted down
        // for sector overhead.
        auto used = _buffer_ostream_size - _buffer_ostream.size();
        return used + sector_overhead(used);
    }

    future<> begin_flush() {
        // This is maintaining the semantica of only using the write-lock
        // as a gate for flushing, i.e. once we've begun a flush for position X
        // we are ok with writes to positions > X
        return _segment_manager->begin_flush();
    }

    void end_flush() {
        _segment_manager->end_flush();
        if (can_delete()) {
            _segment_manager->discard_unused_segments();
        }
    }

public:
    struct cf_mark {
        const segment& s;
    };
    friend struct fmt::formatter<cf_mark>;

    // The commit log entry overhead in bytes (int: length + int: head checksum)
    static constexpr size_t entry_overhead_size = 2 * sizeof(uint32_t);
    static constexpr size_t multi_entry_overhead_size = entry_overhead_size + sizeof(uint32_t);
    static constexpr size_t segment_overhead_size = 2 * sizeof(uint32_t);
    static constexpr size_t descriptor_header_size = 6 * sizeof(uint32_t);
    static constexpr uint32_t segment_magic = ('S'<<24) |('C'<< 16) | ('L' << 8) | 'C';
    static constexpr uint32_t multi_entry_size_magic = 0xffffffff;

    // The commit log (chained) sync marker/header size in bytes (int: length + int: checksum [segmentId, position])
    static constexpr size_t sync_marker_size = 2 * sizeof(uint32_t);

    // TODO : tune initial / default size
    static constexpr size_t default_size = 128 * 1024;

    segment(::shared_ptr<segment_manager> m, descriptor&& d, named_file&& f, size_t alignment)
            : _segment_manager(std::move(m)), _desc(std::move(d)), _file(std::move(f)),
        _alignment(alignment),
        _sync_time(clock_type::now()), _pending_ops(true) // want exception propagation
    {
        ++_segment_manager->totals.segments_created;
        clogger.debug("Created new segment {}", *this);
    }
    ~segment() {
        dispose_mode mode = dispose_mode::Keep;

        if (is_clean()) {
            clogger.debug("Segment {} is no longer active and will submitted for delete now", *this);
            ++_segment_manager->totals.segments_destroyed;
            _segment_manager->totals.active_size_on_disk -= file_position();
            _segment_manager->totals.bytes_released += file_position();
            _segment_manager->totals.wasted_size_on_disk -= _waste;
            mode = dispose_mode::Delete;
        } else if (_segment_manager->cfg.warn_about_segments_left_on_disk_after_shutdown) {
            clogger.warn("Segment {} is dirty and is left on disk.", *this);
        }

        _segment_manager->totals.buffer_list_bytes -= _buffer.size_bytes();

        if (mode != dispose_mode::Keep || _file) {
            _segment_manager->add_file_to_dispose(std::move(_file), mode);
        }
    }

    uint64_t size_on_disk() const noexcept {
        return _file.known_size();
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
        // could have kept the "finally" continuations
        // here, but this potentially missed immediate 
        // exceptions thrown in close/p_o.close.
        std::exception_ptr p;
        try {
            co_await close();
        } catch (...) {
            p = std::current_exception();
        }
    
        co_await _pending_ops.close();
        co_await _file.truncate(_flush_pos);
        co_await _file.close();

        if (p) {
            co_return coroutine::exception(std::move(p));
        }

        co_return me;
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
        SCYLLA_ASSERT(me.use_count() > 1);
        uint64_t pos = _file_pos;

        clogger.trace("Syncing {} {} -> {}", *this, _flush_pos, pos);

        // Only run the flush when all write ops at lower rp:s
        // have completed.
        replay_position rp(_desc.id, position_type(pos));

        // Run like this to ensure flush ordering, and making flushes "waitable"
        co_await _pending_ops.run_with_ordered_post_op(rp, [] {}, [&] {
            SCYLLA_ASSERT(_pending_ops.has_operation(rp));
            return do_flush(pos);
        });
        co_return me;
    }
    future<sseg_ptr> terminate() {
        SCYLLA_ASSERT(_closed);
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
        auto s = co_await sync();
        co_await flush();
        co_await terminate();
        _waste = _file.known_size() - file_position();
        _segment_manager->totals.wasted_size_on_disk += _waste;
        co_return s;
    }
    future<sseg_ptr> do_flush(uint64_t pos) {
        auto me = shared_from_this();
        co_await begin_flush();

        auto finally = defer([&] () noexcept {
            end_flush();
        });

        if (pos <= _flush_pos) {
            clogger.trace("{} already synced! ({} < {})", *this, pos, _flush_pos);
            co_return me;
        }

        try {
            co_await _file.flush();
            // TODO: retry/ignore/fail/stop - optional behaviour in origin.
            // we fast-fail the whole commit.
            _flush_pos = std::max(pos, _flush_pos);
            ++_segment_manager->totals.flush_count;
            clogger.trace("{} synced to {}", *this, _flush_pos);
        } catch (...) {
            clogger.error("Failed to flush commits to disk: {}", std::current_exception());
            throw;
        }

        co_return me;
    }

    /**
     * Allocate a new buffer
     */
    void new_buffer(size_t s) {
        SCYLLA_ASSERT(_buffer.empty());

        auto overhead = segment_overhead_size;
        if (_file_pos == 0) {
            overhead += descriptor_header_size;
        }

        s += overhead;
        // add bookkeep data reqs. 
        auto a = align_up(s + sector_overhead(s), _alignment);
        auto k = std::max(a, default_size);

        _buffer = _segment_manager->acquire_buffer(k, _alignment);
        auto size = _buffer.size_bytes();
        auto n_blocks = size / _alignment;
        // the amount of data we can actually write into.
        auto useable_size = size - n_blocks * detail::sector_overhead_size;

        SCYLLA_ASSERT(useable_size >= s);

        _buffer_ostream = frag_ostream_type(detail::sector_split_iterator(_buffer.begin(), _buffer.end(), _alignment), useable_size);
        // #16298 - keep track of ostream initial size.
        _buffer_ostream_size = useable_size;

        auto out = _buffer_ostream.write_substream(overhead);
        out.fill('\0', overhead);
        _segment_manager->totals.buffer_list_bytes += _buffer.size_bytes();
        // #18488
        // we should be in a allocate or terminate call. In either case, account for overhead now already.
        _segment_manager->account_memory_usage(overhead);

        SCYLLA_ASSERT(buffer_position() == overhead);
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
        auto me = shared_from_this();

        if (_buffer.empty() && !termination) {
            if (flush_after) {
                co_await flush();
            }
            co_return me;
        }

        auto size = clear_buffer_slack();
        auto buf = std::exchange(_buffer, { });
        auto off = _file_pos;
        auto top = off + size;
        auto num = _num_allocs;

        _file_pos = top;
        _buffer_ostream = { };
        _buffer_ostream_size = 0;
        _num_allocs = 0;

        SCYLLA_ASSERT(me.use_count() > 1);

        auto out = buf.get_ostream();

        auto header_size = 0;

        if (off == 0) {
            // first block. write file header.
            write(out, segment_magic);
            write(out, _desc.ver);
            write(out, _desc.id);
            write(out, uint32_t(_alignment));
            crc32_nbo crc;
            crc.process(_desc.ver);
            crc.process<int32_t>(_desc.id & 0xffffffff);
            crc.process<int32_t>(_desc.id >> 32);
            crc.process<uint32_t>(uint32_t(_alignment));
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
            SCYLLA_ASSERT(num == 0);
            SCYLLA_ASSERT(_closed);
            clogger.trace("Terminating {} at pos {}", *this, _file_pos);
            write(out, uint64_t(0));
        }

        buf.remove_suffix(buf.size_bytes() - size);

        // Build sector checksums.
        auto id = net::hton(_desc.id);
        auto ss = _alignment - detail::sector_overhead_size;

        for (auto& tbuf : buf) {
            auto* p = const_cast<char*>(tbuf.get());
            auto* e = p + tbuf.size();
            while (p != e) {
                SCYLLA_ASSERT(align_up(p, _alignment) == p);

                // include segment id in crc:ed data
                auto be = p + ss;
                be = std::copy_n(reinterpret_cast<char*>(&id), sizeof(id), be);

                crc32_nbo crc;
                crc.process_bytes(p, std::distance(p, be));

                auto checksum = crc.checksum();
                auto v = net::hton(checksum);
                // write checksum.
                p = std::copy_n(reinterpret_cast<char*>(&v), sizeof(v), be);
            }
        }

        replay_position rp(_desc.id, position_type(off));

        // The write will be allowed to start now, but flush (below) must wait for not only this,
        // but all previous write/flush pairs.
        co_await _pending_ops.run_with_ordered_post_op(rp, [&]() -> future<> {
            auto view = fragmented_temporary_buffer::view(buf);
            view.remove_suffix(buf.size_bytes() - size);
            SCYLLA_ASSERT(size == view.size_bytes());

            if (view.empty()) {
                co_return;
            }

            auto finally = defer([&] () noexcept {
                _segment_manager->notify_memory_written(size);
                _segment_manager->totals.buffer_list_bytes -= buf.size_bytes();
                if (_file.known_size() < _file_pos) {
                    _segment_manager->totals.total_size_on_disk += (_file_pos - _file.known_size());
                }
            });

            co_await coroutine::switch_to(_segment_manager->cfg.sched_group);

            for (;;) {
                auto current = *view.begin();
                try {
                    auto bytes = co_await _file.dma_write(off, current.data(), current.size());
                    _segment_manager->totals.bytes_written += bytes;
                    _segment_manager->totals.active_size_on_disk += bytes;
                    ++_segment_manager->totals.cycle_count;
                    if (bytes == view.size_bytes()) {
                        clogger.trace("Final write of {} to {}: {}/{} bytes at {}", bytes, *this, size, size, off);
                        break;
                    }
                    // gah, partial write. should always get here with dma chunk sized
                    // "bytes", but lets make sure...
                    bytes = align_down(bytes, _alignment);
                    off += bytes;
                    view.remove_prefix(bytes);
                    clogger.trace("Partial write of {} to {}: {}/{} bytes at at {}", bytes, *this, size - view.size_bytes(), size, off - bytes);
                    continue;
                    // TODO: retry/ignore/fail/stop - optional behaviour in origin.
                    // we fast-fail the whole commit.
                } catch (...) {
                    clogger.error("Failed to persist commits to disk for {}: {}", *this, std::current_exception());
                    throw;
                }
            }
        }, [&]() -> future<> {
            SCYLLA_ASSERT(_pending_ops.has_operation(rp));
            if (flush_after) {
                co_await do_flush(top);
            }
        });
        co_return me;
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
        try {
            co_await _pending_ops.wait_for_pending(timeout);
            if (fp != _file_pos) {
                // some other request already wrote this buffer.
                // If so, wait for the operation at our intended file offset
                // to finish, then we know the flush is complete and we
                // are in accord.
                // (Note: wait_for_pending(pos) waits for operation _at_ pos (and before),
                replay_position rp(_desc.id, position_type(fp));
                co_await _pending_ops.wait_for_pending(rp, timeout);
                
                SCYLLA_ASSERT(_segment_manager->cfg.mode != sync_mode::BATCH || _flush_pos > fp);
                if (_flush_pos <= fp) {
                    // previous op we were waiting for was not sync one, so it did not flush
                    // force flush here
                    co_await do_flush(fp);
                }
            } else {
                // It is ok to leave the sync behind on timeout because there will be at most one
                // such sync, all later allocations will block on _pending_ops until it is done.
                co_await with_timeout(timeout, sync());
            }
        } catch (...) {
            // If we get an IO exception (which we assume this is)
            // we should close the segment.
            // TODO: should we also truncate away any partial write
            // we did?
            me->_closed = true; // just mark segment as closed, no writes will be done.
            throw;
        };
        co_return me;
    }

    void background_cycle() {
        //FIXME: discarded future
        (void)cycle().discard_result().handle_exception([] (auto ex) {
            clogger.error("Failed to flush commits to disk: {}", ex);
        });
    }

    enum class write_result {
        ok,
        must_sync,
        no_space,
        ok_need_batch_sync,
    };

    /**
     * Add a "mutation" to the segment.
     * Should only be called from "allocate_when_possible". "this" must be secure in a shared_ptr that will not
     * die. We don't keep ourselves alive (anymore)
     */
    write_result allocate(entry_writer& writer, segment_manager::request_controller_units& permit, db::timeout_clock::time_point timeout) {
        if (must_sync()) {
            return write_result::must_sync;
        }

        const auto size = writer.size(*this);
        const auto s = size + writer.num_entries * entry_overhead_size + (writer.num_entries > 1 ? multi_entry_overhead_size : 0u); // total size

        _segment_manager->sanity_check_size(s);

        if (!is_still_allocating() || next_position(s) > _segment_manager->max_size) { // would we make the file too big?
            return write_result::no_space;
        } else if (!_buffer.empty() && (s > _buffer_ostream.size())) {  // enough data?
            if (_segment_manager->cfg.mode == sync_mode::BATCH || writer.sync) {
                // TODO: this could cause starvation if we're really unlucky.
                // If we run batch mode and find ourselves not fit in a non-empty
                // buffer, we must force a cycle and wait for it (to keep flush order)
                // This will most likely cause parallel writes, and consecutive flushes.
                return write_result::must_sync;
            }
            background_cycle();
        }

        if (_buffer.empty()) {
            new_buffer(s);
        }

        if (_closed) {
            throw std::runtime_error("commitlog: Cannot add data to a closed segment");
        }

        auto pos = buffer_position();
        auto& out = _buffer_ostream;

        std::optional<crc32_nbo> mecrc;

        // if this is multi-entry write, we need to add an extra header + crc
        // the header and crc formula is:
        // header:
        //      magic : uint32_t
        //      size  : uint32_t
        //      crc1  : uint32_t - crc of magic, size
        // -> entries[]
        if (writer.num_entries > 1) {
            mecrc.emplace();
            write<uint32_t>(out, multi_entry_size_magic);
            write<uint32_t>(out, s);
            mecrc->process(multi_entry_size_magic);
            mecrc->process(uint32_t(s));
            write<uint32_t>(out, mecrc->checksum());
        }

        for (size_t entry = 0; entry < writer.num_entries; ++entry) {
            replay_position rp(_desc.id, position());
            auto id = writer.id(entry);
            auto entry_size = writer.num_entries == 1 ? size : writer.size(*this, entry);
            auto es = entry_size + entry_overhead_size;

            _cf_dirty[id]++; // increase use count for cf.
            _cf_min_time.emplace(id, gc_clock::now()); // if value already exists this does nothing.

            rp_handle h(static_pointer_cast<cf_holder>(shared_from_this()), std::move(id), rp);

            crc32_nbo crc;

            write<uint32_t>(out, es);
            crc.process(uint32_t(es));
            write<uint32_t>(out, crc.checksum());

            // actual data
            auto entry_out = out.write_substream(entry_size);
            writer.write(*this, entry_out, entry);
            writer.result(entry, std::move(h));
        }

        auto npos = buffer_position();

        // #18488
        // When released (notify_memory_written), it will be based on bytes on disk.
        // Do this account based on "disk bytes" (buffer really), i.e. accounting for
        // sector boundaries and CRC overhead.
        auto buf_memory = npos - pos - permit.release(); /* size in permit was already subtracted from sem count - ignore it here */
        _segment_manager->account_memory_usage(buf_memory);

        ++_segment_manager->totals.allocation_count;
        ++_num_allocs;

        if (_segment_manager->cfg.mode == sync_mode::BATCH || writer.sync) {
            return write_result::ok_need_batch_sync;
        } else {
            // If this buffer alone is too big, potentially bigger than the maximum allowed size,
            // then no other request will be allowed in to force the cycle()ing of this buffer. We
            // have to do it ourselves.
            if ((npos >= (db::commitlog::segment::default_size))) {
                background_cycle();
            }
        }
        return write_result::ok;
    }

    position_type position() const {
        return position_type(_file_pos + buffer_position());
    }

    position_type next_position(size_t size) const {
        auto used = _buffer_ostream_size - _buffer_ostream.size();
        used += size;
        return _file_pos + used + sector_overhead(used);
    }

    size_t file_position() const {
        return _file_pos;
    }

    // ensures no more of this segment is writeable, by allocating any unused section at the end and marking it discarded
    // a.k.a. zero the tail.
    size_t clear_buffer_slack() {
        auto buf_pos = buffer_position();
        auto size = align_up(buf_pos, _alignment);
        auto fill_size = size - buf_pos;
        if (fill_size > 0) {
            // we want to fill to a sector boundary, must leave room for metadata
            SCYLLA_ASSERT((fill_size - detail::sector_overhead_size) <= _buffer_ostream.size());
            _buffer_ostream.fill('\0', fill_size - detail::sector_overhead_size);
            _segment_manager->totals.bytes_slack += fill_size;
            _segment_manager->account_memory_usage(fill_size);
        }
        return size;
    }
    void mark_clean(const cf_id_type& id, uint64_t count) noexcept {
        auto i = _cf_dirty.find(id);
        if (i != _cf_dirty.end()) {
            SCYLLA_ASSERT(i->second >= count);
            i->second -= count;
            if (i->second == 0) {
                _cf_dirty.erase(i);
            }
        }
    }
    void mark_clean(const cf_id_type& id) noexcept {
        _cf_dirty.erase(id);
    }
    void mark_clean() noexcept {
        _cf_dirty.clear();
    }
    bool is_still_allocating() const noexcept {
        return !_closed && position() < _segment_manager->max_size;
    }
    bool is_clean() const noexcept {
        return _cf_dirty.empty();
    }
    bool is_unused() const noexcept {
        return !is_still_allocating() && is_clean();
    }
    bool is_flushed() const noexcept {
        return position() <= _flush_pos;
    }
    bool can_delete() const noexcept {
        return is_unused() && is_flushed();
    }
    bool contains(const replay_position& pos) const noexcept {
        return pos.id == _desc.id;
    }
    sstring get_segment_name() const {
        return _desc.filename();
    }
    gc_clock::time_point min_time(const cf_id_type& id) const {
        auto i = _cf_min_time.find(id);
        return i == _cf_min_time.end() ? gc_clock::time_point::max() : i->second;
    }
};

db::replay_position db::commitlog::segment_manager::min_position() {
    if (_segments.empty()) {
        return {_ids, 0};
    } else {
        return {_segments.front()->_desc.id, 0};
    }
}

template<typename T, typename R>
requires std::derived_from<T, db::commitlog::entry_writer> && std::same_as<R, decltype(std::declval<T>().result())>
future<R> db::commitlog::segment_manager::allocate_when_possible(T writer, db::timeout_clock::time_point timeout) {
    auto size = writer.size();
    // If this is already too big now, we should throw early. It's also a correctness issue, since
    // if we are too big at this moment we'll never reach allocate() to actually throw at that
    // point.
    sanity_check_size(size);

    auto fut = get_units(_request_controller, size, timeout);
    if (_request_controller.waiters()) {
        totals.requests_blocked_memory++;
    }

    scope_increment_counter allocating(totals.active_allocations);

    auto permit = co_await std::move(fut);
    sseg_ptr s;

    if (!_segments.empty() && _segments.back()->is_still_allocating()) {
        s = _segments.back();
    } else {
        s = co_await active_segment(timeout);
    }

    for (;;) {
        using write_result = segment::write_result;

        switch (s->allocate(writer, permit, timeout)) {
            case write_result::ok:
                co_return writer.result();
            case write_result::must_sync:
                s = co_await with_timeout(timeout, s->sync());
                continue;
            case write_result::no_space:
                s = co_await s->finish_and_get_new(timeout);
                continue;
            case write_result::ok_need_batch_sync:
                s = co_await s->batch_cycle(timeout);
                co_return writer.result();
        }
    }
}

const size_t db::commitlog::segment::default_size;

db::commitlog::segment_manager::segment_manager(config c)
    : cfg([&c] {
        config cfg(c);

        if (cfg.commit_log_location.empty()) {
            cfg.commit_log_location = "/var/lib/scylla/commitlog";
        }

        if (cfg.max_active_flushes == 0) {
            cfg.max_active_flushes = // TODO: call someone to get an idea...
                            5 * smp::count;
        }
        cfg.max_active_flushes = std::max(uint64_t(1), cfg.max_active_flushes / smp::count);

        if (!cfg.base_segment_id) {
            cfg.base_segment_id = std::chrono::duration_cast<std::chrono::milliseconds>(runtime::get_boot_time().time_since_epoch()).count() + 1;
        }

        return cfg;
    }())
    , max_size(std::min<size_t>(std::numeric_limits<position_type>::max() / (1024 * 1024), std::max<size_t>(cfg.commitlog_segment_size_in_mb, 1)) * 1024 * 1024)
    , max_mutation_size(max_size >> 1)
    , max_disk_size(size_t(std::ceil(cfg.commitlog_total_space_in_mb / double(smp::count))) * 1024 * 1024)
    // our threshold for trying to force a flush. needs heristics, for now max - segment_size/2.
    , disk_usage_threshold([&] {
        if (cfg.commitlog_flush_threshold_in_mb.has_value()) {
            return size_t(std::ceil(*cfg.commitlog_flush_threshold_in_mb / double(smp::count))) * 1024 * 1024;
        } else {
            return max_disk_size / 2;
        }
    }())
    , _flush_semaphore(cfg.max_active_flushes)
    // That is enough concurrency to allow for our largest mutation (max_mutation_size), plus
    // an existing in-flight buffer. Since we'll force the cycling() of any buffer that is bigger
    // than default_size at the end of the allocation, that allows for every valid mutation to
    // always be admitted for processing.
    , _request_controller(max_request_controller_units(), request_controller_timeout_exception_factory{})
    , _reserve_segments(1)
    , _recycled_segments(std::numeric_limits<size_t>::max())
    , _reserve_replenisher(make_ready_future<>())
    , _background_sync(make_ready_future<>())
{
    SCYLLA_ASSERT(max_size > 0);
    SCYLLA_ASSERT(max_mutation_size < segment::multi_entry_size_magic);

    clogger.trace("Commitlog {} maximum disk size: {} MB / cpu ({} cpus)",
            cfg.commit_log_location, max_disk_size / (1024 * 1024),
            smp::count);

    if (!cfg.metrics_category_name.empty()) {
        create_counters(cfg.metrics_category_name);
    }
    if (!boost::regex_match(cfg.fname_prefix, allowed_prefix)) {
        throw std::invalid_argument("Invalid filename prefix: " + cfg.fname_prefix);
    }
}

size_t db::commitlog::segment_manager::max_request_controller_units() const {
    return max_mutation_size + db::commitlog::segment::default_size;
}

future<> db::commitlog::segment_manager::replenish_reserve() {
    while (!_shutdown) {
        co_await _reserve_segments.not_full();
        if (_shutdown) {
            break;
        }
        try {
            gate::holder g(_gate);
            // note: if we were strict with disk size, we would refuse to do this 
            // unless disk footprint is lower than threshold. but we cannot (yet?)
            // trust that flush logic will absolutely free up an existing 
            // segment (because colocation stuff etc), so always allow a new
            // file if needed. That and performance stuff...
            auto s = co_await allocate_segment();
            auto ret = _reserve_segments.push(std::move(s));
            if (!ret) {
                clogger.error("Segment reserve is full! Ignoring and trying to continue, but shouldn't happen");
            }
            continue;
        } catch (shutdown_marker&) {
            break;
        } catch (...) {
            clogger.warn("Exception in segment reservation: {}", std::current_exception());
        }
        co_await sleep(100ms);
    }
}

future<std::vector<db::commitlog::descriptor>>
db::commitlog::segment_manager::list_descriptors(sstring dirname) const {
    auto dir = co_await open_checked_directory(commit_error_handler, dirname);
    std::vector<db::commitlog::descriptor> result;

    auto is_cassandra_segment = [](sstring name) {
        // We want to ignore commitlog segments generated by Cassandra-derived tools (#1112)
        auto c = sstring("Cassandra");
        if (name.size() < c.size()) {
            return false;
        }
        return name.substr(0, c.size()) == c;
    };

    auto h = dir.list_directory([&](directory_entry de) -> future<> {
        auto type = de.type;
        if (!type && !de.name.empty()) {
            type = co_await file_type(dirname + "/" + de.name);
        }
        if (type == directory_entry_type::regular && de.name[0] != '.' && !is_cassandra_segment(de.name)) {
            try {
                result.emplace_back(de.name, cfg.fname_prefix);
            } catch (std::domain_error& e) {
                clogger.warn("{}", e.what());
            }
        }
    });
    co_await h.done();
    co_return result;
}

// #11237 - make get_segments_to_replay on-demand. Since we base the time-part of
// descriptor ids on highest of wall-clock and segments found on disk on init,
// we can just scan files now and include only those representing generations before
// the creation of this commitlog instance.
// This _could_ give weird results iff we had a truly sharded commitlog folder
// where init of the instances was not very synced _and_ we allowed more than
// one shard to do replay. But allowed usage always either is one dir - one shard (hints)
// or does shard 0 init first (main/database), then replays on 0 as well.
future<std::vector<sstring>> db::commitlog::segment_manager::get_segments_to_replay() const {
    std::vector<sstring> segments_to_replay;
    auto descs = co_await list_descriptors(cfg.commit_log_location);
    for (auto& d : descs) {
        auto id = replay_position(d.id).base_id();
        if (id <= _low_id) {
            segments_to_replay.push_back(cfg.commit_log_location + "/" + d.filename());
        }
    }
    co_return segments_to_replay;
}

gc_clock::time_point db::commitlog::segment_manager::min_gc_time(const cf_id_type& id) const {
    auto res = gc_clock::time_point::max();
    for (auto& s : _segments) {
        res = std::min(res, s->min_time(id));
    }
    return res;
}

future<> db::commitlog::segment_manager::init() {
    auto descs = co_await list_descriptors(cfg.commit_log_location);

    SCYLLA_ASSERT(_reserve_segments.empty()); // _segments_to_replay must not pick them up
    segment_id_type id = *cfg.base_segment_id;
    for (auto& d : descs) {
        id = std::max(id, replay_position(d.id).base_id());
    }

    // base id counter is [ <shard> | <base> ]
    _ids = replay_position(this_shard_id(), id).id;
    _low_id = id;

    // always run the timer now, since we need to handle segment pre-alloc etc as well.
    _timer.set_callback(std::bind(&segment_manager::on_timer, this));
    auto delay = this_shard_id() * std::ceil(double(cfg.commitlog_sync_period_in_ms) / smp::count);
    clogger.trace("Delaying timer loop {} ms", delay);
    // We need to wait until we have scanned all other segments to actually start serving new
    // segments. We are ready now
    _reserve_replenisher = with_scheduling_group(cfg.sched_group, [this] { return replenish_reserve(); });
    arm(delay);
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

        sm::make_counter("alloc", totals.allocation_count,
                       sm::description("Counts number of times a new mutation has been added to a segment. "
                                       "Divide bytes_written by this value to get the average number of bytes per mutation written to the disk.")),

        sm::make_counter("cycle", totals.cycle_count,
                       sm::description("Counts number of commitlog write cycles - when the data is written from the internal memory buffer to the disk.")),

        sm::make_counter("flush", totals.flush_count,
                       sm::description("Counts number of times the flush() method was called for a file.")),

        sm::make_counter("bytes_written", totals.bytes_written,
                       sm::description("Counts number of bytes written to the disk. "
                                       "Divide this value by \"alloc\" to get the average number of bytes per mutation written to the disk.")),

        sm::make_counter("bytes_released", totals.bytes_released,
                       sm::description("Counts number of bytes released from disk. (Deleted/recycled)")),

        sm::make_counter("bytes_flush_requested", totals.bytes_flush_requested,
                       sm::description("Counts number of bytes requested to be flushed (persisted).")),

        sm::make_counter("slack", totals.bytes_slack,
                       sm::description("Counts number of unused bytes written to the disk due to disk segment alignment.")),

        sm::make_gauge("pending_flushes", totals.pending_flushes,
                       sm::description("Holds number of currently pending flushes. See the related flush_limit_exceeded metric.")),

        sm::make_gauge("pending_allocations", [this] { return pending_allocations(); },
                       sm::description("Holds number of currently pending allocations. "
                                       "A non-zero value indicates that we have a bottleneck in the disk write flow.")),

        sm::make_counter("requests_blocked_memory", totals.requests_blocked_memory,
                       sm::description("Counts number of requests blocked due to memory pressure. "
                                       "A non-zero value indicates that the commitlog memory quota is not enough to serve the required amount of requests.")),

        sm::make_counter("flush_limit_exceeded", totals.flush_limit_exceeded,
                       sm::description(
                           seastar::format("Counts number of times a flush limit was exceeded. "
                                           "A non-zero value indicates that there are too many pending flush operations (see pending_flushes) and some of "
                                           "them will be blocked till the total amount of pending flush operations drops below {}.", cfg.max_active_flushes))),

        sm::make_gauge("disk_total_bytes", totals.total_size_on_disk,
                       sm::description("Holds size of disk space in bytes reserved for data so far. "
                                       "A too high value indicates that we have some bottleneck in the writing to sstables path.")),

        sm::make_gauge("disk_active_bytes", totals.active_size_on_disk,
                       sm::description("Holds size of disk space in bytes used for data so far. "
                                       "A too high value indicates that we have some bottleneck in the writing to sstables path.")),

        sm::make_gauge("disk_slack_end_bytes", totals.wasted_size_on_disk,
                       sm::description("Holds size of disk space in bytes unused because of segment switching (end slack). "
                                       "A too high value indicates that we do not write enough data to each segment.")),

        sm::make_gauge("memory_buffer_bytes", totals.buffer_list_bytes,
                       sm::description("Holds the total number of bytes in internal memory buffers.")),

        sm::make_gauge("blocked_on_new_segment", totals.blocked_on_new_segment,
                       sm::description("Number of allocations blocked on acquiring new segment.")),

        sm::make_gauge("active_allocations", totals.active_allocations,
                       sm::description("Current number of active allocations.")),
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

    // Now get a set of used CF ids:
    std::unordered_set<cf_id_type> ids;

    uint64_t n = size_to_remove;
    uint64_t flushing = 0;

    for (auto& s : _segments) {
        // if a segment is allocating, it should be included in flush request,
        // because we cannot free anything there anyway. If a segment is allocating,
        // it is the last one, so just break.
        if (s->is_still_allocating()) {
            break;
        }

        auto rp = replay_position(s->_desc.id, db::position_type(s->size_on_disk()));
        if (rp <= _flush_position) {
            // already requested.
            continue;
        }

        auto size = s->size_on_disk();
        auto waste = s->_waste;

        flushing += size - waste;

        for (auto& id : s->_cf_dirty | boost::adaptors::map_keys) {
            ids.insert(id);
        }

        if (size_to_remove != 0) {
            if (n <= size) {
                high = rp;
                break;
            }
            n -= s->size_on_disk();
        }
    }

    clogger.debug("Flushing ({} MB) to {}", flushing/(1024*1024), high);

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

    _flush_position = high;
    totals.bytes_flush_requested += flushing;
}

void db::commitlog::segment_manager::check_no_data_older_than_allowed() {
    auto max = cfg.commitlog_data_max_lifetime_in_seconds;
    if (!max) {
        return;
    }

    auto now = gc_clock::now();

    std::unordered_set<cf_id_type> ids;
    std::optional<replay_position> high;

    for (auto& s : _segments) {
        auto rp = replay_position(s->_desc.id, db::position_type(s->size_on_disk()));
        if (rp <= _flush_position) {
            // already requested.
            continue;
        }

        bool any_found = false;

        for (auto [id, low_ts] : s->_cf_min_time) {
            // Ignore CF:s already released.
            if (!s->_cf_dirty.count(id)) {
                continue;
            }

            uint64_t time_since_first_added = std::chrono::duration_cast<std::chrono::seconds>(now - low_ts).count();
            if (time_since_first_added >= *max) {
                // There is data in this segment that has lived longer than allowed (might be flushed actually
                // but can still affect compaction/resurrect stuff on replay). Collect all dirty 
                // id:s (stuff keeping this segment alive), and ask for them to be memtable flushed
                for (auto& id : s->_cf_dirty | boost::adaptors::map_keys) {
                    ids.insert(id);
                }

                // highest position (so far) is end of segment.
                high = rp;

                // If this segment is actually the active one, we must close it. Otherwise flushing 
                // won't help.
                if (s->is_still_allocating()) {
                    // fixme. discarded future.
                    (void)s->close();
                }
                any_found = true;
                break;
            }
        }

        // segments are sequential. Can't start becoming older.
        if (!any_found) {
            break;
        }
    }

    if (!ids.empty()) {
        auto callbacks = boost::copy_range<std::vector<flush_handler>>(_flush_handlers | boost::adaptors::map_values);
        // For each CF id: for each callback c: call c(id, high)
        for (auto& f : callbacks) {
            for (auto& id : ids) {
                try {
                    f(id, *high);
                } catch (...) {
                    clogger.error("Exception during flush request {}/{}: {}", id, *high, std::current_exception());
                }
            }
        }
        _flush_position = *high;
    }
}

future<db::commitlog::segment_manager::sseg_ptr> db::commitlog::segment_manager::allocate_segment_ex(descriptor d, named_file f, open_flags flags) {
    file_open_options opt;
    opt.extent_allocation_size_hint = max_size;
    opt.append_is_unlikely = true;

    size_t align;
    std::exception_ptr ep;

    auto filename = f.name();

    try {
        // when we get here, f.known_size() is either size of recycled segment we open,
        // or zero in case we create brand new segment
        co_await f.open(flags, opt, f.known_size());

        align = f.disk_write_dma_alignment();
        auto is_overwrite = false;
        auto existing_size = f.known_size();

        if ((flags & open_flags::dsync) != open_flags{}) {
            is_overwrite = true;
            // would be super nice if we just could mmap(/dev/zero) and do sendto
            // instead of this, but for now we must do explicit buffer writes.

            // if recycled (or from last run), we might have either truncated smaller or written it 
            // (slightly) larger due to final zeroing of file
            if (existing_size > max_size) {
                co_await f.truncate(max_size);
            } else if (existing_size < max_size) {
                clogger.trace("Pre-writing {} of {} KB to segment {}", (max_size - existing_size)/1024, max_size/1024, filename);

                // re-open without o_dsync for pre-alloc. The reason/rationale
                // being that we want automatic (meta)data sync from O_DSYNC for when
                // we do actual CL flushes, but here it would just result in
                // data being committed before we've reached eof/finished writing.
                // Again an argument for sendfile-like constructs I guess...
                co_await f.close();
                co_await f.open(flags & open_flags(~int(open_flags::dsync)), opt, existing_size);
                co_await f.allocate(existing_size, max_size - existing_size);

                size_t buf_size = align_up<size_t>(16 * 1024, size_t(align));
                size_t zerofill_size = max_size - align_down(existing_size, align);
                auto rem = zerofill_size;

                auto buf = allocate_single_buffer(buf_size, align);
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
                    auto s = co_await f.dma_write(max_size - rem, std::move(v));
                    if (!s) [[unlikely]] {
                        on_internal_error(clogger, format("dma_write returned 0: max_size={} rem={} iovec.n={}", max_size, rem, n));
                    }
                    rem -= s;
                }

                // sync metadata (size/written)
                co_await f.flush();
                co_await f.close();
                co_await f.open(flags, opt, max_size);

                // we will never add blocks (scouts honour). I can haz smaller align?
                align = f.disk_overwrite_dma_alignment();
            }
        } else {
            co_await f.truncate(max_size);
        }

        // #12810 - we did not update total_size_on_disk unless o_dsync was 
        // on. So kept running with total == 0 -> free for all in creating new segment.
        // Always update total_size_on_disk. Will wrap-around iff existing_size > max_size. 
        // That is ok.
        totals.total_size_on_disk += (max_size - existing_size);
        auto known_size = f.known_size();
 
        if (cfg.extensions && !cfg.extensions->commitlog_file_extensions().empty()) {
            for (auto * ext : cfg.extensions->commitlog_file_extensions()) {
                auto nf = co_await ext->wrap_file(filename, f, flags);
                if (nf) {
                    f.assign(std::move(nf), known_size);
                    align = is_overwrite ? f.disk_overwrite_dma_alignment() : f.disk_write_dma_alignment();
                }
            }
        }

        f.assign(make_checked_file(commit_error_handler, std::move(f)), known_size);
    } catch (...) {
        ep = std::current_exception();
    }
    if (ep) {
        // do this early, so iff we are to fast-fail server,
        // we do it before anything else can go wrong.
        try {
            commit_error_handler(ep);
        } catch (...) {
            ep = std::current_exception();
        }
    }
    if (ep && f) {
        co_await f.close();
    }
    if (ep) {
        add_file_to_dispose(std::move(f), dispose_mode::Delete);
        co_return coroutine::exception(std::move(ep));
    }

    co_return make_shared<segment>(shared_from_this(), std::move(d), std::move(f), align);
}

future<db::commitlog::segment_manager::sseg_ptr> db::commitlog::segment_manager::allocate_segment() {
    for (;;) {
        descriptor d(next_id(), cfg.fname_prefix);
        auto dst = filename(d);
        auto flags = open_flags::wo;
        if (cfg.use_o_dsync) {
            flags |= open_flags::dsync;
        }

        if (!_recycled_segments.empty()) {
            auto f = _recycled_segments.pop();
            // Note: we have to do the rename here to ensure
            // proper descriptor id order. If we renamed in the delete call
            // that recycled the file we could potentially have
            // out-of-order files. (Sort does not help).
            clogger.debug("Using recycled segment file {} -> {} ({} MB)", f.name(), dst, f.known_size()/(1024*1024));
            co_await f.rename(dst);
            co_return co_await allocate_segment_ex(std::move(d), std::move(f), flags);
        }

        if (!cfg.allow_going_over_size_limit && max_disk_size != 0 && totals.total_size_on_disk >= max_disk_size) {
            clogger.debug("Disk usage ({} MB) exceeds maximum ({} MB) - allocation will wait...", totals.total_size_on_disk/(1024*1024), max_disk_size/(1024*1024));
            auto f = _recycled_segments.not_empty();
            if (!f.available()) {
                _new_counter = 0; // zero this so timer task does not duplicate the below flush
                flush_segments(0); // force memtable flush already
            }
            try {
                co_await std::move(f);
            } catch (shutdown_marker&) {
                throw;
            } catch (...) {
                clogger.warn("Exception while waiting for segments {}. Will retry allocation...", std::current_exception());
            }
            continue;
        }

        named_file f(dst);
        co_return co_await allocate_segment_ex(std::move(d), std::move(f), flags|open_flags::create);
    }
}

future<db::commitlog::segment_manager::sseg_ptr> db::commitlog::segment_manager::new_segment() {
    gate::holder g(_gate);

    if (_shutdown) {
        co_await coroutine::return_exception(std::runtime_error("Commitlog has been shut down. Cannot add data"));
    }

    ++_new_counter;

    if (_reserve_segments.empty()) {        
        // don't increase reserve count if we are at max, or we would go over disk limit. 
        if (_reserve_segments.max_size() < cfg.max_reserve_segments && (totals.total_size_on_disk + max_size) <= max_disk_size) {
            _reserve_segments.set_max_size(_reserve_segments.max_size() + 1);
            clogger.debug("Increased segment reserve count to {}", _reserve_segments.max_size());
        }
        // if we have no reserve and we're above/at limits, make background task a little more eager.
        auto cur = totals.active_size_on_disk + totals.wasted_size_on_disk;
        if (!_shutdown && cur >= disk_usage_threshold) {
            _timer.cancel();
            _timer.arm(std::chrono::milliseconds(0));
        }
    }

    auto s = co_await _reserve_segments.pop_eventually();
    _segments.push_back(s);
    _segments.back()->reset_sync_time();
    co_return s;
}

future<db::commitlog::segment_manager::sseg_ptr> db::commitlog::segment_manager::active_segment(db::timeout_clock::time_point timeout) {
    // If there is no active segment, try to allocate one using new_segment(). If we time out,
    // make sure later invocations can still pick that segment up once it's ready.
    for (;;) {
        if (!_segments.empty() && _segments.back()->is_still_allocating()) {
            co_return _segments.back();
        }

        scope_increment_counter blocked_on_new(totals.blocked_on_new_segment);

        // #9896 - we don't want to issue a new_segment call until
        // the old one has terminated with either result or exception.
        // Do all waiting through the shared_future
        if (!_segment_allocating) {
            auto f = new_segment();
            // must check that we are not already done.
            if (f.available()) {
                f.get(); // maybe force exception
                continue;
            }
            _segment_allocating.emplace(f.discard_result().finally([this] {
                // clear the shared_future _before_ resolving its contents
                // (i.e. with result of this finally)
                _segment_allocating = std::nullopt;
            }));
        }
        co_await _segment_allocating->get_future(timeout);
    }
}

/**
 * go through all segments, clear id up to pos. if segment becomes clean and unused by this,
 * it is discarded.
 */
void db::commitlog::segment_manager::discard_completed_segments(const cf_id_type& id, const rp_set& used) noexcept {
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

void db::commitlog::segment_manager::discard_completed_segments(const cf_id_type& id) noexcept {
    clogger.debug("Discard all data for {}", id);
    for (auto&s : _segments) {
        s->mark_clean(id);
    }
    discard_unused_segments();
}

future<> db::commitlog::segment_manager::force_new_active_segment() noexcept {
    if (_segments.empty() || !_segments.back()->is_still_allocating()) {
        co_return;
    }

    auto& s = _segments.back();
    if (s->position()) { // check used.
        co_await s->close();
        discard_unused_segments();
    }
}

future<> db::commitlog::segment_manager::wait_for_pending_deletes() noexcept {
    if (_pending_deletes.available()) {
        co_return;
    }
    promise<> p;
    auto f = std::exchange(_pending_deletes, p.get_future());
    co_await std::move(f);
    p.set_value();
}

namespace db {

std::ostream& operator<<(std::ostream& out, const db::commitlog::segment& s) {
    return out << format_as(s);
}

}

auto fmt::formatter<db::replay_position>::format(const db::replay_position& p,
                                                 fmt::format_context& ctx) const -> decltype(ctx.out()) {
    return fmt::format_to(ctx.out(), "{{{}, {}, {}}}", p.shard_id(), p.base_id(), p.pos);
}

template <>
struct fmt::formatter<db::commitlog::segment::cf_mark> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    template <typename FormatContext>
    auto format(const db::commitlog::segment::cf_mark& m, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "{}", fmt::join(m.s._cf_dirty | boost::adaptors::map_keys, ", "));
    }
};

void db::commitlog::segment_manager::discard_unused_segments() noexcept {
    clogger.trace("Checking for unused segments ({} active)", _segments.size());

    std::erase_if(_segments, [=](sseg_ptr s) {
        if (s->can_delete()) {
            clogger.debug("Segment {} is unused", *s);
            return true;
        }
        if (s->is_still_allocating()) {
            clogger.debug("Not safe to delete segment {}; still allocating.", *s);
        } else if (!s->is_clean()) {
            clogger.debug("Not safe to delete segment {}; dirty is {}", *s, segment::cf_mark {*s});
        } else {
            clogger.debug("Not safe to delete segment {}; disk ops pending", *s);
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

    for (auto& [f, mode] : _files_to_dispose) {
        if (mode == dispose_mode::Delete) {
            mode = dispose_mode::ForceDelete;
        }
    }

    _recycled_segments.consume([&](named_file f) {
        _files_to_dispose.emplace_back(std::move(f), dispose_mode::ForceDelete);
        return true;
    });

    return do_pending_deletes();
}

future<> db::commitlog::segment_manager::sync_all_segments() {
    clogger.debug("Issuing sync for all segments");
    // #8952 - calls that do sync/cycle can end up altering
    // _segments (end_flush()->discard_unused())
    auto def_copy = _segments;
    co_await coroutine::parallel_for_each(def_copy, [] (sseg_ptr s) -> future<> {
        co_await s->sync();
        clogger.debug("Synced segment {}", *s);
    });
}

future<> db::commitlog::segment_manager::shutdown_all_segments() {
    clogger.debug("Issuing shutdown for all segments");
    // #8952 - calls that do sync/cycle can end up altering
    // _segments (end_flush()->discard_unused())
    auto def_copy = _segments;
    co_await coroutine::parallel_for_each(def_copy, [] (sseg_ptr s) -> future<> {
        co_await s->shutdown();
        clogger.debug("Shutdown segment {}", *s);
    });
}

future<> db::commitlog::segment_manager::shutdown() {
    if (!_shutdown_promise) {
        _shutdown_promise = shared_promise<>();

        // Wait for all pending requests to finish. Need to sync first because segments that are
        // alive may be holding semaphore permits.
        auto block_new_requests = get_units(_request_controller, max_request_controller_units());
        try {
            co_await sync_all_segments();
        } catch (...) {
            clogger.error("Syncing all segments failed during shutdown: {}. Aborting.", std::current_exception());
            abort();
        }

        std::exception_ptr p;

        try {
            co_await std::move(block_new_requests);

            _timer.cancel(); // no more timer calls
            _shutdown = true; // no re-arm, no create new segments.

            // do a discard + delete sweep to force 
            // gate holder (i.e. replenish) to wake up
            discard_unused_segments();

            auto f = _gate.close();
            co_await do_pending_deletes();
            auto ep = std::make_exception_ptr(shutdown_marker{});
            if (_recycled_segments.empty()) {
                abort_recycled_list(ep);
            }
            auto f2 = std::exchange(_background_sync, make_ready_future<>());

            co_await std::move(f);
            co_await std::move(f2);

            try {
                co_await shutdown_all_segments();
            } catch (...) {
                clogger.error("Shutting down all segments failed during shutdown: {}. Aborting.", std::current_exception());
                abort();
            }
        } catch (...) {
            p = std::current_exception();
        }
            
        discard_unused_segments();

        try {
            co_await clear_reserve_segments();
        } catch (...) {
            p = std::current_exception();
        }
        try {
            co_await std::move(_reserve_replenisher);
        } catch (...) {
            p = std::current_exception();
        }
        // slight functional change from non-coroutine version: we propagate all/any
        // exceptions, not just the replenish one.
        if (p) {
            _shutdown_promise->set_exception(p);
        } else {
            _shutdown_promise->set_value();
        }
    }
    co_await _shutdown_promise->get_shared_future();
    clogger.debug("Commitlog shutdown complete");
}

void db::commitlog::segment_manager::add_file_to_dispose(named_file f, dispose_mode mode) {
    _files_to_dispose.emplace_back(std::move(f), mode);
}

future<> db::commitlog::segment_manager::delete_segments(std::vector<sstring> files) {
    for (auto& s : files) {
        // Note: this is only for replay files. We can decide to
        // recycle these, but they don't count into footprint,
        // thus unopened named_files are what we want (known_size == 0)

        // #11184 - include replay footprint so we make sure to delete any segments
        // pushing us over limits in "delete_segments" (assumed called after replay)
        // #11237 - cannot do this already in "init()" - we need to be able to create
        // segments before replay+delete, and more to the point: shards that _don't_ replay
        // must not add this to their footprint (a.) shared, b.) not there after this call)
        named_file f(s);
        // #16207 - must make sure the named_file we put up for deletion/recycling
        // has its size updated.
        auto size = co_await file_size(s);
        f.maybe_update_size(size);
        totals.total_size_on_disk += size;
        _files_to_dispose.emplace_back(std::move(f), dispose_mode::Delete);
    }
    co_return co_await do_pending_deletes();
}

void db::commitlog::segment_manager::abort_recycled_list(std::exception_ptr ep) {
    // may not call here with elements in list. that would leak files.
    SCYLLA_ASSERT(_recycled_segments.empty());
    _recycled_segments.abort(ep);
    // and ensure next lap(s) still has a queue
    _recycled_segments = queue<named_file>(std::numeric_limits<size_t>::max());
}

template <>
struct fmt::formatter<db::commitlog::segment_manager::named_file> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const db::commitlog::segment_manager::named_file& f, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "{} ({})", f.name(), f.known_size());
    }
};

template <>
struct fmt::formatter<db::commitlog::segment_manager::dispose_mode> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(db::commitlog::segment_manager::dispose_mode mode, fmt::format_context& ctx) const {
        using enum db::commitlog::segment_manager::dispose_mode;
        string_view name;
        switch (mode) {
        case Delete: name = "Delete"; break;
        case ForceDelete: name = "Force Delete"; break;
        case Keep: name = "Keep"; break;
        default: break;
        }
        return fmt::format_to(ctx.out(), "{}", name);
    }
};

template <typename T>
struct fmt::formatter<db::commitlog::segment_manager::byte_flow<T>> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(db::commitlog::segment_manager::byte_flow<T> f, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "[written={}, released={}, flush_req={}]",
                              f.bytes_written, f.bytes_released, f.bytes_flush_requested);
    }
};

using file_to_dispose_t = std::pair<db::commitlog::segment_manager::named_file,
                                    db::commitlog::segment_manager::dispose_mode>;
template <>
struct fmt::formatter<file_to_dispose_t> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const file_to_dispose_t& p, fmt::format_context& ctx) const {
        auto& [file, mode] = p;
        return fmt::format_to(ctx.out(), "{} ({})", file, mode);
    }
};

future<> db::commitlog::segment_manager::do_pending_deletes() {
    auto ftd = std::exchange(_files_to_dispose, {});

    if (ftd.empty()) {
        co_return;
    }

    promise<> deleting_done;
    auto pending_deletes = std::exchange(_pending_deletes, deleting_done.get_future());

    std::exception_ptr recycle_error;
    auto exts = cfg.extensions;

    clogger.debug("Discarding segments {}", ftd);

    for (auto& [f, mode] : ftd) {
        // `f.remove_file()` resets known_size to 0, so remember the size here,
        // in order to subtract it from total_size_on_disk accurately.
        auto size = f.known_size();
        try {
            if (f) {
                co_await f.close();
            }

            // retain the file (replay...)
            if (mode == dispose_mode::Keep) {
                continue;
            }

            if (exts && !exts->commitlog_file_extensions().empty()) {
                for (auto& ext : exts->commitlog_file_extensions()) {
                    co_await ext->before_delete(f.name());
                }
            }

            auto usage = totals.total_size_on_disk;
            auto next_usage = usage - size;

            if (next_usage <= max_disk_size && mode != dispose_mode::ForceDelete) {
                descriptor d(next_id(), "Recycled-" + cfg.fname_prefix);
                auto dst = this->filename(d);

                clogger.debug("Recycling segment file {} -> {}", f.name(), dst);
                // must rename the file since we must ensure the
                // data is not replayed. Changing the name will
                // cause header ID to be invalid in the file -> ignored
                try {
                    co_await f.rename(dst);
                    auto b = _recycled_segments.push(std::move(f));
                    SCYLLA_ASSERT(b); // we set this to max_size_t so...
                    continue;
                } catch (...) {
                    clogger.error("Could not recycle segment {}: {}", f.name(), std::current_exception());
                    recycle_error = std::current_exception();
                    // fallthrough
                }
            }

            clogger.debug("Deleting segment file {}", f.name());
            // last resort.
            co_await f.remove_file();
        } catch (...) {
            clogger.error("Could not delete segment {}: {}", f.name(), std::current_exception());
        }
        // if we get here, we either successfully deleted the file,
        // or had such an exception that we consider the file dead
        // anyway. In either case we _remove_ the file size from
        // footprint, because it is no longer our problem.
        totals.total_size_on_disk -= size;
    }

    // #8376 - if we had an error in recycling (disk rename?), and no elements
    // are available, we could have waiters hoping they will get segments.
    // abort the queue (wakes up any existing waiters - futures), and let them
    // retry. Since we did deletions instead, disk footprint should allow
    // for new allocs at least. Or more likely, everything is broken, but
    // we will at least make more noise.
    if (recycle_error && _recycled_segments.empty()) {
        abort_recycled_list(recycle_error);
    }
    co_await std::move(pending_deletes);
    deleting_done.set_value();
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
    co_await shutdown();
    clogger.debug("Clearing all segments");
    for (auto& s : _segments) {
        s->mark_clean();
    }
    co_await orphan_all();
}
/**
 * Called by timer in periodic mode.
 */
void db::commitlog::segment_manager::sync() {
    auto f = std::exchange(_background_sync, make_ready_future<>());
    // #8952 - calls that do sync/cycle can end up altering
    // _segments (end_flush()->discard_unused())
    auto def_copy = _segments;
    _background_sync = parallel_for_each(def_copy, [](sseg_ptr s) {
        return s->sync().discard_result();
    }).then([f = std::move(f)]() mutable {
        return std::move(f);
    });
}

void db::commitlog::segment_manager::on_timer() {
    // Gate, because we are starting potentially blocking ops
    // without waiting for them, so segement_manager could be shut down
    // while they are running.
    (void)seastar::with_gate(_gate, [this] {
        if (cfg.mode != sync_mode::BATCH) {
            sync();
        }

        byte_flow<uint64_t> curr = totals;
        auto diff = curr - std::exchange(last_bytes, curr);
        auto now = std::chrono::high_resolution_clock::now();
        auto seconds = std::chrono::duration_cast<std::chrono::duration<double>>(now - last_time).count();
        auto rate = diff / seconds;

        // not using yet. but should maybe, adjust for time windows etc.
        // for now, use simple "timer frequency" based diffs (i.e. rate per 10s)
        // to try to predict where disk foot print will be by the next timer call.
        bytes_rate = rate;
        last_time = now;

        clogger.debug("Rate: {} / s ({} s)", rate, seconds);

        // IFF a new segment was put in use since last we checked, and we're
        // above threshold, request flush.
        if (_new_counter > 0) {
            auto max = disk_usage_threshold;
            auto cur = totals.active_size_on_disk + totals.wasted_size_on_disk;
            uint64_t extra = 0;

            // TODO: better heuristics? Do a semi-pessimistic approach, guess that half
            // of flush request will manage to finish by next lap, so count it as half.
            auto returned = diff.bytes_released + diff.bytes_flush_requested/2;
            if (diff.bytes_written > returned) {
                // we are guessing we are gonna add at least this.
                extra = (diff.bytes_written - returned);
            }

            // do not just measure current footprint, but maybe include expected
            // footprint that will be added.
            if (max != 0 && (cur + extra) >= max) {
                clogger.debug("Used size on disk {} MB ({} MB projected) exceeds local threshold {} MB"
                    , (cur) / (1024 * 1024)
                    , (cur+extra) / (1024 * 1024)
                    , max / (1024 * 1024)
                );
                _new_counter = 0;
                flush_segments(cur + extra - max);
            }
        }

        check_no_data_older_than_allowed();

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

temporary_buffer<char> db::commitlog::segment_manager::allocate_single_buffer(size_t s, size_t alignment) {
    return temporary_buffer<char>::aligned(alignment, s);
}

db::commitlog::segment_manager::buffer_type db::commitlog::segment_manager::acquire_buffer(size_t s, size_t alignment) {
    s = align_up(s, segment::default_size);
    auto fragment_count = s / segment::default_size;

    std::vector<temporary_buffer<char>> buffers;
    buffers.reserve(fragment_count);
    while (buffers.size() < fragment_count) {
        buffers.emplace_back(allocate_single_buffer(segment::default_size, alignment));
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
        const cf_id_type& id(size_t) const override { return _id; }
        size_t size(segment&, size_t) override { return _size; }
        size_t size(segment&) override { return _size; }
        size_t size() const override { return _size; }
        void write(segment&, output& out, size_t) const override {
            _func(out);
        }
        void result(size_t, rp_handle h) override {
            res = std::move(h);
        }

        using result_type = db::rp_handle;

        result_type result() {
            return std::move(res);
        }
    };
    return _segment_manager->allocate_when_possible(serializer_func_entry_writer(id, size, std::move(func), sync), timeout);
}

future<db::rp_handle> db::commitlog::add_entry(const cf_id_type& id, const commitlog_entry_writer& cew, timeout_clock::time_point timeout)
{
    SCYLLA_ASSERT(id == cew.schema()->id());

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

        using result_type = db::rp_handle;

        result_type result() {
            return std::move(res);
        }
    };
    return _segment_manager->allocate_when_possible(cl_entry_writer(cew), timeout);
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
            SCYLLA_ASSERT(i == res.size());
            res.emplace_back(std::move(h));
        }

        using result_type = std::vector<db::rp_handle>;

        result_type result() {
            return std::move(res);
        }
    };

    force_sync sync(std::any_of(entry_writers.begin(), entry_writers.end(), [](auto& w) { return bool(w.sync()); }));
    return _segment_manager->allocate_when_possible(cl_entries_writer(sync, std::move(entry_writers)), timeout);
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
    co_await c._segment_manager->init();
    co_return c;
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

future<> db::commitlog::force_new_active_segment() noexcept {
    co_await _segment_manager->force_new_active_segment();
}

future<> db::commitlog::wait_for_pending_deletes() noexcept {
    co_await _segment_manager->wait_for_pending_deletes();
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

uint64_t db::commitlog::max_active_flushes() const {
    return _segment_manager->cfg.max_active_flushes;
}

future<> db::commitlog::clear() {
    return _segment_manager->clear();
}

const db::commitlog::config& db::commitlog::active_config() const {
    return _segment_manager->cfg;
}


db::commitlog::segment_truncation::segment_truncation(uint64_t pos) 
    : _msg(fmt::format("Segment truncation at {}", pos))
    , _pos(pos)
{}

uint64_t db::commitlog::segment_truncation::position() const {
    return _pos;
}

const char* db::commitlog::segment_truncation::what() const noexcept {
    return _msg.c_str();
}

// No commit_io_check needed in the log reader since the database will fail
// on error at startup if required
future<>
db::commitlog::read_log_file(sstring filename, sstring pfx, commit_load_reader_func next, position_type off, const db::extensions* exts) {
    struct work {
    private:
        file_input_stream_options make_file_input_stream_options() {
            file_input_stream_options fo;
            fo.buffer_size = db::commitlog::segment::default_size;
            fo.read_ahead = 10;
            return fo;
        }
    public:
        file f;
        descriptor d;
        commit_load_reader_func func;
        input_stream<char> fin;
        input_stream<char> r;
        uint64_t id = 0;
        size_t pos = 0;
        size_t next = 0;
        size_t start_off = 0;
        size_t file_size = 0;
        size_t corrupt_size = 0;
        size_t alignment = 0;
        bool eof = false;
        bool header = true;
        bool failed = false;
        fragmented_temporary_buffer::reader frag_reader;
        fragmented_temporary_buffer buffer, initial;

        work(file f, descriptor din, commit_load_reader_func fn, position_type o = 0)
                : f(f), d(din), func(std::move(fn)), fin(make_file_input_stream(f, 0, make_file_input_stream_options())), start_off(o) {
        }
        work(work&&) = default;

        bool end_of_file() const {
            return eof;
        }
        bool end_of_chunk() const {
            return eof || next == pos;
        }
        future<> skip_to_chunk(size_t seek_to_pos) {
            clogger.debug("Skip to {} ({}, {})", seek_to_pos, pos, buffer.size_bytes());

            if (seek_to_pos >= file_size) {
                eof = true;
                pos = file_size;
                co_return;
            }

            // Fix the buffer skip to be correct and more resilient.
            if (buffer.size_bytes()) {
                auto dseek_to_pos = filepos_to_datapos(seek_to_pos);
                auto dpos = filepos_to_datapos(pos);
                auto rem = buffer.size_bytes();
                auto n = std::min(dseek_to_pos - dpos, rem);

                buffer.remove_prefix(n);
                advance_pos(n);
            }

            if (pos == seek_to_pos) {
                co_return;
            }
            // must be on page boundary now!
            SCYLLA_ASSERT(align_down(pos, alignment) == pos);

            // this is in full sectors. no need to fiddle with overhead here.
            auto bytes = seek_to_pos - pos;
            auto skip_bytes = align_down(bytes, alignment);

            pos += skip_bytes;
            co_await fin.skip(skip_bytes);
            // Should not be the case - we only ever skip to page boundaries,
            // but it is nice if the code can handle it. If we get here, we
            // want to get into the current page. Read and discard.
            if (bytes > skip_bytes) {
                // must crc check if we read into a sector
                co_await read_data(bytes - skip_bytes);
            }
        }
        void stop() {
            eof = true;
        }
        void fail() {
            failed = true;
            stop();
        }
        future<> read_header() {
            fragmented_temporary_buffer buf = co_await frag_reader.read_exactly(fin, segment::descriptor_header_size);

            if (buf.empty()) {
                eof = true;
                co_return;
            }

            // Will throw if we got eof
            auto in = buf.get_istream();
            auto magic = read<uint32_t>(in);
            auto ver = read<uint32_t>(in);
            auto id = read<uint64_t>(in);
            auto alignment = read<uint32_t>(in);
            auto checksum = read<uint32_t>(in);

            if (magic == 0 && ver == 0 && id == 0 && checksum == 0) {
                // let's assume this was an empty (pre-allocated)
                // file. just skip it.
                co_return stop();
            }
            if (id != d.id) {
                // filename and id in file does not match.
                // assume not valid/recycled.
                stop();
                co_return;
            }

            if (magic != segment::segment_magic) {
                throw invalid_segment_format();
            }
            if (ver != descriptor::current_version) {
                throw std::invalid_argument("Cannot replay old commitlog segments");
            }

            crc32_nbo crc;
            crc.process(ver);
            crc.process<int32_t>(id & 0xffffffff);
            crc.process<int32_t>(id >> 32);
            crc.process<uint32_t>(alignment);

            auto cs = crc.checksum();
            if (cs != checksum) {
                throw header_checksum_error();
            }

            this->id = id;
            this->next = 0;
            this->alignment = alignment;
            this->initial = std::move(buf);
            this->pos = this->initial.size_bytes();
        }

        future<fragmented_temporary_buffer> read_data(size_t size) {
            auto rem = buffer.size_bytes();
            auto buf_vec = std::move(buffer).release();
            auto block_boundry = align_up(pos - initial.size_bytes(), alignment);

            clogger.debug("Read {} bytes of data ({}, {})", size, pos, rem);

            while (rem < size) {
                if (eof) {
                    throw segment_truncation(block_boundry);
                }

                auto block_size = alignment - initial.size_bytes();
                // using a stream is perhaps not 100% effective, but we need to 
                // potentially address data in pages smaller than the current 
                // disk/fs we are reading from can handle (but please no). 
                auto tmp = co_await frag_reader.read_exactly(fin, block_size);

                if (tmp.size_bytes() == 0) {
                    eof = true;
                    throw segment_truncation(block_boundry);
                }

                crc32_nbo crc;
                // crc all but the final crc
                size_t n = block_size - sizeof(uint32_t);

                bool all_zero = true;

                if (!initial.empty()) {
                    for (auto& bv : initial) {
                        crc.process_bytes(bv.get(), bv.size());
                    }
                    initial = {};
                    all_zero = false;
                }

                for (auto& bv : tmp) {
                    auto np = std::min(bv.size(), n);
                    crc.process_bytes(bv.get(), np);
                    all_zero &= std::all_of(bv.get(), bv.get() + bv.size(), [](char c) { return c == 0; });
                    n -= np;
                }

                block_boundry += alignment;

                if (!all_zero) {
                    auto in = tmp.get_istream();
                    in.skip(block_size - detail::sector_overhead_size);

                    auto id = read<uint64_t>(in);
                    auto check = read<uint32_t>(in);
                    auto checksum = crc.checksum();

                    if (check != checksum) {
                        throw segment_data_corruption_error("Data corruption", alignment);
                    }
                    if (id != this->id) {
                        throw segment_truncation(pos + rem);
                    }
                }
                tmp.remove_suffix(detail::sector_overhead_size);

                rem += tmp.size_bytes();

                auto vec2 = std::move(tmp).release();
                for (auto&& v : vec2) {
                    buf_vec.emplace_back(std::move(v));
                }
            }

            decltype(buf_vec) next;

            auto bytes_to_leave = rem - size;

            auto i = next.end();
            while (bytes_to_leave > 0) {
                auto tmp = std::move(buf_vec.back());
                buf_vec.pop_back();
                auto s = tmp.size();
                if (s <= bytes_to_leave) {
                    i = next.emplace(i, std::move(tmp));
                } else {
                    auto diff = s - bytes_to_leave;
                    auto b1 = tmp.share(0, diff);
                    auto b2 = tmp.share(diff, bytes_to_leave);
                    buf_vec.emplace_back(std::move(b1));
                    i = next.emplace(i, std::move(b2));
                }
                bytes_to_leave -= i->size();
            }

            // this is the remaining buffer now.
            buffer = fragmented_temporary_buffer(std::move(next), rem - size);
            // this is the returned result.
            auto res = fragmented_temporary_buffer(std::move(buf_vec), size);

            // #16298 - adjust position here, based on data returned.
            advance_pos(size);

            SCYLLA_ASSERT(((filepos_to_datapos(pos) + buffer.size_bytes()) % (alignment - detail::sector_overhead_size)) == 0);

            co_return res;
        }

        future<> read_chunk() {
            clogger.debug("read_chunk {}", pos);
            auto start = pos;
            auto buf = co_await read_data(segment::segment_overhead_size); 
            auto in = buf.get_istream();
            auto next = read<uint32_t>(in);
            auto checksum = read<uint32_t>(in);

            if (next == 0 && checksum == 0) {
                // in a pre-allocating world, this means eof
                stop();
                co_return;
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
                stop();
                co_return;
            }

            this->next = next;

            if (start_off >= next) {
                co_return co_await skip_to_chunk(next);
            }

            while (!end_of_chunk()) {
                co_await read_entry();
            }
        }

        // adjust an actual file position to "data stream" position, i.e. without overhead.
        size_t filepos_to_datapos(size_t pos) const {
            return pos - (pos / alignment) * detail::sector_overhead_size;
        }

        // adjust "data stream" pos to file pos, i.e. add overhead
        size_t datapos_to_filepos(size_t pos) const {
            return pos + (pos / (alignment - detail::sector_overhead_size)) * detail::sector_overhead_size;
        }

        size_t next_pos(size_t off) const {
            auto data_pos = filepos_to_datapos(pos);
            auto next_data_pos = data_pos + off;
            return datapos_to_filepos(next_data_pos);
        }

        // #16298 - handle adjusted file position update correctly.
        void advance_pos(size_t off) {
            auto old = pos;
            pos = next_pos(off);
            clogger.trace("Pos {} -> {} ({})", old, pos, off);
        }

        future<> read_entry() {
            static constexpr size_t entry_header_size = segment::entry_overhead_size;

            clogger.debug("read_entry {}", pos);

            /**
             * #598 - Must check that data left in chunk is enough to even read an entry.
             * If not, this is small slack space in the chunk end, and we should just go
             * to the next.
             */
            SCYLLA_ASSERT(pos <= next);
            if (next_pos(entry_header_size) >= next) {
                co_await skip_to_chunk(next);
                co_return;
            }

            replay_position rp(id, position_type(pos));

            auto buf = co_await read_data(entry_header_size);
            auto in = buf.get_istream();
            auto size = read<uint32_t>(in);
            auto checksum = read<uint32_t>(in);

            crc32_nbo crc;
            crc.process(size);

            // check for multi-entry
            if (size == segment::multi_entry_size_magic) {
                auto actual_size = checksum;
                auto end = pos + actual_size - entry_header_size - sizeof(uint32_t);

                SCYLLA_ASSERT(end <= next);
                // really small read...
                buf = co_await read_data(sizeof(uint32_t));
                in = buf.get_istream();
                checksum = read<uint32_t>(in);

                crc.process(actual_size);

                // verify header crc.
                if (actual_size < 2 * segment::entry_overhead_size || crc.checksum() != checksum) {
                    auto slack = next - pos;
                    if (size != 0) {
                        clogger.debug("Segment entry at {} has broken header. Skipping to next chunk ({} bytes)", rp, slack);
                        corrupt_size += slack;
                    }
                    co_await skip_to_chunk(next);
                    co_return;
                }

                // now read all sub-entries
                // and send data to subscriber.
                while (pos < end) {
                    co_await read_entry();
                    if (failed) {
                        break;
                    }
                }

                co_return;
            }

            if (size < 2 * sizeof(uint32_t) || checksum != crc.checksum()) {
                auto slack = next - pos;
                if (size != 0) {
                    clogger.debug("Segment entry at {} has broken header. Skipping to next chunk ({} bytes)", rp, slack);
                    corrupt_size += slack;
                }
                // size == 0 -> special scylla case: zero padding due to dma blocks
                co_await skip_to_chunk(next);
                co_return;
            }

            buf = co_await read_data(size - entry_header_size);

            co_await func({std::move(buf), rp});
        }

        future<> read_file() {
            std::exception_ptr p;
            try {
                file_size = co_await f.size();
                co_await read_header();
                while (!end_of_file()) {
                    co_await read_chunk();
                }
                if (corrupt_size > 0) {
                    throw segment_data_corruption_error("Data corruption", corrupt_size);
                }
            } catch (...) {
                p = std::current_exception();
            }
            co_await fin.close();
            if (p) {
                std::rethrow_exception(p);
            }
        }
    };

    auto bare_filename = std::filesystem::path(filename).filename().string();
    if (bare_filename.rfind(pfx, 0) != 0) {
        co_return;
    }

    file f;

    try {
        f = co_await open_file_dma(filename, open_flags::ro);
        if (exts && !exts->commitlog_file_extensions().empty()) {
            for (auto* ext : exts->commitlog_file_extensions()) {
                auto nf = co_await ext->wrap_file(filename, f, open_flags::ro);
                if (nf) {
                    f = std::move(nf);
                }
            }
        }
    } catch (...) {
        commit_error_handler(std::current_exception());
        throw;
    }

    f = make_checked_file(commit_error_handler, std::move(f));

    descriptor d(filename, pfx);
    work w(std::move(f), d, std::move(next), off);

    co_await w.read_file();
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
    return _segment_manager->totals.active_size_on_disk
        + _segment_manager->totals.wasted_size_on_disk
        + _segment_manager->totals.buffer_list_bytes
        ;
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

uint64_t db::commitlog::get_num_blocked_on_new_segment() const {
    return _segment_manager->totals.blocked_on_new_segment;
}

uint64_t db::commitlog::get_num_active_allocations() const {
    return _segment_manager->totals.active_allocations;
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

gc_clock::time_point db::commitlog::min_gc_time(const cf_id_type& id) const {
    return _segment_manager->min_gc_time(id);
}

db::replay_position db::commitlog::min_position() const {
    return _segment_manager->min_position();
}

void db::commitlog::update_max_data_lifetime(std::optional<uint64_t> commitlog_data_max_lifetime_in_seconds) {
    _segment_manager->cfg.commitlog_data_max_lifetime_in_seconds = commitlog_data_max_lifetime_in_seconds;
}


future<std::vector<sstring>> db::commitlog::get_segments_to_replay() const {
    return _segment_manager->get_segments_to_replay();
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
