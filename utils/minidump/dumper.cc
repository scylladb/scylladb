/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "utils/minidump/dumper.hh"
#include "utils/minidump/elf_core.hh"
#include "utils/minidump/registry.hh"
#include "utils/logalloc.hh"
#include "utils/log.hh"
#include "utils/rjson.hh"
#include "release.hh"

#include <seastar/core/alien.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/resource.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/when_all.hh>
#include <seastar/util/defer.hh>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstring>
#include <fstream>
#include <map>
#include <mutex>
#include <sstream>
#include <thread>

#include <dlfcn.h>
#include <fcntl.h>
#include <fnmatch.h>
#include <linux/userfaultfd.h>
#include <poll.h>
#include <pthread.h>
#include <sys/eventfd.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sched.h>
#include <sys/statvfs.h>
#include <sys/syscall.h>
#include <ucontext.h>
#include <unistd.h>

namespace minidump {

#if defined(__x86_64__) && !defined(SEASTAR_DEFAULT_ALLOCATOR)

static logging::logger mdlog("minidump");

namespace {

using namespace std::chrono_literals;
namespace smi = seastar::memory::internal;

constexpr size_t page_size = 4096;
// Unit of copy-on-write tracking. A write to a protected block waits while a
// fault handler copies it, so blocks are small: copying 64KB takes ~10us.
constexpr size_t block_size = 64 << 10;
// Anonymous mappings outside the seastar allocator bigger than this are not
// dumped (they are buffers, not structures worth debugging).
constexpr size_t max_fuzzy_mapping = 256 << 20;
constexpr size_t max_stack_copy = 4 << 20;
constexpr uint32_t nt_scylla = 0x5c11a001;

int64_t now_ns() {
    return std::chrono::steady_clock::now().time_since_epoch() / 1ns;
}

uintptr_t read_fs_base() noexcept {
    uintptr_t v;
    // glibc keeps the TCB self pointer at %fs:0, and it equals fs_base.
    asm volatile("mov %%fs:0, %0" : "=r"(v));
    return v;
}

size_t static_tls_size() {
    using fn_type = void (*)(size_t*, size_t*);
    static fn_type fn = reinterpret_cast<fn_type>(dlsym(RTLD_DEFAULT, "_dl_get_tls_static_info"));
    size_t size = 64 << 10, align = 64;
    if (fn) {
        fn(&size, &align);
    }
    return size;
}

struct map_entry {
    uintptr_t start;
    uintptr_t end;
    std::string perms;
    uint64_t offset;
    std::string path;
};

std::vector<map_entry> read_maps() {
    std::vector<map_entry> out;
    std::ifstream in("/proc/self/maps");
    std::string line;
    while (std::getline(in, line)) {
        map_entry e;
        std::istringstream ls(line);
        std::string range, dev, inode;
        ls >> range >> e.perms >> std::hex >> e.offset >> std::dec >> dev >> inode;
        std::getline(ls, e.path);
        e.path.erase(0, e.path.find_first_not_of(' '));
        auto dash = range.find('-');
        e.start = std::stoull(range.substr(0, dash), nullptr, 16);
        e.end = std::stoull(range.substr(dash + 1), nullptr, 16);
        out.push_back(std::move(e));
    }
    return out;
}

// Whether [start, end) has pages which are not the file's: copied on write
// (like RELRO after relocation), anonymous or swapped out.
bool has_private_pages(uintptr_t start, uintptr_t end) {
    int fd = ::open("/proc/self/pagemap", O_RDONLY | O_CLOEXEC);
    if (fd < 0) {
        return true;
    }
    auto close_fd = seastar::defer([fd] () noexcept { ::close(fd); });
    constexpr uint64_t present = 1ull << 63, swapped = 1ull << 62, file_or_shared = 1ull << 61;
    std::array<uint64_t, 512> ents;
    for (auto p = start; p < end; ) {
        auto n = std::min<size_t>(ents.size(), (end - p) / page_size);
        auto r = pread(fd, ents.data(), n * sizeof(uint64_t), p / page_size * sizeof(uint64_t));
        if (r <= 0) {
            return true;
        }
        for (size_t i = 0; i < size_t(r) / sizeof(uint64_t); ++i) {
            if ((ents[i] & swapped) || ((ents[i] & present) && !(ents[i] & file_or_shared))) {
                return true;
            }
        }
        p += size_t(r) / sizeof(uint64_t) * page_size;
    }
    return false;
}

uint64_t mem_available() {
    std::ifstream in("/proc/meminfo");
    std::string key;
    uint64_t kb;
    std::string unit;
    while (in >> key >> kb >> unit) {
        if (key == "MemAvailable:") {
            return kb << 10;
        }
    }
    return 0;
}

std::string read_file(const char* path) {
    std::ifstream in(path, std::ios::binary);
    return std::string(std::istreambuf_iterator<char>(in), {});
}

// Collected on each shard before the dump starts.
struct shard_info {
    unsigned shard = 0;
    pid_t tid = 0;
    uintptr_t mem_start = 0;
    uintptr_t mem_end = 0;
    smi::page_map pm{};
    uintptr_t stack_lo = 0;
    uintptr_t stack_hi = 0;
    uintptr_t fs_base = 0;
    size_t tls_size = 0;
    size_t max_segments = 0;
    size_t nr_registered = 0;
    cpu_set_t cpus;
};

shard_info collect_shard_info() {
    shard_info i;
    i.shard = seastar::this_shard_id();
    i.tid = gettid();
    auto l = seastar::memory::get_memory_layout();
    i.mem_start = l.start;
    i.mem_end = l.end;
    i.pm = smi::get_page_map();
    pthread_attr_t attr;
    if (pthread_getattr_np(pthread_self(), &attr) == 0) {
        void* addr;
        size_t size;
        if (pthread_attr_getstack(&attr, &addr, &size) == 0) {
            i.stack_lo = reinterpret_cast<uintptr_t>(addr);
            i.stack_hi = i.stack_lo + size;
        }
        pthread_attr_destroy(&attr);
    }
    i.fs_base = read_fs_base();
    i.tls_size = static_tls_size();
    i.max_segments = logalloc::dump_max_segments();
    CPU_ZERO(&i.cpus);
    sched_getaffinity(0, sizeof(i.cpus), &i.cpus);
    for (auto& r : registered_ranges_list()) {
        i.nr_registered += r.ranges().size();
    }
    return i;
}

struct registered_entry {
    std::string_view tag;
    uintptr_t start;
    size_t size;
};

// protected: write-protected, not copied yet; busy: being copied;
// copied: a copy is in the copy buffer and the block is writable;
// done: writable, nothing to dump (or the dump was aborted).
enum : uint8_t { blk_protected, blk_busy, blk_copied, blk_done };

struct category_stats {
    uint64_t bytes = 0;
    bool included = false;
};

enum park_state : int { park_pending, park_parking, park_parked, park_cancelled };

struct shard_ctx {
    shard_info info;

    // Filled by the park task.
    std::atomic<int> state{park_pending};
    elf::thread_state regs;
    std::vector<char> stack_buf;
    uintptr_t stack_copy_start = 0;
    std::vector<char> tls_buf;
    uintptr_t tls_lo = 0;
    std::vector<uint16_t> seg_owner;
    std::vector<logalloc::dump_region_owner> regions;
    logalloc::dump_segment_export lsa;
    std::vector<registered_entry> reg;
    size_t nr_reg = 0;
    bool reg_overflow = false;
    std::vector<uintptr_t> free_heads;
    int64_t park_ns = 0;
    int64_t arm_ns = 0;
    int arm_errno = 0;
    bool arm = false;
    int uffd = -1;

    // Copy state, one entry per block.
    size_t nr_blocks = 0;
    std::unique_ptr<std::atomic<uint8_t>[]> blk_state;
    std::unique_ptr<int32_t[]> blk_slot;
    std::vector<uint8_t> blk_included;
    std::atomic<bool> classified{false};

    // Classification results.
    std::vector<std::pair<uintptr_t, uintptr_t>> runs;      // included [start, end)
    std::vector<std::pair<uintptr_t, uintptr_t>> zeros;     // scrubbed [start, end)
    uint64_t free_objects = 0;
    std::map<std::string, category_stats> categories;
    uint64_t included_bytes = 0;

    // Time this shard's threads spent waiting in write faults.
    std::atomic<uint64_t> stall_ns{0};
    std::atomic<uint64_t> faults{0};
    // Timeline, ns since the dump started.
    int64_t t_parked = 0;
    int64_t t_classify = 0;
    int64_t t_released = 0;
    int64_t t_precopied = 0;
    uint64_t faults_at_precopied = 0;

    bool parked() const { return state.load(std::memory_order_acquire) == park_parked; }
    uintptr_t block_start(size_t b) const { return info.mem_start + b * block_size; }
    uintptr_t block_end(size_t b) const { return std::min(info.mem_end, block_start(b + 1)); }
};

void collect_free_head(uintptr_t head, void* arg) {
    auto& v = *static_cast<std::vector<uintptr_t>*>(arg);
    if (head && v.size() < v.capacity()) {
        v.push_back(head);
    }
}

// Runs on the shard, at a task boundary. Must stay well under a millisecond
// and must not allocate.
void park(shard_ctx& s) noexcept {
    int expected = park_pending;
    if (!s.state.compare_exchange_strong(expected, park_parking)) {
        return; // the dumper gave up on this shard
    }
    auto t0 = now_ns();
    getcontext(&s.regs.context);
    s.regs.tid = gettid();
    s.regs.fs_base = read_fs_base();

    // The live part of the stack. Everything below it is stale and may hold
    // user data, so it is not dumped. If it doesn't fit, the innermost frames
    // are kept, so that gdb can unwind from the saved registers.
    auto sp = reinterpret_cast<uintptr_t>(__builtin_frame_address(0)) - 512;
    sp = std::max(sp, s.info.stack_lo);
    auto stack_size = std::min<size_t>(s.info.stack_hi - sp, s.stack_buf.size());
    s.stack_copy_start = sp;
    std::memcpy(s.stack_buf.data(), reinterpret_cast<const void*>(s.stack_copy_start), stack_size);
    s.stack_buf.resize(stack_size);
    if (!s.tls_buf.empty()) {
        std::memcpy(s.tls_buf.data(), reinterpret_cast<const void*>(s.tls_lo), s.tls_buf.size());
    }

    logalloc::export_segments_for_dump(s.lsa);
    for (auto& r : registered_ranges_list()) {
        for (auto& rg : r.ranges()) {
            if (s.nr_reg < s.reg.size()) {
                s.reg[s.nr_reg++] = registered_entry{r.tag(), rg.start, rg.size};
            } else {
                s.reg_overflow = true;
            }
        }
    }
    smi::for_each_detached_free_list(collect_free_head, &s.free_heads);

    auto t1 = now_ns();
    if (s.arm) {
        uffdio_writeprotect wp{};
        wp.range.start = s.info.mem_start;
        wp.range.len = s.info.mem_end - s.info.mem_start;
        wp.mode = UFFDIO_WRITEPROTECT_MODE_WP;
        if (ioctl(s.uffd, UFFDIO_WRITEPROTECT, &wp) != 0) {
            s.arm_errno = errno;
        }
    }
    auto t2 = now_ns();
    s.arm_ns = t2 - t1;
    s.park_ns = t2 - t0;
    s.state.store(park_parked, std::memory_order_release);
}

int open_uffd(int flags) {
    int fd = syscall(SYS_userfaultfd, flags);
    if (fd < 0 && errno == EPERM) {
        int dev = ::open("/dev/userfaultfd", O_RDWR | O_CLOEXEC);
        if (dev >= 0) {
            fd = ioctl(dev, USERFAULTFD_IOC_NEW, flags);
            ::close(dev);
        } else {
            errno = EPERM;
        }
    }
    return fd;
}

class dumper {
    options _opts;
    // Shared with the park tasks: a task whose shard missed the deadline still
    // runs later, possibly after the dump is over.
    std::vector<std::shared_ptr<shard_ctx>> _shards;
    // For finding a shard on the fault path.
    std::vector<std::pair<uintptr_t, shard_ctx*>> _by_addr;
    std::vector<std::pair<pid_t, shard_ctx*>> _by_tid;
    int _uffd = -1;
    int _stop_fd = -1;
    std::vector<std::thread> _handlers;

    cpu_set_t _cpus;
    // Copies of blocks, made by the fault handlers or by the pre-copy; slots
    // are never reused.
    // Fault handlers copy into a pre-touched area: copying into fresh memory
    // would add a page fault per 4K to every write fault. The pre-copy uses a
    // separate, lazily populated area.
    char* _fault_buffer = nullptr;
    size_t _fault_slots = 0;
    std::atomic<size_t> _next_fault_slot{0};
    char* _buffer = nullptr;
    size_t _nr_slots = 0;
    std::atomic<size_t> _next_slot{0};
    std::atomic<uint64_t> _fault_copy_ns{0};
    std::atomic<uint64_t> _fault_release_ns{0};
    std::atomic<size_t> _parked_count{0};
    std::atomic<size_t> _done_count{0};
    unsigned _nr_workers = 1;
    unsigned _nr_handlers = 0;
    std::atomic<bool> _precopy_full{false};
    std::atomic<uint64_t> _precopied_blocks{0};
    std::atomic<uint64_t> _wasted_copies{0};
    // Per-fault wait times, for percentiles.
    std::unique_ptr<std::atomic<uint32_t>[]> _fault_us;
    static constexpr size_t max_fault_samples = 1 << 20;

    std::atomic<bool> _abort{false};
    std::mutex _abort_mutex;
    std::string _abort_reason;

    std::atomic<uint64_t> _faults{0};
    std::atomic<uint64_t> _fault_ns_total{0};
    std::atomic<uint64_t> _fault_ns_max{0};
    std::atomic<uint64_t> _staged_blocks{0};

    std::map<std::string, category_stats> _categories;
    std::vector<std::string> _notes;

    struct fuzzy_range {
        uintptr_t start;
        uintptr_t end;
        const char* data;   // copy taken at park, or nullptr to read live memory
    };
    std::vector<fuzzy_range> _extra;
    std::vector<map_entry> _maps;
    uint64_t _fuzzy_bytes = 0;

    int64_t _t_start = 0;
    int64_t _t_parked = 0;
    int64_t _t_classified = 0;
    int64_t _t_written = 0;
    uint64_t _included_bytes = 0;
    uint64_t _file_size = 0;
    bool _created = false;
public:
    dumper(options o, std::vector<shard_info> infos) : _opts(std::move(o)) {
        // Helper threads may run on any CPU the process may use, so the
        // scheduler can put them on idle ones: on a reactor CPU they would
        // preempt the reactor. (The kernel drops CPUs outside our cpuset.)
        CPU_ZERO(&_cpus);
        if (!_opts.helper_cpus.empty()) {
            auto cpus = seastar::resource::parse_cpuset(_opts.helper_cpus);
            if (!cpus) {
                throw std::invalid_argument(fmt::format("invalid helper_cpus: {}", _opts.helper_cpus));
            }
            for (auto c : *cpus) {
                if (c < CPU_SETSIZE) {
                    CPU_SET(c, &_cpus);
                }
            }
        } else {
            // Prefer CPUs no shard runs on (e.g. the ones perftune leaves for
            // interrupts); without any, all of them.
            cpu_set_t shards;
            CPU_ZERO(&shards);
            for (auto& i : infos) {
                CPU_OR(&shards, &shards, &i.cpus);
            }
            for (int c = 0; c < CPU_SETSIZE; ++c) {
                if (!CPU_ISSET(c, &shards)) {
                    CPU_SET(c, &_cpus);
                }
            }
            for (int c = sysconf(_SC_NPROCESSORS_CONF); c < CPU_SETSIZE; ++c) {
                CPU_CLR(c, &_cpus);
            }
            if (!CPU_COUNT(&_cpus)) {
                for (int c = 0; c < CPU_SETSIZE; ++c) {
                    CPU_SET(c, &_cpus);
                }
            }
        }
        _fault_us = std::make_unique<std::atomic<uint32_t>[]>(max_fault_samples);
        for (auto& i : infos) {
            auto s = std::make_shared<shard_ctx>();
            s->info = i;
            _by_addr.emplace_back(i.mem_start, s.get());
            _by_tid.emplace_back(i.tid, s.get());
            _shards.push_back(std::move(s));
        }
        std::ranges::sort(_by_addr);
        std::ranges::sort(_by_tid);
    }
    ~dumper() {
        stop_handler();
        if (_uffd >= 0) {
            ::close(_uffd);
        }
        if (_buffer) {
            munmap(_buffer, _nr_slots * block_size);
        }
        if (_fault_buffer) {
            munmap(_fault_buffer, _fault_slots * block_size);
        }
    }

    std::string run();
    bool ok() const noexcept { return !_abort.load(); }
private:
    void set_abort(std::string reason) {
        std::lock_guard g(_abort_mutex);
        if (!_abort.exchange(true)) {
            _abort_reason = std::move(reason);
        }
    }

    std::string abort_reason() {
        std::lock_guard g(_abort_mutex);
        return _abort_reason;
    }

    // Lifts write protection and wakes the writers waiting on it.
    bool release_wp(uintptr_t start, size_t len) {
        uffdio_writeprotect wp{};
        wp.range.start = start;
        wp.range.len = len;
        wp.mode = 0;
        return ioctl(_uffd, UFFDIO_WRITEPROTECT, &wp) == 0;
    }

    void wake(uintptr_t addr) {
        uffdio_range r{addr & ~(page_size - 1), page_size};
        ioctl(_uffd, UFFDIO_WAKE, &r);
    }

    // Slots below _fault_slots are in the fault area, the rest in the pre-copy area.
    char* slot_ptr(int32_t slot) {
        return size_t(slot) < _fault_slots ? _fault_buffer + size_t(slot) * block_size
                : _buffer + (size_t(slot) - _fault_slots) * block_size;
    }

    int32_t alloc_slot(bool for_fault) {
        if (for_fault) {
            auto s = _next_fault_slot.fetch_add(1, std::memory_order_relaxed);
            return s < _fault_slots ? int32_t(s) : -1;
        }
        auto s = _next_slot.fetch_add(1, std::memory_order_relaxed);
        return s < _nr_slots ? int32_t(_fault_slots + s) : -1;
    }

    // Moves the calling thread to the helper CPUs. The dumper thread does it
    // first and checks the result; the other threads use the same mask.
    void set_affinity() {
        pthread_setaffinity_np(pthread_self(), sizeof(_cpus), &_cpus);
    }

    // Applies the helper CPUs to the dumper thread. An explicit helper_cpus
    // must be usable as given: otherwise the helpers would end up competing
    // with the reactors, which is what the option is meant to prevent.
    void apply_helper_cpus() {
        cpu_set_t shards;
        CPU_ZERO(&shards);
        for (auto& sh : _shards) {
            CPU_OR(&shards, &shards, &sh->info.cpus);
        }
        if (!_opts.helper_cpus.empty()) {
            cpu_set_t overlap;
            CPU_AND(&overlap, &_cpus, &shards);
            if (CPU_COUNT(&overlap)) {
                throw std::invalid_argument(fmt::format("helper_cpus {} overlaps CPUs used by shards", _opts.helper_cpus));
            }
        }
        cpu_set_t got;
        CPU_ZERO(&got);
        if (pthread_setaffinity_np(pthread_self(), sizeof(_cpus), &_cpus) != 0
                || pthread_getaffinity_np(pthread_self(), sizeof(got), &got) != 0
                || (!_opts.helper_cpus.empty() && !CPU_EQUAL(&got, &_cpus))) {
            if (!_opts.helper_cpus.empty()) {
                throw std::invalid_argument(fmt::format("helper_cpus {} includes CPUs this process may not use", _opts.helper_cpus));
            }
            // Default mask: no spare CPU is usable, run beside the shards.
            _cpus = shards;
            pthread_setaffinity_np(pthread_self(), sizeof(_cpus), &_cpus);
            _notes.push_back("no CPU outside the shards is usable; helper threads share the shards' CPUs");
        } else {
            // What the kernel actually granted (it drops CPUs outside our cpuset).
            _cpus = got;
        }
    }

    unsigned nr_fault_handlers() const {
        return std::min<unsigned>(std::clamp<unsigned>(_shards.size() / 8, 4, 16), std::max(1, CPU_COUNT(&_cpus)));
    }

    std::string helper_cpu_list() const {
        std::string out;
        for (int c = 0; c < CPU_SETSIZE; ++c) {
            if (CPU_ISSET(c, &_cpus)) {
                out += fmt::format("{}{}", out.empty() ? "" : ",", c);
            }
        }
        return out;
    }

    shard_ctx* shard_of_tid(pid_t tid) {
        auto it = std::ranges::lower_bound(_by_tid, tid, {}, &std::pair<pid_t, shard_ctx*>::first);
        return it != _by_tid.end() && it->first == tid ? it->second : nullptr;
    }

    shard_ctx* shard_of(uintptr_t addr) {
        auto it = std::ranges::upper_bound(_by_addr, addr, {}, &std::pair<uintptr_t, shard_ctx*>::first);
        if (it == _by_addr.begin()) {
            return nullptr;
        }
        auto* s = std::prev(it)->second;
        return addr < s->info.mem_end ? s : nullptr;
    }

    bool user_included(const std::string& category) const;

    void handler_loop();
    void handle_fault(uintptr_t addr, pid_t tid);
    bool precopy(shard_ctx& s, const std::vector<size_t>& order);
    void process_parked();
    void stop_handler();
    void setup_uffd();
    void park_all();
    void release_all();

    bool snap_read(shard_ctx& s, uintptr_t addr, void* dst, size_t len);
    void classify(shard_ctx& s);
    bool rule_included(const std::string& category) const;
    std::string lsa_category(const shard_ctx& s, uint16_t owner) const;
    void release_excluded_lsa(shard_ctx& s);
    void collect_fuzzy();
    std::string write_file();
    void copy_block(shard_ctx& s, size_t b, char* bounce);
    std::string report(bool written);
    rjson::value note_json();
};

void dumper::setup_uffd() {
    int probe = open_uffd(O_CLOEXEC | O_NONBLOCK);
    if (probe < 0) {
        throw std::runtime_error(fmt::format("userfaultfd unavailable: {}; grant /dev/userfaultfd access or set vm.unprivileged_userfaultfd=1", strerror(errno)));
    }
    uffdio_api api{};
    api.api = UFFD_API;
    if (ioctl(probe, UFFDIO_API, &api) != 0) {
        ::close(probe);
        throw std::runtime_error(fmt::format("UFFDIO_API failed: {}", strerror(errno)));
    }
    ::close(probe);
    // No UFFD_FEATURE_WP_UNPOPULATED: protecting never-touched memory means
    // installing a marker per 4K page, which makes arming a mostly empty shard
    // take milliseconds. Such pages read as zeros at the snapshot; if one is
    // written before it is copied, the dump shows the newer content.
    auto wanted = api.features & (UFFD_FEATURE_WP_HUGETLBFS_SHMEM | UFFD_FEATURE_THREAD_ID);

    _uffd = open_uffd(O_CLOEXEC | O_NONBLOCK);
    if (_uffd < 0) {
        throw std::runtime_error(fmt::format("userfaultfd: {}", strerror(errno)));
    }
    api = uffdio_api{};
    api.api = UFFD_API;
    api.features = wanted;
    if (ioctl(_uffd, UFFDIO_API, &api) != 0) {
        throw std::runtime_error(fmt::format("UFFDIO_API failed: {}", strerror(errno)));
    }
    for (auto& s : _shards) {
        uffdio_register reg{};
        reg.range.start = s->info.mem_start;
        reg.range.len = s->info.mem_end - s->info.mem_start;
        reg.mode = UFFDIO_REGISTER_MODE_WP;
        if (ioctl(_uffd, UFFDIO_REGISTER, &reg) != 0) {
            throw std::runtime_error(fmt::format("UFFDIO_REGISTER of shard {} failed: {}", s->info.shard, strerror(errno)));
        }
        if (!(reg.ioctls & (uint64_t(1) << _UFFDIO_WRITEPROTECT))) {
            throw std::runtime_error("userfaultfd write-protection is not supported for seastar memory");
        }
        s->uffd = _uffd;
        s->arm = true;
    }

    // The copy buffer takes the whole dump if memory allows, so that all of
    // it can be copied at memory speed right after classification and
    // unprotected; writing the file then doesn't keep anything protected.
    auto buffer_bytes = _opts.copy_buffer_bytes ? _opts.copy_buffer_bytes
            : std::min<uint64_t>(8ull << 30, mem_available() / 4);
    _nr_slots = std::max<size_t>(buffer_bytes / block_size, 16);
    auto p = mmap(nullptr, _nr_slots * block_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
    if (p == MAP_FAILED) {
        throw std::runtime_error("cannot map the copy buffer");
    }
    _buffer = static_cast<char*>(p);
    madvise(_buffer, _nr_slots * block_size, MADV_HUGEPAGE);
    _fault_slots = std::max<size_t>(_opts.staging_bytes / block_size, 16);
    p = mmap(nullptr, _fault_slots * block_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE, -1, 0);
    if (p == MAP_FAILED) {
        throw std::runtime_error("cannot map the fault copy area");
    }
    _fault_buffer = static_cast<char*>(p);
    _stop_fd = eventfd(0, EFD_CLOEXEC);
    _nr_handlers = nr_fault_handlers();
    for (unsigned i = 0; i < _nr_handlers; ++i) {
        _handlers.emplace_back([this] {
            // A faulting shard sleeps until a handler runs, so its CPU is
            // idle; any CPU will do, the scheduler picks an idle one.
            set_affinity();
            handler_loop();
        });
    }
}

void dumper::handler_loop() {
    pollfd fds[2] = {{_uffd, POLLIN, 0}, {_stop_fd, POLLIN, 0}};
    uffd_msg msgs[64];
    for (;;) {
        if (poll(fds, 2, -1) < 0) {
            continue;
        }
        if (fds[1].revents) {
            return;
        }
        auto n = read(_uffd, msgs, sizeof(msgs));
        if (n <= 0) {
            continue;
        }
        for (size_t i = 0; i < size_t(n) / sizeof(uffd_msg); ++i) {
            if (msgs[i].event == UFFD_EVENT_PAGEFAULT) {
                handle_fault(msgs[i].arg.pagefault.address, msgs[i].arg.pagefault.feat.ptid);
            }
        }
    }
}

void dumper::handle_fault(uintptr_t addr, pid_t tid) {
    auto t0 = now_ns();
    auto* s = shard_of(addr);
    if (!s) {
        wake(addr);
        return;
    }
    auto b = (addr - s->info.mem_start) / block_size;
    auto& st = s->blk_state[b];
    uint8_t cur = st.load(std::memory_order_acquire);
    if (cur == blk_protected && st.compare_exchange_strong(cur, blk_busy)) {
        bool copy = !_abort.load() && !(s->classified.load(std::memory_order_acquire) && !s->blk_included[b]);
        if (copy) {
            auto slot = alloc_slot(true);
            if (slot < 0) {
                // The fault area is full: use the pre-copy area, whose pages
                // may not be touched yet.
                slot = alloc_slot(false);
            }
            if (slot < 0) {
                set_abort("copy buffer full");
                copy = false;
            } else {
                std::memcpy(slot_ptr(slot), reinterpret_cast<const void*>(s->block_start(b)), s->block_end(b) - s->block_start(b));
                s->blk_slot[b] = slot;
                ++_staged_blocks;
            }
        }
        auto t1 = now_ns();
        st.store(copy ? blk_copied : blk_done, std::memory_order_release);
        release_wp(s->block_start(b), s->block_end(b) - s->block_start(b));
        _fault_copy_ns += t1 - t0;
        _fault_release_ns += now_ns() - t1;
    } else if (cur == blk_copied || cur == blk_done) {
        // Usually a write which raced with the release. If the release
        // failed, the block is still protected: release it again, or the
        // writer would fault forever.
        if (!release_wp(s->block_start(b), s->block_end(b) - s->block_start(b))) {
            wake(addr);
        }
    } else {
        // blk_busy: whoever copies the block releases it, which wakes the writer.
        return;
    }
    auto d = now_ns() - t0;
    auto n = _faults++;
    if (n < max_fault_samples) {
        _fault_us[n].store(std::min<int64_t>(d / 1000, std::numeric_limits<uint32_t>::max()), std::memory_order_relaxed);
    }
    _fault_ns_total += d;
    auto m = _fault_ns_max.load();
    while (uint64_t(d) > m && !_fault_ns_max.compare_exchange_weak(m, d)) {
    }
    auto* waiter = shard_of_tid(tid);
    if (!waiter) {
        waiter = s;
    }
    auto total = waiter->stall_ns += d;
    ++waiter->faults;
    if (uint64_t(d) > uint64_t(_opts.max_fault_us) * 1000) {
        set_abort(fmt::format("a write fault waited {} us", d / 1000));
    } else if (total > uint64_t(_opts.max_stall_us) * 1000) {
        set_abort(fmt::format("shard {} waited {} us in write faults", waiter->info.shard, total / 1000));
    }
}

// Copies every dumped block of the shard to the copy buffer as fast as
// possible and lifts its protection, so that the shard stops faulting long
// before the (slower) file write finishes. Blocks which don't fit stay
// protected and are copied while writing. Returns false when the buffer is full.
bool dumper::precopy(shard_ctx& s, const std::vector<size_t>& order) {
    for (auto b : order) {
        if (_abort.load()) {
            break;
        }
        auto& st = s.blk_state[b];
        if (st.load(std::memory_order_acquire) != blk_protected) {
            continue;
        }
        auto slot = alloc_slot(false);
        if (slot < 0) {
            return false;
        }
        uint8_t cur = blk_protected;
        if (!st.compare_exchange_strong(cur, blk_busy)) {
            continue; // a fault handler got there first; the slot is wasted
        }
        std::memcpy(slot_ptr(slot), reinterpret_cast<const void*>(s.block_start(b)), s.block_end(b) - s.block_start(b));
        s.blk_slot[b] = slot;
        st.store(blk_copied, std::memory_order_release);
        release_wp(s.block_start(b), s.block_end(b) - s.block_start(b));
        ++_precopied_blocks;
    }
    return true;
}

// Classifies and pre-copies shards as soon as they are parked, so that each
// shard is protected only for as long as its own processing takes.
void dumper::process_parked() {
    std::atomic<size_t> next{0};
    std::exception_ptr ex;
    std::mutex ex_mutex;
    std::vector<std::thread> workers;
    // Few workers, at the lowest priority: the fault handlers, which shards
    // wait on, must never wait for a CPU behind them.
    _nr_workers = std::min<unsigned>(_shards.size(), std::clamp(CPU_COUNT(&_cpus) / 3, 1, 8));
    for (unsigned w = 0; w < _nr_workers; ++w) {
        workers.emplace_back([&] {
            set_affinity();
            setpriority(PRIO_PROCESS, gettid(), 19);
            for (;;) {
                auto i = next.fetch_add(1);
                if (i >= _shards.size()) {
                    return;
                }
                while (_parked_count.load(std::memory_order_acquire) <= i) {
                    std::this_thread::sleep_for(50us);
                }
                auto& s = *_shards[i];
                auto done = seastar::defer([this] () noexcept { _done_count.fetch_add(1, std::memory_order_release); });
                if (!s.parked() || _abort.load()) {
                    continue;
                }
                try {
                    classify(s);
                } catch (...) {
                    std::lock_guard g(ex_mutex);
                    ex = std::current_exception();
                    set_abort(fmt::format("{}", ex));
                }
            }
        });
    }
    try {
        park_all();
    } catch (...) {
        set_abort(fmt::format("{}", std::current_exception()));
        // Let the workers see every shard as handled, and wait for them.
        _parked_count.store(_shards.size(), std::memory_order_release);
        for (auto& t : workers) {
            t.join();
        }
        throw;
    }
    _t_parked = now_ns();
    for (auto& t : workers) {
        t.join();
    }
    if (ex) {
        std::rethrow_exception(ex);
    }
    if (_precopy_full.load()) {
        _notes.push_back("the copy buffer was too small to hold the whole dump; the rest was copied while writing");
    }
}

void dumper::stop_handler() {
    if (!_handlers.empty()) {
        uint64_t one = 1;
        (void)!write(_stop_fd, &one, sizeof(one));
        for (auto& t : _handlers) {
            t.join();
        }
        _handlers.clear();
    }
    if (_stop_fd >= 0) {
        ::close(_stop_fd);
        _stop_fd = -1;
    }
}

void dumper::release_all() {
    for (auto& s : _shards) {
        if (!s->parked() || !s->arm || !s->blk_state) {
            continue;
        }
        for (size_t b = 0; b < s->nr_blocks; ++b) {
            uint8_t cur = blk_protected;
            s->blk_state[b].compare_exchange_strong(cur, blk_done);
        }
        release_wp(s->info.mem_start, s->info.mem_end - s->info.mem_start);
    }
}

void dumper::park_all() {
    auto& alien = *seastar::alien::internal::default_instance;
    for (auto& sp : _shards) {
        auto& s = *sp;
        s.stack_buf.resize(std::min(max_stack_copy, s.info.stack_hi - s.info.stack_lo));
        // Static TLS sits right below the thread pointer; the TCB starts at it.
        s.tls_lo = s.info.fs_base - s.info.tls_size;
        uintptr_t tls_hi = s.info.fs_base + page_size;
        for (auto& m : _maps) {
            if (s.info.fs_base >= m.start && s.info.fs_base < m.end) {
                s.tls_lo = std::max(s.tls_lo, m.start);
                tls_hi = std::min(tls_hi, m.end);
            }
        }
        s.tls_buf.resize(tls_hi - s.tls_lo);
        s.seg_owner.resize(s.info.max_segments);
        s.regions.resize(4096);
        s.lsa.segment_owner = s.seg_owner;
        s.lsa.regions = s.regions;
        s.reg.resize(s.info.nr_registered + s.info.nr_registered / 4 + 1024);
        s.free_heads.reserve(1024);
        s.nr_blocks = (s.info.mem_end - s.info.mem_start + block_size - 1) / block_size;
        s.blk_state = std::make_unique<std::atomic<uint8_t>[]>(s.nr_blocks);
        s.blk_slot = std::make_unique<int32_t[]>(s.nr_blocks);
        s.blk_included.assign(s.nr_blocks, 0);
    }
    // One shard at a time, so that arming (and the TLB shootdowns it causes)
    // doesn't contend with other shards' arming.
    for (auto& sp : _shards) {
        auto& s = *sp;
        // A parked shard faults until a worker has copied its memory, so park
        // a shard only when a worker is free to take it right away. This
        // bounds every shard's protected time by its own processing time,
        // however many shards there are.
        while (_parked_count.load(std::memory_order_acquire) - _done_count.load(std::memory_order_acquire) >= _nr_workers) {
            std::this_thread::sleep_for(50us);
        }
        if (_abort.load()) {
            // Protect no more shards; the workers skip the unparked ones.
            _parked_count.store(_shards.size(), std::memory_order_release);
            break;
        }
        auto count = seastar::defer([this, &s] () noexcept {
            s.t_parked = now_ns() - _t_start;
            _parked_count.fetch_add(1, std::memory_order_release);
        });
        seastar::alien::run_on(alien, s.info.shard, [sp] () noexcept { park(*sp); });
        auto deadline = std::chrono::steady_clock::now() + 1s;
        while (s.state.load(std::memory_order_acquire) != park_parked) {
            if (std::chrono::steady_clock::now() > deadline) {
                int expected = park_pending;
                if (s.state.compare_exchange_strong(expected, park_cancelled)) {
                    _notes.push_back(fmt::format("shard {} did not reach a task boundary within 1s; its heap is not dumped", s.info.shard));
                    break;
                }
            }
            std::this_thread::sleep_for(50us);
        }
        if (!s.parked()) {
            continue;
        }
        if (s.arm_errno) {
            set_abort(fmt::format("arming shard {} failed: {}", s.info.shard, strerror(s.arm_errno)));
        }
        if (s.park_ns > int64_t(_opts.max_park_us) * 1000) {
            set_abort(fmt::format("parking shard {} took {} us", s.info.shard, s.park_ns / 1000));
        }
        if (s.reg_overflow) {
            set_abort(fmt::format("too many registered ranges on shard {}", s.info.shard));
        }
        if (s.lsa.regions_full) {
            _notes.push_back(fmt::format("shard {} has more than {} LSA regions; the rest are lsa:unknown", s.info.shard, s.regions.size()));
        }
    }
}

// Reads memory as it was when the shard was parked.
bool dumper::snap_read(shard_ctx& s, uintptr_t addr, void* dst, size_t len) {
    if (addr < s.info.mem_start || addr + len > s.info.mem_end || addr + len < addr) {
        return false;
    }
    auto out = static_cast<char*>(dst);
    while (len) {
        auto b = (addr - s.info.mem_start) / block_size;
        auto n = std::min<size_t>(len, s.block_end(b) - addr);
        if (!s.arm) {
            std::memcpy(out, reinterpret_cast<const void*>(addr), n);
        } else {
            for (;;) {
                auto st = s.blk_state[b].load(std::memory_order_acquire);
                if (st == blk_copied) {
                    std::memcpy(out, slot_ptr(s.blk_slot[b]) + (addr - s.block_start(b)), n);
                    break;
                }
                if (st == blk_done) {
                    return false;
                }
                std::memcpy(out, reinterpret_cast<const void*>(addr), n);
                std::atomic_thread_fence(std::memory_order_acquire);
                auto st2 = s.blk_state[b].load(std::memory_order_acquire);
                if (st2 == blk_protected || st2 == blk_busy) {
                    break;
                }
            }
        }
        addr += n;
        out += n;
        len -= n;
    }
    return true;
}

bool dumper::user_included(const std::string& category) const {
    for (auto& r : _opts.include) {
        if (fnmatch(r.c_str(), category.c_str(), 0) == 0) {
            return true;
        }
    }
    return false;
}

bool dumper::rule_included(const std::string& category) const {
    // User includes, user excludes, then the defaults.
    if (user_included(category)) {
        return true;
    }
    for (auto& r : _opts.exclude) {
        if (fnmatch(r.c_str(), category.c_str(), 0) == 0) {
            return false;
        }
    }
    for (auto r : {"lsa:*", "tag:*", "free"}) {
        if (fnmatch(r, category.c_str(), 0) == 0) {
            return false;
        }
    }
    return true;
}

std::string dumper::lsa_category(const shard_ctx& s, uint16_t v) const {
    if (v == logalloc::dump_segment_lsa_free) {
        return "lsa:free";
    }
    if (v == logalloc::dump_segment_lsa_unknown_region) {
        return "lsa:unknown";
    }
    auto& r = s.regions[v - logalloc::dump_segment_first_region];
    auto name = fmt::format("lsa:{}", r.owner.empty() ? std::string_view("unlabelled") : r.owner);
    if (r.name[0]) {
        name += fmt::format(":{}", r.name);
    }
    return name;
}

// LSA segments are interleaved with everything else in the blocks, and they
// are written to all the time (memtables, cache). Lift the protection from
// the ones which are not dumped right after parking, from the segment map
// taken at park, so writes to them don't make the fault handler copy blocks.
void dumper::release_excluded_lsa(shard_ctx& s) {
    std::vector<int8_t> decision(std::numeric_limits<uint16_t>::max() + 1, -1);
    uintptr_t run_start = 0, run_end = 0;
    auto flush = [&] {
        if (run_end > run_start) {
            release_wp(run_start, run_end - run_start);
        }
        run_start = run_end = 0;
    };
    for (size_t idx = 0; idx < s.lsa.nr_segments; ++idx) {
        auto v = s.seg_owner[idx];
        if (v == logalloc::dump_segment_not_lsa) {
            continue;
        }
        if (decision[v] < 0) {
            decision[v] = rule_included(lsa_category(s, v));
        }
        auto start = std::max(s.lsa.segments_base + idx * logalloc::segment_size, s.info.mem_start);
        auto end = std::min(start + logalloc::segment_size, s.info.mem_end);
        if (decision[v] || start >= end) {
            continue;
        }
        if (start != run_end) {
            flush();
            run_start = start;
        }
        run_end = end;
    }
    flush();
}

void dumper::classify(shard_ctx& s) {
    s.t_classify = now_ns() - _t_start;
    if (s.arm) {
        release_excluded_lsa(s);
    }
    auto& pm = s.info.pm;
    const auto nr = pm.nr_pages;
    const auto mem = pm.memory;
    std::vector<char> pages(nr * pm.descriptor_size);
    if (!snap_read(s, pm.descriptors, pages.data(), pages.size())) {
        throw std::runtime_error(fmt::format("cannot read the page map of shard {}", s.info.shard));
    }
    auto desc = [&] (size_t idx) { return smi::decode_page_descriptor(pages.data() + idx * pm.descriptor_size); };

    std::vector<std::string> cat_names;
    std::map<std::string, uint16_t> cat_ids;
    auto cat = [&] (std::string name) -> uint16_t {
        auto [it, inserted] = cat_ids.emplace(name, cat_names.size());
        if (inserted) {
            cat_names.push_back(std::move(name));
        }
        return it->second;
    };
    const auto c_free = cat("free");
    const auto c_small = cat("small");
    const auto c_large = cat("large");

    struct small_span {
        size_t first;
        size_t n;
        unsigned object_size;
        uintptr_t freelist;
    };
    std::vector<small_span> small_spans;
    std::vector<uint16_t> cats(nr, c_large);
    for (size_t idx = 0; idx < nr;) {
        auto d = desc(idx);
        size_t n = std::clamp<size_t>(d.span_size, 1, nr - idx);
        uint16_t c = c_large;
        if (d.free) {
            c = c_free;
        } else if (d.small_object_size) {
            c = c_small;
            small_spans.push_back(small_span{idx, n, d.small_object_size, d.freelist});
        }
        std::fill_n(cats.begin() + idx, n, c);
        idx += n;
    }

    // LSA segments.
    for (size_t idx = 0; idx < s.lsa.nr_segments; ++idx) {
        auto v = s.seg_owner[idx];
        if (v == logalloc::dump_segment_not_lsa) {
            continue;
        }
        auto c = cat(lsa_category(s, v));
        auto start = s.lsa.segments_base + idx * logalloc::segment_size;
        if (start < mem || start + logalloc::segment_size > mem + nr * page_size) {
            continue;
        }
        std::fill_n(cats.begin() + (start - mem) / page_size, logalloc::segment_size / page_size, c);
    }

    // Free pages are what gets allocated next, so they are written to soon;
    // lift their protection before the slower steps below, unless dumped.
    if (s.arm && !rule_included("free")) {
        for (size_t idx = 0; idx < nr;) {
            if (cats[idx] != c_free) {
                ++idx;
                continue;
            }
            size_t j = idx;
            while (j < nr && cats[j] == c_free) {
                ++j;
            }
            release_wp(mem + idx * page_size, (j - idx) * page_size);
            idx = j;
        }
    }

    // Registered ranges: whole pages are dropped, partial pages are zeroed.
    for (size_t i = 0; i < s.nr_reg; ++i) {
        auto& r = s.reg[i];
        if (r.start < mem || r.start + r.size > mem + nr * page_size || !r.size) {
            continue;
        }
        auto c = cat(fmt::format("tag:{}", r.tag));
        auto first = (r.start + page_size - 1) / page_size * page_size;
        auto last = (r.start + r.size) / page_size * page_size;
        if (first < last) {
            std::fill(cats.begin() + (first - mem) / page_size, cats.begin() + (last - mem) / page_size, c);
            if (r.start < first) {
                s.zeros.emplace_back(r.start, first);
            }
            if (last < r.start + r.size) {
                s.zeros.emplace_back(last, r.start + r.size);
            }
        } else {
            s.zeros.emplace_back(r.start, r.start + r.size);
        }
    }

    std::vector<uint8_t> included(cat_names.size());
    for (size_t i = 0; i < cat_names.size(); ++i) {
        included[i] = rule_included(cat_names[i]);
    }


    // Included runs and per-category totals.
    for (size_t idx = 0; idx < nr;) {
        auto c = cats[idx];
        size_t j = idx;
        while (j < nr && cats[j] == c) {
            ++j;
        }
        auto bytes = (j - idx) * page_size;
        auto& cs = s.categories[cat_names[c]];
        cs.bytes += bytes;
        cs.included = included[c];
        if (included[c]) {
            auto start = mem + idx * page_size;
            auto end = mem + j * page_size;
            if (!s.runs.empty() && s.runs.back().second == start) {
                s.runs.back().second = end;
            } else {
                s.runs.emplace_back(start, end);
            }
            s.included_bytes += bytes;
            for (auto b = (start - s.info.mem_start) / block_size; b <= (end - 1 - s.info.mem_start) / block_size; ++b) {
                s.blk_included[b] = 1;
            }
        }
        idx = j;
    }
    s.classified.store(true, std::memory_order_release);

    // Writes to memory which is not dumped need no copy; lift the protection
    // from it, page-exact (this splits the huge pages it cuts through).
    if (s.arm) {
        for (size_t idx = 0; idx < nr;) {
            size_t j = idx;
            while (j < nr && !included[cats[j]]) {
                ++j;
            }
            if (j > idx) {
                release_wp(mem + idx * page_size, (j - idx) * page_size);
                idx = j;
            } else {
                ++idx;
            }
        }
    }

    // Blocks with nothing to copy are unprotected by now.
    if (s.arm) {
        for (size_t b = 0; b < s.nr_blocks; ++b) {
            if (s.blk_included[b]) {
                continue;
            }
            uint8_t cur = blk_protected;
            if (!s.blk_state[b].compare_exchange_strong(cur, blk_done) && cur == blk_copied) {
                // Copied by a fault handler before classification; not needed.
                s.blk_state[b].store(blk_done, std::memory_order_release);
                ++_wasted_copies;
            }
        }
    }

    s.t_released = now_ns() - _t_start;
    // Copy what is dumped right away, hottest first (the page map, then
    // small-object spans), so the shard stops faulting; the slower steps
    // below then read the copies.
    if (s.arm && !_precopy_full.load()) {
        std::vector<size_t> order;
        std::vector<uint8_t> queued(s.nr_blocks);
        auto queue = [&] (uintptr_t start, uintptr_t end) {
            for (auto b = (start - s.info.mem_start) / block_size; b <= (end - 1 - s.info.mem_start) / block_size && b < s.nr_blocks; ++b) {
                if (s.blk_included[b] && !queued[b]) {
                    queued[b] = 1;
                    order.push_back(b);
                }
            }
        };
        queue(pm.descriptors, pm.descriptors + nr * pm.descriptor_size);
        for (auto& sp : small_spans) {
            queue(mem + sp.first * page_size, mem + (sp.first + sp.n) * page_size);
        }
        for (size_t b = 0; b < s.nr_blocks; ++b) {
            queue(s.block_start(b), s.block_end(b));
        }
        if (!precopy(s, order)) {
            _precopy_full = true;
        }
    }
    s.t_precopied = now_ns() - _t_start;
    s.faults_at_precopied = s.faults.load();

    // Free objects and the unused tails of small-object spans keep whatever
    // was there before; zero them.
    auto page_included = [&] (uintptr_t addr) {
        return addr >= mem && addr < mem + nr * page_size && included[cats[(addr - mem) / page_size]];
    };
    const size_t walk_limit = nr * page_size / 16;
    auto walk = [&] (uintptr_t p) {
        for (size_t steps = 0; p && steps < walk_limit; ++steps) {
            if (p < mem || p >= mem + nr * page_size) {
                break;
            }
            auto d = desc((p - mem) / page_size);
            size_t size = d.small_object_size;
            if (!size) {
                size = size_t(d.span_size) * page_size;
            }
            if (size > sizeof(uintptr_t) && page_included(p)) {
                s.zeros.emplace_back(p + sizeof(uintptr_t), p + size);
                ++s.free_objects;
            }
            uintptr_t next;
            if (!snap_read(s, p, &next, sizeof(next))) {
                break;
            }
            p = next;
        }
    };
    for (auto& sp : small_spans) {
        auto start = mem + sp.first * page_size;
        if (!page_included(start)) {
            continue;
        }
        if (s.arm) {
            walk(sp.freelist);
        }
        auto bytes = sp.n * page_size;
        auto used = bytes / sp.object_size * sp.object_size;
        if (used < bytes) {
            s.zeros.emplace_back(start + used, start + bytes);
        }
    }
    for (auto h : s.free_heads) {
        if (s.arm) {
            walk(h);
        }
    }
    std::sort(s.zeros.begin(), s.zeros.end());
    // Merge overlaps, so that the range covering an address is the last one
    // starting at or before it.
    size_t nz = 0;
    for (auto& z : s.zeros) {
        if (nz && z.first <= s.zeros[nz - 1].second) {
            s.zeros[nz - 1].second = std::max(s.zeros[nz - 1].second, z.second);
        } else {
            s.zeros[nz++] = z;
        }
    }
    s.zeros.resize(nz);
}

// Memory outside the seastar allocator: the binary's and libraries' data,
// TLS, other threads' mappings. It is copied while the process runs (it
// changes rarely), except the parked shards' stacks and TLS, which come from
// the copies taken at park.
void dumper::collect_fuzzy() {
    auto arena_start = _shards.front()->info.pm.arena_start;
    auto arena_end = _shards.front()->info.pm.arena_end;
    std::unique_ptr<char> sentinel(new char);
    auto own = reinterpret_cast<uintptr_t>(sentinel.get());

    std::vector<fuzzy_range> parked;
    for (auto& s : _shards) {
        if (!s->parked()) {
            continue;
        }
        parked.push_back({s->stack_copy_start, s->stack_copy_start + s->stack_buf.size(), s->stack_buf.data()});
        if (!s->tls_buf.empty()) {
            parked.push_back({s->tls_lo, s->tls_lo + s->tls_buf.size(), s->tls_buf.data()});
        }
    }
    std::sort(parked.begin(), parked.end(), [] (auto& a, auto& b) { return a.start < b.start; });

    for (auto& m : _maps) {
        // Private, readable memory which differs from any file behind it:
        // writable mappings, and read-only ones with private copies (like
        // RELRO, which holds .dynamic and .got). This is what the kernel dumps.
        // The vDSO too: gdb reads its link map entry and symbols from memory.
        bool vdso = m.path == "[vdso]";
        if (m.perms.size() < 4 || m.perms[0] != 'r' || m.perms[3] != 'p' || (m.perms[2] == 'x' && !vdso)) {
            continue;
        }
        if (m.perms[1] != 'w' && !vdso && !has_private_pages(m.start, m.end)) {
            continue;
        }
        if (m.end > arena_start && m.start < arena_end) {
            continue;
        }
        if (m.path == "[vvar]" || m.path == "[vsyscall]" || m.path == "[vvar_vclock]") {
            continue;
        }
        if (own >= m.start && own < m.end) {
            continue; // the dumper's own heap
        }
        bool reactor_stack = false;
        for (auto& s : _shards) {
            if (s->info.stack_lo < m.end && s->info.stack_hi > m.start) {
                reactor_stack = true;
            }
        }
        auto cat_name = fmt::format("map:{}", m.path.empty() ? "[anon]" : m.path);
        auto& cs = _categories[cat_name];
        cs.bytes += m.end - m.start;
        if (reactor_stack) {
            continue; // only the live part, from the park copy
        }
        bool inc = rule_included(cat_name);
        if (inc && m.path.empty() && m.end - m.start > max_fuzzy_mapping && !user_included(cat_name)) {
            inc = false;
            _notes.push_back(fmt::format("skipped anonymous mapping {:#x}-{:#x} ({} MB)", m.start, m.end, (m.end - m.start) >> 20));
        }
        cs.included = inc;
        if (!inc) {
            continue;
        }
        // Cut out the parked copies.
        auto cur = m.start;
        for (auto& p : parked) {
            if (p.end <= cur || p.start >= m.end) {
                continue;
            }
            if (p.start > cur) {
                _extra.push_back({cur, p.start, nullptr});
            }
            cur = std::max(cur, p.end);
        }
        if (cur < m.end) {
            _extra.push_back({cur, m.end, nullptr});
        }
    }
    for (auto& p : parked) {
        _extra.push_back(p);
    }
    std::sort(_extra.begin(), _extra.end(), [] (auto& a, auto& b) { return a.start < b.start; });
    // Parked copies may overlap each other (TLS at the top of a stack mapping).
    std::vector<fuzzy_range> merged;
    for (auto& r : _extra) {
        if (!merged.empty() && r.start < merged.back().end) {
            if (r.end <= merged.back().end) {
                continue;
            }
            auto skip = merged.back().end - r.start;
            r.start += skip;
            if (r.data) {
                r.data += skip;
            }
        }
        merged.push_back(r);
    }
    _extra = std::move(merged);
    for (auto& r : _extra) {
        _fuzzy_bytes += r.end - r.start;
    }
}

void dumper::copy_block(shard_ctx& s, size_t b, char* bounce) {
    auto start = s.block_start(b);
    auto len = s.block_end(b) - start;
    for (;;) {
        uint8_t cur = s.blk_state[b].load(std::memory_order_acquire);
        if (cur == blk_protected) {
            if (s.blk_state[b].compare_exchange_strong(cur, blk_busy)) {
                std::memcpy(bounce, reinterpret_cast<const void*>(start), len);
                s.blk_state[b].store(blk_done, std::memory_order_release);
                release_wp(start, len);
                return;
            }
        } else if (cur == blk_copied) {
            std::memcpy(bounce, slot_ptr(s.blk_slot[b]), len);
            return;
        } else if (cur == blk_done) {
            // Released without a copy, which only happens on abort.
            std::memset(bounce, 0, len);
            return;
        } else {
            std::this_thread::yield();
        }
    }
}

rjson::value dumper::note_json() {
    auto strings = [] (const std::vector<std::string>& v) {
        auto a = rjson::empty_array();
        for (auto& x : v) {
            rjson::push_back(a, rjson::from_string(x));
        }
        return a;
    };
    auto out = rjson::empty_object();
    rjson::add(out, "version", rjson::from_string(scylla_version()));
    rjson::add(out, "pid", int64_t(getpid()));
    auto rules = rjson::empty_object();
    rjson::add(rules, "include", strings(_opts.include));
    rjson::add(rules, "exclude", strings(_opts.exclude));
    rjson::add(rules, "defaults", strings({"exclude lsa:*", "exclude tag:*", "exclude free", "include *"}));
    rjson::add(out, "rules", std::move(rules));
    auto cats = rjson::empty_object();
    for (auto& [name, cs] : _categories) {
        auto c = rjson::empty_object();
        rjson::add(c, "bytes", uint64_t(cs.bytes));
        rjson::add(c, "included", cs.included);
        rjson::add_with_string_name(cats, name, std::move(c));
    }
    rjson::add(out, "categories", std::move(cats));
    auto shards = rjson::empty_array();
    for (auto& s : _shards) {
        auto o = rjson::empty_object();
        rjson::add(o, "shard", unsigned(s->info.shard));
        rjson::add(o, "tid", int64_t(s->regs.tid));
        rjson::add(o, "parked", s->parked());
        rjson::add(o, "park_us", s->park_ns / 1000.0);
        rjson::add(o, "arm_us", s->arm_ns / 1000.0);
        rjson::add(o, "fault_wait_us", s->stall_ns.load() / 1000.0);
        rjson::add(o, "faults", uint64_t(s->faults.load()));
        rjson::add(o, "free_objects_scrubbed", uint64_t(s->free_objects));
        rjson::add(o, "t_parked_ms", s->t_parked / 1e6);
        rjson::add(o, "t_classify_ms", s->t_classify / 1e6);
        rjson::add(o, "t_released_ms", s->t_released / 1e6);
        rjson::add(o, "t_precopied_ms", s->t_precopied / 1e6);
        rjson::add(o, "faults_until_precopied", uint64_t(s->faults_at_precopied));
        rjson::push_back(shards, std::move(o));
    }
    rjson::add(out, "shards", std::move(shards));
    rjson::add(out, "notes", strings(_notes));
    return out;
}

std::string dumper::write_file() {
    elf::core_layout layout;
    auto cmdline = read_file("/proc/self/cmdline");
    std::replace(cmdline.begin(), cmdline.end(), '\0', ' ');
    layout.add_process_info(getpid(), "scylla", cmdline);
    for (auto& s : _shards) {
        if (s->parked()) {
            layout.add_thread(s->regs, getpid());
        }
    }
    auto auxv = read_file("/proc/self/auxv");
    layout.add_auxv(auxv);
    std::vector<elf::file_mapping> files;
    for (auto& m : _maps) {
        // Real files only, under the name they had when mapped.
        if (m.path.empty() || m.path[0] != '/' || m.path.starts_with("/[") || m.path.starts_with("/dev/")) {
            continue;
        }
        auto path = m.path;
        if (path.ends_with(" (deleted)")) {
            path.resize(path.size() - 10);
        }
        files.push_back({m.start, m.end, m.offset, std::move(path)});
    }
    layout.add_files(files);
    auto note = rjson::print(note_json());
    layout.add_note("SCYLLA", nt_scylla, note);

    // Segments in address order: shard runs and the other ranges never overlap.
    struct seg_src {
        uintptr_t start;
        uintptr_t end;
        shard_ctx* shard;
        const char* data;
    };
    std::vector<seg_src> segs;
    for (auto& s : _shards) {
        if (!s->parked()) {
            continue;
        }
        for (auto& [a, b] : s->runs) {
            segs.push_back({a, b, s.get(), nullptr});
        }
    }
    for (auto& r : _extra) {
        segs.push_back({r.start, r.end, nullptr, r.data});
    }
    std::sort(segs.begin(), segs.end(), [] (auto& a, auto& b) { return a.start < b.start; });
    for (auto& sg : segs) {
        layout.add_segment(sg.start, sg.end - sg.start);
    }
    auto header = layout.finalize();
    _file_size = layout.file_size();

    // Caps on size.
    uint64_t shard_memory = 0;
    for (auto& s : _shards) {
        shard_memory += s->info.mem_end - s->info.mem_start;
    }
    if (_file_size > _opts.max_size_ratio * shard_memory) {
        throw std::runtime_error(fmt::format("dump would take {} MB, above {} of shard memory", _file_size >> 20, _opts.max_size_ratio));
    }
    auto dir = _opts.path.substr(0, _opts.path.find_last_of('/') + 1);
    struct statvfs vfs;
    if (statvfs(dir.empty() ? "." : dir.c_str(), &vfs) == 0) {
        uint64_t avail = uint64_t(vfs.f_bavail) * vfs.f_frsize;
        uint64_t total = uint64_t(vfs.f_blocks) * vfs.f_frsize;
        if (avail < _file_size || avail - _file_size < total * _opts.min_free_disk_ratio) {
            throw std::runtime_error(fmt::format("not enough disk space for a {} MB dump", _file_size >> 20));
        }
    }

    int fd = ::open(_opts.path.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
    if (fd < 0) {
        throw std::runtime_error(fmt::format("cannot create {}: {}", _opts.path, strerror(errno)));
    }
    _created = true;
    auto close_fd = seastar::defer([fd] () noexcept { ::close(fd); });
    auto pwrite_all = [&] (const char* p, size_t len, uint64_t off) {
        while (len) {
            auto n = ::pwrite(fd, p, len, off);
            if (n < 0) {
                if (errno == EINTR) {
                    continue;
                }
                throw std::runtime_error(fmt::format("write to {} failed: {}", _opts.path, strerror(errno)));
            }
            p += n;
            off += n;
            len -= n;
        }
    };
    pwrite_all(header.data(), header.size(), 0);

    std::vector<char> bounce(block_size);
    auto deadline = now_ns() + int64_t(_opts.max_copy_seconds) * 1'000'000'000;
    uint64_t written = 0;
    auto t_write = now_ns();
    auto throttle = [&] (size_t n) {
        written += n;
        if (_opts.write_rate_mb) {
            auto target = int64_t(written * 1e9 / (_opts.write_rate_mb * 1e6));
            auto ahead = target - (now_ns() - t_write);
            if (ahead > 0) {
                std::this_thread::sleep_for(std::chrono::nanoseconds(ahead));
            }
        }
    };
    auto segs_out = layout.segments();
    struct run_out {
        uintptr_t start;
        uintptr_t end;
        uint64_t offset;
    };
    std::map<shard_ctx*, std::vector<run_out>> shard_runs;
    for (size_t j = 0; j < segs.size(); ++j) {
        if (segs[j].shard) {
            shard_runs[segs[j].shard].push_back({segs[j].start, segs[j].end, segs_out[j].file_offset});
        }
    }
    // Shard memory, block by block: each block is copied (and released) once.
    for (auto& [sp, rv] : shard_runs) {
        auto& s = *sp;
        size_t r = 0;
        for (size_t b = 0; b < s.nr_blocks; ++b) {
            if (!s.blk_included[b]) {
                continue;
            }
            if (_abort.load()) {
                throw std::runtime_error(abort_reason());
            }
            if (now_ns() > deadline) {
                set_abort("copy took too long");
                throw std::runtime_error(abort_reason());
            }
            auto bstart = s.block_start(b);
            auto bend = s.block_end(b);
            copy_block(s, b, bounce.data());
            auto z = std::lower_bound(s.zeros.begin(), s.zeros.end(), std::pair<uintptr_t, uintptr_t>(bstart, 0));
            if (z != s.zeros.begin()) {
                --z;
            }
            for (; z != s.zeros.end() && z->first < bend; ++z) {
                auto a = std::max(z->first, bstart);
                auto e = std::min(z->second, bend);
                if (a < e) {
                    std::memset(bounce.data() + (a - bstart), 0, e - a);
                }
            }
            while (r < rv.size() && rv[r].end <= bstart) {
                ++r;
            }
            for (size_t k = r; k < rv.size() && rv[k].start < bend; ++k) {
                auto a = std::max(rv[k].start, bstart);
                auto e = std::min(rv[k].end, bend);
                if (a < e) {
                    pwrite_all(bounce.data() + (a - bstart), e - a, rv[k].offset + (a - rv[k].start));
                    throttle(e - a);
                }
            }
        }
    }
    // Everything outside the seastar allocator. Live memory is handed to the
    // kernel directly, so a mapping which went away since makes the write
    // fail with EFAULT instead of crashing; it is left as zeros.
    for (size_t j = 0; j < segs.size(); ++j) {
        auto& sg = segs[j];
        if (sg.shard) {
            continue;
        }
        if (sg.data) {
            pwrite_all(sg.data, sg.end - sg.start, segs_out[j].file_offset);
        } else {
            for (auto a = sg.start; a < sg.end;) {
                auto n = std::min<size_t>(sg.end - a, block_size);
                auto off = segs_out[j].file_offset + (a - sg.start);
                auto w = ::pwrite(fd, reinterpret_cast<const void*>(a), n, off);
                if (w < 0 && errno == EFAULT) {
                    w = n;
                } else if (w <= 0) {
                    throw std::runtime_error(fmt::format("write to {} failed: {}", _opts.path, strerror(errno)));
                }
                a += w;
            }
        }
        throttle(sg.end - sg.start);
    }
    auto trailer = layout.trailer();
    if (!trailer.empty()) {
        pwrite_all(trailer.data(), trailer.size(), layout.trailer_offset());
    }
    if (::ftruncate(fd, _file_size) != 0 || ::fsync(fd) != 0) {
        throw std::runtime_error(fmt::format("cannot finish {}: {}", _opts.path, strerror(errno)));
    }
    return note;
}

std::string dumper::report(bool written) {
    auto out = rjson::empty_object();
    rjson::add(out, "path", rjson::from_string(_opts.path));
    rjson::add(out, "written", written);
    rjson::add(out, "aborted", _abort.load());
    if (_abort.load()) {
        rjson::add(out, "abort_reason", rjson::from_string(abort_reason()));
    }
    int64_t park_max = 0;
    uint64_t stall_max = 0;
    for (auto& s : _shards) {
        park_max = std::max(park_max, s->park_ns);
        stall_max = std::max(stall_max, s->stall_ns.load());
    }
    rjson::add(out, "file_bytes", uint64_t(written ? _file_size : 0));
    rjson::add(out, "shard_bytes_included", uint64_t(_included_bytes));
    rjson::add(out, "other_bytes_included", uint64_t(_fuzzy_bytes));
    std::vector<uint32_t> samples(std::min<size_t>(_faults.load(), max_fault_samples));
    for (size_t i = 0; i < samples.size(); ++i) {
        samples[i] = _fault_us[i].load(std::memory_order_relaxed);
    }
    std::sort(samples.begin(), samples.end());
    auto pct = [&] (double q) { return samples.empty() ? 0u : samples[std::min(samples.size() - 1, size_t(q * samples.size()))]; };
    rjson::add(out, "park_us_max", park_max / 1000.0);
    rjson::add(out, "faults", uint64_t(_faults.load()));
    rjson::add(out, "fault_us_p50", pct(0.5));
    rjson::add(out, "fault_us_p99", pct(0.99));
    rjson::add(out, "fault_us_max", _fault_ns_max.load() / 1000.0);
    rjson::add(out, "fault_us_total", _fault_ns_total.load() / 1000.0);
    rjson::add(out, "shard_fault_wait_us_max", stall_max / 1000.0);
    rjson::add(out, "blocks_copied_by_faults", uint64_t(_staged_blocks.load()));
    rjson::add(out, "blocks_precopied", uint64_t(_precopied_blocks.load()));
    rjson::add(out, "copy_buffer_used_bytes",
            uint64_t((std::min(_next_slot.load(), _nr_slots) + std::min(_next_fault_slot.load(), _fault_slots)) * block_size));
    rjson::add(out, "fault_copy_us_total", _fault_copy_ns.load() / 1000.0);
    rjson::add(out, "fault_release_us_total", _fault_release_ns.load() / 1000.0);
    rjson::add(out, "fault_copies_not_needed", uint64_t(_wasted_copies.load()));
    rjson::add(out, "helper_cpus", rjson::from_string(helper_cpu_list()));
    rjson::add(out, "fault_handlers", uint64_t(_nr_handlers));
    rjson::add(out, "workers", uint64_t(_nr_workers));
    auto t_pre = _t_classified;
    auto times = rjson::empty_object();
    rjson::add(times, "park_all", (_t_parked - _t_start) / 1e6);
    rjson::add(times, "park_classify_precopy", (t_pre - _t_start) / 1e6);
    rjson::add(times, "write", written ? (_t_written - t_pre) / 1e6 : 0.0);
    rjson::add(times, "total", ((written ? _t_written : _t_classified) - _t_start) / 1e6);
    rjson::add(out, "time_ms", std::move(times));
    rjson::add(out, "note", note_json());
    return rjson::print(out);
}

std::string dumper::run() {
    _t_start = now_ns();
    _maps = read_maps();
    bool written = false;
    try {
        apply_helper_cpus();
        if (!_opts.dry_run) {
            setup_uffd();
        }
        process_parked();
        // Writing the file competes with the shards; let them win.
        setpriority(PRIO_PROCESS, gettid(), 10);
        if (_abort.load()) {
            throw std::runtime_error(abort_reason());
        }
        for (auto& s : _shards) {
            for (auto& [name, cs] : s->categories) {
                _categories[name].bytes += cs.bytes;
                _categories[name].included = cs.included;
            }
            _included_bytes += s->included_bytes;
        }
        collect_fuzzy();
        _t_classified = now_ns();
        if (!_opts.dry_run) {
            write_file();
            written = true;
            _t_written = now_ns();
        }
    } catch (...) {
        set_abort(fmt::format("{}", std::current_exception()));
        release_all();
        if (_created) {
            ::unlink(_opts.path.c_str());
        }
        if (!_t_classified) {
            _t_classified = now_ns();
        }
        if (!_t_parked) {
            _t_parked = _t_classified;
        }
    }
    release_all();
    stop_handler();
    return report(written);
}

std::atomic<bool> dump_in_progress{false};

}

seastar::future<result> take(options opts) {
    if (!seastar::alien::internal::default_instance) {
        throw std::runtime_error("minidump: no alien instance");
    }
    if (opts.path.empty() && !opts.dry_run) {
        throw std::invalid_argument("minidump: path is required");
    }
    if (dump_in_progress.exchange(true)) {
        throw std::runtime_error("minidump: another dump is in progress");
    }
    auto reset = seastar::defer([] () noexcept { dump_in_progress = false; });

    std::vector<shard_info> infos(seastar::this_smp_shard_count());
    co_await seastar::smp::invoke_on_all([&infos] {
        infos[seastar::this_shard_id()] = collect_shard_info();
    });
    mdlog.info("taking a minidump to {}{}", opts.path, opts.dry_run ? " (dry run)" : "");

    // The dump thread only sets a flag when done (nothing that could fail);
    // shard 0 polls it.
    std::string res;
    bool ok = false;
    std::atomic<bool> done{false};
    std::thread t([&res, &ok, &done, opts = std::move(opts), infos = std::move(infos)] () mutable {
        try {
            dumper d(std::move(opts), std::move(infos));
            res = d.run();
            ok = d.ok();
        } catch (...) {
            try {
                auto err = rjson::empty_object();
                rjson::add(err, "error", rjson::from_string(fmt::format("{}", std::current_exception())));
                res = rjson::print(err);
            } catch (...) {
                res = "{\"error\":\"out of memory\"}";
            }
        }
        done.store(true, std::memory_order_release);
    });
    while (!done.load(std::memory_order_acquire)) {
        co_await seastar::sleep(std::chrono::milliseconds(10));
    }
    t.join();
    mdlog.info("minidump finished: {}", res);
    co_return result{seastar::sstring(res), ok};
}

#else

// Snapshots rely on the seastar allocator's page map (absent in builds with
// the default allocator: debug, sanitize, coverage), and the core writer and
// register capture are x86_64-only.
seastar::future<result> take(options) {
#ifdef SEASTAR_DEFAULT_ALLOCATOR
    throw std::runtime_error("minidump: requires the seastar allocator, not available in this build mode");
#else
    throw std::runtime_error("minidump: only x86_64 is supported");
#endif
}

#endif

}
