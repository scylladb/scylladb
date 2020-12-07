/*
 * Copyright (C) 2018 ScyllaDB
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

#include <seastar/core/seastar.hh>
#include <seastar/core/print.hh>
#include <seastar/util/lazy.hh>
#include <seastar/util/log.hh>

#include "reader_concurrency_semaphore.hh"
#include "utils/exceptions.hh"
#include "schema.hh"
#include "utils/human_readable.hh"

logger rcslog("reader_concurrency_semaphore");

reader_permit::resource_units::resource_units(reader_permit permit, reader_resources res) noexcept
    : _permit(std::move(permit)), _resources(res) {
    _permit.consume(res);
}

reader_permit::resource_units::resource_units(resource_units&& o) noexcept
    : _permit(std::move(o._permit))
    , _resources(std::exchange(o._resources, {})) {
}

reader_permit::resource_units::~resource_units() {
    if (_resources) {
        reset();
    }
}

reader_permit::resource_units& reader_permit::resource_units::operator=(resource_units&& o) noexcept {
    if (&o == this) {
        return *this;
    }
    reset();
    _permit = std::move(o._permit);
    _resources = std::exchange(o._resources, {});
    return *this;
}

void reader_permit::resource_units::add(resource_units&& o) {
    assert(_permit == o._permit);
    _resources += std::exchange(o._resources, {});
}

void reader_permit::resource_units::reset(reader_resources res) {
    _permit.consume(res);
    if (_resources) {
        _permit.signal(_resources);
    }
    _resources = res;
}

class reader_permit::impl : public boost::intrusive::list_base_hook<boost::intrusive::link_mode<boost::intrusive::auto_unlink>> {
    reader_concurrency_semaphore& _semaphore;
    const schema* _schema;
    sstring _op_name;
    std::string_view _op_name_view;
    reader_resources _resources;
    reader_permit::state _state = reader_permit::state::registered;

public:
    struct value_tag {};

    impl(reader_concurrency_semaphore& semaphore, const schema* const schema, const std::string_view& op_name)
        : _semaphore(semaphore)
        , _schema(schema)
        , _op_name_view(op_name)
    { }
    impl(reader_concurrency_semaphore& semaphore, const schema* const schema, sstring&& op_name)
        : _semaphore(semaphore)
        , _schema(schema)
        , _op_name(std::move(op_name))
        , _op_name_view(_op_name)
    { }
    ~impl() {
        if (_resources) {
            on_internal_error_noexcept(rcslog, format("reader_permit::impl::~impl(): permit {}.{}:{} detected a leak of {{count={}, memory={}}} resources",
                        _schema ? _schema->ks_name() : "*",
                        _schema ? _schema->cf_name() : "*",
                        _op_name_view,
                        _resources.count,
                        _resources.memory));
        }
    }

    reader_concurrency_semaphore& semaphore() {
        return _semaphore;
    }

    const ::schema* get_schema() const {
        return _schema;
    }

    std::string_view get_op_name() const {
        return _op_name_view;
    }

    reader_permit::state get_state() const {
        return _state;
    }

    void on_waiting() {
        _state = reader_permit::state::waiting;
    }

    void on_admission() {
        _state = reader_permit::state::admitted;
        _semaphore.consume(_resources);
    }

    void consume(reader_resources res) {
        _resources += res;
        if (_state == reader_permit::state::admitted) {
            _semaphore.consume(res);
        }
    }

    void signal(reader_resources res) {
        _resources -= res;
        if (_state == reader_permit::state::admitted) {
            _semaphore.signal(res);
        }
    }

    reader_resources resources() const {
        return _resources;
    }
};

struct reader_concurrency_semaphore::permit_list {
    using list_type = boost::intrusive::list<reader_permit::impl, boost::intrusive::constant_time_size<false>>;

    list_type permits;
};

reader_permit::reader_permit(reader_concurrency_semaphore& semaphore, const schema* const schema, std::string_view op_name)
    : _impl(::seastar::make_shared<reader_permit::impl>(semaphore, schema, op_name))
{
    semaphore._permit_list->permits.push_back(*_impl);
}

reader_permit::reader_permit(reader_concurrency_semaphore& semaphore, const schema* const schema, sstring&& op_name)
    : _impl(::seastar::make_shared<reader_permit::impl>(semaphore, schema, std::move(op_name)))
{
    semaphore._permit_list->permits.push_back(*_impl);
}

void reader_permit::on_waiting() {
    _impl->on_waiting();
}

void reader_permit::on_admission() {
    _impl->on_admission();
}

reader_permit::~reader_permit() {
}

reader_concurrency_semaphore& reader_permit::semaphore() {
    return _impl->semaphore();
}

future<reader_permit::resource_units> reader_permit::wait_admission(size_t memory, db::timeout_clock::time_point timeout) {
    return _impl->semaphore().do_wait_admission(*this, memory, timeout);
}

void reader_permit::consume(reader_resources res) {
    _impl->consume(res);
}

void reader_permit::signal(reader_resources res) {
    _impl->signal(res);
}

reader_permit::resource_units reader_permit::consume_memory(size_t memory) {
    return consume_resources(reader_resources{0, ssize_t(memory)});
}

reader_permit::resource_units reader_permit::consume_resources(reader_resources res) {
    return resource_units(*this, res);
}

reader_resources reader_permit::consumed_resources() const {
    return _impl->resources();
}

std::ostream& operator<<(std::ostream& os, reader_permit::state s) {
    switch (s) {
        case reader_permit::state::registered:
            os << "registered";
            break;
        case reader_permit::state::waiting:
            os << "waiting";
            break;
        case reader_permit::state::admitted:
            os << "admitted";
            break;
    }
    return os;
}

namespace {

struct permit_stats {
    uint64_t memory = 0;
    uint64_t count = 0;

    void add(uint64_t m) {
        memory += m;
        ++count;
    }

    permit_stats& operator+=(const permit_stats& o) {
        memory += o.memory;
        count += o.count;
        return *this;
    }
};

using permit_group_key = std::tuple<const schema*, std::string_view, reader_permit::state>;

struct permit_group_key_hash {
    size_t operator()(const permit_group_key& k) const {
        using underlying_type = std::underlying_type_t<reader_permit::state>;
        return std::hash<uintptr_t>()(reinterpret_cast<uintptr_t>(std::get<0>(k)))
            ^ std::hash<std::string_view>()(std::get<1>(k))
            ^ std::hash<underlying_type>()(static_cast<underlying_type>(std::get<2>(k)));
    }
};

using permit_groups = std::unordered_map<permit_group_key, permit_stats, permit_group_key_hash>;

static permit_stats do_dump_reader_permit_diagnostics(std::ostream& os, const permit_groups& permits, reader_permit::state state, bool sort_by_memory) {
    struct permit_summary {
        const schema* s;
        std::string_view op_name;
        uint64_t memory;
        uint64_t count;
    };

    std::vector<permit_summary> permit_summaries;
    for (const auto& [k, v] : permits) {
        const auto& [s, op_name, k_state] = k;
        if (k_state == state) {
            permit_summaries.emplace_back(permit_summary{s, op_name, v.memory, v.count});
        }
    }

    std::ranges::sort(permit_summaries, [sort_by_memory] (const permit_summary& a, const permit_summary& b) {
        if (sort_by_memory) {
            return a.memory < b.memory;
        } else {
            return a.count < b.count;
        }
    });

    permit_stats total;

    auto print_line = [&os, sort_by_memory] (auto col1, auto col2, auto col3) {
        if (sort_by_memory) {
            fmt::print(os, "{}\t{}\t{}\n", col2, col1, col3);
        } else {
            fmt::print(os, "{}\t{}\t{}\n", col1, col2, col3);
        }
    };

    fmt::print(os, "Permits with state {}, sorted by {}\n", state, sort_by_memory ? "memory" : "count");
    print_line("count", "memory", "name");
    for (const auto& summary : permit_summaries) {
        total.count += summary.count;
        total.memory += summary.memory;
        print_line(summary.count, utils::to_hr_size(summary.memory), fmt::format("{}.{}:{}",
                    summary.s ? summary.s->ks_name() : "*",
                    summary.s ? summary.s->cf_name() : "*",
                    summary.op_name));
    }
    fmt::print(os, "\n");
    print_line(total.count, utils::to_hr_size(total.memory), "total");
    return total;
}

static void do_dump_reader_permit_diagnostics(std::ostream& os, const reader_concurrency_semaphore& semaphore,
        const reader_concurrency_semaphore::permit_list& list, std::string_view problem) {
    permit_groups permits;

    for (const auto& permit : list.permits) {
        permits[permit_group_key(permit.get_schema(), permit.get_op_name(), permit.get_state())].add(permit.resources().memory);
    }

    permit_stats total;

    fmt::print(os, "Semaphore {}: {}, dumping permit diagnostics:\n", semaphore.name(), problem);
    total += do_dump_reader_permit_diagnostics(os, permits, reader_permit::state::admitted, true);
    fmt::print(os, "\n");
    total += do_dump_reader_permit_diagnostics(os, permits, reader_permit::state::waiting, false);
    fmt::print(os, "\n");
    total += do_dump_reader_permit_diagnostics(os, permits, reader_permit::state::registered, false);
    fmt::print(os, "\n");
    fmt::print(os, "Total: permits: {}, memory: {}\n", total.count, utils::to_hr_size(total.memory));
}

static void maybe_dump_reader_permit_diagnostics(const reader_concurrency_semaphore& semaphore, const reader_concurrency_semaphore::permit_list& list,
        std::string_view problem) {
    static thread_local logger::rate_limit rate_limit(std::chrono::seconds(30));

    rcslog.log(log_level::info, rate_limit, "{}", value_of([&] {
        std::ostringstream os;
        do_dump_reader_permit_diagnostics(os, semaphore, list, problem);
        return os.str();
    }));
}

} // anonymous namespace

void reader_concurrency_semaphore::expiry_handler::operator()(entry& e) noexcept {
    e.pr.set_exception(named_semaphore_timed_out(_semaphore._name));

    maybe_dump_reader_permit_diagnostics(_semaphore, *_semaphore._permit_list, "timed out");
}

void reader_concurrency_semaphore::signal(const resources& r) noexcept {
    _resources += r;
    while (!_wait_list.empty() && has_available_units(_wait_list.front().res)) {
        auto& x = _wait_list.front();
        try {
            x.permit.on_admission();
            x.pr.set_value(reader_permit::resource_units(std::move(x.permit), x.res));
        } catch (...) {
            x.pr.set_exception(std::current_exception());
        }
        _wait_list.pop_front();
    }
}

reader_concurrency_semaphore::reader_concurrency_semaphore(int count, ssize_t memory, sstring name, size_t max_queue_length,
        std::function<void()> prethrow_action)
    : _initial_resources(count, memory)
    , _resources(count, memory)
    , _wait_list(expiry_handler(*this))
    , _name(std::move(name))
    , _max_queue_length(max_queue_length)
    , _prethrow_action(std::move(prethrow_action))
    , _permit_list(std::make_unique<permit_list>()) {}

reader_concurrency_semaphore::reader_concurrency_semaphore(no_limits, sstring name)
    : reader_concurrency_semaphore(
            std::numeric_limits<int>::max(),
            std::numeric_limits<ssize_t>::max(),
            std::move(name)) {}

reader_concurrency_semaphore::~reader_concurrency_semaphore() {
    broken(std::make_exception_ptr(broken_semaphore{}));
}

reader_concurrency_semaphore::inactive_read_handle reader_concurrency_semaphore::register_inactive_read(std::unique_ptr<inactive_read> ir) {
    // Implies _inactive_reads.empty(), we don't queue new readers before
    // evicting all inactive reads.
    if (_wait_list.empty()) {
        const auto [it, _] = _inactive_reads.emplace(_next_id++, std::move(ir));
        (void)_;
        ++_stats.inactive_reads;
        return inactive_read_handle(*this, it->first);
    }

    // The evicted reader will release its permit, hopefully allowing us to
    // admit some readers from the _wait_list.
    ir->evict();
    ++_stats.permit_based_evictions;
    return inactive_read_handle();
}

std::unique_ptr<reader_concurrency_semaphore::inactive_read> reader_concurrency_semaphore::unregister_inactive_read(inactive_read_handle irh) {
    if (irh && irh._sem != this) {
        throw std::runtime_error(fmt::format(
                    "reader_concurrency_semaphore::unregister_inactive_read(): "
                    "attempted to unregister an inactive read with a handle belonging to another semaphore: "
                    "this is {} (0x{:x}) but the handle belongs to {} (0x{:x})",
                    name(),
                    reinterpret_cast<uintptr_t>(this),
                    irh._sem->name(),
                    reinterpret_cast<uintptr_t>(irh._sem)));
    }

    if (auto it = _inactive_reads.find(irh._id); it != _inactive_reads.end()) {
        auto ir = std::move(it->second);
        _inactive_reads.erase(it);
        --_stats.inactive_reads;
        return ir;
    }
    return {};
}

bool reader_concurrency_semaphore::try_evict_one_inactive_read() {
    if (_inactive_reads.empty()) {
        return false;
    }
    auto it = _inactive_reads.begin();
    it->second->evict();
    _inactive_reads.erase(it);

    ++_stats.permit_based_evictions;
    --_stats.inactive_reads;

    return true;
}

bool reader_concurrency_semaphore::has_available_units(const resources& r) const {
    return bool(_resources) && _resources >= r;
}

bool reader_concurrency_semaphore::may_proceed(const resources& r) const {
    // Special case: when there is no active reader (based on count) admit one
    // regardless of availability of memory.
    return _wait_list.empty() && (has_available_units(r) || _resources.count == _initial_resources.count);
}

future<reader_permit::resource_units> reader_concurrency_semaphore::do_wait_admission(reader_permit permit, size_t memory,
        db::timeout_clock::time_point timeout) {
    if (_wait_list.size() >= _max_queue_length) {
        _stats.total_reads_shed_due_to_overload++;
        if (_prethrow_action) {
            _prethrow_action();
        }
        maybe_dump_reader_permit_diagnostics(*this, *_permit_list, "wait queue overloaded");
        return make_exception_future<reader_permit::resource_units>(
                exceptions::overloaded_exception(format("{}: restricted mutation reader queue overload", _name)));
    }
    auto r = resources(1, static_cast<ssize_t>(memory));
    auto it = _inactive_reads.begin();
    while (!may_proceed(r) && it != _inactive_reads.end()) {
        auto ir = std::move(it->second);
        it = _inactive_reads.erase(it);
        ir->evict();

        ++_stats.permit_based_evictions;
        --_stats.inactive_reads;
    }
    if (may_proceed(r)) {
        permit.on_admission();
        return make_ready_future<reader_permit::resource_units>(reader_permit::resource_units(std::move(permit), r));
    }
    promise<reader_permit::resource_units> pr;
    auto fut = pr.get_future();
    permit.on_waiting();
    _wait_list.push_back(entry(std::move(pr), std::move(permit), r), timeout);
    return fut;
}

reader_permit reader_concurrency_semaphore::make_permit(const schema* const schema, const char* const op_name) {
    return reader_permit(*this, schema, std::string_view(op_name));
}

reader_permit reader_concurrency_semaphore::make_permit(const schema* const schema, sstring&& op_name) {
    return reader_permit(*this, schema, std::move(op_name));
}

void reader_concurrency_semaphore::broken(std::exception_ptr ex) {
    while (!_wait_list.empty()) {
        _wait_list.front().pr.set_exception(std::make_exception_ptr(broken_semaphore{}));
        _wait_list.pop_front();
    }
}

// A file that tracks the memory usage of buffers resulting from read
// operations.
class tracking_file_impl : public file_impl {
    file _tracked_file;
    reader_permit _permit;

public:
    tracking_file_impl(file file, reader_permit permit)
        : _tracked_file(std::move(file))
        , _permit(std::move(permit)) {
        _memory_dma_alignment = _tracked_file.memory_dma_alignment();
        _disk_read_dma_alignment = _tracked_file.disk_read_dma_alignment();
        _disk_write_dma_alignment = _tracked_file.disk_write_dma_alignment();
    }

    tracking_file_impl(const tracking_file_impl&) = delete;
    tracking_file_impl& operator=(const tracking_file_impl&) = delete;
    tracking_file_impl(tracking_file_impl&&) = default;
    tracking_file_impl& operator=(tracking_file_impl&&) = default;

    virtual future<size_t> write_dma(uint64_t pos, const void* buffer, size_t len, const io_priority_class& pc) override {
        return get_file_impl(_tracked_file)->write_dma(pos, buffer, len, pc);
    }

    virtual future<size_t> write_dma(uint64_t pos, std::vector<iovec> iov, const io_priority_class& pc) override {
        return get_file_impl(_tracked_file)->write_dma(pos, std::move(iov), pc);
    }

    virtual future<size_t> read_dma(uint64_t pos, void* buffer, size_t len, const io_priority_class& pc) override {
        return get_file_impl(_tracked_file)->read_dma(pos, buffer, len, pc);
    }

    virtual future<size_t> read_dma(uint64_t pos, std::vector<iovec> iov, const io_priority_class& pc) override {
        return get_file_impl(_tracked_file)->read_dma(pos, iov, pc);
    }

    virtual future<> flush(void) override {
        return get_file_impl(_tracked_file)->flush();
    }

    virtual future<struct stat> stat(void) override {
        return get_file_impl(_tracked_file)->stat();
    }

    virtual future<> truncate(uint64_t length) override {
        return get_file_impl(_tracked_file)->truncate(length);
    }

    virtual future<> discard(uint64_t offset, uint64_t length) override {
        return get_file_impl(_tracked_file)->discard(offset, length);
    }

    virtual future<> allocate(uint64_t position, uint64_t length) override {
        return get_file_impl(_tracked_file)->allocate(position, length);
    }

    virtual future<uint64_t> size(void) override {
        return get_file_impl(_tracked_file)->size();
    }

    virtual future<> close() override {
        return get_file_impl(_tracked_file)->close();
    }

    virtual std::unique_ptr<file_handle_impl> dup() override {
        return get_file_impl(_tracked_file)->dup();
    }

    virtual subscription<directory_entry> list_directory(std::function<future<> (directory_entry de)> next) override {
        return get_file_impl(_tracked_file)->list_directory(std::move(next));
    }

    virtual future<temporary_buffer<uint8_t>> dma_read_bulk(uint64_t offset, size_t range_size, const io_priority_class& pc) override {
        return get_file_impl(_tracked_file)->dma_read_bulk(offset, range_size, pc).then([this, units = _permit.consume_memory(range_size)] (temporary_buffer<uint8_t> buf) {
            return make_ready_future<temporary_buffer<uint8_t>>(make_tracked_temporary_buffer(std::move(buf), _permit));
        });
    }
};

file make_tracked_file(file f, reader_permit p) {
    return file(make_shared<tracking_file_impl>(f, std::move(p)));
}
