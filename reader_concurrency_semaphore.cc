/*
 * Copyright (C) 2018-present ScyllaDB
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
#include <seastar/core/coroutine.hh>

#include "reader_concurrency_semaphore.hh"
#include "utils/exceptions.hh"
#include "schema.hh"
#include "utils/human_readable.hh"
#include "flat_mutation_reader.hh"

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

class reader_permit::impl
        : public boost::intrusive::list_base_hook<boost::intrusive::link_mode<boost::intrusive::auto_unlink>>
        , public enable_shared_from_this<reader_permit::impl> {
    reader_concurrency_semaphore& _semaphore;
    const schema* _schema;
    sstring _op_name;
    std::string_view _op_name_view;
    reader_resources _base_resources;
    bool _base_resources_consumed = false;
    reader_resources _resources;
    reader_permit::state _state = reader_permit::state::active_unused;
    uint64_t _used_branches = 0;
    bool _marked_as_used = false;
    uint64_t _blocked_branches = 0;
    bool _marked_as_blocked = false;

private:
    void on_permit_used() {
        _semaphore.on_permit_used();
        _marked_as_used = true;
    }
    void on_permit_unused() {
        _semaphore.on_permit_unused();
        _marked_as_used = false;
    }
    void on_permit_blocked() {
        _semaphore.on_permit_blocked();
        _marked_as_blocked = true;
    }
    void on_permit_unblocked() {
        _semaphore.on_permit_unblocked();
        _marked_as_blocked = false;
    }
    void on_permit_active() {
        if (_used_branches) {
            _state = reader_permit::state::active_used;
            on_permit_used();
        } else {
            _state = reader_permit::state::active_unused;
        }
    }

    void on_permit_inactive(reader_permit::state st) {
        _state = st;
        if (_marked_as_used) {
            on_permit_unused();
        }
    }

public:
    struct value_tag {};

    impl(reader_concurrency_semaphore& semaphore, const schema* const schema, const std::string_view& op_name, reader_resources base_resources)
        : _semaphore(semaphore)
        , _schema(schema)
        , _op_name_view(op_name)
        , _base_resources(base_resources)
    {
        _semaphore.on_permit_created(*this);
    }
    impl(reader_concurrency_semaphore& semaphore, const schema* const schema, sstring&& op_name, reader_resources base_resources)
        : _semaphore(semaphore)
        , _schema(schema)
        , _op_name(std::move(op_name))
        , _op_name_view(_op_name)
        , _base_resources(base_resources)
    {
        _semaphore.on_permit_created(*this);
    }
    ~impl() {
        if (_base_resources_consumed) {
            signal(_base_resources);
        }

        if (_resources) {
            on_internal_error_noexcept(rcslog, format("reader_permit::impl::~impl(): permit {} detected a leak of {{count={}, memory={}}} resources",
                        description(),
                        _resources.count,
                        _resources.memory));
            signal(_resources);
        }

        if (_used_branches) {
            on_internal_error_noexcept(rcslog, format("reader_permit::impl::~impl(): permit {}.{}:{} destroyed with {} used branches",
                        _schema ? _schema->ks_name() : "*",
                        _schema ? _schema->cf_name() : "*",
                        _op_name_view,
                        _used_branches));
            _semaphore.on_permit_unused();
        }

        if (_blocked_branches) {
            on_internal_error_noexcept(rcslog, format("reader_permit::impl::~impl(): permit {}.{}:{} destroyed with {} blocked branches",
                        _schema ? _schema->ks_name() : "*",
                        _schema ? _schema->cf_name() : "*",
                        _op_name_view,
                        _blocked_branches));
            _semaphore.on_permit_unblocked();
        }

        _semaphore.on_permit_destroyed(*this);
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
        on_permit_inactive(reader_permit::state::waiting);
    }

    void on_admission() {
        assert(_state != reader_permit::state::active_blocked);
        on_permit_active();
        consume(_base_resources);
        _base_resources_consumed = true;
    }

    void on_register_as_inactive() {
        assert(_state == reader_permit::state::active_unused || _state == reader_permit::state::active_used);
        on_permit_inactive(reader_permit::state::inactive);
    }

    void on_unregister_as_inactive() {
        assert(_state == reader_permit::state::inactive);
        on_permit_active();
    }

    void on_evicted() {
        assert(_state == reader_permit::state::inactive);
        _state = reader_permit::state::evicted;
        if (_base_resources_consumed) {
            signal(_base_resources);
            _base_resources_consumed = false;
        }
    }

    void consume(reader_resources res) {
        _resources += res;
        _semaphore.consume(res);
    }

    void signal(reader_resources res) {
        _resources -= res;
        _semaphore.signal(res);
    }

    reader_resources resources() const {
        return _resources;
    }

    reader_resources base_resources() const {
        return _base_resources;
    }

    sstring description() const {
        return format("{}.{}:{}",
                _schema ? _schema->ks_name() : "*",
                _schema ? _schema->cf_name() : "*",
                _op_name_view);
    }

    void mark_used() noexcept {
        ++_used_branches;
        if (!_marked_as_used && _state == reader_permit::state::active_unused) {
            _state = reader_permit::state::active_used;
            on_permit_used();
            if (_blocked_branches && !_marked_as_blocked) {
                _state = reader_permit::state::active_blocked;
                on_permit_blocked();
            }
        }
    }

    void mark_unused() noexcept {
        assert(_used_branches);
        --_used_branches;
        if (_marked_as_used && !_used_branches) {
            // When an exception is thrown, blocked and used guards might be
            // destroyed out-of-order. Force an unblock here so that we maintain
            // used >= blocked.
            if (_marked_as_blocked) {
                on_permit_unblocked();
            }
            _state = reader_permit::state::active_unused;
            on_permit_unused();
        }
    }

    void mark_blocked() noexcept {
        ++_blocked_branches;
        if (_blocked_branches == 1 && _state == reader_permit::state::active_used) {
            _state = reader_permit::state::active_blocked;
            on_permit_blocked();
        }
    }

    void mark_unblocked() noexcept {
        assert(_blocked_branches);
        --_blocked_branches;
        if (_marked_as_blocked && !_blocked_branches) {
            _state = reader_permit::state::active_used;
            on_permit_unblocked();
        }
    }

    future<> maybe_wait_readmission(db::timeout_clock::time_point timeout) {
        if (_state != reader_permit::state::evicted) {
            return make_ready_future<>();
        }
        return _semaphore.do_wait_admission(shared_from_this(), timeout);
    }

    // Support method for old-style admission, which passes a new base resources
    // each time the permit is admitted.
    void set_base_resources(reader_resources base_resources) {
        assert(!_base_resources_consumed);
        _base_resources = base_resources;
    }

    // Support method for old-style admission, which passes a new base resources
    // each time the permit is admitted and after admission it stores the base
    // resources externally.
    reader_permit::resource_units base_resources_as_resource_units() {
        auto res_units = reader_permit::resource_units(reader_permit(shared_from_this()), _base_resources);
        if (_base_resources_consumed) {
            signal(_base_resources);
            _base_resources_consumed = false;
        }
        return res_units;
    }
};

struct reader_concurrency_semaphore::permit_list {
    using list_type = boost::intrusive::list<reader_permit::impl, boost::intrusive::constant_time_size<false>>;

    list_type permits;
    permit_stats stats;
};

reader_permit::reader_permit(shared_ptr<impl> impl) : _impl(std::move(impl))
{
}

reader_permit::reader_permit(reader_concurrency_semaphore& semaphore, const schema* const schema, std::string_view op_name,
        reader_resources base_resources)
    : _impl(::seastar::make_shared<reader_permit::impl>(semaphore, schema, op_name, base_resources))
{
}

reader_permit::reader_permit(reader_concurrency_semaphore& semaphore, const schema* const schema, sstring&& op_name,
        reader_resources base_resources)
    : _impl(::seastar::make_shared<reader_permit::impl>(semaphore, schema, std::move(op_name), base_resources))
{
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
    _impl->set_base_resources(reader_resources(1, static_cast<ssize_t>(memory)));
    return _impl->semaphore().do_wait_admission(*this, timeout).then([this] {
        return _impl->base_resources_as_resource_units();
    });
}

future<> reader_permit::maybe_wait_readmission(db::timeout_clock::time_point timeout) {
    return _impl->maybe_wait_readmission(timeout);
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

reader_resources reader_permit::base_resources() const {
    return _impl->base_resources();
}

sstring reader_permit::description() const {
    return _impl->description();
}

void reader_permit::mark_used() noexcept {
    _impl->mark_used();
}

void reader_permit::mark_unused() noexcept {
    _impl->mark_unused();
}

void reader_permit::mark_blocked() noexcept {
    _impl->mark_blocked();
}

void reader_permit::mark_unblocked() noexcept {
    _impl->mark_unblocked();
}

std::ostream& operator<<(std::ostream& os, reader_permit::state s) {
    switch (s) {
        case reader_permit::state::waiting:
            os << "waiting";
            break;
        case reader_permit::state::active_unused:
            os << "active/unused";
            break;
        case reader_permit::state::active_used:
            os << "active/used";
            break;
        case reader_permit::state::active_blocked:
            os << "active/blocked";
            break;
        case reader_permit::state::inactive:
            os << "inactive";
            break;
        case reader_permit::state::evicted:
            os << "evicted";
            break;
    }
    return os;
}

namespace {

struct permit_stats {
    uint64_t permits = 0;
    reader_resources resources;

    void add(const reader_permit::impl& permit) {
        ++permits;
        resources += permit.resources();
    }

    permit_stats& operator+=(const permit_stats& o) {
        permits += o.permits;
        resources += o.resources;
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

static permit_stats do_dump_reader_permit_diagnostics(std::ostream& os, const permit_groups& permits, unsigned max_lines = 20) {
    struct permit_summary {
        const schema* s;
        std::string_view op_name;
        reader_permit::state state;
        uint64_t permits;
        reader_resources resources;
    };

    std::vector<permit_summary> permit_summaries;
    for (const auto& [k, v] : permits) {
        const auto& [s, op_name, k_state] = k;
        permit_summaries.emplace_back(permit_summary{s, op_name, k_state, v.permits, v.resources});
    }

    std::ranges::sort(permit_summaries, [] (const permit_summary& a, const permit_summary& b) {
        return a.resources.memory > b.resources.memory;
    });

    permit_stats total;
    unsigned lines = 0;
    permit_stats omitted_permit_stats;

    auto print_line = [&os] (auto col1, auto col2, auto col3, auto col4) {
        fmt::print(os, "{}\t{}\t{}\t{}\n", col1, col2, col3, col4);
    };

    print_line("permits", "count", "memory", "table/description/state");
    for (const auto& summary : permit_summaries) {
        total.permits += summary.permits;
        total.resources += summary.resources;
        if (!max_lines || lines++ < max_lines) {
            print_line(summary.permits, summary.resources.count, utils::to_hr_size(summary.resources.memory), fmt::format("{}.{}/{}/{}",
                        summary.s ? summary.s->ks_name() : "*",
                        summary.s ? summary.s->cf_name() : "*",
                        summary.op_name,
                        summary.state));
        } else {
            omitted_permit_stats.permits += summary.permits;
            omitted_permit_stats.resources += summary.resources;
        }
    }
    if (max_lines && lines > max_lines) {
        print_line(omitted_permit_stats.permits, omitted_permit_stats.resources.count, utils::to_hr_size(omitted_permit_stats.resources.memory), "permits omitted for brevity");
    }
    fmt::print(os, "\n");
    print_line(total.permits, total.resources.count, utils::to_hr_size(total.resources.memory), "total");
    return total;
}

static void do_dump_reader_permit_diagnostics(std::ostream& os, const reader_concurrency_semaphore& semaphore,
        const reader_concurrency_semaphore::permit_list& list, std::string_view problem, unsigned max_lines = 20) {
    permit_groups permits;

    for (const auto& permit : list.permits) {
        permits[permit_group_key(permit.get_schema(), permit.get_op_name(), permit.get_state())].add(permit);
    }

    permit_stats total;

    fmt::print(os, "Semaphore {} with {}/{} count and {}/{} memory resources: {}, dumping permit diagnostics:\n",
            semaphore.name(),
            semaphore.initial_resources().count - semaphore.available_resources().count,
            semaphore.initial_resources().count,
            semaphore.initial_resources().memory - semaphore.available_resources().memory,
            semaphore.initial_resources().memory,
            problem);
    total += do_dump_reader_permit_diagnostics(os, permits, max_lines);
    fmt::print(os, "\n");
    fmt::print(os, "Total: {} permits with {} count and {} memory resources\n", total.permits, total.resources.count, utils::to_hr_size(total.resources.memory));
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

reader_concurrency_semaphore::inactive_read::~inactive_read() {
    detach();
}

void reader_concurrency_semaphore::inactive_read::detach() noexcept {
    if (handle) {
        handle->_irp = nullptr;
        handle = nullptr;
    }
}

void reader_concurrency_semaphore::inactive_read_handle::abandon() noexcept {
    if (_irp) {
        _sem->close_reader(std::move(_irp->reader));
        delete std::exchange(_irp, nullptr);
    }
}

namespace {

struct stop_execution_loop {
};

}

future<> reader_concurrency_semaphore::execution_loop() noexcept {
    while (!_stopped) {
        try {
            co_await _ready_list.not_empty();
        } catch (stop_execution_loop) {
            co_return;
        }

        while (!_ready_list.empty()) {
            auto e = _ready_list.pop();

            try {
                e.func(std::move(e.permit)).forward_to(std::move(e.pr));
            } catch (...) {
                e.pr.set_exception(std::current_exception());
            }

            if (need_preempt()) {
                co_await make_ready_future<>();
            }
        }
    }
}

void reader_concurrency_semaphore::signal(const resources& r) noexcept {
    _resources += r;
    maybe_admit_waiters();
}

reader_concurrency_semaphore::reader_concurrency_semaphore(int count, ssize_t memory, sstring name, size_t max_queue_length,
        std::function<void()> prethrow_action)
    : _initial_resources(count, memory)
    , _resources(count, memory)
    , _wait_list(expiry_handler(*this))
    , _ready_list(max_queue_length)
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
    if (!_permit_list->stats.total_permits) {
        // We allow destroy without stop() when the semaphore wasn't used at all yet.
        return;
    }
    if (!_stopped) {
        on_internal_error_noexcept(rcslog, format("~reader_concurrency_semaphore(): semaphore {} not stopped before destruction", _name));
        // With the below conditions, we can get away with the semaphore being
        // unstopped. In this case don't force an abort.
        assert(_inactive_reads.empty() && !_close_readers_gate.get_count() && !_permit_gate.get_count() && !_execution_loop_future);
        broken();
    }
}

reader_concurrency_semaphore::inactive_read_handle reader_concurrency_semaphore::register_inactive_read(flat_mutation_reader reader) noexcept {
    auto& permit_impl = *reader.permit()._impl;
    permit_impl.on_register_as_inactive();
    // Implies _inactive_reads.empty(), we don't queue new readers before
    // evicting all inactive reads.
    // Checking the _wait_list covers the count resources only, so check memory
    // separately.
    if (_wait_list.empty() && _resources.memory > 0) {
      try {
        auto irp = std::make_unique<inactive_read>(std::move(reader));
        auto& ir = *irp;
        _inactive_reads.push_back(ir);
        ++_stats.inactive_reads;
        return inactive_read_handle(*this, *irp.release());
      } catch (...) {
        // It is okay to swallow the exception since
        // we're allowed to drop the reader upon registration
        // due to lack of resources. Returning an empty
        // i_r_h here rather than throwing simplifies the caller's
        // error handling.
        rcslog.warn("Registering inactive read failed: {}. Ignored as if it was evicted.", std::current_exception());
      }
    } else {
        permit_impl.on_evicted();
        ++_stats.permit_based_evictions;
    }
    close_reader(std::move(reader));
    return inactive_read_handle();
}

void reader_concurrency_semaphore::set_notify_handler(inactive_read_handle& irh, eviction_notify_handler&& notify_handler, std::optional<std::chrono::seconds> ttl_opt) {
    auto& ir = *irh._irp;
    ir.notify_handler = std::move(notify_handler);
    if (ttl_opt) {
        ir.ttl_timer.set_callback([this, &ir] {
            evict(ir, evict_reason::time);
        });
        ir.ttl_timer.arm(lowres_clock::now() + *ttl_opt);
    }
}

flat_mutation_reader_opt reader_concurrency_semaphore::unregister_inactive_read(inactive_read_handle irh) {
    if (!irh) {
        return {};
    }
    if (irh._sem != this) {
        // unregister from the other semaphore
        // and close the reader, in case on_internal_error
        // doesn't abort.
        auto irp = std::move(irh._irp);
        irp->unlink();
        irh._sem->close_reader(std::move(irp->reader));
        on_internal_error(rcslog, fmt::format(
                    "reader_concurrency_semaphore::unregister_inactive_read(): "
                    "attempted to unregister an inactive read with a handle belonging to another semaphore: "
                    "this is {} (0x{:x}) but the handle belongs to {} (0x{:x})",
                    name(),
                    reinterpret_cast<uintptr_t>(this),
                    irh._sem->name(),
                    reinterpret_cast<uintptr_t>(irh._sem)));
    }

    --_stats.inactive_reads;
    std::unique_ptr<inactive_read> irp(irh._irp);
    irp->reader.permit()._impl->on_unregister_as_inactive();
    return std::move(irp->reader);
}

bool reader_concurrency_semaphore::try_evict_one_inactive_read(evict_reason reason) {
    if (_inactive_reads.empty()) {
        return false;
    }
    evict(_inactive_reads.front(), reason);
    return true;
}

void reader_concurrency_semaphore::clear_inactive_reads() {
    while (!_inactive_reads.empty()) {
        auto& ir = _inactive_reads.front();
        close_reader(std::move(ir.reader));
        // Destroying the read unlinks it too.
        std::unique_ptr<inactive_read> _(&*_inactive_reads.begin());
    }
}

std::runtime_error reader_concurrency_semaphore::stopped_exception() {
    return std::runtime_error(format("{} was stopped", _name));
}

future<> reader_concurrency_semaphore::stop() noexcept {
    assert(!_stopped);
    _stopped = true;
    clear_inactive_reads();
    co_await _close_readers_gate.close();
    co_await _permit_gate.close();
    if (_execution_loop_future) {
        if (_ready_list.has_blocked_consumer()) {
            _ready_list.abort(std::make_exception_ptr(stop_execution_loop{}));
        }
        co_await std::move(*_execution_loop_future);
    }
    broken(std::make_exception_ptr(stopped_exception()));
    co_return;
}

flat_mutation_reader reader_concurrency_semaphore::detach_inactive_reader(inactive_read& ir, evict_reason reason) noexcept {
    auto reader = std::move(ir.reader);
    ir.detach();
    reader.permit()._impl->on_evicted();
    std::unique_ptr<inactive_read> irp(&ir);
    try {
        if (ir.notify_handler) {
            ir.notify_handler(reason);
        }
    } catch (...) {
        rcslog.error("[semaphore {}] evict(): notify handler failed for inactive read evicted due to {}: {}", _name, reason, std::current_exception());
    }
    switch (reason) {
        case evict_reason::permit:
            ++_stats.permit_based_evictions;
            break;
        case evict_reason::time:
            ++_stats.time_based_evictions;
            break;
        case evict_reason::manual:
            break;
    }
    --_stats.inactive_reads;
    return reader;
}

void reader_concurrency_semaphore::evict(inactive_read& ir, evict_reason reason) noexcept {
    close_reader(detach_inactive_reader(ir, reason));
}

void reader_concurrency_semaphore::close_reader(flat_mutation_reader reader) {
    // It is safe to discard the future since it is waited on indirectly
    // by closing the _close_readers_gate in stop().
    (void)with_gate(_close_readers_gate, [reader = std::move(reader)] () mutable {
        return reader.close();
    });
}

bool reader_concurrency_semaphore::has_available_units(const resources& r) const {
    // Special case: when there is no active reader (based on count) admit one
    // regardless of availability of memory.
    return (bool(_resources) && _resources >= r) || _resources.count == _initial_resources.count;
}

std::exception_ptr reader_concurrency_semaphore::check_queue_size(std::string_view queue_name) {
    if ((_wait_list.size() + _ready_list.size()) >= _max_queue_length) {
        _stats.total_reads_shed_due_to_overload++;
        if (_prethrow_action) {
            _prethrow_action();
        }
        maybe_dump_reader_permit_diagnostics(*this, *_permit_list, fmt::format("{} queue overload", queue_name));
        return std::make_exception_ptr(std::runtime_error(format("{}: {} queue overload", _name, queue_name)));
    }
    return {};
}

future<> reader_concurrency_semaphore::enqueue_waiter(reader_permit permit, db::timeout_clock::time_point timeout, read_func func) {
    if (auto ex = check_queue_size("wait")) {
        return make_exception_future<>(std::move(ex));
    }
    promise<> pr;
    auto fut = pr.get_future();
    permit.on_waiting();
    _wait_list.push_back(entry(std::move(pr), std::move(permit), std::move(func)), timeout);
    return fut;
}

void reader_concurrency_semaphore::evict_readers_in_background() {
    // Evict inactive readers in the background while wait list isn't empty
    // This is safe since stop() closes _gate;
    (void)with_gate(_close_readers_gate, [this] {
        return do_until([this] { return _wait_list.empty() || _inactive_reads.empty(); }, [this] {
            return detach_inactive_reader(_inactive_reads.front(), evict_reason::permit).close();
        });
    });
 }

future<> reader_concurrency_semaphore::do_wait_admission(reader_permit permit, db::timeout_clock::time_point timeout, read_func func) {
    if (!_execution_loop_future) {
        _execution_loop_future.emplace(execution_loop());
    }
    auto first = _wait_list.empty() && _ready_list.empty();

    if (first && has_available_units(permit.base_resources())) {
        permit.on_admission();
        if (func) {
            return with_ready_permit(std::move(permit), std::move(func));
        }
        return make_ready_future<>();
    }

    auto fut = enqueue_waiter(std::move(permit), timeout, std::move(func));

    if (first && !_inactive_reads.empty()) {
        evict_readers_in_background();
    }

    return fut;
}

void reader_concurrency_semaphore::maybe_admit_waiters() noexcept {
    while (!_wait_list.empty() && _ready_list.empty() && has_available_units(_wait_list.front().permit.base_resources())) {
        auto& x = _wait_list.front();
        try {
            x.permit.on_admission();
            if (x.func) {
                _ready_list.push(std::move(x));
            } else {
                x.pr.set_value();
            }
        } catch (...) {
            x.pr.set_exception(std::current_exception());
        }
        _wait_list.pop_front();
    }
}

void reader_concurrency_semaphore::on_permit_created(reader_permit::impl& permit) {
    _permit_gate.enter();
    _permit_list->permits.push_back(permit);
    ++_permit_list->stats.total_permits;
    ++_permit_list->stats.current_permits;
}

void reader_concurrency_semaphore::on_permit_destroyed(reader_permit::impl& permit) noexcept {
    permit.unlink();
    _permit_gate.leave();
    --_permit_list->stats.current_permits;
}

reader_concurrency_semaphore::permit_stats reader_concurrency_semaphore::get_permit_stats() const {
    return _permit_list->stats;
}

void reader_concurrency_semaphore::on_permit_used() noexcept {
    ++_permit_list->stats.used_permits;
}

void reader_concurrency_semaphore::on_permit_unused() noexcept {
    assert(_permit_list->stats.used_permits);
    --_permit_list->stats.used_permits;
    assert(_permit_list->stats.used_permits >= _permit_list->stats.blocked_permits);
}

void reader_concurrency_semaphore::on_permit_blocked() noexcept {
    ++_permit_list->stats.blocked_permits;
    assert(_permit_list->stats.used_permits >= _permit_list->stats.blocked_permits);
}

void reader_concurrency_semaphore::on_permit_unblocked() noexcept {
    assert(_permit_list->stats.blocked_permits);
    --_permit_list->stats.blocked_permits;
}

reader_permit reader_concurrency_semaphore::make_permit(const schema* const schema, const char* const op_name) {
    return reader_permit(*this, schema, std::string_view(op_name), {});
}

reader_permit reader_concurrency_semaphore::make_permit(const schema* const schema, sstring&& op_name) {
    return reader_permit(*this, schema, std::move(op_name), {});
}

future<reader_permit> reader_concurrency_semaphore::obtain_permit(const schema* const schema, const char* const op_name, size_t memory,
        db::timeout_clock::time_point timeout) {
    auto permit = reader_permit(*this, schema, std::string_view(op_name), {1, static_cast<ssize_t>(memory)});
    return do_wait_admission(permit, timeout).then([permit] () mutable {
        return std::move(permit);
    });
}

future<reader_permit> reader_concurrency_semaphore::obtain_permit(const schema* const schema, sstring&& op_name, size_t memory,
        db::timeout_clock::time_point timeout) {
    auto permit = reader_permit(*this, schema, std::move(op_name), {1, static_cast<ssize_t>(memory)});
    return do_wait_admission(permit, timeout).then([permit] () mutable {
        return std::move(permit);
    });
}

future<reader_permit> reader_concurrency_semaphore::obtain_permit_nowait(const schema* const schema, const char* const op_name, size_t memory,
        db::timeout_clock::time_point timeout) {
    return make_ready_future<reader_permit>(reader_permit(*this, schema, std::string_view(op_name), {}));
}
future<reader_permit> reader_concurrency_semaphore::obtain_permit_nowait(const schema* const schema, sstring&& op_name, size_t memory,
        db::timeout_clock::time_point timeout) {
    return make_ready_future<reader_permit>(reader_permit(*this, schema, std::move(op_name), {}));
}

reader_permit reader_concurrency_semaphore::make_tracking_only_permit(const schema* const schema, const char* const op_name) {
    return reader_permit(*this, schema, std::string_view(op_name), {});
}

reader_permit reader_concurrency_semaphore::make_tracking_only_permit(const schema* const schema, sstring&& op_name) {
    return reader_permit(*this, schema, std::move(op_name), {});
}

future<> reader_concurrency_semaphore::with_permit(const schema* const schema, const char* const op_name, size_t memory,
        db::timeout_clock::time_point timeout, read_func func) {
    return do_wait_admission(reader_permit(*this, schema, std::string_view(op_name), {1, static_cast<ssize_t>(memory)}), timeout, std::move(func));
}

future<> reader_concurrency_semaphore::with_ready_permit(reader_permit permit, read_func func) {
    if (auto ex = check_queue_size("ready")) {
        return make_exception_future<>(std::move(ex));
    }
    promise<> pr;
    auto fut = pr.get_future();
    _ready_list.push(entry(std::move(pr), std::move(permit), std::move(func)));
    return fut;
}

void reader_concurrency_semaphore::broken(std::exception_ptr ex) {
    if (!ex) {
        ex = std::make_exception_ptr(broken_semaphore{});
    }
    while (!_wait_list.empty()) {
        _wait_list.front().pr.set_exception(ex);
        _wait_list.pop_front();
    }
}

std::string reader_concurrency_semaphore::dump_diagnostics(unsigned max_lines) const {
    std::ostringstream os;
    do_dump_reader_permit_diagnostics(os, *this, *_permit_list, "user request", max_lines);
    return os.str();
}

// A file that tracks the memory usage of buffers resulting from read
// operations.
class tracking_file_impl : public file_impl {
    file _tracked_file;
    reader_permit _permit;

public:
    tracking_file_impl(file file, reader_permit permit)
        : file_impl(*get_file_impl(file))
        , _tracked_file(std::move(file))
        , _permit(std::move(permit)) {
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
