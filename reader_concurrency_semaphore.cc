/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/core/seastar.hh>
#include <seastar/core/print.hh>
#include <seastar/core/file.hh>
#include <seastar/util/lazy.hh>
#include <seastar/util/log.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/core/metrics.hh>
#include <utility>

#include "reader_concurrency_semaphore.hh"
#include "query-result.hh"
#include "readers/mutation_reader.hh"
#include "utils/assert.hh"
#include "utils/exceptions.hh"
#include "schema/schema.hh"
#include "utils/human_readable.hh"
#include "utils/memory_limit_reached.hh"

logger rcslog("reader_concurrency_semaphore");

struct reader_concurrency_semaphore::inactive_read {
    mutation_reader reader;
    const dht::partition_range* range = nullptr;
    eviction_notify_handler notify_handler;
    timer<lowres_clock> ttl_timer;
    inactive_read_handle* handle = nullptr;

    explicit inactive_read(mutation_reader reader_, const dht::partition_range* range_) noexcept
        : reader(std::move(reader_))
        , range(range_)
    { }
    inactive_read(inactive_read&& o)
        : reader(std::move(o.reader))
        , range(o.range)
        , notify_handler(std::move(o.notify_handler))
        , ttl_timer(std::move(o.ttl_timer))
        , handle(o.handle)
    {
        o.handle = nullptr;
    }
    ~inactive_read() {
        detach();
    }
    void detach() noexcept {
        if (handle) {
            handle->_permit = {};
            handle = nullptr;
        }
    }
};

template <>
struct fmt::formatter<reader_concurrency_semaphore::evict_reason> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const reader_concurrency_semaphore::evict_reason& reason, FormatContext& ctx) const {
        static const char* value_table[] = {"permit", "time", "manual"};
        return fmt::format_to(ctx.out(), "{}", value_table[static_cast<int>(reason)]);
    }
};

namespace {

void maybe_dump_reader_permit_diagnostics(const reader_concurrency_semaphore& semaphore, std::string_view problem) noexcept;

}

auto fmt::formatter<reader_resources>::format(const reader_resources& r, fmt::format_context& ctx) const
        -> decltype(ctx.out()) {
    return fmt::format_to(ctx.out(), "{{{}, {}}}", r.count, r.memory);
}

reader_permit::resource_units::resource_units(reader_permit permit, reader_resources res, already_consumed_tag)
    : _permit(std::move(permit)), _resources(res) {
}

reader_permit::resource_units::resource_units(reader_permit permit, reader_resources res)
    : _permit(std::move(permit)) {
    _permit.consume(res);
    _resources = res;
}

reader_permit::resource_units::resource_units(resource_units&& o) noexcept
    : _permit(std::move(o._permit))
    , _resources(std::exchange(o._resources, {})) {
}

reader_permit::resource_units::~resource_units() {
    reset_to_zero();
}

reader_permit::resource_units& reader_permit::resource_units::operator=(resource_units&& o) noexcept {
    if (&o == this) {
        return *this;
    }
    reset_to_zero();
    _permit = std::move(o._permit);
    _resources = std::exchange(o._resources, {});
    return *this;
}

void reader_permit::resource_units::add(resource_units&& o) {
    SCYLLA_ASSERT(_permit == o._permit);
    _resources += std::exchange(o._resources, {});
}

void reader_permit::resource_units::reset_to(reader_resources res) {
    if (res == _resources) {
        return;
    }
    if (res.count < _resources.count && res.memory < _resources.memory) {
        _permit.signal(reader_resources{_resources.count - res.count, _resources.memory - res.memory});
        _resources = res;
        return;
    }

    if (res.non_zero()) {
        _permit.consume(res);
    }
    if (_resources.non_zero()) {
        _permit.signal(_resources);
    }
    _resources = res;
}

void reader_permit::resource_units::reset_to_zero() noexcept {
    if (_resources.non_zero()) {
        _permit.signal(_resources);
        _resources = {};
    }
}

class reader_permit::impl
        : public boost::intrusive::list_base_hook<boost::intrusive::link_mode<boost::intrusive::auto_unlink>>
        , public enable_shared_from_this<reader_permit::impl> {
public:
    struct auxiliary_data {
        promise<> pr;
        std::optional<shared_future<>> fut;
        reader_concurrency_semaphore::read_func func;
        // Self reference to keep the permit alive while queued for execution.
        // Must be cleared on all code-paths, otherwise it will keep the permit alive in perpetuity.
        reader_permit_opt permit_keepalive;
        std::optional<reader_concurrency_semaphore::inactive_read> ir;
    };

private:
    reader_concurrency_semaphore& _semaphore;
    schema_ptr _schema;

    sstring _op_name;
    std::string_view _op_name_view;
    reader_resources _base_resources;
    bool _base_resources_consumed = false;
    reader_resources _resources;
    reader_permit::state _state = reader_permit::state::active;
    uint64_t _need_cpu_branches = 0;
    bool _marked_as_need_cpu = false;
    uint64_t _awaits_branches = 0;
    bool _marked_as_awaits = false;
    std::exception_ptr _ex; // exception the permit was aborted with, nullptr if not aborted
    timer<db::timeout_clock> _ttl_timer;
    query::max_result_size _max_result_size{query::result_memory_limiter::unlimited_result_size};
    uint64_t _sstables_read = 0;
    size_t _requested_memory = 0;
    uint64_t _oom_kills = 0;
    tracing::trace_state_ptr _trace_ptr;

    // Not strictly related to the permit.
    // Used by the semaphore to to manage the permit.
    auxiliary_data _aux_data;

private:
    void on_permit_need_cpu() {
        _semaphore.on_permit_need_cpu();
        _marked_as_need_cpu = true;
    }
    void on_permit_not_need_cpu() {
        _semaphore.on_permit_not_need_cpu();
        _marked_as_need_cpu = false;
    }
    void on_permit_awaits() {
        _semaphore.on_permit_awaits();
        _marked_as_awaits = true;
    }
    void on_permit_not_awaits() {
        _semaphore.on_permit_not_awaits();
        _marked_as_awaits = false;
    }
    void on_permit_active() {
        if (_need_cpu_branches) {
            _state = reader_permit::state::active_need_cpu;
            on_permit_need_cpu();
            if (_awaits_branches) {
                _state = reader_permit::state::active_await;
                on_permit_awaits();
            }
        } else {
            _state = reader_permit::state::active;
        }
    }

    void on_permit_inactive(reader_permit::state st) {
        // If the permit is registered as inactive, while waiting for memory,
        // clear the memory amount, the requests are failed anyway.
        if (_state == reader_permit::state::waiting_for_memory) {
            _requested_memory = {};
        }
        _state = st;
        if (_marked_as_awaits) {
            on_permit_not_awaits();
        }
        if (_marked_as_need_cpu) {
            on_permit_not_need_cpu();
        }
    }

    void on_timeout() {
        auto keepalive = std::exchange(_aux_data.permit_keepalive, std::nullopt);

        _ex = std::make_exception_ptr(timed_out_error{});

        if (_state == state::waiting_for_admission || _state == state::waiting_for_memory || _state == state::waiting_for_execution) {
            _aux_data.pr.set_exception(named_semaphore_timed_out(_semaphore._name));

            maybe_dump_reader_permit_diagnostics(_semaphore, "timed out");

            _semaphore.dequeue_permit(*this);
        } else if (_state == state::inactive) {
            _semaphore.evict(*this, reader_concurrency_semaphore::evict_reason::time);
        }
    }

public:
    struct value_tag {};

    impl(reader_concurrency_semaphore& semaphore, schema_ptr schema, const std::string_view& op_name, reader_resources base_resources, db::timeout_clock::time_point timeout, tracing::trace_state_ptr trace_ptr)
        : _semaphore(semaphore)
        , _schema(std::move(schema))
        , _op_name_view(op_name)
        , _base_resources(base_resources)
        , _ttl_timer([this] { on_timeout(); })
        , _trace_ptr(std::move(trace_ptr))
    {
        set_timeout(timeout);
        _semaphore.on_permit_created(*this);
    }
    impl(reader_concurrency_semaphore& semaphore, schema_ptr schema, sstring&& op_name, reader_resources base_resources, db::timeout_clock::time_point timeout, tracing::trace_state_ptr trace_ptr)
        : _semaphore(semaphore)
        , _schema(std::move(schema))
        , _op_name(std::move(op_name))
        , _op_name_view(_op_name)
        , _base_resources(base_resources)
        , _ttl_timer([this] { on_timeout(); })
        , _trace_ptr(std::move(trace_ptr))
    {
        set_timeout(timeout);
        _semaphore.on_permit_created(*this);
    }
    ~impl() {
        if (_base_resources_consumed) {
            signal(_base_resources);
        }

        if (_resources.non_zero()) {
            on_internal_error_noexcept(rcslog, format("reader_permit::impl::~impl(): permit {} detected a leak of {{count={}, memory={}}} resources",
                        description(),
                        _resources.count,
                        _resources.memory));
            signal(_resources);
        }

        if (_need_cpu_branches) {
            on_internal_error_noexcept(rcslog, format("reader_permit::impl::~impl(): permit {}.{}:{} destroyed with {} need_cpu branches",
                        _schema ? _schema->ks_name() : "*",
                        _schema ? _schema->cf_name() : "*",
                        _op_name_view,
                        _need_cpu_branches));
            _semaphore.on_permit_not_need_cpu();
        }

        if (_awaits_branches) {
            on_internal_error_noexcept(rcslog, format("reader_permit::impl::~impl(): permit {}.{}:{} destroyed with {} awaits branches",
                        _schema ? _schema->ks_name() : "*",
                        _schema ? _schema->cf_name() : "*",
                        _op_name_view,
                        _awaits_branches));
            _semaphore.on_permit_not_awaits();
        }

        // Should probably make a scene here, but its not worth it.
        _semaphore._stats.sstables_read -= _sstables_read;
        _semaphore._stats.disk_reads -= bool(_sstables_read);

        _semaphore.on_permit_destroyed(*this);
    }

    reader_concurrency_semaphore& semaphore() {
        return _semaphore;
    }

    const schema_ptr& get_schema() const {
        return _schema;
    }

    std::string_view get_op_name() const {
        return _op_name_view;
    }

    reader_permit::state get_state() const {
        return _state;
    }

    auxiliary_data& aux_data() {
        return _aux_data;
    }

    void on_waiting_for_admission() {
        on_permit_inactive(reader_permit::state::waiting_for_admission);
    }

    void on_waiting_for_memory() {
        on_permit_inactive(reader_permit::state::waiting_for_memory);
    }

    void on_waiting_for_execution() {
        on_permit_inactive(reader_permit::state::waiting_for_execution);
    }

    void on_admission() {
        SCYLLA_ASSERT(_state != reader_permit::state::active_await);
        on_permit_active();
        consume(_base_resources);
        _base_resources_consumed = true;
    }

    void on_granted_memory() {
        if (_state == reader_permit::state::waiting_for_memory) {
            on_permit_active();
        }
        consume({0, std::exchange(_requested_memory, 0)});
    }

    void on_executing() {
        on_permit_active();
    }

    void on_register_as_inactive() {
        SCYLLA_ASSERT(_state == reader_permit::state::active || _state == reader_permit::state::active_need_cpu || _state == reader_permit::state::waiting_for_memory);
        on_permit_inactive(reader_permit::state::inactive);
    }

    void on_unregister_as_inactive() {
        SCYLLA_ASSERT(_state == reader_permit::state::inactive);
        on_permit_active();
    }

    void on_evicted() {
        SCYLLA_ASSERT(_state == reader_permit::state::inactive);
        _state = reader_permit::state::evicted;
        if (_base_resources_consumed) {
            signal(_base_resources);
            _base_resources_consumed = false;
        }
    }

    void consume(reader_resources res) {
        _semaphore.consume(*this, res);
        _resources += res;
    }

    void signal(reader_resources res) {
        _resources -= res;
        _semaphore.signal(res);
    }

    future<resource_units> request_memory(size_t memory) {
        _requested_memory += memory;
        return _semaphore.request_memory(*this, memory).then([this, memory] {
            return resource_units(reader_permit(shared_from_this()), {0, ssize_t(memory)}, resource_units::already_consumed_tag{});
        });
    }

    reader_resources resources() const {
        return _resources;
    }

    reader_resources base_resources() const {
        return _base_resources;
    }

    void release_base_resources() noexcept {
        if (_base_resources_consumed) {
            _resources -= _base_resources;
            _base_resources_consumed = false;
        }
        _semaphore.signal(std::exchange(_base_resources, {}));
    }

    sstring description() const {
        return format("{}.{}:{}",
                _schema ? _schema->ks_name() : "*",
                _schema ? _schema->cf_name() : "*",
                _op_name_view);
    }

    void mark_need_cpu() noexcept {
        ++_need_cpu_branches;
        if (!_marked_as_need_cpu && _state == reader_permit::state::active) {
            _state = reader_permit::state::active_need_cpu;
            on_permit_need_cpu();
            if (_awaits_branches && !_marked_as_awaits) {
                _state = reader_permit::state::active_await;
                on_permit_awaits();
            }
        }
    }

    void mark_not_need_cpu() noexcept {
        SCYLLA_ASSERT(_need_cpu_branches);
        --_need_cpu_branches;
        if (_marked_as_need_cpu && !_need_cpu_branches) {
            // When an exception is thrown, need_cpu and awaits guards might be
            // destroyed out-of-order. Force the state out of awaits state here
            // so that we maintain awaits >= need_cpu.
            if (_marked_as_awaits) {
                on_permit_not_awaits();
            }
            _state = reader_permit::state::active;
            on_permit_not_need_cpu();
        }
    }

    void mark_awaits() noexcept {
        ++_awaits_branches;
        if (_awaits_branches == 1 && _state == reader_permit::state::active_need_cpu) {
            _state = reader_permit::state::active_await;
            on_permit_awaits();
        }
    }

    void mark_not_awaits() noexcept {
        SCYLLA_ASSERT(_awaits_branches);
        --_awaits_branches;
        if (_marked_as_awaits && !_awaits_branches) {
            _state = reader_permit::state::active_need_cpu;
            on_permit_not_awaits();
        }
    }

    bool needs_readmission() const {
        return _state == reader_permit::state::evicted;
    }

    future<> wait_readmission() {
        return _semaphore.do_wait_admission(*this);
    }

    db::timeout_clock::time_point timeout() const noexcept {
        return _ttl_timer.armed() ? _ttl_timer.get_timeout() : db::no_timeout;
    }

    void set_timeout(db::timeout_clock::time_point timeout) noexcept {
        _ttl_timer.cancel();
        if (timeout != db::no_timeout) {
            _ttl_timer.arm(timeout);
        }
    }

    const tracing::trace_state_ptr& trace_state() const noexcept {
        return _trace_ptr;
    }

    void set_trace_state(tracing::trace_state_ptr trace_ptr) noexcept {
        if (_trace_ptr) {
            // Create a continuation trace point
            tracing::trace(trace_ptr, "Continuing paged query, previous page's trace session is {}", _trace_ptr->session_id());
        }
        _trace_ptr = std::move(trace_ptr);
    }

    void check_abort() {
        if (_ex) {
            std::rethrow_exception(_ex);
        }
    }

    query::max_result_size max_result_size() const {
        return _max_result_size;
    }

    void set_max_result_size(query::max_result_size s) {
        _max_result_size = std::move(s);
    }

    void on_start_sstable_read() noexcept {
        if (!_sstables_read) {
            ++_semaphore._stats.disk_reads;
        }
        ++_sstables_read;
        ++_semaphore._stats.sstables_read;
    }

    void on_finish_sstable_read() noexcept {
        --_sstables_read;
        --_semaphore._stats.sstables_read;
        if (!_sstables_read) {
            --_semaphore._stats.disk_reads;
        }
    }

    bool on_oom_kill() noexcept {
        return !bool(_oom_kills++);
    }
};

static_assert(std::is_nothrow_copy_constructible_v<reader_permit>);
static_assert(std::is_nothrow_move_constructible_v<reader_permit>);

reader_permit::reader_permit(shared_ptr<impl> impl) : _impl(std::move(impl))
{
}

reader_permit::reader_permit(reader_concurrency_semaphore& semaphore, schema_ptr schema, std::string_view op_name,
        reader_resources base_resources, db::timeout_clock::time_point timeout, tracing::trace_state_ptr trace_ptr)
    : _impl(::seastar::make_shared<reader_permit::impl>(semaphore, std::move(schema), op_name, base_resources, timeout, std::move(trace_ptr)))
{
}

reader_permit::reader_permit(reader_concurrency_semaphore& semaphore, schema_ptr schema, sstring&& op_name,
        reader_resources base_resources, db::timeout_clock::time_point timeout, tracing::trace_state_ptr trace_ptr)
    : _impl(::seastar::make_shared<reader_permit::impl>(semaphore, std::move(schema), std::move(op_name), base_resources, timeout, std::move(trace_ptr)))
{
}

reader_permit::~reader_permit() {
}

reader_concurrency_semaphore& reader_permit::semaphore() {
    return _impl->semaphore();
}

const schema_ptr& reader_permit::get_schema() const {
    return _impl->get_schema();
}

std::string_view reader_permit::get_op_name() const {
    return _impl->get_op_name();
}

reader_permit::state reader_permit::get_state() const {
    return _impl->get_state();
}

bool reader_permit::needs_readmission() const {
    return _impl->needs_readmission();
}

future<> reader_permit::wait_readmission() {
    return _impl->wait_readmission();
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

future<reader_permit::resource_units> reader_permit::request_memory(size_t memory) {
    return _impl->request_memory(memory);
}

reader_resources reader_permit::consumed_resources() const {
    return _impl->resources();
}

reader_resources reader_permit::base_resources() const {
    return _impl->base_resources();
}

void reader_permit::release_base_resources() noexcept {
    return _impl->release_base_resources();
}

sstring reader_permit::description() const {
    return _impl->description();
}

void reader_permit::mark_need_cpu() noexcept {
    _impl->mark_need_cpu();
}

void reader_permit::mark_not_need_cpu() noexcept {
    _impl->mark_not_need_cpu();
}

void reader_permit::mark_awaits() noexcept {
    _impl->mark_awaits();
}

void reader_permit::mark_not_awaits() noexcept {
    _impl->mark_not_awaits();
}

db::timeout_clock::time_point reader_permit::timeout() const noexcept {
    return _impl->timeout();
}

void reader_permit::set_timeout(db::timeout_clock::time_point timeout) noexcept {
    _impl->set_timeout(timeout);
}

const tracing::trace_state_ptr& reader_permit::trace_state() const noexcept {
    return _impl->trace_state();
}

void reader_permit::set_trace_state(tracing::trace_state_ptr trace_ptr) noexcept {
    _impl->set_trace_state(std::move(trace_ptr));
}

void reader_permit::check_abort() {
    return _impl->check_abort();
}

query::max_result_size reader_permit::max_result_size() const {
    return _impl->max_result_size();
}

void reader_permit::set_max_result_size(query::max_result_size s) {
    _impl->set_max_result_size(std::move(s));
}

void reader_permit::on_start_sstable_read() noexcept {
    _impl->on_start_sstable_read();
}

void reader_permit::on_finish_sstable_read() noexcept {
    _impl->on_finish_sstable_read();
}

auto fmt::formatter<reader_permit::state>::format(reader_permit::state s, fmt::format_context& ctx) const
        -> decltype(ctx.out()) {
    std::string_view name;
    switch (s) {
        case reader_permit::state::waiting_for_admission:
            name = "waiting_for_admission";
            break;
        case reader_permit::state::waiting_for_memory:
            name = "waiting_for_memory";
            break;
        case reader_permit::state::waiting_for_execution:
            name = "waiting_for_execution";
            break;
        case reader_permit::state::active:
            name = "active";
            break;
        case reader_permit::state::active_need_cpu:
            name = "active/need_cpu";
            break;
        case reader_permit::state::active_await:
            name = "active/await";
            break;
        case reader_permit::state::inactive:
            name= "inactive";
            break;
        case reader_permit::state::evicted:
            name = "evicted";
            break;
    }
    return formatter<string_view>::format(name, ctx);
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

    print_line("permits", "count", "memory", "table/operation/state");
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

static void do_dump_reader_permit_diagnostics(std::ostream& os, const reader_concurrency_semaphore& semaphore, std::string_view problem, unsigned max_lines = 20) {
    permit_groups permits;

    semaphore.foreach_permit([&] (const reader_permit::impl& permit) {
        permits[permit_group_key(permit.get_schema().get(), permit.get_op_name(), permit.get_state())].add(permit);
    });

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
    const auto& stats = semaphore.get_stats();
    fmt::print(os, "Stats:\n"
            "permit_based_evictions: {}\n"
            "time_based_evictions: {}\n"
            "inactive_reads: {}\n"
            "total_successful_reads: {}\n"
            "total_failed_reads: {}\n"
            "total_reads_shed_due_to_overload: {}\n"
            "total_reads_killed_due_to_kill_limit: {}\n"
            "reads_admitted: {}\n"
            "reads_enqueued_for_admission: {}\n"
            "reads_enqueued_for_memory: {}\n"
            "reads_admitted_immediately: {}\n"
            "reads_queued_because_ready_list: {}\n"
            "reads_queued_because_need_cpu_permits: {}\n"
            "reads_queued_because_memory_resources: {}\n"
            "reads_queued_because_count_resources: {}\n"
            "reads_queued_with_eviction: {}\n"
            "total_permits: {}\n"
            "current_permits: {}\n"
            "need_cpu_permits: {}\n"
            "awaits_permits: {}\n"
            "disk_reads: {}\n"
            "sstables_read: {}",
            stats.permit_based_evictions,
            stats.time_based_evictions,
            stats.inactive_reads,
            stats.total_successful_reads,
            stats.total_failed_reads,
            stats.total_reads_shed_due_to_overload,
            stats.total_reads_killed_due_to_kill_limit,
            stats.reads_admitted,
            stats.reads_enqueued_for_admission,
            stats.reads_enqueued_for_memory,
            stats.reads_admitted_immediately,
            stats.reads_queued_because_ready_list,
            stats.reads_queued_because_need_cpu_permits,
            stats.reads_queued_because_memory_resources,
            stats.reads_queued_because_count_resources,
            stats.reads_queued_with_eviction,
            stats.total_permits,
            stats.current_permits,
            stats.need_cpu_permits,
            stats.awaits_permits,
            stats.disk_reads,
            stats.sstables_read);
}

void maybe_dump_reader_permit_diagnostics(const reader_concurrency_semaphore& semaphore, std::string_view problem) noexcept {
    static thread_local logger::rate_limit rate_limit(std::chrono::seconds(30));

    rcslog.log(log_level::info, rate_limit, "{}", value_of([&] {
        std::ostringstream os;
        do_dump_reader_permit_diagnostics(os, semaphore, problem);
        return std::move(os).str();
    }));
}

} // anonymous namespace

void reader_concurrency_semaphore::inactive_read_handle::abandon() noexcept {
    if (_permit) {
        auto& permit = **_permit;
        auto& sem = permit.semaphore();
        sem.close_reader(std::move(permit.aux_data().ir->reader));
        sem.dequeue_permit(permit);
        // Break the handle <-> inactive read connection, to prevent the inactive
        // read attempting to detach(). Not only is that unnecessary (the handle
        // is abandoning the inactive read), but detach() will reset _permit,
        // which might be the last permit instance alive. Destroying it could
        // yank out *this from under our feet.
        permit.aux_data().ir->handle = nullptr;
        permit.aux_data().ir.reset();
    }
}

reader_concurrency_semaphore::inactive_read_handle::inactive_read_handle(reader_permit permit) noexcept
    : _permit(permit) {
    (*_permit)->aux_data().ir->handle = this;
}

reader_concurrency_semaphore::inactive_read_handle::inactive_read_handle(inactive_read_handle&& o) noexcept
    : _permit(std::exchange(o._permit, std::nullopt)) {
    if (_permit) {
        (*_permit)->aux_data().ir->handle = this;
    }
}

reader_concurrency_semaphore::inactive_read_handle&
reader_concurrency_semaphore::inactive_read_handle::operator=(inactive_read_handle&& o) noexcept {
    if (this == &o) {
        return *this;
    }
    abandon();
    _permit = std::exchange(o._permit, std::nullopt);
    if (_permit) {
        (*_permit)->aux_data().ir->handle = this;
    }
    return *this;
}

void reader_concurrency_semaphore::wait_queue::push_to_admission_queue(reader_permit::impl& p) {
    p.unlink();
    _admission_queue.push_back(p);
}

void reader_concurrency_semaphore::wait_queue::push_to_memory_queue(reader_permit::impl& p) {
    p.unlink();
    _memory_queue.push_back(p);
}

reader_permit::impl& reader_concurrency_semaphore::wait_queue::front() {
    if (_memory_queue.empty()) {
        return _admission_queue.front();
    } else {
        return _memory_queue.front();
    }
}

const reader_permit::impl& reader_concurrency_semaphore::wait_queue::front() const {
    return const_cast<wait_queue&>(*this).front();
}

namespace {

struct stop_execution_loop {
};

}

future<> reader_concurrency_semaphore::execution_loop() noexcept {
    while (true) {
        try {
            co_await _ready_list_cv.when();
        } catch (stop_execution_loop) {
            co_return;
        }

        while (!_ready_list.empty()) {
            auto& permit = _ready_list.front();
            dequeue_permit(permit);
            permit.on_executing();
            auto e = std::move(permit.aux_data());

            tracing::trace(permit.trace_state(), "[reader concurrency semaphore {}] executing read", _name);

            try {
                e.func(reader_permit(permit.shared_from_this())).forward_to(std::move(e.pr));
            } catch (...) {
                e.pr.set_exception(std::current_exception());
            }

            // We now possibly have >= CPU concurrency, so even if the above read
            // didn't release any resources, just dequeueing it from the
            // _ready_list could allow us to admit new reads.
            maybe_admit_waiters();

            if (need_preempt()) {
                co_await coroutine::maybe_yield();
            }
        }
    }
}

uint64_t reader_concurrency_semaphore::get_serialize_limit() const {
    if (!_serialize_limit_multiplier() || _serialize_limit_multiplier() == std::numeric_limits<uint32_t>::max() || is_unlimited()) [[unlikely]] {
        return std::numeric_limits<uint64_t>::max();
    }
    return _initial_resources.memory * _serialize_limit_multiplier();
}

uint64_t reader_concurrency_semaphore::get_kill_limit() const {
    if (!_kill_limit_multiplier() || _kill_limit_multiplier() == std::numeric_limits<uint32_t>::max() || is_unlimited()) [[unlikely]] {
        return std::numeric_limits<uint64_t>::max();
    }
    return _initial_resources.memory * _kill_limit_multiplier();
}

void reader_concurrency_semaphore::consume(reader_permit::impl& permit, resources r) {
    // We check whether we even reached the memory limit first.
    // This is a cheap check and should be false most of the time, providing a
    // cheap short-circuit.
    if (_resources.memory <= 0 && std::cmp_greater_equal(consumed_resources().memory + r.memory, get_kill_limit())) [[unlikely]] {
        if (permit.on_oom_kill()) {
            ++_stats.total_reads_killed_due_to_kill_limit;
        }
        maybe_dump_reader_permit_diagnostics(*this, "kill limit triggered");
        throw utils::memory_limit_reached(format("kill limit triggered on semaphore {} by permit {}", _name, permit.description()));
    }
    _resources -= r;
}

void reader_concurrency_semaphore::signal(const resources& r) noexcept {
    _resources += r;
    maybe_admit_waiters();
}

namespace sm = seastar::metrics;
static const sm::label class_label("class");

reader_concurrency_semaphore::reader_concurrency_semaphore(
        utils::updateable_value<int> count,
        ssize_t memory,
        sstring name,
        size_t max_queue_length,
        utils::updateable_value<uint32_t> serialize_limit_multiplier,
        utils::updateable_value<uint32_t> kill_limit_multiplier,
        utils::updateable_value<uint32_t> cpu_concurrency,
        register_metrics metrics)
    : _initial_resources(count, memory)
    , _resources(count, memory)
    , _count_observer(count.observe([this] (const int& new_count) { set_resources({new_count, _initial_resources.memory}); }))
    , _name(std::move(name))
    , _max_queue_length(max_queue_length)
    , _serialize_limit_multiplier(std::move(serialize_limit_multiplier))
    , _kill_limit_multiplier(std::move(kill_limit_multiplier))
    , _cpu_concurrency(cpu_concurrency)
{
    if (metrics == register_metrics::yes) {
        _metrics.emplace();
        _metrics->add_group("database", {
                sm::make_counter("sstable_read_queue_overloads", _stats.total_reads_shed_due_to_overload,
                               sm::description("Counts the number of times the sstable read queue was overloaded. "
                                               "A non-zero value indicates that we have to drop read requests because they arrive faster than we can serve them."),
                               {class_label(_name)}),

                sm::make_gauge("active_reads", [this] { return active_reads(); },
                               sm::description("Holds the number of currently active read operations. "),
                               {class_label(_name)}),

                sm::make_gauge("reads_memory_consumption", [this] { return consumed_resources().memory; },
                               sm::description("Holds the amount of memory consumed by current read operations. "),
                               {class_label(_name)}),

                sm::make_gauge("queued_reads", _stats.waiters,
                               sm::description("Holds the number of currently queued read operations."),
                               {class_label(_name)}),

                sm::make_gauge("paused_reads", _stats.inactive_reads,
                               sm::description("The number of currently active reads that are temporarily paused."),
                               {class_label(_name)}),

                sm::make_counter("paused_reads_permit_based_evictions", _stats.permit_based_evictions,
                               sm::description("The number of paused reads evicted to free up permits."
                                               " Permits are required for new reads to start, and the database will evict paused reads (if any)"
                                               " to be able to admit new ones, if there is a shortage of permits."),
                               {class_label(_name)}),

                sm::make_counter("reads_shed_due_to_overload", _stats.total_reads_shed_due_to_overload,
                               sm::description("The number of reads shed because the admission queue reached its max capacity."
                                               " When the queue is full, excessive reads are shed to avoid overload."),
                               {class_label(_name)}),

                sm::make_gauge("disk_reads", _stats.disk_reads,
                               sm::description("Holds the number of currently active disk read operations. "),
                               {class_label(_name)}),

                sm::make_gauge("sstables_read", _stats.sstables_read,
                               sm::description("Holds the number of currently read sstables. "),
                               {class_label(_name)}),

                sm::make_counter("total_reads", _stats.total_successful_reads,
                               sm::description("Counts the total number of successful user reads on this shard."),
                               {class_label(_name)}),

                sm::make_counter("total_reads_failed", _stats.total_failed_reads,
                               sm::description("Counts the total number of failed user read operations. "
                                               "Add the total_reads to this value to get the total amount of reads issued on this shard."),
                               {class_label(_name)}),
                });
    }
}

reader_concurrency_semaphore::reader_concurrency_semaphore(no_limits, sstring name, register_metrics metrics)
    : reader_concurrency_semaphore(
            utils::updateable_value(std::numeric_limits<int>::max()),
            std::numeric_limits<ssize_t>::max(),
            std::move(name),
            std::numeric_limits<size_t>::max(),
            utils::updateable_value(std::numeric_limits<uint32_t>::max()),
            utils::updateable_value(std::numeric_limits<uint32_t>::max()),
            utils::updateable_value(uint32_t(1)),
            metrics) {}

reader_concurrency_semaphore::~reader_concurrency_semaphore() {
    SCYLLA_ASSERT(!_stats.waiters);
    if (!_stats.total_permits) {
        // We allow destroy without stop() when the semaphore wasn't used at all yet.
        return;
    }
    if (!_stopped) {
        on_internal_error_noexcept(rcslog, format("~reader_concurrency_semaphore(): semaphore {} not stopped before destruction", _name));
        // With the below conditions, we can get away with the semaphore being
        // unstopped. In this case don't force an abort.
        SCYLLA_ASSERT(_inactive_reads.empty() && !_close_readers_gate.get_count() && !_permit_gate.get_count() && !_execution_loop_future);
        broken();
    }
}

reader_concurrency_semaphore::inactive_read_handle reader_concurrency_semaphore::register_inactive_read(mutation_reader reader,
        const dht::partition_range* range) noexcept {
    auto& permit = reader.permit();
    if (permit->get_state() == reader_permit::state::waiting_for_memory) {
        // Kill all outstanding memory requests, the read is going to be evicted.
        permit->aux_data().pr.set_exception(std::make_exception_ptr(std::bad_alloc{}));
        dequeue_permit(*permit);
    }
    permit->on_register_as_inactive();
    if (_blessed_permit == &*permit) {
        _blessed_permit = nullptr;
        maybe_admit_waiters();
    }
    if (!should_evict_inactive_read()) {
      try {
        permit->aux_data().ir.emplace(std::move(reader), range);
        permit->unlink();
        _inactive_reads.push_back(*permit);
        ++_stats.inactive_reads;
        return inactive_read_handle(permit);
      } catch (...) {
        // It is okay to swallow the exception since
        // we're allowed to drop the reader upon registration
        // due to lack of resources. Returning an empty
        // i_r_h here rather than throwing simplifies the caller's
        // error handling.
        rcslog.warn("Registering inactive read failed: {}. Ignored as if it was evicted.", std::current_exception());
      }
    } else {
        permit->on_evicted();
        ++_stats.permit_based_evictions;
    }
    close_reader(std::move(reader));
    return inactive_read_handle();
}

void reader_concurrency_semaphore::set_notify_handler(inactive_read_handle& irh, eviction_notify_handler&& notify_handler, std::optional<std::chrono::seconds> ttl_opt) {
    auto& ir = *(*irh._permit)->aux_data().ir;
    ir.notify_handler = std::move(notify_handler);
    if (ttl_opt) {
        ir.ttl_timer.set_callback([this, permit = *irh._permit] () mutable {
            evict(*permit, evict_reason::time);
        });
        ir.ttl_timer.arm(lowres_clock::now() + *ttl_opt);
    }
}

mutation_reader_opt reader_concurrency_semaphore::unregister_inactive_read(inactive_read_handle irh) {
    if (!irh) {
        return {};
    }
    auto& permit = **irh._permit;
    auto irp = std::move(permit.aux_data().ir);

    if (&permit.semaphore() != this) {
        // unregister from the other semaphore
        // and close the reader, in case on_internal_error
        // doesn't abort.
        auto& sem = permit.semaphore();
        sem.close_reader(std::move(irp->reader));
        on_internal_error(rcslog, fmt::format(
                    "reader_concurrency_semaphore::unregister_inactive_read(): "
                    "attempted to unregister an inactive read with a handle belonging to another semaphore: "
                    "this is {} (0x{:x}) but the handle belongs to {} (0x{:x})",
                    name(),
                    reinterpret_cast<uintptr_t>(this),
                    sem.name(),
                    reinterpret_cast<uintptr_t>(&sem)));
    }

    dequeue_permit(permit);
    permit.on_unregister_as_inactive();
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
        evict(_inactive_reads.front(), evict_reason::manual);
    }
}

future<> reader_concurrency_semaphore::evict_inactive_reads_for_table(table_id id, const dht::partition_range* range) noexcept {
    auto overlaps_with_range = [range] (const reader_concurrency_semaphore::inactive_read& ir) {
        if (!range || !ir.range) {
            return true;
        }
        return ir.range->overlaps(*range, dht::ring_position_comparator(*ir.reader.schema()));
    };

    permit_list_type evicted_readers;
    auto it = _inactive_reads.begin();
    while (it != _inactive_reads.end()) {
        auto& permit = *it;
        auto& ir = *permit.aux_data().ir;
        ++it;
        if (ir.reader.schema()->id() == id && overlaps_with_range(ir)) {
            do_detach_inactive_reader(permit, evict_reason::manual);
            permit.unlink();
            evicted_readers.push_back(permit);
        }
    }
    while (!evicted_readers.empty()) {
        auto& permit = evicted_readers.front();
        auto irp = std::move(permit.aux_data().ir);
        permit.unlink();
        _permit_list.push_back(permit);
        // Closing the reader might destroy the last permit instance, killing the
        // permit itself, so this close has to be last in this scope.
        co_await irp->reader.close();
    }
}

std::runtime_error reader_concurrency_semaphore::stopped_exception() {
    return std::runtime_error(format("{} was stopped", _name));
}

future<> reader_concurrency_semaphore::stop() noexcept {
    SCYLLA_ASSERT(!_stopped);
    _stopped = true;
    co_await stop_ext_pre();
    clear_inactive_reads();
    co_await _permit_gate.close();
    // Gate for closing readers is only closed after waiting for all reads, as the evictable
    // readers might take the inactive registration path and find the gate closed.
    co_await _close_readers_gate.close();
    _ready_list_cv.broken(std::make_exception_ptr(stop_execution_loop{}));
    if (_execution_loop_future) {
        co_await std::move(*_execution_loop_future);
    }
    broken(std::make_exception_ptr(stopped_exception()));
    co_await stop_ext_post();
    co_return;
}

void reader_concurrency_semaphore::do_detach_inactive_reader(reader_permit::impl& permit, evict_reason reason) noexcept {
    dequeue_permit(permit);
    auto& ir = *permit.aux_data().ir;
    ir.ttl_timer.cancel();
    ir.detach();
    ir.reader.permit()->on_evicted();
    tracing::trace(permit.trace_state(), "[reader_concurrency_semaphore {}] evicted, reason: {}", _name, reason);
    try {
        if (ir.notify_handler) {
            ir.notify_handler(reason);
        }
    } catch (...) {
        rcslog.error("[semaphore {}] evict(): notify handler failed for inactive read evicted due to {}: {}", _name, static_cast<int>(reason), std::current_exception());
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
}

mutation_reader reader_concurrency_semaphore::detach_inactive_reader(reader_permit::impl& permit, evict_reason reason) noexcept {
    do_detach_inactive_reader(permit, reason);
    auto irp = std::move(permit.aux_data().ir);
    return std::move(irp->reader);
}

void reader_concurrency_semaphore::evict(reader_permit::impl& permit, evict_reason reason) noexcept {
    close_reader(detach_inactive_reader(permit, reason));
}

void reader_concurrency_semaphore::close_reader(mutation_reader reader) {
    // It is safe to discard the future since it is waited on indirectly
    // by closing the _close_readers_gate in stop().
    (void)with_gate(_close_readers_gate, [reader = std::move(reader)] () mutable {
        return reader.close();
    });
}

bool reader_concurrency_semaphore::has_available_units(const resources& r) const {
    // Special case: when there is no active reader (based on count) admit one
    // regardless of availability of memory.
    return (_resources.non_zero() && _resources.count >= r.count && _resources.memory >= r.memory) || _resources.count == _initial_resources.count;
}

bool reader_concurrency_semaphore::cpu_concurrency_limit_reached() const {
    return (_stats.need_cpu_permits - _stats.awaits_permits) >= _cpu_concurrency();
}

std::exception_ptr reader_concurrency_semaphore::check_queue_size(std::string_view queue_name) {
    if (_stats.waiters >= _max_queue_length) {
        _stats.total_reads_shed_due_to_overload++;
        maybe_dump_reader_permit_diagnostics(*this, fmt::format("{} queue overload", queue_name));
        return std::make_exception_ptr(std::runtime_error(format("{}: {} queue overload", _name, queue_name)));
    }
    return {};
}

future<> reader_concurrency_semaphore::enqueue_waiter(reader_permit::impl& permit, wait_on wait) {
    if (auto ex = check_queue_size("wait")) {
        return make_exception_future<>(std::move(ex));
    }
    auto& ad = permit.aux_data();
    ad.pr = {};
    auto fut = ad.pr.get_future();
    if (wait == wait_on::admission) {
        permit.on_waiting_for_admission();
        _wait_list.push_to_admission_queue(permit);
        ++_stats.reads_enqueued_for_admission;
    } else {
        permit.on_waiting_for_memory();
        ad.fut.emplace(std::move(fut));
        fut = ad.fut->get_future();
        _wait_list.push_to_memory_queue(permit);
        ++_stats.reads_enqueued_for_memory;
    }
    ++_stats.waiters;
    return fut;
}

void reader_concurrency_semaphore::evict_readers_in_background() {
    if (_evicting) {
        return;
    }
    _evicting = true;
    // Evict inactive readers in the background while wait list isn't empty
    // This is safe since stop() closes _gate;
    (void)with_gate(_close_readers_gate, [this] {
        return repeat([this] {
            if (_inactive_reads.empty() || !should_evict_inactive_read()) {
                _evicting = false;
                return make_ready_future<stop_iteration>(stop_iteration::yes);
            }
            return detach_inactive_reader(_inactive_reads.front(), evict_reason::permit).close().then([] {
                return stop_iteration::no;
            });
        });
    });
}

reader_concurrency_semaphore::admit_result
reader_concurrency_semaphore::can_admit_read(const reader_permit::impl& permit) const noexcept {
    if (_resources.memory < 0) [[unlikely]] {
        const auto consumed_memory = consumed_resources().memory;
        if (std::cmp_greater_equal(consumed_memory, get_kill_limit())) {
            return {can_admit::no, reason::memory_resources};
        }
        if (std::cmp_greater_equal(consumed_memory, get_serialize_limit())) {
            if (_blessed_permit) {
                // blessed permit is never in the wait list
                return {can_admit::no, reason::memory_resources};
            } else {
                if (permit.get_state() == reader_permit::state::waiting_for_memory) {
                    return {can_admit::yes, reason::all_ok};
                } else {
                    return {can_admit::no, reason::memory_resources};
                }
            }
        }
    }

    if (permit.get_state() == reader_permit::state::waiting_for_memory) {
        return {can_admit::yes, reason::all_ok};
    }

    if (!_ready_list.empty()) {
        return {can_admit::no, reason::ready_list};
    }

    if (cpu_concurrency_limit_reached()) {
        return {can_admit::no, reason::need_cpu_permits};
    }

    if (!has_available_units(permit.base_resources())) {
        auto reason = _resources.memory >= permit.base_resources().memory ? reason::memory_resources : reason::count_resources;
        if (_inactive_reads.empty()) {
            return {can_admit::no, reason};
        } else {
            return {can_admit::maybe, reason};
        }
    }

    return {can_admit::yes, reason::all_ok};
}

bool reader_concurrency_semaphore::should_evict_inactive_read() const noexcept {
    if (_resources.memory < 0 || _resources.count < 0) {
        return true;
    }
    if (_wait_list.empty()) {
        return false;
    }
    const auto r = can_admit_read(_wait_list.front()).why;
    return r == reason::memory_resources || r == reason::count_resources;
}

future<> reader_concurrency_semaphore::do_wait_admission(reader_permit::impl& permit) {
    if (!_execution_loop_future) {
        _execution_loop_future.emplace(execution_loop());
    }

    static uint64_t stats::*stats_table[] = {
        &stats::reads_admitted_immediately,
        &stats::reads_queued_because_ready_list,
        &stats::reads_queued_because_need_cpu_permits,
        &stats::reads_queued_because_memory_resources,
        &stats::reads_queued_because_count_resources
    };

    static const char* result_as_string[] = {
        "admitted immediately",
        "queued because of non-empty ready list",
        "queued because of need_cpu permits",
        "queued because of memory resources",
        "queued because of count resources"
    };

    const auto [admit, why] = can_admit_read(permit);
    ++(_stats.*stats_table[static_cast<int>(why)]);
    tracing::trace(permit.trace_state(), "[reader concurrency semaphore {}] {}", _name, result_as_string[static_cast<int>(why)]);
    if (admit != can_admit::yes || !_wait_list.empty()) {
        auto fut = enqueue_waiter(permit, wait_on::admission);
        if (admit == can_admit::yes && !_wait_list.empty()) {
            // This is a contradiction: the semaphore could admit waiters yet it has waiters.
            // Normally, the semaphore should admit waiters as soon as it can.
            // So at any point in time, there should either be no waiters, or it
            // shouldn't be able to admit new reads. Otherwise something went wrong.
            maybe_dump_reader_permit_diagnostics(*this, "semaphore could admit new reads yet there are waiters");
            maybe_admit_waiters();
        } else if (admit == can_admit::maybe) {
            tracing::trace(permit.trace_state(), "[reader concurrency semaphore {}] evicting inactive reads in the background to free up resources", _name);
            ++_stats.reads_queued_with_eviction;
            evict_readers_in_background();
        }
        return fut;
    }

    permit.on_admission();
    ++_stats.reads_admitted;
    if (permit.aux_data().func) {
        return with_ready_permit(permit);
    }
    return make_ready_future<>();
}

void reader_concurrency_semaphore::maybe_admit_waiters() noexcept {
    auto admit = can_admit::no;
    while (!_wait_list.empty() && (admit = can_admit_read(_wait_list.front()).decision) == can_admit::yes) {
        auto& permit = _wait_list.front();
        dequeue_permit(permit);
        try {
            if (permit.get_state() == reader_permit::state::waiting_for_memory) {
                _blessed_permit = &permit;
                permit.on_granted_memory();
            } else {
                permit.on_admission();
                ++_stats.reads_admitted;
            }
            if (permit.aux_data().func) {
                permit.unlink();
                _ready_list.push_back(permit);
                _ready_list_cv.signal();
                permit.on_waiting_for_execution();
                ++_stats.waiters;
            } else {
                permit.aux_data().pr.set_value();
            }
        } catch (...) {
            permit.aux_data().pr.set_exception(std::current_exception());
        }
    }
    if (admit == can_admit::maybe) {
        // Evicting readers will trigger another call to `maybe_admit_waiters()` from `signal()`.
        evict_readers_in_background();
    }
}

future<> reader_concurrency_semaphore::request_memory(reader_permit::impl& permit, size_t memory) {
    // Already blocked on memory?
    if (permit.get_state() == reader_permit::state::waiting_for_memory) {
        return permit.aux_data().fut->get_future();
    }

    if (_resources.memory > 0 || (consumed_resources().memory + memory) < get_serialize_limit()) {
        permit.on_granted_memory();
        return make_ready_future<>();
    }

    if (!_blessed_permit) {
        _blessed_permit = &permit;
    }

    if (_blessed_permit == &permit) {
        permit.on_granted_memory();
        return make_ready_future<>();
    }

    return enqueue_waiter(permit, wait_on::memory);
}

void reader_concurrency_semaphore::dequeue_permit(reader_permit::impl& permit) {
    switch (permit.get_state()) {
        case reader_permit::state::waiting_for_admission:
        case reader_permit::state::waiting_for_memory:
        case reader_permit::state::waiting_for_execution:
            --_stats.waiters;
            break;
        case reader_permit::state::inactive:
        case reader_permit::state::evicted:
            --_stats.inactive_reads;
            break;
        case reader_permit::state::active:
        case reader_permit::state::active_need_cpu:
        case reader_permit::state::active_await:
            on_internal_error_noexcept(rcslog, format("reader_concurrency_semaphore::dequeue_permit(): unrecognized queued state: {}", permit.get_state()));
    }
    permit.unlink();
    _permit_list.push_back(permit);
}

void reader_concurrency_semaphore::on_permit_created(reader_permit::impl& permit) {
    _permit_gate.enter();
    _permit_list.push_back(permit);
    ++_stats.total_permits;
    ++_stats.current_permits;
}

void reader_concurrency_semaphore::on_permit_destroyed(reader_permit::impl& permit) noexcept {
    permit.unlink();
    _permit_gate.leave();
    --_stats.current_permits;
    if (_blessed_permit == &permit) {
        _blessed_permit = nullptr;
        maybe_admit_waiters();
    }
}

void reader_concurrency_semaphore::on_permit_need_cpu() noexcept {
    ++_stats.need_cpu_permits;
}

void reader_concurrency_semaphore::on_permit_not_need_cpu() noexcept {
    SCYLLA_ASSERT(_stats.need_cpu_permits);
    --_stats.need_cpu_permits;
    SCYLLA_ASSERT(_stats.need_cpu_permits >= _stats.awaits_permits);
    maybe_admit_waiters();
}

void reader_concurrency_semaphore::on_permit_awaits() noexcept {
    ++_stats.awaits_permits;
    SCYLLA_ASSERT(_stats.need_cpu_permits >= _stats.awaits_permits);
    maybe_admit_waiters();
}

void reader_concurrency_semaphore::on_permit_not_awaits() noexcept {
    SCYLLA_ASSERT(_stats.awaits_permits);
    --_stats.awaits_permits;
}

future<reader_permit> reader_concurrency_semaphore::obtain_permit(schema_ptr schema, const char* const op_name, size_t memory,
        db::timeout_clock::time_point timeout, tracing::trace_state_ptr trace_ptr) {
    auto permit = reader_permit(*this, std::move(schema), std::string_view(op_name), {1, static_cast<ssize_t>(memory)}, timeout, std::move(trace_ptr));
    return do_wait_admission(*permit).then([permit] () mutable {
        return std::move(permit);
    });
}

future<reader_permit> reader_concurrency_semaphore::obtain_permit(schema_ptr schema, sstring&& op_name, size_t memory,
        db::timeout_clock::time_point timeout, tracing::trace_state_ptr trace_ptr) {
    auto permit = reader_permit(*this, std::move(schema), std::move(op_name), {1, static_cast<ssize_t>(memory)}, timeout, std::move(trace_ptr));
    return do_wait_admission(*permit).then([permit] () mutable {
        return std::move(permit);
    });
}

reader_permit reader_concurrency_semaphore::make_tracking_only_permit(schema_ptr schema, const char* const op_name,
        db::timeout_clock::time_point timeout, tracing::trace_state_ptr trace_ptr) {
    return reader_permit(*this, std::move(schema), std::string_view(op_name), {}, timeout, std::move(trace_ptr));
}

reader_permit reader_concurrency_semaphore::make_tracking_only_permit(schema_ptr schema, sstring&& op_name,
        db::timeout_clock::time_point timeout, tracing::trace_state_ptr trace_ptr) {
    return reader_permit(*this, std::move(schema), std::move(op_name), {}, timeout, std::move(trace_ptr));
}

future<> reader_concurrency_semaphore::with_permit(schema_ptr schema, const char* const op_name, size_t memory,
        db::timeout_clock::time_point timeout, tracing::trace_state_ptr trace_ptr, read_func func) {
    auto permit = reader_permit(*this, std::move(schema), std::string_view(op_name), {1, static_cast<ssize_t>(memory)}, timeout, std::move(trace_ptr));
    permit->aux_data().func = std::move(func);
    permit->aux_data().permit_keepalive = permit;
    return do_wait_admission(*permit);
}

future<> reader_concurrency_semaphore::with_ready_permit(reader_permit::impl& permit) {
    if (auto ex = check_queue_size("ready")) {
        return make_exception_future<>(std::move(ex));
    }
    auto& ad = permit.aux_data();
    ad.pr = {};
    auto fut = ad.pr.get_future();
    permit.unlink();
    _ready_list.push_back(permit);
    permit.on_waiting_for_execution();
    ++_stats.waiters;
    _ready_list_cv.signal();
    return fut;
}

future<> reader_concurrency_semaphore::with_ready_permit(reader_permit permit, read_func func) {
    permit->aux_data().func = std::move(func);
    return with_ready_permit(*permit);
}

void reader_concurrency_semaphore::set_resources(resources r) {
    auto delta = r - _initial_resources;
    _initial_resources = r;
    _resources += delta;
    maybe_admit_waiters();
}

void reader_concurrency_semaphore::broken(std::exception_ptr ex) {
    if (!ex) {
        ex = std::make_exception_ptr(broken_semaphore{});
    }
    while (!_wait_list.empty()) {
        auto& permit = _wait_list.front();
        permit.aux_data().pr.set_exception(ex);
        dequeue_permit(permit);
    }
}

std::string reader_concurrency_semaphore::dump_diagnostics(unsigned max_lines) const {
    std::ostringstream os;
    do_dump_reader_permit_diagnostics(os, *this, "user request", max_lines);
    return std::move(os).str();
}

void reader_concurrency_semaphore::foreach_permit(noncopyable_function<void(const reader_permit::impl&)> func) const {
    boost::for_each(_permit_list, std::ref(func));
    boost::for_each(_wait_list._admission_queue, std::ref(func));
    boost::for_each(_wait_list._memory_queue, std::ref(func));
    boost::for_each(_ready_list, std::ref(func));
}

void reader_concurrency_semaphore::foreach_permit(noncopyable_function<void(const reader_permit&)> func) const {
    foreach_permit([func = std::move(func)] (const reader_permit::impl& p) {
        // We cast away const to construct a reader_permit but the resulting
        // object is passed as const& so there is no const violation here.
        func(reader_permit(const_cast<reader_permit::impl&>(p).shared_from_this()));
    });
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

    virtual future<size_t> write_dma(uint64_t pos, const void* buffer, size_t len, io_intent* intent) override {
        return get_file_impl(_tracked_file)->write_dma(pos, buffer, len, intent);
    }

    virtual future<size_t> write_dma(uint64_t pos, std::vector<iovec> iov, io_intent* intent) override {
        return get_file_impl(_tracked_file)->write_dma(pos, std::move(iov), intent);
    }

    virtual future<size_t> read_dma(uint64_t pos, void* buffer, size_t len, io_intent* intent) override {
        return get_file_impl(_tracked_file)->read_dma(pos, buffer, len, intent);
    }

    virtual future<size_t> read_dma(uint64_t pos, std::vector<iovec> iov, io_intent* intent) override {
        return get_file_impl(_tracked_file)->read_dma(pos, iov, intent);
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

    virtual future<temporary_buffer<uint8_t>> dma_read_bulk(uint64_t offset, size_t range_size, io_intent* intent) override {
        return _permit.request_memory(range_size).then([this, offset, range_size, intent] (reader_permit::resource_units units) {
            return get_file_impl(_tracked_file)->dma_read_bulk(offset, range_size, intent).then([units = std::move(units)] (temporary_buffer<uint8_t> buf) mutable {
                return make_ready_future<temporary_buffer<uint8_t>>(make_tracked_temporary_buffer(std::move(buf), std::move(units)));
            });
        });
    }
};

file make_tracked_file(file f, reader_permit p) {
    return file(make_shared<tracking_file_impl>(f, std::move(p)));
}
