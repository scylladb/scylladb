/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <boost/range/algorithm/heap_algorithm.hpp>
#include <boost/range/algorithm/reverse.hpp>
#include <boost/move/iterator.hpp>
#include <variant>

#include <seastar/core/future-util.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/util/closeable.hh>

#include "mutation_reader.hh"
#include "readers/flat_mutation_reader.hh"
#include "readers/empty.hh"
#include "schema_registry.hh"
#include "mutation_compactor.hh"
#include "dht/sharder.hh"
#include "readers/empty_v2.hh"
#include "readers/combined.hh"

logging::logger mrlog("mutation_reader");

snapshot_source make_empty_snapshot_source() {
    return snapshot_source([] {
        return make_empty_mutation_source();
    });
}

mutation_source make_empty_mutation_source() {
    return mutation_source([](schema_ptr s,
            reader_permit permit,
            const dht::partition_range& pr,
            const query::partition_slice& slice,
            const io_priority_class& pc,
            tracing::trace_state_ptr tr,
            streamed_mutation::forwarding fwd,
            mutation_reader::forwarding) {
        return make_empty_flat_reader(s, std::move(permit));
    }, [] {
        return [] (const dht::decorated_key& key) {
            return partition_presence_checker_result::definitely_doesnt_exist;
        };
    });
}

mutation_source make_combined_mutation_source(std::vector<mutation_source> addends) {
    return mutation_source([addends = std::move(addends)] (schema_ptr s,
            reader_permit permit,
            const dht::partition_range& pr,
            const query::partition_slice& slice,
            const io_priority_class& pc,
            tracing::trace_state_ptr tr,
            streamed_mutation::forwarding fwd_sm,
            mutation_reader::forwarding fwd_mr) {
        std::vector<flat_mutation_reader_v2> rd;
        rd.reserve(addends.size());
        for (auto&& ms : addends) {
            rd.emplace_back(ms.make_reader_v2(s, permit, pr, slice, pc, tr, fwd_sm, fwd_mr));
        }
        return make_combined_reader(s, std::move(permit), std::move(rd), fwd_sm, fwd_mr);
    });
}

class queue_reader final : public flat_mutation_reader::impl {
    friend class queue_reader_handle;

private:
    queue_reader_handle* _handle = nullptr;
    std::optional<promise<>> _not_full;
    std::optional<promise<>> _full;
    std::exception_ptr _ex;

private:
    void push_and_maybe_notify(mutation_fragment&& mf) {
        push_mutation_fragment(std::move(mf));
        if (_full && is_buffer_full()) {
            _full->set_value();
            _full.reset();
        }
    }

public:
    explicit queue_reader(schema_ptr s, reader_permit permit)
        : impl(std::move(s), std::move(permit)) {
    }
    virtual future<> fill_buffer() override {
        if (_ex) {
            return make_exception_future<>(_ex);
        }
        if (_end_of_stream || !is_buffer_empty()) {
            return make_ready_future<>();
        }
        if (_not_full) {
            _not_full->set_value();
            _not_full.reset();
        }
        _full.emplace();
        return _full->get_future();
    }
    virtual future<> next_partition() override {
        clear_buffer_to_next_partition();
        if (is_buffer_empty() && !is_end_of_stream()) {
            return fill_buffer().then([this] {
                return next_partition();
            });
        }
        return make_ready_future<>();
    }
    virtual future<> fast_forward_to(const dht::partition_range&) override {
        return make_exception_future<>(make_backtraced_exception_ptr<std::bad_function_call>());
    }
    virtual future<> fast_forward_to(position_range) override {
        return make_exception_future<>(make_backtraced_exception_ptr<std::bad_function_call>());
    }
    virtual future<> close() noexcept override {
        // wake up any waiters to prevent broken_promise errors
        if (_full) {
            _full->set_value();
            _full.reset();
        } else if (_not_full) {
            _not_full->set_value();
            _not_full.reset();
        }
        // detach from the queue_reader_handle
        // since it should never access the reader after close.
        if (_handle) {
            _handle->_reader = nullptr;
            _handle = nullptr;
        }
        return make_ready_future<>();
    }
    future<> push(mutation_fragment&& mf) {
        push_and_maybe_notify(std::move(mf));
        if (!is_buffer_full()) {
            return make_ready_future<>();
        }
        _not_full.emplace();
        return _not_full->get_future();
    }
    void push_end_of_stream() {
        _end_of_stream = true;
        if (_full) {
            _full->set_value();
            _full.reset();
        }
    }
    void abort(std::exception_ptr ep) noexcept {
        _ex = std::move(ep);
        if (_full) {
            _full->set_exception(_ex);
            _full.reset();
        } else if (_not_full) {
            _not_full->set_exception(_ex);
            _not_full.reset();
        }
    }
};

void queue_reader_handle::abandon() noexcept {
    std::exception_ptr ex;
    try {
        ex = std::make_exception_ptr<std::runtime_error>(std::runtime_error("Abandoned queue_reader_handle"));
    } catch (...) {
        ex = std::current_exception();
    }
    abort(std::move(ex));
}

queue_reader_handle::queue_reader_handle(queue_reader& reader) noexcept : _reader(&reader) {
    _reader->_handle = this;
}

queue_reader_handle::queue_reader_handle(queue_reader_handle&& o) noexcept
        : _reader(std::exchange(o._reader, nullptr))
        , _ex(std::exchange(o._ex, nullptr))
{
    if (_reader) {
        _reader->_handle = this;
    }
}

queue_reader_handle::~queue_reader_handle() {
    abandon();
}

queue_reader_handle& queue_reader_handle::operator=(queue_reader_handle&& o) {
    abandon();
    _reader = std::exchange(o._reader, nullptr);
    _ex = std::exchange(o._ex, {});
    if (_reader) {
        _reader->_handle = this;
    }
    return *this;
}

future<> queue_reader_handle::push(mutation_fragment mf) {
    if (!_reader) {
        if (_ex) {
            return make_exception_future<>(_ex);
        }
        return make_exception_future<>(std::runtime_error("Dangling queue_reader_handle"));
    }
    return _reader->push(std::move(mf));
}

void queue_reader_handle::push_end_of_stream() {
    if (!_reader) {
        throw std::runtime_error("Dangling queue_reader_handle");
    }
    _reader->push_end_of_stream();
    _reader->_handle = nullptr;
    _reader = nullptr;
}

bool queue_reader_handle::is_terminated() const {
    return _reader == nullptr;
}

void queue_reader_handle::abort(std::exception_ptr ep) {
    _ex = std::move(ep);
    if (_reader) {
        _reader->abort(_ex);
        _reader->_handle = nullptr;
        _reader = nullptr;
    }
}

std::exception_ptr queue_reader_handle::get_exception() const noexcept {
    return _ex;
}

std::pair<flat_mutation_reader, queue_reader_handle> make_queue_reader(schema_ptr s, reader_permit permit) {
    auto impl = std::make_unique<queue_reader>(std::move(s), std::move(permit));
    auto handle = queue_reader_handle(*impl);
    return {flat_mutation_reader(std::move(impl)), std::move(handle)};
}

class queue_reader_v2 final : public flat_mutation_reader_v2::impl {
    friend class queue_reader_handle_v2;

private:
    queue_reader_handle_v2* _handle = nullptr;
    std::optional<promise<>> _not_full;
    std::optional<promise<>> _full;
    std::exception_ptr _ex;

private:
    void push_and_maybe_notify(mutation_fragment_v2&& mf) {
        push_mutation_fragment(std::move(mf));
        if (_full && is_buffer_full()) {
            _full->set_value();
            _full.reset();
        }
    }

public:
    explicit queue_reader_v2(schema_ptr s, reader_permit permit)
        : impl(std::move(s), std::move(permit)) {
    }
    virtual future<> fill_buffer() override {
        if (_ex) {
            return make_exception_future<>(_ex);
        }
        if (_end_of_stream || !is_buffer_empty()) {
            return make_ready_future<>();
        }
        if (_not_full) {
            _not_full->set_value();
            _not_full.reset();
        }
        _full.emplace();
        return _full->get_future();
    }
    virtual future<> next_partition() override {
        clear_buffer_to_next_partition();
        if (is_buffer_empty() && !is_end_of_stream()) {
            return fill_buffer().then([this] {
                return next_partition();
            });
        }
        return make_ready_future<>();
    }
    virtual future<> fast_forward_to(const dht::partition_range&) override {
        return make_exception_future<>(make_backtraced_exception_ptr<std::bad_function_call>());
    }
    virtual future<> fast_forward_to(position_range) override {
        return make_exception_future<>(make_backtraced_exception_ptr<std::bad_function_call>());
    }
    virtual future<> close() noexcept override {
        // wake up any waiters to prevent broken_promise errors
        if (_full) {
            _full->set_value();
            _full.reset();
        } else if (_not_full) {
            _not_full->set_value();
            _not_full.reset();
        }
        // detach from the queue_reader_handle
        // since it should never access the reader after close.
        if (_handle) {
            _handle->_reader = nullptr;
            _handle = nullptr;
        }
        return make_ready_future<>();
    }
    future<> push(mutation_fragment_v2&& mf) {
        push_and_maybe_notify(std::move(mf));
        if (!is_buffer_full()) {
            return make_ready_future<>();
        }
        _not_full.emplace();
        return _not_full->get_future();
    }
    void push_end_of_stream() {
        _end_of_stream = true;
        if (_full) {
            _full->set_value();
            _full.reset();
        }
    }
    void abort(std::exception_ptr ep) noexcept {
        _ex = std::move(ep);
        if (_full) {
            _full->set_exception(_ex);
            _full.reset();
        } else if (_not_full) {
            _not_full->set_exception(_ex);
            _not_full.reset();
        }
    }
};

void queue_reader_handle_v2::abandon() noexcept {
    std::exception_ptr ex;
    try {
        ex = std::make_exception_ptr<std::runtime_error>(std::runtime_error("Abandoned queue_reader_handle_v2"));
    } catch (...) {
        ex = std::current_exception();
    }
    abort(std::move(ex));
}

queue_reader_handle_v2::queue_reader_handle_v2(queue_reader_v2& reader) noexcept : _reader(&reader) {
    _reader->_handle = this;
}

queue_reader_handle_v2::queue_reader_handle_v2(queue_reader_handle_v2&& o) noexcept
        : _reader(std::exchange(o._reader, nullptr))
        , _ex(std::exchange(o._ex, nullptr))
{
    if (_reader) {
        _reader->_handle = this;
    }
}

queue_reader_handle_v2::~queue_reader_handle_v2() {
    abandon();
}

queue_reader_handle_v2& queue_reader_handle_v2::operator=(queue_reader_handle_v2&& o) {
    abandon();
    _reader = std::exchange(o._reader, nullptr);
    _ex = std::exchange(o._ex, {});
    if (_reader) {
        _reader->_handle = this;
    }
    return *this;
}

future<> queue_reader_handle_v2::push(mutation_fragment_v2 mf) {
    if (!_reader) {
        if (_ex) {
            return make_exception_future<>(_ex);
        }
        return make_exception_future<>(std::runtime_error("Dangling queue_reader_handle_v2"));
    }
    return _reader->push(std::move(mf));
}

void queue_reader_handle_v2::push_end_of_stream() {
    if (!_reader) {
        throw std::runtime_error("Dangling queue_reader_handle_v2");
    }
    _reader->push_end_of_stream();
    _reader->_handle = nullptr;
    _reader = nullptr;
}

bool queue_reader_handle_v2::is_terminated() const {
    return _reader == nullptr;
}

void queue_reader_handle_v2::abort(std::exception_ptr ep) {
    _ex = std::move(ep);
    if (_reader) {
        _reader->abort(_ex);
        _reader->_handle = nullptr;
        _reader = nullptr;
    }
}

std::exception_ptr queue_reader_handle_v2::get_exception() const noexcept {
    return _ex;
}

std::pair<flat_mutation_reader_v2, queue_reader_handle_v2> make_queue_reader_v2(schema_ptr s, reader_permit permit) {
    auto impl = std::make_unique<queue_reader_v2>(std::move(s), std::move(permit));
    auto handle = queue_reader_handle_v2(*impl);
    return {flat_mutation_reader_v2(std::move(impl)), std::move(handle)};
}

namespace {

class compacting_reader : public flat_mutation_reader_v2::impl {
    friend class compact_mutation_state<emit_only_live_rows::no, compact_for_sstables::yes>;

private:
    flat_mutation_reader_v2 _reader;
    compact_mutation_state<emit_only_live_rows::no, compact_for_sstables::yes> _compactor;
    noop_compacted_fragments_consumer _gc_consumer;

    // Uncompacted stream
    partition_start _last_uncompacted_partition_start;
    mutation_fragment_v2::kind _last_uncompacted_kind = mutation_fragment_v2::kind::partition_end;

    // Compacted stream
    bool _has_compacted_partition_start = false;
    bool _ignore_partition_end = false;

private:
    void maybe_push_partition_start() {
        if (_has_compacted_partition_start) {
            push_mutation_fragment(mutation_fragment_v2(*_schema, _permit, std::move(_last_uncompacted_partition_start)));
            _has_compacted_partition_start = false;
        }
    }
    void maybe_inject_partition_end() {
        // The compactor needs a valid stream, but downstream doesn't care about
        // the injected partition end, so ignore it.
        if (_last_uncompacted_kind != mutation_fragment_v2::kind::partition_end) {
            _ignore_partition_end = true;
            _compactor.consume_end_of_partition(*this, _gc_consumer);
            _ignore_partition_end = false;
        }
    }
    void consume_new_partition(const dht::decorated_key& dk) {
        _has_compacted_partition_start = true;
        // We need to reset the partition's tombstone here. If the tombstone is
        // compacted away, `consume(tombstone)` below is simply not called. If
        // it is not compacted away, `consume(tombstone)` below will restore it.
        _last_uncompacted_partition_start.partition_tombstone() = {};
    }
    void consume(tombstone t) {
        _last_uncompacted_partition_start.partition_tombstone() = t;
        maybe_push_partition_start();
    }
    stop_iteration consume(static_row&& sr, tombstone, bool) {
        maybe_push_partition_start();
        push_mutation_fragment(mutation_fragment_v2(*_schema, _permit, std::move(sr)));
        return stop_iteration::no;
    }
    stop_iteration consume(clustering_row&& cr, row_tombstone, bool) {
        maybe_push_partition_start();
        push_mutation_fragment(mutation_fragment_v2(*_schema, _permit, std::move(cr)));
        return stop_iteration::no;
    }
    stop_iteration consume(range_tombstone_change&& rtc) {
        // The compactor will close the active tombstone (if any) on partition
        // end. We ignore this when we don't care about the partition-end.
        if (_ignore_partition_end) {
            return stop_iteration::no;
        }
        maybe_push_partition_start();
        push_mutation_fragment(mutation_fragment_v2(*_schema, _permit, std::move(rtc)));
        return stop_iteration::no;
    }
    stop_iteration consume_end_of_partition() {
        maybe_push_partition_start();
        if (!_ignore_partition_end) {
            push_mutation_fragment(mutation_fragment_v2(*_schema, _permit, partition_end{}));
        }
        return stop_iteration::no;
    }
    void consume_end_of_stream() {
    }
    streamed_mutation::forwarding _fwd;

public:
    compacting_reader(flat_mutation_reader_v2 source, gc_clock::time_point compaction_time,
            std::function<api::timestamp_type(const dht::decorated_key&)> get_max_purgeable,
            streamed_mutation::forwarding fwd = streamed_mutation::forwarding::no)
        : impl(source.schema(), source.permit())
        , _reader(std::move(source))
        , _compactor(*_schema, compaction_time, get_max_purgeable)
        , _last_uncompacted_partition_start(dht::decorated_key(dht::minimum_token(), partition_key::make_empty()), tombstone{})
        , _fwd(fwd) {
    }
    virtual future<> fill_buffer() override {
        return do_until([this] { return is_end_of_stream() || is_buffer_full(); }, [this] {
            return _reader.fill_buffer().then([this] {
                if (_reader.is_buffer_empty()) {
                    _end_of_stream = _reader.is_end_of_stream();
                    if (_end_of_stream && _fwd) {
                        maybe_push_partition_start();
                    }
                }
                // It is important to not consume more than we actually need.
                // Doing so leads to corner cases around `next_partition()`. The
                // fragments consumed after our buffer is full might not be
                // emitted by the compactor, so on a following `next_partition()`
                // call we won't be able to determine whether we are at a
                // partition boundary or not and thus whether we need to forward
                // it to the underlying reader or not.
                // This problem doesn't exist when we want more fragments, in this
                // case we'll keep reading until the compactor emits something or
                // we read EOS, and thus we'll know where we are.
                while (!_reader.is_buffer_empty() && !is_buffer_full()) {
                    auto mf = _reader.pop_mutation_fragment();
                    _last_uncompacted_kind = mf.mutation_fragment_kind();
                    switch (mf.mutation_fragment_kind()) {
                    case mutation_fragment_v2::kind::static_row:
                        _compactor.consume(std::move(mf).as_static_row(), *this, _gc_consumer);
                        break;
                    case mutation_fragment_v2::kind::clustering_row:
                        _compactor.consume(std::move(mf).as_clustering_row(), *this, _gc_consumer);
                        break;
                    case mutation_fragment_v2::kind::range_tombstone_change:
                        _compactor.consume(std::move(mf).as_range_tombstone_change(), *this, _gc_consumer);
                        break;
                    case mutation_fragment_v2::kind::partition_start:
                        _last_uncompacted_partition_start = std::move(mf).as_partition_start();
                        _compactor.consume_new_partition(_last_uncompacted_partition_start.key());
                        if (_last_uncompacted_partition_start.partition_tombstone()) {
                            _compactor.consume(_last_uncompacted_partition_start.partition_tombstone(), *this, _gc_consumer);
                        }
                        if (_fwd) {
                            _compactor.force_partition_not_empty(*this);
                        }
                        break;
                    case mutation_fragment_v2::kind::partition_end:
                        _compactor.consume_end_of_partition(*this, _gc_consumer);
                        break;
                    }
                }
            });
        });
    }
    virtual future<> next_partition() override {
        clear_buffer_to_next_partition();
        if (!is_buffer_empty()) {
            return make_ready_future<>();
        }
        _end_of_stream = false;
        maybe_inject_partition_end();
        return _reader.next_partition();
    }
    virtual future<> fast_forward_to(const dht::partition_range& pr) override {
        clear_buffer();
        _end_of_stream = false;
        maybe_inject_partition_end();
        return _reader.fast_forward_to(pr);
    }
    virtual future<> fast_forward_to(position_range pr) override {
        forward_buffer_to(pr.start());
        _end_of_stream = false;
        return _reader.fast_forward_to(std::move(pr));
    }
    virtual future<> close() noexcept override {
        return _reader.close();
    }
};

} // anonymous namespace

flat_mutation_reader_v2 make_compacting_reader(flat_mutation_reader_v2 source, gc_clock::time_point compaction_time,
        std::function<api::timestamp_type(const dht::decorated_key&)> get_max_purgeable, streamed_mutation::forwarding fwd) {
    return make_flat_mutation_reader_v2<compacting_reader>(std::move(source), compaction_time, get_max_purgeable, fwd);
}
