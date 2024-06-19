/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "utils/assert.hh"
#include <boost/range/algorithm/heap_algorithm.hpp>
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/util/closeable.hh>

#include "dht/sharder.hh"
#include "readers/empty_v2.hh"
#include "readers/evictable.hh"
#include "readers/mutation_reader.hh"
#include "readers/foreign.hh"
#include "readers/multishard.hh"
#include "readers/mutation_source.hh"
#include "schema/schema_registry.hh"
#include "locator/abstract_replication_strategy.hh"

extern logger mrlog;

namespace {

struct remote_fill_buffer_result_v2 {
    foreign_ptr<std::unique_ptr<const mutation_reader::tracked_buffer>> buffer;
    bool end_of_stream = false;

    remote_fill_buffer_result_v2() = default;
    remote_fill_buffer_result_v2(mutation_reader::tracked_buffer&& buffer, bool end_of_stream)
        : buffer(make_foreign(std::make_unique<const mutation_reader::tracked_buffer>(std::move(buffer))))
        , end_of_stream(end_of_stream) {
    }
};

}

/// See make_foreign_reader() for description.
class foreign_reader : public mutation_reader::impl {
    template <typename T>
    using foreign_unique_ptr = foreign_ptr<std::unique_ptr<T>>;

    using fragment_buffer = mutation_reader::tracked_buffer;

    foreign_unique_ptr<mutation_reader> _reader;
    foreign_unique_ptr<future<>> _read_ahead_future;
    streamed_mutation::forwarding _fwd_sm;

    // Forward an operation to the reader on the remote shard.
    // If the remote reader has an ongoing read-ahead, bring it to the
    // foreground (wait on it) and execute the operation after.
    // After the operation completes, kick off a new read-ahead (fill_buffer())
    // and move it to the background (save it's future but don't wait on it
    // now). If all works well read-aheads complete by the next operation and
    // we don't have to wait on the remote reader filling its buffer.
    template <typename Operation, typename Result = futurize_t<std::invoke_result_t<Operation>>>
    Result forward_operation(Operation op) {
        reader_permit::awaits_guard awaits_guard{_permit};
        return smp::submit_to(_reader.get_owner_shard(), [reader = _reader.get(),
                read_ahead_future = std::exchange(_read_ahead_future, nullptr),
                op = std::move(op)] () mutable {
            auto exec_op_and_read_ahead = [=] () mutable {
                // Not really variadic, we expect 0 (void) or 1 parameter.
                return op().then([=] (auto... result) {
                    auto f = reader->is_end_of_stream() ? nullptr : std::make_unique<future<>>(reader->fill_buffer());
                    return make_ready_future<std::tuple<foreign_unique_ptr<future<>>, decltype(result)...>>(
                                std::tuple(make_foreign(std::move(f)), std::move(result)...));
                });
            };
            if (read_ahead_future) {
                return read_ahead_future->then(std::move(exec_op_and_read_ahead));
            } else {
                return exec_op_and_read_ahead();
            }
        }).then([this] (auto fut_and_result) {
            _read_ahead_future = std::get<0>(std::move(fut_and_result)); // NOLINT(bugprone-use-after-move)
            static_assert(std::tuple_size<decltype(fut_and_result)>::value <= 2);
            if constexpr (std::tuple_size<decltype(fut_and_result)>::value == 1) {
                return make_ready_future<>();
            } else {
                auto result = std::get<1>(std::move(fut_and_result)); // NOLINT(bugprone-use-after-move)
                return make_ready_future<decltype(result)>(std::move(result));
            }
        }).finally([awaits_guard = std::move(awaits_guard)] { });
    }
public:
    foreign_reader(schema_ptr schema,
            reader_permit permit,
            foreign_unique_ptr<mutation_reader> reader,
            streamed_mutation::forwarding fwd_sm = streamed_mutation::forwarding::no);

    // this is captured.
    foreign_reader(const foreign_reader&) = delete;
    foreign_reader& operator=(const foreign_reader&) = delete;
    foreign_reader(foreign_reader&&) = delete;
    foreign_reader& operator=(foreign_reader&&) = delete;

    virtual future<> fill_buffer() override;
    virtual future<> next_partition() override;
    virtual future<> fast_forward_to(const dht::partition_range& pr) override;
    virtual future<> fast_forward_to(position_range pr) override;
    virtual future<> close() noexcept override;
};

foreign_reader::foreign_reader(schema_ptr schema,
        reader_permit permit,
        foreign_unique_ptr<mutation_reader> reader,
        streamed_mutation::forwarding fwd_sm)
    : impl(std::move(schema), std::move(permit))
    , _reader(std::move(reader))
    , _fwd_sm(fwd_sm) {
}

future<> foreign_reader::fill_buffer() {
    if (_end_of_stream || is_buffer_full()) {
        return make_ready_future();
    }

    return forward_operation([reader = _reader.get()] () {
        auto f = reader->is_buffer_empty() ? reader->fill_buffer() : make_ready_future<>();
        return f.then([=] {
            return make_ready_future<remote_fill_buffer_result_v2>(remote_fill_buffer_result_v2(reader->detach_buffer(), reader->is_end_of_stream()));
        });
    }).then([this] (remote_fill_buffer_result_v2 res) mutable {
        _end_of_stream = res.end_of_stream;
        for (const auto& mf : *res.buffer) {
            // Need a copy since the mf is on the remote shard.
            push_mutation_fragment(mutation_fragment_v2(*_schema, _permit, mf));
        }
    });
}

future<> foreign_reader::next_partition() {
    if (_fwd_sm == streamed_mutation::forwarding::yes) {
        clear_buffer();
        _end_of_stream = false;
    } else {
        clear_buffer_to_next_partition();
        if (!is_buffer_empty()) {
            co_return;
        }
        _end_of_stream = false;
    }
    co_await forward_operation([reader = _reader.get()] () {
        return reader->next_partition();
    });
}

future<> foreign_reader::fast_forward_to(const dht::partition_range& pr) {
    clear_buffer();
    _end_of_stream = false;
    return forward_operation([reader = _reader.get(), &pr] () {
        return reader->fast_forward_to(pr);
    });
}

future<> foreign_reader::fast_forward_to(position_range pr) {
    clear_buffer();
    _end_of_stream = false;
    return forward_operation([reader = _reader.get(), pr = std::move(pr)] () {
        return reader->fast_forward_to(std::move(pr));
    });
}

future<> foreign_reader::close() noexcept {
    if (!_reader) {
        if (_read_ahead_future) {
            on_internal_error_noexcept(mrlog, "foreign_reader::close can't wait on read_ahead future with disengaged reader");
        }
        return make_ready_future<>();
    }
    return smp::submit_to(_reader.get_owner_shard(),
            [reader = std::move(_reader), read_ahead_future = std::exchange(_read_ahead_future, nullptr)] () mutable {
        auto read_ahead = read_ahead_future ? std::move(*read_ahead_future.get()) : make_ready_future<>();
        return read_ahead.then_wrapped([reader = std::move(reader)] (future<> f) mutable {
            if (f.failed()) {
                auto ex = f.get_exception();
                mrlog.warn("foreign_reader: benign read_ahead failure during close: {}. Ignoring.", ex);
            }
            return reader->close();
        });
    });
}

mutation_reader make_foreign_reader(schema_ptr schema,
            reader_permit permit,
            foreign_ptr<std::unique_ptr<mutation_reader>> reader,
            streamed_mutation::forwarding fwd_sm) {
    if (reader.get_owner_shard() == this_shard_id()) {
        return std::move(*reader);
    }
    return make_mutation_reader<foreign_reader>(std::move(schema), std::move(permit), std::move(reader), fwd_sm);
}

template <typename... Arg>
static void require(bool condition, const char* msg, const Arg&... arg) {
    if (!condition) {
        on_internal_error(mrlog, format(msg, arg...));
    }
}

// Encapsulates all data and logic that is local to the remote shard the
// reader lives on.
class evictable_reader_v2 : public mutation_reader::impl {
public:
    using auto_pause = bool_class<class auto_pause_tag>;

private:
    auto_pause _auto_pause;
    mutation_source _ms;
    const dht::partition_range* _pr;
    const query::partition_slice& _ps;
    tracing::global_trace_state_ptr _trace_state;
    const mutation_reader::forwarding _fwd_mr;
    reader_concurrency_semaphore::inactive_read_handle _irh;
    bool _reader_recreated = false; // set if reader was recreated since last operation
    position_in_partition::tri_compare _tri_cmp;

    std::optional<dht::decorated_key> _last_pkey;
    position_in_partition _next_position_in_partition = position_in_partition::for_partition_start();
    // These are used when the reader has to be recreated (after having been
    // evicted while paused) and the range and/or slice it is recreated with
    // differs from the original ones.
    std::optional<dht::partition_range> _range_override;
    std::optional<query::partition_slice> _slice_override;

    mutation_reader_opt _reader;

private:
    void do_pause(mutation_reader reader) noexcept;
    void maybe_pause(mutation_reader reader) noexcept;
    mutation_reader_opt try_resume();
    void update_next_position();
    void adjust_partition_slice();
    mutation_reader recreate_reader();
    future<mutation_reader> resume_or_create_reader();
    void validate_partition_start(const partition_start& ps);
    void validate_position_in_partition(position_in_partition_view pos) const;
    void examine_first_fragments(mutation_fragment_v2_opt& mf1, mutation_fragment_v2_opt& mf2, mutation_fragment_v2_opt& mf3);

public:
    evictable_reader_v2(
            auto_pause ap,
            mutation_source ms,
            mutation_reader_opt reader,
            schema_ptr schema,
            reader_permit permit,
            const dht::partition_range& pr,
            const query::partition_slice& ps,
            tracing::trace_state_ptr trace_state,
            mutation_reader::forwarding fwd_mr);
    virtual future<> fill_buffer() override;
    virtual future<> next_partition() override;
    virtual future<> fast_forward_to(const dht::partition_range& pr) override;
    virtual future<> fast_forward_to(position_range) override {
        throw_with_backtrace<std::bad_function_call>();
    }
    virtual future<> close() noexcept override {
        if (_reader) {
            return _reader->close();
        }
        if (auto reader_opt = try_resume()) {
            return reader_opt->close();
        }
        return make_ready_future<>();
    }
    reader_concurrency_semaphore::inactive_read_handle inactive_read_handle() && {
        return std::move(_irh);
    }
    void pause() {
        if (_reader) {
            do_pause(std::move(*_reader));
        }
    }
    reader_permit permit() {
        return _permit;
    }
};

void evictable_reader_v2::do_pause(mutation_reader reader) noexcept {
    SCYLLA_ASSERT(!_irh);
    _irh = _permit.semaphore().register_inactive_read(std::move(reader));
}

void evictable_reader_v2::maybe_pause(mutation_reader reader) noexcept {
    if (_auto_pause) {
        do_pause(std::move(reader));
    } else {
        _reader = std::move(reader);
    }
}

mutation_reader_opt evictable_reader_v2::try_resume() {
    if (auto reader_opt = _permit.semaphore().unregister_inactive_read(std::move(_irh))) {
        return std::move(*reader_opt);
    }
    return {};
}

void evictable_reader_v2::update_next_position() {
    if (is_buffer_empty()) {
        return;
    }

    auto rbegin = std::reverse_iterator(buffer().end());
    auto rend = std::reverse_iterator(buffer().begin());
    if (auto pk_it = std::find_if(rbegin, rend, std::mem_fn(&mutation_fragment_v2::is_partition_start)); pk_it != rend) {
        _last_pkey = pk_it->as_partition_start().key();
    }

    const auto last_pos = buffer().back().position();
    switch (last_pos.region()) {
        case partition_region::partition_start:
            _next_position_in_partition = position_in_partition::for_static_row();
            break;
        case partition_region::static_row:
            _next_position_in_partition = position_in_partition::before_all_clustered_rows();
            break;
        case partition_region::clustered:
            if (!_reader->is_buffer_empty() && _reader->peek_buffer().is_end_of_partition()) {
                push_mutation_fragment(_reader->pop_mutation_fragment());
                _next_position_in_partition = position_in_partition::for_partition_start();
            } else {
                _next_position_in_partition = position_in_partition::after_key(*_schema, last_pos);
            }
            break;
        case partition_region::partition_end:
           _next_position_in_partition = position_in_partition::for_partition_start();
           break;
    }
}

void evictable_reader_v2::adjust_partition_slice() {
    _slice_override = _ps;

    auto ranges = _slice_override->default_row_ranges();
    query::trim_clustering_row_ranges_to(*_schema, ranges, _next_position_in_partition);

    _slice_override->clear_ranges();
    _slice_override->set_range(*_schema, _last_pkey->key(), std::move(ranges));
}

mutation_reader evictable_reader_v2::recreate_reader() {
    const dht::partition_range* range = _pr;
    const query::partition_slice* slice = &_ps;

    _range_override.reset();
    _slice_override.reset();

    if (_last_pkey) {
        bool partition_range_is_inclusive = true;

        switch (_next_position_in_partition.region()) {
        case partition_region::partition_start:
            partition_range_is_inclusive = false;
            break;
        case partition_region::static_row:
            break;
        case partition_region::clustered:
            adjust_partition_slice();
            slice = &*_slice_override;
            break;
        case partition_region::partition_end:
            partition_range_is_inclusive = false;
            break;
        }

        // The original range contained a single partition and we've read it
        // all. We'd have to create a reader with an empty range that would
        // immediately be at EOS. This is not possible so just create an empty
        // reader instead.
        // This should be extremely rare (who'd create a multishard reader to
        // read a single partition) but still, let's make sure we handle it
        // correctly.
        if (_pr->is_singular() && !partition_range_is_inclusive) {
            return make_empty_flat_reader_v2(_schema, _permit);
        }

        _range_override = dht::partition_range({dht::partition_range::bound(*_last_pkey, partition_range_is_inclusive)}, _pr->end());
        range = &*_range_override;

        _reader_recreated = true;
    }

    return _ms.make_reader_v2(
            _schema,
            _permit,
            *range,
            *slice,
            _trace_state,
            streamed_mutation::forwarding::no,
            _fwd_mr);
}

future<mutation_reader> evictable_reader_v2::resume_or_create_reader() {
    if (_reader) {
        co_return std::move(*_reader);
    }
    if (auto reader_opt = try_resume()) {
        co_return std::move(*reader_opt);
    }
    // When the reader is created the first time and we are actually resuming a
    // saved reader in `recreate_reader()`, we have two cases here:
    // * the reader is still alive (in inactive state)
    // * the reader was evicted
    // We check for this below with `needs_readmission()` and it is very
    // important to not allow for preemption between said check and
    // `recreate_reader()`, otherwise the reader might be evicted between the
    // check and `recreate_reader()` and the latter will recreate it without
    // waiting for re-admission.
    if (_permit.needs_readmission()) {
        co_await _permit.wait_readmission();
    }
    co_return recreate_reader();
}

void evictable_reader_v2::validate_partition_start(const partition_start& ps) {
    const auto tri_cmp = dht::ring_position_comparator(*_schema);
    // If we recreated the reader after fast-forwarding it we won't have
    // _last_pkey set. In this case it is enough to check if the partition
    // is in range.
    if (_last_pkey) {
        const auto cmp_res = tri_cmp(*_last_pkey, ps.key());
        if (_next_position_in_partition.region() != partition_region::partition_start) { // we expect to continue from the same partition
            // We cannot assume the partition we stopped the read at is still alive
            // when we recreate the reader. It might have been compacted away in the
            // meanwhile, so allow for a larger partition too.
            require(
                    cmp_res <= 0,
                    "{}(): validation failed, expected partition with key larger or equal to _last_pkey {}, but got {}",
                    __FUNCTION__,
                    *_last_pkey,
                    ps.key());
            // Reset next pos if we are not continuing from the same partition
            if (cmp_res < 0) {
                // Close previous partition, we are not going to continue it.
                push_mutation_fragment(*_schema, _permit, partition_end{});
                _next_position_in_partition = position_in_partition::for_partition_start();
            }
        } else { // should be a larger partition
            require(
                    cmp_res < 0,
                    "{}(): validation failed, expected partition with key larger than _last_pkey {}, but got {}",
                    __FUNCTION__,
                    *_last_pkey,
                    ps.key());
        }
    }
    const auto& prange = _range_override ? *_range_override : *_pr;
    require(
            // TODO: somehow avoid this copy
            prange.contains(ps.key(), tri_cmp),
            "{}(): validation failed, expected partition with key that falls into current range {}, but got {}",
            __FUNCTION__,
            prange,
            ps.key());
}

void evictable_reader_v2::validate_position_in_partition(position_in_partition_view pos) const {
    require(
            _tri_cmp(_next_position_in_partition, pos) <= 0,
            "{}(): validation failed, expected position in partition that is larger-than-equal than _next_position_in_partition {}, but got {}",
            __FUNCTION__,
            _next_position_in_partition,
            pos);

    if (_slice_override && pos.region() == partition_region::clustered) {
        const auto ranges = _slice_override->row_ranges(*_schema, _last_pkey->key());
        const bool any_contains = std::any_of(ranges.begin(), ranges.end(), [this, &pos] (const query::clustering_range& cr) {
            // TODO: somehow avoid this copy
            auto range = position_range(cr);
            // We cannot use range.contains() because that treats range as a
            // [a, b) range, meaning a range tombstone change with position
            // after_key(b) will be considered outside of it. Such range
            // tombstone changes can be emitted however when recreating the
            // reader on clustering range edge.
            return _tri_cmp(range.start(), pos) <= 0 && _tri_cmp(pos, range.end()) <= 0;
        });
        require(
                any_contains,
                "{}(): validation failed, expected clustering fragment that is included in the slice {}, but got {}",
                __FUNCTION__,
                *_slice_override,
                pos);
    }
}

void evictable_reader_v2::examine_first_fragments(mutation_fragment_v2_opt& mf1, mutation_fragment_v2_opt& mf2, mutation_fragment_v2_opt& mf3) {
    if (!mf1) {
        return; // the reader is at EOS
    }

    // If engaged, the first fragment is always a partition-start.
    validate_partition_start(mf1->as_partition_start());
    if (_tri_cmp(mf1->position(), _next_position_in_partition) < 0) {
        mf1 = {}; // drop mf1
    }

    const auto continue_same_partition = _next_position_in_partition.region() != partition_region::partition_start;

    // If we have a first fragment, we are guaranteed to have a second one -- if not else, a partition-end.
    if (mf2->is_end_of_partition()) {
        return; // no further fragments, nothing to do
    }

    // We want to validate the position of the first non-dropped fragment.
    // If mf2 is a static row and we need to drop it, this will be mf3.
    if (mf2->is_static_row() && _tri_cmp(mf2->position(), _next_position_in_partition) < 0) {
        mf2 = {}; // drop mf2
    } else {
        if (continue_same_partition) {
            validate_position_in_partition(mf2->position());
        }
        return;
    }

    if (mf3->is_end_of_partition()) {
        return; // no further fragments, nothing to do
    } else if (continue_same_partition) {
        validate_position_in_partition(mf3->position());
    }
}

evictable_reader_v2::evictable_reader_v2(
        auto_pause ap,
        mutation_source ms,
        mutation_reader_opt reader,
        schema_ptr schema,
        reader_permit permit,
        const dht::partition_range& pr,
        const query::partition_slice& ps,
        tracing::trace_state_ptr trace_state,
        mutation_reader::forwarding fwd_mr)
    : impl(std::move(schema), std::move(permit))
    , _auto_pause(ap)
    , _ms(std::move(ms))
    , _pr(&pr)
    , _ps(ps)
    , _trace_state(std::move(trace_state))
    , _fwd_mr(fwd_mr)
    , _tri_cmp(*_schema)
    , _reader(std::move(reader)) {
}

future<> evictable_reader_v2::fill_buffer() {
    if (is_end_of_stream()) {
        co_return;
    }
    _reader = co_await resume_or_create_reader();

    if (_reader_recreated) {
        // Recreating the reader breaks snapshot isolation and creates all sorts
        // of complications around the continuity of range tombstone changes,
        // e.g. a range tombstone started by the previous reader object
        // might not exist anymore with the new reader object.
        // To avoid complications we reset the tombstone state on each reader
        // recreation by emitting a null tombstone change, if we read at least
        // one clustering fragment from the partition.
        if (_next_position_in_partition.region() == partition_region::clustered
                && _tri_cmp(_next_position_in_partition, position_in_partition::before_all_clustered_rows()) > 0) {
            push_mutation_fragment(*_schema, _permit, range_tombstone_change{position_in_partition_view::before_key(_next_position_in_partition), {}});
        }
        auto mf1 = co_await (*_reader)();
        auto mf2 = co_await (*_reader)();
        auto mf3 = co_await (*_reader)();
        examine_first_fragments(mf1, mf2, mf3);
        if (mf3) {
            _reader->unpop_mutation_fragment(std::move(*mf3));
        }
        if (mf2) {
            _reader->unpop_mutation_fragment(std::move(*mf2));
        }
        if (mf1) {
            _reader->unpop_mutation_fragment(std::move(*mf1));
        }
        _reader_recreated = false;
    } else {
        co_await _reader->fill_buffer();
    }

    _reader->move_buffer_content_to(*this);

    // Ensure that each buffer represents forward progress. Only a concern when
    // the last fragment in the buffer is range tombstone change. In this case
    // ensure that:
    // * buffer().back().position() > _next_position_in_partition;
    // * _reader.peek()->position() > buffer().back().position();
    if (!is_buffer_empty() && buffer().back().is_range_tombstone_change()) {
        auto* next_mf = co_await _reader->peek();

        // First make sure we've made progress w.r.t. _next_position_in_partition.
        // This loop becomes infinite when next pos is a partition start.
        // In that case progress is guaranteed anyway, so skip this loop entirely.
        while (!_next_position_in_partition.is_partition_start() && next_mf && _tri_cmp(buffer().back().position(), _next_position_in_partition) <= 0) {
            push_mutation_fragment(_reader->pop_mutation_fragment());
            next_mf = co_await _reader->peek();
        }

        const auto last_pos = position_in_partition(buffer().back().position());
        while (next_mf && _tri_cmp(last_pos, next_mf->position()) == 0) {
            push_mutation_fragment(_reader->pop_mutation_fragment());
            next_mf = co_await _reader->peek();
        }
    }

    update_next_position();
    _end_of_stream = _reader->is_end_of_stream();
    maybe_pause(std::move(*_reader));
}

future<> evictable_reader_v2::next_partition() {
    _next_position_in_partition = position_in_partition::for_partition_start();
    clear_buffer_to_next_partition();
    if (!is_buffer_empty()) {
        co_return;
    }
    auto reader = co_await resume_or_create_reader();
    co_await reader.next_partition();
    maybe_pause(std::move(reader));
}

future<> evictable_reader_v2::fast_forward_to(const dht::partition_range& pr) {
    _pr = &pr;
    _last_pkey.reset();
    _next_position_in_partition = position_in_partition::for_partition_start();
    clear_buffer();
    _end_of_stream = false;

    if (_reader) {
        co_await _reader->fast_forward_to(pr);
        _range_override.reset();
        co_return;
    }
    if (auto reader_opt = try_resume()) {
        std::exception_ptr ex;
        try {
            co_await reader_opt->fast_forward_to(pr);
            _range_override.reset();
        } catch (...) {
            ex = std::current_exception();
        }
        if (ex) {
            co_await reader_opt->close();
            std::rethrow_exception(std::move(ex));
        }
        maybe_pause(std::move(*reader_opt));
    }
}

evictable_reader_handle_v2::evictable_reader_handle_v2(evictable_reader_v2& r) : _r(&r)
{ }

void evictable_reader_handle_v2::evictable_reader_handle_v2::pause() {
    _r->pause();
}

mutation_reader make_auto_paused_evictable_reader_v2(
        mutation_source ms,
        schema_ptr schema,
        reader_permit permit,
        const dht::partition_range& pr,
        const query::partition_slice& ps,
        tracing::trace_state_ptr trace_state,
        mutation_reader::forwarding fwd_mr) {
    return make_mutation_reader<evictable_reader_v2>(evictable_reader_v2::auto_pause::yes, std::move(ms), std::nullopt, std::move(schema), std::move(permit), pr, ps,
            std::move(trace_state), fwd_mr);
}

std::pair<mutation_reader, evictable_reader_handle_v2> make_manually_paused_evictable_reader_v2(
        mutation_source ms,
        schema_ptr schema,
        reader_permit permit,
        const dht::partition_range& pr,
        const query::partition_slice& ps,
        tracing::trace_state_ptr trace_state,
        mutation_reader::forwarding fwd_mr) {
    auto reader = std::make_unique<evictable_reader_v2>(evictable_reader_v2::auto_pause::no, std::move(ms), std::nullopt, std::move(schema), std::move(permit), pr, ps,
            std::move(trace_state), fwd_mr);
    auto handle = evictable_reader_handle_v2(*reader.get());
    return std::pair(mutation_reader(std::move(reader)), handle);
}

namespace {

// A special-purpose shard reader.
//
// Shard reader manages a reader located on a remote shard. It transparently
// supports read-ahead (background fill_buffer() calls).
// This reader is not for general use, it was designed to serve the
// multishard_combining_reader.
// Although it implements the mutation_reader:impl interface it cannot be
// wrapped into a mutation_reader, as it needs to be managed by a shared
// pointer.
class shard_reader_v2 : public mutation_reader::impl {
private:
    shared_ptr<reader_lifecycle_policy_v2> _lifecycle_policy;
    const unsigned _shard;
    foreign_ptr<lw_shared_ptr<const dht::partition_range>> _pr;
    const query::partition_slice& _ps;
    tracing::global_trace_state_ptr _trace_state;
    const mutation_reader::forwarding _fwd_mr;
    std::optional<future<>> _read_ahead;
    foreign_ptr<std::unique_ptr<evictable_reader_v2>> _reader;

private:
    future<> do_fill_buffer();

public:
    shard_reader_v2(
            schema_ptr schema,
            reader_permit permit,
            shared_ptr<reader_lifecycle_policy_v2> lifecycle_policy,
            unsigned shard,
            const dht::partition_range& pr,
            const query::partition_slice& ps,
            tracing::trace_state_ptr trace_state,
            mutation_reader::forwarding fwd_mr)
        : impl(std::move(schema), std::move(permit))
        , _lifecycle_policy(std::move(lifecycle_policy))
        , _shard(shard)
        , _pr(make_foreign(make_lw_shared<const dht::partition_range>(pr)))
        , _ps(ps)
        , _trace_state(std::move(trace_state))
        , _fwd_mr(fwd_mr) {
    }

    shard_reader_v2(shard_reader_v2&&) = delete;
    shard_reader_v2& operator=(shard_reader_v2&&) = delete;

    shard_reader_v2(const shard_reader_v2&) = delete;
    shard_reader_v2& operator=(const shard_reader_v2&) = delete;

    const mutation_fragment_v2& peek_buffer() const {
        return buffer().front();
    }
    virtual future<> fill_buffer() override;
    virtual future<> next_partition() override;
    virtual future<> fast_forward_to(const dht::partition_range& pr) override;
    virtual future<> fast_forward_to(position_range) override;
    virtual future<> close() noexcept override;
    bool done() const {
        return _reader && is_buffer_empty() && is_end_of_stream();
    }
    void read_ahead();
    bool is_read_ahead_in_progress() const {
        return _read_ahead.has_value();
    }
};

future<> shard_reader_v2::close() noexcept {
    if (_read_ahead) {
        try {
            co_await *std::exchange(_read_ahead, std::nullopt);
        } catch (...) {
            auto ex = std::current_exception();
            if (!is_timeout_exception(ex)) {
                mrlog.warn("shard_reader::close(): read_ahead on shard {} failed: {}", _shard, ex);
            }
        }
    }

    try {
        co_await smp::submit_to(_shard, [this] {
            if (!_reader) {
                return make_ready_future<>();
            }

            auto irh = std::move(*_reader).inactive_read_handle();
            return with_closeable(mutation_reader(_reader.release()), [this] (mutation_reader& reader) mutable {
                auto permit = reader.permit();
                const auto& schema = *reader.schema();

                auto unconsumed_fragments = reader.detach_buffer();
                auto rit = std::reverse_iterator(buffer().cend());
                auto rend = std::reverse_iterator(buffer().cbegin());
                for (; rit != rend; ++rit) {
                    unconsumed_fragments.emplace_front(schema, permit, *rit); // we are copying from the remote shard.
                }

                return unconsumed_fragments;
            }).then([this, irh = std::move(irh)] (mutation_reader::tracked_buffer&& buf) mutable {
                return _lifecycle_policy->destroy_reader({std::move(irh), std::move(buf)});
            });
        });
    } catch (...) {
        mrlog.error("shard_reader::close(): failed to stop reader on shard {}: {}", _shard, std::current_exception());
    }
}

future<> shard_reader_v2::do_fill_buffer() {
    struct reader_and_buffer_fill_result {
        foreign_ptr<std::unique_ptr<evictable_reader_v2>> reader;
        remote_fill_buffer_result_v2 result;
    };

    auto res = co_await std::invoke([&] () -> future<remote_fill_buffer_result_v2> {
        if (!_reader) {
            reader_and_buffer_fill_result res = co_await smp::submit_to(_shard, coroutine::lambda([this, gs = global_schema_ptr(_schema)] () -> future<reader_and_buffer_fill_result> {
                auto ms = mutation_source([lifecycle_policy = _lifecycle_policy.get()] (
                            schema_ptr s,
                            reader_permit permit,
                            const dht::partition_range& pr,
                            const query::partition_slice& ps,
                            tracing::trace_state_ptr ts,
                            streamed_mutation::forwarding,
                            mutation_reader::forwarding fwd_mr) {
                    return lifecycle_policy->create_reader(std::move(s), std::move(permit), pr, ps, std::move(ts), fwd_mr);
                });
                auto s = gs.get();
                auto permit = co_await _lifecycle_policy->obtain_reader_permit(s, "shard-reader", timeout(), _trace_state);
                if (permit.needs_readmission()) {
                    co_await permit.wait_readmission();
                }
                auto underlying_reader = _lifecycle_policy->create_reader(s, permit, *_pr, _ps, _trace_state, _fwd_mr);

                std::exception_ptr ex;

                try {
                    // The reader might have been saved from a previous page and
                    // missed some fast-forwarding since the new page started.
                    // Fast forward it to the correct range if that is the case.
                    if (auto pr = _lifecycle_policy->get_read_range(); pr && _pr->start() && pr->after(_pr->start()->value(), dht::ring_position_comparator(*_schema))) {
                        auto new_pr = _pr.get_owner_shard() == this_shard_id() ? _pr.release() : make_lw_shared<const dht::partition_range>(*_pr);
                        co_await underlying_reader.fast_forward_to(*new_pr);
                        _lifecycle_policy->update_read_range(new_pr);
                        _pr = make_foreign(std::move(new_pr));
                    }
                } catch (...) {
                    ex = std::current_exception();
                }
                if (ex) {
                    co_await underlying_reader.close();
                    std::rethrow_exception(std::move(ex));
                }

                auto rreader = make_foreign(std::make_unique<evictable_reader_v2>(evictable_reader_v2::auto_pause::yes, std::move(ms),
                            std::move(underlying_reader), s, std::move(permit), *_pr, _ps, _trace_state, _fwd_mr));

                try {
                    tracing::trace(_trace_state, "Creating shard reader on shard: {}", this_shard_id());
                    reader_permit::need_cpu_guard ncpu_guard{rreader->permit()};
                    co_await rreader->fill_buffer();
                    auto res = remote_fill_buffer_result_v2(rreader->detach_buffer(), rreader->is_end_of_stream());
                    co_return reader_and_buffer_fill_result{std::move(rreader), std::move(res)};
                } catch (...) {
                    ex = std::current_exception();
                }
                co_await rreader->close();
                std::rethrow_exception(std::move(ex));
            }));
            _reader = std::move(res.reader);
            co_return std::move(res.result);
        } else {
            co_return co_await smp::submit_to(_shard, coroutine::lambda([this] () -> future<remote_fill_buffer_result_v2>  {
                reader_permit::need_cpu_guard ncpu_guard{_reader->permit()};
                co_await _reader->fill_buffer();
                co_return remote_fill_buffer_result_v2(_reader->detach_buffer(), _reader->is_end_of_stream());
            }));
        }
    });

    reserve_additional(res.buffer->size());
    for (const auto& mf : *res.buffer) {
        push_mutation_fragment(mutation_fragment_v2(*_schema, _permit, mf));
        co_await coroutine::maybe_yield();
    }
    _end_of_stream = res.end_of_stream;
}

future<> shard_reader_v2::fill_buffer() {
    // FIXME: want to move this to the inner scopes but it makes clang miscompile the code.
    reader_permit::awaits_guard guard(_permit);
    if (_read_ahead) {
        co_await *std::exchange(_read_ahead, std::nullopt);
        co_return;
    }
    if (!is_buffer_empty()) {
        co_return;
    }
    co_await do_fill_buffer();
}

future<> shard_reader_v2::next_partition() {
    if (!_reader) {
        co_return;
    }

    // FIXME: want to move this to the inner scopes but it makes clang miscompile the code.
    reader_permit::awaits_guard guard(_permit);

    if (_read_ahead) {
        co_await *std::exchange(_read_ahead, std::nullopt);
    }
    clear_buffer_to_next_partition();
    if (!is_buffer_empty()) {
        co_return;
    }
    co_return co_await smp::submit_to(_shard, [this] {
        return _reader->next_partition();
    });
}

future<> shard_reader_v2::fast_forward_to(const dht::partition_range& pr) {
    if (!_reader && !_read_ahead) {
        // No need to fast-forward uncreated readers, they will be passed the new
        // range when created.
        _pr = make_foreign(make_lw_shared<const dht::partition_range>(pr));
        co_return;
    }

    reader_permit::awaits_guard guard(_permit);

    if (_read_ahead) {
        co_await *std::exchange(_read_ahead, std::nullopt);
    }
    _end_of_stream = false;
    clear_buffer();

    _pr = co_await smp::submit_to(_shard, [this, &pr] () -> future<foreign_ptr<lw_shared_ptr<const dht::partition_range>>> {
        auto new_pr = make_lw_shared<const dht::partition_range>(pr);
        co_await _reader->fast_forward_to(*new_pr);
        _lifecycle_policy->update_read_range(new_pr);
        co_return make_foreign(std::move(new_pr));
    });
}

future<> shard_reader_v2::fast_forward_to(position_range) {
    return make_exception_future<>(make_backtraced_exception_ptr<std::bad_function_call>());
}

void shard_reader_v2::read_ahead() {
    if (_read_ahead || is_end_of_stream() || !is_buffer_empty()) {
        return;
    }

    _read_ahead.emplace(do_fill_buffer());
}

} // anonymous namespace

// See make_multishard_combining_reader() for description.
class multishard_combining_reader_v2 : public mutation_reader::impl {
    struct shard_and_token {
        shard_id shard;
        dht::token token;

        bool operator<(const shard_and_token& o) const {
            // Reversed, as we want a min-heap.
            return token > o.token;
        }
    };

    std::any _keep_alive_sharder;
    const dht::sharder& _sharder;
    std::vector<std::unique_ptr<shard_reader_v2>> _shard_readers;
    // Contains the position of each shard with token granularity, organized
    // into a min-heap. Used to select the shard with the smallest token each
    // time a shard reader produces a new partition.
    std::vector<shard_and_token> _shard_selection_min_heap;
    unsigned _current_shard;
    bool _crossed_shards;
    unsigned _concurrency = 1;

    void on_partition_range_change(const dht::partition_range& pr);
    bool maybe_move_to_next_shard(const dht::token* const t = nullptr);
    future<> handle_empty_reader_buffer();

public:
    multishard_combining_reader_v2(
            const dht::sharder& sharder,
            std::any keep_alive_sharder,
            shared_ptr<reader_lifecycle_policy_v2> lifecycle_policy,
            schema_ptr s,
            reader_permit permit,
            const dht::partition_range& pr,
            const query::partition_slice& ps,
            tracing::trace_state_ptr trace_state,
            mutation_reader::forwarding fwd_mr);

    // this is captured.
    multishard_combining_reader_v2(const multishard_combining_reader_v2&) = delete;
    multishard_combining_reader_v2& operator=(const multishard_combining_reader_v2&) = delete;
    multishard_combining_reader_v2(multishard_combining_reader_v2&&) = delete;
    multishard_combining_reader_v2& operator=(multishard_combining_reader_v2&&) = delete;

    virtual future<> fill_buffer() override;
    virtual future<> next_partition() override;
    virtual future<> fast_forward_to(const dht::partition_range& pr) override;
    virtual future<> fast_forward_to(position_range pr) override;
    virtual future<> close() noexcept override;
};

void multishard_combining_reader_v2::on_partition_range_change(const dht::partition_range& pr) {
    _shard_selection_min_heap.clear();
    _shard_selection_min_heap.reserve(_sharder.shard_count());

    auto token = pr.start() ? pr.start()->value().token() : dht::minimum_token();
    _current_shard = _sharder.shard_for_reads(token);

    auto sharder = dht::ring_position_range_sharder(_sharder, pr);

    auto next = sharder.next(*_schema);

    // The first value of `next` is thrown away, as it is the ring range of the current shard.
    // We only want to do a full round, until we get back to the shard we started from (`_current_shard`).
    // We stop earlier if the sharder has no ranges for the remaining shards.
    for (next = sharder.next(*_schema); next && next->shard != _current_shard; next = sharder.next(*_schema)) {
        _shard_selection_min_heap.push_back(shard_and_token{next->shard, next->ring_range.start()->value().token()});
        boost::push_heap(_shard_selection_min_heap);
    }
}

bool multishard_combining_reader_v2::maybe_move_to_next_shard(const dht::token* const t) {
    if (_shard_selection_min_heap.empty() || (t && *t < _shard_selection_min_heap.front().token)) {
        return false;
    }

    boost::pop_heap(_shard_selection_min_heap);
    const auto next_shard = _shard_selection_min_heap.back().shard;
    _shard_selection_min_heap.pop_back();

    if (t) {
        _shard_selection_min_heap.push_back(shard_and_token{_current_shard, *t});
        boost::push_heap(_shard_selection_min_heap);
    }

    _crossed_shards = true;
    _current_shard = next_shard;
    return true;
}

future<> multishard_combining_reader_v2::handle_empty_reader_buffer() {
    auto& reader = *_shard_readers[_current_shard];

    if (reader.is_end_of_stream()) {
        if (_shard_selection_min_heap.empty()) {
            _end_of_stream = true;
        } else {
            maybe_move_to_next_shard();
        }
        return make_ready_future<>();
    } else if (reader.is_read_ahead_in_progress()) {
        return reader.fill_buffer();
    } else {
        // If we crossed shards and the next reader has an empty buffer we
        // double concurrency so the next time we cross shards we will have
        // more chances of hitting the reader's buffer.
        if (_crossed_shards) {
            _concurrency = std::min(_concurrency * 2, _sharder.shard_count());

            // Read ahead shouldn't change the min selection heap so we work on a local copy.
            auto shard_selection_min_heap_copy = _shard_selection_min_heap;

            // If concurrency > 1 we kick-off concurrency-1 read-aheads in the
            // background. They will be brought to the foreground when we move
            // to their respective shard.
            for (unsigned i = 1; i < _concurrency && !shard_selection_min_heap_copy.empty(); ++i) {
                boost::pop_heap(shard_selection_min_heap_copy);
                const auto next_shard = shard_selection_min_heap_copy.back().shard;
                shard_selection_min_heap_copy.pop_back();
                _shard_readers[next_shard]->read_ahead();
            }
        }
        return reader.fill_buffer();
    }
}

multishard_combining_reader_v2::multishard_combining_reader_v2(
        const dht::sharder& sharder,
        std::any keep_alive_sharder,
        shared_ptr<reader_lifecycle_policy_v2> lifecycle_policy,
        schema_ptr s,
        reader_permit permit,
        const dht::partition_range& pr,
        const query::partition_slice& ps,
        tracing::trace_state_ptr trace_state,
        mutation_reader::forwarding fwd_mr)
    : impl(std::move(s), std::move(permit)), _keep_alive_sharder(std::move(keep_alive_sharder)), _sharder(sharder) {

    on_partition_range_change(pr);

    _shard_readers.reserve(_sharder.shard_count());
    for (unsigned i = 0; i < _sharder.shard_count(); ++i) {
        _shard_readers.emplace_back(std::make_unique<shard_reader_v2>(_schema, _permit, lifecycle_policy, i, pr, ps, trace_state, fwd_mr));
    }
}

future<> multishard_combining_reader_v2::fill_buffer() {
    _crossed_shards = false;
    return do_until([this] { return is_buffer_full() || is_end_of_stream(); }, [this] {
        auto& reader = *_shard_readers[_current_shard];

        if (reader.is_buffer_empty()) {
            return handle_empty_reader_buffer();
        }

        while (!reader.is_buffer_empty() && !is_buffer_full()) {
            if (const auto& mf = reader.peek_buffer(); mf.is_partition_start() && maybe_move_to_next_shard(&mf.as_partition_start().key().token())) {
                return make_ready_future<>();
            }
            push_mutation_fragment(reader.pop_mutation_fragment());
        }
        return make_ready_future<>();
    });
}

future<> multishard_combining_reader_v2::next_partition() {
    clear_buffer_to_next_partition();
    if (is_buffer_empty()) {
        return _shard_readers[_current_shard]->next_partition();
    }
    return make_ready_future<>();
}

future<> multishard_combining_reader_v2::fast_forward_to(const dht::partition_range& pr) {
    clear_buffer();
    _end_of_stream = false;
    on_partition_range_change(pr);
    return parallel_for_each(_shard_readers, [&pr] (std::unique_ptr<shard_reader_v2>& sr) {
        return sr->fast_forward_to(pr);
    });
}

future<> multishard_combining_reader_v2::fast_forward_to(position_range pr) {
    return make_exception_future<>(make_backtraced_exception_ptr<std::bad_function_call>());
}

future<> multishard_combining_reader_v2::close() noexcept {
    return parallel_for_each(_shard_readers, [] (std::unique_ptr<shard_reader_v2>& sr) {
        return sr->close();
    });
}

mutation_reader make_multishard_combining_reader_v2(
        shared_ptr<reader_lifecycle_policy_v2> lifecycle_policy,
        schema_ptr schema,
        locator::effective_replication_map_ptr erm,
        reader_permit permit,
        const dht::partition_range& pr,
        const query::partition_slice& ps,
        tracing::trace_state_ptr trace_state,
        mutation_reader::forwarding fwd_mr) {
    auto& sharder = erm->get_sharder(*schema);
    return make_mutation_reader<multishard_combining_reader_v2>(sharder, std::any(std::move(erm)), std::move(lifecycle_policy),
            std::move(schema), std::move(permit), pr, ps, std::move(trace_state), fwd_mr);
}

mutation_reader make_multishard_combining_reader_v2_for_tests(
        const dht::sharder& sharder,
        shared_ptr<reader_lifecycle_policy_v2> lifecycle_policy,
        schema_ptr schema,
        reader_permit permit,
        const dht::partition_range& pr,
        const query::partition_slice& ps,
        tracing::trace_state_ptr trace_state,
        mutation_reader::forwarding fwd_mr) {
    return make_mutation_reader<multishard_combining_reader_v2>(sharder, std::any(),
            std::move(lifecycle_policy), std::move(schema), std::move(permit), pr, ps, std::move(trace_state), fwd_mr);
}
