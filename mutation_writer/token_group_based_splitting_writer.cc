/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "mutation_writer/token_group_based_splitting_writer.hh"

#include <seastar/core/shared_mutex.hh>
#include <seastar/core/on_internal_error.hh>

#include "mutation_writer/feed_writers.hh"
#include "utils/error_injection.hh"

namespace mutation_writer {

static logging::logger logger("token_group_based_splitting_mutation_writer");

class token_group_based_splitting_mutation_writer {
    schema_ptr _schema;
    reader_permit _permit;
    classify_by_token_group _classify;
    mutation_reader_consumer _consumer;
    token_group_id _current_group_id = 0;
    std::optional<bucket_writer> _current_writer;
    // Token range tombstones delete whole partitions, so they belong in every
    // output, not just the one the current partition goes to. They arrive
    // before any partition, when there is no writer yet, so they are kept here
    // and replayed into each writer as it is created.
    token_range_tombstone_list _token_range_tombstones;
    // The group ids the tombstones reach, computed on first use. Group ids are
    // monotonic in token order and the tombstones cover whole token ranges, so
    // the ids they reach form an interval.
    mutable std::optional<std::pair<token_group_id, token_group_id>> _tombstone_groups;
    // The next group id which still has to receive the tombstones. Groups are
    // written in increasing id order, so once we move past a group it is too
    // late to give it anything.
    token_group_id _next_tombstone_group = 0;
private:
    // The first and last token a tombstone deletes, as integers, so that the
    // group boundaries within it can be found by bisection. An empty range
    // (first > last) means the tombstone deletes nothing.
    static std::pair<int64_t, int64_t> tombstone_token_bounds(const token_range_tombstone& trt) {
        auto first = trt.start_exclusive().is_minimum()
                ? dht::token::to_int64(dht::first_token())
                : dht::token::to_int64(trt.start_exclusive()) + 1;
        auto last = trt.end_inclusive().is_maximum()
                ? std::numeric_limits<int64_t>::max()
                : dht::token::to_int64(trt.end_inclusive());
        return {first, last};
    }

    token_group_id classify(int64_t t) const {
        return _classify(dht::token::from_int64(t));
    }

    std::pair<token_group_id, token_group_id> tombstone_groups() const {
        if (!_tombstone_groups) {
            auto lo = std::numeric_limits<token_group_id>::max();
            auto hi = std::numeric_limits<token_group_id>::min();
            for (const auto& trt : _token_range_tombstones) {
                auto [first, last] = tombstone_token_bounds(trt);
                if (first > last) {
                    continue;
                }
                lo = std::min(lo, classify(first));
                hi = std::max(hi, classify(last));
            }
            _tombstone_groups = std::make_pair(lo, hi);
        }
        return *_tombstone_groups;
    }

    // The part of `trt` which falls in `group`, or nothing if it does not reach
    // it. The classifier only maps a token to its group, so the boundaries are
    // found by bisection over the tombstone's token range.
    std::optional<token_range_tombstone> clamp_to_group(const token_range_tombstone& trt, token_group_id group) const {
        auto [first, last] = tombstone_token_bounds(trt);
        if (first > last || classify(first) > group || classify(last) < group) {
            return std::nullopt;
        }
        // The range can span the whole ring, so the midpoint is computed in
        // unsigned arithmetic; `hi - lo` would overflow.
        auto distance = [] (int64_t lo, int64_t hi) { return uint64_t(hi) - uint64_t(lo); };
        auto midpoint = [&] (int64_t lo, int64_t hi) { return int64_t(uint64_t(lo) + distance(lo, hi) / 2); };
        auto a = first;
        if (classify(first) < group) {
            // Smallest token in [first, last] whose group is at least `group`.
            auto lo = first, hi = last;
            while (distance(lo, hi) > 1) {
                auto mid = midpoint(lo, hi);
                (classify(mid) < group ? lo : hi) = mid;
            }
            a = hi;
        }
        auto b = last;
        if (classify(last) > group) {
            // Largest token in [a, last] whose group is at most `group`.
            auto lo = a, hi = last;
            while (distance(lo, hi) > 1) {
                auto mid = midpoint(lo, hi);
                (classify(mid) > group ? hi : lo) = mid;
            }
            b = lo;
        }
        auto start = a == first && trt.start_exclusive().is_minimum()
                ? dht::minimum_token()
                : dht::token::from_int64(a - 1);
        auto end = b == last && trt.end_inclusive().is_maximum()
                ? dht::maximum_token()
                : dht::token::from_int64(b);
        return token_range_tombstone(std::move(start), std::move(end), trt.tomb());
    }

    future<> write_token_range_tombstones() {
        for (const auto& trt : _token_range_tombstones) {
            if (auto clamped = clamp_to_group(trt, _current_group_id)) {
                co_await _current_writer->consume(mutation_fragment_v2(*_schema, _permit, std::move(*clamped)));
            }
        }
    }

    // Gives the tombstones to every group below `upto` which holds no
    // partition, and so never gets a writer of its own along the normal path.
    // An sstable which only deletes a token range has no partitions at all, so
    // without this the deletion would be dropped by the split.
    future<> write_tombstone_only_groups(token_group_id upto) {
        if (_token_range_tombstones.empty()) {
            co_return;
        }
        auto [lo, hi] = tombstone_groups();
        auto first = std::max(_next_tombstone_group, lo);
        auto end = std::min(upto, token_group_id(hi + 1));
        if (first >= end) {
            co_return;
        }
        // Each group is its own output, so the writer in progress, if any, has
        // to be finished before the ones below are produced. Leaving
        // _current_writer unset lets the normal path allocate the next one.
        if (_current_writer) {
            co_await _current_writer->consume_end_of_stream();
            auto wr = std::exchange(_current_writer, std::nullopt);
            co_await wr->close();
        }
        for (auto g = first; g < end; g++) {
            _current_writer = bucket_writer(_schema, _permit, _consumer);
            _current_group_id = g;
            co_await write_token_range_tombstones();
            co_await _current_writer->consume_end_of_stream();
            auto wr = std::exchange(_current_writer, std::nullopt);
            co_await wr->close();
        }
        _next_tombstone_group = end;
    }

    future<> write(mutation_fragment_v2&& mf) {
        return _current_writer->consume(std::move(mf));
    }

    bool _needs_token_range_tombstones = true;

    inline void allocate_new_writer_if_needed() {
        if (!_current_writer) [[unlikely]] {
            _current_writer = bucket_writer(_schema, _permit, _consumer);
            // A fresh writer produces its own output, which needs its own copy.
            _needs_token_range_tombstones = true;
        }
    }

    // Keeps the previous writer alive while closed
    // and then allocates a new write, if needed.
    future<> do_switch_to_new_writer() {
        co_await _current_writer->consume_end_of_stream();
        // reset _current_writer while closing the previous one
        // to prevent race with close() after abort()
        auto wr = std::exchange(_current_writer, std::nullopt);
        co_await wr->close();
        allocate_new_writer_if_needed();
        co_await utils::get_local_injector().inject("splitting_mutation_writer_switch_wait", utils::wait_for_message(std::chrono::seconds(60)));
    }

    // Called frequently, hence yields (and allocates)
    // only on the unlikely slow path.
    future<> maybe_switch_to_new_writer(dht::token t) {
        auto prev_group_id = _current_group_id;
        _current_group_id = _classify(t);

        if (_current_group_id < prev_group_id) [[unlikely]] {
            on_internal_error(logger, format("Token group id cannot go backwards, current={}, previous={}", _current_group_id, prev_group_id));
        }

        if (_current_writer && _current_group_id > prev_group_id) [[unlikely]] {
            return do_switch_to_new_writer();
        }
        allocate_new_writer_if_needed();
        return make_ready_future<>();
    }
public:
    token_group_based_splitting_mutation_writer(schema_ptr schema, reader_permit permit, classify_by_token_group classify, mutation_reader_consumer consumer)
        : _schema(std::move(schema))
        , _permit(std::move(permit))
        , _classify(std::move(classify))
        , _consumer(std::move(consumer))
    {}

    future<> consume(partition_start&& ps) {
        // Groups before this partition's own will get no partition of their
        // own, so this is the last chance to give them the tombstones.
        co_await write_tombstone_only_groups(_classify(ps.key().token()));
        co_await maybe_switch_to_new_writer(ps.key().token());
        if (std::exchange(_needs_token_range_tombstones, false)) {
            co_await write_token_range_tombstones();
            _next_tombstone_group = std::max(_next_tombstone_group, token_group_id(_current_group_id + 1));
        }
        co_await write(mutation_fragment_v2(*_schema, _permit, std::move(ps)));
    }

    future<> consume(static_row&& sr) {
        return write(mutation_fragment_v2(*_schema, _permit, std::move(sr)));
    }

    future<> consume(clustering_row&& cr) {
        return write(mutation_fragment_v2(*_schema, _permit, std::move(cr)));
    }

    future<> consume(token_range_tombstone&& trt) {
        _token_range_tombstones.apply(trt);
        return make_ready_future<>();
    }

    future<> consume(range_tombstone_change&& rt) {
        return write(mutation_fragment_v2(*_schema, _permit, std::move(rt)));
    }

    future<> consume(partition_end&& pe) {
        return write(mutation_fragment_v2(*_schema, _permit, std::move(pe)));
    }

    future<> consume_end_of_stream() {
        if (_current_writer) {
            co_await _current_writer->consume_end_of_stream();
            auto wr = std::exchange(_current_writer, std::nullopt);
            co_await wr->close();
        }
        // Groups after the last partition, up to the end of what the
        // tombstones reach, still have to receive them.
        co_await write_tombstone_only_groups(std::numeric_limits<token_group_id>::max());
    }
    void abort(std::exception_ptr ep) {
        if (_current_writer) {
            _current_writer->abort(ep);
        }
    }
    future<> close() noexcept {
        return _current_writer ? _current_writer->close() : make_ready_future<>();
    }
};

future<> segregate_by_token_group(mutation_reader producer, classify_by_token_group classify, mutation_reader_consumer consumer) {
    auto schema = producer.schema();
    auto permit = producer.permit();
    return feed_writer(
        std::move(producer),
        token_group_based_splitting_mutation_writer(std::move(schema), std::move(permit), std::move(classify), std::move(consumer)));
}
} // namespace mutation_writer
