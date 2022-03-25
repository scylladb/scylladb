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
