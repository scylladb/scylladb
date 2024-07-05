/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/future-util.hh>
#include <seastar/core/coroutine.hh>
#include "keys.hh"
#include <seastar/core/do_with.hh>
#include <seastar/core/byteorder.hh>
#include "index_reader.hh"
#include "sstables/mx/partition_reversing_data_source.hh"

namespace sstables {

namespace kl {
    class mp_row_consumer_k_l;
}

namespace mx {
    class mp_row_consumer_m;
}

class mp_row_consumer_reader_base {
public:
    using tracker_link_type = bi::list_member_hook<bi::link_mode<bi::auto_unlink>>;
    friend class reader_tracker;
protected:
    shared_sstable _sst;

    tracker_link_type _tracker_link;

    // Whether index lower bound is in current partition
    bool _index_in_current_partition = false;

    // True iff the consumer finished generating fragments for a partition and hasn't
    // entered the new partition yet.
    // Implies that partition_end was emitted for the last partition.
    // Will cause the reader to skip to the next partition if !_before_partition.
    bool _partition_finished = true;

    // When set, the consumer is positioned right before a partition or at end of the data file.
    // _index_in_current_partition applies to the partition which is about to be read.
    bool _before_partition = true;

    std::optional<dht::decorated_key> _current_partition_key;
public:
    mp_row_consumer_reader_base(shared_sstable sst);

    // Called when all fragments relevant to the query range or fast forwarding window
    // within the current partition have been pushed.
    // If no skipping is required, this method may not be called before transitioning
    // to the next partition.
    virtual void on_out_of_clustering_range() = 0;
};

inline atomic_cell make_atomic_cell(const abstract_type& type,
                                    api::timestamp_type timestamp,
                                    fragmented_temporary_buffer::view value,
                                    gc_clock::duration ttl,
                                    gc_clock::time_point expiration,
                                    atomic_cell::collection_member cm) {
    if (ttl != gc_clock::duration::zero()) {
        return atomic_cell::make_live(type, timestamp, value, expiration, ttl, cm);
    } else {
        return atomic_cell::make_live(type, timestamp, value, cm);
    }
}

atomic_cell make_counter_cell(api::timestamp_type timestamp, fragmented_temporary_buffer::view value);

position_in_partition_view get_slice_upper_bound(const schema& s, const query::partition_slice& slice, dht::ring_position_view key);

// data_consume_rows() iterates over rows in the data file from
// a particular range, feeding them into the consumer. The iteration is
// done as efficiently as possible - reading only the data file (not the
// summary or index files) and reading data in batches.
//
// The consumer object may request the iteration to stop before reaching
// the end of the requested data range (e.g. stop after each sstable row).
// A context object is returned which allows to resume this consumption:
// This context's read() method requests that consumption begins, and
// returns a future which will be resolved when it ends (because the
// consumer asked to stop, or the data range ended). Only after the
// returned future is resolved, may read() be called again to consume
// more.
// The caller must ensure (e.g., using do_with()) that the context object,
// as well as the sstable, remains alive as long as a read() is in
// progress (i.e., returned a future which hasn't completed yet).
//
// The "toread" range specifies the range we want to read initially.
// However, the object returned by the read, a data_consume_context, also
// provides a fast_forward_to(start,end) method which allows resetting
// the reader to a new range. To allow that, we also have a "last_end"
// byte which should be the last end to which fast_forward_to is
// eventually allowed. If last_end==end, fast_forward_to is not allowed
// at all, if last_end==file_size fast_forward_to is allowed until the
// end of the file, and it can be something in between if we know that we
// are planning to skip parts, but eventually read until last_end.
// When last_end==end, we guarantee that the read will only read the
// desired byte range from disk. However, when last_end > end, we may
// read beyond end in anticipation of a small skip via fast_foward_to.
// The amount of this excessive read is controlled by read ahead
// heuristics which learn from the usefulness of previous read aheads.
template <typename DataConsumeRowsContext>
inline std::unique_ptr<DataConsumeRowsContext> data_consume_rows(const schema& s, shared_sstable sst, typename DataConsumeRowsContext::consumer& consumer, sstable::disk_read_range toread, uint64_t last_end) {
    // Although we were only asked to read until toread.end, we'll not limit
    // the underlying file input stream to this end, but rather to last_end.
    // This potentially enables read-ahead beyond end, until last_end, which
    // can be beneficial if the user wants to fast_forward_to() on the
    // returned context, and may make small skips.
    auto input = sst->data_stream(toread.start, last_end - toread.start,
            consumer.permit(), consumer.trace_state(), sst->_partition_range_history);
    return std::make_unique<DataConsumeRowsContext>(s, std::move(sst), consumer, std::move(input), toread.start, toread.end - toread.start);
}

template <typename DataConsumeRowsContext>
struct reversed_context {
    std::unique_ptr<DataConsumeRowsContext> the_context;

    // Underneath, the context is iterating over the sstable file in reverse order.
    // This points to the current position of the context over the underlying sstable file;
    // either the end of partition or the beginning of some row (never in the middle of a row).
    // The reference is valid as long as the context is alive.
    const uint64_t& current_position_in_sstable;
};

// See `sstables::mx::make_partition_reversing_data_source` for documentation.
template <typename DataConsumeRowsContext>
inline reversed_context<DataConsumeRowsContext> data_consume_reversed_partition(
        const schema& s, shared_sstable sst, index_reader& ir,
        typename DataConsumeRowsContext::consumer& consumer, sstable::disk_read_range toread) {
    auto reversing_data_source = sstables::mx::make_partition_reversing_data_source(
            s, sst, ir, toread.start, toread.end - toread.start,
            consumer.permit(), consumer.trace_state());
    return reversed_context<DataConsumeRowsContext> {
        .the_context = std::make_unique<DataConsumeRowsContext>(
                s, std::move(sst), consumer, input_stream<char>(std::move(reversing_data_source.the_source)),
                toread.start, toread.end - toread.start),
        .current_position_in_sstable = reversing_data_source.current_position_in_sstable
    };
}

template <typename DataConsumeRowsContext>
inline std::unique_ptr<DataConsumeRowsContext> data_consume_single_partition(const schema& s, shared_sstable sst, typename DataConsumeRowsContext::consumer& consumer, sstable::disk_read_range toread) {
    auto input = sst->data_stream(toread.start, toread.end - toread.start,
            consumer.permit(), consumer.trace_state(), sst->_single_partition_history);
    return std::make_unique<DataConsumeRowsContext>(s, std::move(sst), consumer, std::move(input), toread.start, toread.end - toread.start);
}

// Like data_consume_rows() with bounds, but iterates over whole range
template <typename DataConsumeRowsContext>
inline std::unique_ptr<DataConsumeRowsContext> data_consume_rows(const schema& s, shared_sstable sst, typename DataConsumeRowsContext::consumer& consumer) {
        auto data_size = sst->data_size();
        return data_consume_rows<DataConsumeRowsContext>(s, std::move(sst), consumer, {0, data_size}, data_size);
}

template<typename T>
concept RowConsumer =
    requires(T t,
                    const partition_key& pk,
                    position_range cr) {
        { t.is_mutation_end() } -> std::same_as<bool>;
        { t.setup_for_partition(pk) } -> std::same_as<void>;
        { t.push_ready_fragments() } -> std::same_as<void>;
        { t.maybe_skip() } -> std::same_as<std::optional<position_in_partition_view>>;
        { t.fast_forward_to(std::move(cr)) } -> std::same_as<std::optional<position_in_partition_view>>;
    };

/*
 * Helper method to set or reset the range tombstone start bound according to the
 * end open marker of a promoted index block.
 *
 * Only applies to consumers that have the following methods:
 *      void reset_range_tombstone_start();
 *      void set_range_tombstone_start(clustering_key_prefix, bound_kind, tombstone);
 *
 * For other consumers, it is a no-op.
 */
template <typename Consumer>
void set_range_tombstone_start_from_end_open_marker(Consumer& c, const schema& s, const index_reader& idx) {
    if constexpr (Consumer::is_setting_range_tombstone_start_supported) {
        auto open_end_marker = idx.end_open_marker();
        if (open_end_marker) {
            auto[pos, tomb] = *open_end_marker;
            c.set_range_tombstone_start(tomb);
        }
    }
}

}
