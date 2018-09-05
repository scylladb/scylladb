/*
 * Copyright (C) 2018 ScyllaDB
 *
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

#pragma once

#include <memory>

#include <seastar/core/future.hh>
#include <seastar/util/optimized_optional.hh>
#include <seastar/util/gcc6-concepts.hh>

#include "shared_sstable.hh"
#include "row.hh"
#include "sstables.hh"

namespace sstables {

class reader_position_tracker;

template <typename DataConsumeRowsContext>
data_consume_context<DataConsumeRowsContext>
data_consume_rows(const schema&, shared_sstable, typename DataConsumeRowsContext::consumer&, sstable::disk_read_range, uint64_t);

template <typename DataConsumeRowsContext>
data_consume_context<DataConsumeRowsContext>
data_consume_single_partition(const schema&, shared_sstable, typename DataConsumeRowsContext::consumer&, sstable::disk_read_range);

template <typename DataConsumeRowsContext>
data_consume_context<DataConsumeRowsContext>
data_consume_rows(const schema&, shared_sstable, typename DataConsumeRowsContext::consumer&);

// data_consume_context is an object returned by sstable::data_consume_rows()
// which allows knowing when the consumer stops reading, and starting it again
// (e.g., when the consumer wants to stop after every sstable row).
//
// The read() method initiates reading into the consumer, and continues to
// read and feed data into the consumer until one of the consumer's callbacks
// requests to stop,  or until we reach the end of the data range originally
// requested. read() returns a future which completes when reading stopped.
// If we're at the end-of-file, the read may complete without reading anything
// so it's the consumer class's task to check if anything was consumed.
// Note:
// The caller MUST ensure that between calling read() on this object,
// and the time the returned future is completed, the object lives on.
// Moreover, the sstable object used for the sstable::data_consume_rows()
// call which created this data_consume_context, must also be kept alive.
//
// data_consume_rows() and data_consume_rows_at_once() both can read just a
// single row or many rows. The difference is that data_consume_rows_at_once()
// is optimized to reading one or few rows (reading it all into memory), while
// data_consume_rows() uses a read buffer, so not all the rows need to fit
// memory in the same time (they are delivered to the consumer one by one).
template <typename DataConsumeRowsContext>
GCC6_CONCEPT(
    requires ConsumeRowsContext<DataConsumeRowsContext>()
)
class data_consume_context {
    shared_sstable _sst;
    std::unique_ptr<DataConsumeRowsContext> _ctx;

    template <typename Consumer>
    data_consume_context(const schema& s, shared_sstable sst, Consumer &consumer, input_stream<char> &&input, uint64_t start, uint64_t maxlen)
        : _sst(std::move(sst))
        , _ctx(std::make_unique<DataConsumeRowsContext>(s, _sst, consumer, std::move(input), start, maxlen))
    { }

    friend class sstable;
    friend data_consume_context<DataConsumeRowsContext>
    data_consume_rows<DataConsumeRowsContext>(const schema&, shared_sstable, typename DataConsumeRowsContext::consumer&, sstable::disk_read_range, uint64_t);
    friend data_consume_context<DataConsumeRowsContext>
    data_consume_single_partition<DataConsumeRowsContext>(const schema&, shared_sstable, typename DataConsumeRowsContext::consumer&, sstable::disk_read_range);
    friend data_consume_context<DataConsumeRowsContext>
    data_consume_rows<DataConsumeRowsContext>(const schema&, shared_sstable, typename DataConsumeRowsContext::consumer&);

    data_consume_context() = default;

    explicit operator bool() const noexcept {
        return bool(_ctx);
    }

    friend class optimized_optional<data_consume_context<DataConsumeRowsContext>>;

public:
    future<> read() {
        return _ctx->consume_input();
    }

    future<> fast_forward_to(uint64_t begin, uint64_t end) {
        _ctx->reset(indexable_element::partition);
        return _ctx->fast_forward_to(begin, end);
    }

    bool need_skip(uint64_t pos) const {
        return pos > _ctx->position();
    }

    future<> skip_to(indexable_element el, uint64_t begin) {
        sstlog.trace("data_consume_rows_context {}: skip_to({} -> {}, el={})", _ctx.get(), _ctx->position(), begin, static_cast<int>(el));
        if (begin <= _ctx->position()) {
            return make_ready_future<>();
        }
        _ctx->reset(el);
        return _ctx->skip_to(begin);
    }

    const reader_position_tracker &reader_position() const {
        return _ctx->reader_position();
    }

    bool eof() const {
        return _ctx->eof();
    }

    ~data_consume_context() {
        if (_ctx) {
            auto f = _ctx->close();
            f.handle_exception([ctx = std::move(_ctx), sst = std::move(_sst)](auto) {});
        }
    }

    data_consume_context(data_consume_context &&) noexcept = default;

    data_consume_context &operator=(data_consume_context &&) noexcept = default;
};

template <typename DataConsumeRowsContext>
using data_consume_context_opt = optimized_optional<data_consume_context<DataConsumeRowsContext>>;


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
// hueristics which learn from the usefulness of previous read aheads.
template <typename DataConsumeRowsContext>
inline data_consume_context<DataConsumeRowsContext> data_consume_rows(const schema& s, shared_sstable sst, typename DataConsumeRowsContext::consumer& consumer, sstable::disk_read_range toread, uint64_t last_end) {
    // Although we were only asked to read until toread.end, we'll not limit
    // the underlying file input stream to this end, but rather to last_end.
    // This potentially enables read-ahead beyond end, until last_end, which
    // can be beneficial if the user wants to fast_forward_to() on the
    // returned context, and may make small skips.
    auto input = sst->data_stream(toread.start, last_end - toread.start, consumer.io_priority(), consumer.resource_tracker(), sst->_partition_range_history);
    return {s, std::move(sst), consumer, std::move(input), toread.start, toread.end - toread.start };
}

template <typename DataConsumeRowsContext>
inline data_consume_context<DataConsumeRowsContext> data_consume_single_partition(const schema& s, shared_sstable sst, typename DataConsumeRowsContext::consumer& consumer, sstable::disk_read_range toread) {
    auto input = sst->data_stream(toread.start, toread.end - toread.start, consumer.io_priority(), consumer.resource_tracker(), sst->_single_partition_history);
    return {s, std::move(sst), consumer, std::move(input), toread.start, toread.end - toread.start };
}

// Like data_consume_rows() with bounds, but iterates over whole range
template <typename DataConsumeRowsContext>
inline data_consume_context<DataConsumeRowsContext> data_consume_rows(const schema& s, shared_sstable sst, typename DataConsumeRowsContext::consumer& consumer) {
        auto data_size = sst->data_size();
        return data_consume_rows<DataConsumeRowsContext>(s, std::move(sst), consumer, {0, data_size}, data_size);
}

}
