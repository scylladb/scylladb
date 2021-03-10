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

#include "shared_sstable.hh"
#include "row.hh"
#include "sstables.hh"

namespace sstables {

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
inline std::unique_ptr<DataConsumeRowsContext> data_consume_rows(const schema& s, shared_sstable sst, typename DataConsumeRowsContext::consumer& consumer, sstable::disk_read_range toread, uint64_t last_end) {
    // Although we were only asked to read until toread.end, we'll not limit
    // the underlying file input stream to this end, but rather to last_end.
    // This potentially enables read-ahead beyond end, until last_end, which
    // can be beneficial if the user wants to fast_forward_to() on the
    // returned context, and may make small skips.
    auto input = sst->data_stream(toread.start, last_end - toread.start, consumer.io_priority(),
            consumer.permit(), consumer.trace_state(), sst->_partition_range_history);
    return std::make_unique<DataConsumeRowsContext>(s, std::move(sst), consumer, std::move(input), toread.start, toread.end - toread.start);
}

template <typename DataConsumeRowsContext>
inline std::unique_ptr<DataConsumeRowsContext> data_consume_single_partition(const schema& s, shared_sstable sst, typename DataConsumeRowsContext::consumer& consumer, sstable::disk_read_range toread) {
    auto input = sst->data_stream(toread.start, toread.end - toread.start, consumer.io_priority(),
            consumer.permit(), consumer.trace_state(), sst->_single_partition_history);
    return std::make_unique<DataConsumeRowsContext>(s, std::move(sst), consumer, std::move(input), toread.start, toread.end - toread.start);
}

// Like data_consume_rows() with bounds, but iterates over whole range
template <typename DataConsumeRowsContext>
inline std::unique_ptr<DataConsumeRowsContext> data_consume_rows(const schema& s, shared_sstable sst, typename DataConsumeRowsContext::consumer& consumer) {
        auto data_size = sst->data_size();
        return data_consume_rows<DataConsumeRowsContext>(s, std::move(sst), consumer, {0, data_size}, data_size);
}

}
