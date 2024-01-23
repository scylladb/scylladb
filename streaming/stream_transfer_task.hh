/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "streaming/stream_fwd.hh"
#include "streaming/stream_task.hh"
#include "dht/i_partitioner_fwd.hh"
#include <seastar/core/semaphore.hh>

namespace streaming {

class send_info;

/**
 * StreamTransferTask sends sections of SSTable files in certain ColumnFamily.
 */
class stream_transfer_task : public stream_task {
private:
    // A stream_transfer_task always contains the same range to stream
    dht::token_range_vector _ranges;
    std::map<unsigned, dht::partition_range_vector> _shard_ranges;
    long _total_size;
    bool _mutation_done_sent = false;
public:
    stream_transfer_task(stream_transfer_task&&) = default;
    stream_transfer_task(shared_ptr<stream_session> session, table_id cf_id, dht::token_range_vector ranges, long total_size = 0);
    ~stream_transfer_task();
public:
    virtual void abort() override {
    }

    virtual int get_total_number_of_files() const override {
        return 1;
    }

    virtual long get_total_size() const override {
        return _total_size;
    }

    future<> execute();

    void append_ranges(const dht::token_range_vector& ranges);
    void sort_and_merge_ranges();
};

} // namespace streaming
