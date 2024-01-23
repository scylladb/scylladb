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

namespace streaming {

/**
 * Task that manages receiving files for the session for certain ColumnFamily.
 */
class stream_receive_task : public stream_task {
private:
    // number of files to receive
    int total_files;
    // total size of files to receive
    long total_size;
public:
    stream_receive_task(shared_ptr<stream_session> _session, table_id _cf_id, int _total_files, long _total_size);
    ~stream_receive_task();

    virtual int get_total_number_of_files() const override {
        return total_files;
    }

    virtual long get_total_size() const override {
        return total_size;
    }


    /**
     * Abort this task.
     * If the task already received all files and
     * {@link org.apache.cassandra.streaming.StreamReceiveTask.OnCompletionRunnable} task is submitted,
     * then task cannot be aborted.
     */
    virtual void abort() override {
    }
};

} // namespace streaming
