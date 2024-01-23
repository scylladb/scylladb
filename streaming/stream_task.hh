/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "schema/schema_fwd.hh"
#include "streaming/stream_summary.hh"
#include <seastar/core/shared_ptr.hh>

namespace streaming {

class stream_session;

/**
 * StreamTask is an abstraction of the streaming task performed over specific ColumnFamily.
 */
class stream_task {
public:
    /** StreamSession that this task belongs */
    shared_ptr<stream_session> session;

    table_id cf_id;

    stream_task(shared_ptr<stream_session> _session, table_id _cf_id);
    virtual ~stream_task();

public:
    /**
     * @return total number of files this task receives/streams.
     */
    virtual int get_total_number_of_files() const = 0;

    /**
     * @return total bytes expected to receive
     */
    virtual long get_total_size() const = 0;

    /**
     * Abort the task.
     * Subclass should implement cleaning up resources.
     */
    virtual void abort() = 0;

    /**
     * @return StreamSummary that describes this task
     */
    virtual stream_summary get_summary() const {
        return stream_summary(this->cf_id, this->get_total_number_of_files(), this->get_total_size());
    }
};

} // namespace streaming
