/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modified by Cloudius Systems.
 * Copyright 2015 Cloudius Systems.
 */

#pragma once

#include "utils/UUID.hh"
#include "streaming/stream_summary.hh"
#include <memory>
#include "core/shared_ptr.hh"

namespace streaming {

class stream_session;

/**
 * StreamTask is an abstraction of the streaming task performed over specific ColumnFamily.
 */
class stream_task {
public:
    using UUID = utils::UUID;
    /** StreamSession that this task belongs */
    shared_ptr<stream_session> session;

    UUID cf_id;

    stream_task(shared_ptr<stream_session> _session, UUID _cf_id);
    ~stream_task();

public:
    /**
     * @return total number of files this task receives/streams.
     */
    virtual int get_total_number_of_files() = 0;

    /**
     * @return total bytes expected to receive
     */
    virtual long get_total_size() = 0;

    /**
     * Abort the task.
     * Subclass should implement cleaning up resources.
     */
    virtual void abort() = 0;

    /**
     * @return StreamSummary that describes this task
     */
    virtual stream_summary get_summary() {
        return stream_summary(this->cf_id, this->get_total_number_of_files(), this->get_total_size());
    }
};

} // namespace streaming
