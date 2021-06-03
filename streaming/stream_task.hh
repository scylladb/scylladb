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
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
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

#include "utils/UUID.hh"
#include "streaming/stream_summary.hh"
#include <memory>
#include <seastar/core/shared_ptr.hh>

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
