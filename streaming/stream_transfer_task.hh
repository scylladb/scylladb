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
#include "streaming/stream_task.hh"
#include "streaming/stream_detail.hh"
#include "sstables/sstables.hh"
#include <map>
#include <seastar/core/semaphore.hh>

namespace streaming {

class stream_session;

/**
 * StreamTransferTask sends sections of SSTable files in certain ColumnFamily.
 */
class stream_transfer_task : public stream_task {
private:
    int32_t sequence_number = 0;
    bool aborted = false;
    // A stream_transfer_task always contains the same range to stream
    std::vector<range<dht::token>> _ranges;
    long _total_size;
public:
    using UUID = utils::UUID;
    stream_transfer_task(stream_transfer_task&&) = default;
    stream_transfer_task(shared_ptr<stream_session> session, UUID cf_id, std::vector<range<dht::token>> ranges, long total_size = 0);
    ~stream_transfer_task();
public:
    virtual void abort() override {
    }

    virtual int get_total_number_of_files() override {
        return 1;
    }

    virtual long get_total_size() override {
        return _total_size;
    }

    void start();
};

} // namespace streaming
