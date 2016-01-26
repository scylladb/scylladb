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
#include "streaming/messages/stream_message.hh"
#include "streaming/stream_detail.hh"
#include "sstables/sstables.hh"
#include <seastar/core/semaphore.hh>

namespace streaming {
namespace messages {

/**
 * OutgoingFileMessage is used to transfer the part(or whole) of a SSTable data file.
 */
class outgoing_file_message : public stream_message {
    using UUID = utils::UUID;
    using format_types = sstables::sstable::format_types;
public:
    int32_t sequence_number;
    stream_detail detail;

    size_t mutations_nr{0};
    semaphore mutations_done{0};

    outgoing_file_message() = default;
    outgoing_file_message(int32_t sequence_number_, stream_detail detail_)
        : stream_message(stream_message::Type::FILE)
        , sequence_number(sequence_number_)
        , detail(std::move(detail_)) {
    }

#if 0
    @Override
    public String toString()
    {
        return "File (" + header + ", file: " + sstable.getFilename() + ")";
    }
#endif
};

} // namespace messages
} // namespace streaming

