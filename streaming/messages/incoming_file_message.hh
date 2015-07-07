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

#include "streaming/messages/stream_message.hh"
#include "streaming/messages/file_message_header.hh"
#include "sstables/sstables.hh"
#include "mutation_reader.hh"

namespace streaming {
namespace messages {

/**
 * IncomingFileMessage is used to receive the part(or whole) of a SSTable data file.
 */
class incoming_file_message : public stream_message {
public:
    file_message_header header;

    incoming_file_message() = default;
    incoming_file_message(file_message_header header_, mutation_reader mr_)
        : stream_message(stream_message::Type::FILE)
        , header(std::move(header_)) {
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
