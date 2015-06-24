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

namespace streaming {
namespace messages {

/**
 * StreamMessage is an abstract base class that every messages in streaming protocol inherit.
 *
 * Every message carries message type({@link Type}) and streaming protocol version byte.
 */
class stream_message {
public:
    enum class Type {
        PREPARE,
        FILE,
        RECEIVED,
        RETRY,
        COMPLETE,
        SESSION_FAILED,
    };

    Type type;
    int priority;

    stream_message() = default;

    stream_message(Type type_)
        : type(type_) {
        if (type == Type::PREPARE) {
            priority = 5;
        } else if (type == Type::FILE) {
            priority = 0;
        } else if (type == Type::RECEIVED) {
            priority = 4;
        } else if (type == Type::RETRY) {
            priority = 4;
        } else if (type == Type::COMPLETE) {
            priority = 1;
        } else if (type == Type::SESSION_FAILED) {
            priority = 5;
        }
    }

    /**
     * @return priority of this message. higher value, higher priority.
     */
    int get_priority() {
        return priority;
    }
};

} // namespace messages
} // namespace streaming
