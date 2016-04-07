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
 * Copyright (C) 2015 ScyllaDB
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

#include "gms/inet_address.hh"
#include "core/sstring.hh"

namespace streaming {

/**
 * ProgressInfo contains file transfer progress.
 */
class progress_info {
public:
    using inet_address = gms::inet_address;
    /**
     * Direction of the stream.
     */
    enum class direction { OUT, IN };

    inet_address peer;
    sstring file_name;
    direction dir;
    long current_bytes;
    long total_bytes;

    progress_info() = default;
    progress_info(inet_address _peer, sstring _file_name, direction _dir, long _current_bytes, long _total_bytes)
        : peer(_peer)
        , file_name(_file_name)
        , dir(_dir)
        , current_bytes(_current_bytes)
        , total_bytes(_total_bytes) {
    }

    /**
     * @return true if file transfer is completed
     */
    bool is_completed() {
        return current_bytes >= total_bytes;
    }

    friend std::ostream& operator<<(std::ostream& os, const progress_info& x);
};

} // namespace streaming
