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
 * Copyright (C) 2021-present ScyllaDB
 *
 */

#pragma once

#include "seastarx.hh"
#include <seastar/core/semaphore.hh>

namespace service {

class memory_limiter final {
    size_t _mem_total;
    semaphore _sem;

public:
    memory_limiter(size_t available_memory) noexcept
        : _mem_total(available_memory / 10)
        , _sem(_mem_total) {}

    future<> stop() {
        return _sem.wait(_mem_total);
    }

    size_t total_memory() const noexcept { return _mem_total; }
    semaphore& get_semaphore() noexcept { return _sem; }
};

} // namespace service
