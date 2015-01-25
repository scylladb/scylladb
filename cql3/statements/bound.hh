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
 */

/*
 * Copyright 2015 Cloudius Systems
 *
 * Modified by Cloudius Systems
 */

#include <cstdint>

#pragma once

namespace cql3 {

namespace statements {

enum class bound : int32_t { START = 0, END };

static inline
int32_t get_idx(bound b) {
    return (int32_t)b;
}

static inline
bound reverse(bound b) {
    return bound((int32_t)b ^ 1);
}

static inline
bool is_start(bound b) {
    return b == bound::START;
}

static inline
bool is_end(bound b) {
    return b == bound::END;
}

}

}
