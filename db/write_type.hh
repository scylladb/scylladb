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

#pragma once

#include <assert.h>

namespace db {

enum class write_type : uint8_t {
    SIMPLE,
    BATCH,
    UNLOGGED_BATCH,
    COUNTER,
    BATCH_LOG,
    CAS,
};

inline std::ostream& operator<<(std::ostream& os, const write_type& t) {
    switch(t) {
        case write_type::SIMPLE: os << "SIMPLE"; break;
        case write_type::BATCH: os << "BATCH"; break;
        case write_type::UNLOGGED_BATCH: os << "UNLOGGED_BATCH"; break;
        case write_type::COUNTER: os << "COUNTER"; break;
        case write_type::BATCH_LOG: os << "BATCH_LOG"; break;
        case write_type::CAS: os << "CAS"; break;
        default:
            assert(false);
    }
    return os;
}

}


