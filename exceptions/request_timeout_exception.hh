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

#include "exceptions/exceptions.hh"
#include "db/consistency_level.hh"

namespace exceptions {

class request_timeout_exception : public cassandra_exception {
public:
    db::consistency_level consistency;
    int32_t received;
    int32_t block_for;

    request_timeout_exception(exception_code code, db::consistency_level consistency, int32_t received, int32_t block_for)
        : cassandra_exception{code, sprint("Operation timed out - received only %d responses.", received)}
        , consistency{consistency}
        , received{received}
        , block_for{block_for}
    { }
};

class read_timeout_exception : public request_timeout_exception {
public:
    bool data_present;

    read_timeout_exception(db::consistency_level consistency, int32_t received, int32_t block_for, bool data_present)
        : request_timeout_exception{exception_code::READ_TIMEOUT, consistency, received, block_for}
        , data_present{data_present}
    { }
};

}
