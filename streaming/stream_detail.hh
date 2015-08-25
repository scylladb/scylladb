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

#include "query-request.hh"
#include "mutation_reader.hh"
#include "utils/UUID.hh"
#include <vector>

namespace streaming {

struct stream_detail {
    using UUID = utils::UUID;
    UUID cf_id;
    lw_shared_ptr<mutation_reader> mr;
    int64_t estimated_keys;
    int64_t repaired_at;
    stream_detail() = default;
    stream_detail(UUID cf_id_, mutation_reader mr_, long estimated_keys_, long repaired_at_)
        : cf_id(std::move(cf_id_))
        , mr(make_lw_shared(std::move(mr_)))
        , estimated_keys(estimated_keys_)
        , repaired_at(repaired_at_) {
    }
};

} // namespace streaming
