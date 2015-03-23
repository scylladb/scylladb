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
 * Modified by Cloudius Systems
 *
 * Copyright 2015 Cloudius Systems
 */

#pragma once

#include "native_scalar_function.hh"
#include "utils/UUID_gen.hh"
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>

namespace cql3 {

namespace functions {

namespace time_uuid_fcts {

inline
shared_ptr<function>
make_now_fct() {
    return make_native_scalar_function<false>("now", timeuuid_type, {},
            [] (int protocol_version, const std::vector<bytes>& values) {
        return to_bytes(utils::UUID_gen::get_time_UUID());
    });
}

inline
shared_ptr<function>
make_min_timeuuid_fct() {
    return make_native_scalar_function<true>("mintimeuuid", timeuuid_type, { timestamp_type },
            [] (int protocol_version, const std::vector<bytes>& values) {
        // FIXME: should values be a vector<optional<bytes>>?
        auto& bb = values[0];
        if (bb.empty()) {
            // FIXME: better null representation?
            return bytes();
        }
        auto ts_obj = timestamp_type->compose(bb);
        if (ts_obj.empty()) {
            return bytes();
        }
        auto ts = boost::any_cast<db_clock::time_point>(ts_obj);
        auto uuid = utils::UUID_gen::min_time_UUID(ts.time_since_epoch().count());
        return timeuuid_type->decompose(uuid);
    });
}

inline
shared_ptr<function>
make_max_timeuuid_fct() {
    return make_native_scalar_function<true>("maxtimeuuid", timeuuid_type, { timestamp_type },
            [] (int protocol_version, const std::vector<bytes>& values) {
        // FIXME: should values be a vector<optional<bytes>>?
        auto& bb = values[0];
        if (bb.empty()) {
            // FIXME: better null representation?
            return bytes();
        }
        auto ts_obj = timestamp_type->compose(bb);
        if (ts_obj.empty()) {
            return bytes();
        }
        auto ts = boost::any_cast<db_clock::time_point>(ts_obj);
        auto uuid = utils::UUID_gen::max_time_UUID(ts.time_since_epoch().count());
        return timeuuid_type->decompose(uuid);
    });
}

inline
shared_ptr<function>
make_date_of_fct() {
    return make_native_scalar_function<true>("dateof", timestamp_type, { timeuuid_type },
            [] (int protocol, const std::vector<bytes>& values) {
        using namespace utils;
        auto& bb = values[0];
        if (bb.empty()) {
            return bytes();
        }
        auto ts = db_clock::time_point(db_clock::duration(UUID_gen::unix_timestamp(UUID_gen::get_UUID(bb))));
        return timestamp_type->decompose(ts);
    });
}

inline
shared_ptr<function>
make_unix_timestamp_of_fcf() {
    return make_native_scalar_function<true>("unixtimestampof", long_type, { timeuuid_type },
            [] (int protocol, const std::vector<bytes>& values) {
        using namespace utils;
        auto& bb = values[0];
        if (bb.empty()) {
            return bytes();
        }
        return long_type->decompose(UUID_gen::unix_timestamp(UUID_gen::get_UUID(bb)));
    });
}

}
}
}
