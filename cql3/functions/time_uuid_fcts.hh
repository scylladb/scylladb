/*
 * Modified by ScyllaDB
 *
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "castas_fcts.hh"
#include "native_scalar_function.hh"
#include "utils/UUID_gen.hh"

namespace cql3 {

namespace functions {

namespace time_uuid_fcts {

inline
shared_ptr<function>
make_now_fct() {
    return make_native_scalar_function<false>("now", timeuuid_type, {},
            [] (std::span<const bytes_opt> values) -> bytes_opt {
        return {to_bytes(utils::UUID_gen::get_time_UUID())};
    });
}

static std::chrono::milliseconds get_valid_timestamp(const data_value& ts_obj) {
    auto ts = value_cast<db_clock::time_point>(ts_obj);
    auto ms = ts.time_since_epoch();
    if (!utils::UUID_gen::is_valid_unix_timestamp(ms)) {
        throw exceptions::server_exception(format("{}: timestamp is out of range. Must be in milliseconds since epoch", ms.count()));
    }
    return ms;
}

inline
shared_ptr<function>
make_min_timeuuid_fct() {
    return make_native_scalar_function<true>("mintimeuuid", timeuuid_type, { timestamp_type },
            [] (std::span<const bytes_opt> values) -> bytes_opt {
        auto& bb = values[0];
        if (!bb) {
            return {};
        }
        auto ts_obj = timestamp_type->deserialize(*bb);
        if (ts_obj.is_null()) {
            return {};
        }
        auto uuid = utils::UUID_gen::min_time_UUID(get_valid_timestamp(ts_obj));
        return {timeuuid_type->decompose(uuid)};
    });
}

inline
shared_ptr<function>
make_max_timeuuid_fct() {
    return make_native_scalar_function<true>("maxtimeuuid", timeuuid_type, { timestamp_type },
            [] (std::span<const bytes_opt> values) -> bytes_opt {
        auto& bb = values[0];
        if (!bb) {
            return {};
        }
        auto ts_obj = timestamp_type->deserialize(*bb);
        if (ts_obj.is_null()) {
            return {};
        }
        auto uuid = utils::UUID_gen::max_time_UUID(get_valid_timestamp(ts_obj));
        return {timeuuid_type->decompose(uuid)};
    });
}

inline utils::UUID get_valid_timeuuid(bytes raw) {
    if (!utils::UUID_gen::is_valid_UUID(raw)) {
        throw exceptions::server_exception(format("invalid timeuuid: size={}", raw.size()));
    }
    auto uuid = utils::UUID_gen::get_UUID(raw);
    if (!uuid.is_timestamp()) {
        throw exceptions::server_exception(format("{}: Not a timeuuid: version={}", uuid, uuid.version()));
    }
    return uuid;
}

inline
shared_ptr<function>
make_date_of_fct() {
    return make_native_scalar_function<true>("dateof", timestamp_type, { timeuuid_type },
            [] (std::span<const bytes_opt> values) -> bytes_opt {
        using namespace utils;
        auto& bb = values[0];
        if (!bb) {
            return {};
        }
        auto ts = db_clock::time_point(db_clock::duration(UUID_gen::unix_timestamp(get_valid_timeuuid(*bb))));
        return {timestamp_type->decompose(ts)};
    });
}

inline
shared_ptr<function>
make_unix_timestamp_of_fct() {
    return make_native_scalar_function<true>("unixtimestampof", long_type, { timeuuid_type },
            [] (std::span<const bytes_opt> values) -> bytes_opt {
        using namespace utils;
        auto& bb = values[0];
        if (!bb) {
            return {};
        }
        return {long_type->decompose(UUID_gen::unix_timestamp(get_valid_timeuuid(*bb)).count())};
    });
}

inline shared_ptr<function>
make_currenttimestamp_fct() {
    return make_native_scalar_function<false>("currenttimestamp", timestamp_type, {},
            [] (std::span<const bytes_opt> values) -> bytes_opt {
        return {timestamp_type->decompose(db_clock::now())};
    });
}

inline shared_ptr<function>
make_currenttime_fct() {
    return make_native_scalar_function<false>("currenttime", time_type, {},
            [] (std::span<const bytes_opt> values) -> bytes_opt {
        constexpr int64_t milliseconds_in_day = 3600 * 24 * 1000;
        int64_t milliseconds_since_epoch = std::chrono::duration_cast<std::chrono::milliseconds>(db_clock::now().time_since_epoch()).count();
        int64_t nanoseconds_today = (milliseconds_since_epoch % milliseconds_in_day) * 1000 * 1000;
        return {time_type->decompose(time_native_type{nanoseconds_today})};
    });
}

inline shared_ptr<function>
make_currentdate_fct() {
    return make_native_scalar_function<false>("currentdate", simple_date_type, {},
            [] (std::span<const bytes_opt> values) -> bytes_opt {
        auto to_simple_date = get_castas_fctn(simple_date_type, timestamp_type);
        return {simple_date_type->decompose(to_simple_date(db_clock::now()))};
    });
}

inline
shared_ptr<function>
make_currenttimeuuid_fct() {
    return make_native_scalar_function<false>("currenttimeuuid", timeuuid_type, {},
            [] (std::span<const bytes_opt> values) -> bytes_opt {
        return {timeuuid_type->decompose(timeuuid_native_type{utils::UUID_gen::get_time_UUID()})};
    });
}

inline
shared_ptr<function>
make_timeuuidtodate_fct() {
    return make_native_scalar_function<true>("todate", simple_date_type, { timeuuid_type },
            [] (std::span<const bytes_opt> values) -> bytes_opt {
        using namespace utils;
        auto& bb = values[0];
        if (!bb) {
            return {};
        }
        auto ts = db_clock::time_point(db_clock::duration(UUID_gen::unix_timestamp(get_valid_timeuuid(*bb))));
        auto to_simple_date = get_castas_fctn(simple_date_type, timestamp_type);
        return {simple_date_type->decompose(to_simple_date(ts))};
    });
}

inline
shared_ptr<function>
make_timestamptodate_fct() {
    return make_native_scalar_function<true>("todate", simple_date_type, { timestamp_type },
            [] (std::span<const bytes_opt> values) -> bytes_opt {
        using namespace utils;
        auto& bb = values[0];
        if (!bb) {
            return {};
        }
        auto ts_obj = timestamp_type->deserialize(*bb);
        if (ts_obj.is_null()) {
            return {};
        }
        auto to_simple_date = get_castas_fctn(simple_date_type, timestamp_type);
        return {simple_date_type->decompose(to_simple_date(ts_obj))};
    });
}

inline
shared_ptr<function>
make_timeuuidtotimestamp_fct() {
    return make_native_scalar_function<true>("totimestamp", timestamp_type, { timeuuid_type },
            [] (std::span<const bytes_opt> values) -> bytes_opt {
        using namespace utils;
        auto& bb = values[0];
        if (!bb) {
            return {};
        }
        auto ts = db_clock::time_point(db_clock::duration(UUID_gen::unix_timestamp(get_valid_timeuuid(*bb))));
        return {timestamp_type->decompose(ts)};
    });
}

inline
shared_ptr<function>
make_datetotimestamp_fct() {
    return make_native_scalar_function<true>("totimestamp", timestamp_type, { simple_date_type },
            [] (std::span<const bytes_opt> values) -> bytes_opt {
        using namespace utils;
        auto& bb = values[0];
        if (!bb) {
            return {};
        }
        auto simple_date_obj = simple_date_type->deserialize(*bb);
        if (simple_date_obj.is_null()) {
            return {};
        }
        auto from_simple_date = get_castas_fctn(timestamp_type, simple_date_type);
        return {timestamp_type->decompose(from_simple_date(simple_date_obj))};
    });
}

inline
shared_ptr<function>
make_timeuuidtounixtimestamp_fct() {
    return make_native_scalar_function<true>("tounixtimestamp", long_type, { timeuuid_type },
            [] (std::span<const bytes_opt> values) -> bytes_opt {
        using namespace utils;
        auto& bb = values[0];
        if (!bb) {
            return {};
        }
        return {long_type->decompose(UUID_gen::unix_timestamp(get_valid_timeuuid(*bb)).count())};
    });
}

inline bytes time_point_to_long(const data_value& v) {
    return serialized(get_valid_timestamp(v).count());
}

inline
shared_ptr<function>
make_timestamptounixtimestamp_fct() {
    return make_native_scalar_function<true>("tounixtimestamp", long_type, { timestamp_type },
            [] (std::span<const bytes_opt> values) -> bytes_opt {
        using namespace utils;
        auto& bb = values[0];
        if (!bb) {
            return {};
        }
        auto ts_obj = timestamp_type->deserialize(*bb);
        if (ts_obj.is_null()) {
            return {};
        }
        return time_point_to_long(ts_obj);
    });
}

inline
shared_ptr<function>
make_datetounixtimestamp_fct() {
    return make_native_scalar_function<true>("tounixtimestamp", long_type, { simple_date_type },
            [] (std::span<const bytes_opt> values) -> bytes_opt {
        using namespace utils;
        auto& bb = values[0];
        if (!bb) {
            return {};
        }
        auto simple_date_obj = simple_date_type->deserialize(*bb);
        if (simple_date_obj.is_null()) {
            return {};
        }
        auto from_simple_date = get_castas_fctn(timestamp_type, simple_date_type);
        return time_point_to_long(from_simple_date(simple_date_obj));
    });
}

}
}
}
