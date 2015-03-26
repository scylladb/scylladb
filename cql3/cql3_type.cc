/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#include "cql3_type.hh"

namespace cql3 {

thread_local shared_ptr<cql3_type> cql3_type::ascii = make("ascii", ascii_type, cql3_type::kind::ASCII);
thread_local shared_ptr<cql3_type> cql3_type::bigint = make("bigint", long_type, cql3_type::kind::BIGINT);
thread_local shared_ptr<cql3_type> cql3_type::blob = make("blob", bytes_type, cql3_type::kind::BLOB);
thread_local shared_ptr<cql3_type> cql3_type::boolean = make("boolean", boolean_type, cql3_type::kind::BOOLEAN);
thread_local shared_ptr<cql3_type> cql3_type::double_ = make("double", double_type, cql3_type::kind::DOUBLE);
thread_local shared_ptr<cql3_type> cql3_type::float_ = make("float", float_type, cql3_type::kind::FLOAT);
thread_local shared_ptr<cql3_type> cql3_type::int_ = make("int", int32_type, cql3_type::kind::INT);
thread_local shared_ptr<cql3_type> cql3_type::text = make("text", utf8_type, cql3_type::kind::TEXT);
thread_local shared_ptr<cql3_type> cql3_type::timestamp = make("timestamp", timestamp_type, cql3_type::kind::TIMESTAMP);
thread_local shared_ptr<cql3_type> cql3_type::uuid = make("uuid", uuid_type, cql3_type::kind::UUID);
thread_local shared_ptr<cql3_type> cql3_type::varchar = make("varchar", utf8_type, cql3_type::kind::TEXT);
thread_local shared_ptr<cql3_type> cql3_type::timeuuid = make("timeuuid", timeuuid_type, cql3_type::kind::TIMEUUID);
thread_local shared_ptr<cql3_type> cql3_type::inet = make("inet", inet_addr_type, cql3_type::kind::INET);

const std::vector<shared_ptr<cql3_type>>&
cql3_type::values() {
    static thread_local std::vector<shared_ptr<cql3_type>> v = {
        cql3_type::ascii,
        cql3_type::bigint,
        cql3_type::blob,
        cql3_type::boolean,
#if 0
        cql3_type::counter,
        cql3_type::decimal,
#endif
        cql3_type::double_,
        cql3_type::float_,
        cql3_type:inet,
        cql3_type::int_,
        cql3_type::text,
        cql3_type::timestamp,
        cql3_type::uuid,
        cql3_type::varchar,
        cql3_type::timeuuid,
    };
    return v;
}

}


