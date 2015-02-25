/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#include "cql3_type.hh"

namespace cql3 {

thread_local shared_ptr<cql3_type> native_cql3_type::ascii = make("ascii", ascii_type, native_cql3_type::kind::ASCII);
thread_local shared_ptr<cql3_type> native_cql3_type::bigint = make("bigint", long_type, native_cql3_type::kind::BIGINT);
thread_local shared_ptr<cql3_type> native_cql3_type::blob = make("blob", bytes_type, native_cql3_type::kind::BLOB);
thread_local shared_ptr<cql3_type> native_cql3_type::boolean = make("boolean", boolean_type, native_cql3_type::kind::BOOLEAN);
//thread_local shared_ptr<cql3_type> native_cql3_type::double = make("double", double_type, native_cql3_type::kind::DOUBLE);
//thread_local shared_ptr<cql3_type> native_cql3_type::float = make("float", float_type, native_cql3_type::kind::FLOAT);
thread_local shared_ptr<cql3_type> native_cql3_type::int_ = make("int", int32_type, native_cql3_type::kind::INT);
thread_local shared_ptr<cql3_type> native_cql3_type::text = make("text", utf8_type, native_cql3_type::kind::TEXT);
thread_local shared_ptr<cql3_type> native_cql3_type::timestamp = make("timestamp", timestamp_type, native_cql3_type::kind::TIMESTAMP);
thread_local shared_ptr<cql3_type> native_cql3_type::uuid = make("uuid", uuid_type, native_cql3_type::kind::UUID);
thread_local shared_ptr<cql3_type> native_cql3_type::varchar = make("varchar", utf8_type, native_cql3_type::kind::TEXT);
thread_local shared_ptr<cql3_type> native_cql3_type::timeuuid = make("timeuuid", timeuuid_type, native_cql3_type::kind::TIMEUUID);
thread_local shared_ptr<cql3_type> native_cql3_type::inet = make("inet", inet_addr_type, native_cql3_type::kind::INET);

const std::vector<shared_ptr<cql3_type>>&
native_cql3_type::values() {
    static thread_local std::vector<shared_ptr<cql3_type>> v = {
        native_cql3_type::ascii,
        native_cql3_type::bigint,
        native_cql3_type::blob,
        native_cql3_type::boolean,
#if 0
        native_cql3_type::counter,
        native_cql3_type::decimal,
        native_cql3_type::double_,
        native_cql3_type::float_,
#endif
        native_cql3_type:inet,
        native_cql3_type::int_,
        native_cql3_type::text,
        native_cql3_type::timestamp,
        native_cql3_type::uuid,
        native_cql3_type::varchar,
        native_cql3_type::timeuuid,
    };
    return v;
}

}


