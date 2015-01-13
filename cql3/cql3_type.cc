/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#include "cql3_type.hh"

namespace cql3 {

thread_local shared_ptr<cql3_type> native_cql3_type::ascii = make("ascii", ascii_type);
thread_local shared_ptr<cql3_type> native_cql3_type::bigint = make("bigint", long_type);
thread_local shared_ptr<cql3_type> native_cql3_type::blob = make("blob", bytes_type);
thread_local shared_ptr<cql3_type> native_cql3_type::boolean = make("boolean", boolean_type);
//thread_local shared_ptr<cql3_type> native_cql3_type::double = make("double", double_type);
//thread_local shared_ptr<cql3_type> native_cql3_type::float = make("float", float_type);
thread_local shared_ptr<cql3_type> native_cql3_type::int_ = make("int", int32_type);
thread_local shared_ptr<cql3_type> native_cql3_type::text = make("text", utf8_type);
thread_local shared_ptr<cql3_type> native_cql3_type::timestamp = make("timestamp", timestamp_type);
thread_local shared_ptr<cql3_type> native_cql3_type::uuid = make("uuid", uuid_type);
thread_local shared_ptr<cql3_type> native_cql3_type::varchar = make("varchar", utf8_type);
thread_local shared_ptr<cql3_type> native_cql3_type::timeuuid = make("timeuuid", timeuuid_type);

const std::vector<shared_ptr<cql3_type>>&
native_cql3_type::values() {
    static thread_local std::vector<shared_ptr<cql3_type>> v = {
        native_cql3_type::ascii,
        native_cql3_type::bigint,
        native_cql3_type::blob,
        native_cql3_type::boolean,
#if 0
        native_cql3_type::double_,
        native_cql3_type::float_,
#endif
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


