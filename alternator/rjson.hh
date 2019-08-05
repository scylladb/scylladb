/*
 * Copyright 2019 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <string>
#include <stdexcept>

namespace rjson {
class error : public std::exception {
    std::string _msg;
public:
    error() = default;
    error(const std::string& msg) : _msg(msg) {}

    virtual const char* what() const noexcept override { return _msg.c_str(); }
};
}

#define RAPIDJSON_HAS_STDSTRING 1
#define RAPIDJSON_ASSERT(x) do { if (!(x)) throw rjson::error(std::string("JSON error: condition not met: ") + #x); } while (0)
#include <rapidjson/document.h>
#include <rapidjson/writer.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/error/en.h>
#include <seastar/core/sstring.hh>
#include "seastarx.hh"

namespace rjson {

using allocator = rapidjson::CrtAllocator;
using encoding = rapidjson::UTF8<>;
using document = rapidjson::GenericDocument<encoding, allocator>;
using value = rapidjson::GenericValue<encoding, allocator>;
using string_ref_type = value::StringRefType;
using string_buffer = rapidjson::GenericStringBuffer<encoding>;
using writer = rapidjson::Writer<string_buffer, encoding>;
using type = rapidjson::Type;

inline rjson::value null_value() {
    return rjson::value(rapidjson::kNullType);
}

inline rjson::value empty_object() {
    return rjson::value(rapidjson::kObjectType);
}

inline rjson::value empty_array() {
    return rjson::value(rapidjson::kArrayType);
}

inline rjson::value empty_string() {
    return rjson::value(rapidjson::kStringType);
}

std::string print(const rjson::value& value);
rjson::value copy(const rjson::value& value);
rjson::value parse(const std::string& str);
rjson::value parse_raw(const char* c_str, size_t size);
rjson::value from_string(const std::string& str);
rjson::value from_string(const sstring& str);
rjson::value* find(rjson::value& value, rjson::string_ref_type name);
const rjson::value* find(const rjson::value& value, rjson::string_ref_type name);
rjson::value& get(rjson::value& value, rjson::string_ref_type name);
const rjson::value& get(const rjson::value& value, rjson::string_ref_type name);
void set_with_string_name(rjson::value& base, const std::string& name, rjson::value&& member);
void set_with_string_name(rjson::value& base, const std::string& name, rjson::string_ref_type member);

void set(rjson::value& base, rjson::string_ref_type name, rjson::value&& member);
void set(rjson::value& base, rjson::string_ref_type name, rjson::string_ref_type member);

void push_back(rjson::value& base_array, rjson::value&& item);

} // end namespace rjson

namespace std {
std::ostream& operator<<(std::ostream& os, const rjson::value& v);
}
