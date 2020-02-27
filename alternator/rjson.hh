/*
 * Copyright 2019 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

/*
 * rjson is a wrapper over rapidjson library, providing fast JSON parsing and generation.
 *
 * rapidjson has strict copy elision policies, which, among other things, involves
 * using provided char arrays without copying them and allows copying objects only explicitly.
 * As such, one should be careful when passing strings with limited liveness
 * (e.g. data underneath local std::strings) to rjson functions, because created JSON objects
 * may end up relying on dangling char pointers. All rjson functions that create JSONs from strings
 * by rjson have both APIs for string_ref_type (more optimal, used when the string is known to live
 * at least as long as the object, e.g. a static char array) and for std::strings. The more optimal
 * variants should be used *only* if the liveness of the string is guaranteed, otherwise it will
 * result in undefined behaviour.
 * Also, bear in mind that methods exposed by rjson::value are generic, but some of them
 * work fine only for specific types. In case the type does not match, an rjson::error will be thrown.
 * Examples of such mismatched usages is calling MemberCount() on a JSON value not of object type
 * or calling Size() on a non-array value.
 */

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

// rapidjson configuration macros
#define RAPIDJSON_HAS_STDSTRING 1
// Default rjson policy is to use assert() - which is dangerous for two reasons:
// 1. assert() can be turned off with -DNDEBUG
// 2. assert() crashes a program
// Fortunately, the default policy can be overridden, and so rapidjson errors will
// throw an rjson::error exception instead.
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

// Returns an object representing JSON's null
inline rjson::value null_value() {
    return rjson::value(rapidjson::kNullType);
}

// Returns an empty JSON object - {}
inline rjson::value empty_object() {
    return rjson::value(rapidjson::kObjectType);
}

// Returns an empty JSON array - []
inline rjson::value empty_array() {
    return rjson::value(rapidjson::kArrayType);
}

// Returns an empty JSON string - ""
inline rjson::value empty_string() {
    return rjson::value(rapidjson::kStringType);
}

// Convert the JSON value to a string with JSON syntax, the opposite of parse().
// The representation is dense - without any redundant indentation.
std::string print(const rjson::value& value);

// Returns a string_view to the string held in a JSON value (which is
// assumed to hold a string, i.e., v.IsString() == true). This is a view
// to the existing data - no copying is done.
inline std::string_view to_string_view(const rjson::value& v) {
    return std::string_view(v.GetString(), v.GetStringLength());
}

// Copies given JSON value - involves allocation
rjson::value copy(const rjson::value& value);

// Parses a JSON value from given string or raw character array.
// The string/char array liveness does not need to be persisted,
// as parse() will allocate member names and values.
// Throws rjson::error if parsing failed.
rjson::value parse(std::string_view str);

// Creates a JSON value (of JSON string type) out of internal string representations.
// The string value is copied, so str's liveness does not need to be persisted.
rjson::value from_string(const std::string& str);
rjson::value from_string(const sstring& str);
rjson::value from_string(const char* str, size_t size);
rjson::value from_string(std::string_view view);

// Returns a pointer to JSON member if it exists, nullptr otherwise
rjson::value* find(rjson::value& value, rjson::string_ref_type name);
const rjson::value* find(const rjson::value& value, rjson::string_ref_type name);

// Returns a reference to JSON member if it exists, throws otherwise
rjson::value& get(rjson::value& value, rjson::string_ref_type name);
const rjson::value& get(const rjson::value& value, rjson::string_ref_type name);

// Sets a member in given JSON object by moving the member - allocates the name.
// Throws if base is not a JSON object.
void set_with_string_name(rjson::value& base, const std::string& name, rjson::value&& member);
void set_with_string_name(rjson::value& base, std::string_view name, rjson::value&& member);

// Sets a string member in given JSON object by assigning its reference - allocates the name.
// NOTICE: member string liveness must be ensured to be at least as long as base's.
// Throws if base is not a JSON object.
void set_with_string_name(rjson::value& base, const std::string& name, rjson::string_ref_type member);
void set_with_string_name(rjson::value& base, std::string_view name, rjson::string_ref_type member);

// Sets a member in given JSON object by moving the member.
// NOTICE: name liveness must be ensured to be at least as long as base's.
// Throws if base is not a JSON object.
void set(rjson::value& base, rjson::string_ref_type name, rjson::value&& member);

// Sets a string member in given JSON object by assigning its reference.
// NOTICE: name liveness must be ensured to be at least as long as base's.
// NOTICE: member liveness must be ensured to be at least as long as base's.
// Throws if base is not a JSON object.
void set(rjson::value& base, rjson::string_ref_type name, rjson::string_ref_type member);

// Adds a value to a JSON list by moving the item to its end.
// Throws if base_array is not a JSON array.
void push_back(rjson::value& base_array, rjson::value&& item);

struct single_value_comp {
    bool operator()(const rjson::value& r1, const rjson::value& r2) const;
};

} // end namespace rjson

namespace std {
std::ostream& operator<<(std::ostream& os, const rjson::value& v);
}
