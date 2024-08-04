/*
 * Copyright 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
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
#include <string_view>
#include <type_traits>
#include "utils/assert.hh"
#include "utils/base64.hh"

#include <seastar/core/future.hh>

namespace seastar {
    template<typename> class output_stream;
}

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
// Default rjson policy is to use SCYLLA_ASSERT() - which is dangerous for two reasons:
// 1. SCYLLA_ASSERT() can be turned off with -DNDEBUG
// 2. SCYLLA_ASSERT() crashes a program
// Fortunately, the default policy can be overridden, and so rapidjson errors will
// throw an rjson::error exception instead.
#define RAPIDJSON_ASSERT(x) do { if (!(x)) throw rjson::error(fmt::format("JSON SCYLLA_ASSERT failed on condition '{}', at: {}", #x, current_backtrace_tasklocal())); } while (0)
// This macro is used for functions which are called for every json char making it
// quite costly if not inlined, by default rapidjson only enables it if NDEBUG
// is defined which isn't the case for us.
#define RAPIDJSON_FORCEINLINE __attribute__((always_inline))

#include <rapidjson/document.h>
#include <rapidjson/writer.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/error/en.h>
#include <rapidjson/allocators.h>
#include <rapidjson/ostreamwrapper.h>
#include <seastar/core/sstring.hh>
#include "seastarx.hh"

namespace rjson {

// The internal namespace is a workaround for the fact that fmt::format
// also has a to_string_view function and erroneously looks up our rjson::to_string_view
// if this allocator is in the rjson namespace.
namespace internal {
// Implements an interface conforming to the one in rapidjson/allocators.h,
// but throws rjson::error on allocation failures
class throwing_allocator : public rapidjson::CrtAllocator {
    using base = rapidjson::CrtAllocator;
public:
    static const bool kNeedFree = base::kNeedFree;
    void* Malloc(size_t size);
    void* Realloc(void* orig_ptr, size_t orig_size, size_t new_size);
    static void Free(void* ptr);
};
}

using allocator = internal::throwing_allocator;
using encoding = rapidjson::UTF8<>;
using document = rapidjson::GenericDocument<encoding, allocator, allocator>;
using value = rapidjson::GenericValue<encoding, allocator>;
using string_ref_type = value::StringRefType;
using string_buffer = rapidjson::GenericStringBuffer<encoding, allocator>;
using writer = rapidjson::Writer<string_buffer, encoding, encoding, allocator>;
using type = rapidjson::Type;

// The default value is derived from the days when rjson resided in alternator:
// - the original DynamoDB nested level limit is 32
// - it's raised by 7 for alternator to make it safer and more cool
// - the value is doubled because the nesting level is bumped twice
//   for every alternator object - because each alternator object
//   consists of a 2-level JSON object.
inline constexpr size_t default_max_nested_level = 78;

/** 
 * exception specializations. 
 */
class malformed_value : public error {
public:
    malformed_value(std::string_view name, const rjson::value& value);
    malformed_value(std::string_view name, std::string_view value);
};

class missing_value : public error {
public:
    missing_value(std::string_view name);
};


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
std::string print(const rjson::value& value, size_t max_nested_level = default_max_nested_level);

// Writes the JSON value to the output_stream, similar to print() -> string, but
// directly. Note this has potentially more data copy overhead and should be 
// reserved for larger values where not creating huge linear strings is useful.
// (Because data will first be written to N buffers before actually committed to stream, 
//  where N = <n bytes text>/k (k dependent on buffer/write block behaviour of stream),
//  and also copy at least 2n bytes (assuming underlying stream is buffered).
//  This may or may not be more data copying/overhead than creating a string directly
//  and printing it, but again, it will guarantee fragmented buffer behaviour).
// Note: input value must remain valid for the call, but is _not_ required to be valid
// until future resolves - i.e. the full json -> text conversion is done _before_ 
// pushing fully to stream. I.e. it is valid to do `return print(rjson::value("... something..."), os);`
seastar::future<> print(const rjson::value& value, seastar::output_stream<char>&, size_t max_nested_level = default_max_nested_level);

// Returns a string_view to the string held in a JSON value (which is
// assumed to hold a string, i.e., v.IsString() == true). This is a view
// to the existing data - no copying is done.
inline std::string_view to_string_view(const rjson::value& v) {
    return std::string_view(v.GetString(), v.GetStringLength());
}

// Copies given JSON value - involves allocation
rjson::value copy(const rjson::value& value);

// copyable_value can be used when seamless copying of value is needed
class copyable_value : public value {
public:
    copyable_value() noexcept : value() {
    }
    copyable_value(const copyable_value& v) : value(copy(v)) {
    }
    copyable_value(copyable_value&& v) noexcept : value(std::move(v)) {
    }
    copyable_value(value&& v) noexcept : value(std::move(v)) {
    }
    copyable_value& operator=(const copyable_value& v) {
        if (this != &v) {
            *this = copy(v);
        }
        return *this;
    }
    copyable_value& operator=(copyable_value&& v) noexcept {
        value::operator=(value(std::move(v)));
        return *this;
    }
};

// Parses a JSON value from given string or raw character array.
// The string/char array liveness does not need to be persisted,
// as parse() will allocate member names and values.
// Throws rjson::error if parsing failed.
rjson::value parse(std::string_view str, size_t max_nested_level = default_max_nested_level);
// Parses a JSON value returns a disengaged optional on failure.
// NOTICE: any error context will be lost, so this function should
// be used only if one does not care why parsing failed.
std::optional<rjson::value> try_parse(std::string_view str, size_t max_nested_level = default_max_nested_level);
// Needs to be run in thread context
rjson::value parse_yieldable(std::string_view str, size_t max_nested_level = default_max_nested_level);

// chunked_content holds a non-contiguous buffer of bytes - such as bytes
// read by util::read_entire_stream(). We assume that chunked_content does
// not contain any empty buffers (the vector can be empty, meaning empty
// content - but individual buffers cannot).
using chunked_content = std::vector<temporary_buffer<char>>;

// Additional variants of parse() and parse_yieldable() that work on non-
// contiguous chunked_content. The chunked_content is moved into the parsing
// function so that we can start freeing chunks as soon as we parse them.
rjson::value parse(chunked_content&&, size_t max_nested_level = default_max_nested_level);
rjson::value parse_yieldable(chunked_content&&, size_t max_nested_level = default_max_nested_level);

// Creates a JSON value (of JSON string type) out of internal string representations.
// The string value is copied, so str's liveness does not need to be persisted.
rjson::value from_string(const char* str, size_t size);
rjson::value from_string(std::string_view view);

// Returns a pointer to JSON member if it exists, nullptr otherwise
rjson::value* find(rjson::value& value, std::string_view name);
const rjson::value* find(const rjson::value& value, std::string_view name);

// Returns a reference to JSON member if it exists, throws otherwise
rjson::value& get(rjson::value& value, std::string_view name);
const rjson::value& get(const rjson::value& value, std::string_view name);

/**
 * Type conversion getter. 
 * Will typically require an existing rapidjson::internal::TypeHelper<...>
 * to exist for the type queried. 
 */
template<typename T>
T get(const rjson::value& value, std::string_view name) {
    auto& v = get(value, name);
    try {
        return v.Get<T>();
    } catch (...) {
        std::throw_with_nested(malformed_value(name, v));
    }
}

/**
 * Type conversion opt getter. 
 * Will typically require an existing rapidjson::internal::TypeHelper<...>
 * to exist for the type queried. 
 * 
 * Return std::nullopt if value does not exist. 
 */
template<typename T>
std::optional<T> get_opt(const rjson::value& value, std::string_view name) {
    auto* v = find(value, name);
    try {
        return v ? std::optional<T>(v->Get<T>()) : std::nullopt;
    } catch (...) {
        std::throw_with_nested(malformed_value(name, *v));
    }
}

// The various add*() functions below *add* a new member to a JSON object.
// They all assume that a member with the same key (name) doesn't already
// exist in that object, so they are meant to be used just to build a new
// object from scratch. If a member with the same name *may* exist, and
// might need to be replaced, use the replace*() functions instead.
// The benefit of the add*() functions is that they are faster (O(1),
// compared to O(n) for the replace* function that need to inspect the
// existing members).

// Adds a member to a given JSON object by moving the member - allocates the name.
// Throws if base is not a JSON object.
// Assumes a member with the same name does not yet exist in base.
void add_with_string_name(rjson::value& base, std::string_view name, rjson::value&& member);

// Adds a string member to a given JSON object by assigning its reference - allocates the name.
// NOTICE: member string liveness must be ensured to be at least as long as base's.
// Throws if base is not a JSON object.
// Assumes a member with the same name does not yet exist in base.
void add_with_string_name(rjson::value& base, std::string_view name, rjson::string_ref_type member);

// Adds a member to a given JSON object by moving the member.
// NOTICE: name liveness must be ensured to be at least as long as base's.
// Throws if base is not a JSON object.
// Assumes a member with the same name does not yet exist in base.
void add(rjson::value& base, rjson::string_ref_type name, rjson::value&& member);

// Adds a string member to a given JSON object by assigning its reference.
// NOTICE: name liveness must be ensured to be at least as long as base's.
// NOTICE: member liveness must be ensured to be at least as long as base's.
// Throws if base is not a JSON object.
// Assumes a member with the same name does not yet exist in base.
void add(rjson::value& base, rjson::string_ref_type name, rjson::string_ref_type member);

/**
 * Type conversion setter. 
 * Will typically require an existing rapidjson::internal::TypeHelper<...>
 * to exist for the type written. 
 * 
 * (Note: order is important. Need to be after set(..., rjson::value)).
 * Assumes a member with the same name does not yet exist in base.
 */
template<typename T>
requires (!std::is_constructible_v<string_ref_type, T>)
void add(rjson::value& base, rjson::string_ref_type name, T&& member) {
    extern allocator the_allocator;

    rjson::value v;
    v.Set(std::forward<T>(member), the_allocator);
    add(base, std::move(name), std::move(v));
}

// Set a member in a given JSON object by moving the member - allocates the name.
// If a member with the same name already exist in base, it is replaced.
// Throws if base is not a JSON object.
void replace_with_string_name(rjson::value& base, std::string_view name, rjson::value&& member);


// Adds a value to a JSON list by moving the item to its end.
// Throws if base_array is not a JSON array.
void push_back(rjson::value& base_array, rjson::value&& item);

// Remove a member from a JSON object. Throws if value isn't an object.
bool remove_member(rjson::value& value, std::string_view name);

struct single_value_comp {
    bool operator()(const rjson::value& r1, const rjson::value& r2) const;
};

// Helper function for parsing a JSON straight into a map
// of strings representing their values - useful for various
// database helper functions.
// This function exists for historical reasons - existing infrastructure
// relies on being able to transform a JSON string into a map of sstrings.
template<typename Map>
requires (std::is_same_v<Map, std::map<sstring, sstring>> || std::is_same_v<Map, std::unordered_map<sstring, sstring>>)
Map parse_to_map(std::string_view raw) {
    Map map;
    rjson::value root = rjson::parse(raw);
    if (root.IsNull()) {
        return map;
    }
    if (!root.IsObject()) {
        throw rjson::error("Only json objects can be transformed to maps. Encountered: " + std::string(raw));
    }
    for (auto it = root.MemberBegin(); it != root.MemberEnd(); ++it) {
        if (it->value.IsString()) {
            map.emplace(sstring(rjson::to_string_view(it->name)), sstring(rjson::to_string_view(it->value)));
        } else {
            map.emplace(sstring(rjson::to_string_view(it->name)), sstring(rjson::print(it->value)));
        }
    }
    return map;
}

// This function exists for historical reasons as well.
rjson::value from_string_map(const std::map<sstring, sstring>& map);

// The function operates on sstrings for historical reasons.
sstring quote_json_string(const sstring& value);

inline bytes base64_decode(const value& v) {
    return ::base64_decode(std::string_view(v.GetString(), v.GetStringLength()));
}

// A writer which allows writing json into an std::ostream in a streaming manner.
//
// It is a wrapper around rapidjson::Writer, with a more convenient API.
class streaming_writer {
    using stream = rapidjson::BasicOStreamWrapper<std::ostream>;
    using writer = rapidjson::Writer<stream, rjson::encoding, rjson::encoding, rjson::allocator>;

    stream _stream;
    writer _writer;

public:
    streaming_writer(std::ostream& os = std::cout) : _stream(os), _writer(_stream)
    { }

    writer& rjson_writer() { return _writer; }

    // following the rapidjson method names here
    bool Null() { return _writer.Null(); }
    bool Bool(bool b) { return _writer.Bool(b); }
    bool Int(int i) { return _writer.Int(i); }
    bool Uint(unsigned i) { return _writer.Uint(i); }
    bool Int64(int64_t i) { return _writer.Int64(i); }
    bool Uint64(uint64_t i) { return _writer.Uint64(i); }
    bool Double(double d) { return _writer.Double(d); }
    bool RawNumber(std::string_view str) { return _writer.RawNumber(str.data(), str.size(), false); }
    bool String(std::string_view str) { return _writer.String(str.data(), str.size(), false); }
    bool StartObject() { return _writer.StartObject(); }
    bool Key(std::string_view str) { return _writer.Key(str.data(), str.size(), false); }
    bool EndObject(rapidjson::SizeType memberCount = 0) { return _writer.EndObject(memberCount); }
    bool StartArray() { return _writer.StartArray(); }
    bool EndArray(rapidjson::SizeType elementCount = 0) { return _writer.EndArray(elementCount); }

    template<typename U>
    bool Write(U v) {
        using T = std::remove_cvref_t<U>;
        if constexpr (std::same_as<T, bool>) {
            return Bool(v);
        } else if constexpr (std::same_as<T, int>) {
            return Int(v);
        } else if constexpr (std::same_as<T, unsigned>) {
            return Uint(v);
        } else if constexpr (std::same_as<T, int64_t>) {
            return Int64(v);
        } else if constexpr (std::same_as<T, uint64_t>) {
            return Uint64(v);
        } else if constexpr (std::same_as<T, double>) {
            return Double(v);
        } else if constexpr (std::convertible_to<T, std::string_view>) {
            return String(v);
        }
    }
};

inline bool is_leaf(const rjson::value& value) {
    return !value.IsObject() && !value.IsArray();
}

future<> destroy_gently(rjson::value&& value);

} // end namespace rjson

template <> struct fmt::formatter<rjson::value> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const rjson::value& v, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "{}", rjson::print(v));
    }
};
