/*
 * Copyright 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

/*
 * rjson provides a JSON document model (rjson::value), a parser producing it
 * from text, and a printer writing it back as text.
 *
 * Its interface follows the one of the RapidJSON library, which rjson used to
 * wrap: values have methods like IsObject(), MemberBegin() or GetInt64().
 *
 * The implementation is designed for Seastar:
 * - Nothing allocates large contiguous memory: arrays and objects are split
 *   into segments, and the input and output can be chunked.
 * - The parser and the printer use an explicit stack instead of recursion,
 *   and can stop and resume anywhere, so the future-returning variants yield
 *   when the task quota runs out, without a seastar::thread.
 * - Long strings parsed from chunked_content are not copied: the value keeps
 *   a reference to the input buffer holding them (see chunk_holder below).
 * - Errors, including allocation failures and malformed input, throw
 *   rjson::error rather than abort.
 * - The nesting level of parsed and printed documents is limited, and so is
 *   the stack depth needed to copy or destroy them.
 *
 * Values are move-only, like RapidJSON's, and copied explicitly with
 * rjson::copy(). A moved-from value is null.
 *
 * Methods of rjson::value that only work for some types, like MemberCount()
 * on a value which is not an object, throw rjson::error when misused.
 *
 * Unlike RapidJSON, there is no GetString() returning a null-terminated
 * string, because strings referencing the input are not null-terminated.
 * Use rjson::to_string_view() instead.
 */

#include <atomic>
#include <bit>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <iterator>
#include <limits>
#include <map>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <vector>
#include <fmt/format.h>

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/util/noncopyable_function.hh>

#include "utils/base64.hh"
#include "utils/chunked_vector.hh"

#include "seastarx.hh"

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

// The types of JSON values, with the names and the order of RapidJSON's.
enum type : uint8_t {
    kNullType = 0,
    kFalseType = 1,
    kTrueType = 2,
    kObjectType = 3,
    kArrayType = 4,
    kStringType = 5,
    kNumberType = 6,
};

using size_type = uint32_t;

// The default value is derived from the days when rjson resided in alternator:
// - the original DynamoDB nested level limit is 32
// - it's raised by 7 for alternator to make it safer and more cool
// - the value is doubled because the nesting level is bumped twice
//   for every alternator object - because each alternator object
//   consists of a 2-level JSON object.
inline constexpr size_t default_max_nested_level = 78;

// Allocates memory for rjson values, throwing rjson::error when it fails.
class allocator {
public:
    static void* Malloc(size_t size);
    static void* Realloc(void* ptr, size_t old_size, size_t new_size);
    static void Free(void* ptr) noexcept;
};

// A reference to a string which outlives every value made from it, so that
// the values need not copy it. To make that hard to get wrong, it can only be
// made from a string literal or another character array of static storage.
class string_ref_type {
    const char* _str;
    size_type _len;
public:
    template<size_t N>
    consteval string_ref_type(const char (&str)[N]) : _str(str), _len(N - 1) {}
    const char* data() const noexcept { return _str; }
    size_type size() const noexcept { return _len; }
};

class value;
struct member;

namespace internal {

// Keeps an input buffer alive while values hold strings pointing into it.
// Values are passed between shards, so the reference count is atomic, and
// the buffer, which belongs to the shard which read it, is freed there.
struct chunk_holder {
    std::atomic<uint32_t> refs;
    unsigned owner;
    temporary_buffer<char> buf;
};
void retain(chunk_holder* h) noexcept;
void copy_into(value& dst, const value& src);
template<typename T>
void copy_container(value& dst, const value& src);
void release(chunk_holder* h) noexcept;

// Holds the elements of an array or an object. A small container keeps its
// elements right after this header. A container larger than a segment keeps
// a table of segments there instead, so that no allocation is larger than a
// segment, or than the table of a huge container. Elements are values or
// members, which hold no pointers to themselves, so they can be relocated
// with memcpy().
template<typename T>
struct storage {
    static constexpr uint32_t segment_bytes = 16 * 1024;
    static constexpr uint32_t segment_shift = std::bit_width(segment_bytes / sizeof(T)) - 1;
    static constexpr uint32_t segment_size = 1u << segment_shift;
    static constexpr uint32_t segment_mask = segment_size - 1;

    uint32_t size;
    uint32_t capacity;

    bool segmented() const noexcept { return capacity > segment_size; }
    T* elements() noexcept { return reinterpret_cast<T*>(this + 1); }
    T** segments() noexcept { return reinterpret_cast<T**>(this + 1); }
    T& operator[](uint32_t i) noexcept {
        return segmented() ? segments()[i >> segment_shift][i & segment_mask] : elements()[i];
    }
};

// A random access iterator over the elements of a storage<T>.
template<typename T, bool Const>
class iterator {
    using storage_type = storage<T>;
    storage_type* _s = nullptr;
    uint32_t _i = 0;
public:
    using iterator_category = std::random_access_iterator_tag;
    using value_type = T;
    using difference_type = std::ptrdiff_t;
    using pointer = std::conditional_t<Const, const T*, T*>;
    using reference = std::conditional_t<Const, const T&, T&>;

    iterator() = default;
    iterator(storage_type* s, uint32_t i) noexcept : _s(s), _i(i) {}
    // A non-const iterator converts to a const one.
    operator iterator<T, true>() const noexcept requires (!Const) { return {_s, _i}; }

    reference operator*() const noexcept { return (*_s)[_i]; }
    pointer operator->() const noexcept { return &(*_s)[_i]; }
    reference operator[](difference_type n) const noexcept { return (*_s)[_i + n]; }
    iterator& operator++() noexcept { ++_i; return *this; }
    iterator operator++(int) noexcept { auto r = *this; ++_i; return r; }
    iterator& operator--() noexcept { --_i; return *this; }
    iterator operator--(int) noexcept { auto r = *this; --_i; return r; }
    iterator& operator+=(difference_type n) noexcept { _i += n; return *this; }
    iterator& operator-=(difference_type n) noexcept { _i -= n; return *this; }
    friend iterator operator+(iterator it, difference_type n) noexcept { return it += n; }
    friend iterator operator+(difference_type n, iterator it) noexcept { return it += n; }
    friend iterator operator-(iterator it, difference_type n) noexcept { return it -= n; }
    friend difference_type operator-(const iterator& a, const iterator& b) noexcept { return difference_type(a._i) - difference_type(b._i); }
    friend bool operator==(const iterator& a, const iterator& b) noexcept { return a._i == b._i; }
    friend auto operator<=>(const iterator& a, const iterator& b) noexcept { return a._i <=> b._i; }

    uint32_t index() const noexcept { return _i; }
};

template<bool Const> class object_view;
template<bool Const> class array_view;

[[noreturn]] void throw_type_error(const value& v, const char* expected);

} // namespace internal

class value {
    // Kinds of values needing no cleanup come first, see needs_cleanup().
    enum class kind : uint8_t {
        null,
        false_,
        true_,
        // A string of at most inline_capacity bytes, kept in the value.
        inline_string,
        // A string referencing a string_ref_type.
        static_string,
        // Non-negative integers, as uint64_t.
        uint,
        // Negative integers, as int64_t.
        int_,
        double_,
        // A string allocated for the value.
        owned_string,
        // A string referencing an input buffer kept by a chunk_holder.
        chunk_string,
        object,
        array,
    };
    static constexpr size_t inline_capacity = 14;
    // A string flag meaning that it has no characters JSON needs escaped.
    static constexpr uint8_t no_escape_flag = 0x80;

    // The layout of _data, by kind:
    // - numbers: the number in bytes 0-7.
    // - inline_string: the characters in bytes 0-13, the length and flags
    //   in byte 14.
    // - static_string, owned_string: the pointer in bytes 0-7, the length in
    //   bytes 8-11, flags in byte 14.
    // - chunk_string: the chunk_holder in bytes 0-7, the offset of the string
    //   in its buffer in bytes 8-11, the length in bytes 12-13, flags in
    //   byte 14.
    // - object, array: the storage, or null if empty, in bytes 0-7.
    alignas(8) char _data[15];
    kind _kind;

    template<typename T>
    T load(size_t offset) const noexcept {
        T ret;
        std::memcpy(&ret, _data + offset, sizeof(T));
        return ret;
    }
    template<typename T>
    void store(size_t offset, T v) noexcept {
        std::memcpy(_data + offset, &v, sizeof(T));
    }

    bool needs_cleanup() const noexcept { return _kind >= kind::owned_string; }
    void destroy() noexcept;
    void move_from(value& o) noexcept {
        std::memcpy(_data, o._data, sizeof(_data));
        _kind = o._kind;
        o._kind = kind::null;
    }
    void set_uint(uint64_t u) noexcept { store(0, u); _kind = kind::uint; }
    void set_int(int64_t i) noexcept {
        store(0, i);
        _kind = i < 0 ? kind::int_ : kind::uint;
    }
    int64_t as_int64() const noexcept { return load<int64_t>(0); }
    uint64_t as_uint64() const noexcept { return load<uint64_t>(0); }
    template<typename T>
    internal::storage<T>* container() const noexcept { return load<internal::storage<T>*>(0); }
    template<typename T>
    void set_container(internal::storage<T>* s) noexcept { store(0, s); }
    void check(bool ok, const char* expected) const {
        if (!ok) [[unlikely]] {
            internal::throw_type_error(*this, expected);
        }
    }
    template<typename T> void push(T&& elem);
    template<typename T> void reserve_more(uint32_t n);

    friend class value_builder;
    friend class printer;
    friend value copy(const value&);
    friend void internal::copy_into(value&, const value&);
    template<typename T>
    friend void internal::copy_container(value&, const value&);
    friend void set_string(value&, std::string_view, bool);
    friend void set_chunk_string(value&, internal::chunk_holder*, uint32_t, uint32_t);
    friend bool needs_escaping(const value&) noexcept;
public:
    using Ch = char;
    using Member = member;
    using MemberIterator = internal::iterator<member, false>;
    using ConstMemberIterator = internal::iterator<member, true>;
    using ValueIterator = internal::iterator<value, false>;
    using ConstValueIterator = internal::iterator<value, true>;
    using Object = internal::object_view<false>;
    using ConstObject = internal::object_view<true>;
    using Array = internal::array_view<false>;
    using ConstArray = internal::array_view<true>;

    value() noexcept : _kind(kind::null) {}
    explicit value(type t) noexcept;
    template<typename T>
    requires std::same_as<T, bool>
    explicit value(T b) noexcept : _kind(b ? kind::true_ : kind::false_) {}
    template<std::integral T>
    requires (!std::same_as<T, bool> && !std::same_as<T, char>)
    explicit value(T i) noexcept {
        if constexpr (std::is_signed_v<T>) {
            set_int(i);
        } else {
            set_uint(i);
        }
    }
    explicit value(double d) noexcept : _kind(kind::double_) { store(0, d); }
    explicit value(float f) noexcept : value(double(f)) {}
    explicit value(string_ref_type s) noexcept : _kind(kind::null) { SetString(s); }

    value(const value&) = delete;
    value& operator=(const value&) = delete;
    value(value&& o) noexcept { move_from(o); }
    value& operator=(value&& o) noexcept {
        // o may be a part of this value, so take it out before destroying.
        value tmp(std::move(o));
        if (needs_cleanup()) {
            destroy();
        }
        move_from(tmp);
        return *this;
    }
    // Always inlined, so that destroying containers of scalars costs a
    // check per element, not a call.
    [[gnu::always_inline]] ~value() {
        if (needs_cleanup()) {
            destroy();
        }
    }

    type GetType() const noexcept;
    bool IsNull() const noexcept { return _kind == kind::null; }
    bool IsFalse() const noexcept { return _kind == kind::false_; }
    bool IsTrue() const noexcept { return _kind == kind::true_; }
    bool IsBool() const noexcept { return _kind == kind::false_ || _kind == kind::true_; }
    bool IsObject() const noexcept { return _kind == kind::object; }
    bool IsArray() const noexcept { return _kind == kind::array; }
    bool IsString() const noexcept {
        return _kind == kind::inline_string || _kind == kind::static_string || _kind == kind::owned_string || _kind == kind::chunk_string;
    }
    bool IsNumber() const noexcept { return _kind == kind::uint || _kind == kind::int_ || _kind == kind::double_; }
    bool IsDouble() const noexcept { return _kind == kind::double_; }
    // As in RapidJSON, an integer is IsInt(), IsUint(), IsInt64() or
    // IsUint64() if it fits that type, and a number with a fraction or an
    // exponent is always a double.
    bool IsInt() const noexcept {
        return (_kind == kind::uint && as_uint64() <= uint64_t(std::numeric_limits<int32_t>::max()))
            || (_kind == kind::int_ && as_int64() >= std::numeric_limits<int32_t>::min());
    }
    bool IsUint() const noexcept { return _kind == kind::uint && as_uint64() <= std::numeric_limits<uint32_t>::max(); }
    bool IsInt64() const noexcept {
        return (_kind == kind::uint && as_uint64() <= uint64_t(std::numeric_limits<int64_t>::max())) || _kind == kind::int_;
    }
    bool IsUint64() const noexcept { return _kind == kind::uint; }

    bool GetBool() const { check(IsBool(), "a boolean"); return _kind == kind::true_; }
    int GetInt() const { check(IsInt(), "a 32-bit integer"); return int(as_int64()); }
    unsigned GetUint() const { check(IsUint(), "an unsigned 32-bit integer"); return unsigned(as_uint64()); }
    int64_t GetInt64() const { check(IsInt64(), "a 64-bit integer"); return as_int64(); }
    uint64_t GetUint64() const { check(IsUint64(), "an unsigned 64-bit integer"); return as_uint64(); }
    double GetDouble() const {
        switch (_kind) {
        case kind::double_: return load<double>(0);
        case kind::uint: return double(as_uint64());
        case kind::int_: return double(as_int64());
        default: internal::throw_type_error(*this, "a number");
        }
    }
    float GetFloat() const { return float(GetDouble()); }

    std::string_view GetStringView() const {
        switch (_kind) {
        case kind::inline_string:
            return std::string_view(_data, uint8_t(_data[inline_capacity]) & ~no_escape_flag);
        case kind::static_string:
        case kind::owned_string:
            return std::string_view(load<const char*>(0), load<uint32_t>(8));
        case kind::chunk_string:
            return std::string_view(load<internal::chunk_holder*>(0)->buf.get() + load<uint32_t>(8), load<uint16_t>(12));
        default:
            internal::throw_type_error(*this, "a string");
        }
    }
    size_type GetStringLength() const { return GetStringView().size(); }
    void SetString(string_ref_type s) noexcept {
        if (needs_cleanup()) {
            destroy();
        }
        store(0, s.data());
        store(8, s.size());
        _data[14] = 0;
        _kind = kind::static_string;
    }

    // Converts the value to T, using rjson::type_helper<T>.
    template<typename T> T Get() const;

    // Objects
    size_type MemberCount() const;
    bool ObjectEmpty() const { return MemberCount() == 0; }
    MemberIterator MemberBegin();
    MemberIterator MemberEnd();
    ConstMemberIterator MemberBegin() const;
    ConstMemberIterator MemberEnd() const;
    MemberIterator FindMember(std::string_view name);
    ConstMemberIterator FindMember(std::string_view name) const;
    MemberIterator FindMember(const value& name) { return FindMember(name.GetStringView()); }
    ConstMemberIterator FindMember(const value& name) const { return FindMember(name.GetStringView()); }
    bool HasMember(std::string_view name) const { return FindMember(name) != MemberEnd(); }
    bool HasMember(const value& name) const { return HasMember(name.GetStringView()); }
    // Returns the member with the given name, which must exist.
    value& operator[](std::string_view name);
    const value& operator[](std::string_view name) const;
    value& operator[](const value& name) { return (*this)[name.GetStringView()]; }
    const value& operator[](const value& name) const { return (*this)[name.GetStringView()]; }
    // Adds a member, without checking whether one with the same name exists.
    void AddMember(value&& name, value&& v);
    // Removes a member, keeping the order of the others.
    MemberIterator EraseMember(ConstMemberIterator pos);
    // Removes the first member with the given name, replacing it with the
    // last member, like RapidJSON's RemoveMember(). Returns false if there
    // was no such member.
    bool RemoveMember(std::string_view name);
    Object GetObject();
    ConstObject GetObject() const;

    // Arrays
    size_type Size() const;
    bool Empty() const { return Size() == 0; }
    ValueIterator Begin();
    ValueIterator End();
    ConstValueIterator Begin() const;
    ConstValueIterator End() const;
    value& operator[](size_type i);
    const value& operator[](size_type i) const;
    void Reserve(size_type n);
    void PushBack(value&& v);
    void PopBack();
    // Removes an element, keeping the order of the others.
    ValueIterator Erase(ConstValueIterator pos);
    Array GetArray();
    ConstArray GetArray() const;

    // Compares values like RapidJSON: numbers by their value, objects by
    // their members regardless of order, the rest by type and contents.
    friend bool operator==(const value& a, const value& b) noexcept;
    // A value equals a string if it is a string with the same contents.
    friend bool operator==(const value& a, std::string_view b) noexcept {
        return a.IsString() && a.GetStringView() == b;
    }
    template<size_t N>
    friend bool operator==(const value& a, const char (&b)[N]) noexcept {
        return a == std::string_view(b, N - 1);
    }
};

static_assert(sizeof(value) == 16);

struct member {
    rjson::value name;
    rjson::value value;
};

inline size_type value::MemberCount() const {
    check(IsObject(), "an object");
    auto s = container<member>();
    return s ? s->size : 0;
}
inline value::MemberIterator value::MemberBegin() { check(IsObject(), "an object"); return {container<member>(), 0}; }
inline value::MemberIterator value::MemberEnd() { return {container<member>(), MemberCount()}; }
inline value::ConstMemberIterator value::MemberBegin() const { check(IsObject(), "an object"); return {container<member>(), 0}; }
inline value::ConstMemberIterator value::MemberEnd() const { return {container<member>(), MemberCount()}; }

inline size_type value::Size() const {
    check(IsArray(), "an array");
    auto s = container<value>();
    return s ? s->size : 0;
}
inline value::ValueIterator value::Begin() { check(IsArray(), "an array"); return {container<value>(), 0}; }
inline value::ValueIterator value::End() { return {container<value>(), Size()}; }
inline value::ConstValueIterator value::Begin() const { check(IsArray(), "an array"); return {container<value>(), 0}; }
inline value::ConstValueIterator value::End() const { return {container<value>(), Size()}; }

namespace internal {

// Views of the members of an object and the elements of an array, which
// are ranges, as returned by GetObject() and GetArray().
template<bool Const>
class object_view : public std::ranges::view_base {
    using value_type = std::conditional_t<Const, const value, value>;
    using iterator_type = std::conditional_t<Const, value::ConstMemberIterator, value::MemberIterator>;
    value_type* _v;
public:
    explicit object_view(value_type& v) : _v(&v) { _v->MemberCount(); }
    iterator_type begin() const { return _v->MemberBegin(); }
    iterator_type end() const { return _v->MemberEnd(); }
    iterator_type MemberBegin() const { return _v->MemberBegin(); }
    iterator_type MemberEnd() const { return _v->MemberEnd(); }
    size_type MemberCount() const { return _v->MemberCount(); }
    bool ObjectEmpty() const { return _v->ObjectEmpty(); }
    bool HasMember(std::string_view name) const { return _v->HasMember(name); }
    iterator_type FindMember(std::string_view name) const { return _v->FindMember(name); }
    value_type& operator[](std::string_view name) const { return (*_v)[name]; }
};

template<bool Const>
class array_view : public std::ranges::view_base {
    using value_type = std::conditional_t<Const, const value, value>;
    using iterator_type = std::conditional_t<Const, value::ConstValueIterator, value::ValueIterator>;
    value_type* _v;
public:
    explicit array_view(value_type& v) : _v(&v) { _v->Size(); }
    iterator_type begin() const { return _v->Begin(); }
    iterator_type end() const { return _v->End(); }
    iterator_type Begin() const { return _v->Begin(); }
    iterator_type End() const { return _v->End(); }
    size_type Size() const { return _v->Size(); }
    size_t size() const { return _v->Size(); }
    bool Empty() const { return _v->Empty(); }
    value_type& operator[](size_type i) const { return (*_v)[i]; }
};

} // namespace internal

} // namespace rjson

template<bool Const>
inline constexpr bool std::ranges::enable_borrowed_range<rjson::internal::object_view<Const>> = true;
template<bool Const>
inline constexpr bool std::ranges::enable_borrowed_range<rjson::internal::array_view<Const>> = true;

namespace rjson {

inline value::Object value::GetObject() { return Object(*this); }
inline value::ConstObject value::GetObject() const { return ConstObject(*this); }
inline value::Array value::GetArray() { return Array(*this); }
inline value::ConstArray value::GetArray() const { return ConstArray(*this); }

inline value& value::operator[](size_type i) {
    if (i >= Size()) [[unlikely]] {
        throw error(fmt::format("rjson: array index {} out of range of {} elements", i, Size()));
    }
    return (*container<value>())[i];
}

inline const value& value::operator[](size_type i) const {
    return const_cast<value&>(*this)[i];
}

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

// Malformed JSON text, found at the given offset of the input.
class parse_error : public error {
    size_t _offset;
public:
    parse_error(std::string_view msg, size_t offset);
    size_t offset() const noexcept { return _offset; }
};

// Returns an object representing JSON's null
inline rjson::value null_value() {
    return rjson::value();
}

// Returns an empty JSON object - {}
inline rjson::value empty_object() {
    return rjson::value(kObjectType);
}

// Returns an empty JSON array - []
inline rjson::value empty_array() {
    return rjson::value(kArrayType);
}

// Returns an empty JSON string - ""
inline rjson::value empty_string() {
    return rjson::value(kStringType);
}

// Convert the JSON value to a string with JSON syntax, the opposite of parse().
// The representation is dense - without any redundant indentation.
std::string print(const rjson::value& value, size_t max_nested_level = default_max_nested_level);

// Writes the JSON value to the output_stream, like print() does to a string,
// but in buffers of limited size, so no large contiguous string is created,
// and yields when the task quota runs out.
// Note: input value must remain valid until the future resolves.
seastar::future<> print(const rjson::value& value, seastar::output_stream<char>&, size_t max_nested_level = default_max_nested_level);

// A variant of print() which prints a JSON object (it assumes the given
// rjson::value is an object) but adds to it at the end an additional array.
// That extra array is given a separate chunked_vector, to avoid putting it
// inside the rjson::value. The value and the array must remain valid until
// the future resolves.
future<> print_with_extra_array(const rjson::value& value,
    std::string_view array_name,
    const utils::chunked_vector<rjson::value>& array,
    seastar::output_stream<char>& os,
    size_t max_nested_level = default_max_nested_level);

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
            value::operator=(copy(v));
        }
        return *this;
    }
    copyable_value& operator=(copyable_value&& v) noexcept {
        value::operator=(value(std::move(v)));
        return *this;
    }
};

// Returns a human-readable name for the given rjson type.
inline std::string_view type_name(rjson::type t) {
    switch (t) {
    case kNullType: return "kNullType";
    case kFalseType: return "kFalseType";
    case kTrueType: return "kTrueType";
    case kObjectType: return "kObjectType";
    case kArrayType: return "kArrayType";
    case kStringType: return "kStringType";
    case kNumberType: return "kNumberType";
    }
    throw std::runtime_error("unknown rjson type provided");
}

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
// function, which frees each buffer once it is parsed, unless the parsed
// value keeps referencing long strings in it.
rjson::value parse(chunked_content&&, size_t max_nested_level = default_max_nested_level);
rjson::value parse_yieldable(chunked_content&&, size_t max_nested_level = default_max_nested_level);

// Parses like parse(chunked_content&&), yielding when the task quota runs out.
future<rjson::value> parse_gently(chunked_content&&, size_t max_nested_level = default_max_nested_level);

// Supplies parse_sax() with input, one buffer at a time. An empty buffer
// marks the end of the input.
using chunk_source = noncopyable_function<temporary_buffer<char>()>;

namespace internal {
// The handler of parse_sax(), as an interface.
class sax_handler {
public:
    virtual ~sax_handler() = default;
    virtual bool null() = 0;
    virtual bool boolean(bool b) = 0;
    virtual bool integer(int64_t i) = 0;
    virtual bool unsigned_integer(uint64_t u) = 0;
    virtual bool floating(double d) = 0;
    virtual bool string(std::string_view s) = 0;
    virtual bool key(std::string_view s) = 0;
    virtual bool start_object() = 0;
    virtual bool end_object(size_type members) = 0;
    virtual bool start_array() = 0;
    virtual bool end_array(size_type elements) = 0;
};

template<typename Handler>
class sax_adapter final : public sax_handler {
    Handler& _h;
public:
    explicit sax_adapter(Handler& h) : _h(h) {}
    bool null() override { return _h.Null(); }
    bool boolean(bool b) override { return _h.Bool(b); }
    // Like RapidJSON, report integers with the narrowest type which fits.
    bool integer(int64_t i) override {
        return i >= std::numeric_limits<int32_t>::min() ? _h.Int(int(i)) : _h.Int64(i);
    }
    bool unsigned_integer(uint64_t u) override {
        return u <= std::numeric_limits<uint32_t>::max() ? _h.Uint(unsigned(u)) : _h.Uint64(u);
    }
    bool floating(double d) override { return _h.Double(d); }
    bool string(std::string_view s) override { return _h.String(s.data(), size_type(s.size()), true); }
    bool key(std::string_view s) override { return _h.Key(s.data(), size_type(s.size()), true); }
    bool start_object() override { return _h.StartObject(); }
    bool end_object(size_type members) override { return _h.EndObject(members); }
    bool start_array() override { return _h.StartArray(); }
    bool end_array(size_type elements) override { return _h.EndArray(elements); }
};

void parse_sax(chunk_source& source, sax_handler& handler, size_t max_nested_level);
}

// Parses JSON read from source, reporting each token to handler. The handler
// follows the SAX Handler concept of RapidJSON, with these methods:
//   Null(), Bool(bool), Int(int), Uint(unsigned), Int64(int64_t),
//   Uint64(uint64_t), Double(double), String(const char*, size_type, bool),
//   Key(const char*, size_type, bool), StartObject(), EndObject(size_type),
//   StartArray(), EndArray(size_type)
// Each returns false to stop parsing. The strings passed to String() and Key()
// are valid only during the call.
// Throws rjson::parse_error on malformed JSON, or when the handler stops
// the parsing. Runs synchronously; the source may wait for its input only
// in a seastar thread.
template<typename Handler>
void parse_sax(chunk_source source, Handler& handler, size_t max_nested_level = default_max_nested_level) {
    internal::sax_adapter<Handler> adapter(handler);
    internal::parse_sax(source, adapter, max_nested_level);
}

// Creates a JSON value (of JSON string type) out of internal string representations.
// The string value is copied, so str's liveness does not need to be persisted.
rjson::value from_string(const char* str, size_t size);
rjson::value from_string(std::string_view view);

// Returns a string_view to the string held in a JSON value.
// Throws if the value does not hold a string.
// This is a view to the existing data - no copying is done.
inline std::string_view to_string_view(const rjson::value& v) {
    if (!v.IsString()) {
        throw std::runtime_error(fmt::format("rjson::to_string_view expects a string value, got: {}", type_name(v.GetType())));
    }
    return v.GetStringView();
}

// Those functions must be called on json string object.
// They make a copy of underlying data so it's safe to destroy
// rjson::value afterwards.
inline sstring to_sstring(const rjson::value& str) {
    if (!str.IsString()) {
        throw std::runtime_error(fmt::format("rjson::to_sstring expects a string value, got: {}", type_name(str.GetType())));
    }
    return sstring(str.GetStringView());
}
inline std::string to_string(const rjson::value& str) {
    if (!str.IsString()) {
        throw std::runtime_error(fmt::format("rjson::to_string expects a string value, got: {}", type_name(str.GetType())));
    }
    return std::string(str.GetStringView());
}
// Returns a pointer to JSON member if it exists, nullptr otherwise
rjson::value* find(rjson::value& value, std::string_view name);
const rjson::value* find(const rjson::value& value, std::string_view name);

// Returns a reference to JSON member if it exists, throws otherwise
rjson::value& get(rjson::value& value, std::string_view name);
const rjson::value& get(const rjson::value& value, std::string_view name);

// Converts between rjson::value and T, for rjson::get<T>(), rjson::get_opt<T>()
// and rjson::add(..., T). This primary template handles booleans, numbers and
// std::string; specialize it for other types, providing:
//   static T get(const rjson::value&);  // throws if the value doesn't hold a T
//   static rjson::value make(const T&);
template<typename T>
struct type_helper {
    static T get(const rjson::value& v) {
        if constexpr (std::is_same_v<T, bool>) {
            return v.GetBool();
        } else if constexpr (std::is_same_v<T, std::string>) {
            return to_string(v);
        } else if constexpr (std::is_floating_point_v<T>) {
            return T(v.GetDouble());
        } else if constexpr (std::is_integral_v<T> && std::is_signed_v<T> && sizeof(T) <= sizeof(int)) {
            return v.GetInt();
        } else if constexpr (std::is_integral_v<T> && std::is_signed_v<T>) {
            return v.GetInt64();
        } else if constexpr (std::is_integral_v<T> && sizeof(T) <= sizeof(unsigned)) {
            return v.GetUint();
        } else {
            static_assert(std::is_integral_v<T> && std::is_unsigned_v<T>, "specialize rjson::type_helper for this type");
            return v.GetUint64();
        }
    }
    static rjson::value make(const T& x) {
        if constexpr (std::is_same_v<T, std::string>) {
            return from_string(x);
        } else {
            return rjson::value(x);
        }
    }
};

template<typename T>
T value::Get() const {
    return type_helper<T>::get(*this);
}

/**
 * Type conversion getter.
 * Requires rjson::type_helper<T> to support the type queried.
 */
template<typename T>
T get(const rjson::value& value, std::string_view name) {
    auto& v = get(value, name);
    try {
        return type_helper<T>::get(v);
    } catch (...) {
        std::throw_with_nested(malformed_value(name, v));
    }
}

/**
 * Type conversion opt getter.
 * Requires rjson::type_helper<T> to support the type queried.
 *
 * Return std::nullopt if value does not exist.
 */
template<typename T>
std::optional<T> get_opt(const rjson::value& value, std::string_view name) {
    auto* v = find(value, name);
    try {
        return v ? std::optional<T>(type_helper<T>::get(*v)) : std::nullopt;
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
// Throws if base is not a JSON object.
// Assumes a member with the same name does not yet exist in base.
void add_with_string_name(rjson::value& base, std::string_view name, rjson::string_ref_type member);

// Adds a member to a given JSON object by moving the member.
// Throws if base is not a JSON object.
// Assumes a member with the same name does not yet exist in base.
void add(rjson::value& base, rjson::string_ref_type name, rjson::value&& member);

// Adds a string member to a given JSON object by assigning its reference.
// Throws if base is not a JSON object.
// Assumes a member with the same name does not yet exist in base.
void add(rjson::value& base, rjson::string_ref_type name, rjson::string_ref_type member);

/**
 * Type conversion setter.
 * Requires rjson::type_helper<T> to support the type written.
 *
 * (Note: order is important. Need to be after set(..., rjson::value)).
 * Assumes a member with the same name does not yet exist in base.
 */
template<typename T>
requires (!std::is_constructible_v<string_ref_type, T>)
void add(rjson::value& base, rjson::string_ref_type name, T&& member) {
    add(base, std::move(name), type_helper<std::remove_cvref_t<T>>::make(member));
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
    return ::base64_decode(to_string_view(v));
}

// A writer which allows writing json into an std::ostream in a streaming manner.
// It follows the method names of RapidJSON's Writer, and throws rjson::error
// when the calls do not form a valid JSON document.
class streaming_writer {
    std::ostream& _os;
    // For each open object or array: whether it is an object, and the
    // number of keys and values written in it so far.
    struct level {
        bool object;
        size_t count;
    };
    std::vector<level> _levels;
    bool _has_root = false;

    void prefix(bool is_key);
    void write(std::string_view s) { _os.write(s.data(), s.size()); }
public:
    streaming_writer(std::ostream& os = std::cout) : _os(os) {}

    bool Null();
    bool Bool(bool b);
    bool Int(int i) { return Int64(i); }
    bool Uint(unsigned i) { return Uint64(i); }
    bool Int64(int64_t i);
    bool Uint64(uint64_t i);
    bool Double(double d);
    bool RawNumber(std::string_view str);
    bool RawValue(std::string_view json, type root_type);
    bool String(std::string_view str);
    bool StartObject();
    bool Key(std::string_view str);
    bool EndObject(size_type memberCount = 0);
    bool StartArray();
    bool EndArray(size_type elementCount = 0);

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
