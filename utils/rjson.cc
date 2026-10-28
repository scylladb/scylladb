/*
 * Copyright 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "rjson.hh"
#include "rjson_fast_float.hh"
#include "utils/small_vector.hh"
#include <algorithm>
#include <charconv>
#include <cmath>
#include <cstdlib>
#include <iomanip>
#include <sstream>
#include <seastar/core/format.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/preempt.hh>
#ifdef SANITIZE
#include <seastar/core/memory.hh>
#endif

namespace rjson {

void* allocator::Malloc(size_t size) {
    // For bypassing the address sanitizer failure in debug mode - allocating
    // too much memory results in an abort
    #ifdef SANITIZE
    if (size > memory::stats().total_memory()) {
        throw rjson::error(format("Failed to allocate {} bytes", size));
    }
    #endif
    void* ret = std::malloc(size);
    if (size > 0 && !ret) {
        throw rjson::error(format("Failed to allocate {} bytes", size));
    }
    return ret;
}

void* allocator::Realloc(void* ptr, size_t old_size, size_t new_size) {
    #ifdef SANITIZE
    if (new_size > memory::stats().total_memory()) {
        throw rjson::error(format("Failed to allocate {} bytes", new_size));
    }
    #endif
    void* ret = std::realloc(ptr, new_size);
    if (new_size > 0 && !ret) {
        throw rjson::error(format("Failed to reallocate {} bytes to {} bytes from {}", old_size, new_size, ptr));
    }
    return ret;
}

void allocator::Free(void* ptr) noexcept {
    std::free(ptr);
}

// memcpy() from a null pointer is undefined even for 0 bytes, and an empty
// std::string_view, or a buffer not allocated yet, may have a null data().
static void copy_chars(char* dst, const char* src, size_t n) noexcept {
    if (n) {
        std::memcpy(dst, src, n);
    }
}

namespace internal {

void retain(chunk_holder* h) noexcept {
    h->refs.fetch_add(1, std::memory_order_relaxed);
}

void release(chunk_holder* h) noexcept {
    if (h->refs.fetch_sub(1, std::memory_order_acq_rel) != 1) {
        return;
    }
    auto holder = std::unique_ptr<chunk_holder>(h);
    if (auto owner = holder->owner; owner != this_shard_id()) {
        // Like foreign_ptr, free the buffer on the shard it came from. The
        // lambda itself is destroyed back on this shard, so it frees the
        // buffer when it runs.
        (void)smp::submit_to(owner, [holder = std::move(holder)] () mutable {
            holder.reset();
        });
    }
}

[[noreturn]] void throw_type_error(const value& v, const char* expected) {
    throw error(fmt::format("rjson: expected {}, got {}", expected, type_name(v.GetType())));
}

namespace {

// Allocates a storage<T> for capacity elements.
template<typename T>
storage<T>* allocate_storage(uint32_t capacity) {
    using S = storage<T>;
    if (capacity <= S::segment_size) {
        auto* s = static_cast<S*>(allocator::Malloc(sizeof(S) + size_t(capacity) * sizeof(T)));
        s->size = 0;
        s->capacity = capacity;
        return s;
    }
    uint32_t segments = (uint64_t(capacity) + S::segment_mask) >> S::segment_shift;
    auto* s = static_cast<S*>(allocator::Malloc(sizeof(S) + std::bit_ceil(segments) * sizeof(T*)));
    s->size = 0;
    s->capacity = 0;
    try {
        for (uint32_t i = 0; i < segments; ++i) {
            s->segments()[i] = static_cast<T*>(allocator::Malloc(S::segment_bytes));
            ++s->capacity;
        }
    } catch (...) {
        for (uint32_t i = 0; i < s->capacity; ++i) {
            allocator::Free(s->segments()[i]);
        }
        allocator::Free(s);
        throw;
    }
    s->capacity = segments << S::segment_shift;
    return s;
}

// Destroys the elements of s and frees it.
template<typename T>
void free_storage(storage<T>* s) noexcept {
    for (uint32_t i = 0; i < s->size; ++i) {
        (*s)[i].~T();
    }
    if (s->segmented()) {
        for (uint32_t i = 0; i < (s->capacity >> storage<T>::segment_shift); ++i) {
            allocator::Free(s->segments()[i]);
        }
    }
    allocator::Free(s);
}

// Returns s, or a replacement of it, with room for at least one more element.
template<typename T>
storage<T>* grow_storage(storage<T>* s) {
    using S = storage<T>;
    if (!s) {
        return allocate_storage<T>(4);
    }
    if (s->capacity < S::segment_size) {
        uint32_t capacity = std::min(s->capacity * 2, S::segment_size);
        s = static_cast<S*>(allocator::Realloc(s, sizeof(S) + size_t(s->capacity) * sizeof(T), sizeof(S) + size_t(capacity) * sizeof(T)));
        s->capacity = capacity;
        return s;
    }
    if (!s->segmented()) {
        // Turn a full single block into the first segment of a table.
        auto* t = static_cast<S*>(allocator::Malloc(sizeof(S) + 2 * sizeof(T*)));
        try {
            t->segments()[0] = static_cast<T*>(allocator::Malloc(S::segment_bytes));
            try {
                t->segments()[1] = static_cast<T*>(allocator::Malloc(S::segment_bytes));
            } catch (...) {
                allocator::Free(t->segments()[0]);
                throw;
            }
        } catch (...) {
            allocator::Free(t);
            throw;
        }
        std::memcpy(static_cast<void*>(t->segments()[0]), s->elements(), S::segment_bytes);
        t->size = s->size;
        t->capacity = 2 * S::segment_size;
        allocator::Free(s);
        return t;
    }
    uint32_t segments = s->capacity >> S::segment_shift;
    T* segment = static_cast<T*>(allocator::Malloc(S::segment_bytes));
    if (segments == std::bit_ceil(segments)) {
        try {
            s = static_cast<S*>(allocator::Realloc(s, sizeof(S) + segments * sizeof(T*), sizeof(S) + 2 * segments * sizeof(T*)));
        } catch (...) {
            allocator::Free(segment);
            throw;
        }
    }
    s->segments()[segments] = segment;
    s->capacity += S::segment_size;
    return s;
}

// Moves the bytes of an element, leaving the source to be overwritten
// without destruction.
template<typename T>
void relocate(T& to, T& from) noexcept {
    std::memcpy(static_cast<void*>(&to), static_cast<const void*>(&from), sizeof(T));
}

// Removes element i, moving the following ones back.
template<typename T>
void erase_at(storage<T>* s, uint32_t i) noexcept {
    (*s)[i].~T();
    if (!s->segmented()) {
        std::memmove(static_cast<void*>(s->elements() + i), static_cast<const void*>(s->elements() + i + 1), (s->size - i - 1) * sizeof(T));
    } else {
        for (uint32_t j = i + 1; j < s->size; ++j) {
            relocate((*s)[j - 1], (*s)[j]);
        }
    }
    --s->size;
}

} // anonymous namespace
} // namespace internal

using internal::storage;

value::value(type t) noexcept {
    switch (t) {
    case kNullType: _kind = kind::null; break;
    case kFalseType: _kind = kind::false_; break;
    case kTrueType: _kind = kind::true_; break;
    case kObjectType: set_container<member>(nullptr); _kind = kind::object; break;
    case kArrayType: set_container<value>(nullptr); _kind = kind::array; break;
    case kStringType: _data[inline_capacity] = no_escape_flag; _kind = kind::inline_string; break;
    case kNumberType: set_uint(0); break;
    }
}

type value::GetType() const noexcept {
    switch (_kind) {
    case kind::null: return kNullType;
    case kind::false_: return kFalseType;
    case kind::true_: return kTrueType;
    case kind::object: return kObjectType;
    case kind::array: return kArrayType;
    case kind::inline_string:
    case kind::static_string:
    case kind::owned_string:
    case kind::chunk_string:
        return kStringType;
    case kind::uint:
    case kind::int_:
    case kind::double_:
        return kNumberType;
    }
    return kNullType;
}

void value::destroy() noexcept {
    switch (_kind) {
    case kind::owned_string:
        allocator::Free(const_cast<char*>(load<const char*>(0)));
        break;
    case kind::chunk_string:
        internal::release(load<internal::chunk_holder*>(0));
        break;
    case kind::object:
        if (auto s = container<member>()) {
            internal::free_storage(s);
        }
        break;
    case kind::array:
        if (auto s = container<value>()) {
            internal::free_storage(s);
        }
        break;
    default:
        break;
    }
    _kind = kind::null;
}

template<typename T>
void value::push(T&& elem) {
    auto s = container<T>();
    if (!s || s->size == s->capacity) {
        s = internal::grow_storage(s);
        set_container(s);
    }
    new (&(*s)[s->size]) T(std::move(elem));
    ++s->size;
}

template<typename T>
void value::reserve_more(uint32_t n) {
    auto s = container<T>();
    uint32_t size = s ? s->size : 0;
    if (s && s->capacity >= size + n) {
        return;
    }
    auto t = internal::allocate_storage<T>(size + n);
    for (uint32_t i = 0; i < size; ++i) {
        internal::relocate((*t)[i], (*s)[i]);
    }
    t->size = size;
    if (s) {
        s->size = 0;
        internal::free_storage(s);
    }
    set_container(t);
}

value::MemberIterator value::FindMember(std::string_view name) {
    auto s = container<member>();
    uint32_t n = MemberCount();
    for (uint32_t i = 0; i < n; ++i) {
        if ((*s)[i].name.GetStringView() == name) {
            return {s, i};
        }
    }
    return {s, n};
}

value::ConstMemberIterator value::FindMember(std::string_view name) const {
    return const_cast<value&>(*this).FindMember(name);
}

value& value::operator[](std::string_view name) {
    auto it = FindMember(name);
    if (it == MemberEnd()) {
        throw error(fmt::format("rjson: no member named '{}'", name));
    }
    return it->value;
}

const value& value::operator[](std::string_view name) const {
    return const_cast<value&>(*this)[name];
}

void value::AddMember(value&& name, value&& v) {
    check(IsObject(), "an object");
    name.check(name.IsString(), "a string member name");
    push(member{std::move(name), std::move(v)});
}

value::MemberIterator value::EraseMember(ConstMemberIterator pos) {
    auto s = container<member>();
    if (pos.index() >= MemberCount()) {
        throw error("rjson: erasing a member past the end of the object");
    }
    internal::erase_at(s, pos.index());
    return {s, pos.index()};
}

bool value::RemoveMember(std::string_view name) {
    auto it = FindMember(name);
    if (it == MemberEnd()) {
        return false;
    }
    auto s = container<member>();
    uint32_t last = s->size - 1;
    (*s)[it.index()].~member();
    if (it.index() != last) {
        internal::relocate((*s)[it.index()], (*s)[last]);
    }
    --s->size;
    return true;
}

void value::Reserve(size_type n) {
    uint32_t size = Size();
    if (n > size) {
        reserve_more<value>(n - size);
    }
}

void value::PushBack(value&& v) {
    check(IsArray(), "an array");
    push(std::move(v));
}

void value::PopBack() {
    if (Empty()) {
        throw error("rjson: PopBack() on an empty array");
    }
    auto s = container<value>();
    (*s)[--s->size].~value();
}

value::ValueIterator value::Erase(ConstValueIterator pos) {
    auto s = container<value>();
    if (pos.index() >= Size()) {
        throw error("rjson: erasing an element past the end of the array");
    }
    internal::erase_at(s, pos.index());
    return {s, pos.index()};
}

bool operator==(const value& a, const value& b) noexcept {
    if (a.IsNumber() && b.IsNumber()) {
        if (a.IsDouble() || b.IsDouble()) {
            double x = a.GetDouble();
            double y = b.GetDouble();
            return x >= y && x <= y;
        }
        return a._kind == b._kind && a.as_uint64() == b.as_uint64();
    }
    if (a.GetType() != b.GetType()) {
        return false;
    }
    switch (a.GetType()) {
    case kStringType:
        return a.GetStringView() == b.GetStringView();
    case kArrayType: {
        if (a.Size() != b.Size()) {
            return false;
        }
        for (uint32_t i = 0; i < a.Size(); ++i) {
            if (!(a[i] == b[i])) {
                return false;
            }
        }
        return true;
    }
    case kObjectType: {
        if (a.MemberCount() != b.MemberCount()) {
            return false;
        }
        for (auto& m : a.GetObject()) {
            auto it = b.FindMember(m.name.GetStringView());
            if (it == b.MemberEnd() || !(m.value == it->value)) {
                return false;
            }
        }
        return true;
    }
    default:
        return true;
    }
}

// Makes a string value holding a copy of s. no_escape tells that s has no
// characters which JSON needs escaped.
// Makes v, a null value, a string. Builds it in place, so that values being
// parsed needn't be moved, see value_builder.
void set_string(value& v, std::string_view s, bool no_escape) {
    uint8_t flags = no_escape ? value::no_escape_flag : 0;
    if (s.size() <= value::inline_capacity) {
        copy_chars(v._data, s.data(), s.size());
        v._data[value::inline_capacity] = char(s.size() | flags);
        v._kind = value::kind::inline_string;
        return;
    }
    if (s.size() > std::numeric_limits<uint32_t>::max()) {
        throw error(fmt::format("rjson: string of {} bytes is too long", s.size()));
    }
    char* p = static_cast<char*>(allocator::Malloc(s.size()));
    std::memcpy(p, s.data(), s.size());
    v.store(0, static_cast<const char*>(p));
    v.store(8, uint32_t(s.size()));
    v._data[14] = char(flags);
    v._kind = value::kind::owned_string;
}

value make_string(std::string_view s, bool no_escape) {
    value v;
    set_string(v, s, no_escape);
    return v;
}

// Makes v, a null value, a string referencing len bytes at offset of h's
// buffer, which need no escaping. Takes over a reference to h.
void set_chunk_string(value& v, internal::chunk_holder* h, uint32_t offset, uint32_t len) {
    v.store(0, h);
    v.store(8, offset);
    v.store(12, uint16_t(len));
    v._data[14] = char(value::no_escape_flag);
    v._kind = value::kind::chunk_string;
}

// Returns true unless the string value is known to have no characters
// which JSON needs escaped.
bool needs_escaping(const value& v) noexcept {
    return !(uint8_t(v._data[value::inline_capacity]) & value::no_escape_flag);
}

namespace {

// A string up to this long is copied rather than referencing the input
// buffer, as copying it is cheaper than the bookkeeping.
constexpr size_t min_chunk_string = 64;

// Returns the first character in [p, end) which can't appear unescaped in
// a JSON string: a quote, a backslash or a control character; or end.
// Uses vector instructions, which compile to SSE/AVX or NEON.
bool is_special(char ch) noexcept {
    unsigned char c = ch;
    return c == '"' || c == '\\' || c < 0x20;
}

const char* find_special(const char* p, const char* end) noexcept {
    // Many strings, like member names and DynamoDB's type tags, are a few
    // characters long: look at the first 8 in a 64-bit word.
    if (end - p >= 8) {
        constexpr uint64_t ones = 0x0101010101010101;
        constexpr uint64_t highs = 0x8080808080808080;
        uint64_t w;
        std::memcpy(&w, p, 8);
        auto zero_bytes = [] (uint64_t x) { return (x - ones) & ~x & highs; };
        // Bytes which are a quote, a backslash, or below 0x20. The lowest
        // set bit is exact, which is all that is used.
        uint64_t special = zero_bytes(w ^ (ones * '"')) | zero_bytes(w ^ (ones * '\\')) | ((w - ones * 0x20) & ~w & highs);
        if (special) {
            return p + std::countr_zero(special) / 8;
        }
        p += 8;
    }
    // One vector compare finds the end of most longer strings.
    using vec = uint8_t __attribute__((vector_size(32)));
    while (end - p >= 32) {
        vec v;
        std::memcpy(&v, p, sizeof(v));
        auto special = (v == '"') | (v == '\\') | (v < 0x20);
#ifdef __AVX2__
        using char_vec = char __attribute__((vector_size(32)));
        if (auto mask = unsigned(__builtin_ia32_pmovmskb256((char_vec)special))) {
            return p + std::countr_zero(mask);
        }
#else
        if (__builtin_reduce_or(special)) {
            break;
        }
#endif
        p += 32;
    }
    while (p != end && !is_special(*p)) {
        ++p;
    }
    return p;
}

bool is_whitespace(char c) noexcept {
    return c == ' ' || c == '\n' || c == '\r' || c == '\t';
}

// Returns the first character in [p, end) which isn't whitespace, or end.
// Uses vector instructions for runs, like the indentation of pretty-printed
// documents.
const char* skip_whitespace(const char* p, const char* end) noexcept {
    using vec = uint8_t __attribute__((vector_size(32)));
    while (end - p >= 32) {
        vec v;
        std::memcpy(&v, p, sizeof(v));
        auto ws = (v == ' ') | (v == '\n') | (v == '\r') | (v == '\t');
#ifdef __AVX2__
        using char_vec = char __attribute__((vector_size(32)));
        if (auto mask = unsigned(__builtin_ia32_pmovmskb256((char_vec)ws)); mask != ~0u) {
            return p + std::countr_one(mask);
        }
#else
        if (!__builtin_reduce_and(ws)) {
            break;
        }
#endif
        p += 32;
    }
    while (p != end && is_whitespace(*p)) {
        ++p;
    }
    return p;
}

bool is_digit(char c) noexcept {
    return c >= '0' && c <= '9';
}

// RapidJSON's parse error codes and messages, which we keep.
enum class parse_error_code {
    document_empty,
    root_not_singular,
    value_invalid,
    object_miss_name,
    object_miss_colon,
    object_miss_comma_or_curly_bracket,
    array_miss_comma_or_square_bracket,
    string_unicode_escape_invalid_hex,
    string_unicode_surrogate_invalid,
    string_escape_invalid,
    string_miss_quotation_mark,
    string_invalid_encoding,
    number_too_big,
    number_miss_fraction,
    number_miss_exponent,
    termination,
};

std::string_view message(parse_error_code code) {
    switch (code) {
    case parse_error_code::document_empty: return "The document is empty.";
    case parse_error_code::root_not_singular: return "The document root must not be followed by other values.";
    case parse_error_code::value_invalid: return "Invalid value.";
    case parse_error_code::object_miss_name: return "Missing a name for object member.";
    case parse_error_code::object_miss_colon: return "Missing a colon after a name of object member.";
    case parse_error_code::object_miss_comma_or_curly_bracket: return "Missing a comma or '}' after an object member.";
    case parse_error_code::array_miss_comma_or_square_bracket: return "Missing a comma or ']' after an array element.";
    case parse_error_code::string_unicode_escape_invalid_hex: return "Incorrect hex digit after \\u escape in string.";
    case parse_error_code::string_unicode_surrogate_invalid: return "The surrogate pair in string is invalid.";
    case parse_error_code::string_escape_invalid: return "Invalid escape character in string.";
    case parse_error_code::string_miss_quotation_mark: return "Missing a closing quotation mark in string.";
    case parse_error_code::string_invalid_encoding: return "Invalid encoding in string.";
    case parse_error_code::number_too_big: return "Number too big to be stored in double.";
    case parse_error_code::number_miss_fraction: return "Miss fraction part in number.";
    case parse_error_code::number_miss_exponent: return "Miss exponent in number.";
    case parse_error_code::termination: return "Terminate parsing due to Handler error.";
    }
    return "Unknown error.";
}

[[noreturn]] void throw_nested_level_error(size_t max_nested_level) {
    throw error(format("Max nested level reached: {}", max_nested_level));
}

// The input of the parser: a sequence of buffers.
class input {
protected:
    const char* _begin = nullptr;
    const char* _end = nullptr;
    // The offset of _begin in the whole input.
    size_t _base = 0;

    void set(const char* begin, const char* end) noexcept {
        _base += _end - _begin;
        _begin = begin;
        _end = end;
    }
public:
    virtual ~input() = default;
    const char* begin() const noexcept { return _begin; }
    const char* end() const noexcept { return _end; }
    size_t base() const noexcept { return _base; }
    // Moves to the next non-empty buffer, which may free the current one.
    // Returns false at the end of the input.
    virtual bool next() = 0;
    // Returns a chunk_holder keeping the current buffer alive, with a
    // reference for the caller, or null if the buffer can't be kept.
    virtual internal::chunk_holder* hold_current() { return nullptr; }
};

class contiguous_input final : public input {
public:
    explicit contiguous_input(std::string_view s) {
        set(s.data(), s.data() + s.size());
        // Skip a UTF-8 byte order mark like RapidJSON did: each of its
        // bytes, if present.
        for (char c : {'\xEF', '\xBB', '\xBF'}) {
            if (_begin != _end && *_begin == c) {
                ++_begin;
                ++_base;
            }
        }
    }
    bool next() override { return false; }
};

class chunked_input final : public input {
    chunked_content _content;
    size_t _current = 0;
    // Holds the current buffer once a value references it.
    internal::chunk_holder* _holder = nullptr;

    void drop_current() noexcept {
        if (_holder) {
            internal::release(std::exchange(_holder, nullptr));
        } else if (_current < _content.size()) {
            _content[_current] = temporary_buffer<char>();
        }
    }
public:
    explicit chunked_input(chunked_content&& content) : _content(std::move(content)) {
        while (_current < _content.size() && _content[_current].empty()) {
            ++_current;
        }
        if (_current < _content.size()) {
            set(_content[_current].get(), _content[_current].get() + _content[_current].size());
        }
    }
    ~chunked_input() {
        drop_current();
    }
    bool next() override {
        if (_current == _content.size()) {
            return false;
        }
        drop_current();
        while (++_current < _content.size()) {
            if (!_content[_current].empty()) {
                set(_content[_current].get(), _content[_current].get() + _content[_current].size());
                return true;
            }
        }
        set(_end, _end);
        return false;
    }
    internal::chunk_holder* hold_current() override {
        if (!_holder) {
            if (_current == _content.size()) {
                return nullptr;
            }
            _holder = std::make_unique<internal::chunk_holder>(1, this_shard_id(), std::move(_content[_current])).release();
        }
        internal::retain(_holder);
        return _holder;
    }
};

class source_input final : public input {
    chunk_source& _source;
    temporary_buffer<char> _buf;
public:
    explicit source_input(chunk_source& source) : _source(source) {
        next();
    }
    bool next() override {
        for (;;) {
            _buf = _source();
            if (_buf.empty()) {
                set(_end, _end);
                return false;
            }
            set(_buf.get(), _buf.get() + _buf.size());
            return true;
        }
    }
};

// Parses JSON text from an input, reporting its tokens to Handler, which has:
//   bool null(), boolean(bool), integer(int64_t), unsigned_integer(uint64_t),
//        floating(double), start_object(), end_object(size_type),
//        start_array(), end_array(size_type),
//        string(std::string_view, bool in_input, bool no_escape),
//        key(std::string_view, bool in_input, bool no_escape)
// where in_input tells that the string lies in the current input buffer.
// The parser keeps its state in an explicit stack rather than recursing, so
// run() can stop between tokens and continue in the next call.
template<typename Handler>
class reader {
    enum class state : uint8_t {
        start,
        value,
        value_or_end_array,
        key_or_end_object,
        key,
        colon,
        comma_or_end_object,
        comma_or_end_array,
        done,
    };
    struct level {
        bool object;
        size_type count;
    };

    input& _in;
    Handler& _h;
    size_t _max_nested_level;
    const char* _p;
    state _state = state::start;
    utils::small_vector<level, 16> _levels;
    // A token split between buffers, or a string with escapes.
    std::string _token;

    size_t offset(const char* p) const noexcept { return _in.base() + (p - _in.begin()); }
    size_t offset() const noexcept { return offset(_p); }

    [[noreturn]] void fail(parse_error_code code, size_t offset) {
        throw parse_error(message(code), offset);
    }

    // Makes _p point at a character, moving to the next buffers if needed.
    // Returns false at the end of the input.
    bool more() {
        while (_p == _in.end()) {
            if (!_in.next()) {
                return false;
            }
            _p = _in.begin();
        }
        return true;
    }

    bool skip_whitespace() {
        for (;;) {
            // Work on a copy of _p: a char store could alias the member, so
            // updating _p itself would store it on every character.
            const char* p = _p;
            const char* end = _in.end();
            if (p != end && !is_whitespace(*p)) {
                return true;
            }
            _p = p = rjson::skip_whitespace(p, end);
            if (p != end) {
                return true;
            }
            if (!more()) {
                return false;
            }
        }
    }

    void check(bool ok, size_t token_offset) {
        if (!ok) [[unlikely]] {
            fail(parse_error_code::termination, token_offset);
        }
    }

    void after_value() {
        if (_levels.empty()) {
            _state = state::done;
        } else {
            ++_levels.back().count;
            _state = _levels.back().object ? state::comma_or_end_object : state::comma_or_end_array;
        }
    }

    void open(bool object) {
        if (_levels.size() + 1 > _max_nested_level) {
            throw_nested_level_error(_max_nested_level);
        }
        size_t token_offset = offset();
        ++_p;
        _levels.push_back(level{object, 0});
        if (object) {
            check(_h.start_object(), token_offset);
            _state = state::key_or_end_object;
        } else {
            check(_h.start_array(), token_offset);
            _state = state::value_or_end_array;
        }
    }

    void close() {
        size_t token_offset = offset();
        ++_p;
        level l = _levels.back();
        _levels.pop_back();
        check(l.object ? _h.end_object(l.count) : _h.end_array(l.count), token_offset);
        after_value();
    }

    void parse_literal(std::string_view literal) {
        size_t token_offset = offset();
        ++_p;
        for (char c : literal.substr(1)) {
            if (!more() || *_p != c) {
                fail(parse_error_code::value_invalid, offset());
            }
            ++_p;
        }
        switch (literal[0]) {
        case 't': check(_h.boolean(true), token_offset); break;
        case 'f': check(_h.boolean(false), token_offset); break;
        default: check(_h.null(), token_offset); break;
        }
        after_value();
    }

    unsigned parse_hex4(size_t escape_offset) {
        unsigned codepoint = 0;
        for (int i = 0; i < 4; ++i) {
            if (!more()) {
                fail(parse_error_code::string_unicode_escape_invalid_hex, escape_offset);
            }
            char c = *_p++;
            codepoint <<= 4;
            if (c >= '0' && c <= '9') {
                codepoint |= c - '0';
            } else if (c >= 'a' && c <= 'f') {
                codepoint |= c - 'a' + 10;
            } else if (c >= 'A' && c <= 'F') {
                codepoint |= c - 'A' + 10;
            } else {
                fail(parse_error_code::string_unicode_escape_invalid_hex, escape_offset);
            }
        }
        return codepoint;
    }

    void append_utf8(unsigned codepoint) {
        if (codepoint <= 0x7F) {
            _token += char(codepoint);
        } else if (codepoint <= 0x7FF) {
            _token += char(0xC0 | (codepoint >> 6));
            _token += char(0x80 | (codepoint & 0x3F));
        } else if (codepoint <= 0xFFFF) {
            _token += char(0xE0 | (codepoint >> 12));
            _token += char(0x80 | ((codepoint >> 6) & 0x3F));
            _token += char(0x80 | (codepoint & 0x3F));
        } else {
            _token += char(0xF0 | (codepoint >> 18));
            _token += char(0x80 | ((codepoint >> 12) & 0x3F));
            _token += char(0x80 | ((codepoint >> 6) & 0x3F));
            _token += char(0x80 | (codepoint & 0x3F));
        }
    }

    void emit_string(bool is_key, std::string_view s, bool in_input, bool no_escape, size_t token_offset) {
        check(is_key ? _h.key(s, in_input, no_escape) : _h.string(s, in_input, no_escape), token_offset);
    }

    // Parses a string, with _p just past its opening quote.
    void parse_string(bool is_key) {
        size_t token_offset = offset() - 1;
        const char* start = _p;
        const char* end = _in.end();
        const char* q = find_special(start, end);
        if (q != end && *q == '"') {
            _p = q + 1;
            emit_string(is_key, std::string_view(start, q - start), true, true, token_offset);
            return;
        }
        // The string has escapes, control characters or continues in the
        // next buffer: build it in _token.
        _token.assign(start, q);
        _p = q;
        bool no_escape = true;
        for (;;) {
            if (!more()) {
                fail(parse_error_code::string_miss_quotation_mark, offset());
            }
            q = find_special(_p, _in.end());
            _token.append(_p, q);
            _p = q;
            if (_p == _in.end()) {
                continue;
            }
            char c = *_p;
            if (c == '"') {
                ++_p;
                break;
            }
            if (c != '\\') {
                // RapidJSON reported a null byte as the end of the input.
                fail(c == '\0' ? parse_error_code::string_miss_quotation_mark : parse_error_code::string_invalid_encoding, offset());
            }
            size_t escape_offset = offset();
            ++_p;
            // Like RapidJSON, report the end of the input in an escape as
            // an error of the escape.
            auto take_in_escape = [&] (parse_error_code code) {
                if (!more()) {
                    fail(code, escape_offset);
                }
                return *_p++;
            };
            char e = take_in_escape(parse_error_code::string_escape_invalid);
            switch (e) {
            case '"': _token += '"'; no_escape = false; break;
            case '\\': _token += '\\'; no_escape = false; break;
            case '/': _token += '/'; break;
            case 'b': _token += '\b'; no_escape = false; break;
            case 'f': _token += '\f'; no_escape = false; break;
            case 'n': _token += '\n'; no_escape = false; break;
            case 'r': _token += '\r'; no_escape = false; break;
            case 't': _token += '\t'; no_escape = false; break;
            case 'u': {
                unsigned codepoint = parse_hex4(escape_offset);
                if (codepoint >= 0xD800 && codepoint <= 0xDFFF) {
                    if (codepoint > 0xDBFF
                            || take_in_escape(parse_error_code::string_unicode_surrogate_invalid) != '\\'
                            || take_in_escape(parse_error_code::string_unicode_surrogate_invalid) != 'u') {
                        fail(parse_error_code::string_unicode_surrogate_invalid, escape_offset);
                    }
                    unsigned codepoint2 = parse_hex4(escape_offset);
                    if (codepoint2 < 0xDC00 || codepoint2 > 0xDFFF) {
                        fail(parse_error_code::string_unicode_surrogate_invalid, escape_offset);
                    }
                    codepoint = (((codepoint - 0xD800) << 10) | (codepoint2 - 0xDC00)) + 0x10000;
                }
                if (codepoint < 0x20 || codepoint == '"' || codepoint == '\\') {
                    no_escape = false;
                }
                append_utf8(codepoint);
                break;
            }
            default:
                fail(parse_error_code::string_escape_invalid, escape_offset);
            }
        }
        emit_string(is_key, _token, false, no_escape, token_offset);
    }

    static bool is_number_char(char c) noexcept {
        return is_digit(c) || c == '-' || c == '+' || c == '.' || c == 'e' || c == 'E';
    }

    // Whether the 8 bytes of a little-endian word are all decimal digits.
    static bool is_eight_digits(uint64_t v) noexcept {
        return ((v & 0xF0F0F0F0F0F0F0F0) | (((v + 0x0606060606060606) & 0xF0F0F0F0F0F0F0F0) >> 4)) == 0x3333333333333333;
    }

    // The value of 8 decimal digits in a little-endian word, computed in
    // parallel (the method of the fast_float library).
    static uint32_t parse_eight_digits(uint64_t v) noexcept {
        v -= 0x3030303030303030;
        v = (v * 10) + (v >> 8);
        return uint32_t(((v & 0x000000FF000000FF) * 0x000F424000000064 + ((v >> 16) & 0x000000FF000000FF) * 0x0000271000000001) >> 32);
    }

    // A number scanned by scan_number().
    struct number_scan {
        // Where the number ends, or where it is invalid if error is set.
        const char* stop;
        std::optional<parse_error_code> error;
        // All its digits, unless they overflow.
        uint64_t mantissa = 0;
        // Its value is mantissa * 10^exp10.
        int64_t exp10 = 0;
        bool minus = false;
        bool is_double = false;
        bool overflow = false;
        bool too_big = false;
    };

    // Scans a number in [p, end), following RapidJSON's grammar. The number
    // ends at the first character which can't continue it, or at end. It
    // doesn't throw, so that a number cut by the end of a buffer can be
    // scanned again once it is whole.
    static number_scan scan_number(const char* p, const char* end) noexcept {
        number_scan r;
        auto digit = [&] { return p != end && is_digit(*p); };
        // Digits taken into the mantissa; up to 19 can't overflow it.
        unsigned taken = 0;
        auto take_digits = [&] {
            const char* first = p;
            while (taken + 8 <= 19 && end - p >= 8) {
                uint64_t eight;
                std::memcpy(&eight, p, 8);
                if (!is_eight_digits(eight)) {
                    break;
                }
                r.mantissa = r.mantissa * 100000000 + parse_eight_digits(eight);
                taken += 8;
                p += 8;
            }
            for (; digit(); ++p) {
                unsigned d = *p - '0';
                if (taken++ < 19) {
                    r.mantissa = r.mantissa * 10 + d;
                } else {
                    r.overflow |= __builtin_mul_overflow(r.mantissa, 10, &r.mantissa);
                    r.overflow |= __builtin_add_overflow(r.mantissa, d, &r.mantissa);
                }
            }
            return p - first;
        };
        auto invalid = [&] (parse_error_code code) {
            r.stop = p;
            r.error = code;
            return r;
        };
        r.minus = p != end && *p == '-';
        p += r.minus;
        if (!digit()) {
            return invalid(parse_error_code::value_invalid);
        }
        if (*p == '0') {
            ++p;
        } else {
            take_digits();
        }
        uint64_t fraction_digits = 0;
        if (p != end && *p == '.') {
            r.is_double = true;
            ++p;
            if (!digit()) {
                return invalid(parse_error_code::number_miss_fraction);
            }
            fraction_digits = take_digits();
        }
        int64_t exp = 0;
        if (p != end && (*p == 'e' || *p == 'E')) {
            r.is_double = true;
            ++p;
            bool exp_minus = false;
            if (p != end && (*p == '+' || *p == '-')) {
                exp_minus = *p == '-';
                ++p;
            }
            if (!digit()) {
                return invalid(parse_error_code::number_miss_exponent);
            }
            uint64_t e = 0;
            while (digit()) {
                e = std::min<uint64_t>(e * 10 + (*p++ - '0'), 1000000);
            }
            // RapidJSON refuses large exponents even if the mantissa is 0.
            r.too_big = !exp_minus && e > 308 + fraction_digits;
            exp = exp_minus ? -int64_t(e) : int64_t(e);
        }
        r.exp10 = exp - int64_t(fraction_digits);
        r.stop = p;
        return r;
    }

    // Powers of ten which are exact doubles.
    static constexpr double exact_powers_of_10[] = {
        1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11,
        1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18, 1e19, 1e20, 1e21, 1e22,
    };

    // Converts a scanned number to the double nearest to it.
    static std::optional<double> to_double(const number_scan& n, const char* begin) {
        // A mantissa and a power of ten which are both exact doubles give a
        // correctly rounded product or quotient.
        if (!n.overflow && n.mantissa <= (uint64_t(1) << 53) && n.exp10 >= -22 && n.exp10 <= 22) {
            double d = double(n.mantissa);
            d = n.exp10 < 0 ? d / exact_powers_of_10[-n.exp10] : d * exact_powers_of_10[n.exp10];
            return n.minus ? -d : d;
        }
        double d;
        if (!n.overflow) {
            // The mantissa holds all the digits.
            d = internal::fast_float::decimal_to_double(n.minus, n.mantissa, n.exp10);
            if (std::isinf(d)) {
                return std::nullopt;
            }
            return d;
        }
        auto [ptr, ec] = std::from_chars(begin, n.stop, d);
        if (ec == std::errc::result_out_of_range) {
            // An overflow, or an underflow which RapidJSON takes as 0 or
            // a subnormal number.
            d = std::strtod(std::string(begin, n.stop).c_str(), nullptr);
            if (std::isinf(d)) {
                return std::nullopt;
            }
        }
        return d;
    }

    // Parses a number, following RapidJSON's grammar and typing: integers
    // are reported with the narrowest type, and a number with a fraction or
    // an exponent is a double.
    void parse_number() {
        size_t token_offset = offset();
        const char* begin = _p;
        const char* end = _in.end();
        number_scan n = scan_number(begin, end);
        bool gathered = n.stop == end;
        if (gathered) {
            // The number may continue in the next buffer: gather it whole.
            _token.assign(begin, end);
            _p = end;
            while (more() && is_number_char(*_p)) {
                _token += *_p++;
            }
            begin = _token.data();
            n = scan_number(begin, begin + _token.size());
        } else {
            _p = n.stop;
        }
        if (n.error) {
            fail(*n.error, token_offset + (n.stop - begin));
        }
        if (n.too_big) {
            fail(parse_error_code::number_too_big, token_offset);
        }
        if (!n.is_double && !n.overflow && (!n.minus || n.mantissa <= uint64_t(std::numeric_limits<int64_t>::max()) + 1)) {
            check(n.minus ? _h.integer(int64_t(~n.mantissa + 1)) : _h.unsigned_integer(n.mantissa), token_offset);
        } else {
            auto d = to_double(n, begin);
            if (!d) {
                fail(parse_error_code::number_too_big, token_offset);
            }
            check(_h.floating(*d), token_offset);
        }
        after_value();
        // A gathered number holds only characters which can continue it.
        if (gathered ? n.stop != begin + _token.size() : is_number_char(*n.stop)) {
            // The number is followed by characters which can't follow it,
            // but could continue a number: report them like RapidJSON does.
            report_unexpected(token_offset + (n.stop - begin));
        }
    }

    // Fails for an unexpected character at the given offset, following the
    // value which was just parsed.
    [[noreturn]] void report_unexpected(size_t at) {
        switch (_state) {
        case state::comma_or_end_object: fail(parse_error_code::object_miss_comma_or_curly_bracket, at);
        case state::comma_or_end_array: fail(parse_error_code::array_miss_comma_or_square_bracket, at);
        default: fail(parse_error_code::root_not_singular, at);
        }
    }

    void parse_value(char c) {
        switch (c) {
        case '{': open(true); break;
        case '[': open(false); break;
        case '"': ++_p; parse_string(false); after_value(); break;
        case 't': parse_literal("true"); break;
        case 'f': parse_literal("false"); break;
        case 'n': parse_literal("null"); break;
        default:
            if (c == '-' || is_digit(c)) {
                parse_number();
            } else if (c == '\0') {
                // Like RapidJSON, take a NUL character for the end of input.
                fail_at_end();
            } else {
                fail(parse_error_code::value_invalid, offset());
            }
        }
    }

    [[noreturn]] void fail_at_end() {
        switch (_state) {
        case state::start: fail(parse_error_code::document_empty, offset());
        case state::value:
        case state::value_or_end_array: fail(parse_error_code::value_invalid, offset());
        case state::key_or_end_object:
        case state::key: fail(parse_error_code::object_miss_name, offset());
        case state::colon: fail(parse_error_code::object_miss_colon, offset());
        case state::comma_or_end_object: fail(parse_error_code::object_miss_comma_or_curly_bracket, offset());
        case state::comma_or_end_array: fail(parse_error_code::array_miss_comma_or_square_bracket, offset());
        case state::done: break;
        }
        std::abort();
    }

public:
    reader(input& in, Handler& h, size_t max_nested_level)
        : _in(in), _h(h), _max_nested_level(max_nested_level), _p(in.begin())
    {}

    // Parses the input, until its end or, if Preemptible, until the task
    // quota runs out. Returns true if the whole input was parsed.
    template<bool Preemptible>
    bool run() {
        unsigned tokens = 0;
        for (;;) {
            if (!skip_whitespace()) {
                if (_state != state::done) {
                    fail_at_end();
                }
                return true;
            }
            char c = *_p;
            switch (_state) {
            case state::start:
            case state::value:
                parse_value(c);
                break;
            case state::value_or_end_array:
                if (c == ']') {
                    close();
                } else {
                    parse_value(c);
                }
                break;
            case state::key_or_end_object:
                if (c == '}') {
                    close();
                    break;
                }
                [[fallthrough]];
            case state::key:
                if (c != '"') {
                    fail(parse_error_code::object_miss_name, offset());
                }
                ++_p;
                parse_string(true);
                _state = state::colon;
                // The colon and the value usually follow: parse them without
                // going around the loop. Anything else is left to the loop,
                // which reports errors the same way.
                if (skip_whitespace() && *_p == ':') {
                    ++_p;
                    _state = state::value;
                    if (skip_whitespace()) {
                        parse_value(*_p);
                    }
                }
                break;
            case state::colon:
                if (c != ':') {
                    fail(parse_error_code::object_miss_colon, offset());
                }
                ++_p;
                _state = state::value;
                break;
            case state::comma_or_end_object:
                if (c == ',') {
                    ++_p;
                    _state = state::key;
                } else if (c == '}') {
                    close();
                } else {
                    fail(parse_error_code::object_miss_comma_or_curly_bracket, offset());
                }
                break;
            case state::comma_or_end_array:
                if (c == ',') {
                    ++_p;
                    _state = state::value;
                    if (skip_whitespace()) {
                        parse_value(*_p);
                    }
                } else if (c == ']') {
                    close();
                } else {
                    fail(parse_error_code::array_miss_comma_or_square_bracket, offset());
                }
                break;
            case state::done:
                if (c == '\0') {
                    return true;
                }
                fail(parse_error_code::root_not_singular, offset());
            }
            if constexpr (Preemptible) {
                if ((++tokens & 63) == 0 && need_preempt()) {
                    return false;
                }
            }
        }
    }
};

} // anonymous namespace

// The values of the containers being parsed. The first values are kept in
// the stack itself, so parsing a small document needs no allocation, and the
// rest in segments of a fixed size, kept until the stack is destroyed. The
// slots are raw memory: only the first size() hold values, so slots need no
// initialization, and values moved out leave nothing to clean.
class value_stack {
    static constexpr uint32_t inline_capacity = 64;
    static constexpr uint32_t segment_shift = 10;
    static constexpr uint32_t segment_size = 1u << segment_shift;
    static constexpr uint32_t segment_mask = segment_size - 1;
    struct slot {
        alignas(value) std::byte bytes[sizeof(value)];
    };
    slot _inline[inline_capacity];
    std::vector<std::unique_ptr<slot[]>> _segments;
    uint32_t _size = 0;

    slot& at(uint32_t i) noexcept {
        if (i < inline_capacity) {
            return _inline[i];
        }
        i -= inline_capacity;
        return _segments[i >> segment_shift][i & segment_mask];
    }
public:
    value_stack() = default;
    value_stack(const value_stack&) = delete;
    ~value_stack() {
        for (uint32_t i = 0; i < _size; ++i) {
            (*this)[i].~value();
        }
    }
    uint32_t size() const noexcept { return _size; }
    value& operator[](uint32_t i) noexcept {
        return *std::launder(reinterpret_cast<value*>(at(i).bytes));
    }
    static constexpr size_t segment_bytes = segment_size * sizeof(value);
    // Returns true if the next emplace() allocates a segment.
    bool full() const noexcept {
        return _size == inline_capacity + _segments.size() * segment_size;
    }
    // Constructs the next value from args.
    template<typename... Args>
    value& emplace(Args&&... args) {
        if (full()) {
            _segments.push_back(std::make_unique_for_overwrite<slot[]>(segment_size));
        }
        value* v = std::construct_at(reinterpret_cast<value*>(at(_size).bytes), std::forward<Args>(args)...);
        ++_size;
        return *v;
    }
    // Moves the n values from index from to raw memory at dst; they must
    // then be dropped with truncate().
    void relocate_to(uint32_t from, uint32_t n, value* dst) noexcept {
        while (n) {
            slot* src = &at(from);
            uint32_t run = from < inline_capacity ? inline_capacity - from : segment_size - ((from - inline_capacity) & segment_mask);
            run = std::min(run, n);
            std::memcpy(static_cast<void*>(dst), static_cast<const void*>(src), run * sizeof(value));
            dst += run;
            from += run;
            n -= run;
        }
    }
    // Drops the values from n on, which must have been moved out.
    void truncate(uint32_t n) noexcept {
        _size = n;
    }
};

// Builds a value from the tokens of a reader.
class value_builder {
    input& _in;
    // The values of open containers; an object's are names and values.
    value_stack _stack;
    // Where each open container's values start in _stack.
    utils::small_vector<uint32_t, 16> _starts;

    // Builds a value in its slot: a temporary value, moved there, would
    // read back the bytes just written in pieces, which stalls the CPU.
    template<typename... Args>
    value& emplace(Args&&... args) {
        return _stack.emplace(std::forward<Args>(args)...);
    }
    template<typename... Args>
    bool add(Args&&... args) {
        emplace(std::forward<Args>(args)...);
        return true;
    }
    bool add_string(std::string_view s, bool in_input, bool no_escape) {
        if (in_input && s.size() >= min_chunk_string && s.size() <= std::numeric_limits<uint16_t>::max()) {
            // emplace() may throw, so take the reference after it.
            value& v = emplace();
            if (auto* h = _in.hold_current()) {
                set_chunk_string(v, h, s.data() - h->buf.get(), s.size());
                return true;
            }
            allocated(s.size());
            set_string(v, s, no_escape);
            return true;
        }
        set_string(emplace(), s, no_escape);
        return true;
    }
    // Replaces the n values of the container being closed with it.
    template<typename T>
    bool close(type t, size_type n) {
        internal::storage<T>* s = nullptr;
        if (n) {
            uint32_t start = _starts.back();
            s = internal::allocate_storage<T>(n);
            // A member is laid out as two values, its name and its value,
            // like an object's names and values are on the stack.
            static_assert(sizeof(T) % sizeof(value) == 0);
            constexpr uint32_t k = sizeof(T) / sizeof(value);
            if (!s->segmented()) {
                _stack.relocate_to(start, n * k, reinterpret_cast<value*>(s->elements()));
            } else {
                for (uint32_t done = 0, j = 0; done < n; ++j) {
                    uint32_t count = std::min(n - done, storage<T>::segment_size);
                    _stack.relocate_to(start + done * k, count * k, reinterpret_cast<value*>(s->segments()[j]));
                    done += count;
                }
            }
            s->size = n;
            _stack.truncate(start);
        }
        _starts.pop_back();
        // With n values gone, the slot exists, so this doesn't allocate or
        // throw, which would leak s.
        value& v = emplace(t);
        if (s) {
            v.set_container(s);
        }
        return true;
    }
public:
    explicit value_builder(input& in) : _in(in) {}

    bool null() { return add(); }
    bool boolean(bool b) { return add(b); }
    bool integer(int64_t i) { return add(i); }
    bool unsigned_integer(uint64_t u) { return add(u); }
    bool floating(double d) { return add(d); }
    bool string(std::string_view s, bool in_input, bool no_escape) { return add_string(s, in_input, no_escape); }
    bool key(std::string_view s, bool in_input, bool no_escape) { return add_string(s, in_input, no_escape); }
    bool start_object() { _starts.push_back(_stack.size()); return true; }
    bool start_array() { _starts.push_back(_stack.size()); return true; }
    bool end_object(size_type n) { return close<member>(kObjectType, n); }
    bool end_array(size_type n) { return close<value>(kArrayType, n); }

    value result() {
        return std::move(_stack[_stack.size() - 1]);
    }
};

namespace {

// Forwards tokens of a reader to a sax_handler.
class sax_forwarder {
    internal::sax_handler& _h;
public:
    explicit sax_forwarder(internal::sax_handler& h) : _h(h) {}
    bool null() { return _h.null(); }
    bool boolean(bool b) { return _h.boolean(b); }
    bool integer(int64_t i) { return _h.integer(i); }
    bool unsigned_integer(uint64_t u) { return _h.unsigned_integer(u); }
    bool floating(double d) { return _h.floating(d); }
    bool string(std::string_view s, bool, bool) { return _h.string(s); }
    bool key(std::string_view s, bool, bool) { return _h.key(s); }
    bool start_object() { return _h.start_object(); }
    bool start_array() { return _h.start_array(); }
    bool end_object(size_type n) { return _h.end_object(n); }
    bool end_array(size_type n) { return _h.end_array(n); }
};

value parse_input(input& in, size_t max_nested_level) {
    value_builder builder(in);
    reader<value_builder> r(in, builder, max_nested_level);
    r.run<false>();
    return builder.result();
}

// Formats a double like RapidJSON's Writer: the shortest representation
// which reads back to the same value, always with a fraction or an exponent.
// Returns the end of the written characters; buf must hold 32 characters.
char* format_double(double d, char* buf) {
    if (!std::isfinite(d)) {
        throw error(fmt::format("rjson: cannot print non-finite number {}", d));
    }
    char* p = buf;
    if (d == 0) {
        if (std::signbit(d)) {
            *p++ = '-';
        }
        std::memcpy(p, "0.0", 3);
        return p + 3;
    }
    if (d < 0) {
        *p++ = '-';
        d = -d;
    }
    // The shortest digits which read back as d, with their exponent:
    // d = digits * 10^k. fmt exports Dragonbox, which computes them
    // directly, without formatting them as text first.
    auto dec = fmt::detail::dragonbox::to_decimal(d);
    char digits[20];
    int length = std::to_chars(digits, digits + sizeof(digits), dec.significand).ptr - digits;
    int k = dec.exponent;
    // As in RapidJSON's Prettify().
    int kk = length + k;
    if (0 <= k && kk <= 21) {
        std::memcpy(p, digits, length);
        std::memset(p + length, '0', kk - length);
        p += kk;
        std::memcpy(p, ".0", 2);
        return p + 2;
    } else if (0 < kk && kk <= 21) {
        std::memcpy(p, digits, kk);
        p[kk] = '.';
        std::memcpy(p + kk + 1, digits + kk, length - kk);
        return p + length + 1;
    } else if (-6 < kk && kk <= 0) {
        int offset = 2 - kk;
        p[0] = '0';
        p[1] = '.';
        std::memset(p + 2, '0', offset - 2);
        std::memcpy(p + offset, digits, length);
        return p + offset + length;
    }
    *p++ = digits[0];
    if (length > 1) {
        *p++ = '.';
        std::memcpy(p, digits + 1, length - 1);
        p += length - 1;
    }
    *p++ = 'e';
    int x = kk - 1;
    if (x < 0) {
        *p++ = '-';
        x = -x;
    }
    return std::to_chars(p, p + 4, x).ptr;
}

// The characters with which RapidJSON escapes characters in strings.
constexpr char escape_char(unsigned char c) noexcept {
    switch (c) {
    case '"': return '"';
    case '\\': return '\\';
    case '\b': return 'b';
    case '\f': return 'f';
    case '\n': return 'n';
    case '\r': return 'r';
    case '\t': return 't';
    default: return 'u';
    }
}

// Collects printed text in a std::string.
class string_sink {
    std::string& _out;
public:
    explicit string_sink(std::string& out) : _out(out) {}
    void put(char c) { _out.push_back(c); }
    void write(std::string_view s) { _out.append(s); }
    bool full() const noexcept { return false; }
};

// Collects printed text in buffers of a fixed size, for an output_stream.
class buffers_sink {
    static constexpr size_t buffer_size = 16 * 1024;
    std::vector<temporary_buffer<char>> _full;
    temporary_buffer<char> _current;
    size_t _pos = 0;

    void next_buffer() {
        if (_pos) {
            _full.push_back(std::move(_current));
        }
        _current = temporary_buffer<char>(buffer_size);
        _pos = 0;
    }
public:
    void put(char c) {
        if (_pos == _current.size()) {
            next_buffer();
        }
        _current.get_write()[_pos++] = c;
    }
    void write(std::string_view s) {
        while (!s.empty()) {
            if (_pos == _current.size()) {
                next_buffer();
            }
            size_t n = std::min(s.size(), _current.size() - _pos);
            std::memcpy(_current.get_write() + _pos, s.data(), n);
            _pos += n;
            s.remove_prefix(n);
        }
    }
    bool full() const noexcept { return !_full.empty(); }
    std::vector<temporary_buffer<char>> take_full() { return std::exchange(_full, {}); }
    temporary_buffer<char> take_last() {
        _current.trim(_pos);
        _pos = 0;
        return std::move(_current);
    }
};

template<typename Sink>
void write_string(Sink& out, std::string_view s, bool escape) {
    out.put('"');
    if (!escape) {
        out.write(s);
    } else {
        const char* p = s.data();
        const char* end = p + s.size();
        while (p != end) {
            const char* q = find_special(p, end);
            out.write(std::string_view(p, q - p));
            if (q == end) {
                break;
            }
            unsigned char c = *q;
            char e = escape_char(c);
            out.put('\\');
            out.put(e);
            if (e == 'u') {
                static constexpr char hex[] = "0123456789ABCDEF";
                out.write("00");
                out.put(hex[c >> 4]);
                out.put(hex[c & 0xF]);
            }
            p = q + 1;
        }
    }
    out.put('"');
}

template<typename Sink>
void write_string(Sink& out, const value& v) {
    write_string(out, v.GetStringView(), needs_escaping(v));
}

template<typename Sink>
void write_uint(Sink& out, uint64_t u) {
    char buf[24];
    out.write(std::string_view(buf, std::to_chars(buf, buf + sizeof(buf), u).ptr));
}

template<typename Sink>
void write_int(Sink& out, int64_t i) {
    char buf[24];
    out.write(std::string_view(buf, std::to_chars(buf, buf + sizeof(buf), i).ptr));
}

template<typename Sink>
void write_double(Sink& out, double d) {
    char buf[32];
    out.write(std::string_view(buf, format_double(d, buf)));
}

} // anonymous namespace


// Prints a value as JSON text. It keeps its state in an explicit stack
// rather than recursing, so run() can stop between tokens and continue in
// the next call.
class printer {
    struct frame {
        const value* container;
        uint32_t index;
    };
    std::vector<frame> _stack;
    const value* _root;
    size_t _max_nested_level;

    void push(frame f) {
        if (_stack.size() + 1 > _max_nested_level) {
            throw_nested_level_error(_max_nested_level);
        }
        _stack.push_back(f);
    }

    template<typename Sink>
    [[gnu::always_inline]] void write_value(Sink& out, const value& v) {
        switch (v._kind) {
        case value::kind::null: out.write("null"); break;
        case value::kind::false_: out.write("false"); break;
        case value::kind::true_: out.write("true"); break;
        case value::kind::uint: write_uint(out, v.as_uint64()); break;
        case value::kind::int_: write_int(out, v.as_int64()); break;
        case value::kind::double_: write_double(out, v.load<double>(0)); break;
        case value::kind::inline_string:
        case value::kind::static_string:
        case value::kind::owned_string:
        case value::kind::chunk_string:
            write_string(out, v);
            break;
        case value::kind::object:
            out.put('{');
            push({&v, 0});
            break;
        case value::kind::array:
            out.put('[');
            push({&v, 0});
            break;
        }
    }
public:
    printer(const value& root, size_t max_nested_level)
        : _root(&root), _max_nested_level(max_nested_level)
    {}

    // Prints until done, or until the sink has a full buffer or, if
    // Preemptible, the task quota runs out. Returns true when done.
    template<bool Preemptible, typename Sink>
    bool run(Sink& out) {
        if (_root) {
            write_value(out, *std::exchange(_root, nullptr));
        }
        unsigned tokens = 0;
        // Returns true if printing should stop for now.
        auto pause = [&] {
            if (out.full()) {
                return true;
            }
            if constexpr (Preemptible) {
                if ((++tokens & 63) == 0 && need_preempt()) {
                    return true;
                }
            }
            return false;
        };
        while (!_stack.empty()) {
            // Prints the elements of the innermost container until one is a
            // container, which write_value() pushes, invalidating f.
            frame& f = _stack.back();
            const value& c = *f.container;
            if (c._kind == value::kind::object) {
                auto* s = c.container<member>();
                uint32_t n = s ? s->size : 0;
                if (f.index == n) {
                    out.put('}');
                    _stack.pop_back();
                    if (pause()) {
                        return _stack.empty();
                    }
                    continue;
                }
                for (;;) {
                    const member& m = (*s)[f.index];
                    if (f.index++) {
                        out.put(',');
                    }
                    write_string(out, m.name);
                    out.put(':');
                    bool nested = m.value.IsObject() || m.value.IsArray();
                    write_value(out, m.value);
                    if (nested || f.index == n) {
                        break;
                    }
                    if (pause()) {
                        return false;
                    }
                }
            } else {
                auto* s = c.container<value>();
                uint32_t n = s ? s->size : 0;
                if (f.index == n) {
                    out.put(']');
                    _stack.pop_back();
                    if (pause()) {
                        return _stack.empty();
                    }
                    continue;
                }
                for (;;) {
                    const value& e = (*s)[f.index];
                    if (f.index++) {
                        out.put(',');
                    }
                    bool nested = e.IsObject() || e.IsArray();
                    write_value(out, e);
                    if (nested || f.index == n) {
                        break;
                    }
                    if (pause()) {
                        return false;
                    }
                }
            }
            if (pause()) {
                return _stack.empty();
            }
        }
        return true;
    }
};

std::string print(const rjson::value& value, size_t max_nested_level) {
    std::string out;
    string_sink sink(out);
    printer(value, max_nested_level).run<false>(sink);
    return out;
}

static future<> print_to_stream(printer p, seastar::output_stream<char>& os) {
    buffers_sink sink;
    for (;;) {
        bool done = p.run<true>(sink);
        for (auto& buf : sink.take_full()) {
            co_await os.write(std::move(buf));
        }
        if (done) {
            break;
        }
        co_await coroutine::maybe_yield();
    }
    auto last = sink.take_last();
    if (!last.empty()) {
        co_await os.write(std::move(last));
    }
}

future<> print(const rjson::value& value, seastar::output_stream<char>& os, size_t max_nested_level) {
    return print_to_stream(printer(value, max_nested_level), os);
}

rjson::malformed_value::malformed_value(std::string_view name, const rjson::value& value)
    : malformed_value(name, print(value))
{}

rjson::malformed_value::malformed_value(std::string_view name, std::string_view value)
    : error(seastar::format("Malformed value {} : {}", name, value))
{}

rjson::missing_value::missing_value(std::string_view name) 
    // TODO: using old message here, but as pointed out. 
    // "parameter" is not really a JSON concept. It is a value
    // missing according to (implicit) schema. 
    : error(seastar::format("JSON parameter {} not found", name))
{}

rjson::parse_error::parse_error(std::string_view msg, size_t offset)
    : error(seastar::format("Parsing JSON failed: {} at {}", msg, offset))
    , _offset(offset)
{}

namespace internal {

template<typename T>
void copy_container(value& dst, const value& src);

// Copies src into dst, which holds no value, building the copy in place.
void copy_into(value& dst, const value& src) {
    switch (src._kind) {
    case value::kind::owned_string:
        set_string(dst, src.GetStringView(), !needs_escaping(src));
        break;
    case value::kind::chunk_string:
        std::memcpy(static_cast<void*>(&dst), static_cast<const void*>(&src), sizeof(value));
        retain(src.load<chunk_holder*>(0));
        break;
    case value::kind::object:
        copy_container<member>(dst, src);
        break;
    case value::kind::array:
        copy_container<value>(dst, src);
        break;
    default:
        std::memcpy(static_cast<void*>(&dst), static_cast<const void*>(&src), sizeof(value));
        break;
    }
}

// Copies the elements bitwise, then replaces the ones which own memory,
// like long strings and containers, by copies of their own. Most elements
// own nothing, so this is mostly a copy of memory.
template<typename T>
void copy_container(value& dst, const value& src) {
    constexpr uint32_t k = sizeof(T) / sizeof(value);
    std::construct_at(&dst, src._kind == value::kind::object ? kObjectType : kArrayType);
    auto* from = src.container<T>();
    uint32_t n = from ? from->size : 0;
    if (!n) {
        return;
    }
    auto* s = allocate_storage<T>(n);
    for (uint32_t i = 0; i < n; ++i) {
        relocate((*s)[i], (*from)[i]);
    }
    s->size = n;
    dst.set_container(s);
    uint32_t i = 0;
    try {
        for (; i < n * k; ++i) {
            value& d = reinterpret_cast<value*>(&(*s)[i / k])[i % k];
            if (d.needs_cleanup()) {
                d._kind = value::kind::null;
                copy_into(d, reinterpret_cast<const value*>(&(*from)[i / k])[i % k]);
            }
        }
    } catch (...) {
        // The values not copied yet share memory with src: drop them, so
        // that destroying dst doesn't free it. The value whose copy failed
        // was left valid, and may hold memory of its own.
        for (++i; i < n * k; ++i) {
            reinterpret_cast<value*>(&(*s)[i / k])[i % k]._kind = value::kind::null;
        }
        throw;
    }
}

} // namespace internal

rjson::value copy(const rjson::value& v) {
    value ret;
    internal::copy_into(ret, v);
    return ret;
}

rjson::value parse(std::string_view str, size_t max_nested_level) {
    contiguous_input in(str);
    return parse_input(in, max_nested_level);
}

rjson::value parse(chunked_content&& content, size_t max_nested_level) {
    chunked_input in(std::move(content));
    return parse_input(in, max_nested_level);
}

std::optional<rjson::value> try_parse(std::string_view str, size_t max_nested_level) {
    try {
        return parse(str, max_nested_level);
    } catch (const rjson::error&) {
        return std::nullopt;
    }
}

future<rjson::value> parse_gently(chunked_content&& content, size_t max_nested_level) {
    chunked_input in(std::move(content));
    value_builder builder(in);
    reader<value_builder> r(in, builder, max_nested_level);
    while (!r.run<true>()) {
        co_await coroutine::maybe_yield();
    }
    co_return builder.result();
}

void internal::parse_sax(chunk_source& source, sax_handler& handler, size_t max_nested_level) {
    source_input in(source);
    sax_forwarder forwarder(handler);
    reader<sax_forwarder> r(in, forwarder, max_nested_level);
    r.run<false>();
}

rjson::value& get(rjson::value& value, std::string_view name) {
    auto member_it = value.FindMember(name);
    if (member_it != value.MemberEnd()) {
        return member_it->value;
    }
    throw missing_value(name);
}

const rjson::value& get(const rjson::value& value, std::string_view name) {
    auto member_it = value.FindMember(name);
    if (member_it != value.MemberEnd()) {
        return member_it->value;
    }
    throw missing_value(name);
}

rjson::value from_string(const char* str, size_t size) {
    return make_string(std::string_view(str, size), false);
}

rjson::value from_string(std::string_view view) {
    return make_string(view, false);
}

const rjson::value* find(const rjson::value& value, std::string_view name) {
    auto member_it = value.FindMember(name);
    return member_it != value.MemberEnd() ? &member_it->value : nullptr;
}

rjson::value* find(rjson::value& value, std::string_view name) {
    auto member_it = value.FindMember(name);
    return member_it != value.MemberEnd() ? &member_it->value : nullptr;
}

bool remove_member(rjson::value& value, std::string_view name) {
    return value.RemoveMember(name);
}

void add_with_string_name(rjson::value& base, std::string_view name, rjson::value&& member) {
    base.AddMember(from_string(name), std::move(member));
}

void add_with_string_name(rjson::value& base, std::string_view name, rjson::string_ref_type member) {
    base.AddMember(from_string(name), rjson::value(member));
}

void add(rjson::value& base, rjson::string_ref_type name, rjson::value&& member) {
    base.AddMember(rjson::value(name), std::move(member));
}

void add(rjson::value& base, rjson::string_ref_type name, rjson::string_ref_type member) {
    base.AddMember(rjson::value(name), rjson::value(member));
}

void replace_with_string_name(rjson::value& base, const std::string_view name, rjson::value&& member) {
    rjson::value *m = rjson::find(base, name);
    if (m) {
        *m = std::move(member);
    } else {
        add_with_string_name(base, name, std::move(member));
    }
}

void push_back(rjson::value& base_array, rjson::value&& item) {
    base_array.PushBack(std::move(item));
}

bool single_value_comp::operator()(const rjson::value& r1, const rjson::value& r2) const {
   auto r1_type = r1.GetType();
   auto r2_type = r2.GetType();

   // null is the smallest type and compares with every other type, nothing is lesser than null
   if (r1_type == rjson::type::kNullType || r2_type == rjson::type::kNullType) {
       return r1_type < r2_type;
   }
   // only null, true, and false are comparable with each other, other types are not compatible
   if (r1_type != r2_type) {
       if (r1_type > rjson::type::kTrueType || r2_type > rjson::type::kTrueType) {
           throw rjson::error(format("Types are not comparable: {} {}", r1, r2));
       }
   }

   switch (r1_type) {
   case rjson::type::kNullType:
       // fall-through
   case rjson::type::kFalseType:
       // fall-through
   case rjson::type::kTrueType:
       return r1_type < r2_type;
   case rjson::type::kObjectType:
       throw rjson::error("Object type comparison is not supported");
   case rjson::type::kArrayType:
       throw rjson::error("Array type comparison is not supported");
   case rjson::type::kStringType:
       return r1.GetStringView() < r2.GetStringView();
   case rjson::type::kNumberType: {
       if (r1.IsInt() && r2.IsInt()) {
           return r1.GetInt() < r2.GetInt();
       } else if (r1.IsUint() && r2.IsUint()) {
           return r1.GetUint() < r2.GetUint();
       } else if (r1.IsInt64() && r2.IsInt64()) {
           return r1.GetInt64() < r2.GetInt64();
       } else if (r1.IsUint64() && r2.IsUint64()) {
           return r1.GetUint64() < r2.GetUint64();
       } else {
           // it's safe to call GetDouble() on any number type
           return r1.GetDouble() < r2.GetDouble();
       }
   }
   default:
       return false;
   }
}

rjson::value from_string_map(const std::map<sstring, sstring>& map) {
    rjson::value v = rjson::empty_object();
    for (auto& entry : map) {
        rjson::add_with_string_name(v, std::string_view(entry.first), rjson::from_string(entry.second));
    }
    return v;
}

static inline bool is_control_char(char c) {
    return c >= 0 && c <= 0x1F;
}

static inline bool needs_escaping(const sstring& s) {
    return std::any_of(s.begin(), s.end(), [](char c) {return is_control_char(c) || c == '"' || c == '\\';});
}


sstring quote_json_string(const sstring& value) {
    if (!needs_escaping(value)) {
        return format("\"{}\"", value);
    }
    std::ostringstream oss;
    oss << std::hex << std::uppercase << std::setfill('0');
    oss.put('"');
    for (char c : value) {
        switch (c) {
        case '"':
            oss.put('\\').put('"');
            break;
        case '\\':
            oss.put('\\').put('\\');
            break;
        case '\b':
            oss.put('\\').put('b');
            break;
        case '\f':
            oss.put('\\').put('f');
            break;
        case '\n':
            oss.put('\\').put('n');
            break;
        case '\r':
            oss.put('\\').put('r');
            break;
        case '\t':
            oss.put('\\').put('t');
            break;
        default:
            if (is_control_char(c)) {
                oss.put('\\').put('u') << std::setw(4) << static_cast<int>(c);
            } else {
                oss.put(c);
            }
            break;
        }
    }
    oss.put('"');
    return oss.str();
}

static future<> destroy_gently_nonleaf(rjson::value&& value_in) {
    // We want the caller to move the value into this function, so 'value_in' is an rvalue reference.
    // We want to hold the value while it's being destroyed, so we move it into the coroutine frame
    // as the local 'value'.
    auto value = std::move(value_in);

    // Destroy the elements from the last, so no element needs to be moved.
    if (value.IsObject()) {
        while (!value.ObjectEmpty()) {
            auto last = value.MemberEnd() - 1;
            co_await destroy_gently(std::move(last->value));
            value.EraseMember(last);
        }
    } else if (value.IsArray()) {
        while (!value.Empty()) {
            co_await destroy_gently(std::move(value[value.Size() - 1]));
            value.PopBack();
        }
    }
}

future<> destroy_gently(rjson::value&& value) {
    // Most nodes will be leaves, so we use a non-coroutine destroy_gently() for them. The
    // few non-leaves will be handled by the coroutine destroy_gently_nonleaf(). We could have
    // coded the whole thing as a non-coroutine, but that's more difficult and not worth the
    // marginal improvement.
    if (rjson::is_leaf(value)) {
        return make_ready_future<>();
    } else {
        return destroy_gently_nonleaf(std::move(value));
    }
}

void streaming_writer::prefix(bool is_key) {
    if (_levels.empty()) {
        if (_has_root) {
            throw error("rjson::streaming_writer: a JSON document has a single root value");
        }
        _has_root = true;
        return;
    }
    auto& l = _levels.back();
    if (l.object && is_key != (l.count % 2 == 0)) {
        throw error(is_key ? "rjson::streaming_writer: a key where a value is expected" : "rjson::streaming_writer: a value where a key is expected");
    }
    if (!l.object && is_key) {
        throw error("rjson::streaming_writer: a key in an array");
    }
    if (l.count) {
        _os.put(l.object && l.count % 2 ? ':' : ',');
    }
    ++l.count;
}

bool streaming_writer::Null() {
    prefix(false);
    write("null");
    return true;
}

bool streaming_writer::Bool(bool b) {
    prefix(false);
    write(b ? "true" : "false");
    return true;
}

bool streaming_writer::Int64(int64_t i) {
    prefix(false);
    char buf[24];
    write(std::string_view(buf, std::to_chars(buf, buf + sizeof(buf), i).ptr));
    return true;
}

bool streaming_writer::Uint64(uint64_t i) {
    prefix(false);
    char buf[24];
    write(std::string_view(buf, std::to_chars(buf, buf + sizeof(buf), i).ptr));
    return true;
}

bool streaming_writer::Double(double d) {
    if (!std::isfinite(d)) {
        // JSON has no NaN or infinity.
        return Null();
    }
    prefix(false);
    char buf[32];
    write(std::string_view(buf, format_double(d, buf)));
    return true;
}

bool streaming_writer::RawNumber(std::string_view str) {
    prefix(false);
    write(str);
    return true;
}

bool streaming_writer::RawValue(std::string_view json, type) {
    prefix(false);
    write(json);
    return true;
}

namespace {
// Adapts an std::ostream to write_string().
class ostream_sink {
    std::ostream& _os;
public:
    explicit ostream_sink(std::ostream& os) : _os(os) {}
    void put(char c) { _os.put(c); }
    void write(std::string_view s) { _os.write(s.data(), s.size()); }
};
}

bool streaming_writer::String(std::string_view str) {
    prefix(false);
    ostream_sink sink(_os);
    write_string(sink, str, true);
    return true;
}

bool streaming_writer::Key(std::string_view str) {
    prefix(true);
    ostream_sink sink(_os);
    write_string(sink, str, true);
    return true;
}

bool streaming_writer::StartObject() {
    prefix(false);
    _levels.push_back({true, 0});
    _os.put('{');
    return true;
}

bool streaming_writer::EndObject(size_type) {
    if (_levels.empty() || !_levels.back().object || _levels.back().count % 2) {
        throw error("rjson::streaming_writer: EndObject() not ending an object");
    }
    _levels.pop_back();
    _os.put('}');
    return true;
}

bool streaming_writer::StartArray() {
    prefix(false);
    _levels.push_back({false, 0});
    _os.put('[');
    return true;
}

bool streaming_writer::EndArray(size_type) {
    if (_levels.empty() || _levels.back().object) {
        throw error("rjson::streaming_writer: EndArray() not ending an array");
    }
    _levels.pop_back();
    _os.put(']');
    return true;
}

} // end namespace rjson
