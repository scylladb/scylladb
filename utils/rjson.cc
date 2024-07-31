/*
 * Copyright 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "rjson.hh"
#include <seastar/core/print.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/iostream.hh>
#ifdef SANITIZE
#include <seastar/core/memory.hh>
#endif

#include <rapidjson/stream.h>

namespace rjson {

allocator the_allocator;

// chunked_content_stream is a wrapper of a chunked_content which
// presents the Stream concept that the rapidjson library expects as input
// for its parser (https://rapidjson.org/classrapidjson_1_1_stream.html).
// This wrapper owns the chunked_content, so it can free each chunk as
// soon as it's parsed.
class chunked_content_stream {
private:
    chunked_content _content;
    chunked_content::iterator _current_chunk;
    // _count only needed for Tell(). 32 bits is enough, we don't allow
    // more than 16 MB requests anyway.
    unsigned _count;
public:
    typedef char Ch;
    chunked_content_stream(chunked_content&& content)
        : _content(std::move(content))
        , _current_chunk(_content.begin())
    {}
    bool eof() const {
        return _current_chunk == _content.end();
    }
    // Methods needed by rapidjson's Stream concept (see
    // https://rapidjson.org/classrapidjson_1_1_stream.html):
    char Peek() const {
        if (eof()) {
            // Rapidjson's Stream concept does not have the explicit notion of
            // an "end of file". Instead, reading after the end of stream will
            // return a null byte. This makes these streams appear like null-
            // terminated C strings. It is good enough for reading JSON, which
            // anyway can't include bare null characters.
            return '\0';
        } else {
            return *_current_chunk->begin();
        }
    }
    char Take() {
        if (eof()) {
            return '\0';
        } else {
            char ret = *_current_chunk->begin();
            _current_chunk->trim_front(1);
            ++_count;
            if (_current_chunk->empty()) {
                *_current_chunk = temporary_buffer<char>();
                ++_current_chunk;
            }
            return ret;
        }
    }
    size_t Tell() const {
        return _count;
    }
    // Not used in input streams, but unfortunately we still need to implement
    Ch* PutBegin() { RAPIDJSON_ASSERT(false); return 0; }
    void Put(Ch) { RAPIDJSON_ASSERT(false); }
    void Flush() { RAPIDJSON_ASSERT(false); }
    size_t PutEnd(Ch*) { RAPIDJSON_ASSERT(false); return 0; }

};

/*
 * This wrapper class adds nested level checks to rapidjson's handlers.
 * Each rapidjson handler implements functions for accepting JSON values,
 * which includes strings, numbers, objects, arrays, etc.
 * Parsing objects and arrays needs to be performed carefully with regard
 * to stack overflow - each object/array layer adds another stack frame
 * to parsing, printing and destroying the parent JSON document.
 * To prevent stack overflow, a rapidjson handler can be wrapped with
 * guarded_json_handler, which accepts an additional max_nested_level parameter.
 * After trying to exceed the max nested level, a proper rjson::error will be thrown.
 */
template<typename Handler, bool EnableYield, typename Buffer = string_buffer>
struct guarded_yieldable_json_handler : public Handler {
    size_t _nested_level = 0;
    size_t _max_nested_level;
public:
    using handler_base = Handler;

    explicit guarded_yieldable_json_handler(size_t max_nested_level) : _max_nested_level(max_nested_level) {}
    guarded_yieldable_json_handler(Buffer& buf, size_t max_nested_level)
            : handler_base(buf), _max_nested_level(max_nested_level) {}

    // Parse any stream fitting https://rapidjson.org/classrapidjson_1_1_stream.html
    template<typename Stream>
    void Parse(Stream& stream) {
        rapidjson::GenericReader<encoding, encoding, allocator> reader(&the_allocator);
        reader.Parse(stream, *this);
        if (reader.HasParseError()) {
            throw rjson::error(
                format("Parsing JSON failed: {} at {}",
                    rapidjson::GetParseError_En(reader.GetParseErrorCode()), reader.GetErrorOffset()));
        }
        //NOTICE: The handler has parsed the string, but in case of rapidjson::GenericDocument
        // the data now resides in an internal stack_ variable, which is private instead of
        // protected... which means we cannot simply access its data. Fortunately, another
        // function for populating documents from SAX events can be abused to extract the data
        // from the stack via gadget-oriented programming - we use an empty event generator
        // which does nothing, and use it to call Populate(), which assumes that the generator
        // will fill the stack with something. It won't, but our stack is already filled with
        // data we want to steal, so once Populate() ends, our document will be properly parsed.
        // A proper solution could be programmed once rapidjson declares this stack_ variable
        // as protected instead of private, so that this class can access it.
        auto dummy_generator = [](handler_base&){return true;};
        handler_base::Populate(dummy_generator);
    }
    void Parse(const char* str, size_t length) {
        rapidjson::MemoryStream ms(static_cast<const char*>(str), length * sizeof(typename encoding::Ch));
        rapidjson::EncodedInputStream<encoding, rapidjson::MemoryStream> is(ms);
        Parse(is);
    }

    void Parse(chunked_content&& content) {
        // Note that content was moved into this function. The intention is
        // that we free every chunk we are done with.
        chunked_content_stream is(std::move(content));
        Parse(is);
    }

    bool StartObject() {
        ++_nested_level;
        check_nested_level();
        maybe_yield();
        return handler_base::StartObject();
    }

    bool EndObject(rapidjson::SizeType elements_count = 0) {
        --_nested_level;
        return handler_base::EndObject(elements_count);
    }

    bool StartArray() {
        ++_nested_level;
        check_nested_level();
        maybe_yield();
        return handler_base::StartArray();
    }

    bool EndArray(rapidjson::SizeType elements_count = 0) {
        --_nested_level;
        return handler_base::EndArray(elements_count);
    }

    bool Null()                 { maybe_yield(); return handler_base::Null(); }
    bool Bool(bool b)           { maybe_yield(); return handler_base::Bool(b); }
    bool Int(int i)             { maybe_yield(); return handler_base::Int(i); }
    bool Uint(unsigned u)       { maybe_yield(); return handler_base::Uint(u); }
    bool Int64(int64_t i64)     { maybe_yield(); return handler_base::Int64(i64); }
    bool Uint64(uint64_t u64)   { maybe_yield(); return handler_base::Uint64(u64); }
    bool Double(double d)       { maybe_yield(); return handler_base::Double(d); }
    bool String(const value::Ch* str, size_t length, bool copy = false) { maybe_yield(); return handler_base::String(str, length, copy); }
    bool Key(const value::Ch* str, size_t length, bool copy = false) { maybe_yield(); return handler_base::Key(str, length, copy); }


protected:
    static void maybe_yield() {
        if constexpr (EnableYield) {
            thread::maybe_yield();
        }
    }

    void check_nested_level() const {
        if (RAPIDJSON_UNLIKELY(_nested_level > _max_nested_level)) {
            throw rjson::error(format("Max nested level reached: {}", _max_nested_level));
        }
    }
};

void* internal::throwing_allocator::Malloc(size_t size) {
    // For bypassing the address sanitizer failure in debug mode - allocating
    // too much memory results in an abort
    #ifdef SANITIZE
    if (size > memory::stats().total_memory()) {
        throw rjson::error(format("Failed to allocate {} bytes", size));
    }
    #endif
    void* ret = base::Malloc(size);
    if (size > 0 && !ret) {
        throw rjson::error(format("Failed to allocate {} bytes", size));
    }
    return ret;
}

void* internal::throwing_allocator::Realloc(void* orig_ptr, size_t orig_size, size_t new_size) {
    // For bypassing the address sanitizer failure in debug mode - allocating
    // too much memory results in an abort
    #ifdef SANITIZE
    if (new_size > memory::stats().total_memory()) {
        throw rjson::error(format("Failed to allocate {} bytes", new_size));
    }
    #endif
    void* ret = base::Realloc(orig_ptr, orig_size, new_size);
    if (new_size > 0 && !ret) {
        throw rjson::error(format("Failed to reallocate {} bytes to {} bytes from {}", orig_size, new_size, orig_ptr));
    }
    return ret;
}

void internal::throwing_allocator::Free(void* ptr) {
    base::Free(ptr);
}

std::string print(const rjson::value& value, size_t max_nested_level) {
    string_buffer buffer;
    guarded_yieldable_json_handler<writer, false> writer(buffer, max_nested_level);
    value.Accept(writer);
    return std::string(buffer.GetString());
}

// This class implements RapidJSON Handler and batches Put() calls into output_stream writes.
class output_stream_buffer {
    static constexpr size_t _buf_size = 512;
    seastar::output_stream<char>& _os;
    temporary_buffer<char> _buf = temporary_buffer<char>(_buf_size);
    size_t _pos = 0;

    future<> send(temporary_buffer<char> b) {
        co_return co_await _os.write(b.get(), b.size());
    }
public:
    output_stream_buffer(seastar::output_stream<char>& os) : _os(os) {}
    using Ch = char; // Used by rjson internally

    void Flush() {
        if (_pos == 0) {
            return;
        }
        if (_pos < _buf_size) {
            _buf.trim(_pos);  // Last flush may be shorter
        }
        send(std::move(_buf)).get();
        _pos = 0;
        _buf = temporary_buffer<char>(_buf_size);
    }

    void Put(Ch c) {
        if (_pos == _buf_size) {
            Flush();
        }
        // Note: Should consider writing directly to the buffer in output_stream
        // instead of double buffering. But output_stream for a single char has higher
        // overhead than the above check + once we hit a non-completed future, we'd have
        // to revert to this method anyway...
        *(_buf.get_write() + _pos) = c;
        ++_pos;
    }
};

future<> print(const rjson::value& value, seastar::output_stream<char>& os, size_t max_nested_level) {
  // Use a thread so that we can yield while printing the JSON. This is only called for large values.
  return async([&value, &os, max_nested_level] {
    output_stream_buffer buf{ os };
    using streamer = rapidjson::Writer<output_stream_buffer, encoding, encoding, allocator>;
    guarded_yieldable_json_handler<streamer, true, output_stream_buffer> writer(buf, max_nested_level);
    value.Accept(writer);
    buf.Flush();
  });
}

rjson::malformed_value::malformed_value(std::string_view name, const rjson::value& value)
    : malformed_value(name, print(value))
{}

rjson::malformed_value::malformed_value(std::string_view name, std::string_view value)
    : error(format("Malformed value {} : {}", name, value))
{}

rjson::missing_value::missing_value(std::string_view name) 
    // TODO: using old message here, but as pointed out. 
    // "parameter" is not really a JSON concept. It is a value
    // missing according to (implicit) schema. 
    : error(format("JSON parameter {} not found", name))
{}

rjson::value copy(const rjson::value& value) {
    return rjson::value(value, the_allocator);
}

rjson::value parse(std::string_view str, size_t max_nested_level) {
    guarded_yieldable_json_handler<document, false> d(max_nested_level);
    d.Parse(str.data(), str.size());
    if (d.HasParseError()) {
        throw rjson::error(format("Parsing JSON failed: {} at {}",
            GetParseError_En(d.GetParseError()), d.GetErrorOffset()));
    }
    rjson::value& v = d;
    return std::move(v);
}

rjson::value parse(chunked_content&& content, size_t max_nested_level) {
    guarded_yieldable_json_handler<document, false> d(max_nested_level);
    d.Parse(std::move(content));
    if (d.HasParseError()) {
        throw rjson::error(format("Parsing JSON failed: {} at {}",
            GetParseError_En(d.GetParseError()), d.GetErrorOffset()));
    }
    rjson::value& v = d;
    return std::move(v);
}

std::optional<rjson::value> try_parse(std::string_view str, size_t max_nested_level) {
    guarded_yieldable_json_handler<document, false> d(max_nested_level);
    try {
        d.Parse(str.data(), str.size());
    } catch (const rjson::error&) {
        return std::nullopt;
    }
    if (d.HasParseError()) {
        return std::nullopt;    
    }
    rjson::value& v = d;
    return std::move(v);
}

rjson::value parse_yieldable(std::string_view str, size_t max_nested_level) {
    guarded_yieldable_json_handler<document, true> d(max_nested_level);
    d.Parse(str.data(), str.size());
    if (d.HasParseError()) {
        throw rjson::error(format("Parsing JSON failed: {} at {}",
            GetParseError_En(d.GetParseError()), d.GetErrorOffset()));
    }
    rjson::value& v = d;
    return std::move(v);
}

rjson::value parse_yieldable(chunked_content&& content, size_t max_nested_level) {
    guarded_yieldable_json_handler<document, true> d(max_nested_level);
    d.Parse(std::move(content));
    if (d.HasParseError()) {
        throw rjson::error(format("Parsing JSON failed: {} at {}",
            GetParseError_En(d.GetParseError()), d.GetErrorOffset()));
    }
    rjson::value& v = d;
    return std::move(v);
}

rjson::value& get(rjson::value& value, std::string_view name) {
    // Although FindMember() has a variant taking a StringRef, it ignores the
    // given length (see https://github.com/Tencent/rapidjson/issues/1649).
    // Luckily, the variant taking a GenericValue doesn't share this bug,
    // and we can create a string GenericValue without copying the string.
    auto member_it = value.FindMember(rjson::value(name.data(), name.size()));
    if (member_it != value.MemberEnd()) {
        return member_it->value;
    }
    throw missing_value(name);
}

const rjson::value& get(const rjson::value& value, std::string_view name) {
    auto member_it = value.FindMember(rjson::value(name.data(), name.size()));
    if (member_it != value.MemberEnd()) {
        return member_it->value;
    }
    throw missing_value(name);
}

rjson::value from_string(const std::string& str) {
    return rjson::value(str.c_str(), str.size(), the_allocator);
}

rjson::value from_string(const sstring& str) {
    return rjson::value(str.c_str(), str.size(), the_allocator);
}

rjson::value from_string(const char* str, size_t size) {
    return rjson::value(str, size, the_allocator);
}

rjson::value from_string(std::string_view view) {
    return rjson::value(view.data(), view.size(), the_allocator);
}

const rjson::value* find(const rjson::value& value, std::string_view name) {
    // Although FindMember() has a variant taking a StringRef, it ignores the
    // given length (see https://github.com/Tencent/rapidjson/issues/1649).
    // Luckily, the variant taking a GenericValue doesn't share this bug,
    // and we can create a string GenericValue without copying the string.
    auto member_it = value.FindMember(rjson::value(name.data(), name.size()));
    return member_it != value.MemberEnd() ? &member_it->value : nullptr;
}

rjson::value* find(rjson::value& value, std::string_view name) {
    auto member_it = value.FindMember(rjson::value(name.data(), name.size()));
    return member_it != value.MemberEnd() ? &member_it->value : nullptr;
}

bool remove_member(rjson::value& value, std::string_view name) {
    // Although RemoveMember() has a variant taking a StringRef, it ignores
    // given length (see https://github.com/Tencent/rapidjson/issues/1649).
    // Luckily, the variant taking a GenericValue doesn't share this bug,
    // and we can create a string GenericValue without copying the string.
    return value.RemoveMember(rjson::value(name.data(), name.size()));
}

void add_with_string_name(rjson::value& base, std::string_view name, rjson::value&& member) {
    base.AddMember(rjson::value(name.data(), name.size(), the_allocator), std::move(member), the_allocator);
}

void add_with_string_name(rjson::value& base, std::string_view name, rjson::string_ref_type member) {
    base.AddMember(rjson::value(name.data(), name.size(), the_allocator), rjson::value(member), the_allocator);
}

void add(rjson::value& base, rjson::string_ref_type name, rjson::value&& member) {
    base.AddMember(name, std::move(member), the_allocator);
}

void add(rjson::value& base, rjson::string_ref_type name, rjson::string_ref_type member) {
    base.AddMember(name, rjson::value(member), the_allocator);
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
    base_array.PushBack(std::move(item), the_allocator);

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
   case rjson::type::kStringType: {
       const size_t r1_len = r1.GetStringLength();
       const size_t r2_len = r2.GetStringLength();
       size_t len = std::min(r1_len, r2_len);
       int result = std::strncmp(r1.GetString(), r2.GetString(), len);
       return result < 0 || (result == 0 && r1_len < r2_len);
   }
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

    if (value.IsObject()) {
        for (auto it = value.MemberBegin(); it != value.MemberEnd();) {
            co_await destroy_gently(std::move(it->value));
            it = value.EraseMember(it);
        }
    } else if (value.IsArray()) {
        for (auto i = value.Size(); i > 0; --i) {
            auto index = i - 1;
            co_await destroy_gently(std::move(value[index]));
            value.Erase(value.Begin() + index);
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

} // end namespace rjson
