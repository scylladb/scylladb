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

#include "rjson.hh"
#include "error.hh"
#include <seastar/core/print.hh>

namespace rjson {

static allocator the_allocator;

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
template<typename Handler>
struct guarded_json_handler : public Handler {
    size_t _nested_level = 0;
    size_t _max_nested_level;
public:
    using handler_base = Handler;

    explicit guarded_json_handler(size_t max_nested_level) : _max_nested_level(max_nested_level) {}
    explicit guarded_json_handler(string_buffer& buf, size_t max_nested_level)
            : handler_base(buf), _max_nested_level(max_nested_level) {}

    void Parse(const char* str, size_t length) {
        rapidjson::MemoryStream ms(static_cast<const char*>(str), length * sizeof(typename encoding::Ch));
        rapidjson::EncodedInputStream<encoding, rapidjson::MemoryStream> is(ms);
        rapidjson::GenericReader<encoding, encoding, allocator> reader(&the_allocator);
        reader.Parse(is, *this);
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

    bool StartObject() {
        ++_nested_level;
        check_nested_level();
        return handler_base::StartObject();
    }

    bool EndObject(rapidjson::SizeType elements_count = 0) {
        --_nested_level;
        return handler_base::EndObject(elements_count);
    }

    bool StartArray() {
        ++_nested_level;
        check_nested_level();
        return handler_base::StartArray();
    }

    bool EndArray(rapidjson::SizeType elements_count = 0) {
        --_nested_level;
        return handler_base::EndArray(elements_count);
    }
protected:
    void check_nested_level() const {
        if (RAPIDJSON_UNLIKELY(_nested_level > _max_nested_level)) {
            throw rjson::error(format("Max nested level reached: {}", _max_nested_level));
        }
    }
};

std::string print(const rjson::value& value) {
    string_buffer buffer;
    guarded_json_handler<writer> writer(buffer, 39);
    value.Accept(writer);
    return std::string(buffer.GetString());
}

rjson::value copy(const rjson::value& value) {
    return rjson::value(value, the_allocator);
}

rjson::value parse(const std::string& str) {
    return parse_raw(str.c_str(), str.size());
}

rjson::value parse_raw(const char* c_str, size_t size) {
    guarded_json_handler<document> d(39);
    d.Parse(c_str, size);
    if (d.HasParseError()) {
        throw rjson::error(format("Parsing JSON failed: {}", GetParseError_En(d.GetParseError())));
    }
    rjson::value& v = d;
    return std::move(v);
}

rjson::value& get(rjson::value& value, rjson::string_ref_type name) {
    auto member_it = value.FindMember(name);
    if (member_it != value.MemberEnd())
        return member_it->value;
    else {
        throw rjson::error(format("JSON parameter {} not found", name));
    }
}

const rjson::value& get(const rjson::value& value, rjson::string_ref_type name) {
    auto member_it = value.FindMember(name);
    if (member_it != value.MemberEnd())
        return member_it->value;
    else {
        throw rjson::error(format("JSON parameter {} not found", name));
    }
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

const rjson::value* find(const rjson::value& value, string_ref_type name) {
    auto member_it = value.FindMember(name);
    return member_it != value.MemberEnd() ? &member_it->value : nullptr;
}

rjson::value* find(rjson::value& value, string_ref_type name) {
    auto member_it = value.FindMember(name);
    return member_it != value.MemberEnd() ? &member_it->value : nullptr;
}

void set_with_string_name(rjson::value& base, const std::string& name, rjson::value&& member) {
    base.AddMember(rjson::value(name.c_str(), name.size(), the_allocator), std::move(member), the_allocator);
}

void set_with_string_name(rjson::value& base, std::string_view name, rjson::value&& member) {
    base.AddMember(rjson::value(name.data(), name.size(), the_allocator), std::move(member), the_allocator);
}

void set_with_string_name(rjson::value& base, const std::string& name, rjson::string_ref_type member) {
    base.AddMember(rjson::value(name.c_str(), name.size(), the_allocator), rjson::value(member), the_allocator);
}

void set_with_string_name(rjson::value& base, std::string_view name, rjson::string_ref_type member) {
    base.AddMember(rjson::value(name.data(), name.size(), the_allocator), rjson::value(member), the_allocator);
}

void set(rjson::value& base, rjson::string_ref_type name, rjson::value&& member) {
    base.AddMember(name, std::move(member), the_allocator);
}

void set(rjson::value& base, rjson::string_ref_type name, rjson::string_ref_type member) {
    base.AddMember(name, rjson::value(member), the_allocator);
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

} // end namespace rjson

std::ostream& std::operator<<(std::ostream& os, const rjson::value& v) {
    return os << rjson::print(v);
}
