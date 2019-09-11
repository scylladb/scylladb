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

std::string print(const rjson::value& value) {
    string_buffer buffer;
    writer writer(buffer);
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
    rjson::document d;
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

void set_with_string_name(rjson::value& base, const std::string& name, rjson::string_ref_type member) {
    base.AddMember(rjson::value(name.c_str(), name.size(), the_allocator), rjson::value(member), the_allocator);
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

} // end namespace rjson

std::ostream& std::operator<<(std::ostream& os, const rjson::value& v) {
    return os << rjson::print(v);
}
