/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#include "db/marshal/type_parser.hh"

#include "exceptions/exceptions.hh"

#include <stdexcept>
#include <string>
#include <tuple>

#include "types/user.hh"
#include "types/map.hh"
#include "types/list.hh"
#include "types/set.hh"
#include "types/vector.hh"

class parse_exception : public exceptions::syntax_exception {
public:
    parse_exception(const sstring& src, size_t index, const sstring& msg)
        : syntax_exception(format("Syntax error parsing '{}' at char {:d}: {}", src, index, msg))
    {}
};

namespace db {

namespace marshal {

type_parser::type_parser(std::string_view str, size_t idx)
    : _str{str.begin(), str.end()}
    , _idx{idx}
{ }

type_parser::type_parser(std::string_view str)
    : type_parser{str, 0}
{ }

data_type type_parser::parse(const sstring& str) {
    return type_parser(std::string_view(str)).parse();
}

data_type type_parser::parse(std::string_view str) {
    return type_parser(str).parse();
}

data_type type_parser::parse() {
    return do_parse(true);
}

data_type type_parser::do_parse(bool multicell)
{
    skip_blank();

    sstring name = read_next_identifier();
    if (name.empty()) {
        if (!is_eos()) {
            throw std::runtime_error("unknown type: " + _str);
        }
        return bytes_type;
    }

    if (_str[_idx] == ':') {
        _idx++;
        size_t pos = 0;
        try {
            std::stoul(name, &pos, 0x10);
        } catch (...) {            
        }
        if (pos != name.size() || pos == 0) {
            throw parse_exception(_str, _idx - 1 - name.size() + pos, "expected 8-byte hex number, found '" + name + "'");
        }
        name = read_next_identifier();
    }

    skip_blank();
    if (!is_eos() && _str[_idx] == '(') {
        return get_abstract_type(name, *this, multicell);
    } else {
        return get_abstract_type(name);
    }
}

std::vector<data_type> type_parser::get_type_parameters(bool multicell)
{
    std::vector<data_type> list;

    if (is_eos()) {
        return list;
    }

    if (_str[_idx] != '(') {
        throw std::logic_error("internal error");
    }

    ++_idx; // skipping '('

    while (skip_blank_and_comma())
    {
        if (_str[_idx] == ')') {
            ++_idx;
            return list;
        }

        list.emplace_back(do_parse(multicell));
    }
    throw parse_exception(_str, _idx, "unexpected end of string");
}

std::tuple<data_type, size_t> type_parser::get_vector_parameters()
{
    if (is_eos() || _str[_idx] != '(') {
        throw std::logic_error("internal error");
    }

    ++_idx; // skipping '('

    skip_blank_and_comma();
    if (_str[_idx] == ')') {
        ++_idx;
        return std::make_tuple(nullptr, 0);
    }

    data_type type = do_parse(true);
    size_t size = 0;
    if (_str[_idx] == ',') {
        ++_idx;
        skip_blank();

        size_t i = _idx;
        while (!is_eos() && _str[_idx] >= '0' && _str[_idx] <= '9') {
            ++_idx;
        }

        if (is_eos() || _str[_idx] != ')') {
            throw parse_exception(_str, _idx, "expected digit or ')'");
        }

        size = std::stoul(_str.substr(i, _idx - i));

        ++_idx; // skipping ')'
        return std::make_tuple(type, size);
    } else if (!is_eos()) {
        throw parse_exception(_str, _idx, "expected size parameter");
    }

    throw parse_exception(_str, _idx, "unexpected end of string");
}

std::tuple<sstring, bytes, std::vector<bytes>, std::vector<data_type>> type_parser::get_user_type_parameters()
{
    if (is_eos() || _str[_idx] != '(') {
        throw std::logic_error("internal error");
    }

    ++_idx; // skipping '('

    skip_blank_and_comma();
    sstring keyspace = read_next_identifier();
    skip_blank_and_comma();
    bytes name = from_hex(read_next_identifier());

    std::vector<bytes> field_names;
    std::vector<data_type> field_types;

    while (skip_blank_and_comma())
    {
        if (_str[_idx] == ')') {
            ++_idx;
            return std::make_tuple(std::move(keyspace), std::move(name), std::move(field_names), std::move(field_types));
        }

        field_names.emplace_back(from_hex(read_next_identifier()));

        if (_str[_idx] != ':') {
            throw parse_exception(_str, _idx, "expecting ':' token");
        }
        ++_idx;

        field_types.emplace_back(do_parse(true));
    }
    throw parse_exception(_str, _idx, "unexpected end of string");
}

data_type type_parser::get_abstract_type(const sstring& compare_with)
{
    sstring class_name;
    if (compare_with.find('.') != sstring::npos) {
        class_name = compare_with;
    } else {
        class_name = "org.apache.cassandra.db.marshal." + compare_with;
    }
    return abstract_type::parse_type(class_name);
}

data_type type_parser::get_abstract_type(const sstring& compare_with, type_parser& parser, bool multicell)
{
    sstring class_name;
    if (compare_with.find('.') != sstring::npos) {
        class_name = compare_with;
    } else {
        class_name = "org.apache.cassandra.db.marshal." + compare_with;
    }
    if (class_name == "org.apache.cassandra.db.marshal.ReversedType") {
        auto l = parser.get_type_parameters(false);
        if (l.size() != 1) {
            throw exceptions::configuration_exception("ReversedType takes exactly 1 type parameter");
        }
        return reversed_type_impl::get_instance(l[0]);
    } else if (class_name == "org.apache.cassandra.db.marshal.FrozenType") {
        auto l = parser.get_type_parameters(false);
        if (l.size() != 1) {
            throw exceptions::configuration_exception("FrozenType takes exactly 1 type parameter");
        }
        return l[0];
    } else if (class_name == "org.apache.cassandra.db.marshal.ListType") {
        auto l = parser.get_type_parameters();
        if (l.size() != 1) {
            throw exceptions::configuration_exception("ListType takes exactly 1 type parameter");
        }
        return list_type_impl::get_instance(l[0], multicell);
    } else if (class_name == "org.apache.cassandra.db.marshal.SetType") {
        auto l = parser.get_type_parameters();
        if (l.size() != 1) {
            throw exceptions::configuration_exception("SetType takes exactly 1 type parameter");
        }
        return set_type_impl::get_instance(l[0], multicell);
    } else if (class_name == "org.apache.cassandra.db.marshal.MapType") {
        auto l = parser.get_type_parameters();
        if (l.size() != 2) {
            throw exceptions::configuration_exception("MapType takes exactly 2 type parameters");
        }
        return map_type_impl::get_instance(l[0], l[1], multicell);
    } else if (class_name == "org.apache.cassandra.db.marshal.TupleType") {
        auto l = parser.get_type_parameters();
        if (l.size() == 0) {
            throw exceptions::configuration_exception("TupleType takes at least 1 type parameter");
        }
        return tuple_type_impl::get_instance(l);
    } else if (class_name == "org.apache.cassandra.db.marshal.VectorType") {
        auto [type, size] = parser.get_vector_parameters();
        if (!type) {
            throw exceptions::configuration_exception("VectorType takes exactly 1 type parameter and 1 size parameter");
        }
        return vector_type_impl::get_instance(type, size);
    } else if (class_name == "org.apache.cassandra.db.marshal.UserType") {
        auto [keyspace, name, field_names, field_types] = parser.get_user_type_parameters();
        return user_type_impl::get_instance(
                std::move(keyspace), std::move(name), std::move(field_names), std::move(field_types), multicell);
    } else {
        throw std::runtime_error("unknown type: " + class_name);
    }
}

bool type_parser::is_eos() const
{
    return is_eos(_str, _idx);
}

bool type_parser::is_eos(const sstring& str, size_t i)
{
    return i >= str.size();
}

bool type_parser::is_blank(char c)
{
    return c == ' ' || c == '\t' || c == '\n';
}

void type_parser::skip_blank()
{
    _idx = skip_blank(_str, _idx);
}

size_t type_parser::skip_blank(const sstring& str, size_t i)
{
    while (!is_eos(str, i) && is_blank(str[i])) {
        ++i;
    }

    return i;
}

bool type_parser::skip_blank_and_comma()
{
    bool comma_found = false;
    while (!is_eos()) {
        int c = _str[_idx];
        if (c == ',') {
            if (comma_found)
                return true;
            else
                comma_found = true;
        } else if (!is_blank(c)) {
            return true;
        }
        ++_idx;
    }
    return false;
}

/*
 * [0..9a..bA..B-+._&]
 */
bool type_parser::is_identifier_char(char c)
{
    return (c >= '0' && c <= '9')
        || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
        || c == '-' || c == '+' || c == '.' || c == '_' || c == '&';
}

// left idx positioned on the character stopping the read
sstring type_parser::read_next_identifier()
{
    size_t i = _idx;
    while (!is_eos() && is_identifier_char(_str[_idx])) {
        ++_idx;
    }
    return _str.substr(i, _idx-i);
}

}

}
