/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright 2015 Cloudius Systems
 *
 * Modified by Cloudius Systems
 */

#include "db/marshal/type_parser.hh"

#include "exceptions/exceptions.hh"

#include <stdexcept>
#include <string>

namespace db {

namespace marshal {

type_parser::type_parser(const sstring& str, size_t idx)
    : _str{str}
    , _idx{idx}
{ }

type_parser::type_parser(const sstring& str)
    : type_parser{str, 0}
{ }

data_type type_parser::parse(const sstring& str) {
#if 0
   if (str == null)
       return BytesType.instance;

   AbstractType<?> type = cache.get(str);

   if (type != null)
       return type;
#endif

    // This could be simplier (i.e. new TypeParser(str).parse()) but we avoid creating a TypeParser object if not really necessary.
    size_t i = 0;
    i = skip_blank(str, i);
    size_t j = i;
    while (!is_eos(str, i) && is_identifier_char(str[i])) {
        ++i;
    }
    if (i == j) {
        return bytes_type;
    }
    sstring name = str.substr(j, i-j);
    i = skip_blank(str, i);

    data_type type;

    if (!is_eos(str, i) && str[i] == '(') {
        type = get_abstract_type(name, type_parser{str, i});
    } else {
        type = get_abstract_type(name);
    }

#if 0
    // We don't really care about concurrency here. Worst case scenario, we do some parsing unnecessarily
    cache.put(str, type);
#endif
    return type;
}

data_type type_parser::parse()
{
    skip_blank();

    sstring name = read_next_identifier();
    if (_str[_idx] == ':') {
        _idx++;
        try {
            size_t pos;
            std::stoul(name, &pos, 0x10);
            if (pos != name.size()) {
                throw exceptions::syntax_exception(sprint("expected 8-byte hex number, found %s", name));
            }
        } catch (const std::invalid_argument & e) {
            throw exceptions::syntax_exception(sprint("expected 8-byte hex number, found %s", name));
        } catch (const std::out_of_range& e) {
            throw exceptions::syntax_exception(sprint("expected 8-byte hex number, found %s", name));
        }
        name = read_next_identifier();
    }

    skip_blank();
    if (!is_eos() && _str[_idx] == '(')
        return get_abstract_type(name, *this);
    else
        return get_abstract_type(name);
}

std::vector<data_type> type_parser::get_type_parameters()
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

        try {
            list.emplace_back(parse());
        } catch (exceptions::syntax_exception e) {
            // FIXME
#if 0
            SyntaxException ex = new SyntaxException(String.format("Exception while parsing '%s' around char %d", str, idx));
            ex.initCause(e);
#endif
            throw e;
        }
    }
    throw exceptions::syntax_exception(sprint("Syntax error parsing '%s' at char %d: unexpected end of string", _str, _idx));
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

data_type type_parser::get_abstract_type(const sstring& compare_with, type_parser parser)
{
    sstring class_name;
    if (compare_with.find('.') != sstring::npos) {
        class_name = compare_with;
    } else {
        class_name = "org.apache.cassandra.db.marshal." + compare_with;
    }
    if (class_name == "org.apache.cassandra.db.marshal.ListType") {
        auto l = parser.get_type_parameters();
        if (l.size() != 1) {
            throw exceptions::configuration_exception("ListType takes exactly 1 type parameter");
        }
        return list_type_impl::get_instance(l[0], true);
    } else if (class_name == "org.apache.cassandra.db.marshal.SetType") {
        auto l = parser.get_type_parameters();
        if (l.size() != 1) {
            throw exceptions::configuration_exception("SetType takes exactly 1 type parameter");
        }
        return set_type_impl::get_instance(l[0], true);
    } else if (class_name == "org.apache.cassandra.db.marshal.MapType") {
        auto l = parser.get_type_parameters();
        if (l.size() != 2) {
            throw exceptions::configuration_exception("MapType takes exactly 2 type parameters");
        }
        return map_type_impl::get_instance(l[0], l[1], true);
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
