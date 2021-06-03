
/*
 * Copyright (C) 2015-present ScyllaDB
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
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

// TODO: upstream this to Boost.

#pragma once

#include <boost/program_options/errors.hpp>
#include <iostream>
#include <sstream>
#include <type_traits>

template<typename T>
concept HasMapInterface = requires(T t) {
    typename std::remove_reference<T>::type::mapped_type;
    typename std::remove_reference<T>::type::key_type;
    typename std::remove_reference<T>::type::value_type;
    t.find(typename std::remove_reference<T>::type::key_type());
    t.begin();
    t.end();
    t.cbegin();
    t.cend();
};

/// A Boost program option holding an enum value.
///
/// The options parser will parse enum values with the help of the Mapper class, which provides a mapping
/// between some parsable form (eg, string) and the enum value.  For example, it may map the word "January" to
/// the enum value JANUARY.
///
/// Mapper must have a static method `map()` that returns a map from a streamable key type (eg, string) to the
/// enum in question.  In fact, enum_option knows which enum it represents only by referencing
/// Mapper::map().mapped_type.
///
/// \note one enum_option holds only one enum value.  When multiple choices are allowed, use
/// vector<enum_option>.
///
/// Example:
///
/// struct Type {
///   enum ty { a1, a2, b1 };
///   static unordered_map<string, ty> map();
/// };
/// unordered_map<string, Type::ty> Type::map() {
///   return {{"a1", Type::a1}, {"a2", Type::a2}, {"b1", Type::b1}};
/// }
/// int main(int ac, char* av[]) {
///   namespace po = boost::program_options;
///   po::options_description desc("Allowed options");
///   desc.add_options()
///     ("val", po::value<enum_option<Type>>(), "Single Type")
///     ("vec", po::value<vector<enum_option<Type>>>()->multitoken(), "Type vector");
/// }
template<typename Mapper>
requires HasMapInterface<decltype(Mapper::map())>
class enum_option {
    using map_t = typename std::remove_reference<decltype(Mapper::map())>::type;
    typename map_t::mapped_type _value;
    map_t _map;
  public:
    // For smooth conversion from enum values:
    enum_option(const typename map_t::mapped_type& v) : _value(v), _map(Mapper::map()) {}

    // So values can be default-constructed before streaming into them:
    enum_option() : _map(Mapper::map()) {}

    bool operator==(const enum_option<Mapper>& that) const {
        return _value == that._value;
    }

    bool operator==(typename map_t::mapped_type value) const {
        return _value == value;
    }

    // For program_options parser:
    friend std::istream& operator>>(std::istream& s, enum_option<Mapper>& opt) {
        typename map_t::key_type key;
        s >> key;
        const auto found = opt._map.find(key);
        if (found == opt._map.end()) {
            std::string text;
            if (s.rdstate() & s.failbit) {
                // key wasn't read successfully.
                s >> text;
            } else {
                // Turn key into text.
                std::ostringstream temp;
                temp << key;
                text = temp.str();
            }
            throw boost::program_options::invalid_option_value(text);
        }
        opt._value = found->second;
        return s;
    }

    // For various printers and formatters:
    friend std::ostream& operator<<(std::ostream& s, const enum_option<Mapper>& opt) {
        auto found = find_if(opt._map.cbegin(), opt._map.cend(),
                             [&opt](const typename map_t::value_type& e) { return e.second == opt._value; });
        if (found == opt._map.cend()) {
            return s << "?unknown";
        } else {
            return s << found->first;
        }
    }
};
