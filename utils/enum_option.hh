
/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

// TODO: upstream this to Boost.

#pragma once

#include <boost/program_options/errors.hpp>
#include <optional>
#include <iosfwd>
#include <sstream>
#include <type_traits>
#include <fmt/ostream.h>
#include <concepts>

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
///   enum class ty { a1, a2, b1 };
///   static unordered_map<string, ty> map();
/// };
/// unordered_map<string, Type::ty> Type::map() {
///   return {{"a1", Type::ty::a1}, {"a2", Type::ty::a2}, {"b1", Type::ty::b1}};
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
    std::optional<typename map_t::key_type> _key;

    static std::optional<typename map_t::key_type> map_value_to_key(const typename map_t::mapped_type& v) {
        auto map = Mapper::map();
        auto found = find_if(map.cbegin(), map.cend(),
                             [&v](const auto& e) {
                                 return e.second == v;
                             });
        if (found == map.cend()) {
            return std::nullopt;
        } else {
            return found->first;
        }
    }

  public:
    // For smooth conversion from enum values:
    enum_option(const typename map_t::mapped_type& v) : _value(v), _key(map_value_to_key(_value)) { }

    // So values can be default-constructed before streaming into them:
    enum_option() {}

    bool operator==(const enum_option<Mapper>& that) const {
        return _value == that._value;
    }

    // For comparison with enum values using if or switch:
    bool operator==(typename map_t::mapped_type value) const {
        return _value == value;
    }
    operator typename map_t::mapped_type() const {
        return _value;
    }

    // For program_options parser:
    friend std::istream& operator>>(std::istream& s, enum_option<Mapper>& opt) {
        typename map_t::key_type key;
        s >> key;
        if constexpr (requires { requires std::ranges::range<typename map_t::key_type>;  requires std::same_as<typename map_t::key_type::value_type, char>; }) {
            if (key.size() >= 2 && key.front() == '"' && key.back() == '"') {
                key = key.substr(1, key.size() - 2);
            }
        }
        auto map = Mapper::map();
        const auto found = map.find(key);
        if (found == map.end()) {
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
        opt._key = key;
        opt._value = found->second;
        return s;
    }

    // For various printers and formatters:
    friend fmt::formatter<enum_option<Mapper>>;
};

template <typename Mapper>
struct fmt::formatter<enum_option<Mapper>> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const enum_option<Mapper>& opt, fmt::format_context& ctx) const {
        if (!opt._key.has_value()) {
            return fmt::format_to(ctx.out(), "?unknown");
        } else {
            return fmt::format_to(ctx.out(), "{}", opt._key.value());
        }
    }
};

template <typename Mapper>
std::ostream& operator<<(std::ostream& s, const enum_option<Mapper>& opt) {
    fmt::print(s, "{}", opt);
    return s;
}
