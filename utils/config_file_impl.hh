/*
 * Copyright (C) 2015 ScyllaDB
 *
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

#pragma once

#include <iterator>
#include <regex>

#include <yaml-cpp/yaml.h>
#include <boost/any.hpp>

#include "config_file.hh"

namespace YAML {

/*
 * Add converters as needed here...
 *
 * TODO: Maybe we should just define all node conversionas as "lexical_cast".
 * However, vanilla yamp-cpp does some special treatment of scalar types,
 * mainly inf handling etc. Hm.
 */
template<>
struct convert<seastar::sstring> {
    static bool decode(const Node& node, sstring& rhs) {
        std::string tmp;
        if (!convert<std::string>::decode(node, tmp)) {
            return false;
        }
        rhs = tmp;
        return true;
    }
};

template<typename K, typename V, typename... Rest>
struct convert<std::unordered_map<K, V, Rest...>> {
    using map_type = std::unordered_map<K, V, Rest...>;

    static bool decode(const Node& node, map_type& rhs) {
        if (!node.IsMap()) {
            return false;
        }
        rhs.clear();
        for (auto& n : node) {
            rhs[n.first.as<K>()] = n.second.as<V>();
        }
        return true;
    }
};

}

namespace std {

template<typename K, typename V, typename... Args>
std::istream& operator>>(std::istream&, std::unordered_map<K, V, Args...>&);

template<>
std::istream& operator>>(std::istream&, std::unordered_map<seastar::sstring, seastar::sstring>&);

extern template
std::istream& operator>>(std::istream&, std::unordered_map<seastar::sstring, seastar::sstring>&);

template<typename V, typename... Args>
std::istream& operator>>(std::istream&, std::vector<V, Args...>&);

template<>
std::istream& operator>>(std::istream&, std::vector<seastar::sstring>&);

extern template
std::istream& operator>>(std::istream&, std::vector<seastar::sstring>&);

template<typename K, typename V, typename... Args>
std::istream& operator>>(std::istream& is, std::unordered_map<K, V, Args...>& map) {
    std::unordered_map<sstring, sstring> tmp;
    is >> tmp;

    for (auto& p : tmp) {
        map[boost::lexical_cast<K>(p.first)] = boost::lexical_cast<V>(p.second);
    }
    return is;
}

template<typename V, typename... Args>
std::istream& operator>>(std::istream& is, std::vector<V, Args...>& dst) {
    std::vector<seastar::sstring> tmp;
    is >> tmp;
    for (auto& v : tmp) {
        dst.emplace_back(boost::lexical_cast<V>(v));
    }
    return is;
}

template<typename K, typename V, typename... Args>
void validate(boost::any& out, const std::vector<std::string>& in, std::unordered_map<K, V, Args...>*, int utf8) {
    using map_type = std::unordered_map<K, V, Args...>;

    if (out.empty()) {
        out = boost::any(map_type());
    }

    static const std::regex key(R"foo((?:^|\:)([^=:]+)=)foo");

    auto* p = boost::any_cast<map_type>(&out);
    for (const auto& s : in) {
        std::sregex_iterator i(s.begin(), s.end(), key), e;

        if (i == e) {
            throw boost::program_options::invalid_option_value(s);
        }

        while (i != e) {
            auto k = (*i)[1].str();
            auto vs = s.begin() + i->position() + i->length();
            auto ve = s.end();

            if (++i != e) {
                ve = s.begin() + i->position();
            }

            (*p)[boost::lexical_cast<K>(k)] = boost::lexical_cast<V>(sstring(vs, ve));
        }
    }
}

}

namespace utils {

namespace {

/*
 * Our own bpo::typed_valye.
 * Only difference is that we _don't_ apply defaults (they are already applied)
 * Needed to make aliases work properly.
 */
template<class T, class charT = char>
class typed_value_ex : public bpo::typed_value<T, charT> {
public:
    typedef bpo::typed_value<T, charT> _Super;

    typed_value_ex(T* store_to)
        : _Super(store_to)
    {}
    bool apply_default(boost::any& value_store) const override {
        return false;
    }
};

template<class T>
inline typed_value_ex<T>* value_ex(T* v) {
    typed_value_ex<T>* r = new typed_value_ex<T>(v);
    return r;
}

template<class T>
inline typed_value_ex<std::vector<T>>* value_ex(std::vector<T>* v) {
    auto r = new typed_value_ex<std::vector<T>>(v);
    r->multitoken();
    return r;
}

}

sstring hyphenate(const stdx::string_view&);

}

template<typename T, utils::config_file::value_status S>
void utils::config_file::named_value<T, S>::add_command_line_option(
                boost::program_options::options_description_easy_init& init,
                const stdx::string_view& name, const stdx::string_view& desc) {
    // NOTE. We are not adding default values. We could, but must in that case manually (in some way) geenrate the textual
    // version, since the available ostream operators for things like pairs and collections don't match what we can deal with parser-wise.
    // See removed ostream operators above.
    init(hyphenate(name).data(), value_ex(&_value)->notifier([this](auto&&) { _source = config_source::CommandLine; }), desc.data());
}

template<typename T, utils::config_file::value_status S>
void utils::config_file::named_value<T, S>::set_value(const YAML::Node& node) {
    (*this)(node.as<T>());
    _source = config_source::SettingsFile;
}
