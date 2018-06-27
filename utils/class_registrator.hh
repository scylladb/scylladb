/*
 * Copyright (C) 2015 ScyllaDB
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

#include <memory>
#include <seastar/core/shared_ptr.hh>
#include <boost/range/algorithm/find_if.hpp>

class no_such_class : public std::runtime_error {
public:
    using runtime_error::runtime_error;
};

inline bool is_class_name_qualified(const sstring& class_name) {
    return class_name.find_last_of('.') != sstring::npos;
}

// BaseType is a base type of a type hierarchy that this registry will hold
// Args... are parameters for object's constructor
template<typename BaseType, typename... Args>
class class_registry {
    template<typename T>
    struct result_for {
        typedef std::unique_ptr<T> type;
        template<typename Impl>
        static inline type make(Args&& ...args) {
            return std::make_unique<Impl>(std::forward<Args>(args)...);
        }
    };
    template<typename T>
    struct result_for<seastar::shared_ptr<T>> {
        typedef seastar::shared_ptr<T> type;
        template<typename Impl>
        static inline type make(Args&& ...args) {
            return seastar::make_shared<Impl>(std::forward<Args>(args)...);
        }
    };
    template<typename T>
    struct result_for<seastar::lw_shared_ptr<T>> {
        typedef seastar::lw_shared_ptr<T> type;
        // lw_shared is not (yet?) polymorph, thus having automatic
        // instantiation of it makes no sense. This way we get a nice
        // compilation error if someone messes up.
    };
    template<typename T>
    struct result_for<std::shared_ptr<T>> {
        typedef std::shared_ptr<T> type;
        template<typename Impl>
        static inline type make(Args&& ...args) {
            return std::make_shared<Impl>(std::forward<Args>(args)...);
        }
   };
    template<typename T, typename D>
    struct result_for<std::unique_ptr<T, D>> {
        typedef std::unique_ptr<T, D> type;
        template<typename Impl>
        static inline type make(Args&& ...args) {
            return std::make_unique<Impl, D>(std::forward<Args>(args)...);
        }
    };
public:
    using result_type = typename result_for<BaseType>::type;
    using creator_type = std::function<result_type(Args...)>;

    static void register_class(sstring name, creator_type creator);
    template<typename T>
    static void register_class(sstring name);
    static result_type create(const sstring& name, Args&&...);

    static std::unordered_map<sstring, creator_type>& classes() {
        static std::unordered_map<sstring, creator_type> _classes;

        return _classes;
    }

    static sstring to_qualified_class_name(sstring class_name);
};

template<typename BaseType, typename... Args>
void class_registry<BaseType, Args...>::register_class(sstring name, typename class_registry<BaseType, Args...>::creator_type creator) {
    classes().emplace(name, std::move(creator));
}

template<typename BaseType, typename... Args>
template<typename T>
void class_registry<BaseType, Args...>::register_class(sstring name) {
    register_class(name, &result_for<BaseType>::template make<T>);
}

template<typename BaseType, typename... Args>
sstring class_registry<BaseType, Args...>::to_qualified_class_name(sstring class_name) {
    if (is_class_name_qualified(class_name)) {
        return class_name;
    } else {
        const auto& classes{class_registry<BaseType, Args...>::classes()};

        const auto it = boost::find_if(classes, [&class_name](const auto& registered_class) {
            // the fully qualified name contains the short name
            auto i = registered_class.first.find_last_of('.');
            return i != sstring::npos && registered_class.first.compare(i + 1, sstring::npos, class_name) == 0;
        });
        return it == classes.end() ? class_name : it->first;
    }
}

template<typename BaseType, typename T, typename... Args>
struct class_registrator {
    class_registrator(const sstring& name) {
        class_registry<BaseType, Args...>::template register_class<T>(name);
    }
    class_registrator(const sstring& name, typename class_registry<BaseType, Args...>::creator_type creator) {
        class_registry<BaseType, Args...>::register_class(name, creator);
    }
};

template<typename BaseType, typename... Args>
typename class_registry<BaseType, Args...>::result_type class_registry<BaseType, Args...>::create(const sstring& name, Args&&... args) {
    auto it = classes().find(name);
    if (it == classes().end()) {
        throw no_such_class(sstring("unable to find class '") + name + sstring("'"));
    }

    return it->second(std::forward<Args>(args)...);
}

template<typename BaseType, typename... Args>
typename class_registry<BaseType, Args...>::result_type  create_object(const sstring& name, Args&&... args) {
    return class_registry<BaseType, Args...>::create(name, std::forward<Args>(args)...);
}

class qualified_name {
    sstring _qname;
public:
    // can be optimized with string_views etc.
    qualified_name(const sstring& pkg_pfx, const sstring& name)
        : _qname(is_class_name_qualified(name) ? name : pkg_pfx + name)
    {}
    operator const sstring&() const {
        return _qname;
    }
};

class unqualified_name {
    sstring _qname;
public:
    // can be optimized with string_views etc.
    unqualified_name(const sstring& pkg_pfx, const sstring& name)
        : _qname(name.compare(0, pkg_pfx.size(), pkg_pfx) == 0 ? name.substr(pkg_pfx.size()) : name)
    {}
    operator const sstring&() const {
        return _qname;
    }
};
