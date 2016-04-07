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

class no_such_class : public std::runtime_error {
public:
    using runtime_error::runtime_error;
};

// BaseType is a base type of a type hierarchy that this registry will hold
// Args... are parameters for object's constructor
template<typename BaseType, typename... Args>
class class_registry {
    using creator_type = std::function<std::unique_ptr<BaseType>(Args...)>;
public:
    static void register_class(sstring name, creator_type creator);
    template<typename T>
    static void register_class(sstring name);
    static std::unique_ptr<BaseType> create(const sstring& name, Args&&...);

    static std::unordered_map<sstring, creator_type>& classes() {
        static std::unordered_map<sstring, creator_type> _classes;

        return _classes;
    }
};

template<typename BaseType, typename... Args>
void class_registry<BaseType, Args...>::register_class(sstring name, class_registry<BaseType, Args...>::creator_type creator) {
    classes().emplace(name, std::move(creator));
}

template<typename BaseType, typename... Args>
template<typename T>
void class_registry<BaseType, Args...>::register_class(sstring name) {
    register_class(name, [](Args&&... args) {
        return std::make_unique<T>(std::forward<Args>(args)...);
    });
}

template<typename BaseType, typename T, typename... Args>
struct class_registrator {
    class_registrator(const sstring& name) {
        class_registry<BaseType, Args...>::template register_class<T>(name);
    }
    class_registrator(const sstring& name, std::function<std::unique_ptr<T>(Args...)> creator) {
        class_registry<BaseType, Args...>::register_class(name, creator);
    }
};

template<typename BaseType, typename... Args>
std::unique_ptr<BaseType> class_registry<BaseType, Args...>::create(const sstring& name, Args&&... args) {
    auto it = classes().find(name);
    if (it == classes().end()) {
        throw no_such_class(sstring("unable to find class '") + name + sstring("'"));
    }

    return it->second(std::forward<Args>(args)...);
}

template<typename BaseType, typename... Args>
std::unique_ptr<BaseType> create_object(const sstring& name, Args&&... args) {
    return class_registry<BaseType, Args...>::create(name, std::forward<Args>(args)...);
}

