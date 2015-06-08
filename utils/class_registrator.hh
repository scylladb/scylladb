/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
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
    static std::unordered_map<sstring, creator_type> _classes;
public:
    static void register_class(sstring name, creator_type creator);
    template<typename T>
    static void register_class(sstring name);
    static std::unique_ptr<BaseType> create(const sstring& name, Args...);
};

template<typename BaseType, typename... Args>
std::unordered_map<sstring, typename class_registry<BaseType, Args...>::creator_type> class_registry<BaseType, Args...>::_classes;

template<typename BaseType, typename... Args>
void class_registry<BaseType, Args...>::register_class(sstring name, class_registry<BaseType, Args...>::creator_type creator) {
    _classes.emplace(name, std::move(creator));
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
std::unique_ptr<BaseType> class_registry<BaseType, Args...>::create(const sstring& name, Args... args) {
    auto it = _classes.find(name);
    if (it == _classes.end()) {
        throw no_such_class(sstring("unable to find class '") + name + sstring("'"));
    }
    return it->second(args...);
}

template<typename BaseType, typename... Args>
std::unique_ptr<BaseType> create_object(const sstring& name, Args&&... args) {
    return class_registry<BaseType, Args...>::create(name, std::forward<Args>(args)...);
}

