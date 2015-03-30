/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef DISTRIBUTED_HH_
#define DISTRIBUTED_HH_

#include "reactor.hh"
#include "future-util.hh"

template <typename Service>
class distributed {
    std::vector<Service*> _instances;
public:
    distributed() {}
    distributed(const distributed& other) = delete;
    distributed(distributed&& other) = default;
    distributed& operator=(const distributed& other) = delete;
    distributed& operator=(distributed& other) = default;
    ~distributed();

    // Start @Service by constructing an instance on every engine
    // with @args passed to the constructor.
    // The return value becomes ready when all instances have been
    // constructed.
    template <typename... Args>
    future<> start(Args&&... args);

    // Start @Service by constructing an instance on a single CPU.
    // with @args passed to the constructor.
    // The return value becomes ready when all instances have been
    // constructed.
    template <typename... Args>
    future<> start_single(Args&&... args);

    // Stop @Service by destroying the instances started by start().
    // The return value becomes ready when all instances have been
    // destroyed.
    future<> stop();

    // Invoke a method on all instances of @Service.
    // The return value becomes ready when all instances have processed
    // the message.
    template <typename... Args>
    future<> invoke_on_all(future<> (Service::*func)(Args...), Args... args);

    // Invoke a method on all instances of @Service.
    // The return value becomes ready when all instances have processed
    // the message.
    template <typename... Args>
    future<> invoke_on_all(void (Service::*func)(Args...), Args... args);

    // Invoke a function on all instances of @Service (the local instance
    // of the service is passed by reference).
    // The return value becomes ready when all instances have processed
    // the message.
    //
    // @func a functor returning void or future<>
    template <typename Func>
    future<> invoke_on_all(Func&& func);

    // Invoke a method on all instances of @Service and reduce the results using
    // @Reducer. See ::map_reduce().
    template <typename Reducer, typename Ret, typename... FuncArgs, typename... Args>
    inline
    auto
    map_reduce(Reducer&& r, Ret (Service::*func)(FuncArgs...), Args&&... args)
        -> typename reducer_traits<Reducer>::future_type
    {
        unsigned c = 0;
        return ::map_reduce(_instances.begin(), _instances.end(),
            [&c, func, args = std::make_tuple(std::forward<Args>(args)...)] (Service* inst) mutable {
                return smp::submit_to(c++, [inst, func, args] () mutable {
                    return apply([inst, func] (Args&&... args) mutable {
                        return (inst->*func)(std::forward<Args>(args)...);
                    }, std::move(args));
                });
            }, std::forward<Reducer>(r));
    }

    // Invoke a method on all instances of @Service and reduce the results using
    // @Reducer. See ::map_reduce().
    // @Func gets local instance reference as argument.
    template <typename Reducer, typename Func>
    inline
    auto map_reduce(Reducer&& r, Func&& func) -> typename reducer_traits<Reducer>::future_type
    {
        unsigned c = 0;
        return ::map_reduce(_instances.begin(), _instances.end(),
            [&c, &func] (Service* inst) mutable {
                return smp::submit_to(c++, [inst, func] () mutable {
                    return func(*inst);
                });
            }, std::forward<Reducer>(r));
    }

    // Invoke a method on a specific instance of @Service.
    // The return value (which must be a future) contains the future
    // returned by @Service.
    template <typename Ret, typename... FuncArgs, typename... Args>
    std::enable_if_t<is_future<Ret>::value, Ret>
    invoke_on(unsigned id, Ret (Service::*func)(FuncArgs...), Args&&... args) {
        auto inst = _instances[id];
        return smp::submit_to(id, [inst, func, args = std::make_tuple(std::forward<Args>(args)...)] () mutable {
            return apply([inst, func] (Args&&... args) mutable {
                return (inst->*func)(std::forward<Args>(args)...);
            }, std::move(args));
        });
    }

    // Invoke a method on a specific instance of @Service.
    template <typename Ret, typename... FuncArgs, typename... Args>
    std::enable_if_t<!is_future<Ret>::value && !std::is_same<Ret, void>::value, future<Ret>>
    invoke_on(unsigned id, Ret (Service::*func)(FuncArgs...), Args&&... args) {
        auto inst = _instances[id];
        return smp::submit_to(id, [inst, func, args = std::make_tuple(std::forward<Args>(args)...)] () mutable {
                return apply([inst, func] (Args&&... args) mutable {
                    return make_ready_future<Ret>((inst->*func)(std::forward<Args>(args)...));
                }, std::move(args));
            });
    }

    template <typename... FuncArgs, typename... Args>
    void invoke_on(unsigned id, void (Service::*func)(FuncArgs...), Args&&... args) {
        auto inst = _instances[id];
        smp::submit_to(id, [inst, func, args...] () mutable {
            (inst->*func)(std::forward<Args>(args)...);
        });
    }

    // Invoke a function object on a specific instance of the service.
    //
    // @func function object, which may return an future, a value, or void.
    template <typename Func>
    futurize_t<std::result_of_t<Func(Service&)>>
    invoke_on(unsigned id, Func&& func) {
        auto inst = _instances[id];
        return smp::submit_to(id, [inst, func = std::forward<Func>(func)] () mutable {
            return func(*inst);
        });
    }

    // Returns reference to the local instance.
    Service& local();
};

template <typename Service>
distributed<Service>::~distributed() {
	assert(_instances.empty());
}

template <typename Service>
template <typename... Args>
future<>
distributed<Service>::start(Args&&... args) {
    _instances.resize(smp::count);
    unsigned c = 0;
    return parallel_for_each(_instances.begin(), _instances.end(),
        [this, &c, args = std::make_tuple(std::forward<Args>(args)...)] (Service*& inst) mutable {
            return smp::submit_to(c++, [&inst, args] () mutable {
                inst = apply([] (Args&&... args) {
                    return new Service(std::forward<Args>(args)...);
                }, std::move(args));
            });
    });
}

template <typename Service>
template <typename... Args>
future<>
distributed<Service>::start_single(Args&&... args) {
    assert(_instances.empty());
    _instances.resize(1);
    return smp::submit_to(0, [this, args = std::make_tuple(std::forward<Args>(args)...)] () mutable {
        _instances[0] = apply([] (Args&&... args) {
            return new Service(std::forward<Args>(args)...);
        }, std::move(args));
    });
}

template <typename Service>
future<>
distributed<Service>::stop() {
    unsigned c = 0;
    return parallel_for_each(_instances.begin(), _instances.end(), [&c] (Service*& inst) mutable {
        return smp::submit_to(c++, [inst] () mutable {
            return inst->stop().then([inst] () mutable {
                delete inst;
            });
        });
    }).then([this] {
        _instances.clear();
    });
}

template <typename Service>
template <typename... Args>
inline
future<>
distributed<Service>::invoke_on_all(future<> (Service::*func)(Args...), Args... args) {
    unsigned c = 0;
    return parallel_for_each(_instances.begin(), _instances.end(), [&c, func, args...] (Service* inst) {
        return smp::submit_to(c++, [inst, func, args...] {
            return (inst->*func)(args...);
        });
    });
}

template <typename Service>
template <typename... Args>
inline
future<>
distributed<Service>::invoke_on_all(void (Service::*func)(Args...), Args... args) {
    unsigned c = 0;
    return parallel_for_each(_instances.begin(), _instances.end(), [&c, func, args...] (Service* inst) {
        return smp::submit_to(c++, [inst, func, args...] {
            (inst->*func)(args...);
        });
    });
}

template <typename Service>
template <typename Func>
inline
future<>
distributed<Service>::invoke_on_all(Func&& func) {
    static_assert(std::is_same<futurize_t<std::result_of_t<Func(Service&)>>, future<>>::value,
                  "invoke_on_all()'s func must return void or future<>");
    unsigned c = 0;
    return parallel_for_each(_instances.begin(), _instances.end(), [&c, &func] (Service* inst) {
        return smp::submit_to(c++, [inst, func] {
            return func(*inst);
        });
    });
}

template <typename Service>
Service& distributed<Service>::local() {
    return *_instances[engine().cpu_id()];
}

// Smart pointer wrapper which makes it safe to move across CPUs.
// An armed pointer will be deleted on the CPU on which it was wrapped.
// Empty pointer will be deleted on the current CPU so it must be SMP-safe.
template <typename PtrType>
class foreign_ptr {
private:
    PtrType _value;
    unsigned _cpu;
private:
    bool on_origin() {
        return engine().cpu_id() == _cpu;
    }
public:
    using element_type = typename PtrType::element_type;

    foreign_ptr()
        : _value(PtrType())
        , _cpu(engine().cpu_id()) {
    }
    foreign_ptr(std::nullptr_t) : foreign_ptr() {}
    foreign_ptr(PtrType value)
        : _value(std::move(value))
        , _cpu(engine().cpu_id()) {
    }
    // The type is intentionally non-copyable because copies
    // are expensive because each copy requires across-CPU call.
    foreign_ptr(const foreign_ptr&) = delete;
    foreign_ptr(foreign_ptr&& other) = default;
    ~foreign_ptr() {
        if (_value && !on_origin()) {
            smp::submit_to(_cpu, [v = std::move(_value)] () mutable {
                auto local(std::move(v));
            });
        }
    }
    element_type& operator*() const { return *_value; }
    element_type* operator->() const { return &*_value; }
    operator bool() const { return static_cast<bool>(_value); }
    foreign_ptr& operator=(foreign_ptr&& other) = default;
};

template <typename T>
foreign_ptr<T> make_foreign(T ptr) {
    return foreign_ptr<T>(std::move(ptr));
}

#endif /* DISTRIBUTED_HH_ */
