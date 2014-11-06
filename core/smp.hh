/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef SMP_HH_
#define SMP_HH_

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

    // Stop @Service by destroying the instances started by start().
    // The return value becomes ready when all instances have been
    // destroyed.
    future<> stop();

    // Invoke a method on all instances of @Service.
    // The return value becomes ready when all instances have processed
    // the message.
    template <typename... Args>
    future<> invoke_on_all(future<> (Service::*func)(Args...), Args... args);

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
                return smp::submit_to(c++, [inst, func, args = std::move(args)] () mutable {
                    return apply([inst, func] (Args&&... args) mutable {
                        return (inst->*func)(std::forward<Args>(args)...);
                    }, std::move(args));
                });
            }, std::forward<Reducer>(r));
    }

    // Invoke a method on a specific instance of @Service.
    // The return value (which must be a future) contains the future
    // returned by @Service.
    template <typename Ret, typename... FuncArgs, typename... Args>
    std::enable_if_t<is_future<Ret>::value, Ret>
    invoke_on(unsigned id, Ret (Service::*func)(FuncArgs...), Args&&... args) {
        return smp::submit_to(id, [inst = _instances[id], func, args = std::make_tuple(std::forward<Args>(args)...)] () mutable {
            return apply([inst, func] (Args&&... args) mutable {
                return (inst->*func)(std::forward<Args>(args)...);
            }, std::move(args));
        });
    }

    // Invoke a method on a specific instance of @Service.
    template <typename Ret, typename... FuncArgs, typename... Args>
    std::enable_if_t<!is_future<Ret>::value && !std::is_same<Ret, void>::value, future<Ret>>
    invoke_on(unsigned id, Ret (Service::*func)(FuncArgs...), Args&&... args) {
        return smp::submit_to(id, [inst = _instances[id], func, args = std::make_tuple(std::forward<Args>(args)...)] () mutable {
                return apply([inst, func] (Args&&... args) mutable {
                    return make_ready_future<Ret>((inst->*func)(std::forward<Args>(args)...));
                }, std::move(args));
            });
    }
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
            return smp::submit_to(c++, [&inst, args = std::move(args)] () mutable {
                inst = apply([] (Args&&... args) {
                    return new Service(std::forward<Args>(args)...);
                }, std::move(args));
            });
    });
}

template <typename Service>
future<>
distributed<Service>::stop() {
    unsigned c = 0;
    return parallel_for_each(_instances.begin(), _instances.end(), [&c] (Service*& inst) mutable {
        return smp::submit_to(c++, [inst] () mutable {
            return inst->stop().then([&inst] () mutable {
                delete inst;
                inst = nullptr;
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

}

#endif /* SMP_HH_ */
