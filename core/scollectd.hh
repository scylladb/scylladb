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
 *
 * This work is open source software, licensed under the terms of the
 * BSD license as described in the LICENSE file in the top-level directory.
 */

#ifndef SCOLLECTD_HH_
#define SCOLLECTD_HH_

#include <type_traits>
#include <utility>
#include <functional>
#include <array>
#include <iterator>
#include <stdint.h>
#include <memory>
#include <string>
#include <tuple>
#include <chrono>
#include <boost/program_options.hpp>

#include "future.hh"
#include "net/byteorder.hh"
#include "core/shared_ptr.hh"

/**
 * Implementation of rudimentary collectd data gathering.
 *
 * Usage is hopefully straight forward. Though, feel free to read
 * https://collectd.org/wiki/index.php/Naming_schema
 * for an explanation on the naming model.
 *
 * Typically, you'll add values something like:
 *
 * 		scollectd::type_instance_id typ("<pluginname>", "<instance_name>", "<type_name>", "<instance_name>");
 *		scollectd::add_polled_metric(typ, [<metric var> | scollectd::make_typed(<data_type>, <metric_var>) [, ...]);
 *
 * Where
 * 	<pluginname> would be the overall 'module', e.g. "cpu"
 *  <instance_name> -> optional distinguisher between plugin instances. For cpu, the built-in
 *  scollectd::per_cpu_plugin_instance constant is a good choice, i.e. 0->N cpu.
 *  If there are no instances (e.g. only one), empty constant is appropriate (none)
 *  <type_name> is the 'type' of metric collected, for ex. "usage" (cpu/0/usage)
 *  <type_instance> is a distinguisher for metric parts of the type, e.g. "idle", "user", "kernel"
 *  -> cpu/0/usage/idle | cpu/0/usage/user | cpu/0/usage/kernel
 *
 *  Each type instance can bind an arbitrary number of values, ech representing some aspect in turn of the instance.
 *  The structure and interpretation is up to the producer/consumer
 *
 * There is a single "scollectd" instance per cpu, and values should be bound locally
 * to this cpu. Polling is done at a frequency set in the seastar config (def once per s),
 * and all registered values will be sent via UDP packages to the destination host(s)
 *
 * Note that the tuple { plugin, plugin_instance, type, type_instance } is considered a
 * unique ID for a value registration, so using the same tuple twice will remove the previously
 * registered values.
 *
 * Values can be unregistered at any time, though they must be so on the same thread/cpu
 * as they we're registered. The "registration" achor type provides RAII style value unregistration
 * semantics.
 *
 */

namespace scollectd {

// The value binding data types
enum class data_type : uint8_t {
    COUNTER, // unsigned int 64
    GAUGE, // double
    DERIVE, // signed int 64
    ABSOLUTE, // unsigned int 64
};

// don't use directly. use make_typed.
template<typename T>
struct typed {
    typed(data_type t, T && v)
    : type(t), value(std::forward<T>(v)) {
    }
    data_type type;
    T value;
};

template<typename T>
static inline typed<T> make_typed(data_type type, T&& t) {
    return typed<T>(type, std::forward<T>(t));
}

typedef std::string plugin_id;
typedef std::string plugin_instance_id;
typedef std::string type_id;

class type_instance_id {
public:
    type_instance_id() = default;
    type_instance_id(const plugin_id & p, const plugin_instance_id & pi,
            const type_id & t, const std::string & ti = std::string())
    : _plugin(p), _plugin_instance(pi), _type(t), _type_instance(ti) {
    }
    type_instance_id(type_instance_id &&) = default;
    type_instance_id(const type_instance_id &) = default;

    type_instance_id & operator=(type_instance_id &&) = default;
    type_instance_id & operator=(const type_instance_id &) = default;

    const plugin_id & plugin() const {
        return _plugin;
    }
    const plugin_instance_id & plugin_instance() const {
        return _plugin_instance;
    }
    const type_id & type() const {
        return _type;
    }
    const std::string & type_instance() const {
        return _type_instance;
    }
private:
    plugin_id _plugin;
    plugin_instance_id _plugin_instance;
    type_id _type;
    std::string _type_instance;
};

extern const plugin_instance_id per_cpu_plugin_instance;

void configure(const boost::program_options::variables_map&);
boost::program_options::options_description get_options_description();
void remove_polled_metric(const type_instance_id &);

/**
 * Anchor for polled registration.
 * Iff the registered type is in some way none-persistent,
 * use this as receiver of the reg and ensure it dies before the
 * added value(s).
 *
 * Use:
 * uint64_t v = 0;
 * registration r = add_polled_metric(v);
 * ++r;
 * <scope end, above dies>
 */
struct registration {
    registration() = default;
    registration(const type_instance_id& id)
    : _id(id) {
    }
    registration(type_instance_id&& id)
    : _id(std::move(id)) {
    }
    registration(const registration&) = default;
    registration(registration&&) = default;
    ~registration() {
        unregister();
    }
    registration & operator=(const registration&) = default;
    registration & operator=(registration&&) = default;

    void unregister() {
        remove_polled_metric(_id);
        _id = type_instance_id();
    }
private:
    type_instance_id _id;
};

// lots of template junk to build typed value list tuples
// for registered values.
template<typename T, typename En = void>
struct data_type_for;

template<typename T, typename En = void>
struct is_callable;

template<typename T>
struct is_callable<T,
typename std::enable_if<
!std::is_void<typename std::result_of<T()>::type>::value,
void>::type> : public std::true_type {
};

template<typename T>
struct is_callable<T,
typename std::enable_if<std::is_fundamental<T>::value, void>::type> : public std::false_type {
};

template<typename T>
struct data_type_for<T,
typename std::enable_if<
std::is_integral<T>::value && std::is_unsigned<T>::value,
void>::type> : public std::integral_constant<data_type,
data_type::COUNTER> {
};
template<typename T>
struct data_type_for<T,
typename std::enable_if<
std::is_integral<T>::value && std::is_signed<T>::value, void>::type> : public std::integral_constant<
data_type, data_type::DERIVE> {
};
template<typename T>
struct data_type_for<T,
typename std::enable_if<std::is_floating_point<T>::value, void>::type> : public std::integral_constant<
data_type, data_type::GAUGE> {
};
template<typename T>
struct data_type_for<T,
typename std::enable_if<is_callable<T>::value, void>::type> : public data_type_for<
typename std::result_of<T()>::type> {
};
template<typename T>
struct data_type_for<typed<T>> : public data_type_for<T> {
};

template<typename T>
class value {
public:
    template<typename W>
    struct wrap {
        wrap(const W & v)
        : _v(v) {
        }
        const W & operator()() const {
            return _v;
        }
        const W & _v;
    };

    typedef typename std::remove_reference<T>::type value_type;
    typedef typename std::conditional<
            is_callable<typename std::remove_reference<T>::type>::value,
            value_type, wrap<value_type> >::type stored_type;

    value(const value_type & t)
    : value<T>(data_type_for<value_type>::value, t) {
    }
    value(data_type type, const value_type & t)
    : _type(type), _t(t) {
    }
    uint64_t operator()() const {
        auto v = _t();
        if (_type == data_type::GAUGE) {
            return convert(double(v));
        } else {
            uint64_t u = v;
            return convert(u);
        }
    }
    operator uint64_t() const {
        return (*this)();
    }
    operator data_type() const {
        return _type;
    }
    data_type type() const {
        return _type;
    }
private:
    // not super quick value -> protocol endian 64-bit values.
    template<typename _Iter>
    void bpack(_Iter s, _Iter e, uint64_t v) const {
        while (s != e) {
            *s++ = (v & 0xff);
            v >>= 8;
        }
    }
    template<typename V>
    typename std::enable_if<std::is_integral<V>::value, uint64_t>::type convert(
            V v) const {
        uint64_t i = v;
        // network byte order
        return ntohq(i);
    }
    template<typename V>
    typename std::enable_if<std::is_floating_point<V>::value, uint64_t>::type convert(
            V t) const {
        union {
            uint64_t i;
            double v;
        } v;
        union {
            uint64_t i;
            uint8_t b[8];
        } u;
        v.v = t;
        // intel byte order. could also obviously be faster.
        // could be ignored if we just assume we're le (for now),
        // but this is ok me thinks.
        bpack(std::begin(u.b), std::end(u.b), v.i);
        return u.i;
    }
    ;

    const data_type _type;
    const stored_type _t;
};

template<typename T>
class value<typed<T>> : public value<T> {
public:
    value(const typed<T> & args)
: value<T>(args.type, args.value) {
    }
};

class value_list {
public:
    virtual size_t size() const = 0;

    virtual void types(data_type *) const = 0;
    virtual void values(net::packed<uint64_t> *) const = 0;

    bool empty() const {
        return size() == 0;
    }
};

template<typename ... Args>
class values_impl: public value_list {
public:
    static const size_t num_values = sizeof...(Args);

    values_impl(Args&& ...args)
    : _values(std::forward<Args>(args)...)
    {}

    values_impl(values_impl<Args...>&& a) = default;
    values_impl(const values_impl<Args...>& a) = default;

    size_t size() const override {
        return num_values;
    }
    void types(data_type * p) const override {
        unpack(_values, [p](Args... args) {
            const std::array<data_type, num_values> tmp = { (args)... };
            std::copy(tmp.begin(), tmp.end(), p);
        });
    }
    void values(net::packed<uint64_t> * p) const override {
        unpack(_values, [p](Args... args) {
            std::array<uint64_t, num_values> tmp = { (args)... };
            std::copy(tmp.begin(), tmp.end(), p);
        });
    }
private:
    template<typename _Op>
    void unpack(const std::tuple<Args...>& t, _Op&& op) const {
        do_unpack(t, std::index_sequence_for<Args...> {}, std::forward<_Op>(op));
    }

    template<size_t ...S, typename _Op>
    void do_unpack(const std::tuple<Args...>& t, const std::index_sequence<S...> &, _Op&& op) const {
        op(std::get<S>(t)...);
    }

    std::tuple < Args... > _values;
};

void add_polled(const type_instance_id &, const shared_ptr<value_list> &);

typedef std::function<void()> notify_function;
template<typename... _Args>
static auto make_type_instance(_Args && ... args) -> values_impl < decltype(value<_Args>(std::forward<_Args>(args)))... >
{
    return values_impl<decltype(value<_Args>(std::forward<_Args>(args)))... >
    (value<_Args>(std::forward<_Args>(args))...);
}
template<typename ... _Args>
static type_instance_id add_polled_metric(const plugin_id & plugin,
        const plugin_instance_id & plugin_instance, const type_id & type,
        const std::string & type_instance, _Args&& ... args) {
    return add_polled_metric(
            type_instance_id(plugin, plugin_instance, type, type_instance),
            std::forward<_Args>(args)...);
}
template<typename ... _Args>
static future<> send_explicit_metric(const plugin_id & plugin,
        const plugin_instance_id & plugin_instance, const type_id & type,
        const std::string & type_instance, _Args&& ... args) {
    return send_explicit_metric(
            type_instance_id(plugin, plugin_instance, type, type_instance),
            std::forward<_Args>(args)...);
}
template<typename ... _Args>
static notify_function create_explicit_metric(const plugin_id & plugin,
        const plugin_instance_id & plugin_instance, const type_id & type,
        const std::string & type_instance, _Args&& ... args) {
    return create_explicit_metric(
            type_instance_id(plugin, plugin_instance, type, type_instance),
            std::forward<_Args>(args)...);
}
template<typename ... _Args>
static type_instance_id add_polled_metric(const type_instance_id & id,
        _Args&& ... args) {
    typedef decltype(make_type_instance(std::forward<_Args>(args)...)) impl_type;
    add_polled(id,
            ::make_shared<impl_type>(
                    make_type_instance(std::forward<_Args>(args)...)));
    return id;
}
// "Explicit" metric sends. Sends a single value list as a message.
// Obviously not super efficient either. But maybe someone needs it sometime.
template<typename ... _Args>
static future<> send_explicit_metric(const type_instance_id & id,
        _Args&& ... args) {
    return send_metric(id, make_type_instance(std::forward<_Args>(args)...));
}
template<typename ... _Args>
static notify_function create_explicit_metric(const type_instance_id & id,
        _Args&& ... args) {
    auto list = make_type_instance(std::forward<_Args>(args)...);
    return [id, list=std::move(list)]() {
        send_metric(id, list);
    };
}

// Send a message packet (string)
future<> send_notification(const type_instance_id & id, const std::string & msg);
};

#endif /* SCOLLECTD_HH_ */
