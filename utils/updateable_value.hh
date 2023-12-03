/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */



#pragma once

#include <seastar/core/future.hh>
#include <vector>
#include <functional>
#include "observable.hh"
#include "serialized_action.hh"
#include "seastarx.hh"

namespace utils {

// This file contains two templates, updateable_value_source<T> and updateable_value<T>.
//
// The two are analogous to T and const T& respectively, with the following additional
// functionality:
//
//  - updateable_value contains a copy of T, so it can be accessed without indirection
//  - updateable_value and updateable_value_source track each other, so if they move,
//    the references are updated
//  - an observe() function is provided (to both) that can be used to attach a callback
//    that is called whenever the value changes

template <typename T>
class updateable_value_source;

class updateable_value_source_base;

// Base class for updateable_value<T>, containing functionality for tracking
// the update source. Used to reduce template bloat and not meant to be used
// directly.
class updateable_value_base {
protected:
    const updateable_value_source_base* _source = nullptr;
public:
    updateable_value_base() = default;
    explicit updateable_value_base(const updateable_value_source_base& source);
    ~updateable_value_base();
    updateable_value_base(const updateable_value_base&);
    updateable_value_base& operator=(const updateable_value_base&);
    updateable_value_base(updateable_value_base&&) noexcept;
    updateable_value_base& operator=(updateable_value_base&&) noexcept;
    updateable_value_base& operator=(std::nullptr_t);

    friend class updateable_value_source_base;
};


// A T that can be updated at runtime; uses updateable_value_base to track
// the source as the object is moved or copied. Copying across shards is supported
// unless #7316 is still open
template <typename T>
class updateable_value : public updateable_value_base {
    T _value = {};
private:
    const updateable_value_source<T>* source() const;
public:
    updateable_value() = default;
    explicit updateable_value(T value) : _value(std::move(value)) {}
    explicit updateable_value(const updateable_value_source<T>& source);
    updateable_value(const updateable_value& v);
    updateable_value& operator=(T value);
    updateable_value& operator=(const updateable_value&);
    updateable_value(updateable_value&&) noexcept;
    updateable_value& operator=(updateable_value&&) noexcept;
    const T& operator()() const { return _value; }
    operator const T& () const { return _value; }
    const T& get() const { return _value; }
    observer<T> observe(std::function<void (const T&)> callback) const;

    friend class updateable_value_source_base;
    template <typename U>
    friend class updateable_value_source;
};

// Contains the mechanisms to track updateable_value_base.  Used to reduce template
// bloat and not meant to be used directly.
class updateable_value_source_base {
protected:
    // This class contains two different types of state: values and
    // references to updateable_value_base. We consider adding and removing
    // such references const operations since they don't change the logical
    // state of the object (they don't allow changing the carried value).
    mutable std::vector<updateable_value_base*> _refs; // all connected updateable_values on this shard
    void for_each_ref(std::function<void (updateable_value_base* ref)> func);
protected:
    ~updateable_value_source_base();
    void add_ref(updateable_value_base* ref) const;
    void del_ref(updateable_value_base* ref) const;
    void update_ref(updateable_value_base* old_ref, updateable_value_base* new_ref) const;

    friend class updateable_value_base;
};

template <typename T>
class updateable_value_source : public updateable_value_source_base {
    T _value;
    mutable observable<T> _updater;
    void for_each_ref(std::function<void (updateable_value<T>*)> func) {
        updateable_value_source_base::for_each_ref([func = std::move(func)] (updateable_value_base* ref) {
            func(static_cast<updateable_value<T>*>(ref));
        });
    };
private:
    void add_ref(updateable_value<T>* ref) const {
        updateable_value_source_base::add_ref(ref);
    }
    void del_ref(updateable_value<T>* ref) const {
        updateable_value_source_base::del_ref(ref);
    }
    void update_ref(updateable_value<T>* old_ref, updateable_value<T>* new_ref) const {
        updateable_value_source_base::update_ref(old_ref, new_ref);
    }
public:
    explicit updateable_value_source(T value = T{})
            : _value(std::move(value)) {}
    updateable_value_source(const updateable_value_source& x) : updateable_value_source(x.get()) {
        // We can't copy x's _refs and therefore also _updater. So this is an imperfect copy.
        // This copy constructor therefore breaks updates made to the original copy; it only
        // exists because unit tests copy configs like mad.
    }
    void set(T value) {
        if (value == _value) {
            return;
        }
        _value = std::move(value);
        for_each_ref([&] (updateable_value<T>* ref) {
            ref->_value = _value;
        });
        _updater(_value);
    }
    const T& get() const {
        return _value;
    }
    const T& operator()() const {
        return _value;
    }
    observable<T>& as_observable() const {
        return _updater;
    }
    observer<T> observe(std::function<void (const T&)> callback) const {
        return _updater.observe(std::move(callback));
    }

    friend class updateable_value_base;
};

template <typename T>
updateable_value<T>::updateable_value(const updateable_value_source<T>& source)
        : updateable_value_base(source)
        , _value(source.get()) {
}

template <typename T>
updateable_value<T>::updateable_value(const updateable_value& v) : updateable_value_base(v), _value(v._value) {
}

template <typename T>
updateable_value<T>& updateable_value<T>::operator=(T value) {
    updateable_value_base::operator=(nullptr);
    _value = std::move(value);
    return *this;
}

template <typename T>
updateable_value<T>& updateable_value<T>::operator=(const updateable_value& v) {
    if (this != &v) {
        // Copy early to trigger exceptions, later move
        auto new_val = v._value;
        updateable_value_base::operator=(v);
        _value = std::move(new_val);
    }
    return *this;
}

template <typename T>
updateable_value<T>::updateable_value(updateable_value&& v) noexcept
        : updateable_value_base(v)
        , _value(std::move(v._value)) {
}

template <typename T>
updateable_value<T>& updateable_value<T>::operator=(updateable_value&& v) noexcept {
    if (this != &v) {
        updateable_value_base::operator=(std::move(v));
        _value = std::move(v._value);
    }
    return *this;
}

template <typename T>
inline
const updateable_value_source<T>*
updateable_value<T>::source() const {
    return static_cast<const updateable_value_source<T>*>(_source);
}

template <typename T>
observer<T>
updateable_value<T>::observe(std::function<void (const T&)> callback) const {
    auto* src = source();
    return src ? src->observe(std::move(callback)) : dummy_observer<T>();
}

// Automatically updates a value from a utils::updateable_value
// Where they can be of different types.
// An optional transform function can provide an additional transformation
// when updating the value, like multiplying it by a factor for unit conversion,
// for example.
template <typename ValueType, typename UpdateableValueType>
class transforming_value_updater {
    ValueType& _value;
    utils::updateable_value<UpdateableValueType> _updateable_value;
    serialized_action _updater;
    utils::observer<UpdateableValueType> _observer;

public:
    transforming_value_updater(ValueType& value, utils::updateable_value<UpdateableValueType> updateable_value,
            std::function<ValueType (UpdateableValueType)> transform = [] (UpdateableValueType uv) { return static_cast<ValueType>(uv); })
        : _value(value)
        , _updateable_value(std::move(updateable_value))
        , _updater([this, transform = std::move(transform)] {
                _value = transform(_updateable_value());
                return make_ready_future<>();
          })
        , _observer(_updateable_value.observe(_updater.make_observer()))
    {}
};

}
