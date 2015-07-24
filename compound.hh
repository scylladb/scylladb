/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#pragma once

#include "types.hh"
#include <iostream>
#include <algorithm>
#include <vector>
#include <boost/range/iterator_range.hpp>
#include "utils/serialization.hh"
#include "unimplemented.hh"

// value_traits is meant to abstract away whether we are working on 'bytes'
// elements or 'bytes_opt' elements. We don't support optional values, but
// there are some generic layers which use this code which provide us with
// data in that format. In order to avoid allocation and rewriting that data
// into a new vector just to throw it away soon after that, we accept that
// format too.

template <typename T>
struct value_traits {
    static const T& unwrap(const T& t) { return t; }
};

template<>
struct value_traits<bytes_opt> {
    static const bytes& unwrap(const bytes_opt& t) {
        assert(t);
        return *t;
    }
};

enum class allow_prefixes { no, yes };

template<allow_prefixes AllowPrefixes = allow_prefixes::no>
class compound_type final {
private:
    const std::vector<data_type> _types;
    const bool _byte_order_equal;
    const bool _byte_order_comparable;
    const bool _is_reversed;
public:
    static constexpr bool is_prefixable = AllowPrefixes == allow_prefixes::yes;
    using prefix_type = compound_type<allow_prefixes::yes>;
    using value_type = std::vector<bytes>;

    compound_type(std::vector<data_type> types)
        : _types(std::move(types))
        , _byte_order_equal(std::all_of(_types.begin(), _types.end(), [] (auto t) {
                return t->is_byte_order_equal();
            }))
        , _byte_order_comparable(_types.size() == 1 && _types[0]->is_byte_order_comparable())
        , _is_reversed(_types.size() == 1 && _types[0]->is_reversed())
    { }

    compound_type(compound_type&&) = default;

    auto const& types() const {
        return _types;
    }

    bool is_singular() const {
        return _types.size() == 1;
    }

    prefix_type as_prefix() {
        return prefix_type(_types);
    }

    /*
     * Format:
     *   <len(value1)><value1><len(value2)><value2>...<len(value_n-1)><value_n-1>(len(value_n))?<value_n>
     *
     * For non-prefixable compounds, the value corresponding to the last component of types doesn't
     * have its length encoded, its length is deduced from the input range.
     *
     * serialize_value() and serialize_optionals() for single element rely on the fact that for a single-element
     * compounds their serialized form is equal to the serialized form of the component.
     */
    template<typename Wrapped>
    void serialize_value(const std::vector<Wrapped>& values, bytes::iterator& out) {
        if (AllowPrefixes == allow_prefixes::yes) {
            assert(values.size() <= _types.size());
        } else {
            assert(values.size() == _types.size());
        }

        size_t n_left = _types.size();
        for (auto&& wrapped : values) {
            auto&& val = value_traits<Wrapped>::unwrap(wrapped);
            assert(val.size() <= std::numeric_limits<uint16_t>::max());
            if (--n_left || AllowPrefixes == allow_prefixes::yes) {
                write<uint16_t>(out, uint16_t(val.size()));
            }
            out = std::copy(val.begin(), val.end(), out);
        }
    }
    template <typename Wrapped>
    size_t serialized_size(const std::vector<Wrapped>& values) {
        size_t len = 0;
        size_t n_left = _types.size();
        for (auto&& wrapped : values) {
            auto&& val = value_traits<Wrapped>::unwrap(wrapped);
            assert(val.size() <= std::numeric_limits<uint16_t>::max());
            if (--n_left || AllowPrefixes == allow_prefixes::yes) {
                len += sizeof(uint16_t);
            }
            len += val.size();
        }
        return len;
    }
    bytes serialize_single(bytes&& v) {
        if (AllowPrefixes == allow_prefixes::no) {
            assert(_types.size() == 1);
            return std::move(v);
        } else {
            // FIXME: Optimize
            std::vector<bytes> vec;
            vec.reserve(1);
            vec.emplace_back(std::move(v));
            return ::serialize_value(*this, vec);
        }
    }
    bytes serialize_value(const std::vector<bytes>& values) {
        return ::serialize_value(*this, values);
    }
    bytes serialize_value(std::vector<bytes>&& values) {
        if (AllowPrefixes == allow_prefixes::no && _types.size() == 1 && values.size() == 1) {
            return std::move(values[0]);
        }
        return ::serialize_value(*this, values);
    }
    bytes serialize_optionals(const std::vector<bytes_opt>& values) {
        return ::serialize_value(*this, values);
    }
    bytes serialize_optionals(std::vector<bytes_opt>&& values) {
        if (AllowPrefixes == allow_prefixes::no && _types.size() == 1 && values.size() == 1) {
            assert(values[0]);
            return std::move(*values[0]);
        }
        return ::serialize_value(*this, values);
    }
    bytes serialize_value_deep(const std::vector<boost::any>& values) {
        // TODO: Optimize
        std::vector<bytes> partial;
        partial.reserve(values.size());
        auto i = _types.begin();
        for (auto&& component : values) {
            assert(i != _types.end());
            partial.push_back((*i++)->decompose(component));
        }
        return serialize_value(partial);
    }
    bytes decompose_value(const value_type& values) {
        return ::serialize_value(*this, values);
    }
    class iterator : public std::iterator<std::input_iterator_tag, bytes_view> {
    private:
        ssize_t _types_left;
        bytes_view _v;
        value_type _current;
    private:
        void read_current() {
            if (_types_left == 0) {
                if (!_v.empty()) {
                    throw marshal_exception();
                }
                _v = bytes_view(nullptr, 0);
                return;
            }
            --_types_left;
            uint16_t len;
            if (_types_left == 0 && AllowPrefixes == allow_prefixes::no) {
                len = _v.size();
            } else {
                if (_v.empty()) {
                    if (AllowPrefixes == allow_prefixes::yes) {
                        _v = bytes_view(nullptr, 0);
                        return;
                    } else {
                        throw marshal_exception();
                    }
                }
                len = read_simple<uint16_t>(_v);
                if (_v.size() < len) {
                    throw marshal_exception();
                }
            }
            _current = bytes_view(_v.begin(), len);
            _v.remove_prefix(len);
        }
    public:
        struct end_iterator_tag {};
        iterator(const compound_type& t, const bytes_view& v) : _types_left(t._types.size()), _v(v) {
            read_current();
        }
        iterator(end_iterator_tag, const bytes_view& v) : _v(nullptr, 0) {}
        iterator& operator++() {
            read_current();
            return *this;
        }
        iterator operator++(int) {
            iterator i(*this);
            ++(*this);
            return i;
        }
        const value_type& operator*() const { return _current; }
        const value_type* operator->() const { return &_current; }
        bool operator!=(const iterator& i) const { return _v.begin() != i._v.begin(); }
        bool operator==(const iterator& i) const { return _v.begin() == i._v.begin(); }
    };
    iterator begin(const bytes_view& v) const {
        return iterator(*this, v);
    }
    iterator end(const bytes_view& v) const {
        return iterator(typename iterator::end_iterator_tag(), v);
    }
    boost::iterator_range<iterator> components(const bytes_view& v) const {
        return { begin(v), end(v) };
    }
    auto iter_items(const bytes_view& v) {
        return boost::iterator_range<iterator>(begin(v), end(v));
    }
    value_type deserialize_value(bytes_view v) {
        std::vector<bytes> result;
        result.reserve(_types.size());
        std::transform(begin(v), end(v), std::back_inserter(result), [] (auto&& v) {
            return bytes(v.begin(), v.end());
        });
        return result;
    }
    bool less(bytes_view b1, bytes_view b2) {
        return compare(b1, b2) < 0;
    }
    size_t hash(bytes_view v) {
        if (_byte_order_equal) {
            return std::hash<bytes_view>()(v);
        }
        auto t = _types.begin();
        size_t h = 0;
        for (auto&& value : iter_items(v)) {
            h ^= (*t)->hash(value);
            ++t;
        }
        return h;
    }
    int compare(bytes_view b1, bytes_view b2) {
        if (_byte_order_comparable) {
            if (_is_reversed) {
                return compare_unsigned(b2, b1);
            } else {
                return compare_unsigned(b1, b2);
            }
        }
        return lexicographical_tri_compare(_types.begin(), _types.end(),
            begin(b1), end(b1), begin(b2), end(b2), [] (auto&& type, auto&& v1, auto&& v2) {
                return type->compare(v1, v2);
            });
    }
    bytes from_string(sstring_view s) {
        throw std::runtime_error("not implemented");
    }
    sstring to_string(const bytes& b) {
        throw std::runtime_error("not implemented");
    }
    // Retruns true iff given prefix has no missing components
    bool is_full(bytes_view v) const {
        assert(AllowPrefixes == allow_prefixes::yes);
        return std::distance(begin(v), end(v)) == (ssize_t)_types.size();
    }
    void validate(bytes_view v) {
        // FIXME: implement
        warn(unimplemented::cause::VALIDATION);
    }
    bool equal(bytes_view v1, bytes_view v2) {
        if (_byte_order_equal) {
            return compare_unsigned(v1, v2) == 0;
        }
        // FIXME: call equal() on each component
        return compare(v1, v2) == 0;
    }
};

using compound_prefix = compound_type<allow_prefixes::yes>;
