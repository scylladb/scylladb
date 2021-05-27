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

#include "types.hh"
#include <iosfwd>
#include <algorithm>
#include <vector>
#include <boost/range/iterator_range.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include "utils/serialization.hh"
#include <seastar/util/backtrace.hh>
#include "cql_serialization_format.hh"

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
    using size_type = uint16_t;

    compound_type(std::vector<data_type> types)
        : _types(std::move(types))
        , _byte_order_equal(std::all_of(_types.begin(), _types.end(), [] (const auto& t) {
                return t->is_byte_order_equal();
            }))
        , _byte_order_comparable(false)
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
private:
    /*
     * Format:
     *   <len(value1)><value1><len(value2)><value2>...<len(value_n)><value_n>
     *
     */
    template<typename RangeOfSerializedComponents, FragmentedMutableView Out>
    static void serialize_value(RangeOfSerializedComponents&& values, Out out) {
        for (auto&& val : values) {
            using val_type = std::remove_cvref_t<decltype(val)>;
            if constexpr (FragmentedView<val_type>) {
                assert(val.size_bytes() <= std::numeric_limits<size_type>::max());
                write<size_type>(out, size_type(val.size_bytes()));
                write_fragmented(out, val);
            } else if constexpr (std::same_as<val_type, managed_bytes>) {
                assert(val.size() <= std::numeric_limits<size_type>::max());
                write<size_type>(out, size_type(val.size()));
                write_fragmented(out, managed_bytes_view(val));
            } else {
                assert(val.size() <= std::numeric_limits<size_type>::max());
                write<size_type>(out, size_type(val.size()));
                write_fragmented(out, single_fragmented_view(val));
            }
        }
    }
    template <typename RangeOfSerializedComponents>
    static size_t serialized_size(RangeOfSerializedComponents&& values) {
        size_t len = 0;
        for (auto&& val : values) {
            using val_type = std::remove_cvref_t<decltype(val)>;
            if constexpr (FragmentedView<val_type>) {
                len += sizeof(size_type) + val.size_bytes();
            } else {
                len += sizeof(size_type) + val.size();
            }
        }
        return len;
    }
public:
    managed_bytes serialize_single(const managed_bytes& v) const {
        return serialize_value(boost::make_iterator_range(&v, 1+&v));
    }
    managed_bytes serialize_single(const bytes& v) const {
        return serialize_value(boost::make_iterator_range(&v, 1+&v));
    }
    template<typename RangeOfSerializedComponents>
    static managed_bytes serialize_value(RangeOfSerializedComponents&& values) {
        auto size = serialized_size(values);
        if (size > std::numeric_limits<size_type>::max()) {
            throw std::runtime_error(format("Key size too large: {:d} > {:d}", size, std::numeric_limits<size_type>::max()));
        }
        managed_bytes b(managed_bytes::initialized_later(), size);
        serialize_value(values, managed_bytes_mutable_view(b));
        return b;
    }
    template<typename T>
    static managed_bytes serialize_value(std::initializer_list<T> values) {
        return serialize_value(boost::make_iterator_range(values.begin(), values.end()));
    }
    managed_bytes serialize_optionals(const std::vector<bytes_opt>& values) const {
        return serialize_value(values | boost::adaptors::transformed([] (const bytes_opt& bo) -> bytes_view {
            if (!bo) {
                throw std::logic_error("attempted to create key component from empty optional");
            }
            return *bo;
        }));
    }
    managed_bytes serialize_optionals(const std::vector<managed_bytes_opt>& values) const {
        return serialize_value(values | boost::adaptors::transformed([] (const managed_bytes_opt& bo) -> managed_bytes_view {
            if (!bo) {
                throw std::logic_error("attempted to create key component from empty optional");
            }
            return managed_bytes_view(*bo);
        }));
    }
    managed_bytes serialize_value_deep(const std::vector<data_value>& values) const {
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
    managed_bytes decompose_value(const value_type& values) const {
        return serialize_value(values);
    }
    class iterator {
    public:
        using iterator_category = std::input_iterator_tag;
        using value_type = const managed_bytes_view;
        using difference_type = std::ptrdiff_t;
        using pointer = const value_type*;
        using reference = const value_type&;
    private:
        managed_bytes_view _v;
        managed_bytes_view _current;
        size_t _remaining = 0;
    private:
        void read_current() {
            _remaining = _v.size_bytes();
            size_type len;
            {
                if (_v.empty()) {
                    return;
                }
                len = read_simple<size_type>(_v);
                if (_v.size() < len) {
                    throw_with_backtrace<marshal_exception>(format("compound_type iterator - not enough bytes, expected {:d}, got {:d}", len, _v.size()));
                }
            }
            _current = _v.prefix(len);
            _v.remove_prefix(_current.size_bytes());
        }
    public:
        struct end_iterator_tag {};
        iterator(const managed_bytes_view& v) : _v(v) {
            read_current();
        }
        iterator(end_iterator_tag, const managed_bytes_view& v) : _v() {}
        iterator() {}
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
        bool operator==(const iterator& i) const { return _remaining == i._remaining; }
    };
    static iterator begin(managed_bytes_view v) {
        return iterator(v);
    }
    static iterator end(managed_bytes_view v) {
        return iterator(typename iterator::end_iterator_tag(), v);
    }
    static boost::iterator_range<iterator> components(managed_bytes_view v) {
        return { begin(v), end(v) };
    }
    value_type deserialize_value(managed_bytes_view v) const {
        std::vector<bytes> result;
        result.reserve(_types.size());
        std::transform(begin(v), end(v), std::back_inserter(result), [] (auto&& v) {
            return to_bytes(v);
        });
        return result;
    }
    bool less(managed_bytes_view b1, managed_bytes_view b2) const {
        return with_linearized(b1, [&] (bytes_view bv1) {
            return with_linearized(b2, [&] (bytes_view bv2) {
                return less(bv1, bv2);
            });
        });
    }
    bool less(bytes_view b1, bytes_view b2) const {
        return compare(b1, b2) < 0;
    }
    size_t hash(managed_bytes_view v) const{
        return with_linearized(v, [&] (bytes_view v) {
            return hash(v);
        });
    }
    size_t hash(bytes_view v) const {
        if (_byte_order_equal) {
            return std::hash<bytes_view>()(v);
        }
        auto t = _types.begin();
        size_t h = 0;
        for (auto&& value : components(v)) {
            h ^= (*t)->hash(value);
            ++t;
        }
        return h;
    }
    int compare(managed_bytes_view b1, managed_bytes_view b2) const {
        return with_linearized(b1, [&] (bytes_view bv1) {
            return with_linearized(b2, [&] (bytes_view bv2) {
                return compare(bv1, bv2);
            });
        });
    }
    int compare(bytes_view b1, bytes_view b2) const {
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
    // Retruns true iff given prefix has no missing components
    bool is_full(managed_bytes_view v) const {
        assert(AllowPrefixes == allow_prefixes::yes);
        return std::distance(begin(v), end(v)) == (ssize_t)_types.size();
    }
    bool is_empty(managed_bytes_view v) const {
        return v.empty();
    }
    bool is_empty(const managed_bytes& v) const {
        return v.empty();
    }
    bool is_empty(bytes_view v) const {
        return begin(v) == end(v);
    }
    void validate(managed_bytes_view v) const {
        std::vector<managed_bytes_view> values(begin(v), end(v));
        if (AllowPrefixes == allow_prefixes::no && values.size() < _types.size()) {
            throw marshal_exception(fmt::format("compound::validate(): non-prefixable compound cannot be a prefix"));
        }
        if (values.size() > _types.size()) {
            throw marshal_exception(fmt::format("compound::validate(): cannot have more values than types, have {} values but only {} types",
                        values.size(), _types.size()));
        }
        for (size_t i = 0; i != values.size(); ++i) {
            //FIXME: is it safe to assume internal serialization-format format?
            _types[i]->validate(values[i], cql_serialization_format::internal());
        }
    }
    bool equal(managed_bytes_view v1, managed_bytes_view v2) const {
        return with_linearized(v1, [&] (bytes_view bv1) {
            return with_linearized(v2, [&] (bytes_view bv2) {
                return equal(bv1, bv2);
            });
        });
    }
    bool equal(bytes_view v1, bytes_view v2) const {
        if (_byte_order_equal) {
            return compare_unsigned(v1, v2) == 0;
        }
        // FIXME: call equal() on each component
        return compare(v1, v2) == 0;
    }
};

using compound_prefix = compound_type<allow_prefixes::yes>;
