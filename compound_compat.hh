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

#include <boost/range/algorithm/copy.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include "compound.hh"
#include "schema.hh"
#include "sstables/version.hh"

//
// This header provides adaptors between the representation used by our compound_type<>
// and representation used by Origin.
//
// For single-component keys the legacy representation is equivalent
// to the only component's serialized form. For composite keys it the following
// (See org.apache.cassandra.db.marshal.CompositeType):
//
//   <representation> ::= ( <component> )+
//   <component>      ::= <length> <value> <EOC>
//   <length>         ::= <uint16_t>
//   <EOC>            ::= <uint8_t>
//
//  <value> is component's value in serialized form. <EOC> is always 0 for partition key.
//

// Given a representation serialized using @CompoundType, provides a view on the
// representation of the same components as they would be serialized by Origin.
//
// The view is exposed in a form of a byte range. For example of use see to_legacy() function.
template <typename CompoundType>
class legacy_compound_view {
    static_assert(!CompoundType::is_prefixable, "Legacy view not defined for prefixes");
    CompoundType& _type;
    bytes_view _packed;
public:
    legacy_compound_view(CompoundType& c, bytes_view packed)
        : _type(c)
        , _packed(packed)
    { }

    class iterator : public std::iterator<std::input_iterator_tag, bytes::value_type> {
        bool _singular;
        // Offset within virtual output space of a component.
        //
        // Offset: -2             -1             0  ...  LEN-1 LEN
        // Field:  [ length MSB ] [ length LSB ] [   VALUE   ] [ EOC ]
        //
        int32_t _offset;
        typename CompoundType::iterator _i;
    public:
        struct end_tag {};

        iterator(const legacy_compound_view& v)
            : _singular(v._type.is_singular())
            , _offset(_singular ? 0 : -2)
            , _i(v._type.begin(v._packed))
        { }

        iterator(const legacy_compound_view& v, end_tag)
            : _offset(-2)
            , _i(v._type.end(v._packed))
        { }

        value_type operator*() const {
            int32_t component_size = _i->size();
            if (_offset == -2) {
                return (component_size >> 8) & 0xff;
            } else if (_offset == -1) {
                return component_size & 0xff;
            } else if (_offset < component_size) {
                return (*_i)[_offset];
            } else { // _offset == component_size
                return 0; // EOC field
            }
        }

        iterator& operator++() {
            auto component_size = (int32_t) _i->size();
            if (_offset < component_size
                // When _singular, we skip the EOC byte.
                && (!_singular || _offset != (component_size - 1)))
            {
                ++_offset;
            } else {
                ++_i;
                _offset = -2;
            }
            return *this;
        }

        bool operator==(const iterator& other) const {
            return _offset == other._offset && other._i == _i;
        }

        bool operator!=(const iterator& other) const {
            return !(*this == other);
        }
    };

    // A trichotomic comparator defined on @CompoundType representations which
    // orders them according to lexicographical ordering of their corresponding
    // legacy representations.
    //
    //   tri_comparator(t)(k1, k2)
    //
    // ...is equivalent to:
    //
    //   compare_unsigned(to_legacy(t, k1), to_legacy(t, k2))
    //
    // ...but more efficient.
    //
    struct tri_comparator {
        const CompoundType& _type;

        tri_comparator(const CompoundType& type)
            : _type(type)
        { }

        // @k1 and @k2 must be serialized using @type, which was passed to the constructor.
        int operator()(bytes_view k1, bytes_view k2) const {
            if (_type.is_singular()) {
                return compare_unsigned(*_type.begin(k1), *_type.begin(k2));
            }
            return lexicographical_tri_compare(
                _type.begin(k1), _type.end(k1),
                _type.begin(k2), _type.end(k2),
                [] (const bytes_view& c1, const bytes_view& c2) -> int {
                    if (c1.size() != c2.size()) {
                        return c1.size() < c2.size() ? -1 : 1;
                    }
                    return memcmp(c1.begin(), c2.begin(), c1.size());
                });
        }
    };

    // Equivalent to std::distance(begin(), end()), but computes faster
    size_t size() const {
        if (_type.is_singular()) {
            return _type.begin(_packed)->size();
        }
        size_t s = 0;
        for (auto&& component : _type.components(_packed)) {
            s += 2 /* length field */ + component.size() + 1 /* EOC */;
        }
        return s;
    }

    iterator begin() const {
        return iterator(*this);
    }

    iterator end() const {
        return iterator(*this, typename iterator::end_tag());
    }
};

// Converts compound_type<> representation to legacy representation
// @packed is assumed to be serialized using supplied @type.
template <typename CompoundType>
static inline
bytes to_legacy(CompoundType& type, bytes_view packed) {
    legacy_compound_view<CompoundType> lv(type, packed);
    bytes legacy_form(bytes::initialized_later(), lv.size());
    std::copy(lv.begin(), lv.end(), legacy_form.begin());
    return legacy_form;
}

class composite_view;

// Represents a value serialized according to Origin's CompositeType.
// If is_compound is true, then the value is one or more components encoded as:
//
//   <representation> ::= ( <component> )+
//   <component>      ::= <length> <value> <EOC>
//   <length>         ::= <uint16_t>
//   <EOC>            ::= <uint8_t>
//
// If false, then it encodes a single value, without a prefix length or a suffix EOC.
class composite final {
    bytes _bytes;
    bool _is_compound;
public:
    composite(bytes&& b, bool is_compound)
            : _bytes(std::move(b))
            , _is_compound(is_compound)
    { }

    explicit composite(bytes&& b)
            : _bytes(std::move(b))
            , _is_compound(true)
    { }

    composite()
            : _bytes()
            , _is_compound(true)
    { }

    using size_type = uint16_t;
    using eoc_type = int8_t;

    /*
     * The 'end-of-component' byte should always be 0 for actual column name.
     * However, it can set to 1 for query bounds. This allows to query for the
     * equivalent of 'give me the full range'. That is, if a slice query is:
     *   start = <3><"foo".getBytes()><0>
     *   end   = <3><"foo".getBytes()><1>
     * then we'll return *all* the columns whose first component is "foo".
     * If for a component, the 'end-of-component' is != 0, there should not be any
     * following component. The end-of-component can also be -1 to allow
     * non-inclusive query. For instance:
     *   end = <3><"foo".getBytes()><-1>
     * allows to query everything that is smaller than <3><"foo".getBytes()>, but
     * not <3><"foo".getBytes()> itself.
     */
    enum class eoc : eoc_type {
        start = -1,
        none = 0,
        end = 1
    };

    using component = std::pair<bytes, eoc>;
    using component_view = std::pair<bytes_view, eoc>;
private:
    template<typename Value, typename = std::enable_if_t<!std::is_same<const data_value, std::decay_t<Value>>::value>>
    static size_t size(const Value& val) {
        return val.size();
    }
    static size_t size(const data_value& val) {
        return val.serialized_size();
    }
    template<typename Value, typename = std::enable_if_t<!std::is_same<data_value, std::decay_t<Value>>::value>>
    static void write_value(Value&& val, bytes::iterator& out) {
        out = std::copy(val.begin(), val.end(), out);
    }
    static void write_value(const data_value& val, bytes::iterator& out) {
        val.serialize(out);
    }
    template<typename RangeOfSerializedComponents>
    static void serialize_value(RangeOfSerializedComponents&& values, bytes::iterator& out, bool is_compound) {
        if (!is_compound) {
            auto it = values.begin();
            write_value(std::forward<decltype(*it)>(*it), out);
            return;
        }

        for (auto&& val : values) {
            write<size_type>(out, static_cast<size_type>(size(val)));
            write_value(std::forward<decltype(val)>(val), out);
            // Range tombstones are not keys. For collections, only frozen
            // values can be keys. Therefore, for as long as it is safe to
            // assume that this code will be used to create keys, it is safe
            // to assume the trailing byte is always zero.
            write<eoc_type>(out, eoc_type(eoc::none));
        }
    }
    template <typename RangeOfSerializedComponents>
    static size_t serialized_size(RangeOfSerializedComponents&& values, bool is_compound) {
        size_t len = 0;
        auto it = values.begin();
        if (it != values.end()) {
            // CQL3 uses a specific prefix (0xFFFF) to encode "static columns"
            // (CASSANDRA-6561). This does mean the maximum size of the first component of a
            // composite is 65534, not 65535 (or we wouldn't be able to detect if the first 2
            // bytes is the static prefix or not).
            auto value_size = size(*it);
            if (value_size > static_cast<size_type>(std::numeric_limits<size_type>::max() - uint8_t(is_compound))) {
                throw std::runtime_error(sprint("First component size too large: %d > %d", value_size, std::numeric_limits<size_type>::max() - is_compound));
            }
            if (!is_compound) {
                return value_size;
            }
            len += sizeof(size_type) + value_size + sizeof(eoc_type);
            ++it;
        }
        for ( ; it != values.end(); ++it) {
            auto value_size = size(*it);
            if (value_size > std::numeric_limits<size_type>::max()) {
                throw std::runtime_error(sprint("Component size too large: %d > %d", value_size, std::numeric_limits<size_type>::max()));
            }
            len += sizeof(size_type) + value_size + sizeof(eoc_type);
        }
        return len;
    }
public:
    template <typename Describer>
    auto describe_type(sstables::sstable_version_types v, Describer f) const {
        return f(const_cast<bytes&>(_bytes));
    }

    // marker is ignored if !is_compound
    template<typename RangeOfSerializedComponents>
    static composite serialize_value(RangeOfSerializedComponents&& values, bool is_compound = true, eoc marker = eoc::none) {
        auto size = serialized_size(values, is_compound);
        bytes b(bytes::initialized_later(), size);
        auto i = b.begin();
        serialize_value(std::forward<decltype(values)>(values), i, is_compound);
        if (is_compound && !b.empty()) {
            b.back() = eoc_type(marker);
        }
        return composite(std::move(b), is_compound);
    }

    template<typename RangeOfSerializedComponents>
    static composite serialize_static(const schema& s, RangeOfSerializedComponents&& values) {
        // FIXME: Optimize
        auto b = bytes(size_t(2), bytes::value_type(0xff));
        std::vector<bytes_view> sv(s.clustering_key_size());
        b += composite::serialize_value(boost::range::join(sv, std::forward<RangeOfSerializedComponents>(values)), true).release_bytes();
        return composite(std::move(b));
    }

    static eoc to_eoc(int8_t eoc_byte) {
        return eoc_byte == 0 ? eoc::none : (eoc_byte < 0 ? eoc::start : eoc::end);
    }

    class iterator : public std::iterator<std::input_iterator_tag, const component_view> {
        bytes_view _v;
        component_view _current;
    private:
        void read_current() {
            size_type len;
            {
                if (_v.empty()) {
                    _v = bytes_view(nullptr, 0);
                    return;
                }
                len = read_simple<size_type>(_v);
                if (_v.size() < len) {
                    throw_with_backtrace<marshal_exception>(sprint("composite iterator - not enough bytes, expected %d, got %d", len, _v.size()));
                }
            }
            auto value = bytes_view(_v.begin(), len);
            _v.remove_prefix(len);
            _current = component_view(std::move(value), to_eoc(read_simple<eoc_type>(_v)));
        }
    public:
        struct end_iterator_tag {};

        iterator(const bytes_view& v, bool is_compound, bool is_static)
                : _v(v) {
            if (is_static) {
                _v.remove_prefix(2);
            }
            if (is_compound) {
                read_current();
            } else {
                _current = component_view(_v, eoc::none);
                _v.remove_prefix(_v.size());
            }
        }

        iterator(end_iterator_tag) : _v(nullptr, 0) {}

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

    iterator begin() const {
        return iterator(_bytes, _is_compound, is_static());
    }

    iterator end() const {
        return iterator(iterator::end_iterator_tag());
    }

    boost::iterator_range<iterator> components() const & {
        return { begin(), end() };
    }

    auto values() const & {
        return components() | boost::adaptors::transformed([](auto&& c) { return c.first; });
    }

    std::vector<component> components() const && {
        std::vector<component> result;
        std::transform(begin(), end(), std::back_inserter(result), [](auto&& p) {
            return component(bytes(p.first.begin(), p.first.end()), p.second);
        });
        return result;
    }

    std::vector<bytes> values() const && {
        std::vector<bytes> result;
        boost::copy(components() | boost::adaptors::transformed([](auto&& c) { return to_bytes(c.first); }), std::back_inserter(result));
        return result;
    }

    const bytes& get_bytes() const {
        return _bytes;
    }

    bytes release_bytes() && {
        return std::move(_bytes);
    }

    size_t size() const {
        return _bytes.size();
    }

    bool empty() const {
        return _bytes.empty();
    }

    static bool is_static(bytes_view bytes, bool is_compound) {
        return is_compound && bytes.size() > 2 && (bytes[0] & bytes[1] & 0xff) == 0xff;
    }

    bool is_static() const {
        return is_static(_bytes, _is_compound);
    }

    bool is_compound() const {
        return _is_compound;
    }

    template <typename ClusteringElement>
    static composite from_clustering_element(const schema& s, const ClusteringElement& ce) {
        return serialize_value(ce.components(s), s.is_compound());
    }

    static composite from_exploded(const std::vector<bytes_view>& v, bool is_compound, eoc marker = eoc::none) {
        if (v.size() == 0) {
            return composite(bytes(size_t(1), bytes::value_type(marker)), is_compound);
        }
        return serialize_value(v, is_compound, marker);
    }

    static composite static_prefix(const schema& s) {
        return serialize_static(s, std::vector<bytes_view>());
    }

    explicit operator bytes_view() const {
        return _bytes;
    }

    template <typename Component>
    friend inline std::ostream& operator<<(std::ostream& os, const std::pair<Component, eoc>& c) {
        return os << "{value=" << c.first << "; eoc=" << sprint("0x%02x", eoc_type(c.second) & 0xff) << "}";
    }

    friend std::ostream& operator<<(std::ostream& os, const composite& v);

    struct tri_compare {
        const std::vector<data_type>& _types;
        tri_compare(const std::vector<data_type>& types) : _types(types) {}
        int operator()(const composite&, const composite&) const;
        int operator()(composite_view, composite_view) const;
    };
};

class composite_view final {
    bytes_view _bytes;
    bool _is_compound;
public:
    composite_view(bytes_view b, bool is_compound = true)
            : _bytes(b)
            , _is_compound(is_compound)
    { }

    composite_view(const composite& c)
            : composite_view(static_cast<bytes_view>(c), c.is_compound())
    { }

    composite_view()
            : _bytes(nullptr, 0)
            , _is_compound(true)
    { }

    std::vector<bytes_view> explode() const {
        if (!_is_compound) {
            return { _bytes };
        }

        std::vector<bytes_view> ret;
        ret.reserve(8);
        for (auto it = begin(), e = end(); it != e; ) {
            ret.push_back(it->first);
            auto marker = it->second;
            ++it;
            if (it != e && marker != composite::eoc::none) {
                throw runtime_exception(sprint("non-zero component divider found (%d) mid", sprint("0x%02x", composite::eoc_type(marker) & 0xff)));
            }
        }
        return ret;
    }

    composite::iterator begin() const {
        return composite::iterator(_bytes, _is_compound, is_static());
    }

    composite::iterator end() const {
        return composite::iterator(composite::iterator::end_iterator_tag());
    }

    boost::iterator_range<composite::iterator> components() const {
        return { begin(), end() };
    }

    composite::eoc last_eoc() const {
        if (!_is_compound || _bytes.empty()) {
            return composite::eoc::none;
        }
        bytes_view v(_bytes);
        v.remove_prefix(v.size() - 1);
        return composite::to_eoc(read_simple<composite::eoc_type>(v));
    }

    auto values() const {
        return components() | boost::adaptors::transformed([](auto&& c) { return c.first; });
    }

    size_t size() const {
        return _bytes.size();
    }

    bool empty() const {
        return _bytes.empty();
    }

    bool is_static() const {
        return composite::is_static(_bytes, _is_compound);
    }

    explicit operator bytes_view() const {
        return _bytes;
    }

    bool operator==(const composite_view& k) const { return k._bytes == _bytes && k._is_compound == _is_compound; }
    bool operator!=(const composite_view& k) const { return !(k == *this); }

    friend inline std::ostream& operator<<(std::ostream& os, composite_view v) {
        return os << "{" << ::join(", ", v.components()) << ", compound=" << v._is_compound << ", static=" << v.is_static() << "}";
    }
};

inline
std::ostream& operator<<(std::ostream& os, const composite& v) {
    return os << composite_view(v);
}

inline
int composite::tri_compare::operator()(const composite& v1, const composite& v2) const {
    return (*this)(composite_view(v1), composite_view(v2));
}

inline
int composite::tri_compare::operator()(composite_view v1, composite_view v2) const {
    // See org.apache.cassandra.db.composites.AbstractCType#compare
    if (v1.empty()) {
        return v2.empty() ? 0 : -1;
    }
    if (v2.empty()) {
        return 1;
    }
    if (v1.is_static() != v2.is_static()) {
        return v1.is_static() ? -1 : 1;
    }
    auto a_values = v1.components();
    auto b_values = v2.components();
    auto cmp = [&](const data_type& t, component_view c1, component_view c2) {
        // First by value, then by EOC
        auto r = t->compare(c1.first, c2.first);
        if (r) {
            return r;
        }
        return static_cast<int>(c1.second) - static_cast<int>(c2.second);
    };
    return lexicographical_tri_compare(_types.begin(), _types.end(),
        a_values.begin(), a_values.end(),
        b_values.begin(), b_values.end(),
        cmp);
}
