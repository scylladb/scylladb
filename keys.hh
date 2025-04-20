/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "bytes.hh"
#include "types/types.hh"
#include "compound_compat.hh"
#include "utils/managed_bytes.hh"
#include "utils/hashing.hh"
#include "utils/utf8.hh"
#include "replica/database_fwd.hh"
#include "schema/schema_fwd.hh"
#include <compare>
#include <span>

//
// This header defines type system for primary key holders.
//
// We distinguish partition keys and clustering keys. API-wise they are almost
// the same, but they're separate type hierarchies.
//
// Clustering keys are further divided into prefixed and non-prefixed (full).
// Non-prefixed keys always have full component set, as defined by schema.
// Prefixed ones can have any number of trailing components missing. They may
// differ in underlying representation.
//
// The main classes are:
//
//   partition_key           - full partition key
//   clustering_key          - full clustering key
//   clustering_key_prefix   - clustering key prefix
//
// These classes wrap only the minimum information required to store the key
// (the key value itself). Any information which can be inferred from schema
// is not stored. Therefore accessors need to be provided with a pointer to
// schema, from which information about structure is extracted.

// Abstracts a view to serialized compound.
template <typename TopLevelView>
class compound_view_wrapper {
protected:
    managed_bytes_view _bytes;
protected:
    compound_view_wrapper(managed_bytes_view v)
        : _bytes(v)
    { }

    static inline const auto& get_compound_type(const schema& s) {
        return TopLevelView::get_compound_type(s);
    }
public:
    std::vector<bytes> explode(const schema& s) const {
        return get_compound_type(s)->deserialize_value(_bytes);
    }

    managed_bytes_view representation() const {
        return _bytes;
    }

    struct less_compare {
        typename TopLevelView::compound _t;
        less_compare(const schema& s) : _t(get_compound_type(s)) {}
        bool operator()(const TopLevelView& k1, const TopLevelView& k2) const {
            return _t->less(k1.representation(), k2.representation());
        }
    };

    struct tri_compare {
        typename TopLevelView::compound _t;
        tri_compare(const schema &s) : _t(get_compound_type(s)) {}
        std::strong_ordering operator()(const TopLevelView& k1, const TopLevelView& k2) const {
            return _t->compare(k1.representation(), k2.representation());
        }
    };

    struct hashing {
        typename TopLevelView::compound _t;
        hashing(const schema& s) : _t(get_compound_type(s)) {}
        size_t operator()(const TopLevelView& o) const {
            return _t->hash(o.representation());
        }
    };

    struct equality {
        typename TopLevelView::compound _t;
        equality(const schema& s) : _t(get_compound_type(s)) {}
        bool operator()(const TopLevelView& o1, const TopLevelView& o2) const {
            return _t->equal(o1.representation(), o2.representation());
        }
    };

    bool equal(const schema& s, const TopLevelView& other) const {
        return get_compound_type(s)->equal(representation(), other.representation());
    }

    // begin() and end() return iterators over components of this compound. The iterator yields a managed_bytes_view to the component.
    // The iterators satisfy InputIterator concept.
    auto begin() const {
        return TopLevelView::compound::element_type::begin(representation());
    }

    // See begin()
    auto end() const {
        return TopLevelView::compound::element_type::end(representation());
    }

    // begin() and end() return iterators over components of this compound. The iterator yields a managed_bytes_view to the component.
    // The iterators satisfy InputIterator concept.
    auto begin(const schema& s) const {
        return begin();
    }

    // See begin()
    auto end(const schema& s) const {
        return end();
    }

    // Returns a range of managed_bytes_view
    auto components() const {
        return TopLevelView::compound::element_type::components(representation());
    }

    // Returns a range of managed_bytes_view
    auto components(const schema& s) const {
        return components();
    }

    bool is_empty() const {
        return _bytes.empty();
    }

    explicit operator bool() const {
        return !is_empty();
    }

    // For backward compatibility with existing code.
    bool is_empty(const schema& s) const {
        return is_empty();
    }
};

template <typename TopLevel, typename TopLevelView>
class compound_wrapper {
protected:
    managed_bytes _bytes;
protected:
    compound_wrapper(managed_bytes&& b) : _bytes(std::move(b)) {}

    static inline const auto& get_compound_type(const schema& s) {
        return TopLevel::get_compound_type(s);
    }
private:
    static const data_type& get_singular_type(const schema& s) {
        const auto& ct = get_compound_type(s);
        if (!ct->is_singular()) {
            throw std::invalid_argument("compound is not singular");
        }
        return ct->types()[0];
    }
public:
    struct with_schema_wrapper {
        with_schema_wrapper(const schema& s, const TopLevel& key) : s(s), key(key) {}
        const schema& s;
        const TopLevel& key;
    };

    with_schema_wrapper with_schema(const schema& s) const {
        return with_schema_wrapper(s, *static_cast<const TopLevel*>(this));
    }

    static TopLevel make_empty() {
        return from_exploded(std::vector<bytes>());
    }

    static TopLevel make_empty(const schema&) {
        return make_empty();
    }

    template<typename RangeOfSerializedComponents>
    static TopLevel from_exploded(RangeOfSerializedComponents&& v) {
        return TopLevel::from_range(std::forward<RangeOfSerializedComponents>(v));
    }

    static TopLevel from_exploded(const schema& s, const std::vector<bytes>& v) {
        return from_exploded(v);
    }
    static TopLevel from_exploded(const schema& s, const std::vector<managed_bytes>& v) {
        return from_exploded(v);
    }
    static TopLevel from_exploded_view(const std::vector<bytes_view>& v) {
        return from_exploded(v);
    }

    // We don't allow optional values, but provide this method as an efficient adaptor
    static TopLevel from_optional_exploded(const schema& s, std::span<const bytes_opt> v) {
        return TopLevel::from_bytes(get_compound_type(s)->serialize_optionals(v));
    }
    static TopLevel from_optional_exploded(const schema& s, std::span<const managed_bytes_opt> v) {
        return TopLevel::from_bytes(get_compound_type(s)->serialize_optionals(v));
    }

    static TopLevel from_deeply_exploded(const schema& s, const std::vector<data_value>& v) {
        return TopLevel::from_bytes(get_compound_type(s)->serialize_value_deep(v));
    }

    static TopLevel from_single_value(const schema& s, const bytes& v) {
        return TopLevel::from_bytes(get_compound_type(s)->serialize_single(v));
    }

    static TopLevel from_single_value(const schema& s, const managed_bytes& v) {
        return TopLevel::from_bytes(get_compound_type(s)->serialize_single(v));
    }

    template <typename T>
    static
    TopLevel from_singular(const schema& s, const T& v) {
        const auto& type = get_singular_type(s);
        return from_single_value(s, type->decompose(v));
    }

    static TopLevel from_singular_bytes(const schema& s, const bytes& b) {
        get_singular_type(s); // validation
        return from_single_value(s, b);
    }

    TopLevelView view() const {
        return TopLevelView::from_bytes(_bytes);
    }

    operator TopLevelView() const {
        return view();
    }

    // FIXME: return views
    std::vector<bytes> explode(const schema& s) const {
        return get_compound_type(s)->deserialize_value(_bytes);
    }

    std::vector<bytes> explode() const {
        std::vector<bytes> result;
        for (managed_bytes_view c : components()) {
            result.emplace_back(to_bytes(c));
        }
        return result;
    }

    std::vector<managed_bytes> explode_fragmented() const {
        std::vector<managed_bytes> result;
        for (managed_bytes_view c : components()) {
            result.emplace_back(managed_bytes(c));
        }
        return result;
    }

    struct tri_compare {
        typename TopLevel::compound _t;
        tri_compare(const schema& s) : _t(get_compound_type(s)) {}
        std::strong_ordering operator()(const TopLevel& k1, const TopLevel& k2) const {
            return _t->compare(k1.representation(), k2.representation());
        }
        std::strong_ordering operator()(const TopLevelView& k1, const TopLevel& k2) const {
            return _t->compare(k1.representation(), k2.representation());
        }
        std::strong_ordering operator()(const TopLevel& k1, const TopLevelView& k2) const {
            return _t->compare(k1.representation(), k2.representation());
        }
    };

    struct less_compare {
        typename TopLevel::compound _t;
        less_compare(const schema& s) : _t(get_compound_type(s)) {}
        bool operator()(const TopLevel& k1, const TopLevel& k2) const {
            return _t->less(k1.representation(), k2.representation());
        }
        bool operator()(const TopLevelView& k1, const TopLevel& k2) const {
            return _t->less(k1.representation(), k2.representation());
        }
        bool operator()(const TopLevel& k1, const TopLevelView& k2) const {
            return _t->less(k1.representation(), k2.representation());
        }
    };

    struct hashing {
        typename TopLevel::compound _t;
        hashing(const schema& s) : _t(get_compound_type(s)) {}
        size_t operator()(const TopLevel& o) const {
            return _t->hash(o.representation());
        }
        size_t operator()(const TopLevelView& o) const {
            return _t->hash(o.representation());
        }
    };

    struct equality {
        typename TopLevel::compound _t;
        equality(const schema& s) : _t(get_compound_type(s)) {}
        bool operator()(const TopLevel& o1, const TopLevel& o2) const {
            return _t->equal(o1.representation(), o2.representation());
        }
        bool operator()(const TopLevelView& o1, const TopLevel& o2) const {
            return _t->equal(o1.representation(), o2.representation());
        }
        bool operator()(const TopLevel& o1, const TopLevelView& o2) const {
            return _t->equal(o1.representation(), o2.representation());
        }
    };

    bool equal(const schema& s, const TopLevel& other) const {
        return get_compound_type(s)->equal(representation(), other.representation());
    }

    bool equal(const schema& s, const TopLevelView& other) const {
        return get_compound_type(s)->equal(representation(), other.representation());
    }

    operator managed_bytes_view() const
    {
        return _bytes;
    }

    const managed_bytes& representation() const {
        return _bytes;
    }

    // begin() and end() return iterators over components of this compound. The iterator yields a managed_bytes_view to the component.
    // The iterators satisfy InputIterator concept.
    auto begin(const schema& s) const {
        return get_compound_type(s)->begin(_bytes);
    }

    // See begin()
    auto end(const schema& s) const {
        return get_compound_type(s)->end(_bytes);
    }

    bool is_empty() const {
        return _bytes.empty();
    }

    explicit operator bool() const {
        return !is_empty();
    }

    // For backward compatibility with existing code.
    bool is_empty(const schema& s) const {
        return is_empty();
    }

    // Returns a range of managed_bytes_view
    auto components() const {
        return TopLevelView::compound::element_type::components(representation());
    }

    // Returns a range of managed_bytes_view
    auto components(const schema& s) const {
        return components();
    }

    managed_bytes_view get_component(const schema& s, size_t idx) const {
        auto it = begin(s);
        std::advance(it, idx);
        return *it;
    }

    // Returns the number of components of this compound.
    size_t size(const schema& s) const {
        return std::distance(begin(s), end(s));
    }

    size_t minimal_external_memory_usage() const {
        return _bytes.minimal_external_memory_usage();
    }

    size_t external_memory_usage() const noexcept {
        return _bytes.external_memory_usage();
    }

    size_t memory_usage() const noexcept {
        return sizeof(*this) + external_memory_usage();
    }
};

template <typename TopLevel, typename PrefixTopLevel>
class prefix_view_on_full_compound {
public:
    using iterator = typename compound_type<allow_prefixes::no>::iterator;
private:
    bytes_view _b;
    unsigned _prefix_len;
    iterator _begin;
    iterator _end;
public:
    prefix_view_on_full_compound(const schema& s, bytes_view b, unsigned prefix_len)
        : _b(b)
        , _prefix_len(prefix_len)
        , _begin(TopLevel::get_compound_type(s)->begin(_b))
        , _end(_begin)
    {
        std::advance(_end, prefix_len);
    }

    iterator begin() const { return _begin; }
    iterator end() const { return _end; }

    struct less_compare_with_prefix {
        typename PrefixTopLevel::compound prefix_type;

        less_compare_with_prefix(const schema& s)
            : prefix_type(PrefixTopLevel::get_compound_type(s))
        { }

        bool operator()(const prefix_view_on_full_compound& k1, const PrefixTopLevel& k2) const {
            return lexicographical_tri_compare(
                prefix_type->types().begin(), prefix_type->types().end(),
                k1.begin(), k1.end(),
                prefix_type->begin(k2), prefix_type->end(k2),
                tri_compare) < 0;
        }

        bool operator()(const PrefixTopLevel& k1, const prefix_view_on_full_compound& k2) const {
            return lexicographical_tri_compare(
                prefix_type->types().begin(), prefix_type->types().end(),
                prefix_type->begin(k1), prefix_type->end(k1),
                k2.begin(), k2.end(),
                tri_compare) < 0;
        }
    };
};

template <typename TopLevel>
class prefix_view_on_prefix_compound {
public:
    using iterator = typename compound_type<allow_prefixes::yes>::iterator;
private:
    bytes_view _b;
    unsigned _prefix_len;
    iterator _begin;
    iterator _end;
public:
    prefix_view_on_prefix_compound(const schema& s, bytes_view b, unsigned prefix_len)
        : _b(b)
        , _prefix_len(prefix_len)
        , _begin(TopLevel::get_compound_type(s)->begin(_b))
        , _end(_begin)
    {
        std::advance(_end, prefix_len);
    }

    iterator begin() const { return _begin; }
    iterator end() const { return _end; }

    struct less_compare_with_prefix {
        typename TopLevel::compound prefix_type;

        less_compare_with_prefix(const schema& s)
            : prefix_type(TopLevel::get_compound_type(s))
        { }

        bool operator()(const prefix_view_on_prefix_compound& k1, const TopLevel& k2) const {
            return lexicographical_tri_compare(
                prefix_type->types().begin(), prefix_type->types().end(),
                k1.begin(), k1.end(),
                prefix_type->begin(k2), prefix_type->end(k2),
                tri_compare) < 0;
        }

        bool operator()(const TopLevel& k1, const prefix_view_on_prefix_compound& k2) const {
            return lexicographical_tri_compare(
                prefix_type->types().begin(), prefix_type->types().end(),
                prefix_type->begin(k1), prefix_type->end(k1),
                k2.begin(), k2.end(),
                tri_compare) < 0;
        }
    };
};

template <typename TopLevel, typename TopLevelView, typename PrefixTopLevel>
class prefixable_full_compound : public compound_wrapper<TopLevel, TopLevelView> {
    using base = compound_wrapper<TopLevel, TopLevelView>;
protected:
    prefixable_full_compound(bytes&& b) : base(std::move(b)) {}
public:
    using prefix_view_type = prefix_view_on_full_compound<TopLevel, PrefixTopLevel>;

    bool is_prefixed_by(const schema& s, const PrefixTopLevel& prefix) const {
        const auto& t = base::get_compound_type(s);
        const auto& prefix_type = PrefixTopLevel::get_compound_type(s);
        return ::is_prefixed_by(t->types().begin(),
            t->begin(*this), t->end(*this),
            prefix_type->begin(prefix), prefix_type->end(prefix),
            ::equal);
    }

    struct less_compare_with_prefix {
        typename PrefixTopLevel::compound prefix_type;
        typename TopLevel::compound full_type;

        less_compare_with_prefix(const schema& s)
            : prefix_type(PrefixTopLevel::get_compound_type(s))
            , full_type(TopLevel::get_compound_type(s))
        { }

        bool operator()(const TopLevel& k1, const PrefixTopLevel& k2) const {
            return lexicographical_tri_compare(
                prefix_type->types().begin(), prefix_type->types().end(),
                full_type->begin(k1), full_type->end(k1),
                prefix_type->begin(k2), prefix_type->end(k2),
                tri_compare) < 0;
        }

        bool operator()(const PrefixTopLevel& k1, const TopLevel& k2) const {
            return lexicographical_tri_compare(
                prefix_type->types().begin(), prefix_type->types().end(),
                prefix_type->begin(k1), prefix_type->end(k1),
                full_type->begin(k2), full_type->end(k2),
                tri_compare) < 0;
        }
    };

    // In prefix equality two sequences are equal if any of them is a prefix
    // of the other. Otherwise lexicographical ordering is applied.
    // Note: full compounds sorted according to lexicographical ordering are also
    // sorted according to prefix equality ordering.
    struct prefix_equality_less_compare {
        typename PrefixTopLevel::compound prefix_type;
        typename TopLevel::compound full_type;

        prefix_equality_less_compare(const schema& s)
            : prefix_type(PrefixTopLevel::get_compound_type(s))
            , full_type(TopLevel::get_compound_type(s))
        { }

        bool operator()(const TopLevel& k1, const PrefixTopLevel& k2) const {
            return prefix_equality_tri_compare(prefix_type->types().begin(),
                full_type->begin(k1), full_type->end(k1),
                prefix_type->begin(k2), prefix_type->end(k2),
                tri_compare) < 0;
        }

        bool operator()(const PrefixTopLevel& k1, const TopLevel& k2) const {
            return prefix_equality_tri_compare(prefix_type->types().begin(),
                prefix_type->begin(k1), prefix_type->end(k1),
                full_type->begin(k2), full_type->end(k2),
                tri_compare) < 0;
        }
    };

    prefix_view_type prefix_view(const schema& s, unsigned prefix_len) const {
        return { s, this->representation(), prefix_len };
    }
};

template <typename TopLevel, typename FullTopLevel>
class prefix_compound_view_wrapper : public compound_view_wrapper<TopLevel> {
    using base = compound_view_wrapper<TopLevel>;
protected:
    prefix_compound_view_wrapper(managed_bytes_view v)
        : compound_view_wrapper<TopLevel>(v)
    { }

public:
    bool is_full(const schema& s) const {
        return TopLevel::get_compound_type(s)->is_full(base::_bytes);
    }
};

template <typename TopLevel, typename TopLevelView, typename FullTopLevel>
class prefix_compound_wrapper : public compound_wrapper<TopLevel, TopLevelView> {
    using base = compound_wrapper<TopLevel, TopLevelView>;
protected:
    prefix_compound_wrapper(managed_bytes&& b) : base(std::move(b)) {}
public:
    using prefix_view_type = prefix_view_on_prefix_compound<TopLevel>;

    prefix_view_type prefix_view(const schema& s, unsigned prefix_len) const {
        return { s, this->representation(), prefix_len };
    }

    bool is_full(const schema& s) const {
        return TopLevel::get_compound_type(s)->is_full(base::_bytes);
    }

    // Can be called only if is_full()
    FullTopLevel to_full(const schema& s) const {
        return FullTopLevel::from_exploded(s, base::explode(s));
    }

    bool is_prefixed_by(const schema& s, const TopLevel& prefix) const {
        const auto& t = base::get_compound_type(s);
        return ::is_prefixed_by(t->types().begin(),
            t->begin(*this), t->end(*this),
            t->begin(prefix), t->end(prefix),
            equal);
    }

    // In prefix equality two sequences are equal if any of them is a prefix
    // of the other. Otherwise lexicographical ordering is applied.
    // Note: full compounds sorted according to lexicographical ordering are also
    // sorted according to prefix equality ordering.
    struct prefix_equality_less_compare {
        typename TopLevel::compound prefix_type;

        prefix_equality_less_compare(const schema& s)
            : prefix_type(TopLevel::get_compound_type(s))
        { }

        bool operator()(const TopLevel& k1, const TopLevel& k2) const {
            return prefix_equality_tri_compare(prefix_type->types().begin(),
                prefix_type->begin(k1.representation()), prefix_type->end(k1.representation()),
                prefix_type->begin(k2.representation()), prefix_type->end(k2.representation()),
                tri_compare) < 0;
        }
    };

    // See prefix_equality_less_compare.
    struct prefix_equal_tri_compare {
        typename TopLevel::compound prefix_type;

        prefix_equal_tri_compare(const schema& s)
            : prefix_type(TopLevel::get_compound_type(s))
        { }

        std::strong_ordering operator()(const TopLevel& k1, const TopLevel& k2) const {
            return prefix_equality_tri_compare(prefix_type->types().begin(),
                prefix_type->begin(k1.representation()), prefix_type->end(k1.representation()),
                prefix_type->begin(k2.representation()), prefix_type->end(k2.representation()),
                tri_compare);
        }
    };
};

class partition_key_view : public compound_view_wrapper<partition_key_view> {
public:
    using c_type = compound_type<allow_prefixes::no>;
private:
    partition_key_view(managed_bytes_view v)
        : compound_view_wrapper<partition_key_view>(v)
    { }
public:
    using compound = lw_shared_ptr<c_type>;

    static partition_key_view from_bytes(managed_bytes_view v) {
        return { v };
    }

    static const compound& get_compound_type(const schema& s) {
        return s.partition_key_type();
    }

    // Returns key's representation which is compatible with Origin.
    // The result is valid as long as the schema is live.
    const legacy_compound_view<c_type> legacy_form(const schema& s) const;

    // A trichotomic comparator for ordering compatible with Origin.
    std::strong_ordering legacy_tri_compare(const schema& s, partition_key_view o) const;

    // Checks if keys are equal in a way which is compatible with Origin.
    bool legacy_equal(const schema& s, partition_key_view o) const {
        return legacy_tri_compare(s, o) == 0;
    }

    void validate(const schema& s) const {
        return s.partition_key_type()->validate(representation());
    }

    // A trichotomic comparator which orders keys according to their ordering on the ring.
    std::strong_ordering ring_order_tri_compare(const schema& s, partition_key_view o) const;
};

template <>
struct fmt::formatter<partition_key_view> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const partition_key_view& pk, FormatContext& ctx) const {
        return with_linearized(pk.representation(), [&] (bytes_view v) {
            return fmt::format_to(ctx.out(), "pk{{{}}}", fmt_hex(v));
        });
    }
};

class partition_key : public compound_wrapper<partition_key, partition_key_view> {
    explicit partition_key(managed_bytes&& b)
        : compound_wrapper<partition_key, partition_key_view>(std::move(b))
    { }
public:
    using c_type = compound_type<allow_prefixes::no>;

    template<typename RangeOfSerializedComponents>
    static partition_key from_range(RangeOfSerializedComponents&& v) {
        return partition_key(managed_bytes(c_type::serialize_value(std::forward<RangeOfSerializedComponents>(v))));
    }

    /*!
     * \brief create a partition_key from a nodetool style string
     * takes a nodetool style string representation of a partition key and returns a partition_key.
     * With composite keys, columns are concatenate using ':'.
     * For example if a composite key is has two columns (col1, col2) to get the partition key that
     * have col1=val1 and col2=val2 use the string 'val1:val2'
     */
    static partition_key from_nodetool_style_string(const schema_ptr s, const sstring& key);

    partition_key(std::vector<bytes> v)
        : compound_wrapper(managed_bytes(c_type::serialize_value(std::move(v))))
    { }
    partition_key(std::initializer_list<bytes> v) : partition_key(std::vector(v)) {}    

    partition_key(partition_key&& v) = default;
    partition_key(const partition_key& v) = default;
    partition_key(partition_key& v) = default;
    partition_key& operator=(const partition_key&) = default;
    partition_key& operator=(partition_key&) = default;
    partition_key& operator=(partition_key&&) = default;

    partition_key(partition_key_view key)
        : partition_key(managed_bytes(key.representation()))
    { }

    using compound = lw_shared_ptr<c_type>;

    static partition_key from_bytes(managed_bytes_view b) {
        return partition_key(managed_bytes(b));
    }
    static partition_key from_bytes(managed_bytes&& b) {
        return partition_key(std::move(b));
    }
    static partition_key from_bytes(bytes_view b) {
        return partition_key(managed_bytes(b));
    }

    static const compound& get_compound_type(const schema& s) {
        return s.partition_key_type();
    }

    // Returns key's representation which is compatible with Origin.
    // The result is valid as long as the schema is live.
    const legacy_compound_view<c_type> legacy_form(const schema& s) const {
        return view().legacy_form(s);
    }

    // A trichotomic comparator for ordering compatible with Origin.
    std::strong_ordering legacy_tri_compare(const schema& s, const partition_key& o) const {
        return view().legacy_tri_compare(s, o);
    }

    // Checks if keys are equal in a way which is compatible with Origin.
    bool legacy_equal(const schema& s, const partition_key& o) const {
        return view().legacy_equal(s, o);
    }

    void validate(const schema& s) const {
        return s.partition_key_type()->validate(representation());
    }
};

template <>
struct fmt::formatter<partition_key> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const partition_key& pk, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "pk{{{}}}", managed_bytes_view(pk.representation()));
    }
};

namespace detail {

template <typename WithSchemaWrapper, typename FormatContext>
auto format_pk(const WithSchemaWrapper& pk, FormatContext& ctx) {
    const auto& [schema, key] = pk;
    auto type_iterator = key.get_compound_type(schema)->types().begin();
    bool first = true;
    auto out = ctx.out();
    for (auto&& component : key.components(schema)) {
        if (!first) {
            out = fmt::format_to(out, "{}", ":");
        }
        first = false;
        auto key = (*type_iterator++)->to_string(to_bytes(component));
        if (utils::utf8::validate((const uint8_t *) key.data(), key.size())) {
            out = fmt::format_to(out, "{}", key);
        } else {
            out = fmt::format_to(out, "{}", "<non-utf8-key>");
        }
    }
    return out;
}
} // namespace detail

template <>
struct fmt::formatter<partition_key::with_schema_wrapper> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const partition_key::with_schema_wrapper& pk, FormatContext& ctx) const {
        return ::detail::format_pk(pk, ctx);
    }
};

class exploded_clustering_prefix {
    std::vector<bytes> _v;
public:
    exploded_clustering_prefix(std::vector<bytes>&& v) : _v(std::move(v)) {}
    exploded_clustering_prefix() {}
    size_t size() const {
        return _v.size();
    }
    auto const& components() const {
        return _v;
    }
    explicit operator bool() const {
        return !_v.empty();
    }
    bool is_full(const schema& s) const {
        return _v.size() == s.clustering_key_size();
    }
    friend std::ostream& operator<<(std::ostream& os, const exploded_clustering_prefix& ecp);
};

class clustering_key_prefix_view : public prefix_compound_view_wrapper<clustering_key_prefix_view, clustering_key> {
    clustering_key_prefix_view(managed_bytes_view v)
        : prefix_compound_view_wrapper<clustering_key_prefix_view, clustering_key>(v)
    { }
public:
    static clustering_key_prefix_view from_bytes(const managed_bytes& v) {
        return { v };
    }
    static clustering_key_prefix_view from_bytes(managed_bytes_view v) {
        return { v };
    }
    static clustering_key_prefix_view from_bytes(bytes_view v) {
        return { v };
    }

    using compound = lw_shared_ptr<compound_type<allow_prefixes::yes>>;

    static const compound& get_compound_type(const schema& s) {
        return s.clustering_key_prefix_type();
    }

    static clustering_key_prefix_view make_empty() {
        return { bytes_view() };
    }
};

class clustering_key_prefix : public prefix_compound_wrapper<clustering_key_prefix, clustering_key_prefix_view, clustering_key> {
    explicit clustering_key_prefix(managed_bytes&& b)
            : prefix_compound_wrapper<clustering_key_prefix, clustering_key_prefix_view, clustering_key>(std::move(b))
    { }
public:
    template<typename RangeOfSerializedComponents>
    static clustering_key_prefix from_range(RangeOfSerializedComponents&& v) {
        return clustering_key_prefix(compound::element_type::serialize_value(std::forward<RangeOfSerializedComponents>(v)));
    }

    clustering_key_prefix(std::vector<bytes> v)
        : prefix_compound_wrapper(compound::element_type::serialize_value(std::move(v)))
    { }
    clustering_key_prefix(std::vector<managed_bytes> v)
        : prefix_compound_wrapper(compound::element_type::serialize_value(std::move(v)))
    { }
    clustering_key_prefix(std::initializer_list<bytes> v) : clustering_key_prefix(std::vector(v)) {}

    clustering_key_prefix(clustering_key_prefix&& v) = default;
    clustering_key_prefix(const clustering_key_prefix& v) = default;
    clustering_key_prefix(clustering_key_prefix& v) = default;
    clustering_key_prefix& operator=(const clustering_key_prefix&) = default;
    clustering_key_prefix& operator=(clustering_key_prefix&) = default;
    clustering_key_prefix& operator=(clustering_key_prefix&&) = default;

    clustering_key_prefix(clustering_key_prefix_view v)
        : clustering_key_prefix(managed_bytes(v.representation()))
    { }

    using compound = lw_shared_ptr<compound_type<allow_prefixes::yes>>;

    static clustering_key_prefix from_bytes(const managed_bytes& b) { return clustering_key_prefix(managed_bytes(b)); }
    static clustering_key_prefix from_bytes(managed_bytes&& b) { return clustering_key_prefix(std::move(b)); }
    static clustering_key_prefix from_bytes(managed_bytes_view b) { return clustering_key_prefix(managed_bytes(b)); }
    static clustering_key_prefix from_bytes(bytes_view b) {
        return clustering_key_prefix(managed_bytes(b));
    }

    static const compound& get_compound_type(const schema& s) {
        return s.clustering_key_prefix_type();
    }

    static clustering_key_prefix from_clustering_prefix(const schema& s, const exploded_clustering_prefix& prefix) {
        return from_exploded(s, prefix.components());
    }

    /* This function makes the passed clustering key full by filling its
     * missing trailing components with empty values.
     * This is used to represesent clustering keys of rows in compact tables that may be non-full.
     * Returns whether a key wasn't full before the call.
     */
    static bool make_full(const schema& s, clustering_key_prefix& ck) {
        if (!ck.is_full(s)) {
            // TODO: avoid unnecessary copy here
            auto full_ck_size = s.clustering_key_columns().size();
            auto exploded = ck.explode(s);
            exploded.resize(full_ck_size);
            ck = clustering_key_prefix::from_exploded(std::move(exploded));
            return true;
        }
        return false;
    }
};

template <>
struct fmt::formatter<clustering_key_prefix> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const clustering_key_prefix& ckp, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "ckp{{{}}}", managed_bytes_view(ckp.representation()));
    }
};

template <>
struct fmt::formatter<clustering_key_prefix::with_schema_wrapper> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const clustering_key_prefix::with_schema_wrapper& pk, FormatContext& ctx) const {
        return ::detail::format_pk(pk, ctx);
    }
};

template<>
struct appending_hash<partition_key_view> {
    template<typename Hasher>
    void operator()(Hasher& h, const partition_key_view& pk, const schema& s) const {
        for (managed_bytes_view v : pk.components(s)) {
            ::feed_hash(h, v);
        }
    }
};

template<>
struct appending_hash<partition_key> {
    template<typename Hasher>
    void operator()(Hasher& h, const partition_key& pk, const schema& s) const {
        appending_hash<partition_key_view>()(h, pk.view(), s);
    }
};

template<>
struct appending_hash<clustering_key_prefix_view> {
    template<typename Hasher>
    void operator()(Hasher& h, const clustering_key_prefix_view& ck, const schema& s) const {
        for (managed_bytes_view v : ck.components(s)) {
            ::feed_hash(h, v);
        }
    }
};

template<>
struct appending_hash<clustering_key_prefix> {
    template<typename Hasher>
    void operator()(Hasher& h, const clustering_key_prefix& ck, const schema& s) const {
        appending_hash<clustering_key_prefix_view>()(h, ck.view(), s);
    }
};
