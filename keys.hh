/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#pragma once

#include "schema.hh"
#include "bytes.hh"
#include "types.hh"

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

class partition_key;
class clustering_key;
class clustering_key_prefix;

// Abstracts serialized compound, managed by compound_type.
template <typename TopLevel>
class compound_wrapper {
protected:
    bytes _bytes;
protected:
    compound_wrapper(bytes&& b) : _bytes(std::move(b)) {}

    static inline const auto& get_compound_type(const schema& s) {
        return TopLevel::get_compound_type(s);
    }
public:
    static TopLevel make_empty(const schema& s) {
        std::vector<bytes> v;
        v.resize(get_compound_type(s)->types().size());
        return from_exploded(s, v);
    }

    static TopLevel from_exploded(const schema& s, const std::vector<bytes>& v) {
        return TopLevel::from_bytes(get_compound_type(s)->serialize_value(v));
    }

    static TopLevel from_exploded(const schema& s, std::vector<bytes>&& v) {
        return TopLevel::from_bytes(get_compound_type(s)->serialize_value(std::move(v)));
    }

    // We don't allow optional values, but provide this method as an efficient adaptor
    static TopLevel from_optional_exploded(const schema& s, const std::vector<bytes_opt>& v) {
        return TopLevel::from_bytes(get_compound_type(s)->serialize_optionals(v));
    }

    static TopLevel from_deeply_exploded(const schema& s, const std::vector<boost::any>& v) {
        return TopLevel::from_bytes(get_compound_type(s)->serialize_value_deep(v));
    }

    static TopLevel from_single_value(const schema& s, bytes v) {
        return TopLevel::from_bytes(get_compound_type(s)->serialize_single(std::move(v)));
    }

    // FIXME: return views
    std::vector<bytes> explode(const schema& s) const {
        return get_compound_type(s)->deserialize_value(_bytes);
    }

    struct less_compare {
        typename TopLevel::compound _t;
        less_compare(const schema& s) : _t(get_compound_type(s)) {}
        bool operator()(const TopLevel& k1, const TopLevel& k2) const {
            return _t->less(k1, k2);
        }
    };

    struct hashing {
        typename TopLevel::compound _t;
        hashing(const schema& s) : _t(get_compound_type(s)) {}
        size_t operator()(const TopLevel& o) const {
            return _t->hash(o);
        }
    };

    struct equality {
        typename TopLevel::compound _t;
        equality(const schema& s) : _t(get_compound_type(s)) {}
        bool operator()(const TopLevel& o1, const TopLevel& o2) const {
            return _t->equal(o1, o2);
        }
    };

    bool equal(const schema& s, const TopLevel& other) const {
        return get_compound_type(s)->equal(*this, other);
    }

    operator bytes_view() const {
        return _bytes;
    }

    // begin() and end() return iterators over components of this compound. The iterator yields a bytes_view to the component.
    auto begin(const schema& s) const {
        return get_compound_type(s)->begin(_bytes);
    }

    // See begin()
    auto end(const schema& s) const {
        return get_compound_type(s)->end(_bytes);
    }

    bytes_view get_component(const schema& s, size_t idx) const {
        auto it = begin(s);
        std::advance(it, idx);
        return *it;
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

template <typename TopLevel, typename PrefixTopLevel>
class prefixable_full_compound : public compound_wrapper<TopLevel> {
    using base = compound_wrapper<TopLevel>;
protected:
    prefixable_full_compound(bytes&& b) : base(std::move(b)) {}
public:
    using prefix_view_type = prefix_view_on_full_compound<TopLevel, PrefixTopLevel>;

    bool is_prefixed_by(const schema& s, const PrefixTopLevel& prefix) const {
        auto t = base::get_compound_type(s);
        auto prefix_type = PrefixTopLevel::get_compound_type(s);
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

    auto prefix_view(const schema& s, unsigned prefix_len) const {
        return prefix_view_type(s, *this, prefix_len);
    }
};

template <typename TopLevel, typename FullTopLevel>
class prefix_compound_wrapper : public compound_wrapper<TopLevel> {
    using base = compound_wrapper<TopLevel>;
protected:
    prefix_compound_wrapper(bytes&& b) : base(std::move(b)) {}
public:
    bool is_full(const schema& s) const {
        return TopLevel::get_compound_type(s)->is_full(base::_bytes);
    }

    // Can be called only if is_full()
    FullTopLevel to_full(const schema& s) const {
        return FullTopLevel::from_exploded(s, base::explode(s));
    }

    bool is_prefixed_by(const schema& s, const TopLevel& prefix) const {
        auto t = base::get_compound_type(s);
        return ::is_prefixed_by(t->types().begin(),
            t->begin(*this), t->end(*this),
            t->begin(prefix), t->end(prefix),
            equal);
    }
};

class partition_key : public compound_wrapper<partition_key> {
public:
    partition_key(bytes&& b) : compound_wrapper<partition_key>(std::move(b)) {}
public:
    using compound = lw_shared_ptr<compound_type<allow_prefixes::no>>;

    static partition_key from_bytes(bytes b) {
        return partition_key(std::move(b));
    }

    static const compound& get_compound_type(const schema& s) {
        return s.partition_key_type();
    }

    friend std::ostream& operator<<(std::ostream& out, const partition_key& pk);
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

class clustering_key : public prefixable_full_compound<clustering_key, clustering_key_prefix> {
public:
    clustering_key(bytes&& b) : prefixable_full_compound<clustering_key, clustering_key_prefix>(std::move(b)) {}
public:
    using compound = lw_shared_ptr<compound_type<allow_prefixes::no>>;

    static clustering_key from_bytes(bytes b) {
        return clustering_key(std::move(b));
    }

    static const compound& get_compound_type(const schema& s) {
        return s.clustering_key_type();
    }

    static clustering_key from_clustering_prefix(const schema& s, const exploded_clustering_prefix& prefix) {
        assert(prefix.is_full(s));
        return from_exploded(s, prefix.components());
    }

    friend std::ostream& operator<<(std::ostream& out, const clustering_key& ck);
};

class clustering_key_prefix : public prefix_compound_wrapper<clustering_key_prefix, clustering_key> {
    clustering_key_prefix(bytes&& b) : prefix_compound_wrapper<clustering_key_prefix, clustering_key>(std::move(b)) {}
public:
    using compound = lw_shared_ptr<compound_type<allow_prefixes::yes>>;

    static clustering_key_prefix from_bytes(bytes b) {
        return clustering_key_prefix(std::move(b));
    }

    static const compound& get_compound_type(const schema& s) {
        return s.clustering_key_prefix_type();
    }

    static clustering_key_prefix from_clustering_prefix(const schema& s, const exploded_clustering_prefix& prefix) {
        return from_exploded(s, prefix.components());
    }

    friend std::ostream& operator<<(std::ostream& out, const clustering_key_prefix& ckp);
};
