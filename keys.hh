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

// Abstracts serialized tuple, managed by tuple_type.
template <typename TopLevel>
class tuple_wrapper {
protected:
    bytes _bytes;
protected:
    tuple_wrapper(bytes&& b) : _bytes(std::move(b)) {}

    static inline auto get_tuple_type(const schema& s) {
        return TopLevel::get_tuple_type(s);
    }
public:
    static TopLevel make_empty(const schema& s) {
        std::vector<bytes> v;
        v.resize(get_tuple_type(s)->types().size());
        return from_exploded(s, v);
    }

    static TopLevel from_exploded(const schema& s, const std::vector<bytes>& v) {
        return TopLevel::from_bytes(get_tuple_type(s)->serialize_value(v));
    }

    static TopLevel from_exploded(const schema& s, std::vector<bytes>&& v) {
        return TopLevel::from_bytes(get_tuple_type(s)->serialize_value(std::move(v)));
    }

    // We don't allow optional values, but provide this method as an efficient adaptor
    static TopLevel from_optional_exploded(const schema& s, const std::vector<bytes_opt>& v) {
        return TopLevel::from_bytes(get_tuple_type(s)->serialize_optionals(v));
    }

    static TopLevel from_deeply_exploded(const schema& s, const std::vector<boost::any>& v) {
        return TopLevel::from_bytes(get_tuple_type(s)->serialize_value_deep(v));
    }

    static TopLevel from_single_value(const schema& s, bytes v) {
        return TopLevel::from_bytes(get_tuple_type(s)->serialize_single(std::move(v)));
    }

    // FIXME: return views
    std::vector<bytes> explode(const schema& s) const {
        return get_tuple_type(s)->deserialize_value(_bytes);
    }

    struct less_compare {
        typename TopLevel::tuple _t;
        less_compare(const schema& s) : _t(get_tuple_type(s)) {}
        bool operator()(const TopLevel& k1, const TopLevel& k2) const {
            return _t->less(k1, k2);
        }
    };

    struct hashing {
        typename TopLevel::tuple _t;
        hashing(const schema& s) : _t(get_tuple_type(s)) {}
        size_t operator()(const TopLevel& o) const {
            return _t->hash(o);
        }
    };

    struct equality {
        typename TopLevel::tuple _t;
        equality(const schema& s) : _t(get_tuple_type(s)) {}
        bool operator()(const TopLevel& o1, const TopLevel& o2) const {
            return _t->equal(o1, o2);
        }
    };

    bool equal(const schema& s, const TopLevel& other) const {
        return get_tuple_type(s)->equal(*this, other);
    }

    operator bytes_view() const {
        return _bytes;
    }
};

template <typename TopLevel, typename PrefixTopLevel>
class prefix_view_on_full_tuple {
public:
    using iterator = typename tuple_type<false>::iterator;
private:
    bytes_view _b;
    unsigned _prefix_len;
    iterator _begin;
    iterator _end;
public:
    prefix_view_on_full_tuple(const schema& s, bytes_view b, unsigned prefix_len)
        : _b(b)
        , _prefix_len(prefix_len)
        , _begin(TopLevel::get_tuple_type(s)->begin(_b))
        , _end(_begin)
    {
        std::advance(_end, prefix_len);
    }

    iterator begin() const { return _begin; }
    iterator end() const { return _end; }

    struct less_compare_with_prefix {
        typename PrefixTopLevel::tuple prefix_type;

        less_compare_with_prefix(const schema& s)
            : prefix_type(PrefixTopLevel::get_tuple_type(s))
        { }

        bool operator()(const prefix_view_on_full_tuple& k1, const PrefixTopLevel& k2) const {
            return lexicographical_tri_compare(
                prefix_type->types().begin(), prefix_type->types().end(),
                k1.begin(), k1.end(),
                prefix_type->begin(k2), prefix_type->end(k2),
                tri_compare) < 0;
        }

        bool operator()(const PrefixTopLevel& k1, const prefix_view_on_full_tuple& k2) const {
            return lexicographical_tri_compare(
                prefix_type->types().begin(), prefix_type->types().end(),
                prefix_type->begin(k1), prefix_type->end(k1),
                k2.begin(), k2.end(),
                tri_compare) < 0;
        }
    };
};

template <typename TopLevel, typename PrefixTopLevel>
class prefixable_full_tuple : public tuple_wrapper<TopLevel> {
    using base = tuple_wrapper<TopLevel>;
protected:
    prefixable_full_tuple(bytes&& b) : base(std::move(b)) {}
public:
    using prefix_view_type = prefix_view_on_full_tuple<TopLevel, PrefixTopLevel>;

    bool is_prefixed_by(const schema& s, const PrefixTopLevel& prefix) const {
        auto t = base::get_tuple_type(s);
        auto prefix_type = PrefixTopLevel::get_tuple_type(s);
        return ::is_prefixed_by(t->types().begin(),
            t->begin(*this), t->end(*this),
            prefix_type->begin(prefix), prefix_type->end(prefix),
            ::equal);
    }

    struct less_compare_with_prefix {
        typename PrefixTopLevel::tuple prefix_type;
        typename TopLevel::tuple full_type;

        less_compare_with_prefix(const schema& s)
            : prefix_type(PrefixTopLevel::get_tuple_type(s))
            , full_type(TopLevel::get_tuple_type(s))
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
    // Note: full tuples sorted according to lexicographical ordering are also
    // sorted according to prefix equality ordering.
    struct prefix_equality_less_compare {
        typename PrefixTopLevel::tuple prefix_type;
        typename TopLevel::tuple full_type;

        prefix_equality_less_compare(const schema& s)
            : prefix_type(PrefixTopLevel::get_tuple_type(s))
            , full_type(TopLevel::get_tuple_type(s))
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
class prefix_tuple_wrapper : public tuple_wrapper<TopLevel> {
    using base = tuple_wrapper<TopLevel>;
protected:
    prefix_tuple_wrapper(bytes&& b) : base(std::move(b)) {}
public:
    bool is_full(const schema& s) const {
        return TopLevel::get_tuple_type(s)->is_full(base::_bytes);
    }

    // Can be called only if is_full()
    FullTopLevel to_full(const schema& s) const {
        return FullTopLevel::from_exploded(s, base::explode(s));
    }

    bool is_prefixed_by(const schema& s, const TopLevel& prefix) const {
        auto t = base::get_tuple_type(s);
        return ::is_prefixed_by(t->types().begin(),
            t->begin(*this), t->end(*this),
            t->begin(prefix), t->end(prefix),
            equal);
    }
};

class partition_key : public tuple_wrapper<partition_key> {
public:
    partition_key(bytes&& b) : tuple_wrapper<partition_key>(std::move(b)) {}
public:
    using tuple = lw_shared_ptr<tuple_type<false>>;

    static partition_key from_bytes(bytes b) { return partition_key(std::move(b)); }
    static auto get_tuple_type(const schema& s) { return s.partition_key_type; }
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

class clustering_key : public prefixable_full_tuple<clustering_key, clustering_key_prefix> {
public:
    clustering_key(bytes&& b) : prefixable_full_tuple<clustering_key, clustering_key_prefix>(std::move(b)) {}
public:
    using tuple = lw_shared_ptr<tuple_type<false>>;

    static clustering_key from_bytes(bytes b) { return clustering_key(std::move(b)); }
    static auto get_tuple_type(const schema& s) { return s.clustering_key_type; }

    static clustering_key from_clustering_prefix(const schema& s, const exploded_clustering_prefix& prefix) {
        assert(prefix.is_full(s));
        return from_exploded(s, prefix.components());
    }
};

class clustering_key_prefix : public prefix_tuple_wrapper<clustering_key_prefix, clustering_key> {
    clustering_key_prefix(bytes&& b) : prefix_tuple_wrapper<clustering_key_prefix, clustering_key>(std::move(b)) {}
public:
    using tuple = lw_shared_ptr<tuple_type<true>>;

    static clustering_key_prefix from_bytes(bytes b) { return clustering_key_prefix(std::move(b)); }
    static auto get_tuple_type(const schema& s) { return s.clustering_key_prefix_type; }

    static clustering_key_prefix from_clustering_prefix(const schema& s, const exploded_clustering_prefix& prefix) {
        return from_exploded(s, prefix.components());
    }
};

static inline
std::ostream& operator<<(std::ostream& out, const partition_key& pk) {
    return out << "pk{" << to_hex(pk) << "}";
}

static inline
std::ostream& operator<<(std::ostream& out, const clustering_key& ck) {
    return out << "ck{" << to_hex(ck) << "}";
}

static inline
std::ostream& operator<<(std::ostream& out, const clustering_key_prefix& ckp) {
    return out << "ckp{" << to_hex(ckp) << "}";
}
