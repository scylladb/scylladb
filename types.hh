/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#pragma once

#include <experimental/optional>
#include <boost/any.hpp>
#include <boost/functional/hash.hpp>
#include <iostream>
#include <sstream>

#include "core/sstring.hh"
#include "core/shared_ptr.hh"
#include "utils/UUID.hh"
#include "net/byteorder.hh"
#include "db_clock.hh"
#include "bytes.hh"
#include "log.hh"
#include "atomic_cell.hh"
#include "serialization_format.hh"
#include "tombstone.hh"
#include "to_string.hh"
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/algorithm/for_each.hpp>
#include <boost/range/numeric.hpp>
#include <boost/range/combine.hpp>

class tuple_type_impl;

namespace cql3 {

class cql3_type;
class column_specification;
shared_ptr<cql3_type> make_cql3_tuple_type(shared_ptr<tuple_type_impl> t);

}

// Like std::lexicographical_compare but injects values from shared sequence (types) to the comparator
// Compare is an abstract_type-aware less comparator, which takes the type as first argument.
template <typename TypesIterator, typename InputIt1, typename InputIt2, typename Compare>
bool lexicographical_compare(TypesIterator types, InputIt1 first1, InputIt1 last1,
        InputIt2 first2, InputIt2 last2, Compare comp) {
    while (first1 != last1 && first2 != last2) {
        if (comp(*types, *first1, *first2)) {
            return true;
        }
        if (comp(*types, *first2, *first1)) {
            return false;
        }
        ++first1;
        ++first2;
        ++types;
    }
    return (first1 == last1) && (first2 != last2);
}

// Like std::lexicographical_compare but injects values from shared sequence
// (types) to the comparator. Compare is an abstract_type-aware trichotomic
// comparator, which takes the type as first argument.
//
// A trichotomic comparator returns an integer which is less, equal or greater
// than zero when the first value is respectively smaller, equal or greater
// than the second value.
template <typename TypesIterator, typename InputIt1, typename InputIt2, typename Compare>
int lexicographical_tri_compare(TypesIterator types_first, TypesIterator types_last,
        InputIt1 first1, InputIt1 last1,
        InputIt2 first2, InputIt2 last2,
        Compare comp) {
    while (types_first != types_last && first1 != last1 && first2 != last2) {
        auto c = comp(*types_first, *first1, *first2);
        if (c) {
            return c;
        }
        ++first1;
        ++first2;
        ++types_first;
    }
    bool e1 = first1 == last1;
    bool e2 = first2 == last2;
    if (e1 != e2) {
        return e2 ? 1 : -1;
    }
    return 0;
}

// A trichotomic comparator for prefix equality total ordering.
// In this ordering, two sequences are equal iff any of them is a prefix
// of the another. Otherwise, lexicographical ordering determines the order.
//
// 'comp' is an abstract_type-aware trichotomic comparator, which takes the
// type as first argument.
//
template <typename TypesIterator, typename InputIt1, typename InputIt2, typename Compare>
int prefix_equality_tri_compare(TypesIterator types, InputIt1 first1, InputIt1 last1,
        InputIt2 first2, InputIt2 last2, Compare comp) {
    while (first1 != last1 && first2 != last2) {
        auto c = comp(*types, *first1, *first2);
        if (c) {
            return c;
        }
        ++first1;
        ++first2;
        ++types;
    }
    return 0;
}

// Returns true iff the second sequence is a prefix of the first sequence
// Equality is an abstract_type-aware equality checker which takes the type as first argument.
template <typename TypesIterator, typename InputIt1, typename InputIt2, typename Equality>
bool is_prefixed_by(TypesIterator types, InputIt1 first1, InputIt1 last1,
        InputIt2 first2, InputIt2 last2, Equality equality) {
    while (first1 != last1 && first2 != last2) {
        if (!equality(*types, *first1, *first2)) {
            return false;
        }
        ++first1;
        ++first2;
        ++types;
    }
    return first2 == last2;
}

class marshal_exception : public std::exception {
    sstring _why;
public:
    marshal_exception() : _why("marshalling error") {}
    marshal_exception(sstring why) : _why(sstring("marshaling error: ") + why) {}
    virtual const char* why() const { return _why.c_str(); }
};

struct runtime_exception : public std::exception {
    sstring _why;
public:
    runtime_exception(sstring why) : _why(sstring("runtime error: ") + why) {}
    virtual const char* why() const { return _why.c_str(); }
};

inline int32_t compare_unsigned(bytes_view v1, bytes_view v2) {
    auto n = memcmp(v1.begin(), v2.begin(), std::min(v1.size(), v2.size()));
    if (n) {
        return n;
    }
    return (int32_t) (v1.size() - v2.size());
}

class serialized_compare;

class abstract_type : public enable_shared_from_this<abstract_type> {
    sstring _name;
public:
    abstract_type(sstring name) : _name(name) {}
    virtual ~abstract_type() {}
    virtual void serialize(const boost::any& value, bytes::iterator& out) = 0;
    virtual size_t serialized_size(const boost::any& value) = 0;
    virtual bool less(bytes_view v1, bytes_view v2) = 0;
    // returns a callable that can be called with two byte_views, and calls this->less() on them.
    serialized_compare as_less_comparator();
    virtual size_t hash(bytes_view v) = 0;
    virtual bool equal(bytes_view v1, bytes_view v2) {
        if (is_byte_order_equal()) {
            return compare_unsigned(v1, v2) == 0;
        }
        return compare(v1, v2) == 0;
    }
    virtual int32_t compare(bytes_view v1, bytes_view v2) {
        if (less(v1, v2)) {
            return -1;
        } else if (less(v2, v1)) {
            return 1;
        } else {
            return 0;
        }
    }
    virtual boost::any deserialize(bytes_view v) = 0;
    virtual void validate(bytes_view v) {
        // FIXME
    }
    virtual void validate_collection_member(bytes_view v, const bytes& collection_name) {
        validate(v);
    }
    virtual bool is_compatible_with(abstract_type& previous) {
        return equals(previous);
    }
    /*
     * Types which are wrappers over other types should override this.
     * For example the reversed_type returns the type it is reversing.
     */
    virtual shared_ptr<abstract_type> underlying_type() {
        return shared_from_this();
    }
    /**
     * Returns true if values of the other AbstractType can be read and "reasonably" interpreted by the this
     * AbstractType. Note that this is a weaker version of isCompatibleWith, as it does not require that both type
     * compare values the same way.
     *
     * The restriction on the other type being "reasonably" interpreted is to prevent, for example, IntegerType from
     * being compatible with all other types.  Even though any byte string is a valid IntegerType value, it doesn't
     * necessarily make sense to interpret a UUID or a UTF8 string as an integer.
     *
     * Note that a type should be compatible with at least itself.
     */
    bool is_value_compatible_with(abstract_type& other) {
        return is_value_compatible_with_internal(*other.underlying_type());
    }
    bool equals(const shared_ptr<abstract_type>& other) const {
        return equals(*other);
    }
protected:
    virtual bool equals(const abstract_type& other) const {
        return this == &other;
    }
    /**
     * Needed to handle ReversedType in value-compatibility checks.  Subclasses should implement this instead of
     * is_value_compatible_with().
     */
    virtual bool is_value_compatible_with_internal(abstract_type& other) {
        return is_compatible_with(other);
    }
public:
    virtual boost::any compose(const bytes& v) {
        return deserialize(v);
    }
    bytes decompose(const boost::any& value) {
        bytes b(bytes::initialized_later(), serialized_size(value));
        auto i = b.begin();
        serialize(value, i);
        return b;
    }
    sstring name() const {
        return _name;
    }
    virtual bool is_byte_order_comparable() const {
        return false;
    }

    /**
     * When returns true then equal values have the same byte representation and if byte
     * representation is different, the values are not equal.
     *
     * When returns false, nothing can be inferred.
     */
    virtual bool is_byte_order_equal() const {
        // If we're byte order comparable, then we must also be byte order equal.
        return is_byte_order_comparable();
    }
    virtual sstring get_string(const bytes& b) {
        validate(b);
        return to_string(b);
    }
    virtual sstring to_string(const bytes& b) = 0;
    virtual bytes from_string(sstring_view text) = 0;
    virtual bool is_counter() { return false; }
    virtual bool is_collection() { return false; }
    virtual bool is_multi_cell() { return false; }
    virtual bool is_reversed() { return false; }
    virtual ::shared_ptr<cql3::cql3_type> as_cql3_type() = 0;
    virtual shared_ptr<abstract_type> freeze() { return shared_from_this(); }
    friend class list_type_impl;
};

using data_type = shared_ptr<abstract_type>;
using bytes_view_opt = std::experimental::optional<bytes_view>;

static inline
bool optional_less_compare(data_type t, bytes_view_opt e1, bytes_view_opt e2) {
    if (bool(e1) != bool(e2)) {
        return bool(e2);
    }
    if (!e1) {
        return false;
    }
    return t->less(*e1, *e2);
}

static inline
bool optional_equal(data_type t, bytes_view_opt e1, bytes_view_opt e2) {
    if (bool(e1) != bool(e2)) {
        return false;
    }
    if (!e1) {
        return true;
    }
    return t->equal(*e1, *e2);
}

static inline
bool less_compare(data_type t, bytes_view e1, bytes_view e2) {
    return t->less(e1, e2);
}

static inline
int tri_compare(data_type t, bytes_view e1, bytes_view e2) {
    return t->compare(e1, e2);
}

inline
int
tri_compare_opt(data_type t, bytes_view_opt v1, bytes_view_opt v2) {
    if (!v1 || !v2) {
        return int(bool(v1)) - int(bool(v2));
    } else {
        return tri_compare(std::move(t), *v1, *v2);
    }
}

static inline
bool equal(data_type t, bytes_view e1, bytes_view e2) {
    return t->equal(e1, e2);
}

class collection_type_impl : public abstract_type {
    static thread_local logging::logger _logger;
    shared_ptr<cql3::cql3_type> _cql3_type;
public:
    static constexpr const size_t max_elements = 65535;

    class kind {
        std::function<shared_ptr<cql3::column_specification> (shared_ptr<cql3::column_specification> collection, bool is_key)> _impl;
    public:
        kind(std::function<shared_ptr<cql3::column_specification> (shared_ptr<cql3::column_specification> collection, bool is_key)> impl)
            : _impl(std::move(impl)) {}
        shared_ptr<cql3::column_specification> make_collection_receiver(shared_ptr<cql3::column_specification> collection, bool is_key) const;
        static const kind map;
        static const kind set;
        static const kind list;
    };

    const kind& _kind;

protected:
    explicit collection_type_impl(sstring name, const kind& k)
            : abstract_type(std::move(name)), _kind(k) {}
    virtual sstring cql3_type_name() const = 0;
public:
    // representation of a collection mutation, key/value pairs, value is a mutation itself
    struct mutation {
        tombstone tomb;
        std::vector<std::pair<bytes, atomic_cell>> cells;
    };
    struct mutation_view {
        tombstone tomb;
        std::vector<std::pair<bytes_view, atomic_cell_view>> cells;
        mutation materialize() const;
    };
    virtual data_type name_comparator() = 0;
    virtual data_type value_comparator() = 0;
    shared_ptr<cql3::column_specification> make_collection_receiver(shared_ptr<cql3::column_specification> collection, bool is_key);
    virtual bool is_collection() override { return true; }
    bool is_map() const { return &_kind == &kind::map; }
    std::vector<atomic_cell> enforce_limit(std::vector<atomic_cell>, int version);
    virtual std::vector<bytes> serialized_values(std::vector<atomic_cell> cells) = 0;
    bytes serialize_for_native_protocol(std::vector<atomic_cell> cells, int version);
    virtual bool is_compatible_with(abstract_type& previous) override;
    virtual bool is_compatible_with_frozen(collection_type_impl& previous) = 0;
    virtual bool is_value_compatible_with_frozen(collection_type_impl& previous) = 0;
    virtual shared_ptr<cql3::cql3_type> as_cql3_type() override;
    template <typename BytesViewIterator>
    static bytes pack(BytesViewIterator start, BytesViewIterator finish, int elements, serialization_format sf);
    mutation_view deserialize_mutation_form(collection_mutation::view in);
    bool is_empty(collection_mutation::view in);
    bool is_any_live(collection_mutation::view in, tombstone tomb);
    virtual bytes to_value(mutation_view mut, serialization_format sf) = 0;
    bytes to_value(collection_mutation::view mut, serialization_format sf);
    // FIXME: use iterators?
    collection_mutation::one serialize_mutation_form(const mutation& mut);
    collection_mutation::one serialize_mutation_form(mutation_view mut);
    collection_mutation::one serialize_mutation_form_only_live(mutation_view mut);
    collection_mutation::one merge(collection_mutation::view a, collection_mutation::view b);
    virtual void serialize(const boost::any& value, bytes::iterator& out, serialization_format sf) = 0;
    virtual boost::any deserialize(bytes_view v, serialization_format sf) = 0;
    bytes_opt reserialize(serialization_format from, serialization_format to, bytes_view_opt v);
};

using collection_type = shared_ptr<collection_type_impl>;

template <typename... T>
struct simple_tuple_hash;

template <>
struct simple_tuple_hash<> {
    size_t operator()() const { return 0; }
};

template <typename Arg0, typename... Args>
struct simple_tuple_hash<Arg0, Args...> {
    size_t operator()(const Arg0& arg0, const Args&... args) const {
        size_t h0 = std::hash<Arg0>()(arg0);
        size_t h1 = simple_tuple_hash<Args...>()(args...);
        return h0 ^ ((h1 << 7) | (h1 >> (std::numeric_limits<size_t>::digits - 7)));
    }
};

template <typename InternedType, typename... BaseTypes>
class type_interning_helper {
    using key_type = std::tuple<BaseTypes...>;
    using value_type = shared_ptr<InternedType>;
    struct hash_type {
        size_t operator()(const key_type& k) const {
            return apply(simple_tuple_hash<BaseTypes...>(), k);
        }
    };
    using map_type = std::unordered_map<key_type, value_type, hash_type>;
    static thread_local map_type _instances;
public:
    static shared_ptr<InternedType> get_instance(BaseTypes... keys) {
        auto key = std::make_tuple(keys...);
        auto i = _instances.find(key);
        if (i == _instances.end()) {
            auto v = make_shared<InternedType>(keys...);
            i = _instances.insert(std::make_pair(std::move(key), std::move(v))).first;
        }
        return i->second;
    }
};

template <typename InternedType, typename... BaseTypes>
thread_local typename type_interning_helper<InternedType, BaseTypes...>::map_type
    type_interning_helper<InternedType, BaseTypes...>::_instances;

class map_type_impl final : public collection_type_impl {
    using map_type = shared_ptr<map_type_impl>;
    using intern = type_interning_helper<map_type_impl, data_type, data_type, bool>;
    data_type _keys;
    data_type _values;
    data_type _key_value_pair_type;
    bool _is_multi_cell;
protected:
    virtual sstring cql3_type_name() const override;
public:
    // type returned by deserialize() and expected by serialize
    // does not support mutations/ttl/tombstone - purely for I/O.
    using native_type = std::vector<std::pair<boost::any, boost::any>>;
    static shared_ptr<map_type_impl> get_instance(data_type keys, data_type values, bool is_multi_cell);
    map_type_impl(data_type keys, data_type values, bool is_multi_cell);
    data_type get_keys_type() const { return _keys; }
    data_type get_values_type() const { return _values; }
    virtual data_type name_comparator() override { return _keys; }
    virtual data_type value_comparator() override { return _values; }
    virtual bool is_multi_cell() override { return _is_multi_cell; }
    virtual data_type freeze() override;
    virtual bool is_compatible_with_frozen(collection_type_impl& previous) override;
    virtual bool is_value_compatible_with_frozen(collection_type_impl& previous) override;
    virtual bool less(bytes_view o1, bytes_view o2) override;
    static int32_t compare_maps(data_type keys_comparator, data_type values_comparator,
                        bytes_view o1, bytes_view o2);
    virtual bool is_byte_order_comparable() const override { return false; }
    virtual void serialize(const boost::any& value, bytes::iterator& out) override;
    virtual void serialize(const boost::any& value, bytes::iterator& out, serialization_format sf) override;
    virtual size_t serialized_size(const boost::any& value);
    virtual boost::any deserialize(bytes_view v) override;
    virtual boost::any deserialize(bytes_view v, serialization_format sf) override;
    virtual sstring to_string(const bytes& b) override;
    virtual size_t hash(bytes_view v) override;
    virtual bytes from_string(sstring_view text) override;
    virtual std::vector<bytes> serialized_values(std::vector<atomic_cell> cells) override;
    static bytes serialize_partially_deserialized_form(const std::vector<std::pair<bytes_view, bytes_view>>& v,
            serialization_format sf);
    virtual bytes to_value(mutation_view mut, serialization_format sf) override;
};

using map_type = shared_ptr<map_type_impl>;

class set_type_impl final : public collection_type_impl {
    using set_type = shared_ptr<set_type_impl>;
    using intern = type_interning_helper<set_type_impl, data_type, bool>;
    data_type _elements;
    bool _is_multi_cell;
protected:
    virtual sstring cql3_type_name() const override;
public:
    // type returned by deserialize() and expected by serialize
    // does not support mutations/ttl/tombstone - purely for I/O.
    using native_type = std::vector<boost::any>;
    static set_type get_instance(data_type elements, bool is_multi_cell);
    set_type_impl(data_type elements, bool is_multi_cell);
    data_type get_elements_type() const { return _elements; }
    virtual data_type name_comparator() override { return _elements; }
    virtual data_type value_comparator() override;
    virtual bool is_multi_cell() override { return _is_multi_cell; }
    virtual data_type freeze() override;
    virtual bool is_compatible_with_frozen(collection_type_impl& previous) override;
    virtual bool is_value_compatible_with_frozen(collection_type_impl& previous) override;
    virtual bool less(bytes_view o1, bytes_view o2) override;
    virtual bool is_byte_order_comparable() const override { return _elements->is_byte_order_comparable(); }
    virtual void serialize(const boost::any& value, bytes::iterator& out) override;
    virtual void serialize(const boost::any& value, bytes::iterator& out, serialization_format sf) override;
    virtual size_t serialized_size(const boost::any& value) override;
    virtual boost::any deserialize(bytes_view v) override;
    virtual boost::any deserialize(bytes_view v, serialization_format sf) override;
    virtual sstring to_string(const bytes& b) override;
    virtual size_t hash(bytes_view v) override;
    virtual bytes from_string(sstring_view text) override;
    virtual std::vector<bytes> serialized_values(std::vector<atomic_cell> cells) override;
    virtual bytes to_value(mutation_view mut, serialization_format sf) override;
    bytes serialize_partially_deserialized_form(
            const std::vector<bytes_view>& v, serialization_format sf);

};

using set_type = shared_ptr<set_type_impl>;

class list_type_impl final : public collection_type_impl {
    using list_type = shared_ptr<list_type_impl>;
    using intern = type_interning_helper<list_type_impl, data_type, bool>;
    data_type _elements;
    bool _is_multi_cell;
protected:
    virtual sstring cql3_type_name() const override;
public:
    // type returned by deserialize() and expected by serialize
    // does not support mutations/ttl/tombstone - purely for I/O.
    using native_type = std::vector<boost::any>;
    static list_type get_instance(data_type elements, bool is_multi_cell);
    list_type_impl(data_type elements, bool is_multi_cell);
    data_type get_elements_type() const { return _elements; }
    virtual data_type name_comparator() override;
    virtual data_type value_comparator() override;
    virtual bool is_multi_cell() override { return _is_multi_cell; }
    virtual data_type freeze() override;
    virtual bool is_compatible_with_frozen(collection_type_impl& previous) override;
    virtual bool is_value_compatible_with_frozen(collection_type_impl& previous) override;
    virtual bool less(bytes_view o1, bytes_view o2) override;
    // FIXME: origin doesn't override is_byte_order_comparable().  Why?
    virtual void serialize(const boost::any& value, bytes::iterator& out) override;
    virtual void serialize(const boost::any& value, bytes::iterator& out, serialization_format sf) override;
    virtual size_t serialized_size(const boost::any& value) override;
    virtual boost::any deserialize(bytes_view v) override;
    virtual boost::any deserialize(bytes_view v, serialization_format sf) override;
    virtual sstring to_string(const bytes& b) override;
    virtual size_t hash(bytes_view v) override;
    virtual bytes from_string(sstring_view text) override;
    virtual std::vector<bytes> serialized_values(std::vector<atomic_cell> cells) override;
    virtual bytes to_value(mutation_view mut, serialization_format sf) override;
};

using list_type = shared_ptr<list_type_impl>;

inline
size_t hash_value(const shared_ptr<abstract_type>& x) {
    return std::hash<abstract_type*>()(x.get());
}

template <typename Type>
shared_ptr<abstract_type> data_type_for();

class serialized_compare {
    data_type _type;
public:
    serialized_compare(data_type type) : _type(type) {}
    bool operator()(const bytes& v1, const bytes& v2) const {
        return _type->less(v1, v2);
    }
};

inline
serialized_compare
abstract_type::as_less_comparator() {
    return serialized_compare(shared_from_this());
}

using key_compare = serialized_compare;

// FIXME: add missing types
// Remember to update type_codec in transport/server.cc and cql3/cql3_type.cc
extern thread_local const shared_ptr<abstract_type> int32_type;
extern thread_local const shared_ptr<abstract_type> long_type;
extern thread_local const shared_ptr<abstract_type> ascii_type;
extern thread_local const shared_ptr<abstract_type> bytes_type;
extern thread_local const shared_ptr<abstract_type> utf8_type;
extern thread_local const shared_ptr<abstract_type> boolean_type;
extern thread_local const shared_ptr<abstract_type> date_type;
extern thread_local const shared_ptr<abstract_type> timeuuid_type;
extern thread_local const shared_ptr<abstract_type> timestamp_type;
extern thread_local const shared_ptr<abstract_type> uuid_type;
extern thread_local const shared_ptr<abstract_type> inet_addr_type;
extern thread_local const shared_ptr<abstract_type> float_type;
extern thread_local const shared_ptr<abstract_type> double_type;
extern thread_local const data_type empty_type;

template <>
inline
shared_ptr<abstract_type> data_type_for<int32_t>() {
    return int32_type;
}

template <>
inline
shared_ptr<abstract_type> data_type_for<int64_t>() {
    return long_type;
}

template <>
inline
shared_ptr<abstract_type> data_type_for<sstring>() {
    return utf8_type;
}


namespace std {

template <>
struct hash<shared_ptr<abstract_type>> : boost::hash<shared_ptr<abstract_type>> {
};

}

// FIXME: make more explicit
inline
bytes
to_bytes(const char* x) {
    return bytes(reinterpret_cast<const int8_t*>(x), std::strlen(x));
}

// FIXME: make more explicit
inline
bytes
to_bytes(const std::string& x) {
    return bytes(reinterpret_cast<const int8_t*>(x.data()), x.size());
}

inline
bytes
to_bytes(bytes_view x) {
    return bytes(x.begin(), x.size());
}

// FIXME: make more explicit
inline
bytes
to_bytes(const sstring& x) {
    return bytes(reinterpret_cast<const int8_t*>(x.c_str()), x.size());
}

inline
bytes
to_bytes(const utils::UUID& uuid) {
    struct {
        uint64_t msb;
        uint64_t lsb;
    } tmp = { net::hton(uint64_t(uuid.get_most_significant_bits())),
        net::hton(uint64_t(uuid.get_least_significant_bits())) };
    return bytes(reinterpret_cast<int8_t*>(&tmp), 16);
}

// This follows java.util.Comparator
// FIXME: Choose a better place than database.hh
template <typename T>
struct comparator {
    comparator() = default;
    comparator(std::function<int32_t (T& v1, T& v2)> fn)
        : _compare_fn(std::move(fn))
    { }
    int32_t compare() { return _compare_fn(); }
private:
    std::function<int32_t (T& v1, T& v2)> _compare_fn;
};

inline bool
less_unsigned(bytes_view v1, bytes_view v2) {
    return compare_unsigned(v1, v2) < 0;
}

class serialized_hash {
private:
    data_type _type;
public:
    serialized_hash(data_type type) : _type(type) {}
    size_t operator()(const bytes& v) const {
        return _type->hash(v);
    }
};

class serialized_equal {
private:
    data_type _type;
public:
    serialized_equal(data_type type) : _type(type) {}
    bool operator()(const bytes& v1, const bytes& v2) const {
        return _type->equal(v1, v2);
    }
};

template<typename Type>
static inline
typename Type::value_type deserialize_value(Type& t, bytes_view v) {
    return t.deserialize_value(v);
}

template<typename Type, typename Value>
static inline
bytes serialize_value(Type& t, const Value& value) {
    bytes b(bytes::initialized_later(), t.serialized_size(value));
    auto i = b.begin();
    t.serialize_value(value, i);
    return b;
}

template<typename T>
T read_simple(bytes_view& v) {
    if (v.size() < sizeof(T)) {
        throw marshal_exception();
    }
    auto p = v.begin();
    v.remove_prefix(sizeof(T));
    return net::ntoh(*reinterpret_cast<const net::packed<T>*>(p));
}

template<typename T>
T read_simple_exactly(bytes_view v) {
    if (v.size() != sizeof(T)) {
        throw marshal_exception();
    }
    auto p = v.begin();
    return net::ntoh(*reinterpret_cast<const net::packed<T>*>(p));
}

inline
bytes_view
read_simple_bytes(bytes_view& v, size_t n) {
    if (v.size() < n) {
        throw marshal_exception();
    }
    bytes_view ret(v.begin(), n);
    v.remove_prefix(n);
    return ret;
}

template<typename T>
boost::any read_simple_opt(bytes_view& v) {
    if (v.empty()) {
        return {};
    }
    if (v.size() != sizeof(T)) {
        throw marshal_exception();
    }
    auto p = v.begin();
    v.remove_prefix(sizeof(T));
    return boost::any(net::ntoh(*reinterpret_cast<const net::packed<T>*>(p)));
}

inline sstring read_simple_short_string(bytes_view& v) {
    uint16_t len = read_simple<uint16_t>(v);
    if (v.size() < len) {
        throw marshal_exception();
    }
    sstring ret(sstring::initialized_later(), len);
    std::copy(v.begin(), v.begin() + len, ret.begin());
    v.remove_prefix(len);
    return ret;
}

size_t collection_size_len(serialization_format sf);
size_t collection_value_len(serialization_format sf);
void write_collection_size(bytes::iterator& out, int size, serialization_format sf);
void write_collection_value(bytes::iterator& out, serialization_format sf, bytes_view val_bytes);
void write_collection_value(bytes::iterator& out, serialization_format sf, data_type type, const boost::any& value);

template <typename BytesViewIterator>
bytes
collection_type_impl::pack(BytesViewIterator start, BytesViewIterator finish, int elements, serialization_format sf) {
    size_t len = collection_size_len(sf);
    size_t psz = collection_value_len(sf);
    for (auto j = start; j != finish; j++) {
        len += j->size() + psz;
    }
    bytes out(bytes::initialized_later(), len);
    bytes::iterator i = out.begin();
    write_collection_size(i, elements, sf);
    while (start != finish) {
        write_collection_value(i, sf, *start++);
    }
    return out;
}

struct tuple_deserializing_iterator : public std::iterator<std::input_iterator_tag, const bytes_view_opt> {
    bytes_view _v;
    bytes_view_opt _current;
public:
    struct end_tag {};
    tuple_deserializing_iterator(bytes_view v) : _v(v) {
        parse();
    }
    tuple_deserializing_iterator(end_tag, bytes_view v) : _v(v) {
        _v.remove_prefix(_v.size());
    }
    static tuple_deserializing_iterator start(bytes_view v) {
        return tuple_deserializing_iterator(v);
    }
    static tuple_deserializing_iterator finish(bytes_view v) {
        return tuple_deserializing_iterator(end_tag(), v);
    }
    const bytes_view_opt& operator*() const {
        return _current;
    }
    const bytes_view_opt* operator->() const {
        return &_current;
    }
    tuple_deserializing_iterator& operator++() {
        skip();
        parse();
        return *this;
    }
    void operator++(int) {
        skip();
        parse();
    }
    bool operator==(const tuple_deserializing_iterator& x) const {
        return _v == x._v;
    }
    bool operator!=(const tuple_deserializing_iterator& x) const {
        return !operator==(x);
    }
private:
    void parse() {
        _current = std::experimental::nullopt;
        if (_v.empty()) {
            return;
        }
        // we don't consume _v, otherwise operator==
        // or the copy constructor immediately after
        // parse() yields the wrong results.
        auto tmp = _v;
        auto s = read_simple<int32_t>(tmp);
        if (s < 0) {
            return;
        }
        _current = read_simple_bytes(tmp, s);
    }
    void skip() {
        _v.remove_prefix(4 + (_current ? _current->size() : 0));
    }
};

class tuple_type_impl : public abstract_type {
protected:
    std::vector<data_type> _types;
    static boost::iterator_range<tuple_deserializing_iterator> make_range(bytes_view v) {
        return { tuple_deserializing_iterator::start(v), tuple_deserializing_iterator::finish(v) };
    }
    tuple_type_impl(sstring name, std::vector<data_type> types);
public:
    using native_type = std::vector<boost::any>;
    tuple_type_impl(std::vector<data_type> types);
    static shared_ptr<tuple_type_impl> get_instance(std::vector<data_type> types);
    data_type type(size_t i) const {
        return _types[i];
    }
    size_t size() const {
        return _types.size();
    }
    const std::vector<data_type>& all_types() const {
        return _types;
    }
    virtual int32_t compare(bytes_view v1, bytes_view v2) override;
    virtual bool less(bytes_view v1, bytes_view v2) override;
    virtual size_t serialized_size(const boost::any& value) override;
    virtual void serialize(const boost::any& value, bytes::iterator& out) override;
    virtual boost::any deserialize(bytes_view v) override;
    std::vector<bytes_view_opt> split(bytes_view v) const;
    template <typename RangeOf_bytes_opt>  // also accepts bytes_view_opt
    static bytes build_value(RangeOf_bytes_opt&& range) {
        auto item_size = [] (auto&& v) { return 4 + (v ? v->size() : 0); };
        auto size = boost::accumulate(range | boost::adaptors::transformed(item_size), 0);
        auto ret = bytes(bytes::initialized_later(), size);
        auto out = ret.begin();
        auto put = [&out] (auto&& v) {
            if (v) {
                write(out, int32_t(v->size()));
                out = std::copy(v->begin(), v->end(), out);
            } else {
                write(out, int32_t(-1));
            }
        };
        boost::range::for_each(range, put);
        return ret;
    }
    virtual size_t hash(bytes_view v) override;
    virtual bytes from_string(sstring_view s) override;
    virtual sstring to_string(const bytes& b) override;
    virtual bool equals(const abstract_type& other) const override;
    virtual bool is_compatible_with(abstract_type& previous) override;
    virtual bool is_value_compatible_with_internal(abstract_type& previous) override;
    virtual shared_ptr<cql3::cql3_type> as_cql3_type() override;
private:
    bool check_compatibility(abstract_type& previous, bool (abstract_type::*predicate)(abstract_type&)) const;
    static sstring make_name(const std::vector<data_type>& types);
};

// FIXME: conflicts with another tuple_type
using db_tuple_type = shared_ptr<tuple_type_impl>;

class user_type_impl : public tuple_type_impl {
public:
    const sstring _keyspace;
    const bytes _name;
private:
    std::vector<bytes> _field_names;
public:
    user_type_impl(sstring keyspace, bytes name, std::vector<bytes> field_names, std::vector<data_type> field_types)
            : tuple_type_impl(make_name(keyspace, name, field_names, field_types), field_types)
            , _keyspace(keyspace)
            , _name(name)
            , _field_names(field_names) {
    }
    static shared_ptr<user_type_impl> get_instance(sstring keyspace, bytes name, std::vector<bytes> field_names, std::vector<data_type> field_types) {
        return ::make_shared<user_type_impl>(std::move(keyspace), std::move(name), std::move(field_names), std::move(field_types));
    }
    data_type field_type(size_t i) const { return type(i); }
    const std::vector<data_type>& field_types() const { return _types; }
    bytes_view field_name(size_t i) const { return _field_names[i]; }
    const std::vector<bytes>& field_names() const { return _field_names; }
    sstring get_name_as_string() const;
    virtual shared_ptr<cql3::cql3_type> as_cql3_type() override;
private:
    static sstring make_name(sstring keyspace, bytes name, std::vector<bytes> field_names, std::vector<data_type> field_types);
};

using user_type = shared_ptr<user_type_impl>;
