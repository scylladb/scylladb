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

namespace cql3 {

class cql3_type;
class column_specification;

}

using object_opt = std::experimental::optional<boost::any>;

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

class abstract_type : public enable_shared_from_this<abstract_type> {
    sstring _name;
public:
    abstract_type(sstring name) : _name(name) {}
    virtual ~abstract_type() {}
    virtual void serialize(const boost::any& value, std::ostream& out) = 0;
    virtual bool less(bytes_view v1, bytes_view v2) = 0;
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
    virtual object_opt deserialize(bytes_view v) = 0;
    virtual void validate(const bytes& v) {
        // FIXME
    }
    virtual void validate_collection_member(const bytes& v, const bytes& collection_name) {
        validate(v);
    }
    virtual bool is_compatible_with(abstract_type& previous) {
        // FIXME
        abort();
        return false;
    }
    /*
     * Types which are wrappers over other types should override this.
     * For example the reversed_type returns the type it is reversing.
     */
    virtual abstract_type& underlying_type() {
        return *this;
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
        return is_value_compatible_with_internal(other.underlying_type());
    }
protected:
    /**
     * Needed to handle ReversedType in value-compatibility checks.  Subclasses should implement this instead of
     * is_value_compatible_with().
     */
    virtual bool is_value_compatible_with_internal(abstract_type& other) {
        return is_compatible_with(other);
    }
public:
    virtual object_opt compose(const bytes& v) {
        return deserialize(v);
    }
    bytes decompose(const boost::any& value) {
        // FIXME: optimize
        std::ostringstream oss;
        serialize(value, oss);
        auto s = oss.str();
        return bytes(s.data(), s.size());
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
    virtual ::shared_ptr<cql3::cql3_type> as_cql3_type() = 0;
    virtual shared_ptr<abstract_type> freeze() { return shared_from_this(); }
};

using data_type = shared_ptr<abstract_type>;

class collection_type_impl : public abstract_type {
    static thread_local logging::logger _logger;
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
public:
    // representation of a collection mutation, key/value pairs, value is a mutation itself
    using mutation = std::vector<std::pair<bytes_view, atomic_cell::view>>;
    virtual data_type name_comparator() = 0;
    virtual data_type value_comparator() = 0;
    shared_ptr<cql3::column_specification> make_collection_receiver(shared_ptr<cql3::column_specification> collection, bool is_key);
    virtual bool is_collection() override { return true; }
    bool is_map() const { return &_kind == &kind::map; }
    std::vector<atomic_cell::one> enforce_limit(std::vector<atomic_cell::one>, int version);
    virtual std::vector<bytes> serialized_values(std::vector<atomic_cell::one> cells) = 0;
    bytes serialize_for_native_protocol(std::vector<atomic_cell::one> cells, int version);
    virtual bool is_compatible_with(abstract_type& previous) override;
    virtual bool is_compatible_with_frozen(collection_type_impl& previous) = 0;
    virtual bool is_value_compatible_with_frozen(collection_type_impl& previous) = 0;
    virtual shared_ptr<cql3::cql3_type> as_cql3_type() override;
    mutation deserialize_mutation_form(bytes_view in);
    // FIXME: use iterators?
    collection_mutation::one serialize_mutation_form(mutation mut);
    collection_mutation::one merge(collection_mutation::view a, collection_mutation::view b);
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
    virtual void serialize(const boost::any& value, std::ostream& out) override;
    void serialize(const boost::any& value, std::ostream& out, int protocol_version);
    virtual object_opt deserialize(bytes_view v) override;
    object_opt deserialize(bytes_view v, int protocol_version);
    virtual sstring to_string(const bytes& b) override;
    virtual size_t hash(bytes_view v) override;
    virtual bytes from_string(sstring_view text) override;
    virtual std::vector<bytes> serialized_values(std::vector<atomic_cell::one> cells) override;
};

using map_type = shared_ptr<map_type_impl>;

class set_type_impl final : public collection_type_impl {
    using set_type = shared_ptr<set_type_impl>;
    using intern = type_interning_helper<set_type_impl, data_type, bool>;
    data_type _elements;
    bool _is_multi_cell;
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
    virtual void serialize(const boost::any& value, std::ostream& out) override;
    void serialize(const boost::any& value, std::ostream& out, int protocol_version);
    virtual object_opt deserialize(bytes_view v) override;
    object_opt deserialize(bytes_view v, int protocol_version);
    virtual sstring to_string(const bytes& b) override;
    virtual size_t hash(bytes_view v) override;
    virtual bytes from_string(sstring_view text) override;
    virtual std::vector<bytes> serialized_values(std::vector<atomic_cell::one> cells) override;
};

using set_type = shared_ptr<set_type_impl>;

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

using key_compare = serialized_compare;

// FIXME: add missing types
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

inline
bytes
to_bytes(const char* x) {
    return bytes(reinterpret_cast<const char*>(x), std::strlen(x));
}

inline
bytes
to_bytes(const std::string& x) {
    return bytes(reinterpret_cast<const char*>(x.data()), x.size());
}

inline
bytes
to_bytes(sstring_view x) {
    return bytes(x.begin(), x.size());
}

inline
bytes
to_bytes(const sstring& x) {
    return bytes(reinterpret_cast<const char*>(x.c_str()), x.size());
}

inline
bytes
to_bytes(const utils::UUID& uuid) {
    struct {
        uint64_t msb;
        uint64_t lsb;
    } tmp = { net::hton(uint64_t(uuid.get_most_significant_bits())),
        net::hton(uint64_t(uuid.get_least_significant_bits())) };
    return bytes(reinterpret_cast<char*>(&tmp), 16);
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

template<typename Type>
static inline
bytes serialize_value(Type& t, const typename Type::value_type& value) {
    std::ostringstream oss;
    t.serialize_value(value, oss);
    auto s = oss.str();
    return bytes(s.data(), s.size());
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
T read_simple_exactly(bytes_view& v) {
    if (v.size() != sizeof(T)) {
        throw marshal_exception();
    }
    auto p = v.begin();
    v.remove_prefix(sizeof(T));
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
object_opt read_simple_opt(bytes_view& v) {
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
