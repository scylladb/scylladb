/*
 * Copyright (C) 2014-present ScyllaDB
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

#include <optional>
#include <boost/functional/hash.hpp>
#include <iosfwd>
#include <sstream>
#include <iterator>

#include <seastar/core/sstring.hh>
#include <seastar/core/shared_ptr.hh>
#include "utils/UUID.hh"
#include <seastar/net/byteorder.hh>
#include "db_clock.hh"
#include "bytes.hh"
#include "to_string.hh"
#include "duration.hh"
#include "marshal_exception.hh"
#include <seastar/net/ip.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/util/backtrace.hh>
#include "hashing.hh"
#include "utils/fragmented_temporary_buffer.hh"
#include "utils/exceptions.hh"
#include "utils/managed_bytes.hh"
#include "utils/bit_cast.hh"
#include "utils/chunked_vector.hh"

class tuple_type_impl;
class big_decimal;
class cql_serialization_format;

namespace utils {

class multiprecision_int;

}

namespace cql3 {

class cql3_type;

}

// Specifies position in a lexicographically ordered sequence
// relative to some value.
//
// For example, if used with a value "bc" with lexicographical ordering on strings,
// each enum value represents the following positions in an example sequence:
//
//   aa
//   aaa
//   b
//   ba
// --> before_all_prefixed
//   bc
// --> before_all_strictly_prefixed
//   bca
//   bcd
// --> after_all_prefixed
//   bd
//   bda
//   c
//   ca
//
enum class lexicographical_relation : int8_t {
    before_all_prefixed,
    before_all_strictly_prefixed,
    after_all_prefixed
};

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
template <std::input_iterator TypesIterator, std::input_iterator InputIt1, std::input_iterator InputIt2, typename Compare>
requires requires (TypesIterator types, InputIt1 i1, InputIt2 i2, Compare cmp) {
    { cmp(*types, *i1, *i2) } -> std::same_as<std::strong_ordering>;
}
std::strong_ordering lexicographical_tri_compare(TypesIterator types_first, TypesIterator types_last,
        InputIt1 first1, InputIt1 last1,
        InputIt2 first2, InputIt2 last2,
        Compare comp,
        lexicographical_relation relation1 = lexicographical_relation::before_all_strictly_prefixed,
        lexicographical_relation relation2 = lexicographical_relation::before_all_strictly_prefixed) {
    while (types_first != types_last && first1 != last1 && first2 != last2) {
        auto c = comp(*types_first, *first1, *first2);
        if (c != 0) {
            return c;
        }
        ++first1;
        ++first2;
        ++types_first;
    }
    bool e1 = first1 == last1;
    bool e2 = first2 == last2;
    if (e1 && e2) {
        return static_cast<int>(relation1) <=> static_cast<int>(relation2);
    }
    if (e2) {
        return relation2 == lexicographical_relation::after_all_prefixed ? std::strong_ordering::less : std::strong_ordering::greater;
    } else if (e1) {
        return relation1 == lexicographical_relation::after_all_prefixed ? std::strong_ordering::greater : std::strong_ordering::less;
    } else {
        return std::strong_ordering::equal;
    }
}

// Trichotomic version of std::lexicographical_compare()
template <typename InputIt1, typename InputIt2, typename Compare>
requires requires (InputIt1 i1, InputIt2 i2, Compare c) {
    { c(*i1, *i2) } -> std::same_as<std::strong_ordering>;
}
std::strong_ordering lexicographical_tri_compare(InputIt1 first1, InputIt1 last1,
        InputIt2 first2, InputIt2 last2,
        Compare comp,
        lexicographical_relation relation1 = lexicographical_relation::before_all_strictly_prefixed,
        lexicographical_relation relation2 = lexicographical_relation::before_all_strictly_prefixed) {
    while (first1 != last1 && first2 != last2) {
        auto c = comp(*first1, *first2);
        if (c != 0) {
            return c;
        }
        ++first1;
        ++first2;
    }
    bool e1 = first1 == last1;
    bool e2 = first2 == last2;
    if (e1 == e2) {
        return (static_cast<int>(relation1) - static_cast<int>(relation2)) <=> 0;
    }
    if (e2) {
        return relation2 == lexicographical_relation::after_all_prefixed ? std::strong_ordering::less : std::strong_ordering::greater;
    } else {
        return relation1 == lexicographical_relation::after_all_prefixed ? std::strong_ordering::greater : std::strong_ordering::less;
    }
}

// A trichotomic comparator for prefix equality total ordering.
// In this ordering, two sequences are equal iff any of them is a prefix
// of the another. Otherwise, lexicographical ordering determines the order.
//
// 'comp' is an abstract_type-aware trichotomic comparator, which takes the
// type as first argument.
//
template <typename TypesIterator, typename InputIt1, typename InputIt2, typename Compare>
requires requires (TypesIterator ti, InputIt1 i1, InputIt2 i2, Compare c) {
    { c(*ti, *i1, *i2) } -> std::same_as<std::strong_ordering>;
}
std::strong_ordering prefix_equality_tri_compare(TypesIterator types, InputIt1 first1, InputIt1 last1,
        InputIt2 first2, InputIt2 last2, Compare comp) {
    while (first1 != last1 && first2 != last2) {
        auto c = comp(*types, *first1, *first2);
        if (c != 0) {
            return c;
        }
        ++first1;
        ++first2;
        ++types;
    }
    return std::strong_ordering::equal;
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

struct runtime_exception : public std::exception {
    sstring _why;
public:
    runtime_exception(sstring why) : _why(sstring("runtime error: ") + why) {}
    virtual const char* what() const noexcept override { return _why.c_str(); }
};

struct empty_t {};

class empty_value_exception : public std::exception {
public:
    virtual const char* what() const noexcept override {
        return "Unexpected empty value";
    }
};

[[noreturn]] void on_types_internal_error(std::exception_ptr ex);

// Cassandra has a notion of empty values even for scalars (i.e. int).  This is
// distinct from NULL which means deleted or never set.  It is serialized
// as a zero-length byte array (whereas NULL is serialized as a negative-length
// byte array).
template <typename T>
class emptyable {
    // We don't use optional<>, to avoid lots of ifs during the copy and move constructors
    static_assert(std::is_default_constructible<T>::value, "must be default constructible");
    bool _is_empty = false;
    T _value;
public:
    // default-constructor defaults to a non-empty value, since empty is the
    // exception rather than the rule
    emptyable() : _value{} {}
    emptyable(const T& x) : _value(x) {}
    emptyable(T&& x) : _value(std::move(x)) {}
    emptyable(empty_t) : _is_empty(true) {}
    template <typename... U>
    emptyable(U&&... args) : _value(std::forward<U>(args)...) {}
    bool empty() const { return _is_empty; }
    operator const T& () const { verify(); return _value; }
    operator T&& () && { verify(); return std::move(_value); }
    const T& get() const & { verify(); return _value; }
    T&& get() && { verify(); return std::move(_value); }
private:
    void verify() const {
        if (_is_empty) {
            throw empty_value_exception();
        }
    }
};

template <typename T>
inline
bool
operator==(const emptyable<T>& me1, const emptyable<T>& me2) {
    if (me1.empty() && me2.empty()) {
        return true;
    }
    if (me1.empty() != me2.empty()) {
        return false;
    }
    return me1.get() == me2.get();
}

template <typename T>
inline
bool
operator<(const emptyable<T>& me1, const emptyable<T>& me2) {
    if (me1.empty()) {
        if (me2.empty()) {
            return false;
        } else {
            return true;
        }
    }
    if (me2.empty()) {
        return false;
    } else {
        return me1.get() < me2.get();
    }
}

// Checks whether T::empty() const exists and returns bool
template <typename T>
concept has_empty = requires (T obj) {
    { obj.empty() } -> std::same_as<bool>;
};

template <typename T>
using maybe_empty =
        std::conditional_t<has_empty<T>, T, emptyable<T>>;

class abstract_type;
class data_value;

struct ascii_native_type {
    using primary_type = sstring;
    primary_type string;
};

struct simple_date_native_type {
    using primary_type = uint32_t;
    primary_type days;
};

struct date_type_native_type {
    using primary_type = db_clock::time_point;
    primary_type tp;
};

struct time_native_type {
    using primary_type = int64_t;
    primary_type nanoseconds;
};

struct timeuuid_native_type {
    using primary_type = utils::UUID;
    primary_type uuid;
};

using data_type = shared_ptr<const abstract_type>;

template <typename T>
const T& value_cast(const data_value& value);

template <typename T>
T&& value_cast(data_value&& value);

class data_value {
    void* _value;  // FIXME: use "small value optimization" for small types
    data_type _type;
private:
    data_value(void* value, data_type type) : _value(value), _type(std::move(type)) {}
    template <typename T>
    static data_value make_new(data_type type, T&& value);
public:
    ~data_value();
    data_value(const data_value&);
    data_value(data_value&& x) noexcept : _value(x._value), _type(std::move(x._type)) {
        x._value = nullptr;
    }
    // common conversions from C++ types to database types
    // note: somewhat dangerous, consider a factory function instead
    explicit data_value(bytes);

    data_value(sstring&&);
    data_value(std::string_view);
    // We need the following overloads just to avoid ambiguity because
    // seastar::net::inet_address is implicitly constructible from a
    // const sstring&.
    data_value(const char*);
    data_value(const std::string&);
    data_value(const sstring&);

    // Do not allow construction of a data_value from nullptr. The reason is
    // that this is error prone, for example: it conflicts with `const char*` overload
    // which tries to allocate a value from it and will cause UB.
    //
    // We want the null value semantics here instead. So the user will be forced
    // to explicitly call `make_null()` instead.
    data_value(std::nullptr_t) = delete;

    data_value(ascii_native_type);
    data_value(bool);
    data_value(int8_t);
    data_value(int16_t);
    data_value(int32_t);
    data_value(int64_t);
    data_value(utils::UUID);
    data_value(float);
    data_value(double);
    data_value(net::ipv4_address);
    data_value(net::ipv6_address);
    data_value(seastar::net::inet_address);
    data_value(simple_date_native_type);
    data_value(db_clock::time_point);
    data_value(time_native_type);
    data_value(timeuuid_native_type);
    data_value(date_type_native_type);
    data_value(utils::multiprecision_int);
    data_value(big_decimal);
    data_value(cql_duration);
    explicit data_value(std::optional<bytes>);
    template <typename NativeType>
    data_value(std::optional<NativeType>);
    template <typename NativeType>
    data_value(const std::unordered_set<NativeType>&);

    data_value& operator=(const data_value&);
    data_value& operator=(data_value&&);
    const data_type& type() const {
        return _type;
    }
    bool is_null() const {   // may return false negatives for strings etc.
        return !_value;
    }
    size_t serialized_size() const;
    void serialize(bytes::iterator& out) const;
    bytes_opt serialize() const;
    bytes serialize_nonnull() const;
    friend bool operator==(const data_value& x, const data_value& y);
    friend inline bool operator!=(const data_value& x, const data_value& y);
    friend class abstract_type;
    static data_value make_null(data_type type) {
        return data_value(nullptr, std::move(type));
    }
    template <typename T>
    static data_value make(data_type type, std::unique_ptr<T> value) {
        return data_value(value.release(), std::move(type));
    }
    friend class empty_type_impl;
    template <typename T> friend const T& value_cast(const data_value&);
    template <typename T> friend T&& value_cast(data_value&&);
    friend std::ostream& operator<<(std::ostream&, const data_value&);
    friend data_value make_tuple_value(data_type, maybe_empty<std::vector<data_value>>);
    friend data_value make_set_value(data_type, maybe_empty<std::vector<data_value>>);
    friend data_value make_list_value(data_type, maybe_empty<std::vector<data_value>>);
    friend data_value make_map_value(data_type, maybe_empty<std::vector<std::pair<data_value, data_value>>>);
    friend data_value make_user_value(data_type, std::vector<data_value>);
    template <typename Func>
    friend inline auto visit(const data_value& v, Func&& f);
};

template<typename T>
inline bytes serialized(T v) {
    return data_value(v).serialize_nonnull();
}

class serialized_compare;
class serialized_tri_compare;
class user_type_impl;

// Unsafe to access across shards unless otherwise noted.
class abstract_type : public enable_shared_from_this<abstract_type> {
    sstring _name;
    std::optional<uint32_t> _value_length_if_fixed;
public:
    enum class kind : int8_t {
        ascii,
        boolean,
        byte,
        bytes,
        counter,
        date,
        decimal,
        double_kind,
        duration,
        empty,
        float_kind,
        inet,
        int32,
        list,
        long_kind,
        map,
        reversed,
        set,
        short_kind,
        simple_date,
        time,
        timestamp,
        timeuuid,
        tuple,
        user,
        utf8,
        uuid,
        varint,
        last = varint,
    };
private:
    kind _kind;
public:
    kind get_kind() const { return _kind; }

    abstract_type(kind k, sstring name, std::optional<uint32_t> value_length_if_fixed)
        : _name(name), _value_length_if_fixed(std::move(value_length_if_fixed)), _kind(k) {}
    virtual ~abstract_type() {}
    bool less(bytes_view v1, bytes_view v2) const { return compare(v1, v2) < 0; }
    // returns a callable that can be called with two byte_views, and calls this->less() on them.
    serialized_compare as_less_comparator() const ;
    serialized_tri_compare as_tri_comparator() const ;
    static data_type parse_type(const sstring& name);
    size_t hash(bytes_view v) const;
    size_t hash(managed_bytes_view v) const;
    bool equal(bytes_view v1, bytes_view v2) const;
    bool equal(managed_bytes_view v1, managed_bytes_view v2) const;
    std::strong_ordering compare(bytes_view v1, bytes_view v2) const;
    std::strong_ordering compare(managed_bytes_view v1, managed_bytes_view v2) const;

private:
    // Explicitly instantiated in .cc
    template <FragmentedView View> data_value deserialize_impl(View v) const;
public:
    template <FragmentedView View> data_value deserialize(View v) const {
        if (v.size_bytes() == v.current_fragment().size()) [[likely]] {
            return deserialize_impl(single_fragmented_view(v.current_fragment()));
        } else {
            return deserialize_impl(v);
        }
    }
    data_value deserialize(bytes_view v) const {
        return deserialize_impl(single_fragmented_view(v));
    }
    template <FragmentedView View> data_value deserialize_value(View v) const {
        return deserialize(v);
    }
    data_value deserialize_value(bytes_view v) const {
        return deserialize_impl(single_fragmented_view(v));
    };
    // Explicitly instantiated in .cc
    template <FragmentedView View> void validate(const View& v, cql_serialization_format sf) const;
    void validate(bytes_view view, cql_serialization_format sf) const;
    bool is_compatible_with(const abstract_type& previous) const;
    /*
     * Types which are wrappers over other types return the inner type.
     * For example the reversed_type returns the type it is reversing.
     */
    shared_ptr<const abstract_type> underlying_type() const;

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
    bool is_value_compatible_with(const abstract_type& other) const;
    bool references_user_type(const sstring& keyspace, const bytes& name) const;

    // For types that contain (or are equal to) the given user type (e.g., a set of elements of this type),
    // updates them with the new version of the type ('updated'). For other types does nothing.
    std::optional<data_type> update_user_type(const shared_ptr<const user_type_impl> updated) const;

    bool references_duration() const;
    std::optional<uint32_t> value_length_if_fixed() const {
        return _value_length_if_fixed;
    }
public:
    bytes decompose(const data_value& value) const;
    // Safe to call across shards
    const sstring& name() const {
        return _name;
    }

    /**
     * When returns true then equal values have the same byte representation and if byte
     * representation is different, the values are not equal.
     *
     * When returns false, nothing can be inferred.
     */
    bool is_byte_order_equal() const;
    sstring get_string(const bytes& b) const;
    sstring to_string(bytes_view bv) const {
        return to_string_impl(deserialize(bv));
    }
    sstring to_string(const bytes& b) const {
        return to_string(bytes_view(b));
    }
    sstring to_string_impl(const data_value& v) const;
    bytes from_string(sstring_view text) const;
    bool is_counter() const;
    bool is_string() const;
    bool is_collection() const;
    bool is_map() const { return _kind == kind::map; }
    bool is_set() const { return _kind == kind::set; }
    bool is_list() const { return _kind == kind::list; }
    // Lists and sets are similar: they are both represented as std::vector<data_value>
    // @sa listlike_collection_type_impl
    bool is_listlike() const { return _kind == kind::list || _kind == kind::set; }
    bool is_multi_cell() const;
    bool is_atomic() const { return !is_multi_cell(); }
    bool is_reversed() const { return _kind == kind::reversed; }
    bool is_tuple() const;
    bool is_user_type() const { return _kind == kind::user; }
    bool is_native() const;
    cql3::cql3_type as_cql3_type() const;
    const sstring& cql3_type_name() const;
    virtual shared_ptr<const abstract_type> freeze() const { return shared_from_this(); }

    const abstract_type& without_reversed() const {
        return is_reversed() ? *underlying_type() : *this;
    }

    friend class list_type_impl;
private:
    mutable sstring _cql3_type_name;
protected:
    // native_value_* methods are virualized versions of native_type's
    // sizeof/alignof/copy-ctor/move-ctor etc.
    void* native_value_clone(const void* from) const;
    const std::type_info& native_typeid() const;
    // abstract_type is a friend of data_value, but derived classes are not.
    static const void* get_value_ptr(const data_value& v) {
        return v._value;
    }
    friend void write_collection_value(bytes::iterator& out, cql_serialization_format sf, data_type type, const data_value& value);
    friend class tuple_type_impl;
    friend class data_value;
    friend class reversed_type_impl;
    template <typename T> friend const T& value_cast(const data_value& value);
    template <typename T> friend T&& value_cast(data_value&& value);
    friend bool operator==(const abstract_type& x, const abstract_type& y);
};

inline bool operator==(const abstract_type& x, const abstract_type& y)
{
     return &x == &y;
}

template <typename T>
inline
data_value
data_value::make_new(data_type type, T&& v) {
    maybe_empty<std::remove_reference_t<T>> value(std::forward<T>(v));
    return data_value(type->native_value_clone(&value), type);
}

template <typename T>
const T& value_cast(const data_value& value) {
    return value_cast<T>(const_cast<data_value&&>(value));
}

template <typename T>
T&& value_cast(data_value&& value) {
    if (typeid(maybe_empty<T>) != value.type()->native_typeid()) {
        throw std::bad_cast();
    }
    if (value.is_null()) {
        throw std::runtime_error("value is null");
    }
    return std::move(*reinterpret_cast<maybe_empty<T>*>(value._value));
}

/// Special case: sometimes we cast uuid to timeuuid so we can correctly compare timestamps.  See #7729.
template <>
inline timeuuid_native_type&& value_cast<timeuuid_native_type>(data_value&& value) {
    static thread_local timeuuid_native_type value_holder; // Static so it survives return from this function.
    value_holder.uuid = value_cast<utils::UUID>(value);
    return std::move(value_holder);
}

// CRTP: implements translation between a native_type (C++ type) to abstract_type
// AbstractType is parametrized because we want a
//    abstract_type -> collection_type_impl -> map_type
// type hierarchy, and native_type is only known at the last step.
template <typename NativeType, typename AbstractType = abstract_type>
class concrete_type : public AbstractType {
public:
    using native_type = maybe_empty<NativeType>;
    using AbstractType::AbstractType;
public:
    data_value make_value(std::unique_ptr<native_type> value) const {
        return data_value::make(this->shared_from_this(), std::move(value));
    }
    data_value make_value(native_type value) const {
        return make_value(std::make_unique<native_type>(std::move(value)));
    }
    data_value make_null() const {
        return data_value::make_null(this->shared_from_this());
    }
    data_value make_empty() const {
        return make_value(native_type(empty_t()));
    }
    const native_type& from_value(const void* v) const {
        return *reinterpret_cast<const native_type*>(v);
    }
    const native_type& from_value(const data_value& v) const {
        return this->from_value(AbstractType::get_value_ptr(v));
    }

    friend class abstract_type;
};

bool operator==(const data_value& x, const data_value& y);

inline bool operator!=(const data_value& x, const data_value& y)
{
    return !(x == y);
}

using bytes_view_opt = std::optional<bytes_view>;
using managed_bytes_view_opt = std::optional<managed_bytes_view>;

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
std::strong_ordering tri_compare(data_type t, managed_bytes_view e1, managed_bytes_view e2) {
    return t->compare(e1, e2);
}

inline
std::strong_ordering
tri_compare_opt(data_type t, managed_bytes_view_opt v1, managed_bytes_view_opt v2) {
    if (!v1 || !v2) {
        return int(bool(v1)) - int(bool(v2)) <=> 0;
    } else {
        return tri_compare(std::move(t), *v1, *v2);
    }
}

static inline
bool equal(data_type t, managed_bytes_view e1, managed_bytes_view e2) {
    return t->equal(e1, e2);
}

class row_tombstone;

class collection_type_impl;
using collection_type = shared_ptr<const collection_type_impl>;

template <typename... T>
struct simple_tuple_hash;

template <>
struct simple_tuple_hash<> {
    size_t operator()() const { return 0; }
};

template <typename Arg0, typename... Args >
struct simple_tuple_hash<std::vector<Arg0>, Args...> {
    size_t operator()(const std::vector<Arg0>& vec, const Args&... args) const {
        size_t h0 = 0;
        size_t h1;
        for (auto&& i : vec) {
            h1 = std::hash<Arg0>()(i);
            h0 = h0 ^ ((h1 << 7) | (h1 >> (std::numeric_limits<size_t>::digits - 7)));
        }
        h1 = simple_tuple_hash<Args...>()(args...);
        return h0 ^ ((h1 << 7) | (h1 >> (std::numeric_limits<size_t>::digits - 7)));
    }
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
    using value_type = shared_ptr<const InternedType>;
    struct hash_type {
        size_t operator()(const key_type& k) const {
            return apply(simple_tuple_hash<BaseTypes...>(), k);
        }
    };
    using map_type = std::unordered_map<key_type, value_type, hash_type>;
    static thread_local map_type _instances;
public:
    static shared_ptr<const InternedType> get_instance(BaseTypes... keys) {
        auto key = std::make_tuple(keys...);
        auto i = _instances.find(key);
        if (i == _instances.end()) {
            auto v = ::make_shared<InternedType>(std::move(keys)...);
            i = _instances.insert(std::make_pair(std::move(key), std::move(v))).first;
        }
        return i->second;
    }
};

template <typename InternedType, typename... BaseTypes>
thread_local typename type_interning_helper<InternedType, BaseTypes...>::map_type
    type_interning_helper<InternedType, BaseTypes...>::_instances;

class reversed_type_impl : public abstract_type {
    using intern = type_interning_helper<reversed_type_impl, data_type>;
    friend struct shared_ptr_make_helper<reversed_type_impl, true>;

    data_type _underlying_type;
    reversed_type_impl(data_type t)
        : abstract_type(kind::reversed, "org.apache.cassandra.db.marshal.ReversedType(" + t->name() + ")",
                        t->value_length_if_fixed())
        , _underlying_type(t)
    {}
public:
    const data_type& underlying_type() const {
        return _underlying_type;
    }

    static shared_ptr<const reversed_type_impl> get_instance(data_type type);
};
using reversed_type = shared_ptr<const reversed_type_impl>;

// Reverse the sort order of the type by wrapping in or stripping reversed_type,
// as needed.
data_type reversed(data_type);

class map_type_impl;
using map_type = shared_ptr<const map_type_impl>;

class set_type_impl;
using set_type = shared_ptr<const set_type_impl>;

class list_type_impl;
using list_type = shared_ptr<const list_type_impl>;

inline
size_t hash_value(const shared_ptr<const abstract_type>& x) {
    return std::hash<const abstract_type*>()(x.get());
}

template <typename Type>
shared_ptr<const abstract_type> data_type_for();

class serialized_compare {
    data_type _type;
public:
    serialized_compare(data_type type) : _type(type) {}
    bool operator()(const bytes& v1, const bytes& v2) const {
        return _type->less(v1, v2);
    }
    bool operator()(const managed_bytes& v1, const managed_bytes& v2) const {
        return _type->compare(v1, v2) < 0;
    }
};

inline
serialized_compare
abstract_type::as_less_comparator() const {
    return serialized_compare(shared_from_this());
}

class serialized_tri_compare {
    data_type _type;
public:
    serialized_tri_compare(data_type type) : _type(type) {}
    std::strong_ordering operator()(const bytes_view& v1, const bytes_view& v2) const {
        return _type->compare(v1, v2) <=> 0;
    }
    std::strong_ordering operator()(const managed_bytes_view& v1, const managed_bytes_view& v2) const {
        return _type->compare(v1, v2) <=> 0;
    }
};

inline
serialized_tri_compare
abstract_type::as_tri_comparator() const {
    return serialized_tri_compare(shared_from_this());
}

using key_compare = serialized_compare;

// Remember to update type_codec in transport/server.cc and cql3/cql3_type.cc
extern thread_local const shared_ptr<const abstract_type> byte_type;
extern thread_local const shared_ptr<const abstract_type> short_type;
extern thread_local const shared_ptr<const abstract_type> int32_type;
extern thread_local const shared_ptr<const abstract_type> long_type;
extern thread_local const shared_ptr<const abstract_type> ascii_type;
extern thread_local const shared_ptr<const abstract_type> bytes_type;
extern thread_local const shared_ptr<const abstract_type> utf8_type;
extern thread_local const shared_ptr<const abstract_type> boolean_type;
extern thread_local const shared_ptr<const abstract_type> date_type;
extern thread_local const shared_ptr<const abstract_type> timeuuid_type;
extern thread_local const shared_ptr<const abstract_type> timestamp_type;
extern thread_local const shared_ptr<const abstract_type> simple_date_type;
extern thread_local const shared_ptr<const abstract_type> time_type;
extern thread_local const shared_ptr<const abstract_type> uuid_type;
extern thread_local const shared_ptr<const abstract_type> inet_addr_type;
extern thread_local const shared_ptr<const abstract_type> float_type;
extern thread_local const shared_ptr<const abstract_type> double_type;
extern thread_local const shared_ptr<const abstract_type> varint_type;
extern thread_local const shared_ptr<const abstract_type> decimal_type;
extern thread_local const shared_ptr<const abstract_type> counter_type;
extern thread_local const shared_ptr<const abstract_type> duration_type;
extern thread_local const data_type empty_type;

template <>
inline
shared_ptr<const abstract_type> data_type_for<int8_t>() {
    return byte_type;
}

template <>
inline
shared_ptr<const abstract_type> data_type_for<int16_t>() {
    return short_type;
}

template <>
inline
shared_ptr<const abstract_type> data_type_for<int32_t>() {
    return int32_type;
}

template <>
inline
shared_ptr<const abstract_type> data_type_for<int64_t>() {
    return long_type;
}

template <>
inline
shared_ptr<const abstract_type> data_type_for<sstring>() {
    return utf8_type;
}

template <>
inline
shared_ptr<const abstract_type> data_type_for<bytes>() {
    return bytes_type;
}

template <>
inline
shared_ptr<const abstract_type> data_type_for<utils::UUID>() {
    return uuid_type;
}

template <>
inline
shared_ptr<const abstract_type> data_type_for<date_type_native_type>() {
    return date_type;
}

template <>
inline
shared_ptr<const abstract_type> data_type_for<simple_date_native_type>() {
    return simple_date_type;
}

template <>
inline
shared_ptr<const abstract_type> data_type_for<db_clock::time_point>() {
    return timestamp_type;
}

template <>
inline
shared_ptr<const abstract_type> data_type_for<ascii_native_type>() {
    return ascii_type;
}

template <>
inline
shared_ptr<const abstract_type> data_type_for<time_native_type>() {
    return time_type;
}

template <>
inline
shared_ptr<const abstract_type> data_type_for<timeuuid_native_type>() {
    return timeuuid_type;
}

template <>
inline
shared_ptr<const abstract_type> data_type_for<net::inet_address>() {
    return inet_addr_type;
}

template <>
inline
shared_ptr<const abstract_type> data_type_for<bool>() {
    return boolean_type;
}

template <>
inline
shared_ptr<const abstract_type> data_type_for<float>() {
    return float_type;
}

template <>
inline
shared_ptr<const abstract_type> data_type_for<double>() {
    return double_type;
}

template <>
inline
shared_ptr<const abstract_type> data_type_for<utils::multiprecision_int>() {
    return varint_type;
}

template <>
inline
shared_ptr<const abstract_type> data_type_for<big_decimal>() {
    return decimal_type;
}

template<>
inline
shared_ptr<const abstract_type> data_type_for<cql_duration>() {
    return duration_type;
}

namespace std {

template <>
struct hash<shared_ptr<const abstract_type>> : boost::hash<shared_ptr<abstract_type>> {
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
bytes_view
to_bytes_view(const std::string& x) {
    return bytes_view(reinterpret_cast<const int8_t*>(x.data()), x.size());
}

inline
bytes
to_bytes(bytes_view x) {
    return bytes(x.begin(), x.size());
}

inline
bytes_opt
to_bytes_opt(bytes_view_opt bv) {
    if (bv) {
        return to_bytes(*bv);
    }
    return std::nullopt;
}

inline
bytes_view_opt
as_bytes_view_opt(const bytes_opt& bv) {
    if (bv) {
        return bytes_view{*bv};
    }
    return std::nullopt;
}

// FIXME: make more explicit
inline
bytes
to_bytes(const sstring& x) {
    return bytes(reinterpret_cast<const int8_t*>(x.c_str()), x.size());
}

// FIXME: make more explicit
inline
managed_bytes
to_managed_bytes(const sstring& x) {
    return managed_bytes(reinterpret_cast<const int8_t*>(x.c_str()), x.size());
}

inline
bytes_view
to_bytes_view(const sstring& x) {
    return bytes_view(reinterpret_cast<const int8_t*>(x.c_str()), x.size());
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

inline bool
less_unsigned(bytes_view v1, bytes_view v2) {
    return compare_unsigned(v1, v2) < 0;
}

template<typename Type>
static inline
typename Type::value_type deserialize_value(Type& t, bytes_view v) {
    return t.deserialize_value(v);
}

template<typename T>
T read_simple(bytes_view& v) {
    if (v.size() < sizeof(T)) {
        throw_with_backtrace<marshal_exception>(format("read_simple - not enough bytes (expected {:d}, got {:d})", sizeof(T), v.size()));
    }
    auto p = v.begin();
    v.remove_prefix(sizeof(T));
    return net::ntoh(read_unaligned<T>(p));
}

template<typename T>
T read_simple_exactly(bytes_view v) {
    if (v.size() != sizeof(T)) {
        throw_with_backtrace<marshal_exception>(format("read_simple_exactly - size mismatch (expected {:d}, got {:d})", sizeof(T), v.size()));
    }
    auto p = v.begin();
    return net::ntoh(read_unaligned<T>(p));
}

inline
bytes_view
read_simple_bytes(bytes_view& v, size_t n) {
    if (v.size() < n) {
        throw_with_backtrace<marshal_exception>(format("read_simple_bytes - not enough bytes (requested {:d}, got {:d})", n, v.size()));
    }
    bytes_view ret(v.begin(), n);
    v.remove_prefix(n);
    return ret;
}

template<FragmentedView View>
View read_simple_bytes(View& v, size_t n) {
    if (v.size_bytes() < n) {
        throw_with_backtrace<marshal_exception>(format("read_simple_bytes - not enough bytes (requested {:d}, got {:d})", n, v.size_bytes()));
    }
    auto prefix = v.prefix(n);
    v.remove_prefix(n);
    return prefix;
}

inline sstring read_simple_short_string(bytes_view& v) {
    uint16_t len = read_simple<uint16_t>(v);
    if (v.size() < len) {
        throw_with_backtrace<marshal_exception>(format("read_simple_short_string - not enough bytes ({:d})", v.size()));
    }
    sstring ret = uninitialized_string(len);
    std::copy(v.begin(), v.begin() + len, ret.begin());
    v.remove_prefix(len);
    return ret;
}

size_t collection_size_len(cql_serialization_format sf);
size_t collection_value_len(cql_serialization_format sf);
void write_collection_size(bytes::iterator& out, int size, cql_serialization_format sf);
void write_collection_size(managed_bytes_mutable_view&, int size, cql_serialization_format sf);
void write_collection_value(bytes::iterator& out, cql_serialization_format sf, bytes_view val_bytes);
void write_collection_value(managed_bytes_mutable_view&, cql_serialization_format sf, bytes_view val_bytes);
void write_collection_value(managed_bytes_mutable_view&, cql_serialization_format sf, const managed_bytes_view& val_bytes);

// Splits a serialized collection into a vector of elements, but does not recursively deserialize the elements.
// Does not perform validation.
template <FragmentedView View>
utils::chunked_vector<managed_bytes> partially_deserialize_listlike(View in, cql_serialization_format sf);
template <FragmentedView View>
std::vector<std::pair<managed_bytes, managed_bytes>> partially_deserialize_map(View in, cql_serialization_format sf);

using user_type = shared_ptr<const user_type_impl>;
using tuple_type = shared_ptr<const tuple_type_impl>;

inline
data_value::data_value(std::optional<bytes> v)
        : data_value(v ? data_value(*v) : data_value::make_null(data_type_for<bytes>())) {
}

template <typename NativeType>
data_value::data_value(std::optional<NativeType> v)
        : data_value(v ? data_value(*v) : data_value::make_null(data_type_for<NativeType>())) {
}

template<>
struct appending_hash<data_type> {
    template<typename Hasher>
    void operator()(Hasher& h, const data_type& v) const {
        feed_hash(h, v->name());
    }
};
