/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#include <boost/lexical_cast.hpp>
#include <algorithm>
#include "cql3/cql3_type.hh"
#include "types.hh"
#include "core/print.hh"
#include "net/ip.hh"
#include "database.hh"
#include "util/serialization.hh"
#include "combine.hh"
#include <cmath>
#include <sstream>
#include <boost/iterator/transform_iterator.hpp>
#include <boost/range/adaptor/filtered.hpp>
#include <boost/range/numeric.hpp>

template<typename T>
struct simple_type_traits {
    static T read_nonempty(bytes_view v) {
        return read_simple_exactly<T>(v);
    }
};

template<>
struct simple_type_traits<bool> {
    static bool read_nonempty(bytes_view v) {
        return read_simple_exactly<int8_t>(v) != 0;
    }
};

template<>
struct simple_type_traits<db_clock::time_point> {
    static db_clock::time_point read_nonempty(bytes_view v) {
        return db_clock::time_point(db_clock::duration(read_simple_exactly<int64_t>(v)));
    }
};

template <typename T>
struct simple_type_impl : abstract_type {
    simple_type_impl(sstring name) : abstract_type(std::move(name)) {}
    virtual int32_t compare(bytes_view v1, bytes_view v2) override {
        if (v1.empty()) {
            return v2.empty() ? 0 : -1;
        }
        if (v2.empty()) {
            return 1;
        }
        T a = simple_type_traits<T>::read_nonempty(v1);
        T b = simple_type_traits<T>::read_nonempty(v2);
        return a == b ? 0 : a < b ? -1 : 1;
    }
    virtual bool less(bytes_view v1, bytes_view v2) override {
        return compare(v1, v2) < 0;
    }
    virtual bool is_byte_order_equal() const override {
        return true;
    }
    virtual size_t hash(bytes_view v) override {
        return std::hash<bytes_view>()(v);
    }
};

template<typename T>
struct integer_type_impl : simple_type_impl<T> {
    integer_type_impl(sstring name) : simple_type_impl<T>(name) {}
    virtual void serialize(const boost::any& value, bytes::iterator& out) override {
        auto v = boost::any_cast<const T&>(value);
        auto u = net::hton(v);
        out = std::copy_n(reinterpret_cast<const char*>(&u), sizeof(u), out);
    }
    virtual size_t serialized_size(const boost::any& value) override {
        auto v = boost::any_cast<const T&>(value);
        return sizeof(v);
    }
    virtual boost::any deserialize(bytes_view v) override {
        return read_simple_opt<T>(v);
    }
    T compose_value(const bytes& b) {
        if (b.size() != sizeof(T)) {
            throw marshal_exception();
        }
        return (T)net::ntoh(*reinterpret_cast<const T*>(b.begin()));
    }
    bytes decompose_value(T v) {
        bytes b(bytes::initialized_later(), sizeof(v));
        *reinterpret_cast<T*>(b.begin()) = (T)net::hton(v);
        return b;
    }
    T parse_int(sstring_view s) {
        try {
            return boost::lexical_cast<T>(s.begin(), s.size());
        } catch (const boost::bad_lexical_cast& e) {
            throw marshal_exception(sprint("Invalid number format '%s'", s));
        }
    }
    virtual bytes from_string(sstring_view s) override {
        return decompose_value(parse_int(s));
    }
    virtual sstring to_string(const bytes& b) override {
        if (b.empty()) {
            return {};
        }
        return to_sstring(compose_value(b));
    }
};

struct int32_type_impl : integer_type_impl<int32_t> {
    int32_type_impl() : integer_type_impl{"int32"}
    { }

    virtual ::shared_ptr<cql3::cql3_type> as_cql3_type() override {
        return cql3::cql3_type::int_;
    }
};

struct long_type_impl : integer_type_impl<int64_t> {
    long_type_impl() : integer_type_impl{"long"}
    { }

    virtual ::shared_ptr<cql3::cql3_type> as_cql3_type() override {
        return cql3::cql3_type::bigint;
    }
    virtual bool is_value_compatible_with_internal(abstract_type& other) override {
        return &other == this || &other == date_type.get() || &other == timestamp_type.get();
    }
};

struct string_type_impl : public abstract_type {
    string_type_impl(sstring name, std::function<shared_ptr<cql3::cql3_type>()> cql3_type)
        : abstract_type(name), _cql3_type(cql3_type) {}
    virtual void serialize(const boost::any& value, bytes::iterator& out) override {
        auto& v = boost::any_cast<const sstring&>(value);
        out = std::copy(v.begin(), v.end(), out);
    }
    virtual size_t serialized_size(const boost::any& value) override {
        auto& v = boost::any_cast<const sstring&>(value);
        return v.size();
    }
    virtual boost::any deserialize(bytes_view v) override {
        // FIXME: validation?
        return boost::any(sstring(reinterpret_cast<const char*>(v.begin()), v.size()));
    }
    virtual bool less(bytes_view v1, bytes_view v2) override {
        return less_unsigned(v1, v2);
    }
    virtual bool is_byte_order_equal() const override {
        return true;
    }
    virtual bool is_byte_order_comparable() const override {
        return true;
    }
    virtual size_t hash(bytes_view v) override {
        return std::hash<bytes_view>()(v);
    }
    virtual bytes from_string(sstring_view s) override {
        return to_bytes(bytes_view(reinterpret_cast<const int8_t*>(s.begin()), s.size()));
    }
    virtual sstring to_string(const bytes& b) override {
        return sstring(reinterpret_cast<const char*>(b.begin()), b.size());
    }
    virtual ::shared_ptr<cql3::cql3_type> as_cql3_type() override {
        return _cql3_type();
    }
    std::function<shared_ptr<cql3::cql3_type>()> _cql3_type;
};

struct bytes_type_impl final : public abstract_type {
    bytes_type_impl() : abstract_type("bytes") {}
    virtual void serialize(const boost::any& value, bytes::iterator& out) override {
        auto& v = boost::any_cast<const bytes&>(value);
        out = std::copy(v.begin(), v.end(), out);
    }
    virtual size_t serialized_size(const boost::any& value) override {
        auto& v = boost::any_cast<const bytes&>(value);
        return v.size();
    }
    virtual boost::any deserialize(bytes_view v) override {
        return boost::any(bytes(v.begin(), v.end()));
    }
    virtual bool less(bytes_view v1, bytes_view v2) override {
        return less_unsigned(v1, v2);
    }
    virtual bool is_byte_order_equal() const override {
        return true;
    }
    virtual bool is_byte_order_comparable() const override {
        return true;
    }
    virtual size_t hash(bytes_view v) override {
        return std::hash<bytes_view>()(v);
    }
    virtual bytes from_string(sstring_view s) override {
        return from_hex(s);
    }
    virtual sstring to_string(const bytes& b) override {
        return to_hex(b);
    }
    virtual ::shared_ptr<cql3::cql3_type> as_cql3_type() override {
        return cql3::cql3_type::blob;
    }
    virtual bool is_value_compatible_with_internal(abstract_type& other) override {
        return true;
    }
};

struct boolean_type_impl : public simple_type_impl<bool> {
    boolean_type_impl() : simple_type_impl<bool>("boolean") {}
    virtual void serialize(const boost::any& value, bytes::iterator& out) override {
        auto v = boost::any_cast<bool>(value);
        *out++ = char(v);
    }
    virtual size_t serialized_size(const boost::any& value) override {
        return 1;
    }
    virtual boost::any deserialize(bytes_view v) override {
        if (v.empty()) {
            return {};
        }
        if (v.size() != 1) {
            throw marshal_exception();
        }
        return boost::any(*v.begin() != 0);
    }
    virtual bytes from_string(sstring_view s) override {
        throw std::runtime_error("not implemented");
    }
    virtual sstring to_string(const bytes& b) override {
        throw std::runtime_error("not implemented");
    }
    virtual ::shared_ptr<cql3::cql3_type> as_cql3_type() override {
        return cql3::cql3_type::boolean;
    }
};

struct date_type_impl : public abstract_type {
    date_type_impl() : abstract_type("date") {}
    virtual void serialize(const boost::any& value, bytes::iterator& out) override {
        auto v = boost::any_cast<db_clock::time_point>(value);
        int64_t i = v.time_since_epoch().count();
        i = net::hton(uint64_t(i));
        out = std::copy_n(reinterpret_cast<const char*>(&i), sizeof(i), out);
    }
    virtual size_t serialized_size(const boost::any& value) override {
         return 8;
    }
    virtual boost::any deserialize(bytes_view v) override {
        if (v.empty()) {
            return {};
        }
        auto tmp = read_simple_exactly<uint64_t>(v);
        return boost::any(db_clock::time_point(db_clock::duration(tmp)));
    }
    virtual bool less(bytes_view b1, bytes_view b2) override {
        return compare_unsigned(b1, b2);
    }
    virtual bool is_byte_order_comparable() const override {
        return true;
    }
    virtual size_t hash(bytes_view v) override {
        return std::hash<bytes_view>()(v);
    }
    virtual bytes from_string(sstring_view s) override {
        throw std::runtime_error("not implemented");
    }
    virtual sstring to_string(const bytes& b) override {
        throw std::runtime_error("not implemented");
    }
    virtual ::shared_ptr<cql3::cql3_type> as_cql3_type() override {
        return cql3::cql3_type::timestamp;
    }
    virtual bool is_value_compatible_with_internal(abstract_type& other) override {
        return &other == this || &other == timestamp_type.get() || &other == long_type.get();
    }
};

struct timeuuid_type_impl : public abstract_type {
    timeuuid_type_impl() : abstract_type("timeuuid") {}
    virtual void serialize(const boost::any& value, bytes::iterator& out) override {
        auto& uuid = boost::any_cast<const utils::UUID&>(value);
        out = std::copy_n(uuid.to_bytes().begin(), sizeof(uuid), out);
    }
    virtual size_t serialized_size(const boost::any& value) override {
        return 16;
    }
    virtual boost::any deserialize(bytes_view v) override {
        uint64_t msb, lsb;
        if (v.empty()) {
            return {};
        }
        msb = read_simple<uint64_t>(v);
        lsb = read_simple<uint64_t>(v);
        if (!v.empty()) {
            throw marshal_exception();
        }
        return boost::any(utils::UUID(msb, lsb));
    }
    virtual bool less(bytes_view b1, bytes_view b2) override {
        if (b1.empty()) {
            return b2.empty() ? false : true;
        }
        if (b2.empty()) {
            return false;
        }
        auto r = compare_bytes(b1, b2);
        if (r != 0) {
            return r < 0;
        } else {
            return std::lexicographical_compare(b1.begin(), b1.end(), b2.begin(), b2.end());
        }
    }
    virtual bool is_byte_order_equal() const override {
        return true;
    }
    virtual size_t hash(bytes_view v) override {
        return std::hash<bytes_view>()(v);
    }
    virtual bytes from_string(sstring_view s) override {
        throw std::runtime_error("not implemented");
    }
    virtual sstring to_string(const bytes& b) override {
        throw std::runtime_error("not implemented");
    }
    virtual ::shared_ptr<cql3::cql3_type> as_cql3_type() override {
        return cql3::cql3_type::timeuuid;
    }
private:
    static int compare_bytes(bytes_view o1, bytes_view o2) {
        auto compare_pos = [&] (unsigned pos, int mask, int ifequal) {
            int d = (o1[pos] & mask) - (o2[pos] & mask);
            return d ? d : ifequal;
        };
        return compare_pos(6, 0xf,
            compare_pos(7, 0xff,
                compare_pos(4, 0xff,
                    compare_pos(5, 0xff,
                        compare_pos(0, 0xff,
                            compare_pos(1, 0xff,
                                compare_pos(2, 0xff,
                                    compare_pos(3, 0xff, 0))))))));
    }
    friend class uuid_type_impl;
};

struct timestamp_type_impl : simple_type_impl<db_clock::time_point> {
    timestamp_type_impl() : simple_type_impl("timestamp") {}
    virtual void serialize(const boost::any& value, bytes::iterator& out) override {
        uint64_t v = boost::any_cast<db_clock::time_point>(value).time_since_epoch().count();
        v = net::hton(v);
        out = std::copy_n(reinterpret_cast<const char*>(&v), sizeof(v), out);
    }
    virtual size_t serialized_size(const boost::any& value) override {
        return 8;
    }
    virtual boost::any deserialize(bytes_view in) override {
        if (in.empty()) {
            return {};
        }
        auto v = read_simple_exactly<uint64_t>(in);
        return boost::any(db_clock::time_point(db_clock::duration(v)));
    }
    // FIXME: isCompatibleWith(timestampuuid)
    virtual bytes from_string(sstring_view s) override {
        throw std::runtime_error("not implemented");
    }
    virtual sstring to_string(const bytes& b) override {
        throw std::runtime_error("not implemented");
    }
    virtual ::shared_ptr<cql3::cql3_type> as_cql3_type() override {
        return cql3::cql3_type::timestamp;
    }
    virtual bool is_value_compatible_with_internal(abstract_type& other) override {
        return &other == this || &other == date_type.get() || &other == long_type.get();
    }
};

struct uuid_type_impl : abstract_type {
    uuid_type_impl() : abstract_type("uuid") {}
    virtual void serialize(const boost::any& value, bytes::iterator& out) override {
        auto& uuid = boost::any_cast<const utils::UUID&>(value);
        out = std::copy_n(uuid.to_bytes().begin(), sizeof(uuid), out);
    }
    virtual size_t serialized_size(const boost::any& value) override {
        return 16;
    }
    virtual boost::any deserialize(bytes_view v) override {
        if (v.empty()) {
            return {};
        }
        auto msb = read_simple<uint64_t>(v);
        auto lsb = read_simple<uint64_t>(v);
        if (!v.empty()) {
            throw marshal_exception();
        }
        return boost::any(utils::UUID(msb, lsb));
    }
    virtual bool less(bytes_view b1, bytes_view b2) override {
        if (b1.size() < 16) {
            return b2.size() < 16 ? false : true;
        }
        if (b2.size() < 16) {
            return false;
        }
        auto v1 = (b1[6] >> 4) & 0x0f;
        auto v2 = (b2[6] >> 4) & 0x0f;

        if (v1 != v2) {
            return v1 < v2;
        }

        if (v1 == 1) {
            auto c1 = timeuuid_type_impl::compare_bytes(b1, b2);
            auto c2 = timeuuid_type_impl::compare_bytes(b2, b1);
            // Require strict ordering
            if (c1 != c2) {
                return c1;
            }
        }
        return less_unsigned(b1, b2);
    }
    // FIXME: isCompatibleWith(uuid)
    virtual bool is_byte_order_equal() const override {
        return true;
    }
    virtual size_t hash(bytes_view v) override {
        return std::hash<bytes_view>()(v);
    }
    virtual bytes from_string(sstring_view s) override {
        throw std::runtime_error("not implemented");
    }
    virtual sstring to_string(const bytes& b) override {
        throw std::runtime_error("not implemented");
    }
    virtual ::shared_ptr<cql3::cql3_type> as_cql3_type() override {
        return cql3::cql3_type::uuid;
    }
    virtual bool is_value_compatible_with_internal(abstract_type& other) override {
        return &other == this || &other == timeuuid_type.get();
    }
};

std::ostream& operator<<(std::ostream& os, const bytes& b) {
    return os << to_hex(b);
}

std::ostream& operator<<(std::ostream& os, const bytes_opt& b) {
    if (b) {
        return os << *b;
    }
    return os << "null";
}

namespace std {

std::ostream& operator<<(std::ostream& os, const bytes_view& b) {
    return os << to_hex(b);
}

}

struct inet_addr_type_impl : abstract_type {
    inet_addr_type_impl() : abstract_type("inet_addr") {}
    virtual void serialize(const boost::any& value, bytes::iterator& out) override {
        // FIXME: support ipv6
        auto& ipv4 = boost::any_cast<const net::ipv4_address&>(value);
        uint32_t u = htonl(ipv4.ip);
        out = std::copy_n(reinterpret_cast<const char*>(&u), sizeof(u), out);
    }
    virtual size_t serialized_size(const boost::any& value) override {
        return 4;
    }
    virtual boost::any deserialize(bytes_view v) override {
        if (v.empty()) {
            return {};
        }
        if (v.size() == 16) {
            throw std::runtime_error("IPv6 addresses not supported");
        }
        auto ip = read_simple<int32_t>(v);
        if (!v.empty()) {
            throw marshal_exception();
        }
        return boost::any(net::ipv4_address(ip));
    }
    virtual bool less(bytes_view v1, bytes_view v2) override {
        return less_unsigned(v1, v2);
    }
    virtual bool is_byte_order_equal() const override {
        return true;
    }
    virtual bool is_byte_order_comparable() const override {
        return true;
    }
    virtual size_t hash(bytes_view v) override {
        return std::hash<bytes_view>()(v);
    }
    virtual bytes from_string(sstring_view s) override {
        throw std::runtime_error("not implemented");
    }
    virtual sstring to_string(const bytes& b) override {
        throw std::runtime_error("not implemented");
    }
    virtual ::shared_ptr<cql3::cql3_type> as_cql3_type() override {
        return cql3::cql3_type::inet;
    }
};

// Integer of same length of a given type. This is useful because our
// ntoh functions only know how to operate on integers.
template <typename T> struct int_of_size;
template <typename D, typename I> struct int_of_size_impl {
    using dtype = D;
    using itype = I;
    static_assert(sizeof(dtype) == sizeof(itype), "size mismatch");
    static_assert(alignof(dtype) == alignof(itype), "align mismatch");
};
template <> struct int_of_size<double> :
    public int_of_size_impl<double, uint64_t> {};
template <> struct int_of_size<float> :
    public int_of_size_impl<float, uint32_t> {};

template <typename T>
struct float_type_traits {
    static double read_nonempty(bytes_view v) {
        union {
            T d;
            typename int_of_size<T>::itype i;
        } x;
        x.i = read_simple_exactly<typename int_of_size<T>::itype>(v);
        return x.d;
    }
};
template<> struct simple_type_traits<float> : public float_type_traits<float> {};
template<> struct simple_type_traits<double> : public float_type_traits<double> {};


template <typename T>
struct floating_type_impl : public simple_type_impl<T> {
    floating_type_impl(sstring name) : simple_type_impl<T>(std::move(name)) {}
    virtual void serialize(const boost::any& value, bytes::iterator& out) override {
        T d = boost::any_cast<const T&>(value);
        if (std::isnan(d)) {
            // Java's Double.doubleToLongBits() documentation specifies that
            // any nan must be serialized to the same specific value
            d = std::numeric_limits<T>::quiet_NaN();
        }
        union {
            T d;
            typename int_of_size<T>::itype i;
        } x;
        x.d = d;
        auto u = net::hton(x.i);
        out = std::copy_n(reinterpret_cast<const char*>(&u), sizeof(u), out);
    }
    virtual size_t serialized_size(const boost::any& value) override {
        return sizeof(T);
    }

    virtual boost::any deserialize(bytes_view v) override {
        if (v.empty()) {
            return {};
        }
        union {
            T d;
            typename int_of_size<T>::itype i;
        } x;
        x.i = read_simple<typename int_of_size<T>::itype>(v);
        if (!v.empty()) {
            throw marshal_exception();
        }
        return boost::any(x.d);
    }
    virtual bytes from_string(sstring_view s) override {
        throw std::runtime_error("not implemented");
    }
    virtual sstring to_string(const bytes& b) override {
        throw std::runtime_error("not implemented");
    }
};

struct double_type_impl : floating_type_impl<double> {
    double_type_impl() : floating_type_impl{"double"} { }
    virtual ::shared_ptr<cql3::cql3_type> as_cql3_type() override {
        return cql3::cql3_type::double_;
    }
};

struct float_type_impl : floating_type_impl<float> {
    float_type_impl() : floating_type_impl{"float"} { }
    virtual ::shared_ptr<cql3::cql3_type> as_cql3_type() override {
        return cql3::cql3_type::float_;
    }
};

struct empty_type_impl : abstract_type {
    empty_type_impl() : abstract_type("void") {}
    virtual void serialize(const boost::any& value, bytes::iterator& out) override {
    }
    virtual size_t serialized_size(const boost::any& value) override {
        return 0;
    }

    virtual bool less(bytes_view v1, bytes_view v2) override {
        return false;
    }
    virtual size_t hash(bytes_view v) override {
        return 0;
    }
    virtual boost::any deserialize(bytes_view v) override {
        return {};
    }
    virtual sstring to_string(const bytes& b) override {
        // FIXME:
        abort();
    }
    virtual bytes from_string(sstring_view text) override {
        // FIXME:
        abort();
    }
    virtual shared_ptr<cql3::cql3_type> as_cql3_type() override {
        // Can't happen
        abort();
    }
};


thread_local logging::logger collection_type_impl::_logger("collection_type_impl");
const size_t collection_type_impl::max_elements;

const collection_type_impl::kind collection_type_impl::kind::map(
        [] (shared_ptr<cql3::column_specification> collection, bool is_key) -> shared_ptr<cql3::column_specification> {
            // FIXME: implement
            // return isKey ? Maps.keySpecOf(collection) : Maps.valueSpecOf(collection);
            abort();
        });
const collection_type_impl::kind collection_type_impl::kind::set(
        [] (shared_ptr<cql3::column_specification> collection, bool is_key) -> shared_ptr<cql3::column_specification> {
            // FIXME: implement
            // return Sets.valueSpecOf(collection);
            abort();
        });
const collection_type_impl::kind collection_type_impl::kind::list(
        [] (shared_ptr<cql3::column_specification> collection, bool is_key) -> shared_ptr<cql3::column_specification> {
            // FIXME: implement
            // return Lists.valueSpecOf(collection);
            abort();
        });

shared_ptr<cql3::column_specification>
collection_type_impl::kind::make_collection_receiver(shared_ptr<cql3::column_specification> collection, bool is_key) const {
    return _impl(std::move(collection), is_key);
}

shared_ptr<cql3::column_specification>
collection_type_impl::make_collection_receiver(shared_ptr<cql3::column_specification> collection, bool is_key) {
    return _kind.make_collection_receiver(std::move(collection), is_key);
}

std::vector<atomic_cell>
collection_type_impl::enforce_limit(std::vector<atomic_cell> cells, int version) {
    assert(is_multi_cell());
    if (version >= 3 || cells.size() <= max_elements) {
        return cells;
    }
    _logger.error("Detected collection with {} elements, more than the {} limit. Only the first {} elements will be returned to the client. "
            "Please see http://cassandra.apache.org/doc/cql3/CQL.html#collections for more details.", cells.size(), max_elements, max_elements);
    cells.erase(cells.begin() + max_elements, cells.end());
    return cells;
}

bytes
collection_type_impl::serialize_for_native_protocol(std::vector<atomic_cell> cells, int version) {
    assert(is_multi_cell());
    cells = enforce_limit(std::move(cells), version);
    std::vector<bytes> values = serialized_values(std::move(cells));
    // FIXME: implement
    abort();
    // return CollectionSerializer.pack(values, cells.size(), version);
}

bool
collection_type_impl::is_compatible_with(abstract_type& previous) {
    // FIXME: implement
    abort();
}

shared_ptr<cql3::cql3_type>
collection_type_impl::as_cql3_type() {
    if (!_cql3_type) {
        auto name = cql3_type_name();
        if (!is_multi_cell()) {
            name = "frozen<" + name + ">";
        }
        _cql3_type = make_shared<cql3::cql3_type>(name, shared_from_this(), false);
    }
    return _cql3_type;
}

bytes
collection_type_impl::to_value(collection_mutation::view mut, serialization_format sf) {
    return to_value(deserialize_mutation_form(mut), sf);
}

collection_type_impl::mutation
collection_type_impl::mutation_view::materialize() const {
    collection_type_impl::mutation m;
    m.tomb = tomb;
    m.cells.reserve(cells.size());
    for (auto&& e : cells) {
        m.cells.emplace_back(bytes(e.first.begin(), e.first.end()), e.second);
    }
    return m;
}


size_t collection_size_len(serialization_format sf) {
    if (sf.using_32_bits_for_collections()) {
        return sizeof(int32_t);
    }
    return sizeof(uint16_t);
}

size_t collection_value_len(serialization_format sf) {
    if (sf.using_32_bits_for_collections()) {
        return sizeof(int32_t);
    }
    return sizeof(uint16_t);
}


int read_collection_size(bytes_view& in, serialization_format sf) {
    if (sf.using_32_bits_for_collections()) {
        return read_simple<int32_t>(in);
    } else {
        return read_simple<uint16_t>(in);
    }
}

void write_collection_size(bytes::iterator& out, int size, serialization_format sf) {
    if (sf.using_32_bits_for_collections()) {
        serialize_int32(out, size);
    } else {
        serialize_int16(out, uint16_t(size));
    }
}

bytes_view read_collection_value(bytes_view& in, serialization_format sf) {
    auto size = sf.using_32_bits_for_collections() ? read_simple<int32_t>(in) : read_simple<uint16_t>(in);
    return read_simple_bytes(in, size);
}

void write_collection_value(bytes::iterator& out, serialization_format sf, bytes_view val_bytes) {
    if (sf.using_32_bits_for_collections()) {
        serialize_int32(out, int32_t(val_bytes.size()));
    } else {
        serialize_int16(out, uint16_t(val_bytes.size()));
    }
    out = std::copy_n(val_bytes.begin(), val_bytes.size(), out);
}

void write_collection_value(bytes::iterator& out, serialization_format sf, data_type type, const boost::any& value) {
    size_t val_len = type->serialized_size(value);

    if (sf.using_32_bits_for_collections()) {
        serialize_int32(out, val_len);
    } else {
        serialize_int16(out, val_len);
    }

    type->serialize(value, out);
}

shared_ptr<map_type_impl>
map_type_impl::get_instance(data_type keys, data_type values, bool is_multi_cell) {
    return intern::get_instance(std::move(keys), std::move(values), is_multi_cell);
}

map_type_impl::map_type_impl(data_type keys, data_type values, bool is_multi_cell)
        : collection_type_impl("map<" + keys->name() + ", " + values->name() + ">", kind::map)
        , _keys(std::move(keys))
        , _values(std::move(values))
        , _is_multi_cell(is_multi_cell) {
}

data_type
map_type_impl::freeze() {
    if (_is_multi_cell) {
        return get_instance(_keys, _values, false);
    } else {
        return shared_from_this();
    }
}

bool
map_type_impl::is_compatible_with_frozen(collection_type_impl& previous) {
    assert(!_is_multi_cell);
    auto* p = dynamic_cast<map_type_impl*>(&previous);
    if (!p) {
        return false;
    }
    return _keys->is_compatible_with(*p->_keys)
            && _values->is_compatible_with(*p->_values);
}

bool
map_type_impl::is_value_compatible_with_frozen(collection_type_impl& previous) {
    assert(!_is_multi_cell);
    auto* p = dynamic_cast<map_type_impl*>(&previous);
    if (!p) {
        return false;
    }
    return _keys->is_compatible_with(*p->_keys)
            && _values->is_value_compatible_with(*p->_values);
}

bool
map_type_impl::less(bytes_view o1, bytes_view o2) {
    return compare_maps(_keys, _values, o1, o2) < 0;
}

int32_t
map_type_impl::compare_maps(data_type keys, data_type values, bytes_view o1, bytes_view o2) {
    if (o1.empty()) {
        return o2.empty() ? 0 : -1;
    } else if (o2.empty()) {
        return 1;
    }
    auto sf = serialization_format::internal();
    int size1 = read_collection_size(o1, sf);
    int size2 = read_collection_size(o2, sf);
    // FIXME: use std::lexicographical_compare()
    for (int i = 0; i < std::min(size1, size2); ++i) {
        auto k1 = read_collection_value(o1, sf);
        auto k2 = read_collection_value(o2, sf);
        auto cmp = keys->compare(k1, k2);
        if (cmp != 0) {
            return cmp;
        }
        auto v1 = read_collection_value(o1, sf);
        auto v2 = read_collection_value(o2, sf);
        cmp = values->compare(v1, v2);
        if (cmp != 0) {
            return cmp;
        }
    }
    return size1 == size2 ? 0 : (size1 < size2 ? -1 : 1);
}

void
map_type_impl::serialize(const boost::any& value, bytes::iterator& out) {
    return serialize(value, out, serialization_format::internal());
}

size_t
map_type_impl::serialized_size(const boost::any& value) {
    auto& m = boost::any_cast<const native_type&>(value);
    size_t len = collection_size_len(serialization_format::internal());
    size_t psz = collection_value_len(serialization_format::internal());
    for (auto&& kv : m) {
        len += psz + _keys->serialized_size(kv.first);
        len += psz + _values->serialized_size(kv.second);
    }

    return len;
}

void
map_type_impl::serialize(const boost::any& value, bytes::iterator& out, serialization_format sf) {
    auto& m = boost::any_cast<const native_type&>(value);
    write_collection_size(out, m.size(), sf);
    for (auto&& kv : m) {
        write_collection_value(out, sf, _keys, kv.first);
        write_collection_value(out, sf, _values, kv.second);
    }
}

boost::any
map_type_impl::deserialize(bytes_view v) {
    return deserialize(v, serialization_format::internal());
}

boost::any
map_type_impl::deserialize(bytes_view in, serialization_format sf) {
    if (in.empty()) {
        return {};
    }
    native_type m;
    auto size = read_collection_size(in, sf);
    for (int i = 0; i < size; ++i) {
        auto kb = read_collection_value(in, sf);
        auto k = _keys->deserialize(kb);
        auto vb = read_collection_value(in, sf);
        auto v = _values->deserialize(vb);
        m.insert(m.end(), std::make_pair(std::move(k), std::move(v)));
    }
    return { std::move(m) };
}

sstring
map_type_impl::to_string(const bytes& b) {
    // FIXME:
    abort();
}

size_t
map_type_impl::hash(bytes_view v) {
    // FIXME:
    abort();
}

bytes
map_type_impl::from_string(sstring_view text) {
    // FIXME:
    abort();
}

std::vector<bytes>
map_type_impl::serialized_values(std::vector<atomic_cell> cells) {
    // FIXME:
    abort();
}

bytes
map_type_impl::to_value(mutation_view mut, serialization_format sf) {
    std::vector<bytes_view> tmp;
    tmp.reserve(mut.cells.size() * 2);
    for (auto&& e : mut.cells) {
        if (e.second.is_live(mut.tomb)) {
            tmp.emplace_back(e.first);
            tmp.emplace_back(e.second.value());
        }
    }
    return pack(tmp.begin(), tmp.end(), tmp.size() / 2, sf);
}

bytes
map_type_impl::serialize_partially_deserialized_form(
        const std::vector<std::pair<bytes_view, bytes_view>>& v, serialization_format sf) {
    size_t len = collection_value_len(sf) * v.size() * 2 + collection_size_len(sf);
    for (auto&& e : v) {
        len += e.first.size() + e.second.size();
    }
    bytes b(bytes::initialized_later(), len);
    bytes::iterator out = b.begin();

    write_collection_size(out, v.size(), sf);
    for (auto&& e : v) {
        write_collection_value(out, sf, e.first);
        write_collection_value(out, sf, e.second);
    }
    return b;


}
sstring
map_type_impl::cql3_type_name() const {
    return sprint("map<%s, %s>", _keys->as_cql3_type(), _values->as_cql3_type());
}

auto collection_type_impl::deserialize_mutation_form(collection_mutation::view cm) -> mutation_view {
    auto&& in = cm.data;
    mutation_view ret;
    auto has_tomb = read_simple<bool>(in);
    if (has_tomb) {
        auto ts = read_simple<api::timestamp_type>(in);
        auto ttl = read_simple<gc_clock::duration::rep>(in);
        ret.tomb = tombstone{ts, gc_clock::time_point(gc_clock::duration(ttl))};
    }
    auto nr = read_simple<uint32_t>(in);
    ret.cells.reserve(nr);
    for (uint32_t i = 0; i != nr; ++i) {
        // FIXME: we could probably avoid the need for size
        auto ksize = read_simple<uint32_t>(in);
        auto key = read_simple_bytes(in, ksize);
        auto vsize = read_simple<uint32_t>(in);
        auto value = atomic_cell_view::from_bytes(read_simple_bytes(in, vsize));
        ret.cells.emplace_back(key, value);
    }
    assert(in.empty());
    return ret;
}

bool collection_type_impl::is_empty(collection_mutation::view cm) {
    auto&& in = cm.data;
    auto has_tomb = read_simple<bool>(in);
    if (has_tomb) {
        in.remove_prefix(sizeof(api::timestamp_type) + sizeof(gc_clock::duration::rep));
    }
    return read_simple<uint32_t>(in) == 0;
}

bool collection_type_impl::is_any_live(collection_mutation::view cm, tombstone tomb) {
    auto&& in = cm.data;
    auto has_tomb = read_simple<bool>(in);
    if (has_tomb) {
        auto ts = read_simple<api::timestamp_type>(in);
        auto ttl = read_simple<gc_clock::duration::rep>(in);
        tomb.apply(tombstone{ts, gc_clock::time_point(gc_clock::duration(ttl))});
    }
    auto nr = read_simple<uint32_t>(in);
    for (uint32_t i = 0; i != nr; ++i) {
        auto ksize = read_simple<uint32_t>(in);
        in.remove_prefix(ksize);
        auto vsize = read_simple<uint32_t>(in);
        auto value = atomic_cell_view::from_bytes(read_simple_bytes(in, vsize));
        if (value.is_live(tomb)) {
            return true;
        }
    }
    return false;
}

template <typename Iterator>
collection_mutation::one
do_serialize_mutation_form(
        std::experimental::optional<tombstone> tomb,
        boost::iterator_range<Iterator> cells) {
    auto element_size = [] (size_t c, auto&& e) -> size_t {
        return c + 8 + e.first.size() + e.second.serialize().size();
    };
    auto size = accumulate(cells, (size_t)4, element_size);
    size += 1;
    if (tomb) {
        size += sizeof(tomb->timestamp) + sizeof(tomb->deletion_time);
    }
    bytes ret(bytes::initialized_later(), size);
    bytes::iterator out = ret.begin();
    *out++ = bool(tomb);
    if (tomb) {
        write(out, tomb->timestamp);
        write(out, tomb->deletion_time.time_since_epoch().count());
    }
    auto writeb = [&out] (bytes_view v) {
        serialize_int32(out, v.size());
        out = std::copy_n(v.begin(), v.size(), out);
    };
    // FIXME: overflow?
    serialize_int32(out, boost::distance(cells));
    for (auto&& kv : cells) {
        auto&& k = kv.first;
        auto&& v = kv.second;
        writeb(k);
        writeb(v.serialize());
    }
    return collection_mutation::one{std::move(ret)};
}

collection_mutation::one
collection_type_impl::serialize_mutation_form(const mutation& mut) {
    return do_serialize_mutation_form(mut.tomb, boost::make_iterator_range(mut.cells.begin(), mut.cells.end()));
}

collection_mutation::one
collection_type_impl::serialize_mutation_form(mutation_view mut) {
    return do_serialize_mutation_form(mut.tomb, boost::make_iterator_range(mut.cells.begin(), mut.cells.end()));
}

collection_mutation::one
collection_type_impl::serialize_mutation_form_only_live(mutation_view mut) {
    return do_serialize_mutation_form(mut.tomb, mut.cells | boost::adaptors::filtered([t = mut.tomb] (auto&& e) {
        return e.second.is_live(t);
    }));
}

collection_mutation::one
collection_type_impl::merge(collection_mutation::view a, collection_mutation::view b) {
    auto aa = deserialize_mutation_form(a);
    auto bb = deserialize_mutation_form(b);
    mutation_view merged;
    merged.cells.reserve(aa.cells.size() + bb.cells.size());
    using element_type = std::pair<bytes_view, atomic_cell_view>;
    auto key_type = name_comparator();
    auto compare = [key_type] (const element_type& e1, const element_type& e2) {
        return key_type->less(e1.first, e2.first);
    };
    auto merge = [this] (const element_type& e1, const element_type& e2) {
        // FIXME: use std::max()?
        return std::make_pair(e1.first, compare_atomic_cell_for_merge(e1.second, e2.second) > 0 ? e1.second : e2.second);
    };
    // applied to a tombstone, returns a predicate checking whether a cell is killed by
    // the tombstone
    auto cell_killed = [] (const std::experimental::optional<tombstone>& t) {
        return [&t] (const element_type& e) {
            if (!t) {
                return false;
            }
            // tombstone wins if timestamps equal here, unlike row tombstones
            if (t->timestamp < e.second.timestamp()) {
                return false;
            }
            return true;
            // FIXME: should we consider TTLs too?
        };
    };
    combine(aa.cells.begin(), std::remove_if(aa.cells.begin(), aa.cells.end(), cell_killed(bb.tomb)),
            bb.cells.begin(), std::remove_if(bb.cells.begin(), bb.cells.end(), cell_killed(aa.tomb)),
            std::back_inserter(merged.cells),
            compare,
            merge);
    merged.tomb = std::max(aa.tomb, bb.tomb);
    return serialize_mutation_form(merged);
}

bytes_opt
collection_type_impl::reserialize(serialization_format from, serialization_format to, bytes_view_opt v) {
    if (!v) {
        return std::experimental::nullopt;
    }
    auto val = deserialize(*v, from);
    bytes ret(bytes::initialized_later(), serialized_size(v));  // FIXME: serialized_size want @to
    auto out = ret.begin();
    serialize(std::move(val), out, to);
    return ret;
}

// iterator that takes a set or list in serialized form, and emits
// each element, still in serialized form
class listlike_partial_deserializing_iterator
          : public std::iterator<std::input_iterator_tag, bytes_view> {
    bytes_view* _in;
    int _remain;
    bytes_view _cur;
    serialization_format _sf;
private:
    struct end_tag {};
    listlike_partial_deserializing_iterator(bytes_view& in, serialization_format sf)
            : _in(&in), _sf(sf) {
        _remain = read_collection_size(*_in, _sf);
        parse();
    }
    listlike_partial_deserializing_iterator(end_tag)
            : _remain(0), _sf(serialization_format::internal()) {  // _sf is bogus, but doesn't matter
    }
public:
    bytes_view operator*() const { return _cur; }
    listlike_partial_deserializing_iterator& operator++() {
        --_remain;
        parse();
        return *this;
    }
    void operator++(int) {
        --_remain;
        parse();
    }
    bool operator==(const listlike_partial_deserializing_iterator& x) const {
        return _remain == x._remain;
    }
    bool operator!=(const listlike_partial_deserializing_iterator& x) const {
        return _remain != x._remain;
    }
    static listlike_partial_deserializing_iterator begin(bytes_view& in, serialization_format sf) {
        return { in, sf };
    }
    static listlike_partial_deserializing_iterator end(bytes_view in, serialization_format sf) {
        return { end_tag() };
    }
private:
    void parse() {
        if (_remain) {
            _cur = read_collection_value(*_in, _sf);
        } else {
            _cur = {};
        }
    }
};

set_type
set_type_impl::get_instance(data_type elements, bool is_multi_cell) {
    return intern::get_instance(elements, is_multi_cell);
}

set_type_impl::set_type_impl(data_type elements, bool is_multi_cell)
        : collection_type_impl("sey<" + elements->name() + ">", kind::set)
        , _elements(std::move(elements))
        , _is_multi_cell(is_multi_cell) {
}

data_type
set_type_impl::value_comparator() {
    return empty_type;
}

data_type
set_type_impl::freeze() {
    if (_is_multi_cell) {
        return get_instance(_elements, false);
    } else {
        return shared_from_this();
    }
}

bool
set_type_impl::is_compatible_with_frozen(collection_type_impl& previous) {
    assert(!_is_multi_cell);
    auto* p = dynamic_cast<set_type_impl*>(&previous);
    if (!p) {
        return false;
    }
    return _elements->is_compatible_with(*p->_elements);

}

bool
set_type_impl::is_value_compatible_with_frozen(collection_type_impl& previous) {
    return is_compatible_with(previous);
}

bool
set_type_impl::less(bytes_view o1, bytes_view o2) {
    using llpdi = listlike_partial_deserializing_iterator;
    auto sf = serialization_format::internal();
    return std::lexicographical_compare(
            llpdi::begin(o1, sf), llpdi::end(o1, sf),
            llpdi::begin(o2, sf), llpdi::end(o2, sf),
            [this] (bytes_view o1, bytes_view o2) { return _elements->less(o1, o2); });
}

void
set_type_impl::serialize(const boost::any& value, bytes::iterator& out) {
    return serialize(value, out, serialization_format::internal());
}

size_t
set_type_impl::serialized_size(const boost::any& value) {
    auto& s = boost::any_cast<const native_type&>(value);
    size_t len = collection_size_len(serialization_format::internal());
    size_t psz = collection_value_len(serialization_format::internal());
    for (auto&& e : s) {
        len += psz + _elements->serialized_size(e);
    }
    return len;
}



void
set_type_impl::serialize(const boost::any& value, bytes::iterator& out, serialization_format sf) {
    auto& s = boost::any_cast<const native_type&>(value);
    write_collection_size(out, s.size(), sf);
    for (auto&& e : s) {
        write_collection_value(out, sf, _elements, e);
    }
}

boost::any
set_type_impl::deserialize(bytes_view in) {
    return deserialize(in, serialization_format::internal());
}

boost::any
set_type_impl::deserialize(bytes_view in, serialization_format sf) {
    if (in.empty()) {
        return {};
    }
    auto nr = read_collection_size(in, sf);
    native_type s;
    s.reserve(nr);
    for (int i = 0; i != nr; ++i) {
        auto e = _elements->deserialize(read_collection_value(in, sf));
        if (e.empty()) {
            throw marshal_exception();
        }
        s.push_back(std::move(e));
    }
    return { s };
}

sstring
set_type_impl::to_string(const bytes& b) {
    using llpdi = listlike_partial_deserializing_iterator;
    std::ostringstream out;
    bool first = true;
    auto v = bytes_view(b);
    auto sf = serialization_format::internal();
    std::for_each(llpdi::begin(v, sf), llpdi::end(v, sf), [&first, &out, this] (bytes_view e) {
        if (first) {
            first = false;
        } else {
            out << "; ";
        }
        out << _elements->to_string(bytes(e.begin(), e.end()));
    });
    return out.str();
}

size_t
set_type_impl::hash(bytes_view v) {
    // FIXME:
    abort();
}

bytes
set_type_impl::from_string(sstring_view text) {
    // FIXME:
    abort();
}

std::vector<bytes>
set_type_impl::serialized_values(std::vector<atomic_cell> cells) {
    // FIXME:
    abort();
}

bytes
set_type_impl::to_value(mutation_view mut, serialization_format sf) {
    std::vector<bytes_view> tmp;
    tmp.reserve(mut.cells.size());
    for (auto&& e : mut.cells) {
        if (e.second.is_live(mut.tomb)) {
            tmp.emplace_back(e.first);
        }
    }
    return pack(tmp.begin(), tmp.end(), tmp.size(), sf);
}

bytes
set_type_impl::serialize_partially_deserialized_form(
        const std::vector<bytes_view>& v, serialization_format sf) {
    return pack(v.begin(), v.end(), v.size(), sf);
}

sstring
set_type_impl::cql3_type_name() const {
    return sprint("set<%s>", _elements->as_cql3_type());
}

list_type
list_type_impl::get_instance(data_type elements, bool is_multi_cell) {
    return intern::get_instance(elements, is_multi_cell);
}

list_type_impl::list_type_impl(data_type elements, bool is_multi_cell)
        : collection_type_impl("list<" + elements->name() + ">", kind::list)
        , _elements(std::move(elements))
        , _is_multi_cell(is_multi_cell) {
}

data_type
list_type_impl::name_comparator() {
    return timeuuid_type;
}

data_type
list_type_impl::value_comparator() {
    return _elements;
}

data_type
list_type_impl::freeze() {
    if (_is_multi_cell) {
        return get_instance(_elements, false);
    } else {
        return shared_from_this();
    }
}

bool
list_type_impl::is_compatible_with_frozen(collection_type_impl& previous) {
    assert(!_is_multi_cell);
    auto* p = dynamic_cast<list_type_impl*>(&previous);
    if (!p) {
        return false;
    }
    return _elements->is_compatible_with(*p->_elements);

}

bool
list_type_impl::is_value_compatible_with_frozen(collection_type_impl& previous) {
    auto& lp = dynamic_cast<list_type_impl&>(previous);
    return _elements->is_value_compatible_with_internal(*lp._elements);
}

bool
list_type_impl::less(bytes_view o1, bytes_view o2) {
    using llpdi = listlike_partial_deserializing_iterator;
    auto sf = serialization_format::internal();
    return std::lexicographical_compare(
            llpdi::begin(o1, sf), llpdi::end(o1, sf),
            llpdi::begin(o2, sf), llpdi::end(o2, sf),
            [this] (bytes_view o1, bytes_view o2) { return _elements->less(o1, o2); });
}

void
list_type_impl::serialize(const boost::any& value, bytes::iterator& out) {
    return serialize(value, out, serialization_format::internal());
}

void
list_type_impl::serialize(const boost::any& value, bytes::iterator& out, serialization_format sf) {
    auto& s = boost::any_cast<const native_type&>(value);
    write_collection_size(out, s.size(), sf);
    for (auto&& e : s) {
        write_collection_value(out, sf, _elements, e);
    }
}

size_t
list_type_impl::serialized_size(const boost::any& value) {
    auto& s = boost::any_cast<const native_type&>(value);
    size_t len = collection_size_len(serialization_format::internal());
    size_t psz = collection_value_len(serialization_format::internal());
    for (auto&& e : s) {
        len += psz + _elements->serialized_size(e);
    }
    return len;
}

boost::any
list_type_impl::deserialize(bytes_view in) {
    return deserialize(in, serialization_format::internal());
}

boost::any
list_type_impl::deserialize(bytes_view in, serialization_format sf) {
    if (in.empty()) {
        return {};
    }
    auto nr = read_collection_size(in, sf);
    native_type s;
    s.reserve(nr);
    for (int i = 0; i != nr; ++i) {
        auto e = _elements->deserialize(read_collection_value(in, sf));
        if (e.empty()) {
            throw marshal_exception();
        }
        s.push_back(std::move(e));
    }
    return { s };
}

sstring
list_type_impl::to_string(const bytes& b) {
    using llpdi = listlike_partial_deserializing_iterator;
    std::ostringstream out;
    bool first = true;
    auto v = bytes_view(b);
    auto sf = serialization_format::internal();
    std::for_each(llpdi::begin(v, sf), llpdi::end(v, sf), [&first, &out, this] (bytes_view e) {
        if (first) {
            first = false;
        } else {
            out << ", ";
        }
        out << _elements->to_string(bytes(e.begin(), e.end()));
    });
    return out.str();
}

size_t
list_type_impl::hash(bytes_view v) {
    // FIXME:
    abort();
}

bytes
list_type_impl::from_string(sstring_view text) {
    // FIXME:
    abort();
}

std::vector<bytes>
list_type_impl::serialized_values(std::vector<atomic_cell> cells) {
    // FIXME:
    abort();
}

bytes
list_type_impl::to_value(mutation_view mut, serialization_format sf) {
    std::vector<bytes_view> tmp;
    tmp.reserve(mut.cells.size());
    for (auto&& e : mut.cells) {
        if (e.second.is_live(mut.tomb)) {
            tmp.emplace_back(e.second.value());
        }
    }
    return pack(tmp.begin(), tmp.end(), tmp.size(), sf);
}

sstring
list_type_impl::cql3_type_name() const {
    return sprint("list<%s>", _elements->as_cql3_type());
}

tuple_type_impl::tuple_type_impl(std::vector<data_type> types)
        : abstract_type(make_name(types)), _types(std::move(types)) {
    for (auto& t : _types) {
        t = t->freeze();
    }
}

shared_ptr<tuple_type_impl>
tuple_type_impl::get_instance(std::vector<data_type> types) {
    return ::make_shared<tuple_type_impl>(std::move(types));
}

int32_t
tuple_type_impl::compare(bytes_view v1, bytes_view v2) {
    return lexicographical_tri_compare(_types.begin(), _types.end(),
            tuple_deserializing_iterator::start(v1), tuple_deserializing_iterator::finish(v1),
            tuple_deserializing_iterator::start(v2), tuple_deserializing_iterator::finish(v2),
            tri_compare_opt);
}

bool
tuple_type_impl::less(bytes_view v1, bytes_view v2) {
    return tuple_type_impl::compare(v1, v2) < 0;
}

size_t
tuple_type_impl::serialized_size(const boost::any& value) {
    size_t size = 0;
    if (value.empty()) {
        return size;
    }
    auto&& v = boost::any_cast<const native_type&>(value);
    auto find_serialized_size = [] (auto&& t_v) {
        const data_type& t = boost::get<0>(t_v);
        const boost::any& v = boost::get<1>(t_v);
        return 4 + (v.empty() ? 0 : t->serialized_size(v));
    };
    return boost::accumulate(boost::combine(_types, v) | boost::adaptors::transformed(find_serialized_size), 0);
}

void
tuple_type_impl::serialize(const boost::any& value, bytes::iterator& out) {
    if (value.empty()) {
        return;
    }
    auto&& v = boost::any_cast<const native_type&>(value);
    auto do_serialize = [&out] (auto&& t_v) {
        const data_type& t = boost::get<0>(t_v);
        const boost::any& v = boost::get<1>(t_v);
        if (v.empty()) {
            write(out, int32_t(-1));
        } else {
            write(out, int32_t(t->serialized_size(v)));
            t->serialize(v, out);
        }
    };
    boost::range::for_each(boost::combine(_types, v), do_serialize);
}

boost::any
tuple_type_impl::deserialize(bytes_view v) {
    native_type ret;
    ret.reserve(_types.size());
    auto ti = _types.begin();
    auto vi = tuple_deserializing_iterator::start(v);
    while (ti != _types.end() && vi != tuple_deserializing_iterator::finish(v)) {
        boost::any obj;
        if (*vi) {
            obj = (*ti)->deserialize(**vi);
        }
        ret.push_back(std::move(obj));
        ++ti;
        ++vi;
    }
    ret.resize(_types.size());
    return { ret };
}

std::vector<bytes_view_opt>
tuple_type_impl::split(bytes_view v) const {
    return { tuple_deserializing_iterator::start(v), tuple_deserializing_iterator::finish(v) };
}

bytes
tuple_type_impl::from_string(sstring_view s) {
    throw std::runtime_error("not implemented");
}

sstring
tuple_type_impl::to_string(const bytes& b) {
    throw std::runtime_error("not implemented");
}

bool
tuple_type_impl::equals(const abstract_type& other) const {
    auto x = dynamic_cast<const tuple_type_impl*>(&other);
    return x && std::equal(_types.begin(), _types.end(), x->_types.begin(), x->_types.end(),
            [] (auto&& a, auto&& b) { return a->equals(b); });
}

bool
tuple_type_impl::is_compatible_with(abstract_type& previous) {
    return check_compatibility(previous, &abstract_type::is_compatible_with);
}

bool
tuple_type_impl::is_value_compatible_with_internal(abstract_type& previous) {
    return check_compatibility(previous, &abstract_type::is_value_compatible_with);
}

bool
tuple_type_impl::check_compatibility(abstract_type& previous, bool (abstract_type::*predicate)(abstract_type&)) const {
    auto* x = dynamic_cast<tuple_type_impl*>(&previous);
    if (!x) {
        return false;
    }
    auto c = std::mismatch(
                _types.begin(), _types.end(),
                x->_types.begin(), x->_types.end(),
                [predicate] (data_type a, data_type b) { return ((*a).*predicate)(*b); });
    return c.first == _types.end();  // previous allowed to be longer
}

size_t
tuple_type_impl::hash(bytes_view v) {
    auto apply_hash = [] (auto&& type_value) {
        auto&& type = boost::get<0>(type_value);
        auto&& value = boost::get<1>(type_value);
        return value ? type->hash(*value) : 0;
    };
    // FIXME: better accumulation function
    return boost::accumulate(combine(_types, make_range(v))
                             | boost::adaptors::transformed(apply_hash),
                             0,
                             std::bit_xor<>());
}

shared_ptr<cql3::cql3_type>
tuple_type_impl::as_cql3_type() {
    throw "not implemented";
}

sstring
tuple_type_impl::make_name(const std::vector<data_type>& types) {
    return sprint("tuple<%s>", ::join(", ", types | boost::adaptors::transformed(std::mem_fn(&abstract_type::name))));
}

thread_local const shared_ptr<abstract_type> int32_type(make_shared<int32_type_impl>());
thread_local const shared_ptr<abstract_type> long_type(make_shared<long_type_impl>());
thread_local const shared_ptr<abstract_type> ascii_type(make_shared<string_type_impl>("ascii", [] { return cql3::cql3_type::ascii; }));
thread_local const shared_ptr<abstract_type> bytes_type(make_shared<bytes_type_impl>());
thread_local const shared_ptr<abstract_type> utf8_type(make_shared<string_type_impl>("utf8", [] { return cql3::cql3_type::text; }));
thread_local const shared_ptr<abstract_type> boolean_type(make_shared<boolean_type_impl>());
thread_local const shared_ptr<abstract_type> date_type(make_shared<date_type_impl>());
thread_local const shared_ptr<abstract_type> timeuuid_type(make_shared<timeuuid_type_impl>());
thread_local const shared_ptr<abstract_type> timestamp_type(make_shared<timestamp_type_impl>());
thread_local const shared_ptr<abstract_type> uuid_type(make_shared<uuid_type_impl>());
thread_local const shared_ptr<abstract_type> inet_addr_type(make_shared<inet_addr_type_impl>());
thread_local const shared_ptr<abstract_type> float_type(make_shared<double_type_impl>());
thread_local const shared_ptr<abstract_type> double_type(make_shared<double_type_impl>());
thread_local const data_type empty_type(make_shared<empty_type_impl>());
