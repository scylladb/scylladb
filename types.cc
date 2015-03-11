/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#include <boost/lexical_cast.hpp>
#include "cql3/cql3_type.hh"
#include "types.hh"
#include "core/print.hh"
#include "net/ip.hh"
#include "database.hh"
#include "util/serialization.hh"
#include "combine.hh"
#include <cmath>
#include <sstream>

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
    virtual void serialize(const boost::any& value, std::ostream& out) override {
        auto v = boost::any_cast<const T&>(value);
        auto u = net::hton(v);
        out.write(reinterpret_cast<const char*>(&u), sizeof(u));
    }
    virtual object_opt deserialize(bytes_view v) override {
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
        return cql3::native_cql3_type::int_;
    }
};

struct long_type_impl : integer_type_impl<int64_t> {
    long_type_impl() : integer_type_impl{"long"}
    { }

    virtual ::shared_ptr<cql3::cql3_type> as_cql3_type() override {
        return cql3::native_cql3_type::bigint;
    }
    virtual bool is_value_compatible_with_internal(abstract_type& other) override {
        return &other == this || &other == date_type.get() || &other == timestamp_type.get();
    }
};

struct string_type_impl : public abstract_type {
    string_type_impl(sstring name, shared_ptr<cql3::cql3_type> cql3_type)
        : abstract_type(name), _cql3_type(cql3_type) {}
    virtual void serialize(const boost::any& value, std::ostream& out) override {
        auto& v = boost::any_cast<const sstring&>(value);
        out.write(v.c_str(), v.size());
    }
    virtual object_opt deserialize(bytes_view v) override {
        // FIXME: validation?
        return boost::any(sstring(v.begin(), v.end()));
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
        return to_bytes(s);
    }
    virtual sstring to_string(const bytes& b) override {
        return sstring(b);
    }
    virtual ::shared_ptr<cql3::cql3_type> as_cql3_type() override {
        return _cql3_type;
    }
    shared_ptr<cql3::cql3_type> _cql3_type;
};

struct bytes_type_impl final : public abstract_type {
    bytes_type_impl() : abstract_type("bytes") {}
    virtual void serialize(const boost::any& value, std::ostream& out) override {
        auto& v = boost::any_cast<const bytes&>(value);
        out.write(v.c_str(), v.size());
    }
    virtual object_opt deserialize(bytes_view v) override {
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
        return cql3::native_cql3_type::blob;
    }
    virtual bool is_value_compatible_with_internal(abstract_type& other) override {
        return true;
    }
};

struct boolean_type_impl : public simple_type_impl<bool> {
    boolean_type_impl() : simple_type_impl<bool>("boolean") {}
    virtual void serialize(const boost::any& value, std::ostream& out) override {
        auto v = boost::any_cast<bool>(value);
        char c = v;
        out.put(c);
    }
    virtual object_opt deserialize(bytes_view v) override {
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
        return cql3::native_cql3_type::boolean;
    }
};

struct date_type_impl : public abstract_type {
    date_type_impl() : abstract_type("date") {}
    virtual void serialize(const boost::any& value, std::ostream& out) override {
        auto v = boost::any_cast<db_clock::time_point>(value);
        int64_t i = v.time_since_epoch().count();
        i = net::hton(uint64_t(i));
        out.write(reinterpret_cast<char*>(&i), 8);
    }
    virtual object_opt deserialize(bytes_view v) override {
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
        return cql3::native_cql3_type::timestamp;
    }
    virtual bool is_value_compatible_with_internal(abstract_type& other) override {
        return &other == this || &other == timestamp_type.get() || &other == long_type.get();
    }
};

struct timeuuid_type_impl : public abstract_type {
    timeuuid_type_impl() : abstract_type("timeuuid") {}
    virtual void serialize(const boost::any& value, std::ostream& out) override {
        // FIXME: optimize
        auto& uuid = boost::any_cast<const utils::UUID&>(value);
        out.write(to_bytes(uuid).begin(), 16);
    }
    virtual object_opt deserialize(bytes_view v) override {
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
        return cql3::native_cql3_type::timeuuid;
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
    virtual void serialize(const boost::any& value, std::ostream& out) override {
        uint64_t v = boost::any_cast<db_clock::time_point>(value).time_since_epoch().count();
        v = net::hton(v);
        out.write(reinterpret_cast<char*>(&v), 8);
    }
    virtual object_opt deserialize(bytes_view in) override {
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
        return cql3::native_cql3_type::timestamp;
    }
    virtual bool is_value_compatible_with_internal(abstract_type& other) override {
        return &other == this || &other == date_type.get() || &other == long_type.get();
    }
};

struct uuid_type_impl : abstract_type {
    uuid_type_impl() : abstract_type("uuid") {}
    virtual void serialize(const boost::any& value, std::ostream& out) override {
        // FIXME: optimize
        auto& uuid = boost::any_cast<const utils::UUID&>(value);
        out.write(to_bytes(uuid).begin(), 16);
    }
    virtual object_opt deserialize(bytes_view v) override {
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
        return cql3::native_cql3_type::uuid;
    }
    virtual bool is_value_compatible_with_internal(abstract_type& other) override {
        return &other == this || &other == timeuuid_type.get();
    }
};

std::ostream& operator<<(std::ostream& os, const bytes& b) {
    return os << to_hex(b);
}

struct inet_addr_type_impl : abstract_type {
    inet_addr_type_impl() : abstract_type("inet_addr") {}
    virtual void serialize(const boost::any& value, std::ostream& out) override {
        // FIXME: support ipv6
        auto& ipv4 = boost::any_cast<const net::ipv4_address&>(value);
        uint32_t u = htonl(ipv4.ip);
        out.write(reinterpret_cast<const char*>(&u), 4);
    }
    virtual object_opt deserialize(bytes_view v) override {
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
        return cql3::native_cql3_type::inet;
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
    virtual void serialize(const boost::any& value, std::ostream& out) override {
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
        out.write(reinterpret_cast<const char*>(&u), sizeof(u));
    }
    virtual object_opt deserialize(bytes_view v) override {
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
        return cql3::native_cql3_type::double_;
    }
};

struct float_type_impl : floating_type_impl<float> {
    float_type_impl() : floating_type_impl{"float"} { }
    virtual ::shared_ptr<cql3::cql3_type> as_cql3_type() override {
        return cql3::native_cql3_type::float_;
    }
};

struct empty_type_impl : abstract_type {
    empty_type_impl() : abstract_type("void") {}
    virtual void serialize(const boost::any& value, std::ostream& out) override {
    }
    virtual bool less(bytes_view v1, bytes_view v2) override {
        return false;
    }
    virtual size_t hash(bytes_view v) override {
        return 0;
    }
    virtual object_opt deserialize(bytes_view v) override {
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

std::vector<atomic_cell::one>
collection_type_impl::enforce_limit(std::vector<atomic_cell::one> cells, int version) {
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
collection_type_impl::serialize_for_native_protocol(std::vector<atomic_cell::one> cells, int version) {
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
    // FIXME: implement
    abort();
}

int read_collection_size(bytes_view& in, int version) {
    if (version >= 3) {
        return read_simple<int32_t>(in);
    } else {
        return read_simple<uint16_t>(in);
    }
}

void write_collection_size(std::ostream& out, int size, int version) {
    if (version >= 3) {
        serialize_int32(out, size);
    } else {
        serialize_int16(out, uint16_t(size));
    }
}

bytes_view read_collection_value(bytes_view& in, int version) {
    auto size = version >= 3 ? read_simple<int32_t>(in) : read_simple<uint16_t>(in);
    return read_simple_bytes(in, size);
}

void write_collection_value(std::ostream& out, int version, bytes_view val_bytes) {
    if (version >= 3) {
        serialize_int32(out, int32_t(val_bytes.size()));
    } else {
        serialize_int16(out, uint16_t(val_bytes.size()));
    }
    out.rdbuf()->sputn(val_bytes.data(), val_bytes.size());
}

void write_collection_value(std::ostream& out, int version, data_type type, const boost::any& value) {
    // We have to copy here, because we can't guess the size.
    // FIXME: somehow.
    std::ostringstream tmp;
    type->serialize(value, tmp);
    auto val_bytes = tmp.str();
    write_collection_value(out, version, val_bytes);
}

template <typename BytesViewIterator>
bytes
collection_type_impl::pack(BytesViewIterator start, BytesViewIterator finish, int elements, int protocol_version) {
    std::ostringstream out;
    write_collection_size(out, elements, protocol_version);
    while (start != finish) {
        write_collection_value(out, protocol_version, *start++);
    }
    return out.str();
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
    int protocol_version = 3;
    int size1 = read_collection_size(o1, protocol_version);
    int size2 = read_collection_size(o2, protocol_version);
    // FIXME: use std::lexicographical_compare()
    for (int i = 0; i < std::min(size1, size2); ++i) {
        auto k1 = read_collection_value(o1, protocol_version);
        auto k2 = read_collection_value(o2, protocol_version);
        auto cmp = keys->compare(k1, k2);
        if (cmp != 0) {
            return cmp;
        }
        auto v1 = read_collection_value(o1, protocol_version);
        auto v2 = read_collection_value(o2, protocol_version);
        cmp = values->compare(v1, v2);
        if (cmp != 0) {
            return cmp;
        }
    }
    return size1 == size2 ? 0 : (size1 < size2 ? -1 : 1);
}

void
map_type_impl::serialize(const boost::any& value, std::ostream& out) {
    return serialize(value, out, 3);
}

void
map_type_impl::serialize(const boost::any& value, std::ostream& out, int protocol_version) {
    auto& m = boost::any_cast<const native_type&>(value);
    write_collection_size(out, m.size(), protocol_version);
    for (auto&& kv : m) {
        write_collection_value(out, protocol_version, _keys, kv.first);
        write_collection_value(out, protocol_version, _values, kv.second);
    }
}

object_opt
map_type_impl::deserialize(bytes_view v) {
    return deserialize(v, 3);
}

object_opt
map_type_impl::deserialize(bytes_view in, int protocol_version) {
    if (in.empty()) {
        return {};
    }
    native_type m;
    auto size = read_collection_size(in, protocol_version);
    for (int i = 0; i < size; ++i) {
        auto kb = read_collection_value(in, protocol_version);
        auto k = _keys->deserialize(kb);
        auto vb = read_collection_value(in, protocol_version);
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
map_type_impl::serialized_values(std::vector<atomic_cell::one> cells) {
    // FIXME:
    abort();
}

auto collection_type_impl::deserialize_mutation_form(bytes_view in) -> mutation_view {
    auto nr = read_simple<uint32_t>(in);
    mutation_view ret;
    ret.reserve(nr);
    for (uint32_t i = 0; i != nr; ++i) {
        // FIXME: we could probably avoid the need for size
        auto ksize = read_simple<uint32_t>(in);
        auto key = read_simple_bytes(in, ksize);
        auto vsize = read_simple<uint32_t>(in);
        auto value = atomic_cell::view::from_bytes(read_simple_bytes(in, vsize));
        ret.emplace_back(key, value);
    }
    assert(in.empty());
    return ret;
}

template <typename Iterator>
collection_mutation::one
do_serialize_mutation_form(Iterator begin, Iterator end) {
    std::ostringstream out;
    auto write32 = [&out] (uint32_t v) {
        v = net::hton(v);
        out.write(reinterpret_cast<char*>(&v), sizeof(v));
    };
    auto writeb = [&out, write32] (bytes_view v) {
        write32(v.size());
        out.write(v.begin(), v.size());
    };
    // FIXME: overflow?
    write32(std::distance(begin, end));
    while (begin != end) {
        auto&& kv = *begin++;
        auto&& k = kv.first;
        auto&& v = kv.second;
        writeb(k);
        writeb(v.serialize());
    }
    auto s = out.str();
    return collection_mutation::one{bytes(s.data(), s.size())};
}

collection_mutation::one
collection_type_impl::serialize_mutation_form(const mutation& mut) {
    return do_serialize_mutation_form(mut.begin(), mut.end());
}

collection_mutation::one
collection_type_impl::serialize_mutation_form(mutation_view mut) {
    return do_serialize_mutation_form(mut.begin(), mut.end());
}

collection_mutation::one
collection_type_impl::merge(collection_mutation::view a, collection_mutation::view b) {
    auto aa = deserialize_mutation_form(a.data);
    auto bb = deserialize_mutation_form(b.data);
    mutation_view merged;
    merged.reserve(aa.size() + bb.size());
    using element_type = std::pair<bytes_view, atomic_cell::view>;
    auto key_type = name_comparator();
    auto compare = [key_type] (const element_type& e1, const element_type& e2) {
        return key_type->less(e1.first, e2.first);
    };
    auto merge = [this] (const element_type& e1, const element_type& e2) {
        // FIXME: use std::max()?
        return std::make_pair(e1.first, compare_atomic_cell_for_merge(e1.second, e2.second) > 0 ? e1.second : e2.second);
    };
    combine(aa.begin(), aa.end(),
            bb.begin(), bb.end(),
            std::back_inserter(merged),
            compare,
            merge);
    return serialize_mutation_form(merged);
}

// iterator that takes a set or list in serialized form, and emits
// each element, still in serialized form
class listlike_partial_deserializing_iterator
          : public std::iterator<std::input_iterator_tag, bytes_view> {
    bytes_view* _in;
    int _remain;
    bytes_view _cur;
    int _protocol_version;
private:
    struct end_tag {};
    listlike_partial_deserializing_iterator(bytes_view in, int protocol_version)
            : _in(&in), _protocol_version(protocol_version) {
        _remain = read_collection_size(*_in, _protocol_version);
        parse();
    }
    listlike_partial_deserializing_iterator(end_tag)
            : _remain(0) {
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
    static listlike_partial_deserializing_iterator begin(bytes_view in, int protocol_version) {
        return { in, protocol_version };
    }
    static listlike_partial_deserializing_iterator end(bytes_view in, int protocol_version) {
        return { end_tag() };
    }
private:
    void parse() {
        if (_remain) {
            _cur = read_collection_value(*_in, _protocol_version);
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
    return std::lexicographical_compare(
            llpdi::begin(o1, 3), llpdi::end(o1, 3),
            llpdi::begin(o2, 3), llpdi::end(o2, 3),
            [this] (bytes_view o1, bytes_view o2) { return _elements->less(o1, o2); });
}

void
set_type_impl::serialize(const boost::any& value, std::ostream& out) {
    return serialize(value, out, 3);
}

void
set_type_impl::serialize(const boost::any& value, std::ostream& out, int protocol_version) {
    auto s = boost::any_cast<const native_type&>(value);
    write_collection_size(out, s.size(), protocol_version);
    for (auto&& e : s) {
        write_collection_value(out, protocol_version, _elements, e);
    }
}

object_opt
set_type_impl::deserialize(bytes_view in) {
    return deserialize(in, 3);
}

object_opt
set_type_impl::deserialize(bytes_view in, int protocol_version) {
    if (in.empty()) {
        return {};
    }
    auto nr = read_collection_size(in, protocol_version);
    native_type s;
    s.reserve(nr);
    for (int i = 0; i != nr; ++i) {
        auto e = _elements->deserialize(read_collection_value(in, protocol_version));
        if (!e) {
            throw marshal_exception();
        }
        s.push_back(std::move(*e));
    }
    return { s };
}

sstring
set_type_impl::to_string(const bytes& b) {
    using llpdi = listlike_partial_deserializing_iterator;
    std::ostringstream out;
    bool first = true;
    std::for_each(llpdi::begin(b, 3), llpdi::end(b, 3), [&first, &out, this] (bytes_view e) {
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
set_type_impl::serialized_values(std::vector<atomic_cell::one> cells) {
    // FIXME:
    abort();
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
    return std::lexicographical_compare(
            llpdi::begin(o1, 3), llpdi::end(o1, 3),
            llpdi::begin(o2, 3), llpdi::end(o2, 3),
            [this] (bytes_view o1, bytes_view o2) { return _elements->less(o1, o2); });
}

void
list_type_impl::serialize(const boost::any& value, std::ostream& out) {
    return serialize(value, out, 3);
}

void
list_type_impl::serialize(const boost::any& value, std::ostream& out, int protocol_version) {
    auto s = boost::any_cast<const native_type&>(value);
    write_collection_size(out, s.size(), protocol_version);
    for (auto&& e : s) {
        write_collection_value(out, protocol_version, _elements, e);
    }
}

object_opt
list_type_impl::deserialize(bytes_view in) {
    return deserialize(in, 3);
}

object_opt
list_type_impl::deserialize(bytes_view in, int protocol_version) {
    if (in.empty()) {
        return {};
    }
    auto nr = read_collection_size(in, protocol_version);
    native_type s;
    s.reserve(nr);
    for (int i = 0; i != nr; ++i) {
        auto e = _elements->deserialize(read_collection_value(in, protocol_version));
        if (!e) {
            throw marshal_exception();
        }
        s.push_back(std::move(*e));
    }
    return { s };
}

sstring
list_type_impl::to_string(const bytes& b) {
    using llpdi = listlike_partial_deserializing_iterator;
    std::ostringstream out;
    bool first = true;
    std::for_each(llpdi::begin(b, 3), llpdi::end(b, 3), [&first, &out, this] (bytes_view e) {
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
list_type_impl::serialized_values(std::vector<atomic_cell::one> cells) {
    // FIXME:
    abort();
}

thread_local const shared_ptr<abstract_type> int32_type(make_shared<int32_type_impl>());
thread_local const shared_ptr<abstract_type> long_type(make_shared<long_type_impl>());
thread_local const shared_ptr<abstract_type> ascii_type(make_shared<string_type_impl>("ascii", cql3::native_cql3_type::ascii));
thread_local const shared_ptr<abstract_type> bytes_type(make_shared<bytes_type_impl>());
thread_local const shared_ptr<abstract_type> utf8_type(make_shared<string_type_impl>("utf8", cql3::native_cql3_type::text));
thread_local const shared_ptr<abstract_type> boolean_type(make_shared<boolean_type_impl>());
thread_local const shared_ptr<abstract_type> date_type(make_shared<date_type_impl>());
thread_local const shared_ptr<abstract_type> timeuuid_type(make_shared<timeuuid_type_impl>());
thread_local const shared_ptr<abstract_type> timestamp_type(make_shared<timestamp_type_impl>());
thread_local const shared_ptr<abstract_type> uuid_type(make_shared<uuid_type_impl>());
thread_local const shared_ptr<abstract_type> inet_addr_type(make_shared<inet_addr_type_impl>());
thread_local const shared_ptr<abstract_type> float_type(make_shared<double_type_impl>());
thread_local const shared_ptr<abstract_type> double_type(make_shared<double_type_impl>());
thread_local const data_type empty_type(make_shared<empty_type_impl>());
