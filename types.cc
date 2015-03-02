/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#include <boost/lexical_cast.hpp>
#include "cql3/cql3_type.hh"
#include "types.hh"
#include "core/print.hh"
#include "net/ip.hh"

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

thread_local shared_ptr<abstract_type> int32_type(make_shared<int32_type_impl>());
thread_local shared_ptr<abstract_type> long_type(make_shared<long_type_impl>());
thread_local shared_ptr<abstract_type> ascii_type(make_shared<string_type_impl>("ascii", cql3::native_cql3_type::ascii));
thread_local shared_ptr<abstract_type> bytes_type(make_shared<bytes_type_impl>());
thread_local shared_ptr<abstract_type> utf8_type(make_shared<string_type_impl>("utf8", cql3::native_cql3_type::text));
thread_local shared_ptr<abstract_type> boolean_type(make_shared<boolean_type_impl>());
thread_local shared_ptr<abstract_type> date_type(make_shared<date_type_impl>());
thread_local shared_ptr<abstract_type> timeuuid_type(make_shared<timeuuid_type_impl>());
thread_local shared_ptr<abstract_type> timestamp_type(make_shared<timestamp_type_impl>());
thread_local shared_ptr<abstract_type> uuid_type(make_shared<uuid_type_impl>());
thread_local shared_ptr<abstract_type> inet_addr_type(make_shared<inet_addr_type_impl>());
