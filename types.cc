/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#include "types.hh"

template <typename T, typename Compare>
bool
abstract_type::default_less(const bytes& v1, const bytes& v2, Compare compare) {
    auto o1 = deserialize(v1);
    auto o2 = deserialize(v2);
    if (!o1) {
        return bool(o2);
    }
    if (!o2) {
        return false;
    }
    auto& x1 = boost::any_cast<const T&>(*o1);
    auto& x2 = boost::any_cast<const T&>(*o2);
    return compare(x1, x2);
}


template <typename T>
struct simple_type_impl : abstract_type {
    simple_type_impl(sstring name) : abstract_type(std::move(name)) {}
    virtual bool less(const bytes& v1, const bytes& v2) override {
        return default_less<T>(v1, v2);
    }
};

struct int32_type_impl : simple_type_impl<int32_t> {
    int32_type_impl() : simple_type_impl<int32_t>("int32") {}
    virtual void serialize(const boost::any& value, std::ostream& out) override {
        auto v = boost::any_cast<const int32_t&>(value);
        auto u = net::hton(uint32_t(v));
        out.write(reinterpret_cast<const char*>(&u), sizeof(u));
    }
    virtual object_opt deserialize(std::istream& in) {
        uint32_t u;
        auto n = in.rdbuf()->sgetn(reinterpret_cast<char*>(&u), sizeof(u));
        if (!n) {
            return {};
        }
        if (n != 4) {
            throw marshal_exception();
        }
        auto v = int32_t(net::ntoh(u));
        return boost::any(v);
    }
};

struct long_type_impl : simple_type_impl<int64_t> {
    long_type_impl() : simple_type_impl<int64_t>("long") {}
    virtual void serialize(const boost::any& value, std::ostream& out) override {
        auto v = boost::any_cast<const int64_t&>(value);
        auto u = net::hton(uint64_t(v));
        out.write(reinterpret_cast<const char*>(&u), sizeof(u));
    }
    virtual object_opt deserialize(std::istream& in) {
        uint64_t u;
        auto n = in.rdbuf()->sgetn(reinterpret_cast<char*>(&u), sizeof(u));
        if (!n) {
            return {};
        }
        if (n != 8) {
            throw marshal_exception();
        }
        auto v = int64_t(net::ntoh(u));
        return boost::any(v);
    }
};

struct string_type_impl : public abstract_type {
    string_type_impl(sstring name) : abstract_type(name) {}
    virtual void serialize(const boost::any& value, std::ostream& out) override {
        auto& v = boost::any_cast<const sstring&>(value);
        out.write(v.c_str(), v.size());
    }
    virtual object_opt deserialize(std::istream& in) {
        std::vector<char> tmp(std::istreambuf_iterator<char>(in.rdbuf()),
            std::istreambuf_iterator<char>());
        // FIXME: validation?
        return boost::any(sstring(tmp.data(), tmp.size()));
    }
    virtual bool less(const bytes& v1, const bytes& v2) override {
        return less_unsigned(v1, v2);
    }
};

struct bytes_type_impl : public abstract_type {
    bytes_type_impl() : abstract_type("bytes") {}
    virtual void serialize(const boost::any& value, std::ostream& out) override {
        auto& v = boost::any_cast<const bytes&>(value);
        out.write(v.c_str(), v.size());
    }
    virtual object_opt deserialize(std::istream& in) {
        std::vector<char> tmp(std::istreambuf_iterator<char>(in.rdbuf()),
            std::istreambuf_iterator<char>());
        return boost::any(bytes(reinterpret_cast<const char*>(tmp.data()), tmp.size()));
    }
    virtual bool less(const bytes& v1, const bytes& v2) override {
        return less_unsigned(v1, v2);
    }
};

struct boolean_type_impl : public simple_type_impl<bool> {
    boolean_type_impl() : simple_type_impl<bool>("boolean") {}
    virtual void serialize(const boost::any& value, std::ostream& out) override {
        auto v = boost::any_cast<bool>(value);
        char c = v;
        out.put(c);
    }
    virtual object_opt deserialize(std::istream& in) override {
        char tmp;
        auto n = in.rdbuf()->sgetn(&tmp, 1);
        if (n == 0) {
            return {};
        }
        return boost::any(tmp != 0);
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
    virtual object_opt deserialize(std::istream& in) override {
        int64_t tmp;
        auto n = in.rdbuf()->sgetn(reinterpret_cast<char*>(&tmp), 8);
        if (n == 0) {
            return {};
        }
        if (n != 8) {
            throw marshal_exception();
        }
        tmp = net::ntoh(uint64_t(tmp));
        return boost::any(db_clock::time_point(db_clock::duration(tmp)));
    }
    virtual bool less(const bytes& b1, const bytes& b2) override {
        return compare_unsigned(b1, b2);
    }
    virtual bool is_byte_order_comparable() const override {
        return true;
    }
};

struct timeuuid_type_impl : public abstract_type {
    timeuuid_type_impl() : abstract_type("timeuuid") {}
    virtual void serialize(const boost::any& value, std::ostream& out) override {
        // FIXME: optimize
        auto& uuid = boost::any_cast<const utils::UUID&>(value);
        out.write(to_bytes(uuid).begin(), 16);
    }
    virtual object_opt deserialize(std::istream& in) override {
        struct tmp { uint64_t msb, lsb; } t;
        auto n = in.rdbuf()->sgetn(reinterpret_cast<char*>(&t), 16);
        if (n == 0) {
            return {};
        }
        if (n != 16) {
            throw marshal_exception();
        }
        return boost::any(utils::UUID(net::ntoh(t.msb), net::ntoh(t.lsb)));
    }
    virtual bool less(const bytes& b1, const bytes& b2) override {
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
private:
    static int compare_bytes(const bytes& o1, const bytes& o2) {
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
    virtual object_opt deserialize(std::istream& is) override {
        uint64_t v;
        auto n = is.rdbuf()->sgetn(reinterpret_cast<char*>(&v), 8);
        if (n == 0) {
            return {};
        }
        if (n != 8) {
            throw marshal_exception();
        }
        return boost::any(db_clock::time_point(db_clock::duration(net::ntoh(v))));
    }
    // FIXME: isCompatibleWith(timestampuuid)
};

struct uuid_type_impl : abstract_type {
    uuid_type_impl() : abstract_type("uuid") {}
    virtual void serialize(const boost::any& value, std::ostream& out) override {
        // FIXME: optimize
        auto& uuid = boost::any_cast<const utils::UUID&>(value);
        out.write(to_bytes(uuid).begin(), 16);
    }
    virtual object_opt deserialize(std::istream& in) override {
        struct tmp { uint64_t msb, lsb; } t;
        auto n = in.rdbuf()->sgetn(reinterpret_cast<char*>(&t), 16);
        if (n == 0) {
            return {};
        }
        if (n != 16) {
            throw marshal_exception();
        }
        return boost::any(utils::UUID(net::ntoh(t.msb), net::ntoh(t.lsb)));
    }
    virtual bool less(const bytes& b1, const bytes& b2) override {
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
};

thread_local shared_ptr<abstract_type> int32_type(make_shared<int32_type_impl>());
thread_local shared_ptr<abstract_type> long_type(make_shared<long_type_impl>());
thread_local shared_ptr<abstract_type> ascii_type(make_shared<string_type_impl>("ascii"));
thread_local shared_ptr<abstract_type> bytes_type(make_shared<bytes_type_impl>());
thread_local shared_ptr<abstract_type> utf8_type(make_shared<string_type_impl>("utf8"));
thread_local shared_ptr<abstract_type> boolean_type(make_shared<boolean_type_impl>());
thread_local shared_ptr<abstract_type> date_type(make_shared<date_type_impl>());
thread_local shared_ptr<abstract_type> timeuuid_type(make_shared<timeuuid_type_impl>());
thread_local shared_ptr<abstract_type> timestamp_type(make_shared<timestamp_type_impl>());
thread_local shared_ptr<abstract_type> uuid_type(make_shared<uuid_type_impl>());
